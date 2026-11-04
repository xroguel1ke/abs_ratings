"""Record / replay all non-ABS HTTP traffic (Audible, Hardcover, Goodreads) for offline matching tests.

RECORD_DIR=<dir>  (+ RECORD_REUSE=1: already recorded answers are served from disk, only new requests go live)
                  every response of an outside request is stored (key = method + url + params + body; no headers/tokens).
REPLAY_DIR=<dir>  outside requests are answered from that folder; nothing leaves the machine. A missing recording
                  returns HTTP 599 and is appended to <dir>/_misses.jsonl so the test can report it.
ABS itself (ABS_URL host) is never touched here.
"""
import base64, hashlib, json, os, time, urllib.parse
import requests

_LAST_LIVE = {}  # host -> monotonic time of the last live request
_GAP = {"api.hardcover.app": 1.1, "www.goodreads.com": float(os.environ.get("GOODREADS_INTERVAL", 10))}  # seconds between live requests per host (Audible default 0.6)

RECORD_DIR = os.environ.get('RECORD_DIR', '').strip()
REPLAY_DIR = os.environ.get('REPLAY_DIR', '').strip()


def _key(method, url, params, body):
    q = urllib.parse.urlparse(url).query
    parts = [method.upper(), url.split('?')[0], q, json.dumps(params, sort_keys=True, default=str) if params else '',
             json.dumps(body, sort_keys=True, default=str) if body else '']
    return hashlib.sha1('\n'.join(parts).encode('utf-8')).hexdigest()


def install():
    if not (RECORD_DIR or REPLAY_DIR): return
    abs_host = urllib.parse.urlparse(os.environ.get('ABS_URL', '')).netloc
    orig = requests.sessions.Session.request
    folder = REPLAY_DIR or RECORD_DIR
    os.makedirs(folder, exist_ok=True)

    def patched(self, method, url, **kw):
        if abs_host and urllib.parse.urlparse(url).netloc == abs_host:
            return orig(self, method, url, **kw)
        k = _key(method, url, kw.get('params'), kw.get('json') if kw.get('json') is not None else kw.get('data'))
        path = os.path.join(folder, k + '.json')
        reuse = RECORD_DIR and os.environ.get("RECORD_REUSE") == "1" and os.path.exists(path)  # hybrid: known answers offline, new ones live
        no_live = [h for h in os.environ.get("RECORD_NO_LIVE", "").split(",") if h]  # hosts that must NEVER be asked live in this run (quota protection)
        if RECORD_DIR and not REPLAY_DIR and not reuse and any(h in url for h in no_live):
            with open(os.path.join(folder, "_misses.jsonl"), "a", encoding="utf-8") as f:
                f.write(json.dumps({"method": method, "url": url, "params": kw.get("params"), "blocked": "no live requests to this host"}, default=str) + "\n")
            r = requests.Response(); r.status_code, r._content, r.url = 599, b"", url
            return r
        if REPLAY_DIR or reuse:
            if os.path.exists(path):
                rec = json.load(open(path, encoding='utf-8'))
                r = requests.Response()
                r.status_code, r.url = rec['status'], rec['url']
                r._content = base64.b64decode(rec['body_b64']) if 'body_b64' in rec else rec['body'].encode('utf-8')  # images are stored as base64
                r.headers['Content-Type'] = rec.get('content_type', 'text/html')
                r.encoding = 'utf-8'
                return r
            with open(os.path.join(folder, '_misses.jsonl'), 'a', encoding='utf-8') as f:
                f.write(json.dumps({'method': method, 'url': url, 'params': kw.get('params')}, default=str) + '\n')
            r = requests.Response(); r.status_code, r._content, r.url = 599, b'', url
            return r
        host = urllib.parse.urlparse(url).netloc
        wait = _GAP.get(host, 0.6) - (time.monotonic() - _LAST_LIVE.get(host, 0))
        if wait > 0: time.sleep(wait)
        _LAST_LIVE[host] = time.monotonic()
        resp = orig(self, method, url, **kw)
        if resp.status_code == 429 or resp.status_code >= 500: return resp  # transient errors are never recorded (they would be replayed as truth)
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump({'method': method, 'url': resp.url, 'params': kw.get('params'), 'status': resp.status_code,
                           'content_type': resp.headers.get('Content-Type', ''),
                           **({'body_b64': base64.b64encode(resp.content).decode('ascii')} if resp.headers.get('Content-Type', '').startswith('image/') else {'body': resp.text})}, f, ensure_ascii=False, default=str)
        except OSError:
            pass
        return resp

    requests.sessions.Session.request = patched
