"""Record / replay all non-ABS HTTP traffic (Audible, Hardcover, Goodreads) for offline matching tests.

RECORD_DIR=<dir>  (+ RECORD_REUSE=1: already recorded answers are served from disk, only new requests go live)
                  every response of an outside request is stored (key = method + url + params + body; no headers/tokens).
REPLAY_DIR=<dir>  outside requests are answered from that folder; nothing leaves the machine. A missing recording
                  returns HTTP 599 and is appended to <dir>/_misses.jsonl so the test can report it.
ABS itself (ABS_URL host) is never touched here.
"""
import hashlib, json, os, urllib.parse
import requests

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
        if REPLAY_DIR or reuse:
            if os.path.exists(path):
                rec = json.load(open(path, encoding='utf-8'))
                r = requests.Response()
                r.status_code, r._content, r.url = rec['status'], rec['body'].encode('utf-8'), rec['url']
                r.headers['Content-Type'] = rec.get('content_type', 'text/html')
                r.encoding = 'utf-8'
                return r
            with open(os.path.join(folder, '_misses.jsonl'), 'a', encoding='utf-8') as f:
                f.write(json.dumps({'method': method, 'url': url, 'params': kw.get('params')}, default=str) + '\n')
            r = requests.Response(); r.status_code, r._content, r.url = 599, b'', url
            return r
        resp = orig(self, method, url, **kw)
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump({'method': method, 'url': resp.url, 'params': kw.get('params'), 'status': resp.status_code,
                           'content_type': resp.headers.get('Content-Type', ''), 'body': resp.text}, f, ensure_ascii=False, default=str)
        except OSError:
            pass
        return resp

    requests.sessions.Session.request = patched
