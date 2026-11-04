"""Fake Audiobookshelf for tests (never talks to a real server). Serves a snapshot JSON
({libraries, lists, items}) and records every write in PATCH_LOG (jsonl) while applying it in memory.
Run: python tools/fake_abs.py --snapshot tests/private/abs_snapshot.json --port 18999 [--patch-log patches.jsonl]
Auth: any 'Bearer <token>' is accepted."""
import argparse, json, os, time
from flask import Flask, jsonify, request

app = Flask(__name__)
SNAP, PATCH_LOG = {}, None


def log_write(kind, iid, body):
    if PATCH_LOG:
        with open(PATCH_LOG, 'a', encoding='utf-8') as f:
            f.write(json.dumps({'ts': time.time(), 'kind': kind, 'item': iid, 'body': body}, ensure_ascii=False) + '\n')


@app.get('/ping')
def ping(): return jsonify(success=True)

@app.get('/api/libraries')
def libraries(): return jsonify(SNAP['libraries'])

@app.get('/api/libraries/<lib>/items')
def lib_items(lib):
    res = SNAP['lists'].get(lib)
    if res is None: return jsonify(error='not found'), 404
    return jsonify(res)

@app.get('/api/items/<iid>')
def item(iid):
    it = SNAP['items'].get(iid)
    return (jsonify(it), 200) if it else (jsonify(error='not found'), 404)

@app.patch('/api/items/<iid>/media')
def patch_media(iid):
    it, body = SNAP['items'].get(iid), request.get_json(force=True, silent=True) or {}
    if not it: return jsonify(error='not found'), 404
    log_write('media', iid, body)
    if body.get('metadata'): it['media']['metadata'].update(body['metadata'])
    if 'tags' in body: it['media']['tags'] = body['tags']
    return jsonify(updated=True)

@app.post('/api/items/<iid>/cover')
def cover(iid):
    f = request.files.get('cover')  # undo uploads the backed-up file (multipart)
    if f and not f.filename.lower().endswith(('.png', '.jpg', '.jpeg', '.webp')):  # like the real ABS
        return ('Invalid image type (Supported: png,jpg,jpeg,webp)', 400)
    log_write('cover', iid, request.get_json(force=True, silent=True) or ({'file': f.filename, 'bytes': len(f.read())} if f else None))
    return jsonify(success=True)

@app.get('/api/items/<iid>/cover')
def get_cover(iid):  # a small fake image, so the backup step of a cover replacement can be tested
    return (b'\xff\xd8\xff\xe0FAKE-COVER-' + iid.encode() + b'\xff\xd9', 200, {'Content-Type': 'image/jpeg'})

@app.delete('/api/items/<iid>/cover')
def del_cover(iid):
    log_write('cover_delete', iid, None)
    return jsonify(success=True)

@app.post('/api/backups')
def backups():
    log_write('backup', None, None)
    return jsonify(backups=[])


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--snapshot', required=True); ap.add_argument('--port', type=int, default=18999)
    ap.add_argument('--patch-log', default=''); ap.add_argument('--extra', default='', help='synthetic batch (tools/synth_make.py) merged into the library')
    a = ap.parse_args()
    SNAP.update(json.load(open(a.snapshot, encoding='utf-8')))
    PATCH_LOG = a.patch_log or None
    if a.extra:  # synthetic books that are NOT in the real library
        ex = json.load(open(a.extra, encoding='utf-8'))
        SNAP['items'].update(ex['items'])
        SNAP['lists'].setdefault(ex['library'], {'results': []})['results'] += ex['list']
    app.run(host='0.0.0.0', port=a.port)
