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
    log_write('cover', iid, request.get_json(force=True, silent=True))
    return jsonify(success=True)

@app.post('/api/backups')
def backups():
    log_write('backup', None, None)
    return jsonify(backups=[])


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--snapshot', required=True); ap.add_argument('--port', type=int, default=18999)
    ap.add_argument('--patch-log', default='')
    a = ap.parse_args()
    SNAP.update(json.load(open(a.snapshot, encoding='utf-8')))
    PATCH_LOG = a.patch_log or None
    app.run(host='0.0.0.0', port=a.port)
