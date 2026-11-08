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

@app.get('/api/libraries/<lib>/series')
def lib_series(lib):  # names only (the script uses them to split the listing's seriesName)
    names = sorted({x['name'] for it in SNAP['items'].values() if it.get('libraryId') == lib
                    for x in (it.get('media') or {}).get('metadata', {}).get('series') or [] if x.get('name')})
    return jsonify(results=[{'id': f"ser{i}", 'name': n} for i, n in enumerate(names)], total=len(names))

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
    sync_listing(iid, it)
    return jsonify(updated=True)

def sync_listing(iid, it):
    """Like ABS: the library listing shows the current metadata (authorName / narratorName / seriesName derived)."""
    import time as _t
    md = it['media']['metadata']
    names = lambda xs: ', '.join((x.get('name') if isinstance(x, dict) else str(x)) for x in (xs or []) if x)
    for lst in SNAP['lists'].values():
        for e in lst.get('results') or []:
            if e.get('id') != iid: continue
            em = e.setdefault('media', {}).setdefault('metadata', {})
            for k in ('title', 'subtitle', 'asin', 'isbn', 'language', 'publisher', 'publishedYear', 'description', 'genres'):
                if k in md: em[k] = md[k]
            if 'authors' in md: em['authorName'] = names(md['authors'])
            if 'narrators' in md: em['narratorName'] = names(md['narrators'])
            if 'series' in md: em['seriesName'] = ', '.join(f"{x.get('name')} #{x.get('sequence')}" if x.get('sequence') else x.get('name') for x in md['series'] or [] if isinstance(x, dict))
            if 'tags' in it['media']: e['media']['tags'] = it['media']['tags']
            e['updatedAt'] = int(_t.time() * 1000)

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

def _authors(lib=None):
    """Author objects derived from the items (+ SNAP['empty_authors'] = authors without books, for the clean-up test)."""
    out = {}
    for it in SNAP['items'].values():
        if lib and it.get('libraryId') not in (lib, None): continue
        for a in ((it.get('media') or {}).get('metadata') or {}).get('authors') or []:
            o = out.setdefault(a.get('name'), {'id': a.get('id') or 'au-' + a.get('name', ''), 'name': a.get('name'), 'numBooks': 0, 'items': []})
            o['numBooks'] += 1; o['items'].append(it['id'])
    for a in SNAP.get('empty_authors') or []:
        if a['id'] not in DELETED_AUTHORS: out.setdefault(a['name'], {'id': a['id'], 'name': a['name'], 'numBooks': 0, 'items': []})
    return [a for a in out.values() if a['id'] not in DELETED_AUTHORS]

DELETED_AUTHORS = set()

@app.get('/api/libraries/<lib>/authors')
def lib_authors(lib): return jsonify(authors=[{k: v for k, v in a.items() if k != 'items'} for a in _authors(lib)])

@app.get('/api/authors/<aid>')
def author(aid):
    a = next((x for x in _authors() if x['id'] == aid), None)
    if not a: return jsonify(error='not found'), 404
    return jsonify(id=a['id'], name=a['name'], numBooks=a['numBooks'], libraryItems=[{'id': i} for i in a['items']] if request.args.get('include') == 'items' else None)

@app.delete('/api/authors/<aid>')
def delete_author(aid):
    a = next((x for x in _authors() if x['id'] == aid), None)
    if not a: return jsonify(error='not found'), 404
    if a['numBooks']: return jsonify(error='author has books'), 400  # the fake refuses what must never happen
    DELETED_AUTHORS.add(aid); log_write('author_delete', None, {'id': aid, 'name': a['name']})
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
