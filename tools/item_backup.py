"""Backup / restore of single ABS items (metadata, tags, cover) - safety net for the real pilot run.
  backup:  python tools/item_backup.py backup  <keys.txt> <out_dir>      (GET only: /api/items/<id>?expanded=1 and the cover file)
  check:   python tools/item_backup.py diff    <keys.txt> <out_dir>      (GET only: what differs now from the backup)
  restore: python tools/item_backup.py restore <keys.txt> <out_dir> [--apply]   (without --apply: only shows what it would do)
Env: CONFIG_DIR with settings.json (ABS_URL, API_TOKEN). The token is never printed."""
import json, os, sys
import requests

FIELDS = ['title', 'subtitle', 'authors', 'narrators', 'series', 'genres', 'publishedYear', 'publisher', 'description',
          'isbn', 'asin', 'language', 'explicit', 'abridged']

cfg = json.load(open(os.path.join(os.environ.get('CONFIG_DIR', '/config'), 'settings.json'), encoding='utf-8'))
URL, TOKEN = cfg['ABS_URL'].rstrip('/'), cfg['API_TOKEN']
S = requests.Session(); S.headers['Authorization'] = f'Bearer {TOKEN}'
mode, keys_file, out = sys.argv[1], sys.argv[2], sys.argv[3]
apply = '--apply' in sys.argv
ids = [l.strip().split('_', 1)[1] for l in open(keys_file) if l.strip()]
os.makedirs(out, exist_ok=True)


def meta_of(item):
    m = (item.get('media') or {}).get('metadata') or {}
    o = {f: m.get(f) for f in FIELDS}
    o['authors'] = [{'name': a.get('name')} for a in (m.get('authors') or [])]
    o['series'] = [{'name': s.get('name'), 'sequence': s.get('sequence')} for s in (m.get('series') or [])]
    return o


def current(iid):
    return S.get(f'{URL}/api/items/{iid}', params={'expanded': 1}, timeout=30).json()


if mode == 'backup':
    for iid in ids:
        it = current(iid)
        json.dump(it, open(f'{out}/{iid}.json', 'w', encoding='utf-8'), ensure_ascii=False)
        r = S.get(f'{URL}/api/items/{iid}/cover', params={'raw': 1}, timeout=60)
        if r.status_code == 200 and r.content: open(f'{out}/{iid}.cover', 'wb').write(r.content)
        print(f"backed up {iid[:8]} '{meta_of(it)['title']}' cover={'yes' if r.status_code == 200 and r.content else 'none'}")
    print('backup done:', len(ids), 'items ->', out)

elif mode in ('diff', 'restore'):
    for iid in ids:
        b = json.load(open(f'{out}/{iid}.json', encoding='utf-8'))
        old, now = meta_of(b), meta_of(current(iid))
        old_tags, now_tags = (b.get('media') or {}).get('tags') or [], (current(iid).get('media') or {}).get('tags') or []
        diff = {f: old[f] for f in FIELDS if old[f] != now[f]}
        cover_changed = os.path.exists(f'{out}/{iid}.cover')  # a backed-up cover is always uploaded again on restore (we cannot compare files cheaply)
        print(f"{iid[:8]} '{old['title']}': {len(diff)} field(s) differ {sorted(diff)}{' + tags' if old_tags != now_tags else ''}")
        if mode == 'restore' and apply:
            if diff or old_tags != now_tags:
                r = S.patch(f'{URL}/api/items/{iid}/media', json={'metadata': diff, 'tags': old_tags}, timeout=30)
                print('   metadata/tags restored' if r.status_code == 200 else f'   ❌ HTTP {r.status_code}: {r.text[:150]}')
            if cover_changed:
                data = open(f'{out}/{iid}.cover', 'rb').read()
                ext = 'png' if data[:8] == b'\x89PNG\r\n\x1a\n' else 'webp' if data[:4] == b'RIFF' and data[8:12] == b'WEBP' else 'jpg'
                r = S.post(f'{URL}/api/items/{iid}/cover', files={'cover': (f'cover.{ext}', data, f'image/{"jpeg" if ext == "jpg" else ext}')}, timeout=60)
                print('   cover restored' if r.status_code == 200 else f'   ❌ cover HTTP {r.status_code}: {r.text[:150]}')
    if mode == 'restore' and not apply: print('(nothing written - add --apply)')
