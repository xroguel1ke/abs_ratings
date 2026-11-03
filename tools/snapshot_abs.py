"""Read-only snapshot of an ABS library for offline tests (GET requests only).
Env: CONFIG_DIR (settings.json with ABS_URL, API_TOKEN, LIBRARY_IDS), OUT (default tests/private/abs_snapshot.json).
The snapshot contains the user's library -> it stays in a git-ignored folder. The token is never written or printed."""
import json, os, sys
import requests

cfg = json.load(open(os.path.join(os.environ.get('CONFIG_DIR', '/config'), 'settings.json'), encoding='utf-8'))
url, token = cfg['ABS_URL'].rstrip('/'), cfg['API_TOKEN']
out = os.environ.get('OUT', '/src/tests/private/abs_snapshot.json')
s = requests.Session(); s.headers['Authorization'] = f'Bearer {token}'
libs = [l for l in (cfg.get('LIBRARY_IDS') or '').replace(';', ',').split(',') if l.strip()]
snap = {'libraries': s.get(f'{url}/api/libraries', timeout=30).json(), 'lists': {}, 'items': {}}
for lib in libs:
    lib = lib.strip()
    res = s.get(f'{url}/api/libraries/{lib}/items', timeout=120).json()
    snap['lists'][lib] = res
    for i, it in enumerate(res['results']):
        snap['items'][it['id']] = s.get(f"{url}/api/items/{it['id']}", timeout=30).json()
        if i % 200 == 0: print(f'{lib[:8]} {i}/{len(res["results"])}', flush=True)
json.dump(snap, open(out, 'w', encoding='utf-8'), ensure_ascii=False)
print('items:', len(snap['items']), '->', out)
