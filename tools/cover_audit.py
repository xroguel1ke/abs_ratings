"""Cover audit (READ-ONLY): how many ABS covers differ from the Audible cover of the verified ASIN?
GET requests only (ABS: /api/items/<id>/cover, Audible: catalog API + CDN image). Needs Pillow.
Env: CONFIG_DIR (settings.json for ABS_URL/API_TOKEN), OUT (default /src/tests/private/covers). Private data stays in git-ignored tests/private/.
Steps (resumable): fetch -> hash -> report. Usage: python tools/cover_audit.py fetch|report"""
import io, json, os, sys, time
import requests
from PIL import Image

OUT = os.environ.get('OUT', '/src/tests/private/covers')
os.makedirs(f'{OUT}/abs', exist_ok=True); os.makedirs(f'{OUT}/aud', exist_ok=True)
GOLD = json.load(open('/src/tests/private/golden.json', encoding='utf-8'))['books']
SNAP = json.load(open('/src/tests/private/abs_snapshot.json', encoding='utf-8'))['items']
UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36'


def book_asin(key):
    iid = key.split('_', 1)[1]
    g = GOLD.get(key) or {}
    return iid, g.get('asin') or (((SNAP.get(iid) or {}).get('media') or {}).get('metadata') or {}).get('asin')


def dhash(data, size=16):
    """Difference hash (size*size bits) of the image; robust against resizing/re-compression."""
    im = Image.open(io.BytesIO(data)).convert('L').resize((size + 1, size), Image.LANCZOS)
    px = list(im.getdata())
    bits = 0
    for y in range(size):
        for x in range(size):
            bits = (bits << 1) | (1 if px[y * (size + 1) + x] > px[y * (size + 1) + x + 1] else 0)
    return bits


def fetch():
    cfg = json.load(open(os.path.join(os.environ.get('CONFIG_DIR', '/config'), 'settings.json'), encoding='utf-8'))
    url, token = cfg['ABS_URL'].rstrip('/'), cfg['API_TOKEN']
    s = requests.Session(); s.headers['Authorization'] = f'Bearer {token}'
    n = 0
    for key, g in GOLD.items():
        if g.get('status') != 'confirmed': continue
        iid, asin = book_asin(key)
        if not asin: continue
        fa, fb = f'{OUT}/abs/{iid}.img', f'{OUT}/aud/{asin}.img'
        if not os.path.exists(fa):
            r = s.get(f'{url}/api/items/{iid}/cover', params={'width': 400}, timeout=60)
            open(fa, 'wb').write(r.content if r.status_code == 200 else b'')
        if not os.path.exists(fb):
            data = b''
            for dom in ('com', 'de', 'co.uk', 'fr', 'it', 'es', 'ca', 'com.au'):
                time.sleep(0.5)
                r = requests.get(f'https://api.audible.{dom}/1.0/catalog/products/{asin}', params={'response_groups': 'media'}, headers={'User-Agent': UA}, timeout=30)
                imgs = ((r.json().get('product') or {}).get('product_images') or {}) if r.status_code == 200 else {}
                if imgs:
                    u = imgs.get('500') or imgs[max(imgs, key=int)]
                    ri = requests.get(u, headers={'User-Agent': UA}, timeout=30)
                    if ri.status_code == 200: data = ri.content
                    break
            open(fb, 'wb').write(data)
        n += 1
        if n % 100 == 0: print('fetched', n, flush=True)
    print('fetch done', n)


def report():
    rows = []
    for key, g in GOLD.items():
        if g.get('status') != 'confirmed': continue
        iid, asin = book_asin(key)
        if not asin: continue
        try:
            a, b = open(f'{OUT}/abs/{iid}.img', 'rb').read(), open(f'{OUT}/aud/{asin}.img', 'rb').read()
        except OSError: continue
        row = {'key': key, 'iid': iid, 'asin': asin, 'title': g.get('title'), 'abs_bytes': len(a), 'aud_bytes': len(b)}
        if not a: row['dist'] = 'no_abs_cover'
        elif not b: row['dist'] = 'no_audible_cover'
        else:
            try:
                row['dist'] = bin(dhash(a) ^ dhash(b)).count('1'); row['abs_size'] = Image.open(io.BytesIO(a)).size
            except Exception as e: row['dist'] = f'error {type(e).__name__}'
        rows.append(row)
    json.dump(rows, open(f'{OUT}/report.json', 'w'), ensure_ascii=False)
    nums = sorted(r['dist'] for r in rows if isinstance(r['dist'], int))
    print('books', len(rows), '| no ABS cover', sum(r['dist'] == 'no_abs_cover' for r in rows), '| no Audible cover', sum(r['dist'] == 'no_audible_cover' for r in rows), '| compared', len(nums))
    for t in (0, 5, 10, 15, 20, 30, 40, 60):
        print(f'  distance <= {t:2d} (of 256 bits): {sum(1 for d in nums if d <= t)}')


if __name__ == '__main__':
    {'fetch': fetch, 'report': report}[sys.argv[1]]()
