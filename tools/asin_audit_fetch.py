"""Read-only: load the Audible product behind the ASIN of every book in the snapshot (one request per book, 0.6 s apart; tries .com first,
then the other stores only for misses). Output: tests/private/asin_products.json {asin: compact product | null}. No ABS access, no writes to ABS."""
import json, os, sys, time
import requests

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'tests', 'private')
snap = json.load(open(os.path.join(ROOT, 'abs_snapshot.json'), encoding='utf-8'))
out_path = os.path.join(ROOT, 'asin_products.json')
out = json.load(open(out_path, encoding='utf-8')) if os.path.exists(out_path) else {}
asins = sorted({d['media']['metadata'].get('asin') for d in snap['items'].values() if d['media']['metadata'].get('asin')})
todo = [a for a in asins if a not in out]
print(f'{len(asins)} ASINs, {len(todo)} to load', flush=True)
UA = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36', 'Accept': 'application/json'}
GROUPS = 'product_attrs,contributors,series,media,rating'
for i, a in enumerate(todo, 1):
    res = None
    for m in ('com', 'co.uk', 'de', 'ca', 'com.au', 'fr', 'it', 'es'):
        time.sleep(0.6)
        try:
            r = requests.get(f'https://api.audible.{m}/1.0/catalog/products/{a}', params={'response_groups': GROUPS}, headers=UA, timeout=20)
        except requests.RequestException:
            continue
        if r.status_code == 429: time.sleep(60); continue
        if r.status_code != 200: continue
        p = r.json().get('product') or {}
        if not p.get('title'): continue
        res = {'store': m, 'title': p.get('title'), 'subtitle': p.get('subtitle'), 'authors': [x['name'] for x in p.get('authors') or []],
               'narrators': [x['name'] for x in p.get('narrators') or []], 'runtime_min': p.get('runtime_length_min'), 'language': p.get('language'),
               'series': [{'title': s.get('title'), 'seq': s.get('sequence'), 'asin': s.get('asin')} for s in p.get('series') or []],
               'ratings': (p.get('rating') or {}).get('num_reviews'), 'format': p.get('format_type')}
        break
    out[a] = res
    if i % 50 == 0:
        json.dump(out, open(out_path, 'w', encoding='utf-8'), ensure_ascii=False); print(i, '/', len(todo), flush=True)
json.dump(out, open(out_path, 'w', encoding='utf-8'), ensure_ascii=False)
print('done:', sum(1 for v in out.values() if v), 'found,', sum(1 for v in out.values() if not v), 'not found anywhere', flush=True)
