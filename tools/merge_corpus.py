"""One-time corpus for the synthetic merge batches: real Audible series (members, numbers, authors, narrators, runtime) that are NOT
in the user's library. Public Audible data only -> tests/synthetic/merge_corpus.json.
  python tools/merge_corpus.py [max_series]      (sequential requests, 0.5 s apart, api.audible.com)"""
import json, re, sys, time
import requests

sys.path.insert(0, '/src')
from abs_series import series_key

MAX = int(sys.argv[1]) if len(sys.argv) > 1 else 300
H = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36', 'Accept': 'application/json'}
known = json.load(open('tests/private/audible_series.json', encoding='utf-8'))
snap = json.load(open('tests/private/abs_snapshot.json', encoding='utf-8'))['items']
lib_series = {series_key(s.get('name') or '') for it in snap.values() for s in ((it.get('media') or {}).get('metadata') or {}).get('series') or []}


def get(url, **params):
    for attempt in (1, 2):  # one retry; a network error skips this series instead of ending the whole run
        time.sleep(0.5 * attempt)
        try:
            r = requests.get(url, params=params, headers=H, timeout=30)
            return r.json() if r.status_code == 200 else {}
        except (requests.RequestException, ValueError): continue
    return {}


out, done = {}, 0
for title, asins in sorted(known.items()):
    if done >= MAX: break
    if series_key(title) in lib_series or not asins: continue
    rel = (get(f'https://api.audible.com/1.0/catalog/products/{sorted(asins)[0]}', response_groups='relationships').get('product') or {}).get('relationships') or []
    kids = [(x['asin'], x.get('sequence')) for x in rel if x.get('relationship_to_product') == 'child' and x.get('asin')]
    kids = [(a, s) for a, s in kids if re.fullmatch(r'\d{1,3}', str(s or ''))][:12]
    if len(kids) < 2: continue
    prods = get('https://api.audible.com/1.0/catalog/products', asins=','.join(a for a, _ in kids),
                response_groups='product_attrs,contributors,product_desc').get('products') or []
    seq = dict(kids); books = []
    for p in prods:
        au = [a.get('name') for a in p.get('authors') or [] if a.get('name')]
        if not au or not p.get('title'): continue
        books.append({'title': p['title'], 'asin': p['asin'], 'seq': str(seq.get(p['asin'])), 'author': au[0],
                      'narrators': [n.get('name') for n in p.get('narrators') or [] if n.get('name')],
                      'duration': int(p.get('runtime_length_min') or 0) * 60, 'language': (p.get('language') or 'english').capitalize()})
    if len(books) >= 2 and len({b['author'] for b in books}) == 1:
        out[title] = sorted(books, key=lambda b: int(b['seq'])); done += 1
        if done % 25 == 0:
            print('series', done, flush=True); json.dump(out, open('tests/synthetic/merge_corpus.json', 'w', encoding='utf-8'), ensure_ascii=False)
json.dump(out, open('tests/synthetic/merge_corpus.json', 'w', encoding='utf-8'), ensure_ascii=False)
print('corpus:', len(out), 'series ->', 'tests/synthetic/merge_corpus.json')
