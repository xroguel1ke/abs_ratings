"""Synthetic books for a test batch: N random REAL Audible books that are NOT in the user's library, turned into ABS-style items with
typical defects (no ASIN, dead ASIN, messy file-name titles, marketing suffix, initials, accents, odd runtime ...).
The expected result for every item is known (the real ASIN), so the batch is a true test, not a lookup of things we already have.
Usage: python tools/synth_make.py --seed 7 --n 30 --out tests/synthetic/batch_7.json
Data is public Audible catalog data (no library content) -> may be committed. Needs network (Audible API, ~15 requests)."""
import argparse, json, os, random, re, sys, time, unicodedata
import requests

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
WORDS = ['dragon', 'detective', 'space', 'war', 'magic', 'history', 'spy', 'ghost', 'robot', 'pirate', 'kingdom', 'murder', 'empire', 'ocean', 'witch', 'knight', 'crime',
         'vampire', 'garden', 'secret', 'island', 'winter', 'doctor', 'soldier', 'legend', 'shadow', 'city', 'forest', 'mountain', 'storm', 'prince', 'thief', 'alien', 'desert',
         'school', 'wizard', 'road', 'night', 'river', 'train', 'castle', 'sword', 'moon', 'fire', 'queen', 'hunter', 'angel', 'heist', 'colony', 'rebel']
CASES = ['clean_ok', 'no_asin', 'dead_asin', 'messy_number_prefix', 'duplicate_title', 'marketing_suffix', 'initials_author', 'accent_free_author',
         'runtime_8pct_off', 'series_name_variant', 'messy_series_prefix', 'no_asin', 'year_prefix', 'iso_language', 'wrong_narrator']
# 2026-11-09: other categories too (non-fiction, kids, mystery/thriller, romance) - real libraries are not only fantasy
WORDS += ['biography', 'business', 'psychology', 'science', 'health', 'cooking', 'travel', 'philosophy', 'economics', 'memoir', 'politics',
          'bear', 'bunny', 'princess', 'dinosaur', 'friends', 'thriller', 'romance', 'wedding', 'cowboy', 'horror', 'zombie', 'poetry']
WORDS_DE = ['drache', 'mord', 'krieg', 'liebe', 'magie', 'geheimnis', 'königin', 'schatten', 'insel', 'winter', 'nacht', 'wald', 'stadt', 'schwert', 'mond', 'feuer', 'jäger', 'engel', 'reise', 'erbe', 'dunkel', 'welt',
            'geschichte', 'biografie', 'psychologie', 'kochen', 'kinder', 'pferd', 'hexe', 'krimi', 'kommissar', 'familie', 'gesundheit', 'wirtschaft']
CASES_DE = ['clean_ok', 'no_asin', 'dead_asin', 'messy_number_prefix', 'duplicate_title', 'accent_free_author', 'no_asin', 'runtime_8pct_off', 'dead_asin', 'no_asin',
            'year_prefix', 'iso_language', 'wrong_narrator']
# 2026-11-09 cases: 'YYYY - Title' file-name title; ISO language code ('eng'/'deu', correct ASIN: no ASIN change); ABS narrator of
# another recording (came with a dead ASIN) while the book folder names the real one. Every book gets an 'Author/Series/N - Title - Narrator' folder.
OTHER_NARR = ['Michael Kramer', 'Kate Reading', 'Simon Vance', 'Ray Porter', 'David Nathan', 'Andreas Fröhlich', 'Julia Nachtmann', 'Steven Pacey']
GROUPS = 'product_attrs,contributors,series,media'
UA = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36', 'Accept': 'application/json'}

def norm(s): return re.sub(r'[^a-z0-9]+', ' ', unicodedata.normalize('NFKD', s or '').encode('ascii', 'ignore').decode().lower()).strip()
def strip_accents(s): return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--seed', type=int, required=True); ap.add_argument('--n', type=int, default=30); ap.add_argument('--n-de', type=int, default=0); ap.add_argument('--out', required=True)
    a = ap.parse_args(); rng = random.Random(a.seed)
    snap = json.load(open(os.path.join(ROOT, 'tests', 'private', 'abs_snapshot.json'), encoding='utf-8'))
    lib = next(iter(snap['lists'])); have_asin, have_title = set(), set()
    for d in snap['items'].values():
        md = d['media']['metadata']; have_asin.add(md.get('asin')); have_title.add(norm(md.get('title')))
    def collect(host, words, lang):
      out = {}
      for w in rng.sample(words, 12):
        time.sleep(0.7)
        try:
            r = requests.get(f'https://{host}/1.0/catalog/products', params={'keywords': w, 'num_results': 50, 'page': rng.randint(0, 2), 'response_groups': GROUPS}, headers=UA, timeout=20)
            prods = r.json().get('products') or [] if r.status_code == 200 else []
        except (requests.RequestException, ValueError): prods = []
        for p in prods:
            t, au, na = p.get('title'), [x['name'] for x in p.get('authors') or []], [x['name'] for x in p.get('narrators') or []]
            if not (p.get('asin') and t and au and na and (p.get('runtime_length_min') or 0) >= 120 and p.get('language') == lang): continue
            if p['asin'] in have_asin or norm(t) in have_title or re.search(r'(?i)box set|books? \d+\s*[-–]\s*\d+|collection|omnibus|bible', t): continue
            out[p['asin']] = p
      return out
    cands = collect('api.audible.com', WORDS, 'english')
    cands_de = collect('api.audible.de', WORDS_DE, 'german') if a.n_de else {}
    picks = [(x, 'en') for x in rng.sample(sorted(cands), min(a.n, len(cands)))] + [(x, 'de') for x in rng.sample(sorted(cands_de), min(a.n_de, len(cands_de)))]
    items, lst, expected = {}, [], {}
    for i, (asin, lg) in enumerate(picks):
        p = (cands if lg == 'en' else cands_de)[asin]; case = (CASES if lg == 'en' else CASES_DE)[(i if lg == 'en' else i - a.n) % len(CASES if lg == 'en' else CASES_DE)]
        title, sub = p['title'], p.get('subtitle'); authors = [x['name'] for x in p['authors']][:2]; narr = [x['name'] for x in p['narrators']]
        ser = [{'name': s['title'], 'sequence': s.get('sequence')} for s in p.get('series') or [] if s.get('title')][:1]
        dur = p['runtime_length_min'] * 60; abs_asin = asin; exp = None
        if case in ('no_asin',): abs_asin, exp = None, asin
        elif case == 'dead_asin': abs_asin, exp = 'B0' + ''.join(rng.choice('ABCDEFGHJKLMNPQRSTUVWXYZ0123456789') for _ in range(8)), asin
        elif case == 'messy_number_prefix': title, abs_asin, exp = f"0{rng.randint(1, 9)} {title}", None, asin
        elif case == 'duplicate_title': title, sub, abs_asin, exp = f"{title}: {title}", sub or 'A Novel', None, asin
        elif case == 'marketing_suffix': title, abs_asin, exp = f"{title}: A Gripping LitRPG Adventure", None, asin
        elif case == 'initials_author':
            authors = [re.sub(r'^(\w)\w*\s+', r'\1. ', x, count=1) for x in authors]; abs_asin, exp = None, asin
        elif case == 'accent_free_author': authors = [strip_accents(x) for x in authors]; abs_asin, exp = None, asin
        elif case == 'runtime_8pct_off': dur = int(dur * 1.08)  # correct ASIN, other cut -> no ASIN change expected
        elif case == 'series_name_variant' and ser: ser = [{'name': ser[0]['name'] + (' Series' if not ser[0]['name'].lower().endswith('series') else ''), 'sequence': ser[0]['sequence']}]; abs_asin, exp = None, asin
        elif case == 'messy_series_prefix' and ser and ser[0].get('sequence'): title, abs_asin, exp = f"{ser[0]['name']} 0{ser[0]['sequence']} {title}", None, asin
        elif case == 'year_prefix': title, abs_asin, exp = f"{(p.get('issue_date') or '2020')[:4]} - {title}", None, asin
        elif case == 'iso_language': pass  # language set below; correct ASIN stays
        elif case == 'wrong_narrator': abs_asin, exp = 'B0' + ''.join(rng.choice('ABCDEFGHJKLMNPQRSTUVWXYZ0123456789') for _ in range(8)), asin
        elif case in ('series_name_variant', 'messy_series_prefix'): case, abs_asin, exp = 'no_asin', None, asin
        real_narr = list(narr)
        if case == 'wrong_narrator': narr = [rng.choice([x for x in OTHER_NARR if x not in narr])]
        sl = lambda x: str(x).replace('/', '-')
        layout = rng.choice(['series', 'series', 'flat', 'title'])  # Author/Series/N - Title - Narrator | Author/Title | Title
        folder = (f"{sl(authors[0])}/{sl(ser[0]['name'] if ser else p['title'])}/{(ser[0].get('sequence') or '1') if ser else '1'} - {sl(p['title'])} - {' + '.join(real_narr[:2])}"
                  if layout == 'series' or case == 'wrong_narrator' else f"{sl(authors[0])}/{sl(p['title'])}" if layout == 'flat' else sl(p['title']))
        iid = f"syn-{a.seed}-{lg}{i:02d}"
        md = {'title': title, 'subtitle': sub, 'authors': [{'id': f'a{j}', 'name': x} for j, x in enumerate(authors)], 'narrators': narr, 'series': [{'id': 's0', 'name': s['name'], 'sequence': s['sequence']} for s in ser],
              'genres': [], 'publishedYear': (p.get('issue_date') or '')[:4] or None, 'publisher': p.get('publisher_name'), 'description': '', 'asin': abs_asin, 'isbn': None, 'language': ('eng' if lg == 'en' else 'deu') if case == 'iso_language' else ('English' if lg == 'en' else 'German'), 'tags': []}
        media = {'metadata': md, 'duration': dur, 'tags': [], 'coverPath': None, 'size': dur * 8000}
        items[iid] = {'id': iid, 'libraryId': lib, 'mediaType': 'book', 'media': media, 'tags': [], 'relPath': folder}
        lst.append({'id': iid, 'libraryId': lib, 'mediaType': 'book', 'media': dict(media), 'relPath': folder})
        expected[f'{lib}_{iid}'] = {'asin': exp, 'status': 'confirmed', 'title': p['title'], 'note': f'synthetic case {case}; true product {asin}', 'case': case}
    json.dump({'library': lib, 'items': items, 'list': lst, 'expected': expected}, open(a.out, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    print(f'{len(items)} synthetic books -> {a.out} (candidates seen: {len(cands)}); cases: ' + ', '.join(f'{c}={sum(1 for e in expected.values() if e["case"] == c)}' for c in dict.fromkeys(CASES)))

if __name__ == '__main__': main()
