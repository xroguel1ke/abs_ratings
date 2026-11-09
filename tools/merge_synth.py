"""Synthetic series-merge / author-normalisation batches (books that are NOT in the user's library).

  python tools/merge_synth.py make <seed> <out.json>         -> fake-ABS snapshot (own library SYNLIB) + expected outcome per case
  python tools/merge_synth.py eval <batch.json> <data dir>   -> compares planned merges + merge_review.json with the expectation

Source: real Audible products of the synthetic Audible batches (tests/synthetic/batch_*.json): real series, authors, numbers.
Cases (one series or author group per case, books split into two name variants):
  must merge (series):  article ('The X' / 'X'), suffix ('X Series', 'X Saga'), format label ('X (Light Novel)'), case, punctuation
  must merge (author):  initials spacing ('J.R.R.' / 'J. R. R.'), accents ('Kurmaić' / 'Kurmaic')
  never merge:          contained name ('X' / 'X: Origins' with its own numbers), same name but different author, overlapping
                        numbers, other edition ('X (Full Cast)'), lock_series, different persons with the same surname
A case is WRONG if a never-merge pair is merged (or a must-merge pair is merged into a third name); MISSED if a must-merge pair is
neither merged nor on the merge review (a review entry for a must-merge case counts as missed too, it should be automatic)."""
import glob, json, random, re, sys, unicodedata

LIB = 'SYNLIB'


def corpus():
    """Real products grouped by (series, first author): {key: [book, ...]} with distinct integer numbers."""
    groups, seen = {}, set()
    for f in sorted(glob.glob('tests/synthetic/batch_*.json')):
        d = json.load(open(f, encoding='utf-8'))
        for it in (d.get('items') or {}).values():
            md = it['media']['metadata']
            if not md.get('series') or not md.get('authors') or not md.get('asin') or md['asin'] in seen: continue
            s, a = md['series'][0].get('name'), md['authors'][0].get('name')
            seq = str(md['series'][0].get('sequence') or '')
            if not s or not a or not re.fullmatch(r'\d{1,3}', seq): continue
            seen.add(md['asin'])
            groups.setdefault((s, a), []).append({'title': md['title'], 'asin': md['asin'], 'seq': seq, 'author': a,
                                                  'narrators': md.get('narrators') or [], 'duration': it['media'].get('duration'),
                                                  'language': md.get('language') or 'English'})
    try:  # real Audible series fetched once by tools/merge_corpus.py (not in the user's library)
        for s, books in json.load(open('tests/synthetic/merge_corpus.json', encoding='utf-8')).items():
            if books and not any(b['asin'] in seen for b in books): groups.setdefault((s, books[0]['author']), []).extend(books); seen.update(b['asin'] for b in books)
    except (OSError, ValueError): pass
    out = {}
    for k, books in groups.items():
        nums = [b['seq'] for b in books]
        if len(books) >= 2 and len(set(nums)) == len(nums): out[k] = sorted(books, key=lambda b: int(b['seq']))
    return out


def fold(s): return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))


def item(n, b, series, author, tags=()):
    iid = f'syn-m-{n:04d}'
    md = {'title': b['title'], 'subtitle': None, 'authors': [{'id': f'au-{author}', 'name': author}], 'narrators': b['narrators'],
          'series': [{'id': f'se-{series}', 'name': series, 'sequence': b['seq']}] if series else [], 'genres': [], 'publishedYear': None,
          'publisher': None, 'description': '', 'isbn': None, 'asin': b['asin'], 'language': b['language'], 'explicit': False, 'abridged': False}
    return {'id': iid, 'libraryId': LIB, 'mediaType': 'book', 'tags': list(tags),
            'media': {'metadata': md, 'duration': b['duration'], 'coverPath': None, 'tags': list(tags)}}


def make(seed, out):
    rng = random.Random(seed)
    groups = corpus()
    keys = sorted(groups); rng.shuffle(keys)
    by_author = {}
    for (s, a) in keys: by_author.setdefault(a, []).append((s, a))
    items, cases, n = [], [], [0]

    def add(b, series, author, tags=()):
        n[0] += 1; items.append(item(n[0], b, series, author, tags))

    def split(books): h = max(1, len(books) // 2); return books[:h], books[h:]

    used = set()
    def take(pred=lambda k: True, min_books=2):
        for k in keys:
            if k not in used and len(groups[k]) >= min_books and pred(k):
                used.add(k); return k
        return None

    variants = [
        ('article', lambda s: s[4:] if s.lower().startswith('the ') else 'The ' + s),
        ('suffix_series', lambda s: s + ' Series'),
        ('suffix_saga', lambda s: s + ' Saga'),
        ('light_novel', lambda s: s + ' (Light Novel)'),
        ('case', lambda s: s.lower() if s.lower() != s else s.upper()),
        ('punct', lambda s: re.sub(r'[:,!?.]', '', s) if re.search(r'[:,!?.]', s) else s + '.'),
    ]
    contiguous = lambda k: (lambda n: n[-1] - n[0] + 1 == len(n))(sorted(int(b['seq']) for b in groups[k]))
    plain = lambda k: not re.search(r'(?i)\b(series|saga|trilogy|light novel|cycle|chronicles?)\b', k[0])
    for name, fn in variants:                       # must merge
        for _ in range(3):
            k = take(plain)
            if not k: break
            s, a = k; v = fn(s)
            if v == s: continue
            h1, h2 = split(groups[k])
            for b in h1: add(b, s, a)
            for b in h2: add(b, v, a)
            cases.append({'case': name, 'kind': 'series', 'expect': 'merge', 'names': [s, v]})
    for _ in range(3):                              # must merge: loosely similar name, numbers interleave (user rule)
        k = take(lambda k: plain(k) and contiguous(k), 4)
        if not k: break
        s, a = k; v = s + ' Chronicles'
        for i, b in enumerate(groups[k]): add(b, s if (i // max(1, len(groups[k]) // 4)) % 2 == 0 else v, a)
        cases.append({'case': 'interleaved_loose_name', 'kind': 'series', 'expect': 'merge', 'names': [s, v]})
    for _ in range(3):                              # never (review): loosely similar name, numbers only continue
        k = take(lambda k: plain(k) and contiguous(k), 4)
        if not k: break
        s, a = k; v = s + ' Chronicles'; h1, h2 = split(groups[k])
        for b in h1: add(b, s, a)
        for b in h2: add(b, v, a)
        cases.append({'case': 'loose_name_no_interleave', 'kind': 'series', 'expect': 'never', 'names': [s, v]})
    for _ in range(3):                              # never: contained name with its own numbering
        k1 = take(plain); k2 = take(lambda k: k[1] == (k1 or ('', ''))[1] and plain(k)) if k1 else None
        if not k1: break
        s, a = k1; other = s + ': Origins'
        for b in groups[k1]: add(b, s, a)
        for b in (groups[k2] if k2 else groups[k1][:1]): add(dict(b, asin=b['asin'] + 'X' if not k2 else b['asin']), other, a)
        cases.append({'case': 'contained_name', 'kind': 'series', 'expect': 'never', 'names': [s, other]})
    for _ in range(3):                              # never: same name, different author
        k1 = take(plain); k2 = take(lambda k: k[1] != (k1 or ('', ''))[1])
        if not k1 or not k2: break
        s, a = k1
        for b in groups[k1]: add(b, s, a)
        for b in groups[k2]: add(b, s + ' Series', k2[1])
        cases.append({'case': 'other_author', 'kind': 'series', 'expect': 'never', 'names': [s, s + ' Series']})
    for _ in range(3):                              # never: overlapping numbers
        k = take(plain, 4)
        if not k: break
        s, a = k; h1, h2 = split(groups[k])
        for b in h1: add(b, s, a)
        for i, b in enumerate(h2): add(dict(b, seq=h1[i % len(h1)]['seq']), s + ' Series', a)
        cases.append({'case': 'overlapping_numbers', 'kind': 'series', 'expect': 'never', 'names': [s, s + ' Series']})
    for _ in range(3):                              # never: other edition
        k = take(plain)
        if not k: break
        s, a = k; h1, h2 = split(groups[k])
        for b in h1: add(b, s, a)
        for b in h2: add(b, s + ' (Full Cast)', a)
        cases.append({'case': 'full_cast_edition', 'kind': 'series', 'expect': 'never', 'names': [s, s + ' (Full Cast)']})
    for _ in range(2):                              # never: lock_series
        k = take(plain)
        if not k: break
        s, a = k; h1, h2 = split(groups[k])
        for b in h1: add(b, s, a)
        for i, b in enumerate(h2): add(b, s + ' Series', a, tags=('lock_series',) if i == 0 else ())
        cases.append({'case': 'lock_series', 'kind': 'series', 'expect': 'never', 'names': [s, s + ' Series']})
    # authors
    ini = [k for k in keys if re.search(r'\b[A-Z]\.[A-Z]\.', k[1]) and k not in used]
    for k in ini[:3]:
        used.add(k); s, a = k; v = re.sub(r'\b([A-Z])\.(?=[A-Z]\.)', r'\1. ', a)
        h1, h2 = split(groups[k])
        for b in h1: add(b, s, a)
        for b in h2: add(b, s, v)
        cases.append({'case': 'author_initials', 'kind': 'author', 'expect': 'merge', 'names': [a, v]})
    acc = [k for k in keys if fold(k[1]) != k[1] and k not in used]
    for k in acc[:3]:
        used.add(k); s, a = k; v = fold(a)
        h1, h2 = split(groups[k])
        for b in h1: add(b, s, a)
        for b in h2: add(b, s, v)
        cases.append({'case': 'author_accent', 'kind': 'author', 'expect': 'merge', 'names': [a, v]})
    sur = {}
    for k in keys:
        if k in used or len(k[1].split()) < 2: continue
        sur.setdefault(k[1].split()[-1].lower(), {}).setdefault(k[1], k)
    pairs = [list(v.values())[:2] for v in sur.values() if len(v) >= 2][:3]
    for k1, k2 in pairs:                            # never: different persons, same surname
        used.update({k1, k2})
        for b in groups[k1]: add(b, k1[0], k1[1])
        for b in groups[k2]: add(b, k2[0], k2[1])
        cases.append({'case': 'different_person', 'kind': 'author', 'expect': 'never', 'names': [k1[1], k2[1]]})
    # only real authors: role credits (user rule 2026-11-04)
    role_cases = []
    for role in ('illustrator', 'translator', 'Übersetzer', 'editor', 'foreword'):
        k = take()
        if not k: break
        s, a = k; b = groups[k][0]; helper = f'Helper {role.title()} {len(role_cases)}'
        add(b, s, a); items[-1]['media']['metadata']['authors'].append({'id': 'au-h' + helper, 'name': f'{helper} - {role}'})
        role_cases.append({'case': 'role_suffix', 'item': items[-1]['id'], 'remove': f'{helper} - {role}'})
    k = take(lambda k: len(groups[k]) >= 2)
    if k:  # learned: the same illustrator WITHOUT suffix on another volume is removed too
        s, a = k; b1, b2 = groups[k][:2]; ill = 'Kurone Testillustrator'
        add(b1, s, a); items[-1]['media']['metadata']['authors'].append({'id': 'au-ill', 'name': ill + ' - illustrator'})
        role_cases.append({'case': 'role_suffix', 'item': items[-1]['id'], 'remove': ill + ' - illustrator'})
        add(b2, s, a); items[-1]['media']['metadata']['authors'].append({'id': 'au-ill2', 'name': ill})
        role_cases.append({'case': 'role_learned_plain', 'item': items[-1]['id'], 'remove': ill})
    k = take()
    if k:  # never: the only author of a book is a role credit (no real author would remain)
        s, a = k; b = groups[k][0]; add(b, s, 'Solo Person - translator')
        role_cases.append({'case': 'role_only_author', 'item': items[-1]['id'], 'remove': None})
    k1, k2 = take(), take()
    if k1 and k2:  # never: a real author (first author of a book) who illustrates elsewhere stays
        s, a = k1; add(groups[k1][0], s, 'Dual Talent')
        s2, a2 = k2; add(groups[k2][0], s2, a2); items[-1]['media']['metadata']['authors'].append({'id': 'au-dt', 'name': 'Dual Talent - illustrator'})
        role_cases.append({'case': 'role_suffix', 'item': items[-1]['id'], 'remove': 'Dual Talent - illustrator'})
        role_cases.append({'case': 'real_author_kept', 'item': items[-2]['id'], 'remove': None})
    # 2026-11-09 library-task cases: genre spellings, folder number in a numbered series, series language, wrong language
    extra = []
    k = take(min_books=3)
    if k:
        s, a = k
        for i, b in enumerate(groups[k][:3]):
            add(b, s, a); items[-1]['media']['metadata']['genres'] = ['Fantasy Romance', 'Adventure'] if i < 2 else ['fantasy romance', 'Adventure', 'FANTASY ROMANCE']
        extra.append({'case': 'genre_spelling', 'item': items[-1]['id'], 'field': 'genres', 'expect': ['Fantasy Romance', 'Adventure']})
    k = take(lambda k: plain(k), 4)
    if k:
        s, a = k
        for i, b in enumerate(groups[k]):
            add(b, s, a); items[-1]['relPath'] = f"{a}/{s}/{b['seq']} - {b['title']} - {(b['narrators'] or ['N'])[0]}"
            if i == 1: items[-1]['media']['metadata']['series'][0]['sequence'] = None; gap = (items[-1]['id'], b['seq'])
        extra.append({'case': 'folder_number_gap', 'item': gap[0], 'field': 'series', 'expect': gap[1]})
    k = take(lambda k: all((b.get('language') or 'English') == 'English' and b['narrators'] for b in groups[k]), 3)
    if k:
        s, a = k
        for i, b in enumerate(groups[k]):
            add(b, s, a)
            if i == 0: items[-1]['media']['metadata']['language'] = 'und'; und = items[-1]['id']
        extra.append({'case': 'language_consensus', 'item': und, 'field': 'language', 'expect': 'English'})
    k = take(lambda k: all((b.get('language') or 'English') == 'English' for b in groups[k]))
    if k:
        s, a = k
        b = groups[k][0]; add(dict(b, narrators=['Nobody Shared']), s, a); items[-1]['media']['metadata']['language'] = 'French'
        extra.append({'case': 'wrong_language_review', 'item': items[-1]['id'], 'field': 'review', 'expect': 'languages'})
    empty = [{'id': f'au-empty-{i}', 'name': f'Empty Leftover {i}'} for i in range(3)]
    snap = {'libraries': {'libraries': [{'id': LIB, 'name': 'Synthetic merge test', 'mediaType': 'book'}]},
            'lists': {LIB: {'results': items}}, 'items': {x['id']: x for x in items}}
    snap['empty_authors'] = empty
    json.dump({'seed': seed, 'snapshot': snap, 'cases': cases, 'role_cases': role_cases, 'empty_authors': [e['name'] for e in empty], 'extra_cases': extra},
              open(out, 'w', encoding='utf-8'), ensure_ascii=False)
    print(f"{len(items)} books, {len(cases)} cases -> {out}: " + ', '.join(f"{c}={sum(1 for x in cases if x['case'] == c)}" for c in dict.fromkeys(x['case'] for x in cases)))


def evaluate(batch, data):
    cases = json.load(open(batch, encoding='utf-8'))['cases']
    plan = []
    for f in glob.glob(f'{data}/reports/planned_changes_*.jsonl'):
        plan += [json.loads(l) for l in open(f, encoding='utf-8') if l.strip()]
    merged = set()
    for e in plan:
        m = re.match(r'(series|author)\|[^|]*\|(.*)->(.*)$', e.get('merge_id') or '')
        if m: merged.add((m.group(1), m.group(2), m.group(3)))
    try: rv = json.load(open(f'{data}/merge_review.json', encoding='utf-8'))
    except OSError: rv = {}
    review = {('series', x.get('series'), x.get('other')) for x in rv.get('series') or []} | {('author', x.get('series') or x.get('name'), x.get('other')) for x in rv.get('authors') or []}
    ok = wrong = missed = 0
    for c in cases:
        names = set(c['names'])
        hit = [m for m in merged if m[0] == c['kind'] and ({m[1], m[2]} & names)]
        in_rev = any(r[0] == c['kind'] and {r[1], r[2]} == names for r in review)
        if c['case'] == 'lock_series':
            # user rule 2026-11-08 (was: a lock on either side blocks): lock_series protects the BOOK - when all locked books sit in ONE
            # series, that series is the target and only unlocked books move. Good: no merge, or a merge INTO the locked name.
            if any(m[2] != c['names'][1] for m in hit): wrong += 1; print('  WRONG lock_series target', c['names'], '->', hit)
            else: ok += 1
            continue
        if c['expect'] == 'merge':
            good = any({m[1], m[2]} == names for m in hit)
            if c['case'] == 'author_accent' and good and not any(m[2] == c['names'][0] for m in hit):  # direction: the accented name must be the target
                wrong += 1; print('  WRONG direction', c['case'], c['names'], '->', hit); continue
            bad = [m for m in hit if {m[1], m[2]} != names]
            if bad: wrong += 1; print('  WRONG', c['case'], c['names'], '->', bad)
            elif good: ok += 1
            else: missed += 1; print('  MISSED', c['case'], c['names'], '(on review)' if in_rev else '')
        else:
            if hit: wrong += 1; print('  WRONG', c['case'], c['names'], '->', hit)
            else: ok += 1
    # only real authors + empty authors
    batch_d = json.load(open(batch, encoding='utf-8'))
    removed = {}
    for e in plan:
        if e.get('source') == 'role_credit_removed':
            removed[e['item']] = {a['name'] for a in e.get('old') or []} - {a['name'] for a in e.get('new') or []}
    for c in batch_d.get('role_cases') or []:
        got = removed.get(c['item'], set())
        exp = {c['remove']} if c['remove'] else set()
        if c['remove'] and c['remove'] not in got: missed += 1; print('  MISSED', c['case'], c['remove'])
        elif got - exp: wrong += 1; print('  WRONG', c['case'], 'removed', got - exp)
        else: ok += 1
    del_names = {e['old'] for e in plan if e.get('source') == 'empty_author_removed'}
    exp_del = set(batch_d.get('empty_authors') or [])
    if del_names - exp_del: wrong += 1; print('  WRONG deleted authors that have books:', del_names - exp_del)
    elif exp_del - del_names: missed += 1; print('  MISSED empty authors:', exp_del - del_names)
    elif exp_del: ok += 1
    for c in batch_d.get('extra_cases') or []:
        es = [e for e in plan if e.get('item') == c['item'] and e.get('field') == c['field']]
        new = es[-1]['new'] if es else None
        if c['field'] == 'series': good = bool(new) and any(str(x.get('sequence')) == c['expect'] for x in new)
        elif c['field'] == 'review': good = any(x.get('key', '').endswith(c['item']) for x in rv.get('languages') or [])
        else: good = new == c['expect']
        if good: ok += 1
        else: missed += 1; print('  MISSED', c['case'], c['item'], 'got', str(new)[:80])
    n_all = len(cases) + len(batch_d.get('role_cases') or []) + (1 if exp_del else 0) + len(batch_d.get('extra_cases') or [])
    print(f"merge batch: {ok}/{n_all} ok | wrong {wrong} | missed {missed}")
    return 0 if not wrong and not missed else 1


if __name__ == '__main__':
    if sys.argv[1] == 'make': make(int(sys.argv[2]), sys.argv[3])
    else: sys.exit(evaluate(sys.argv[2], sys.argv[3]))
