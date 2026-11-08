"""Damage simulation (user request 2026-11-08): the repair rules must fix typical metadata damage on ANY book and must leave
undamaged books alone. Takes books that have Audible recordings (a finished round), injects one damage per chosen book into a copy
of the snapshot and writes the expectations. tools/damage_test.sh runs baseline + damaged round and calls --check.

  python tools/damage_test.py --make  --snapshot S --keys K --out DIR [--seed N]
  python tools/damage_test.py --check --out DIR --base PLANNED_BASE.jsonl --dmg PLANNED_DMG.jsonl
"""
import argparse, copy, json, os, random, re, sys

FOREIGN = ['La Saga de {s}', 'Les Chroniques de {s}', 'Il Ciclo di {s}', 'La caduta di {s}', 'Die Chronik von {s}']  # translated names (German on English books)


def core_word(name):
    """'The Stormlight Archive' -> 'Stormlight': a translated series name keeps only the proper name, never 'The ... of the ...'."""
    w = [x for x in re.findall(r"[A-Za-z][\w']+", name) if x.lower() not in ('the', 'of', 'a', 'an', 'and', 'series', 'saga', 'chronicles', 'archive', 'trilogy', 'light', 'novel')]
    return w[0] if w else name


def series_str(ser):
    return ', '.join(f"{x['name']} #{x['sequence']}" if x.get('sequence') else x['name'] for x in ser)


def make(a):
    snap = json.load(open(a.snapshot, encoding='utf-8'))
    keys = [k.strip() for k in open(a.keys) if k.strip()]
    lib = keys[0].split('_')[0]
    listing = {x['id']: x for x in snap['lists'][lib]['results']}
    rnd = random.Random(a.seed)
    ids = [k.split('_', 1)[1] for k in keys if k.split('_', 1)[1] in snap['items']]
    rnd.shuffle(ids)
    exp, used = {}, set()
    eng = lambda md: str(md.get('language') or '').lower() in ('english', 'eng', 'en')

    def apply(iid, field, value):
        md = snap['items'][iid]['media']['metadata']; lm = listing[iid]['media']['metadata']
        if field == 'series':
            md['series'] = [dict(x, id=f"dmg{i}") for i, x in enumerate(value)]; lm['seriesName'] = series_str(value)
        elif field == 'authors':
            md['authors'] = [{'id': f"dmga{i}", 'name': n} for i, n in enumerate(value)]; lm['authorName'] = ', '.join(value)
        elif field == 'narrators':
            md['narrators'] = list(value); lm['narratorName'] = ', '.join(value)
        else:
            md[field] = value; lm[field] = value

    for iid in ids:
        if len(exp) >= a.n: break
        md = snap['items'][iid]['media']['metadata']
        ser = [{'name': x['name'], 'sequence': x.get('sequence')} for x in md.get('series') or []]
        auth = [x['name'] for x in md.get('authors') or []]; narr = list(md.get('narrators') or [])
        kinds = []
        if ser and eng(md): kinds += ['foreign_extra', 'foreign_only']
        if auth and narr and set(auth) != set(narr): kinds.append('swap')
        if len(auth) == 2: kinds.append('glue')
        tags = set(snap['items'][iid]['media'].get('tags') or [])
        if not kinds or not md.get('asin') or any(t.startswith('lock_') for t in tags): continue
        kind = rnd.choice(kinds)
        key = f"{lib}_{iid}"
        main = ser[0]['name'] if ser else ''
        if kind == 'foreign_extra':  # an extra foreign series next to the right one -> must go
            bad = rnd.choice(FOREIGN).format(s=core_word(main))
            apply(iid, 'series', ser + [{'name': bad, 'sequence': ser[0].get('sequence')}])
            exp[key] = {'kind': kind, 'bad': bad, 'want_series_name': main}
        elif kind == 'foreign_only':  # the only series is foreign -> must become an English one
            bad = rnd.choice(FOREIGN).format(s=core_word(main))
            apply(iid, 'series', [{'name': bad, 'sequence': ser[0].get('sequence')}] + ser[1:])
            exp[key] = {'kind': kind, 'bad': bad, 'want_series_name': main}
        elif kind == 'swap':
            apply(iid, 'authors', narr); apply(iid, 'narrators', auth)
            exp[key] = {'kind': kind, 'want_authors': auth}
        elif kind == 'glue':
            apply(iid, 'authors', [' '.join(auth)])
            exp[key] = {'kind': kind, 'want_authors': auth}
    os.makedirs(a.out, exist_ok=True)
    json.dump(snap, open(os.path.join(a.out, 'damaged_snapshot.json'), 'w', encoding='utf-8'), ensure_ascii=False)
    json.dump(exp, open(os.path.join(a.out, 'expect.json'), 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    print(f"damaged {len(exp)} of {len(ids)} books:", {k: sum(1 for e in exp.values() if e['kind'] == k) for k in ('foreign_extra', 'foreign_only', 'swap', 'glue')})


def load_planned(p):
    out = {}
    for l in open(p, encoding='utf-8'):
        e = json.loads(l)
        out.setdefault(e['key'], {})[e['field']] = e['new']
    return out


def check(a):
    exp = json.load(open(os.path.join(a.out, 'expect.json'), encoding='utf-8'))
    base, dmg = load_planned(a.base), load_planned(a.dmg)
    norm = lambda v: json.dumps(v, sort_keys=True, ensure_ascii=False)
    fixed, missed, harm = [], [], []
    res = json.load(open(a.base.split('/reports/')[0] + '/results.json', encoding='utf-8'))
    snap = json.load(open(a.snapshot, encoding='utf-8'))['items']
    orig = {f"{v['libraryId']}_{k}": v['media']['metadata'].get('series') or [] for k, v in snap.items()}
    for key, e in exp.items():
        if (res.get(key) or {}).get('status') not in ('ok', 'other_edition'): continue  # no Audible data: nothing to compare with (not a rule gap)
        ch = dmg.get(key, {})
        if e['kind'].startswith('foreign'):
            names = [x.get('name') for x in ch.get('series', [])] if 'series' in ch else None
            ok = names is not None and e['bad'] not in names
            (fixed if ok else missed).append((key, e['kind'], e['bad'], names))
        else:
            got = [x.get('name') if isinstance(x, dict) else x for x in ch.get('authors', [])] if 'authors' in ch else None
            ok = got is not None and bool(set(got) & set(e['want_authors']))  # Audible may add a co-author / drop a translator
            (fixed if ok else missed).append((key, e['kind'], e['want_authors'], got))
    # undamaged books: the damaged run must plan exactly what the baseline planned for them (identity, series, authors, title)
    for key in set(base) | set(dmg):
        if key in exp: continue
        for f in ('asin', 'title', 'series', 'authors', 'narrators'):
            b_, d_ = base.get(key, {}).get(f), dmg.get(key, {}).get(f)
            if f == 'series' and b_ is None and d_ is not None:  # an ADDED series is fine, a removed/renamed one is harm
                if {x['name'] for x in orig.get(key, [])} <= {x.get('name') for x in d_}: continue
            if norm(b_) != norm(d_):
                harm.append((key, f, base.get(key, {}).get(f), dmg.get(key, {}).get(f)))
    print(f"damage repaired: {len(fixed)}/{len(exp)} | not repaired: {len(missed)} | undamaged books changed differently: {len(harm)}")
    for m in missed: print('  MISSED', m)
    for h in harm: print('  HARM', h)
    sys.exit(1 if harm else 0)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--make', action='store_true'); ap.add_argument('--check', action='store_true')
    ap.add_argument('--snapshot'); ap.add_argument('--keys'); ap.add_argument('--out'); ap.add_argument('--seed', type=int, default=1)
    ap.add_argument('--n', type=int, default=60); ap.add_argument('--base'); ap.add_argument('--dmg')
    a = ap.parse_args()
    make(a) if a.make else check(a)
