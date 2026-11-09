"""Missing-volumes test with REAL Audible series lists (public catalog data, recorded once in series_members.json).

  python tools/missing_test.py make <seed> <series_members.json> tests/synthetic/missing_<seed>.json
  python tools/missing_test.py eval tests/synthetic/missing_<seed>.json

make: picks real series (>= 5 plain numbered volumes) and builds a synthetic library per series - every volume present except 1-2
removed ones, with traps that must NOT count as missing: another edition of a volume (other ASIN, same number), a volume without
number but with its title, a volume under a longer library title ('Title: A Novel'), an omnibus range ('1-3') replacing its volumes.
eval: missing_series.compare must report exactly the removed numbers."""
import json, random, re, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
import missing_series as MS


def plain(members):
    out = {}
    for m in members:
        n = MS.num_of(m.get('num'))
        if n is None or n != int(n) or MS.BOX_RE.search(m.get('title') or '') or MS.EDITION_RE.search(m.get('title') or ''): continue
        if (m.get('release') or '')[:4] >= '2026': continue  # released volumes only (no upcoming ones in the expectation)
        out.setdefault(int(n), m)
    return out


def make(seed, cache_file, out_file):
    rng = random.Random(seed)
    cache = json.load(open(cache_file, encoding='utf-8'))
    series = [(k, v['members']) for k, v in sorted(cache.items()) if k.startswith('v2|') and len(plain(v.get('members') or [])) >= 5]
    rng.shuffle(series)
    cases = []
    for k, members in series[:25]:
        vols = plain(members)
        titles = {}
        for m in members:
            if MS.num_of(m.get('num')) is not None: titles.setdefault(m['num'], set()).add(MS.norm_title(m.get('title')))
        if any(len(v) > 1 for v in titles.values()): continue  # mixed formats / editions under one number (light novel + manga): no clean truth
        nums = sorted(vols)
        if nums != list(range(nums[0], nums[0] + len(nums))): continue  # gapless on Audible
        removed = rng.sample(nums[1:], rng.choice([1, 2]))
        lib, trap = [], rng.choice(['other_edition', 'no_number', 'long_title', 'omnibus'])
        for n in nums:
            if n in removed: continue
            m = vols[n]
            lib.append({'num': str(n), 'title': m['title'], 'asin': m['asin']})
        if trap == 'other_edition': lib[0] = dict(lib[0], asin='B0OTHEREDN')
        elif trap == 'no_number': lib[-1] = dict(lib[-1], num=None, asin=None)
        elif trap == 'long_title': lib[0] = dict(lib[0], title=lib[0]['title'] + ': A Novel', asin=None)
        elif trap == 'omnibus':
            first3 = [n for n in nums[:3] if n not in removed]
            if len(first3) == 3:
                lib = [b for b in lib if int(b['num']) not in first3] + [{'num': f'{first3[0]}-{first3[2]}', 'title': 'Books 1-3 Omnibus', 'asin': None}]
        cases.append({'series': k, 'members': members, 'library': lib, 'expect': sorted(str(n) for n in removed), 'trap': trap})
    json.dump({'seed': seed, 'cases': cases}, open(out_file, 'w', encoding='utf-8'), ensure_ascii=False)
    print(f"{len(cases)} series -> {out_file}")


def evaluate(f):
    d = json.load(open(f, encoding='utf-8')); bad = 0
    for c in d['cases']:
        got = sorted(m['num'] for m in MS.compare(c['library'], c['members'], today='2026-01-01') if not m['upcoming'])
        if got != c['expect']: bad += 1; print(f"  WRONG {c['series']} trap={c['trap']}: expected {c['expect']}, got {got}")
    print(f"missing volumes: {len(d['cases']) - bad}/{len(d['cases'])} ok")
    return 1 if bad else 0


if __name__ == '__main__':
    if sys.argv[1] == 'make': make(int(sys.argv[2]), sys.argv[3], sys.argv[4])
    else: sys.exit(evaluate(sys.argv[2]))
