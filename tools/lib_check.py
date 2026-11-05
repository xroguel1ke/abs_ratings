"""Library-wide offline check of series merges and author fixes (pure functions, no network, no ABS).
Usage: python tools/lib_check.py [--accept]
Computes merges / not-merged (uncertain) / author fixes for the whole snapshot (tests/private/abs_snapshot.json) and compares them with the
hand-verified baseline tests/private/lib_baseline.json. ANY difference is printed; exit code 1 = a rule change altered a decision -> check by hand,
then re-run with --accept (only after every difference was judged correct)."""
import json, os, sys, logging
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
logging.disable(logging.CRITICAL)
import abs_series as S

root = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'tests', 'private')
# env overrides (check against a fresh snapshot of the real library): SNAPSHOT, LINKS, SERIES_NAMES, ROLES, DECISIONS, BASELINE; --show prints everything
snap = json.load(open(os.environ.get('SNAPSHOT') or os.path.join(root, 'abs_snapshot.json'), encoding='utf-8'))
links_file = os.environ.get('LINKS') or os.path.join(root, 'audible_authors.json')
links = {n: set(a) for n, a in json.load(open(links_file, encoding='utf-8')).items()} if os.path.exists(links_file) else None
recs = []
for lib, lst in snap['lists'].items():
    for it in lst['results']:
        d = snap['items'][it['id']]; md = d['media']['metadata']
        raw = (d.get('tags') or []) + (d['media'].get('tags') or []) + (md.get('tags') or [])
        recs.append({'id': d['id'], 'lib': lib, 'title': md.get('title') or '?', 'authors': [a['name'] for a in md.get('authors') or [] if a.get('name')],
                     'narrators': md.get('narrators') or [], 'asin': md.get('asin'), 'isbn': md.get('isbn'),
                     'series': [{'name': s['name'], 'sequence': s.get('sequence')} for s in md.get('series') or [] if s.get('name')],
                     'media_tags': list(d['media'].get('tags') or []), 'tags': S.expand_locks({t.strip() for t in raw if isinstance(t, str)}),
                     'description': '', 'language': md.get('language'), 'genres': md.get('genres') or [], 'cover': None, 'duration': d['media'].get('duration')})
sn_file = os.environ.get('SERIES_NAMES') or os.path.join(root, 'audible_series.json')
series_names = set(json.load(open(sn_file, encoding='utf-8'))) if os.path.exists(sn_file) else None
roles = json.load(open(os.environ['ROLES'], encoding='utf-8')) if os.environ.get('ROLES') and os.path.exists(os.environ['ROLES']) else ()
decisions = json.load(open(os.environ['DECISIONS'], encoding='utf-8')) if os.environ.get('DECISIONS') and os.path.exists(os.environ['DECISIONS']) else {}
merges, rejected = S.find_merges(recs, decisions, series_names)
fixes, _, a_review = S.find_author_fixes(recs, decisions, links, roles)
def msum(m):  # stable, readable: library, target series name, member names (+ book counts)
    nm = lambda s: f"{s.name} [{len(s.entries)} books]"
    return f"{m['lib'][:8]} | {nm(m['target'])} <- {'; '.join(sorted(nm(s) for s in m['members']))}"
cur = {'merges': sorted(msum(m) for m in merges),
       'uncertain': sorted(f"{r.get('series')} | {r.get('other')} | {r.get('reason')}" for r in rejected if str(r.get('reason', '')).startswith(S.UNCERTAIN)),
       'author_fixes': {k: v for k, v in sorted(fixes.items())},
       'author_review': sorted(f"{r['series']} | {r['other']}" for r in a_review)}
path = os.environ.get('BASELINE') or os.path.join(root, 'lib_baseline.json')
if '--show' in sys.argv:
    for k, v in cur.items():
        print(f'== {k}')
        for x in (v.items() if isinstance(v, dict) else v): print('  ', x if isinstance(x, str) else f'{x[0]} -> {x[1]}')
print(f"merges {len(cur['merges'])} | uncertain {len(cur['uncertain'])} | author fixes {len(cur['author_fixes'])} | author review {len(cur['author_review'])}")
if '--accept' in sys.argv or not os.path.exists(path):
    json.dump(cur, open(path, 'w', encoding='utf-8'), indent=1, ensure_ascii=False); print('baseline written:', path); sys.exit(0)
base = json.load(open(path, encoding='utf-8')); diff = 0
for k in cur:
    a, b = base.get(k), cur[k]
    if a == b: continue
    diff += 1; print(f'DIFFERENCE in {k}:')
    if isinstance(b, dict):
        for x in sorted(set(a) | set(b)):
            if a.get(x) != b.get(x): print(f'   {x!r}: baseline {a.get(x)!r} -> now {b.get(x)!r}')
    else:
        for x in sorted(set(b) - set(a)): print('   + ', x[:230])
        for x in sorted(set(a) - set(b)): print('   - ', x[:230])
print('no differences' if not diff else f'{diff} group(s) differ - verify by hand'); sys.exit(1 if diff else 0)
