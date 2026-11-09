"""Compare a planned_changes_<run>.jsonl with the golden list and flag suspicious changes.
Usage: python tools/evaluate.py <planned_changes.jsonl> [golden.json] [keys.txt]
keys.txt (optional): only golden books listed there (one libid_itemid per line) are judged = the books of THIS run
golden.json: {"books": {key: {"asin": "B0.."|null, "status": "confirmed"|"review", "title":..., "note":...}}}
 - asin null  = no automatic ASIN change expected; status review = must NOT be written automatically (review list).
Exit code 1 if any golden book deviates (wrong or missing result)."""
import json, sys, collections

planned = [json.loads(l) for l in open(sys.argv[1], encoding='utf-8') if l.strip()]
golden = json.load(open(sys.argv[2] if len(sys.argv) > 2 else "tests/private/golden.json", encoding="utf-8"))["books"]
if len(sys.argv) > 3:
    run_keys = {l.strip() for l in open(sys.argv[3], encoding="utf-8") if l.strip()}
    golden = {k: v for k, v in golden.items() if k in run_keys}
by_key = collections.defaultdict(lambda: collections.defaultdict(list))
for e in planned: by_key[e['key']][e['field']].append(e)

bad, ok = [], 0
for key, g in golden.items():
    got = by_key.get(key, {}).get('asin')
    new = got[-1]['new'] if got else None
    if not (new == g.get('asin') or (new and new in (g.get('alt') or []))):  # 'alt' = same product, ASIN of another store
        bad.append((g.get('title') or key, f"asin {g.get('asin')}", new, g.get('note') or g.get('status'))); continue
    # 2026-11-09: the other repairs of a synthetic case (expect_* keys; older batches have none)
    fl = by_key.get(key, {})
    last = lambda f: (fl[f][-1]['new'] if fl.get(f) else None)
    nt = lambda t: ''.join(ch for ch in __import__('re').sub(r'\([^)]*\)', '', str(t or '')).lower() if ch.isalnum())  # '(Tokyo Empire)' = series, moved to the series field
    nn = lambda xs: sorted(nt(x.get('name') if isinstance(x, dict) else x) for x in (xs or []))
    if g.get('expect_title') and nt(last('title')) != nt(g['expect_title']):
        bad.append((g.get('title') or key, f"title {g['expect_title']!r}", last('title'), g.get('note'))); continue
    if g.get('expect_lang') and last('language') != g['expect_lang']:
        bad.append((g.get('title') or key, f"language {g['expect_lang']}", last('language'), g.get('note'))); continue
    if g.get('expect_narr') and nn(last('narrators')) != nn(g['expect_narr']):
        bad.append((g.get('title') or key, f"narrators {g['expect_narr']}", last('narrators'), g.get('note'))); continue
    ok += 1
print(f"golden: {ok}/{len(golden)} ok, {len(bad)} deviations")
for t, exp, got, why in bad: print(f"  DEVIATION {t[:50]!r}: expected {exp}, got {str(got)[:80]}  ({why})")

risky = collections.Counter()
for key, fields in by_key.items():
    for f in ('asin', 'title', 'series', 'language'):
        if f in fields and key not in golden:
            risky[f] += 1
            e = fields[f][-1]
            print(f"  ⚠️  not in golden: {f} {str(e['title'])[:45]!r}: {str(e['old'])[:40]} -> {str(e['new'])[:40]}")
print("risky changes outside golden:", dict(risky) or "none")
sys.exit(1 if bad else 0)
