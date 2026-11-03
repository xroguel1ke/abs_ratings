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
    if new == g.get('asin'): ok += 1
    else: bad.append((g.get('title') or key, g.get('asin'), new, g.get('note') or g.get('status')))
print(f"golden: {ok}/{len(golden)} ok, {len(bad)} deviations")
for t, exp, got, why in bad: print(f"  DEVIATION {t[:50]!r}: expected asin {exp}, got {got}  ({why})")

risky = collections.Counter()
for key, fields in by_key.items():
    for f in ('asin', 'title', 'series', 'language'):
        if f in fields and key not in golden:
            risky[f] += 1
            e = fields[f][-1]
            print(f"  ⚠️  not in golden: {f} {str(e['title'])[:45]!r}: {str(e['old'])[:40]} -> {str(e['new'])[:40]}")
print("risky changes outside golden:", dict(risky) or "none")
sys.exit(1 if bad else 0)
