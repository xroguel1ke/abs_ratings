#!/bin/bash
# Integration test of the per-source search state (user rule 2026-11-04) against the FAKE ABS, real-run mode, offline (recordings only):
#  run 1: a mini library (the given keys) is checked completely
#  run 2: same day -> nothing is due
#  'next day' (all source_state timestamps 21 h back) -> run 3: only the MISSING sources are asked, no Audible request at all
# Usage: tools/source_state_test.sh <keys-file>
set -e
KEYS="$1"; DEV=/mnt/user/appdata/abs_ratings_dev; T=/mnt/user/appdata/abs_scripts_test/sst_it
rm -rf "$T"; mkdir -p "$T/logs"; cp "$KEYS" "$T/keys.txt"
LIB=$(head -1 "$KEYS" | cut -d_ -f1)
docker run --rm --cpuset-cpus=${ABS_CPUS:-0-9,12-31} --name abs_ratings_test_sst_it --network host -e PYTHONDONTWRITEBYTECODE=1 -v "$DEV":/src -v "$T":/data -w /src \
  -e DATA_DIR=/data -e LIBRARY_IDS="$LIB" -e RECORD_DIR=/src/tests/private/recordings -e RECORD_REUSE=1 \
  -e RECORD_NO_LIVE=audible,api.hardcover.app,www.goodreads.com,media-amazon -e GOODREADS_ENABLED=true -e HARDCOVER_ENABLED=true \
  -e HARDCOVER_TOKEN=offline -e ABS_BACKUP=false -e REFRESH_DAYS=90 -e BATCH_SIZE=500 -e WORKERS=3 -e SERIES_MERGE=false -e LIBRARY_REPORT=false \
  -e AUTHOR_NORMALIZE=false -e REMOVE_EMPTY_AUTHORS=false -e REMOVE_ROLE_CREDITS=false ghcr.io/xroguel1ke/abs_ratings:web-ui sh -c '
set -e
python -I - <<PY
import json
keys = {l.strip().split("_", 1)[1] for l in open("/data/keys.txt") if l.strip()}
s = json.load(open("/src/tests/private/abs_snapshot.json"))
s["items"] = {k: v for k, v in s["items"].items() if k in keys}
for lib in s["lists"].values(): lib["results"] = [x for x in lib["results"] if x["id"] in keys]
json.dump(s, open("/data/mini_snapshot.json", "w"))
print("mini library:", len(s["items"]), "books")
PY
python -I tools/fake_abs.py --snapshot /data/mini_snapshot.json --port 18991 --patch-log /data/patches.jsonl >/data/fake.log 2>&1 & sleep 3
export ABS_URL=http://127.0.0.1:18991 API_TOKEN=fake DRY_RUN=false
run() { RUN_ID=$1 python -u abs_ratings.py > /data/run_$1.out 2>&1; }
run r1; run r2
python -I - <<PY
import json, datetime
p = "/data/source_state.json"; s = json.load(open(p))
for book in s.values():
    for src, e in book.items():
        if isinstance(e, dict) and e.get("ts"):
            t = datetime.datetime.fromisoformat(e["ts"]) - datetime.timedelta(hours=21); e["ts"] = t.isoformat(timespec="seconds")
json.dump(s, open(p, "w"))
from collections import Counter
print("state after run 1:", dict(Counter(src + ":" + str(e.get("st")) for b in s.values() for src, e in b.items() if isinstance(e, dict))))
PY
run r3
for r in r1 r2 r3; do
  echo "== $r: $(grep -o "Due: .*re-checks\?, [0-9]* only missing ratings" /data/run_$r.out | head -1)"
  echo "   Audible requests: $(grep -cE "Audible API: (found|ASIN not)|Searching Replacement" /data/run_$r.out) | Goodreads lookups: $(grep -c "Checking www.goodreads.com" /data/run_$r.out) | Hardcover lookups: $(grep -c "Checking hardcover.app" /data/run_$r.out) | rating-only passes: $(grep -c "Only missing ratings" /data/run_$r.out)"
done
'
# verdict (used by tools/regress.sh): run 2 asks nothing, run 3 makes no Audible request and only rating-only passes
r2=$(grep -cE "Audible API: (found|ASIN not)|Checking www.goodreads.com|Checking hardcover.app" "$T/run_r2.out" || true)
r3a=$(grep -cE "Audible API: (found|ASIN not)|Searching Replacement" "$T/run_r3.out" || true)
r3p=$(grep -c "Only missing ratings" "$T/run_r3.out" || true)
if [ "$r2" = 0 ] && [ "$r3a" = 0 ] && [ "$r3p" -gt 0 ]; then echo "source state: ok (run 2 idle, run 3: $r3p rating-only passes, 0 Audible requests)"; else echo "source state: FAILED (run2 requests $r2, run3 Audible $r3a, rating-only $r3p)"; exit 1; fi
