#!/bin/bash
# Real WRITE + UNDO test against the FAKE ABS (never the real one): one throwaway container, recordings only (no live requests).
# Usage: tools/write_undo_test.sh <keys-file>   -> prints the writes the script sent, the undo requests, and PASS/FAIL
set -e
KEYS="$1"; R=wu_$(date +%s); DEV=/mnt/user/appdata/abs_ratings_dev; DATA=/mnt/user/appdata/abs_scripts_test/$R
mkdir -p "$DATA/logs"
LIB=$(jq -r '.LIBRARY_IDS' /mnt/user/appdata/audiobookshelf/abs_scripts/settings.json)
timeout 3600 docker run --rm --name "abs_ratings_test_$R" --network host -e PYTHONDONTWRITEBYTECODE=1 -v "$DEV":/src -v "$DATA":/data \
  -e DATA_DIR=/data -e ONLY_ITEMS="$(paste -sd, "$KEYS")" -e LIBRARY_IDS="$LIB" -e HARDCOVER_ENABLED=false -e GOODREADS_ENABLED=false \
  -e RECORD_DIR=/src/tests/private/recordings -e RECORD_REUSE=1 -e RECORD_NO_LIVE=audible,api.hardcover.app,www.goodreads.com,media-amazon \
  -e PORT=${FAKE_PORT:-18997} ghcr.io/xroguel1ke/abs_ratings:web-ui sh -c '
set -e; cd /src
if curl -fsS "http://127.0.0.1:$PORT/ping" >/dev/null 2>&1; then echo "port in use - abort"; exit 3; fi
python -I tools/fake_abs.py --snapshot tests/private/abs_snapshot.json --port $PORT --patch-log /data/patches.jsonl > /data/fake_abs.log 2>&1 &
for i in $(seq 1 15); do curl -fsS "http://127.0.0.1:$PORT/api/libraries" 2>/dev/null | grep -q libraries && break; sleep 1; done
export ABS_URL="http://127.0.0.1:$PORT" API_TOKEN=fake
DRY_RUN=false ABS_BACKUP=false python -u abs_ratings.py > /data/write.out 2>&1
RUN=$(python -I -c "import json; print([json.loads(l) for l in open(\"/data/changes.jsonl\") if l.strip()][-1][\"run\"])"); echo "run $RUN"
python -I abs_undo.py --run "$RUN" --apply > /data/undo.out 2>&1 || true
'
echo "== journal (changes.jsonl): $(wc -l < "$DATA/changes.jsonl") changes; fields: $(jq -r .field "$DATA/changes.jsonl" | sort | uniq -c | tr '\n' ' ')"
echo "== requests the fake ABS received: $(jq -r .kind "$DATA/patches.jsonl" | sort | uniq -c | tr '\n' ' ')"
echo "== cover backups: $(ls "$DATA/cover_backup" 2>/dev/null | wc -l)"
tail -3 "$DATA/undo.out"
W=$(jq -c 'select(.kind!="cover_delete")' "$DATA/patches.jsonl" | wc -l); C=$(wc -l < "$DATA/changes.jsonl")
grep -q "❌" "$DATA/undo.out" && echo "FAIL: undo errors" || echo "PASS? journal $C changes, fake ABS got $W write requests (writes + undo) - judge the numbers"
echo "data: $DATA"
