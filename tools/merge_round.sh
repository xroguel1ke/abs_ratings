#!/bin/bash
# Synthetic series/author merge batch: tools/merge_round.sh <seed>   (fake ABS with its own library, DRY_RUN, no API requests)
set -e
N="$1"; DEV=/mnt/user/appdata/abs_ratings_dev; D=/mnt/user/appdata/abs_scripts_test/merge_$N; B=tests/synthetic/merge_$N.json
rm -rf "$D"; mkdir -p "$D"
docker run --rm --cpuset-cpus=${ABS_CPUS:-0-9,12-31} --name "abs_ratings_test_merge_$N" --network host -e PYTHONDONTWRITEBYTECODE=1 -v "$DEV":/src -v "$D":/data -w /src \
  ghcr.io/xroguel1ke/abs_ratings:web-ui sh -c "
set -e
[ -f $B ] || python -I tools/merge_synth.py make $N $B
python -I -c \"import json; json.dump(json.load(open('$B'))['snapshot'], open('/data/snap.json', 'w'))\"
PORT=\$((19300 + $N % 500))
python -I tools/fake_abs.py --snapshot /data/snap.json --port \$PORT --patch-log /data/patches.jsonl > /data/fake.log 2>&1 &
for i in \$(seq 1 15); do curl -fsS http://127.0.0.1:\$PORT/api/libraries 2>/dev/null | grep -q libraries && break; sleep 1; done
ABS_URL=http://127.0.0.1:\$PORT API_TOKEN=fake DATA_DIR=/data LIBRARY_IDS=SYNLIB SERIES_ONLY=true MISSING_SERIES=false SERIES_MERGE=true AUTHOR_NORMALIZE=true REMOVE_ROLE_CREDITS=true REMOVE_EMPTY_AUTHORS=true LIBRARY_REPORT=true \
  DRY_RUN=true HARDCOVER_ENABLED=false GOODREADS_ENABLED=false python -u abs_ratings.py > /data/run.out 2>&1
grep -E 'Series merges:|Errors:' /data/run.out | sed 's/^.* - INFO - //'
python -I tools/merge_synth.py eval $B /data"
