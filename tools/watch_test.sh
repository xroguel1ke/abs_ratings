#!/bin/bash
# Integration test of the change monitoring (user wish 2026-11-03) against the FAKE ABS, real-run mode, offline (recordings only):
#  run 1 writes a cleaned title + publisher; then ABS 'rescans' (title back to the old value) and the user sets an own publisher;
#  run 2 must write the title again (any change is checked again; only lock_ tags protect - user decision 2026-11-08) and must keep
#  the publisher (publisher is only filled when empty).
set -e
DEV=/mnt/user/appdata/abs_ratings_dev; T=/mnt/user/appdata/abs_scripts_test/watch_it
LIB=114068f9-142d-4524-b0ef-a349354b5f88; ID=8c6e8b09-009d-4b32-a346-6788161e7b2b
rm -rf "$T"; mkdir -p "$T/logs"
docker run --rm --cpuset-cpus=${ABS_CPUS:-0-9,12-31} --name abs_ratings_test_watch_it --network host -e PYTHONDONTWRITEBYTECODE=1 -v "$DEV":/src -v "$T":/data -w /src \
  -e DATA_DIR=/data -e LIBRARY_IDS="$LIB" -e RECORD_DIR=/src/tests/private/recordings -e RECORD_REUSE=1 \
  -e RECORD_NO_LIVE=audible,api.hardcover.app,www.goodreads.com,media-amazon -e GOODREADS_ENABLED=false -e HARDCOVER_ENABLED=false \
  -e ABS_BACKUP=false -e REFRESH_DAYS=90 -e BATCH_SIZE=50 -e WORKERS=1 -e SERIES_MERGE=false -e LIBRARY_REPORT=false -e FILL_COVERS=false -e UPDATE_COVERS=false \
  -e AUTHOR_NORMALIZE=false -e REMOVE_EMPTY_AUTHORS=false -e REMOVE_ROLE_CREDITS=false -e ID=$ID ghcr.io/xroguel1ke/abs_ratings:web-ui sh -c '
set -e
python -I - <<PY
import json
s = json.load(open("/src/tests/private/abs_snapshot.json"))
s["items"] = {k: v for k, v in s["items"].items() if k == "$ID"}
for lib in s["lists"].values(): lib["results"] = [x for x in lib["results"] if x["id"] == "$ID"]
json.dump(s, open("/data/mini.json", "w"))
PY
python -I tools/fake_abs.py --snapshot /data/mini.json --port 18992 --patch-log /data/patches.jsonl >/data/fake.log 2>&1 & sleep 3
export ABS_URL=http://127.0.0.1:18992 API_TOKEN=fake DRY_RUN=false
RUN_ID=w1 python -u abs_ratings.py > /data/run_w1.out 2>&1
sleep 2
python -I - <<PY
import requests
u = "http://127.0.0.1:18992/api/items/$ID"
md = requests.get(u).json()["media"]["metadata"]
print("after run 1:", md["title"], "|", md.get("publisher"))
requests.patch(u + "/media", json={"metadata": {"title": "01 Nightside the Long Sun", "publisher": "My Own Publisher"}})
PY
RUN_ID=w2 python -u abs_ratings.py > /data/run_w2.out 2>&1
python -I - <<PY
import json, requests
md = requests.get("http://127.0.0.1:18992/api/items/$ID").json()["media"]["metadata"]
ok = md["title"] == "Nightside the Long Sun" and md.get("publisher") == "My Own Publisher"
print("after run 2:", md["title"], "|", md.get("publisher"))
print("change monitoring:", "ok (title re-applied after the rescan, publisher kept: only filled when empty)" if ok else "FAILED")
raise SystemExit(0 if ok else 1)
PY
'
