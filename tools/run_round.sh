#!/bin/bash
# Usage: tools/run_round.sh <round-name> <file-with-item-keys (libid_itemid per line)> [record|replay]
# One throwaway container (name abs_ratings_test_<round>, --rm, hard timeout), fake ABS inside, DRY_RUN only.
# Data goes to /mnt/user/appdata/abs_scripts_test/<round>; recordings to tests/private/recordings (git-ignored).
set -e
R="$1"; KEYS="$2"; MODE="${3:-record}"
DEV=/mnt/user/appdata/abs_ratings_dev
DATA=/mnt/user/appdata/abs_scripts_test/$R
SET=/mnt/user/appdata/audiobookshelf/abs_scripts/settings.json     # read only
mkdir -p "$DATA/logs" "$DEV/tests/private/recordings"
[ -f /mnt/user/appdata/abs_scripts_test/series_index.json ] && cp -n /mnt/user/appdata/abs_scripts_test/series_index.json "$DATA/" || true
ONLY=$(paste -sd, "$KEYS")
LIB=$(jq -r '.LIBRARY_IDS' "$SET")
HCT=$(jq -r '.HARDCOVER_TOKEN' "$SET")
HC_ON=true; [ "$HARDCOVER_OFF" = "1" ] && HC_ON=false
EXTRA=(-e RECORD_DIR=/src/tests/private/recordings -e RECORD_REUSE=${REUSE:-0})
[ "$MODE" = "replay" ] && EXTRA=(-e REPLAY_DIR=/src/tests/private/recordings)
timeout 14400 docker run --rm --name "abs_ratings_test_${R}" --network host -e PYTHONDONTWRITEBYTECODE=1 \
  -v "$DEV":/src -v "$DATA":/data \
  -e DATA_DIR=/data -e ONLY_ITEMS="$ONLY" -e LIBRARY_IDS="$LIB" -e HARDCOVER_TOKEN="$HCT" -e HARDCOVER_ENABLED=$HC_ON \
  -e GOODREADS_ENABLED=false -e SLEEP_TIMER=0 -e AUDIBLE_API_INTERVAL=0.5 -e REFRESH_DAYS=0 -e BATCH_SIZE=2500 -e ABS_BACKUP=false \
  "${EXTRA[@]}" ghcr.io/xroguel1ke/abs_ratings:web-ui sh /src/tools/round_entry.sh
