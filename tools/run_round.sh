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
[ -f "$DEV/tests/private/audible_series.json" ] && cp -n "$DEV/tests/private/audible_series.json" "$DATA/" || true
ONLY=$(paste -sd, "$KEYS")
if [ -n "$SYNTH" ]; then  # SYNTH=<seed>: add 30 synthetic books that are not in the library (generated once per seed, public data)
  SF="$DEV/tests/synthetic/batch_${SYNTH}.json"
  [ -f "$SF" ] || docker run --rm --name "abs_ratings_test_synth_${R}" -e PYTHONDONTWRITEBYTECODE=1 -v "$DEV":/src -w /src ghcr.io/xroguel1ke/abs_ratings:web-ui python -I tools/synth_make.py --seed "$SYNTH" --n "${SYNTH_EN:-60}" --n-de "${SYNTH_DE:-20}" --out "/src/tests/synthetic/batch_${SYNTH}.json"
  ONLY="$(printf '%s\n' $(echo "$ONLY,$(jq -r '.expected|keys|join(",")' "$SF")" | tr ',' ' ') | sort -u | paste -sd,)"
  SYNTH_ENV=(-e SYNTH_FILE=/src/tests/synthetic/batch_${SYNTH}.json)
fi
LIB=$(jq -r '.LIBRARY_IDS' "$SET")
HCT=$(jq -r '.HARDCOVER_TOKEN' "$SET")
HC_ON=true; [ "$HARDCOVER_OFF" = "1" ] && HC_ON=false
EXTRA=(-e RECORD_DIR=/src/tests/private/recordings -e RECORD_REUSE=${REUSE:-0})
[ "$MODE" = "replay" ] && EXTRA=(-e REPLAY_DIR=/src/tests/private/recordings)
timeout 14400 docker run --rm --name "abs_ratings_test_${R}" --network host -e PYTHONDONTWRITEBYTECODE=1 -e WORKERS="${WORKERS:-3}" \
  -v "$DEV":/src -v "$DATA":/data \
  -e FAKE_PORT="${FAKE_PORT:-18999}" -e DATA_DIR=/data -e ONLY_ITEMS="$ONLY" -e LIBRARY_IDS="$LIB" -e HARDCOVER_TOKEN="$HCT" -e HARDCOVER_ENABLED=$HC_ON \
  -e RECORD_NO_LIVE="${RECORD_NO_LIVE:-}" -e GOODREADS_ENABLED=${GOODREADS:-false} -e GOODREADS_SAMPLE=${GR_SAMPLE:-1} -e GR_ID_TRIES=${GR_TRIES:-2} -e GOODREADS_INTERVAL=${GR_INTERVAL:-10} -e SLEEP_TIMER=0 -e AUDIBLE_API_INTERVAL=0.5 -e REFRESH_DAYS=0 -e BATCH_SIZE=2500 -e ABS_BACKUP=false \
  -e GR_IMPERSONATE=${GR_IMPERSONATE:-false} -e GOODREADS_PAGES_PER_RUN=${GR_PAGES:-3} ${SNAPSHOT:+-e SNAPSHOT=$SNAPSHOT} \
  "${EXTRA[@]}" "${SYNTH_ENV[@]}" ${IMG:-ghcr.io/xroguel1ke/abs_ratings:web-ui} sh /src/tools/round_entry.sh
