#!/bin/sh
# Runs INSIDE the throwaway container: fake ABS (snapshot) + the dev script in DRY_RUN, then exits.
# Aborts if the port is already answering (it must never be a real ABS).
set -e
PORT=${FAKE_PORT:-18999}
if curl -fsS "http://127.0.0.1:$PORT/ping" >/dev/null 2>&1; then echo "port $PORT already in use - abort" >&2; exit 3; fi
cd /src
python -I tools/fake_abs.py --snapshot /src/tests/private/abs_snapshot.json --port $PORT --patch-log "$DATA_DIR/patches.jsonl" >"$DATA_DIR/fake_abs.log" 2>&1 &
ok=0
for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
  if curl -fsS "http://127.0.0.1:$PORT/api/libraries" 2>/dev/null | grep -q '"libraries"'; then ok=1; break; fi; sleep 1
done
[ "$ok" = 1 ] || { echo "fake ABS did not start" >&2; exit 4; }
export ABS_URL="http://127.0.0.1:$PORT" API_TOKEN=fake DRY_RUN=true
exec python -u abs_ratings.py
