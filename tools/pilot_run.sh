#!/bin/bash
# Pilot against the REAL ABS for a few books (user OK 2026-11-04: 30 books). Real settings (read-only), own data dir, Goodreads off.
# Usage: tools/pilot_run.sh <keys-file> backup|dry|real|diff|restore|restore-apply
#   backup         read-only: item JSON + cover of every book (tools/item_backup.py)
#   dry            DRY_RUN against the real ABS (nothing written)
#   real           DRY_RUN=false: writes into the real ABS (ABS backup first), journal in the pilot data dir
#   diff / restore / restore-apply   compare with / restore the backup
set -e
KEYS="$1"; MODE="$2"; DEV=/mnt/user/appdata/abs_ratings_dev; P=/mnt/user/appdata/abs_scripts_test/pilot
REAL=/mnt/user/appdata/audiobookshelf/abs_scripts
mkdir -p "$P/logs" "$P/backup"; cp -n "$REAL/series_index.json" "$P/" 2>/dev/null || true
cp "$KEYS" "$P/keys.txt"
IMG=ghcr.io/xroguel1ke/abs_ratings:web-ui
run() { docker run --rm --cpuset-cpus=${ABS_CPUS:-0-9,12-31} --name "abs_ratings_test_pilot_$MODE" --network host -e PYTHONDONTWRITEBYTECODE=1 -e CONFIG_DIR=/config \
  -v "$REAL":/config:ro -v "$DEV":/src:ro -v "$P":/data -w /src $IMG "$@"; }
case "$MODE" in
  backup)  run python -I tools/item_backup.py backup /data/keys.txt /data/backup ;;
  diff)    run python -I tools/item_backup.py diff /data/keys.txt /data/backup ;;
  restore) run python -I tools/item_backup.py restore /data/keys.txt /data/backup ;;
  restore-apply) run python -I tools/item_backup.py restore /data/keys.txt /data/backup --apply ;;
  series-dry|series-real)  # series merge + author fix only (whole library; merge_decisions.json in the pilot dir limits which merges run)
    DRY=true; [ "$MODE" = series-real ] && DRY=false
    run python -I -c "
import os, subprocess, sys
sys.path.insert(0, '/src')
from webui import core
s = core.load_settings()
env = core.script_env(s, {'DATA_DIR': '/data', 'SERIES_ONLY': 'true', 'SERIES_MERGE': 'true', 'AUTHOR_NORMALIZE': 'true', 'LIBRARY_REPORT': 'true',
                          'DRY_RUN': '$DRY', 'GOODREADS_ENABLED': 'false', 'ABS_BACKUP': 'true' if '$DRY' == 'false' else 'false'})
sys.exit(subprocess.call([sys.executable, '-u', '/src/abs_ratings.py'], env=env, cwd='/src'))" ;;
  dry|real)
    DRY=true; [ "$MODE" = real ] && DRY=false
    [ "$MODE" = real ] && [ ! -s "$P/backup/$(head -1 "$KEYS" | cut -d_ -f2).json" ] && { echo "no backup - run 'backup' first"; exit 2; }
    run python -I -c "
import os, subprocess, sys
sys.path.insert(0, '/src')
from webui import core
s = core.load_settings()
env = core.script_env(s, {'DATA_DIR': '/data', 'ONLY_ITEMS': ','.join(l.strip() for l in open('/data/keys.txt') if l.strip()),
                          'DRY_RUN': '$DRY', 'GOODREADS_ENABLED': 'false', 'ABS_BACKUP': 'true' if '$DRY' == 'false' else 'false', 'SLEEP_TIMER': '0'})
sys.exit(subprocess.call([sys.executable, '-u', '/src/abs_ratings.py'], env=env, cwd='/src'))" ;;
  *) echo "unknown mode"; exit 1 ;;
esac
