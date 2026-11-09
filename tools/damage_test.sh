#!/bin/bash
# Damage simulation: baseline round + damaged round on the same books (replayed from recordings, nothing live), then compare.
#   tools/damage_test.sh [seed]     (uses tests/private/abs_snapshot_post3.json + tests/private/live_fix4_keys.txt)
DEV=/mnt/user/appdata/abs_ratings_dev; cd $DEV; SEED=${1:-1}
SNAP=tests/private/abs_snapshot_post3.json; KEYS=tests/private/live_fix4_keys.txt; OUT=tests/private/damage_$SEED
py() { timeout 900 docker run --rm --cpuset-cpus=${ABS_CPUS:-0-9,12-31} --name "abs_ratings_test_dmg_$$_$RANDOM" -e PYTHONDONTWRITEBYTECODE=1 -v $DEV:/src -w /src ghcr.io/xroguel1ke/abs_ratings:web-ui python -I "$@"; }
py tools/damage_test.py --make --snapshot $SNAP --keys $KEYS --out $OUT --seed $SEED || exit 2
for r in base dmg; do
  d=/mnt/user/appdata/abs_scripts_test/damage_${SEED}_$r; rm -rf $d; mkdir -p $d
  cp /mnt/user/appdata/audiobookshelf/abs_scripts/series_index.json /mnt/user/appdata/audiobookshelf/abs_scripts/role_credits.json $d/ 2>/dev/null
  s=/src/$SNAP; [ $r = dmg ] && s=/src/$OUT/damaged_snapshot.json
  port=$((19200 + SEED % 50 * 2)); [ $r = dmg ] && port=$((port + 1))
  RECORD_NO_LIVE=audible,api.hardcover.app,www.goodreads.com REUSE=1 SNAPSHOT=$s FAKE_PORT=$port \
    tools/run_round.sh damage_${SEED}_$r $KEYS record > /tmp/claude-0/damage_${SEED}_$r.out 2>&1 &
done
wait
P() { ls /mnt/user/appdata/abs_scripts_test/damage_${SEED}_$1/reports/planned_changes_*.jsonl | head -1 | sed "s#/mnt/user/appdata/abs_scripts_test#/t#"; }
timeout 300 docker run --rm --cpuset-cpus=${ABS_CPUS:-0-9,12-31} --name "abs_ratings_test_dmgc_$$" -v $DEV:/src -v /mnt/user/appdata/abs_scripts_test:/t:ro -w /src ghcr.io/xroguel1ke/abs_ratings:web-ui \
  python -I tools/damage_test.py --check --snapshot $SNAP --out $OUT --base $(P base) --dmg $(P dmg)
