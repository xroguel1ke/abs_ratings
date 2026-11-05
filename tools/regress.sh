#!/bin/bash
# THE gate for every change to matching, series or author logic. Run before every commit that touches abs_ratings.py / abs_series.py:
#   tools/regress.sh
# 1) syntax  2) author/series unit tests (synthetic)  3) whole-library series+author decisions vs hand-verified baseline
# 4) every golden round (all books ever verified) replayed from recordings and compared with tests/private/golden.json
# Exit code 0 only if nothing deviates. A deviation must be judged BY HAND: wrong -> fix the rule; better -> update golden/baseline with a note.
DEV=/mnt/user/appdata/abs_ratings_dev; cd $DEV; fail=0
IMG=ghcr.io/xroguel1ke/abs_ratings:web-ui
py() { timeout 600 docker run --rm --name "abs_ratings_test_regress_$$" -e PYTHONDONTWRITEBYTECODE=1 -v $DEV:/src -w /src $IMG python -I "$@"; }
echo "== 1 syntax";  py -c 'import sys; [compile(open(f).read(), f, "exec") for f in ("abs_ratings.py","abs_series.py","abs_undo.py","recorder.py","source_state.py")]; print("ok")' || fail=1
echo "== 2 unit tests"; py tests/test_authors.py | tail -1 || fail=1; py tests/test_series.py | tail -1 || fail=1; py tests/test_asin.py | tail -1 || fail=1; py tests/test_webui_queue.py | tail -1 || fail=1; py tests/test_webui_i18n.py | tail -1 || fail=1; py tests/test_source_state.py | tail -1 || fail=1
echo "== 2b per-source search state (real-run mode vs fake ABS, offline)"; tools/source_state_test.sh tests/private/sst_keys.txt > /tmp/claude-0/sst_it.out 2>&1 || fail=1; tail -1 /tmp/claude-0/sst_it.out
echo "== 2c Hardcover matches of the hand-checked batches"; tools/hc_golden_check.sh || fail=1
echo "== 3 library-wide series/author decisions"; py tools/lib_check.py || fail=1
echo "== 4 golden rounds"
i=0
for k in tests/private/round*_keys.txt tests/private/wl0*_keys.txt; do
  i=$((i+1)); r=reg_$(basename $k _keys.txt)
  rm -rf /mnt/user/appdata/abs_scripts_test/$r
  ( RECORD_NO_LIVE=audible,api.hardcover.app,www.goodreads.com REUSE=1 FAKE_PORT=$((19100+i)) tools/run_round.sh $r $k record > /tmp/claude-0/regress_$r.out 2>&1 ) &
done
# synthetic batches (books NOT in the library, EN + DE): replayed from recordings, expected ASIN known
for sf in tests/synthetic/batch_*.json; do
  n=$(basename $sf .json | sed 's/batch_//'); i=$((i+1)); r=reg_syn_$n
  docker ps --format '{{.Names}}' | grep -q "^abs_ratings_test_h$n$" && { echo "-- skip batch $n (still being recorded)"; continue; }
  jq '{books: (.expected|map_values(del(.case)))}' $sf > /tmp/claude-0/syn_golden_$n.json; jq -r '.expected|keys[]' $sf > /tmp/claude-0/syn_keys_$n.txt
  rm -rf /mnt/user/appdata/abs_scripts_test/$r
  ( RECORD_NO_LIVE=audible,api.hardcover.app,www.goodreads.com SYNTH=$n HARDCOVER_OFF=1 REUSE=1 FAKE_PORT=$((19100+i)) tools/run_round.sh $r tests/private/none_keys.txt record > /tmp/claude-0/regress_$r.out 2>&1 ) &
done
wait
for k in tests/private/round*_keys.txt tests/private/wl0*_keys.txt; do
  r=reg_$(basename $k _keys.txt); echo "-- $r"; grep -cE "Traceback|ERROR" /tmp/claude-0/regress_$r.out | sed 's/^/errors: /'
  tools/check_round.sh $r $k || fail=1
done
for sf in tests/synthetic/batch_*.json; do
  n=$(basename $sf .json | sed 's/batch_//'); r=reg_syn_$n; [ -d /mnt/user/appdata/abs_scripts_test/$r ] || continue; echo "-- $r (synthetic books, not in the library)"; grep -cE "Traceback|ERROR" /tmp/claude-0/regress_$r.out | sed 's/^/errors: /'
  docker run --rm --name abs_ratings_test_evalsyn -e PYTHONDONTWRITEBYTECODE=1 -v $DEV:/src:ro -v /mnt/user/appdata/abs_scripts_test:/t:ro -v /tmp/claude-0:/tmp/claude-0:ro -w /src $IMG python -I tools/evaluate.py $(ls /mnt/user/appdata/abs_scripts_test/$r/reports/planned_changes_*.jsonl | head -1 | sed 's#.*/abs_scripts_test#/t#') /tmp/claude-0/syn_golden_$n.json /tmp/claude-0/syn_keys_$n.txt || fail=1
done
echo "-- requests that had no recording (blocked, never sent): $(cat tests/private/recordings/_misses.jsonl 2>/dev/null | grep -c blocked)"
[ $fail = 0 ] && echo "REGRESSION: all green" || echo "REGRESSION: DEVIATIONS - judge by hand"
exit $fail
