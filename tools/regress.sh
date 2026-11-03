#!/bin/bash
# THE gate for every change to matching, series or author logic. Run before every commit that touches abs_ratings.py / abs_series.py:
#   tools/regress.sh
# 1) syntax  2) author/series unit tests (synthetic)  3) whole-library series+author decisions vs hand-verified baseline
# 4) every golden round (all books ever verified) replayed from recordings and compared with tests/private/golden.json
# Exit code 0 only if nothing deviates. A deviation must be judged BY HAND: wrong -> fix the rule; better -> update golden/baseline with a note.
DEV=/mnt/user/appdata/abs_ratings_dev; cd $DEV; fail=0
IMG=ghcr.io/xroguel1ke/abs_ratings:web-ui
py() { timeout 600 docker run --rm --name "abs_ratings_test_regress_$$" -e PYTHONDONTWRITEBYTECODE=1 -v $DEV:/src -w /src $IMG python -I "$@"; }
echo "== 1 syntax";  py -c 'import sys; [compile(open(f).read(), f, "exec") for f in ("abs_ratings.py","abs_series.py","abs_undo.py","recorder.py")]; print("ok")' || fail=1
echo "== 2 unit tests"; py tests/test_authors.py | tail -1 || fail=1; py tests/test_series.py | tail -1 || fail=1; py tests/test_asin.py | tail -1 || fail=1
echo "== 3 library-wide series/author decisions"; py tools/lib_check.py || fail=1
echo "== 4 golden rounds"
i=0
for k in tests/private/round*_keys.txt; do
  i=$((i+1)); r=reg_$(basename $k _keys.txt)
  rm -rf /mnt/user/appdata/abs_scripts_test/$r
  ( REUSE=1 FAKE_PORT=$((18950+i)) tools/run_round.sh $r $k record > /tmp/claude-0/regress_$r.out 2>&1 ) &
done
wait
for k in tests/private/round*_keys.txt; do
  r=reg_$(basename $k _keys.txt); echo "-- $r"; grep -cE "Traceback|ERROR" /tmp/claude-0/regress_$r.out | sed 's/^/errors: /'
  tools/check_round.sh $r $k || fail=1
done
[ $fail = 0 ] && echo "REGRESSION: all green" || echo "REGRESSION: DEVIATIONS - judge by hand"
exit $fail
