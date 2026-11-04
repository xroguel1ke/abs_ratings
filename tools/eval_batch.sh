#!/bin/bash
# Compact evaluation of one synthetic batch round: ONE line when clean, deviations only otherwise (token-saving loop mode).
# Usage: tools/eval_batch.sh <round-name> <seed>
R="$1"; N="$2"; DEV=/mnt/user/appdata/abs_ratings_dev; D=/mnt/user/appdata/abs_scripts_test/$R; T=/tmp/claude-0/evb; mkdir -p $T
L=$(ls -t $D/logs/run_*.log 2>/dev/null | head -1); P=$(ls -t $D/reports/planned_changes_*.jsonl 2>/dev/null | head -1)
[ -z "$L" ] || [ -z "$P" ] && { echo "$R: NO RESULT (run missing)"; exit 1; }
grep -q "Planned changes:" $L || { echo "$R: NOT FINISHED"; exit 1; }
jq '{books: (.expected|map_values(del(.case)))}' $DEV/tests/synthetic/batch_$N.json > $T/g$N.json; jq -r '.expected|keys[]' $DEV/tests/synthetic/batch_$N.json > $T/k$N.txt
OUT=$(docker run --rm -e PYTHONDONTWRITEBYTECODE=1 -v $DEV:/src:ro -v /mnt/user/appdata/abs_scripts_test:/t:ro -v $T:$T:ro -w /src ghcr.io/xroguel1ke/abs_ratings:web-ui \
      python -I tools/evaluate.py "$(echo $P | sed 's#.*/abs_scripts_test#/t#')" $T/g$N.json $T/k$N.txt 2>&1)
ERR=$(grep -oP "Errors: \K\d+" $L | tail -1); HC=$(grep -c '✅ Hardcover via' $L); DROP=$(grep -ciE "cross-check.*(drop|reject)" $L)
echo "$R (seed $N): $(echo "$OUT" | grep -E '^golden' | sed 's/golden: //') | errors $ERR | HC hits $HC, cross-check drops $DROP"
echo "$OUT" | grep -E "DEVIATION" | cut -c1-260
