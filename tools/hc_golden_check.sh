#!/bin/bash
# Replays the hand-checked Hardcover batches offline WITH Hardcover and compares every match with tests/synthetic/hc_golden.json
DEV=/mnt/user/appdata/abs_ratings_dev; cd $DEV; fail=0; E=$(mktemp -d /tmp/claude-0/hcg.XXXX); : > $E/empty
for n in $(jq -r '.batches|keys[]' tests/synthetic/hc_golden.json); do
  ( rm -rf /mnt/user/appdata/abs_scripts_test/hcg$n; SYNTH=$n REUSE=1 RECORD_NO_LIVE=audible,api.hardcover.app,www.goodreads.com,media-amazon FAKE_PORT=$((19300+n)) WORKERS=3 tools/run_round.sh hcg$n $E/empty record > $E/r$n.out 2>&1 ) &
done; wait
for n in $(jq -r '.batches|keys[]' tests/synthetic/hc_golden.json); do
  d=$(jq -r --arg n $n --slurpfile g tests/synthetic/hc_golden.json 'to_entries[] | select(.key|test("syn-")) | ((.value.hardcover // "")|sub("https://hardcover.app/books/";"")) as $now | ($g[0].batches[$n][.key] // "") as $exp | select($now != $exp) | "  HC DEVIATION \(.value.title): expected \($exp), got \($now)"' /mnt/user/appdata/abs_scripts_test/hcg$n/results.json)
  if [ -n "$d" ]; then echo "-- hc batch $n"; echo "$d"; fail=1; else echo "-- hc batch $n: all Hardcover matches as hand-checked"; fi
done
exit $fail
