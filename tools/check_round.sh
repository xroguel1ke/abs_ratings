#!/bin/bash
# Usage: tools/check_round.sh <round-name> <keys-file>   -> evaluate a finished round against tests/private/golden.json (only that round's books)
DEV=/mnt/user/appdata/abs_ratings_dev; P=$(ls -t $DEV/../abs_scripts_test/$1/reports/planned_changes_*.jsonl | head -1 | sed "s#.*/abs_scripts_test#/t#")
docker run --rm --cpuset-cpus=${ABS_CPUS:-0-9,12-31} --name abs_ratings_test_eval -e PYTHONDONTWRITEBYTECODE=1 -v $DEV:/src:ro -v /mnt/user/appdata/abs_scripts_test:/t:ro -w /src ghcr.io/xroguel1ke/abs_ratings:web-ui python -I tools/evaluate.py "$P" ${3:-tests/private/golden.json} "$2"
