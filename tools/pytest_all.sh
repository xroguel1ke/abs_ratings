#!/bin/bash
# All unit tests under pytest, one process per file (the web UI tests set their own config folder at import time).
# Usage: tools/pytest_all.sh   (inside the image: pip install pytest is done once)
DEV=/mnt/user/appdata/abs_ratings_dev
timeout 900 docker run --rm --name abs_ratings_test_pytest -e PYTHONDONTWRITEBYTECODE=1 -v $DEV:/src -w /src ghcr.io/xroguel1ke/abs_ratings:web-ui sh -c '
pip install -q pytest >/dev/null 2>&1; fail=0; n=0
for f in tests/test_*.py; do out=$(python -m pytest $f 2>&1); r=$?; n=$((n+1)); [ $r -ne 0 ] && { echo "FAILED: $f"; echo "$out" | tail -15; fail=1; }; done
[ $fail = 0 ] && echo "pytest: all $n test files passed"; exit $fail'
