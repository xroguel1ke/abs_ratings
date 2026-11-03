#!/bin/bash
# Usage: tools/notify.sh "Title" "Body"  – runs tools/notify.py in a throwaway container (read-only mounts).
docker run --rm --name abs_ratings_test_notify \
  -v /mnt/user/appdata/abs_ratings_dev:/src:ro \
  -v /mnt/user/appdata/audiobookshelf/abs_scripts:/config:ro \
  ghcr.io/xroguel1ke/abs_ratings:web-ui python /src/tools/notify.py "$1" "$2"
