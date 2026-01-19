#!/bin/bash
# name=ABS Ratings Debugger
# description=Deep-Dive Analysis. Source: Manual ASIN or data directly from Audiobookshelf.
# arrayStarted=true

# ================= CONFIGURATION =================

# OPTION A: Manual ASIN (Takes precedence if filled)
MANUAL_ASIN=""

# OPTION B: Audiobookshelf Data (Used if MANUAL_ASIN is empty)
# URL to your ABS (e.g., http://192.168.1.10:13378)
ABS_URL="http://192.168.xxx.xx:13378"
# Your API Token (Settings -> Users -> Click on User -> API Token)
ABS_TOKEN="API-TOKEN-HERE"

# Item Identification (Fill only ONE of these)
# 1. By ID (The ID from the URL when you click on a book)
ABS_ITEM_ID=""
# 2. By Search (Simply enter the title or part of it)
ABS_SEARCH_QUERY=""

# Paths
SCRIPT_DIR="/mnt/user/appdata/audiobookshelf/abs_scripts"
SCRIPT_NAME="debug.py"

# ================= END CONFIGURATION =================

BASE_LOG_DIR="${SCRIPT_DIR}/debug_logs"
mkdir -p "$BASE_LOG_DIR"

echo "================================================"
echo "    ABS RATINGS DIAGNOSTIC TOOL"
echo "================================================"
echo "Timestamp:    $(date)"
echo "------------------------------------------------"

# Start Docker
# We pass all variables to the Python script
docker run --rm \
  -v "$SCRIPT_DIR:$SCRIPT_DIR" \
  -e MANUAL_ASIN="$MANUAL_ASIN" \
  -e ABS_URL="$ABS_URL" \
  -e ABS_TOKEN="$ABS_TOKEN" \
  -e ABS_ITEM_ID="$ABS_ITEM_ID" \
  -e ABS_SEARCH_QUERY="$ABS_SEARCH_QUERY" \
  -e LOG_DIR="$BASE_LOG_DIR" \
  python:3.11-slim \
  /bin/bash -c "pip install requests beautifulsoup4 lxml > /dev/null 2>&1 && python3 -u \"$SCRIPT_DIR/$SCRIPT_NAME\""

echo "------------------------------------------------"
echo "Debug finished. Check folder in: $BASE_LOG_DIR"
echo "================================================"
