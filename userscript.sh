#!/bin/bash
# name=ABS Ratings Updater
# description=Starts a temporary Python container to scrape Audible and Goodreads ratings <br>for Audiobookshelf and inserts them into the ABS comments field.
# arrayStarted=true

# ================= CONFIGURATION =================

# 1. Paths
# The directory where your Python script is located on the host (Unraid).
# IMPORTANT: This path will be mounted to the exact same location inside the container.
SCRIPT_DIR="PATH_TO_abs_ratings.py"
# The filename of the Python script to execute.
SCRIPT_NAME="abs_ratings.py"

# 2. Audiobookshelf Connection
# Your internal ABS URL (do not add a trailing slash).
ABS_URL="http://<YOUR_SERVER_IP>:<PORT>"

# 3. Authentication
# Your API Token (Bearer Token).
API_TOKEN="<YOUR_API_TOKEN_HERE>"

# 4. Target Libraries
# Comma-separated list of Library IDs to scan.
LIBRARY_IDS="<LIBRARY_ID_1>,<LIBRARY_ID_2>"

# 5. Execution Settings
# Number of items to process before saving/pausing.
BATCH_SIZE=250
# Time in seconds to wait between batches (prevents API rate limiting).
SLEEP_TIMER=10
# Only update items that haven't been scanned in X days.
REFRESH_DAYS=90
# If set to true, no changes will be written to the ABS database.
DRY_RUN=false

# ================= END CONFIGURATION =================

echo "------------------------------------------------"
echo "Starting ABS Ratings Update (Docker Run Method)"
echo "Date: $(date)"
echo "------------------------------------------------"

# Launching a temporary Python container.
# Explanation of flags:
# --rm: Automatically remove the container after the script finishes.
# -v "$SCRIPT_DIR:$SCRIPT_DIR": Mounts the host directory to the exact same path inside the container.
#     This ensures the Python script finds files exactly where it expects them based on host paths.
# -e ...: Passes the configuration variables as environment variables to the Python script.

docker run --rm \
  -v "$SCRIPT_DIR:$SCRIPT_DIR" \
  -e ABS_URL="$ABS_URL" \
  -e API_TOKEN="$API_TOKEN" \
  -e LIBRARY_IDS="$LIBRARY_IDS" \
  -e BATCH_SIZE="$BATCH_SIZE" \
  -e SLEEP_TIMER="$SLEEP_TIMER" \
  -e REFRESH_DAYS="$REFRESH_DAYS" \
  -e DRY_RUN="$DRY_RUN" \
  python:3.11-slim \
  /bin/bash -c "pip install requests beautifulsoup4 lxml > /dev/null 2>&1 && python3 \"$SCRIPT_DIR/$SCRIPT_NAME\""

echo "------------------------------------------------"
echo "Script finished."
echo "------------------------------------------------"
