#!/bin/bash
# name=ABS Ratings Updater
# description=Starts a temporary Python container that adds Audible and Goodreads ratings to Audiobookshelf,<br>keeps metadata clean and merges split series.
# arrayStarted=true

# ================= CONFIGURATION =================

# 1. Paths (data, logs and reports are stored here; the .py files are downloaded into it)
SCRIPT_DIR="PATH_TO_YOUR_SCRIPT_FOLDER"
SCRIPT_NAME="abs_ratings.py"

# 1b. Code source: the .py files are downloaded from GitHub before every run
AUTO_UPDATE=true                      # false = use the .py files that are already in SCRIPT_DIR
GITHUB_REPO="xroguel1ke/abs_ratings"
GIT_BRANCH="main"                     # e.g. "main" or a test branch like "claude/festive-fermi-xhscdy"

# 2. Audiobookshelf Connection
ABS_URL="http://<YOUR_SERVER_IP>:<PORT>"

# 3. Authentication
API_TOKEN="<YOUR_API_TOKEN_HERE>"

# 4. Target Libraries (comma separated)
LIBRARY_IDS="<LIBRARY_ID_1>,<LIBRARY_ID_2>"

# 5. Execution Settings
BATCH_SIZE=300             # Books per run
SLEEP_TIMER=3              # Pause between books (each source is additionally throttled)
REFRESH_DAYS=90            # Refresh ratings after X days
DRY_RUN=true               # true = change nothing, only log + reports/planned_changes_*.jsonl
SERIES_ONLY=false          # true = only series merge + library report (ABS only, no Audible/Goodreads)
GOODREADS_MAX_PER_RUN=300  # Goodreads searches per run (first pass is spread over several runs)
LOG_LEVEL=INFO             # INFO or DEBUG
TIMEZONE="Europe/Berlin"   # Timestamps in the logs

# 6. Undo (optional): enter a run id (see log summary or: UNDO_RUN="list") to undo that run INSTEAD of a normal run
UNDO_RUN=""
UNDO_APPLY=false           # false = only show what would be undone, true = really undo

# 7. Notifications & Logs
NOTIFY_SUCCESS=true        # Unraid notification after every run (errors are always notified)
LOG_RETENTION_DAYS=14      # Also removes old planned_changes_*.jsonl files

# ================= END CONFIGURATION =================

LOG_DIR="${SCRIPT_DIR}/logs"
REPORT_DIR="${SCRIPT_DIR}/reports"
CONTAINER_NAME="abs-ratings"
NOTIFY="/usr/local/emhttp/webGui/scripts/notify"
mkdir -p "$LOG_DIR" "$REPORT_DIR"

echo "------------------------------------------------"
echo "Starting ABS Ratings Update (Docker Run Method)"
echo "Date: $(date)"
echo "------------------------------------------------"

# ================= CODE UPDATE FROM GITHUB =================
# Downloads the .py files of GIT_BRANCH. Uses the exact commit (no stale GitHub cache), downloads into a
# temp folder first and only replaces the files when everything arrived. If GitHub is unreachable,
# the files already in SCRIPT_DIR are used.
CODE_FILES="abs_ratings.py abs_series.py abs_undo.py"
CODE_VERSION="local files"
if [ "$AUTO_UPDATE" = "true" ]; then
    echo "Updating code from GitHub: $GITHUB_REPO @ $GIT_BRANCH"
    SHA=$(curl -fsS --max-time 20 -H "Accept: application/vnd.github.sha" \
          "https://api.github.com/repos/$GITHUB_REPO/commits/$GIT_BRANCH" 2>/dev/null)
    if [ -z "$SHA" ] || [ ${#SHA} -ne 40 ]; then
        echo "Warning: Could not resolve branch '$GIT_BRANCH' on GitHub. Using local files."
        $NOTIFY -e "ABS Ratings" -s "ABS Ratings: GitHub nicht erreichbar ⚠️" -d "Branch $GIT_BRANCH nicht gefunden/erreichbar, lokale Dateien werden genutzt." -i "warning"
    else
        TMP_DL=$(mktemp -d)
        DL_OK=true
        for f in $CODE_FILES; do
            CODE=$(curl -sS --max-time 30 -w "%{http_code}" -o "$TMP_DL/$f" \
                   "https://raw.githubusercontent.com/$GITHUB_REPO/$SHA/$f")
            if [ "$CODE" = "404" ]; then
                rm -f "$TMP_DL/$f"          # File doesn't exist in this branch (e.g. older version)
            elif [ "$CODE" != "200" ] || [ ! -s "$TMP_DL/$f" ]; then
                echo "Warning: Download of $f failed (HTTP $CODE)."; DL_OK=false
            fi
        done
        if [ "$DL_OK" = "true" ] && [ -s "$TMP_DL/abs_ratings.py" ]; then
            for f in $CODE_FILES; do
                if [ -f "$TMP_DL/$f" ]; then cp "$TMP_DL/$f" "$SCRIPT_DIR/$f"
                else rm -f "$SCRIPT_DIR/$f"; fi  # Remove files that don't belong to this branch
            done
            CODE_VERSION="$GIT_BRANCH @ ${SHA:0:7}"
            echo "$CODE_VERSION ($(date '+%Y-%m-%d %H:%M'))" > "$SCRIPT_DIR/.code_version"
            echo "Code updated: $CODE_VERSION ($(cd "$TMP_DL" && ls | tr '\n' ' '))"
        else
            echo "Warning: Download incomplete. Using local files."
            $NOTIFY -e "ABS Ratings" -s "ABS Ratings: Update fehlgeschlagen ⚠️" -d "Download von $GIT_BRANCH unvollständig, lokale Dateien werden genutzt." -i "warning"
        fi
        rm -rf "$TMP_DL"
    fi
fi
[ "$CODE_VERSION" = "local files" ] && [ -f "$SCRIPT_DIR/.code_version" ] && CODE_VERSION="$(cat "$SCRIPT_DIR/.code_version") (cached)"

if [ ! -f "$SCRIPT_DIR/$SCRIPT_NAME" ]; then
    echo "Error: $SCRIPT_DIR/$SCRIPT_NAME is missing."
    $NOTIFY -e "ABS Ratings" -s "ABS Ratings: Datei fehlt ❌" -d "$SCRIPT_NAME fehlt in $SCRIPT_DIR" -i "alert"
    exit 1
fi

# Never run twice at the same time (e.g. a manual start while the nightly run is still busy)
if docker ps --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
    echo "Error: A run is still in progress (container '$CONTAINER_NAME'). Aborting."
    $NOTIFY -e "ABS Ratings" -s "ABS Ratings: Übersprungen ⏭️" -d "Ein vorheriger Lauf ist noch aktiv." -i "warning"
    exit 1
fi

# Clean up old logs and planned-change files (keep last X days)
find "$LOG_DIR" -type f -name "*.log" -mtime +$LOG_RETENTION_DAYS -delete
find "$REPORT_DIR" -type f -name "planned_changes_*.jsonl" -mtime +$LOG_RETENTION_DAYS -delete

# Remove old env file if exists
rm -f "$SCRIPT_DIR/last_run.env"

# Undo mode instead of a normal run
if [ -n "$UNDO_RUN" ]; then
    if [ ! -f "$SCRIPT_DIR/abs_undo.py" ]; then echo "Error: abs_undo.py is not part of branch $GIT_BRANCH."; exit 1; fi
    if [ "$UNDO_RUN" = "list" ]; then UNDO_ARGS="--list"; else UNDO_ARGS="--run $UNDO_RUN"; fi
    [ "$UNDO_APPLY" = "true" ] && UNDO_ARGS="$UNDO_ARGS --apply"
    echo "UNDO mode: abs_undo.py $UNDO_ARGS"
    docker run --rm --name "$CONTAINER_NAME" \
      -v "$SCRIPT_DIR:$SCRIPT_DIR" \
      -e ABS_URL="$ABS_URL" -e API_TOKEN="$API_TOKEN" -e TZ="$TIMEZONE" \
      python:3.11-slim \
      /bin/bash -c "pip install -q --disable-pip-version-check --root-user-action=ignore requests > /dev/null && python3 -u \"$SCRIPT_DIR/abs_undo.py\" $UNDO_ARGS"
    echo "------------------------------------------------"
    echo "Undo finished. Set UNDO_RUN=\"\" again for normal runs."
    exit 0
fi

# Launch Docker (-u = unbuffered output, so the User Scripts log shows progress live)
docker run --rm --name "$CONTAINER_NAME" \
  -v "$SCRIPT_DIR:$SCRIPT_DIR" \
  -e ABS_URL="$ABS_URL" \
  -e API_TOKEN="$API_TOKEN" \
  -e LIBRARY_IDS="$LIBRARY_IDS" \
  -e BATCH_SIZE="$BATCH_SIZE" \
  -e SLEEP_TIMER="$SLEEP_TIMER" \
  -e REFRESH_DAYS="$REFRESH_DAYS" \
  -e DRY_RUN="$DRY_RUN" \
  -e SERIES_ONLY="$SERIES_ONLY" \
  -e GOODREADS_MAX_PER_RUN="$GOODREADS_MAX_PER_RUN" \
  -e LOG_LEVEL="$LOG_LEVEL" \
  -e TZ="$TIMEZONE" \
  -e CODE_VERSION="$CODE_VERSION" \
  python:3.11-slim \
  /bin/bash -c "pip install -q --disable-pip-version-check --root-user-action=ignore requests beautifulsoup4 lxml > /tmp/pip.log 2>&1 || { echo 'pip install failed:'; cat /tmp/pip.log; exit 1; }; python3 -u \"$SCRIPT_DIR/$SCRIPT_NAME\""

# ================= NOTIFICATION =================

# Load variables generated by Python script (stats, status, icons)
if [ -f "$SCRIPT_DIR/last_run.env" ]; then
    source "$SCRIPT_DIR/last_run.env"

    SEP=" | "
    BIG_SEP=" || "
    FINAL_MSG="${ABS_HEADER}${SEP}Dauer: ${ABS_DURATION}${SEP}Log: ${ABS_LOG_FILE}${BIG_SEP}${ABS_REPORT_BODY}"

    # Errors are always notified, successful runs only with NOTIFY_SUCCESS=true
    if [ "$NOTIFY_SUCCESS" = "true" ] || [ "$ABS_ICON" = "alert" ]; then
        $NOTIFY -e "ABS Ratings" -s "$ABS_SUBJECT" -d "$FINAL_MSG" -i "$ABS_ICON"
    fi
else
    echo "Error: No result environment file found. Python script might have crashed."
    $NOTIFY -e "ABS Ratings" -s "ABS Update: Kritischer Fehler ❌" -d "Das Python-Script konnte nicht korrekt beendet werden. Bitte Logs prüfen." -i "alert"
fi

echo "------------------------------------------------"
echo "Script finished."
echo "------------------------------------------------"
