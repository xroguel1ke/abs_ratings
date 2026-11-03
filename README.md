# 🎧 ABS Ratings Updater

![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)
![Platform](https://img.shields.io/badge/Platform-Unraid%20%7C%20Docker-orange)
![Vibecoding](https://img.shields.io/badge/Vibecoded-✨-purple)

A fully automated script designed for **Audiobookshelf (ABS)** on **Unraid**. It scrapes ratings from **Audible** (multiple regions) and **Goodreads** and injects them directly into your audiobook descriptions.

## ✨ Features

* **Dockerized Execution:** Runs via a temporary `python:3.11-slim` container using a wrapper script. No dependency hell on your host.
* **Dual Source Scraping:** Fetches ratings from Audible (cross-checks `.com` & `.de`) and Goodreads.
* **Auto-Repair & Fallback:** Automatically searches for missing ASINs/ISBNs and updates them in ABS. If a direct ID match fails, it attempts a text-based search to find the correct book.
* **Multi-Series Support:** Correctly handles books that belong to multiple series (e.g., "Cosmere" & "Stormlight Archive").
* **Manual Control (Locks):** Use tags in ABS to prevent specific fields (like Series or Language) from being overwritten.
* **Visual Ratings:** Adds "Moon" emojis (e.g., 🌕🌕🌗🌑🌑) for a quick visual overview in your library.
* **Smart Reporting:** Generates JSON reports for missing matches (`reports/`) and maintains a history to avoid redundant API calls.
* **Unraid Notifications:** Sends a status summary (Success/Failures/Duration) directly to the Unraid WebGUI.
* **Rate Limit Protection:** Built-in cool-downs and batching to keep your IP safe.

## 🚀 Installation (Unraid)

This script is optimized for the **User Scripts** plugin on Unraid.

1.  **Prepare Directory:**
    Create a folder for your scripts (e.g., in appdata):
    ```bash
    /mnt/user/appdata/audiobookshelf/abs_scripts/
    ```
2.  **Upload Files:**
    Place `abs_ratings.py` and `userscript.sh` into this folder.
3.  **Setup User Script:**
    * Open the **User Scripts** plugin in Unraid.
    * Add a new script (e.g., "ABS Ratings").
    * Paste the contents of `userscript.sh` into the script editor.
4.  **Configuration:**
    Edit the variables inside `userscript.sh` to match your setup (see below).

## ⚙️ Configuration

Open `userscript.sh` and adjust the **CONFIGURATION** section:

| Variable | Description | Example |
| :--- | :--- | :--- |
| `SCRIPT_DIR` | Absolute path to the folder containing `abs_ratings.py`. | `/mnt/user/appdata/...` |
| `ABS_URL` | Your internal Audiobookshelf URL (no trailing slash). | `http://192.168.1.10:13378` |
| `API_TOKEN` | Your ABS API Token (Bearer Token). | `eyJhbG...` |
| `LIBRARY_IDS` | Comma-separated list of Library IDs to scan. | `lib-uuid-1,lib-uuid-2` |
| `BATCH_SIZE` | Items to process per run (prevents bans). | `250` |
| `REFRESH_DAYS` | Update interval for existing ratings. | `90` |
| `DRY_RUN` | If `true`, nothing is written to ABS and the run history is not updated. | `false` |

**Optional** (add as `-e NAME=value` to the `docker run` call in `userscript.sh`):

| Variable | Description | Default |
| :--- | :--- | :--- |
| `ABS_TIMEOUT` | Timeout in seconds for each ABS API request. | `30` |
| `NO_RATINGS_RECHECK_DAYS` | Re-check interval for new releases that have no Audible ratings yet. | `7` |
| `GR_MISSING_RECHECK_DAYS` | Re-check interval when Audible worked but Goodreads found nothing. | `30` |

> **Note:** The script automatically creates `logs/` and `reports/` subdirectories in your script folder.

### Re-checks instead of strikes

* **No ratings yet:** If the Audible product page exists but nobody has rated the book yet, the script does **not** search for a replacement ASIN. It re-checks the book after `NO_RATINGS_RECHECK_DAYS`.
* **Only Goodreads missing:** If Audible worked, a missing Goodreads match does not count as a failed attempt. The book is re-checked after `GR_MISSING_RECHECK_DAYS`.
* **Goodreads blocked:** If Goodreads blocks the script (HTTP 403/429/503, bot challenge, login wall), Goodreads is skipped for the rest of the run and the book is re-checked the next day. The notification shows `⚠️ Goodreads blocked`.
* Only books without any Audible data get "strikes". After 5 strikes they wait for the normal `REFRESH_DAYS`.

Scheduled re-checks are stored in `recheck_history.json`.

## 🔒 Lock Tags (Manual Control)

You can prevent the script from updating specific metadata fields by adding **Tags** to your audiobooks directly in Audiobookshelf.

| Tag | Effect |
| :--- | :--- |
| `lock_all` | **Completely skips** this item. It never enters the queue: no updates, no API calls, no pause. |
| `lock_series` | Prevents updates to **Series** name and sequence. |
| `lock_language` | Prevents **Language** updates. |
| `lock_publisher` | Prevents **Publisher** updates. |
| `lock_year` | Prevents **Publish Year** updates. |
| `lock_genres` | Prevents adding new **Genres**. |
| `lock_isbn` | Prevents **ISBN** repairs/updates. |
| `lock_abridged` | Prevents changes to the **Abridged** flag. |
| `lock_description` | Prevents ratings from being written to the **Description**. |

## 🖼️ Preview

The script looks for `⭐ Ratings & Infos` in your description. If found, it updates the block; otherwise, it appends it.

**Example output in Audiobookshelf:**

> **⭐ Ratings & Infos**<br>
> Audible (124):<br>
> 🏆 🌕🌕🌕🌕🌗 4.6 / 5 - Overall<br>
> 🎙️ 🌕🌕🌕🌕🌕 4.8 / 5 - Performance<br>
> 📖 🌕🌕🌕🌕🌑 4.2 / 5 - Story<br>
> Goodreads (450):<br>
> 🏆 🌕🌕🌕🌕🌗 4.4 / 5 - Rating<br>
> ⭐<br>
> <br>
> *[Original Description follows here...]*

## 🛠️ How it Works

1.  The Bash script (`userscript.sh`) launches a Docker container mounting your script directory.
2.  Dependencies (`requests`, `beautifulsoup4`, `lxml`) are installed on-the-fly.
3.  The Python script scans your library, identifying items needing updates or missing metadata.
4.  It fetches data, potentially repairs missing ASINs/ISBNs, and pushes updates to ABS.
5.  Finally, it sends a notification to Unraid and rotates logs.

---
<sub>Vibecoded: Concept & Logic by the developer, refined with AI assistance.</sub>
