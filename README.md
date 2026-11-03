# 🎧 ABS Ratings Updater

![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)
![Platform](https://img.shields.io/badge/Platform-Unraid%20%7C%20Docker-orange)
![Vibecoding](https://img.shields.io/badge/Vibecoded-✨-purple)

A fully automated script designed for **Audiobookshelf (ABS)** on **Unraid**. It scrapes ratings from **Audible** (multiple regions) and **Goodreads** and injects them directly into your audiobook descriptions.

## ✨ Features

* **Dockerized Execution:** Runs via a temporary `python:3.11-slim` container using a wrapper script. No dependency hell on your host.
* **Audible Catalog API:** Ratings (overall/performance/story) and metadata come from Audible's JSON API (`.com` & `.de`). Scraping the product pages is only used as a fallback.
* **Goodreads:** Ratings via ISBN/ASIN lookup or text search with title/author/volume matching.
* **Auto-Repair & Fallback:** Automatically searches for missing ASINs (Audible search API, verified by title, author and runtime) and updates them in ABS.
* **Title Repair:** Broken, file-name style titles such as `The Book of the New Sun 05 The Urth of the New Sun` or `High Spirits: High Spirits: A Cultural History…` are detected, searched with the clean title and corrected to Audible's title (and subtitle, if empty). Normal titles are never touched.
* **Goodreads Cache:** Once a Goodreads book page was found, its URL is remembered (`goodreads_cache.json`), so refreshes need a single request.
* **Multi-Series Support:** Correctly handles books that belong to multiple series (e.g., "Cosmere" & "Stormlight Archive").
* **Manual Control (Locks):** Use tags in ABS to prevent specific fields (like Series or Language) from being overwritten.
* **Visual Ratings:** Adds "Moon" emojis (e.g., 🌕🌕🌗🌑🌑) for a quick visual overview in your library.
* **Smart Reporting:** Generates JSON reports for missing matches (`reports/`) and maintains a history to avoid redundant API calls.
* **Unraid Notifications:** Sends a status summary (Success/Failures/Duration) directly to the Unraid WebGUI.
* **Rate Limit Protection:** Per-source request throttling, cool-downs and batching to keep your IP safe.

## 🚀 Installation (Unraid)

This script is optimized for the **User Scripts** plugin on Unraid.

1.  **Prepare Directory:**
    Create a folder for your scripts (e.g., in appdata):
    ```bash
    /mnt/user/appdata/audiobookshelf/abs_scripts/
    ```
2.  **Code:**
    Nothing to upload: with `AUTO_UPDATE=true` the userscript downloads the `.py` files of `GIT_BRANCH` from GitHub before every run (exact commit, temp folder first, falls back to the last downloaded version if GitHub is unreachable). Switch between `main` and a test branch by changing `GIT_BRANCH`. The used version is shown in the log header (`Code version: <branch> @ <commit>`).
    With `AUTO_UPDATE=false`, place `abs_ratings.py`, `abs_series.py` and `abs_undo.py` into this folder yourself.
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
| `SLEEP_TIMER` | Pause in seconds between two books. | `3` |
| `REFRESH_DAYS` | Update interval for existing ratings. | `90` |
| `DRY_RUN` | If `true`, nothing is written to ABS and the run history is not updated. | `false` |

**Optional** (add as `-e NAME=value` to the `docker run` call in `userscript.sh`):

| Variable | Description | Default |
| :--- | :--- | :--- |
| `ABS_TIMEOUT` | Timeout in seconds for each ABS API request. | `30` |
| `NO_RATINGS_RECHECK_DAYS` | Re-check interval for new releases that have no Audible ratings yet. | `7` |
| `GR_MISSING_RECHECK_DAYS` | Re-check interval when Audible worked but Goodreads found nothing. | `30` |
| `USE_AUDIBLE_API` | Use the Audible catalog API (`false` = scrape product pages only). | `true` |
| `AUDIBLE_API_INTERVAL` | Minimum seconds between two Audible API requests. | `1` |
| `AUDIBLE_WEB_INTERVAL` | Minimum seconds between two Audible web page requests (searches, fallback). | `3` |
| `GOODREADS_INTERVAL` | Minimum seconds between two Goodreads requests. | `3` |
| `GR_BLOCK_COOLDOWN` | Pause in seconds after Goodreads blocks, before one retry. | `120` |
| `GR_MAX_COOLDOWNS` | Cool-downs per run before Goodreads is skipped for the rest of the run. | `1` |
| `GOODREADS_MAX_PER_RUN` | Max Goodreads searches per run (cached books don't count, `0` = unlimited). The rest follows in the next runs. | `300` |
| `SEARCH_PENALTY_SLEEP` | Optional extra pause after an ASIN search. | `0` |
| `RUNTIME_TOLERANCE_PCT` / `RUNTIME_TOLERANCE_MIN` | Runtime difference that counts as another edition (both must be exceeded). | `15` / `20` |
| `FILL_COVERS` | Set the Audible cover when ABS has none. | `true` |
| `GENRE_LANGUAGE` | `en` = translate audible.de genres, `keep` = keep as delivered. | `en` |
| `SERIES_MERGE` / `LIBRARY_REPORT` | ABS-only series merge / library report at the end of the run. | `true` |
| `SERIES_ONLY` | Only run the series merge + report (no Audible/Goodreads). | `false` |
| `LOG_LEVEL` | `INFO` or `DEBUG`. | `INFO` |

> **Pauses:** Every source is throttled on its own (see the `*_INTERVAL` variables), so `SLEEP_TIMER` can stay small. If you ever hit a rate limit, raise the interval of the affected source first.

> **Note:** The script automatically creates `logs/` and `reports/` subdirectories in your script folder.

> **Troubleshooting:** after every run `logs/debug_latest.log` (and `logs/debug_<run>.log`) contains everything in one file: settings (without token), the full run log, all planned/applied changes of the run, the library report summary and the state of the data files. Send this single file when something needs to be checked.

**Userscript extras:** `TIMEZONE` for log timestamps, `NOTIFY_SUCCESS` (errors are always notified), `UNDO_RUN` / `UNDO_APPLY` to undo a run directly from the User Scripts plugin (`UNDO_RUN="list"` shows all runs). The userscript refuses to start while a previous run is still active and checks that all three `.py` files are present.

### Large libraries

Audible ratings and metadata come from the API and are fast (one request per book). Goodreads has no API and blocks bots after a while, so the **first** pass is spread over several runs: each run searches Goodreads for at most `GOODREADS_MAX_PER_RUN` books, the rest is re-checked the next day. Once a book is found, its Goodreads URL is cached, so later refreshes need one request per book.

### Re-checks instead of strikes

* **No ratings yet:** If the Audible product page exists but nobody has rated the book yet, the script does **not** search for a replacement ASIN. It re-checks the book after `NO_RATINGS_RECHECK_DAYS`.
* **Only Goodreads missing:** If Audible worked, a missing Goodreads match does not count as a failed attempt. The book is re-checked after `GR_MISSING_RECHECK_DAYS`.
* **Goodreads blocked:** If Goodreads blocks the script (HTTP 403/429/503, bot challenge, login wall), the script pauses for `GR_BLOCK_COOLDOWN` seconds and retries. If it is still blocked, Goodreads is skipped for the rest of the run and the book is re-checked the next day. The notification shows `⚠️ Goodreads blocked`.
* Only books without any Audible data get "strikes". After 5 strikes they wait for the normal `REFRESH_DAYS`.

Scheduled re-checks are stored in `recheck_history.json`.

## 📝 Metadata Rules

Everything is decided automatically. If a match is not certain, nothing is changed (the reason is logged).

* **Runtime check:** If the ABS file duration and Audible's runtime differ by more than `RUNTIME_TOLERANCE_PCT` (15 %) **and** `RUNTIME_TOLERANCE_MIN` (20 min), the ASIN probably points to another edition. The script searches for the right one; if none is found, only the ratings are used and no metadata is changed.
* **Filled only when empty:** narrators, authors, publisher, year, ISBN, cover, description (Audible summary).
* **Explicit:** only set to true when Audible marks the product as adult content.
* **Language:** ISO codes are normalized (`eng` → `English`, `deu` → `German`, ...).
* **Genres from audible.de** are translated to the English Audible names (`GENRE_LANGUAGE=en`); untranslated names are listed in the library report.
* **Publisher / Year:** only filled when empty in ABS (Audible has the audiobook edition's data).
* **Genres:** only the most specific Audible category of each branch is added (e.g. `Space Opera`, not `Science Fiction & Fantasy`); bare top-level categories (`Literature & Fiction`) only when Audible has nothing more specific. Existing genres are kept.
* **Series:** Audible's series are **added**; existing ABS series are never removed and existing numbers never blanked or overwritten. Series names go through `series_aliases.json` and the existing ABS series first.
* **Wrong ASIN detection:** different volume numbers in ABS and Audible title (`Vol. 9` vs `Vol. 18`) or a large runtime difference trigger a search for the right ASIN; if none is found, metadata is left alone.
* **Title:** only corrected for broken titles (see Title Repair).

## 📚 Series Merge & Library Report (ABS only)

At the end of every run a second task runs **only against ABS** (no Audible/Goodreads requests):

**Series merge.** Audible often uses different series names within one series. Two series are merged when **all** of these are true:
* the names are the same after normalization (`The Osten Ard Saga` = `Osten Ard`, `Hell Mode Light Novel` = `Hell Mode`) or only differ in spelling with the same number of words (`Realm`/`Realms of the Elderlings`). A name that contains the other plus extra words is treated as a sub-series and never merged (`Dune, les origines`, `Discworld: Witches`),
* they are the same edition: *Full Cast*, *Dramatized*, *GraphicAudio*, *Abridged* and *Manga* versions are never merged with the plain series,
* every book of the smaller series shares an author with the larger one,
* no book number exists in both series,
* for names that are not identical, the numbers must complement each other (e.g. `1-3, 5-7` + `4, 8-9`),
* no book of the smaller series has `lock_series`.

The smaller series is renamed to the series that had more books. The merge is remembered in `series_aliases.json` (`{"library id": {"old name": "series name"}}`, editable): from then on the Audible sync writes the merged name automatically, so no lock tag is needed and the books keep getting updates (numbers, additional series). `series_index.json` lists all series in ABS, so a new book whose Audible series name matches an existing series (same normalized name, same edition, shared author) joins it instead of creating a split. Every decision (merged or not, and why) is in the log. `SERIES_LOCK_AFTER_MERGE=true` restores the old behaviour (tag `lock_series`).

**Author spellings.** `R. A. Salvatore` / `R.A. Salvatore` (same name except dots, spaces, case, `Last, First`) are unified to the spelling used by more books. Done per book, so `abs_undo.py` can revert it. Disable with `AUTHOR_NORMALIZE=false`.

**Library report** (`reports/library_report.html` + `.json`): merges, similar series that were *not* merged and why, gaps in series, books without series number, duplicate ASINs, author spelling variants, books without ASIN/cover/description/narrators/language/genres, untranslated genres.

Run only this task (fast, no external requests): `SERIES_ONLY=true`. Disable with `SERIES_MERGE=false` / `LIBRARY_REPORT=false`.

## ↩️ Change Journal & Undo

Every change is written to `changes.jsonl` (old value → new value). In `DRY_RUN` the planned changes go to `reports/planned_changes_<run>.jsonl` instead.

```bash
python3 abs_undo.py --list                          # runs and number of changes
python3 abs_undo.py --run 20261103-031927           # show what would be undone
python3 abs_undo.py --run 20261103-031927 --apply   # undo a whole run
python3 abs_undo.py --item <item id> --apply        # undo one book
python3 abs_undo.py --run <id> --field series --apply
```
`abs_undo.py` needs `ABS_URL` and `API_TOKEN` (run it in the same Docker container as the main script). Covers set by the script are removed again.

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
| `lock_title` | Prevents **Title**/**Subtitle** corrections. |
| `lock_narrators` | Prevents filling **Narrators**. |
| `lock_authors` | Prevents filling **Authors**. |
| `lock_cover` | Prevents setting a **Cover**. |
| `lock_explicit` | Prevents setting the **Explicit** flag. |
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
4.  It fetches ratings and metadata from the Audible catalog API (falling back to the product pages if needed) and from Goodreads, potentially repairs missing ASINs/ISBNs, and pushes updates to ABS.
5.  If Goodreads search results can't be parsed (layout change), the first such page is saved to `logs/goodreads_search_sample.html` for inspection.
6.  Finally, it sends a notification to Unraid and rotates logs.

---
<sub>Vibecoded: Concept & Logic by the developer, refined with AI assistance.</sub>
