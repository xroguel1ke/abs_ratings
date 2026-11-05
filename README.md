# 🎧 ABS Ratings Updater

![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)
![Platform](https://img.shields.io/badge/Platform-Unraid%20%7C%20Docker-orange)
![Vibecoding](https://img.shields.io/badge/Vibecoded-✨-purple)

A fully automated tool for **Audiobookshelf (ABS)** on **Unraid**. It writes ratings from **Audible** (all regions), **Hardcover** and **Goodreads**
into your audiobook descriptions and keeps the metadata clean (ASIN repair, titles, series, authors, genres, language, covers, ISBN) -
with a web UI, a test-run mode, a change journal and undo for every write. The guiding rule: **0 wrong matches**; anything uncertain goes to the
review list instead of being written.

## ✨ Features

* **Web UI (Docker):** overview, books with certainty score, review list, status/coverage, scans with test run + apply, history with undo, settings, English/German.
* **Hardcover:** ratings and the print ISBN via the GraphQL API (cross-checked against the Audible product; free plan 5000/day or paid plan 50000/day).
* **Search only what is missing:** every source is tracked per book (found -> next refresh, nothing found -> next day, 5 misses or not listed -> pause).
* **Parallel books:** 3 books at once, never more than one request in flight per host.
* **Dockerized Execution:** Runs via a temporary `python:3.11-slim` container using a wrapper script. No dependency hell on your host.
* **Audible Catalog API:** Ratings (overall/performance/story) and metadata come from Audible's JSON API (`.com` & `.de`). Scraping the product pages is only used as a fallback.
* **Goodreads (experimental):** ratings read straight from the search list (no book pages - those trigger the bot block), series name + number confirm volumes; stops at the first block.
* **Auto-Repair & Fallback:** Automatically searches for missing ASINs (Audible search API, verified by title, author and runtime) and updates them in ABS.
* **Title Repair:** Broken, file-name style titles such as `The Book of the New Sun 05 The Urth of the New Sun` or `High Spirits: High Spirits: A Cultural History…` are detected, searched with the clean title and corrected to Audible's title (and subtitle, if empty). Normal titles are never touched.
* **Goodreads Cache:** Once a Goodreads book page was found, its URL is remembered (`goodreads_cache.json`), so refreshes need a single request.
* **Multi-Series Support:** Correctly handles books that belong to multiple series (e.g., "Cosmere" & "Stormlight Archive").
* **Manual Control (Locks):** Use tags in ABS to prevent specific fields (like Series or Language) from being overwritten.
* **Visual Ratings:** Adds "Moon" emojis (e.g., 🌕🌕🌗🌑🌑) for a quick visual overview in your library.
* **Smart Reporting:** Generates JSON reports for missing matches (`reports/`) and maintains a history to avoid redundant API calls.
* **Unraid Notifications:** Sends a status summary (Success/Failures/Duration) directly to the Unraid WebGUI.
* **Rate Limit Protection:** Per-source request throttling, cool-downs and batching to keep your IP safe.

## 🐳 Docker container with web UI (recommended)

A permanent container with a web UI on port **47823**, English by default (German under *Settings → Interface*):

* **Overview** – what needs you, last/next scan, library numbers.
* **Books** – every checked book with a certainty score (0-100), ABS ↔ Audible ↔ Hardcover comparison, reasoning, manual fix (ASIN or link), remove ratings, test/real run per book.
* **Review list** – only real decisions: uncertain ASIN/title/ISBN/Hardcover proposals (accept/discard), series and author merges (merge / merge as … / keep separate), automatically merged series/authors to confirm or undo, fields you changed yourself, duplicates.
* **Status** – coverage of every field with trend, books without a value (linked, one-click re-check), Hardcover budget, Goodreads state, lock report, CSV export.
* **Scans** – tasks as checkboxes (metadata & ratings, authors, series, only real authors, empty authors, remove ratings), test run or real (with optional ABS backup); options for one scan only; live progress. A test run can be reviewed and applied selectively without new requests.
* **History** – every real change, old → new (removed parts red, added parts green), undo per field, book or scan.
* **Settings** – sections with save buttons, schedule (weekdays/time or cron), quick mode, run mode (build-up / maintenance), sources and rating block format, notifications via Apprise, logs, export/import.

Image: `ghcr.io/xroguel1ke/abs_ratings` (`:web-ui` test branch, `:latest` = main), built automatically by GitHub Actions.

**Unraid:** Docker → Add Container → Template URL / or fill in manually:
* Repository: `ghcr.io/xroguel1ke/abs_ratings:web-ui`
* Port: `47823` → `47823`
* Path: `/config` → `/mnt/user/appdata/audiobookshelf/abs_scripts` (your existing folder: history, caches and logs are kept)

Template: [`unraid/abs-ratings.xml`](unraid/abs-ratings.xml). First login: password `admin`, you are asked to change it.
Then enter ABS URL + token under *Settings → Connection*, pick the libraries and save. Start with a **test run**.

Plain Docker:
```bash
docker run -d --name abs-ratings -p 47823:47823 -v /path/to/data:/config ghcr.io/xroguel1ke/abs_ratings:web-ui
```

Planned features and status: [`ROADMAP.md`](ROADMAP.md).

## 🚀 Installation (Unraid) (User Scripts, without web UI)

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
| `BATCH_SIZE` | Books per run, picked at random from the due books of all libraries together. | `300` |
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
| `GOODREADS_INTERVAL` | Minimum seconds between two Goodreads requests. | `10` |
| `GR_BLOCK_COOLDOWN` | Pause in seconds after Goodreads blocks, before retrying. | `900` |
| `GR_MAX_COOLDOWNS` | Cool-downs per run before Goodreads is skipped for the rest of the run. | `2` |
| `GOODREADS_ENABLED` | `false` = no Goodreads requests at all (existing Goodreads ratings in the description are kept). | `true` |
| `GOODREADS_MAX_PER_RUN` | Max Goodreads searches per run (cached books don't count, `0` = unlimited). The rest follows in the next runs. | `300` |
| `SEARCH_PENALTY_SLEEP` | Optional extra pause after an ASIN search. | `0` |
| `RUNTIME_TOLERANCE_PCT` / `RUNTIME_TOLERANCE_MIN` | Runtime difference that counts as another BOOK (both must be exceeded); recording details need 10 min. | `15` / `20` |
| `HARDCOVER_ENABLED` / `HARDCOVER_TOKEN` / `HARDCOVER_PAID` | Hardcover ratings + print ISBN; own daily budget 5000 (free) or 50000 (paid). | `true` / – / `false` |
| `WORKERS` | Books processed at the same time (max. 1 request per host). | `3` |
| `RUN_MODE` / `MAINTAIN_SAMPLE` | `auto`, `buildup` or `maintain` / random due books per maintenance run. | `auto` / `30` |
| `GR_BOOK_PAGES` | Also load Goodreads book pages (off = search list only). | `false` |
| `RATING_ORDER`, `RATING_ICONS`, `RATING_LABELS`, `RATING_POSITION`, `RATING_MIN_COUNT` | Shown sources + order, moons/stars, en/de, top/bottom, minimum ratings. | `audible,goodreads,hardcover`, `moons`, `en`, `top`, `0` |
| `GENRE_RULES` | Lines `Old => New` or `Old => -` for genres from Audible. | – |
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

### Only what is missing is searched (per source)

Every source (Audible, Hardcover, Goodreads) is tracked per book in `source_state.json`:

* **found** – not asked again until `REFRESH_DAYS` have passed.
* **nothing found** – asked again in the next daily run; after 5 misses in a row paused until `REFRESH_DAYS`.
* **surely not there** (the source has no such book at all) – paused at once.
* **blocked / budget used** – no miss, the next run simply tries again.

If Audible is fresh and only Goodreads or Hardcover is missing, a light pass asks only those sources (no Audible request) and adds the
rating to the existing block. **Run mode** `auto`: build-up until every book was checked once, then maintenance (new and changed books always,
plus `MAINTAIN_SAMPLE` random due books). **Change monitoring:** a field the script wrote that shows its old value again (ABS rescan) is written
again; a field you changed yourself afterwards is never overwritten (listed in the review list, optional lock tag).

## 📝 Metadata Rules

Everything is decided automatically. If a match is not certain, nothing is changed (the reason is logged).

* **Runtime check:** If the ABS file duration and Audible's runtime differ by more than `RUNTIME_TOLERANCE_PCT` (15 %) **and** `RUNTIME_TOLERANCE_MIN` (20 min), the ASIN probably points to another edition. The script searches for the right one; if none is found, only the ratings are used and no metadata is changed.
* **Filled only when empty:** narrators, authors, publisher, year, ISBN, cover, description (Audible summary). Recording details (narrators, publisher, year, abridged) only from the same recording (runtime within 10 min).
* **Cover:** set for a verified ASIN (same recording) even if ABS has one (`UPDATE_COVERS`); the old cover is backed up, undo uploads it again.
* **ISBN = the print edition:** from the verified Hardcover book (same language, registration group checked), then Goodreads, the audio ISBN from Audible only as a last fallback; an ASIN in the ISBN field is replaced only by a verified ISBN.
* **Description:** Audible's complete summary replaces the ABS text only if that is a truncated beginning of it or very short (chapter names, placeholders); an own text is never replaced.
* **Narrator = author** (file tags) is replaced by Audible's narrators of the same recording.
* **Only real authors:** illustrators, translators, editors … are removed from the author field when a real author remains (`REMOVE_ROLE_CREDITS`); empty ABS authors (0 books) are deleted (`REMOVE_EMPTY_AUTHORS`).
* **Explicit:** only set to true when Audible marks the product as adult content.
* **Language:** ISO codes are normalized (`eng` → `English`, `deu` → `German`, ...). If ABS and Audible disagree, title, narrator and series decide (if there is no narrator/series to compare, nothing is changed): if they match Audible, the ABS language is corrected; if not (translated title, different narrator, or `lock_language`), the ASIN belongs to another language edition and the edition in the ABS language is searched. If none is found, metadata is left alone.
* **Genres from audible.de** are translated to the English Audible names (`GENRE_LANGUAGE=en`); untranslated names are listed in the library report.
* **Publisher / Year:** only filled when empty in ABS (Audible has the audiobook edition's data).
* **Genres:** only the most specific Audible category of each branch is added (e.g. `Space Opera`, not `Science Fiction & Fantasy`); bare top-level categories (`Literature & Fiction`) only when Audible has nothing more specific. Existing genres are kept.
* **Series:** taken from Audible only when the book has **no series in ABS**. Otherwise only missing numbers of the same series are filled (Audible often lists reading orders, universes and translations as extra series; those are logged, not added). Full-cast/dramatized series are never added to a normal edition. Series names go through `series_aliases.json` and the existing ABS series first.
* **Marketplaces:** an ASIN is looked up in all Audible marketplaces (`com, co.uk, ca, com.au, de, fr, it, es`, configurable with `AUDIBLE_MARKETPLACES`) before it counts as broken.
* **Preferred store:** English books should have an audible.com ASIN, German books an audible.de ASIN. If the ASIN is from another store (e.g. co.uk), the same edition is searched in the preferred store and the ASIN is swapped only if title, author, runtime and narrator match. Otherwise the existing ASIN is kept.
* **Replacing an existing ASIN** requires title, author **and** runtime to match; a different volume number is always rejected.
* **Wrong ASIN detection:** different volume numbers in ABS and Audible title (`Vol. 9` vs `Vol. 18`) or a large runtime difference trigger a search for the right ASIN; if none is found, metadata is left alone.
* **Title:** only corrected for broken, file-name style titles (see Title Repair). A new ASIN alone never changes the title.

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
| `lock_asin` | Keeps the **ASIN** (no replacement search; if Audible's data doesn't fit the book, only ratings are used). |
| `lock_metadata` | All metadata locks at once: only the **ratings** are updated. |
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
