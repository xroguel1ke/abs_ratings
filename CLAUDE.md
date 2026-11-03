# ABS Ratings – project notes for Claude

Maintenance tool for **Audiobookshelf (ABS)**: writes Audible/Goodreads ratings into book descriptions and keeps
metadata clean (ASIN repair, titles, series, authors, genres, language, covers). Owner: German-speaking user
(answer in German, code/comments/commits in English). Runs on **Unraid**.

## Layout
| File | Role |
|---|---|
| `abs_ratings.py` | Main script (CLI, env-configured). Queue → per book: Audible API → metadata sync → Goodreads → rating block → journal. |
| `abs_series.py` | ABS-only task at the end: series merge, author normalization, library report, merge history/review files. |
| `abs_undo.py` | Undo from `changes.jsonl` (`--run`, `--item`, `--field`, `--merge-id`, `--apply`). |
| `webui/` | Flask web UI (`python -m webui.app`, port 47823). `core.py` = settings, RunManager (starts the script as subprocess), scheduler, Apprise, file readers. |
| `Dockerfile`, `.github/workflows/docker.yml`, `unraid/abs-ratings.xml` | Container: image `ghcr.io/xroguel1ke/abs_ratings:<tag>` (`web-ui` branch → `:web-ui`, main → `:latest`). |
| `userscript.sh` | Old way (Unraid User Scripts + temporary container). Still works. |

Data folder (`DATA_DIR`, container `/config` = `/mnt/user/appdata/audiobookshelf/abs_scripts` on the server):
`rating_history.json`, `failed_history.json`, `recheck_history.json`, `goodreads_cache.json`, `hardcover_cache.json`, `series_aliases.json`,
`series_index.json`, `changes.jsonl` (journal), `undone.jsonl`, `results.json` (per-book status for the UI),
`overrides.json` (manual ASIN/Goodreads/ignore), `merges.json`, `merge_review.json`, `merge_decisions.json`,
`settings.json` + `webui_runs.json` (UI), `logs/` (`run_<id>.log`, `debug_latest.log`, `console_<id>.log`),
`reports/` (`planned_changes_<run>.jsonl`, `library_report.html/json`).

## Hard rules (learned from real dry runs on ~2000 books – don't loosen without data)
- **Every ABS write goes through `abs_update()` / `abs_set_cover()`** → journaled (old → new), DRY_RUN only plans.
- **Fill only when empty:** publisher, year, narrators, authors, ISBN (Goodreads ISBN is the print edition), cover, description base.
- **Series:** Audible series only if the book has *no* series in ABS; otherwise only fill missing numbers. Never add
  full-cast/dramatized/GraphicAudio/abridged/manga series to a normal edition. Names go through aliases + series index.
- **Titles:** only replaced for messy file-name titles (`The Book of the New Sun 05 Title`, zero-padded numbers only;
  `X: X: Subtitle`; `01 Title`). A new ASIN alone never changes a title (Audible titles carry marketing suffixes).
- **ASIN search:** different volume numbers = always reject. Subtitle never adds containment points. A longer Audible
  title only counts if the ABS title has the number or the extra words are a marketing suffix (`MARKETING_WORDS`).
  Replacing an *existing* ASIN is strict: title ≥0.85, author, runtime (±10 min) and narrator (if known) must match.
- **Marketplaces:** look up ASINs in all stores (com, co.uk, ca, com.au, de, fr, it, es) before calling them broken.
  Preferred store: English → .com, German → .de (swap only with a strict match, otherwise keep).
- **Other edition:** ASIN gone but same title/author exists as another recording → use its ratings, change nothing else.
- **Language mismatch:** title + narrator + series decide whether ABS language or the ASIN is wrong; if nothing decides → change nothing.
- **Series merge** (`abs_series.merge_check`): names equal after normalization (articles, generic words like Series/Saga,
  format labels like Light Novel) or same word count + ratio ≥0.9; never a name contained in a longer one
  (`Dune` vs `Dune, les origines`), same edition, every book shares an author, no overlapping numbers, complementary
  numbers for non-identical names, no `lock_series`. Uncertain cases → `merge_review.json`; user decisions in
  `merge_decisions.json` (merge / keep / undone / confirmed) always win.
- **Hardcover** (ratings only): series numbers differ between ABS and Hardcover (reading order, part schemes) – a different
  number only rejects when the title is not clearly equal. Search hits need a pure title ratio ≥0.85 (no containment bonus).
- **Goodreads** blocks bots after ~40 requests in a few minutes: interval 10 s, cool-down 900 s ×2, max 300 searches/run,
  URL cache per book, 1 request per book when possible. Never try to bypass the bot protection.
- Respect lock tags: `lock_all`, `lock_metadata`, `lock_asin`, `lock_title`, `lock_series`, `lock_language`,
  `lock_genres`, `lock_publisher`, `lock_year`, `lock_narrators`, `lock_authors`, `lock_cover`, `lock_explicit`,
  `lock_abridged`, `lock_isbn`, `lock_description`.

## Testing
- **Regression gate (mandatory):** every change to matching, series or author logic runs `tools/regress.sh` before it is kept (12 s: syntax, synthetic unit tests `tests/test_authors.py`/`tests/test_series.py`,
  whole-library series+author decisions vs hand-verified baseline `tools/lib_check.py`, all golden rounds replayed from recordings). A deviation is judged by hand:
  wrong -> fix the rule; better -> update golden/baseline with a note. Private data (snapshot, recordings, golden) lives in git-ignored `tests/private/` (repo is public).
- Always start with `DRY_RUN=true`. The single best debugging artefact is `logs/debug_latest.log`
  (settings, full log, all planned changes, report summary, state file sizes).
- Single book: `ONLY_ITEMS=<libraryId>_<itemId>` (web UI: problem page → "Erst als Testlauf").
- Full library dry run: `REFRESH_DAYS=0 BATCH_SIZE=2500 GOODREADS_ENABLED=false` (~3 h).
- Before a real run: ABS backup (`ABS_BACKUP=true`, done automatically by the web UI setting).
- Local unit-style tests used during development mocked `requests`/ABS; there is no test suite in the repo yet
  (ROADMAP). Web UI was tested with a fake ABS (Flask) + Playwright.

## Running
- Container: `docker run -p 47823:47823 -v /mnt/user/appdata/audiobookshelf/abs_scripts:/config ghcr.io/xroguel1ke/abs_ratings:web-ui`
- Web UI without Docker: `pip install -r requirements.txt && CONFIG_DIR=/path/to/data python -m webui.app`
- Script without UI: env vars as in `userscript.sh` / README, `DATA_DIR=/path/to/data python abs_ratings.py`.

## Open work
See `ROADMAP.md`. Next most valuable: Hardcover source (need a real API response first), rating block settings
(sources/order/format), apply-a-dry-run, report/history/undo pages.
