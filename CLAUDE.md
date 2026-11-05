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
  Exception DESCRIPTION (user wish 2026-11-03, built 2026-11-05): Audible's summary replaces the ABS text only if it is provably not an own text: a truncated beginning of Audible's text, or very short (< 30 % and < 300 characters; library: chapter names, "Phantom", converter URLs). Never with lock_description; journaled.
  Exception COVER (user decision 2026-11-04): for a VERIFIED ASIN (same recording: runtime ±2 min + narrator, or ASIN confirmed by this run's search + runtime ±2 min) the Audible cover is set even if ABS has one (UPDATE_COVERS). The old cover is backed up first (cover_backup/), undo uploads it again; lock_cover respected; cover_state.json prevents re-uploading. Without verification no cover at all (also not for empty covers).
  ISBN = the PRINT edition (user decision 2026-11-04: the audiobook is identified by the ASIN). Order: print ISBN-13 of the verified book (Hardcover, same language) -> print ISBN from Goodreads (empty field only) -> LAST fallback the audio-edition ISBN from Audible (same recording, checksum, never the ASIN itself). A wrong ISBN (e.g. an ASIN in the field) with no better value stays as it is (review list).
  Exception ISBN (user decision 2026-11-03): an existing ISBN may be replaced only if it is provably wrong (invalid checksum, e.g. an ASIN in the ISBN field, or it resolves to a DIFFERENT work with title and author contradicting), the book is verified, and a verified replacement exists; otherwise review list. Journaled, lock_isbn respected.
- **Series:** Audible series only if the book has *no* series in ABS; otherwise only fill missing numbers. Never add
  full-cast/dramatized/GraphicAudio/abridged/manga series to a normal edition. Names go through aliases + series index.
- **Titles:** only replaced for messy file-name titles (`The Book of the New Sun 05 Title`, zero-padded numbers only;
  `X: X: Subtitle`; `01 Title`). A new ASIN alone never changes a title (Audible titles carry marketing suffixes).
- **ASIN search:** different volume numbers = always reject. Subtitle never adds containment points. A longer Audible
  title only counts if the ABS title has the number or the extra words are a marketing suffix (`MARKETING_WORDS`).
  Replacing an *existing* ASIN is strict: title ≥0.85, author, runtime (±10 min) and narrator (if known) must match.
- **Marketplaces:** look up ASINs in all stores (com, co.uk, ca, com.au, de, fr, it, es) before calling them broken.
  Preferred store: English → .com, German → .de (swap only with a strict match, otherwise keep).
- **Group confirmation** (`group_confirm`, user idea 2026-11-04): >= 3 ABS books with the SAME title whose candidates each match runtime (to the minute) + narrator and are DIFFERENT volumes of ONE Audible series (distinct numbers, agreeing with ABS numbers of the same series) confirm each other: the shared title is a collection name, the books get ASIN + Audible title (lock_asin/lock_title respected, journaled). Regress round 08 (Realms of War = War of the Spider Queen 1-6).
- **Other edition:** ASIN gone but same title/author exists as another recording → use its ratings, change nothing else.
- **Language mismatch:** title + narrator + series decide whether ABS language or the ASIN is wrong; if nothing decides → change nothing.
- **Only real authors** (user rule 2026-11-04): role credits ("X - illustrator/translator/editor/introduction/foreword/Übersetzer/Herausgeber …", `abs_series.ROLE_CREDIT_RE`) are never taken from Audible as authors and are removed in ABS when a real author remains (REMOVE_ROLE_CREDITS). A plain name counts as role credit when it was seen WITH a suffix somewhere (role_credits.json, learned from Audible) and it is nowhere the first author. lock_authors respected.
- **Empty authors** (user rule): ABS author objects with 0 books are deleted (REMOVE_EMPTY_AUTHORS), re-checked right before the delete; books are never touched.
- **lock_series on EITHER side** blocks a series merge; if the merge would otherwise be allowed it goes to the review list.
- **Interleaving numbers** (user rule 2026-11-04): two series with only LOOSELY similar names (ratio >= 0.8 or one name inside the other) merge when their numbers interleave into ONE gapless run (both sides >= 2 books, sides alternate >= 2 times, e.g. 1-3,7-8,10 | 4-6,9,11-13); same author, same edition, no lock_series still required. Loosely similar without interleaving -> review list.
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
- Live batches: always `REUSE=1` (tools/run_round.sh): without it live answers OVERWRITE existing recordings and the golden replays drift (2026-11-05: batch 13 broke because Audible changed a runtime).
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

## Counting batches (user rule – do not weaken, do not change without the user's explicit OK)
- Every counting batch = **100 NEW library books** (never in an earlier counting batch) + **60 EN + 20 DE synthetic books** (real Audible products NOT in the library, fresh seed each batch). **When no new library books remain (since 2026-11-04): 100 EN + 80 DE synthetic books** (user decision; reusing library books is pointless). Hardcover batches use the same mix (Hardcover ON).
- Books that pass go into `tests/private/golden.json` / `tests/synthetic/` (regress gate). After every rule change the whole golden list is replayed (`tools/regress.sh`).
- Streaks: Audible 4 clean batches in a row, Hardcover 3 clean batches in a row. Any fix resets the streak. Never count a batch with a weaker mix.

## Open items
`docs/STATUS.md` is the single list of ALL open items (task + user requests) with state. Update it after every batch/chain and whenever an item changes; read it before choosing the next task.
