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
| `Dockerfile`, `.github/workflows/docker.yml`, `unraid/abs-ratings.xml` | Container: image `ghcr.io/xroguel1ke/abs_ratings:<tag>` (only `main` is built → `:latest`; since 2026-11-09 all work happens on main, the claude/* branches were merged and deleted). |
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
  Glued subtitle (library 2026-11-09, 49 books checked by hand): ABS title = Audible title + ': ' + Audible subtitle AND the ABS subtitle
  field is Audible's (shown twice), a series label of a series the book is not in, or empty with the subtitle twice in the title
  -> Audible's title (`glued_subtitle`). 'Mistborn: The Final Empire' without subtitle field stays.
- **ASIN search:** different volume numbers = always reject (also an ABS series number: same title + runtime ±2 min + narrator is NOT
  enough to ignore it - The Hunter's Code 1 and 2 share title and narrator and differ by 2 min, regress reg_syn_21).
  ONLY exception (user wish 2026-11-08): the ABS number is PROVABLY wrong - same recording (identical title, runtime ±2 min, a shared
  narrator) AND Audible's own series list (relationships) has no other VOLUME with this title (same title + same number = another
  edition, fine) AND the ABS number belongs to another title there or does not exist. Then the ASIN is taken and the ABS number is
  replaced by Audible's (Discworld #11 -> The Light Fantastic #2, Heritage of Shannara #15 -> #2). No series list -> no proof. Subtitle never adds containment points. A longer Audible
  title only counts if the ABS title has the number or the extra words are a marketing suffix (`MARKETING_WORDS`).
  Replacing an *existing* ASIN is strict: title ≥0.85, author, runtime (±10 min) and narrator (if known) must match.
- **Marketplaces** (user rule 2026-11-08): English books ask audible.com FIRST, German books audible.de (1 request). Not there -> strict
  search in that store first; only if it finds nothing are the other stores asked for the current ASIN (in parallel) - kept as fallback
  (after the usual plausibility/runtime checks); dead in every store -> search all stores. A product every store answers as 'not
  available' is gone (no page scraping). The Audible API throttle is per marketplace host (AUDIBLE_API_INTERVAL, default 0.5 s).
- **Group confirmation** (`group_confirm`, user idea 2026-11-04): >= 3 ABS books with the SAME title whose candidates each match runtime (to the minute) + narrator and are DIFFERENT volumes of ONE Audible series (distinct numbers, agreeing with ABS numbers of the same series) confirm each other: the shared title is a collection name, the books get ASIN + Audible title (lock_asin/lock_title respected, journaled). Regress round 08 (Realms of War = War of the Spider Queen 1-6).
- **Other edition:** ASIN gone but same title/author exists as another recording → use its ratings, change nothing else.
- **Language mismatch:** title + narrator + series decide whether ABS language or the ASIN is wrong; if nothing decides → change nothing.
- **Only real authors** (user rule 2026-11-04): role credits ("X - illustrator/translator/editor/introduction/foreword/Übersetzer/Herausgeber …", `abs_series.ROLE_CREDIT_RE`) are never taken from Audible as authors and are removed in ABS when a real author remains (REMOVE_ROLE_CREDITS). A plain name counts as role credit when it was seen WITH a suffix somewhere (role_credits.json, learned from Audible) and it is nowhere the first author. lock_authors respected.
- **Empty authors** (user rule): ABS author objects with 0 books are deleted (REMOVE_EMPTY_AUTHORS), re-checked right before the delete; books are never touched.
- **lock_series protects the BOOK** (user rule 2026-11-08, was: lock on either side blocks): if all locked books sit in ONE series of a
  merge group, that series becomes the target and only unlocked books move; locked books in two or more series -> review list.
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

- **Per-source search state** (user rule 2026-11-04, source_state.json): found -> REFRESH_DAYS; nothing found -> next day, 5 misses -> pause;
  surely not listed (0 hits) -> pause at once; blocked/deferred = no miss. Rating-only passes ask only the missing sources (no Audible request).
- **Parallel books** (WORKERS=3): every per-book global must be thread-local (_TLDict: LAST_MATCH, LAST_SEARCH, SEARCH_CTX, LOOKUP_HITS, GR_CTX,
  HC_CTX, stats per worker). A shared one produced wrong ASINs/ratings twice (2026-11-04/05) - never add a new module-level per-book dict.
  Max 1 request in flight per host; same author in one worker. Regress must be identical with 1 and 3 workers.
- **Hardcover:** a main-title hit beats an alternative-title hit; number rules (only numbers < 1000, Hardcover MAIN title, ABS title vs Hardcover
  title, Hardcover title = series name without number, 'Part N', alternative-title hit of another volume for English books). Hardcover counts
  every search FIELD of a request against its limits - never combine lookup steps in one request. Contributors with a role are learned as role names.
- **ISBN:** registration group must fit the book language (978-0/1, 979-8 English; 978-3 German ...).
- **Goodreads** (user decision 2026-11-08): book page first (robots.txt allows /book/show, /book/isbn; disallows /search), max GOODREADS_PAGES_PER_RUN=3 pages
  (challenge after 3-7); a challenge on a book page only switches pages off for the run, the search list stays as FALLBACK (max 25 searches,
  block after ~100-150 in a row); series name + number + author confirm. AutoThrottle: jitter, slow answer widens the interval, Retry-After wins.
  No browser (image size). Browser signature via curl_cffi only as opt-in GR_IMPERSONATE (user decision 2026-11-08, default off).
- **Change monitoring** (user decision 2026-11-08, was: a NEW value is the user's and never overwritten): ONLY lock_<field> tags protect a
  field. A field the script wrote that shows any other value (ABS rescan or a manual edit - ABS cannot tell) makes the book due again and
  the normal rules decide. List fields compare as sets (ABS re-sorts the listing). manual_fields.json is no longer written or used.
- **Series names** (user rules 2026-11-08, same recording with both runtimes known): a non-Latin series name (English/German books) is
  replaced by Audible's series (none -> review list); a series that is only a folder of the book path ('Elolaivat') is replaced when
  Audible's series exists in the library; an Audible series that exists EXACTLY so in the library for this author is added even if the
  book has other series (Way of Kings back into The Stormlight Archive). A series number shared by two or more different books is no
  evidence: it never rejects an ASIN candidate and is replaced by Audible's number.
- **ABS forced rescan 2026-11-07** (precedence absMetadata last, storeMetadataWithItem=false -> stale August metadata.json in the book
  folders) reverted ~1900 books (1261 ASINs to dead ones, Japanese/folder series). No backup restore (user): the rules above fix it per
  book; every changed book is a 'manual update' in the queue.
- **After the first full run 2026-11-08** (user OK "so vorgehen wie sinnvoll"):
  - Shared title (another library book of the same author has it AND its runtime would also fit the candidate): only runtime ±2 min
    accepts (Mushoku Tensei 21/23 got the neighbour volume at 27-37 min off; 'Threshold' by two authors and Judicator Jane 1 vs 6 stay normal).
  - `folder_volume_conflict`: shared title + the book folder ('2 - Solo Leveling, Vol. 2 - ...') names another title with a number that
    neither the ABS title nor the ABS number has -> ABS holds another volume's data: strict search with the folder title (±2 min), ABS
    number ignored; a hit (or the same ASIN found again) replaces title (Audible's, or the folder's if Audible's is the same ambiguous one)
    and number (folder number when Audible counts parts and the title agrees).
  - Twin listings must not contradict the wanted volume (Vol. 21 is no twin of the bare-titled vol. 22).
  - Non-Latin series: the same TITLE is enough (not the same recording); Audible lists no series -> removed; Audible's series already in ABS -> removed.
  - A series number held by 3+ different books is junk: removed when Audible's series of the same recording is added.
  - Merge: a non-Latin name is never the target for English/German books (locked books in it -> review list); spellings that differ
    only in upper/lower case are ONE name for ABS: the apply step empties them first, then writes the target; another case-variant
    with locked books -> review list. '&' = 'and' in series keys.
  - Reverted values (exactly the value before our change, same ASIN, no lock): group confirmation / review decisions / title cleanup
    are written again (`reapply_reverted`); merges and author normalization redo themselves at the end.
  - An ABS number that equals the number of ANOTHER series of the same book (Fitz and the Fool #16 = Realm of the Elderlings #16) is
    replaced by Audible's (same recording). Audible's series number that contradicts its own title AND the ABS title ('Vol. 8' vs #2) loses.
  - Refresh 0 days = one complete pass also in maintenance mode. ETA = speed of the last 150 FINISHED books blended with the run average
    (was: elapsed / queue index at the start of a book -> jumped between 4 s and 40 h with parallel workers).
  - Foreign series names in LATIN script (French/Spanish/... function words, abs_series.foreign_series_name) on English/German books are
    replaced like non-Latin ones, but kept when Audible lists the same name for the book; Audible lists no series -> review list (only a
    non-Latin SCRIPT is removed automatically). A number held by 3+ different WORKS of one language (distinct_works: contained titles =
    one work) is junk: Audible's number, else the book folder number when the series is the folder, else removed.
- **Series cleanup** (series_cleanup, user findings 2026-11-08, same book = same title or same recording): with Audible's list of the SAME
  book an ABS series Audible does not list is removed when the book is in another Audible/folder series and it has <= 2 books or is foreign
  (replaced instead when it is spelled like an Audible series the book lacks; an own sub-series like 'Hoid's Travails' stays); an only
  series of <= 2 books is replaced by Audible's; a single-book series named like ANOTHER author's series goes; a variant spelling becomes
  the library's spelling of Audible's series; folder + Audible series is added. German books keep German-looking series names.
- Listing seriesName is split at ' #N' (names contain commas). listing_value() keeps strings (iterating gave '' -> every series write
  was 'changed outside' in the next run). Review entries a later run no longer raises are closed (review_sweep).
- Authors (same recording, runtime 2 min): swapped authors/narrators and several Audible authors glued into one ABS author are fixed.
- Round 6/7 (2026-11-08): an Audible sequence 'N, Subtitle' ('1, The Stolen Heir' in The Folk of the Air) is a sub-series position = no
  number. The listing seriesName glues an UNNUMBERED leading series to the next ('Elfhame, The Folk of the Air #1'): split via the ABS
  series list (LIB_SERIES_NAMES). A novella range ('0.1 - 0.5', span < 1) containing the ABS number is no conflict; box sets ('1-3') are.
  Audible '#P.V' for 'Part P, Volume V' is no conflict when both titles name that part + volume. Narrator/author keys fold accents.
- Round 7 (2026-11-09): an ABS narrator of ANOTHER recording is replaced when the book folder AND Audible's same recording (runtime
  2 min) name the same other narrator; a folder narrator contradicting ABS drives the ASIN search with runtime ±2 min (Fool's Errand
  flipped between two ASINs). Folder narrators = person names only ('09 - Volume 09 - The Town of Strife II' is a title). Title
  'YYYY - Title' = file-name title. ISO language codes ('eng') are spelled out for every book, also without Audible data.
- 2026-11-09 (user): BOOK_LANGUAGES = the library's languages (user: English, German): an Audible product in another language is never
  taken (neither metadata nor ratings), an ABS language outside it counts as unknown and is never spelled out; a series never mixes
  languages (non-English book -> Audible's series of its language); library task sets a book to its series' language (90 %, shared
  narrator); unresolved -> review list 'wrong language'. Source health (source_health.py): canary per source + breaker. Missing volumes
  (missing_series.py, reports/missing_in_series.json, page /missing). Test containers run with --cpuset-cpus=0-9,12-31 (core 20 defect).
- **Narrator = author** in ABS is replaced by Audible's narrators of the same recording (runtime 2 min) only.
- 2026-11-09 evening (bug hunt on the live library):
  - Rating block frame '─── Ratings & Infos ───' ... '───────────────' (the '⭐' frame looked like a rating with yellow stars; the old
    frame is still recognised, its END must be a line of its own - the old regex stopped at the first yellow star -> doubled blocks).
  - A rating block that vanished from the description (ABS rescan reloads the old text; description is not in last_written) makes
    the book due again (`block_lost`).
  - ASIN not released yet (release date in the future, re-release): strict search for the released edition, else its ratings.
  - Series number conflict only when NO candidate entry of the same series agrees ('Dune' #1 + 'The Dune Sequence' #12, both key 'dune').
  - Hardcover cross-check: both titles carry a volume number and share none -> another volume (rejected).
  - Our own later writes (apply, symbol rewrite, library task; journal ts) are no 'manual update' (the symbol rewrite would have
    re-queued all 1917 books). A value the user UNDID is never written again by the script (`_UNDONE_VAL`), unless the user applies it.
  - Undo skips a field that changed again since (newer run / by hand) - `abs_undo.py --force` to override. abs_apply: the same field
    twice in one test run applies the final value.
  - Web UI e2e tests against the fake ABS: tests/test_webui_e2e.py (rename + undo, lock, symbol rewrite, remove ratings, apply selection,
    undo conflict, no inherited alert).
- 2026-11-09/10 library audit rounds 1-10 (user: 'the first series I looked at was wrong'):
  - Packs: Audible's range is the number ('1-2', also replacing a first-volume number); the audio file's own tags naming another volume
    drive a strict search (Arch Magus 'Pack 1' file = Pack 3); a title that is only '<series> #N' / has the series name appended /
    '(Unabridged)' / zero-padded numbers / 'Library Edition' -> Audible's title (same recording); subtitle = title -> Audible's or none.
  - Same-title volumes with different ASINs count as different books for a shared number; '04' -> '4'; a future or non-year publishing
    year -> the recording's year; Audible's subtitle '<series>, Book N' fixes only an OUTLIER number (#100 next to #1, #2).
  - Audible series named like the book's folder (same edition) is added; an unnumbered series Audible does not list whose books all sit
    in that folder goes. Descriptions that are Audible's summary of ANOTHER book are replaced (summary_index.json).
  - One spelling per narrator (library's most used); swapped author/narrator match crosswise in the search; author-only narrator is no
    evidence. ISBN of another language group or shared with another book counts as wrong (only a verified one replaces it; the book whose
    language fits the group keeps it). Genres: glued lists split, 'Audiobook'/'General' removed.
  - Chapters (MATCH_CHAPTERS, lock_chapters): Audible's chapters (Audnexus) for poor chapters (none, one, only numbers on both sides is
    skipped, > 30 s past the end) when the file length matches to 5 s, the file's own marks match Audible's lengths, or Audible's intro/outro
    explain up to 2 min. Journal keeps chapter start/end/title; undo/apply support it.
  - tools/regress.sh runs at most 10 test containers at a time (REGRESS_JOBS); ~100 parallel containers disturbed the server.
  - ABS_BACKUP: at most one ABS backup per ABS_BACKUP_HOURS (24); 6 run backups a day had pushed every older one out of ABS's keep-limit.
  - Moved-item guard (item_paths.json): ABS 2.x's folder watcher gives a NEW folder an existing entry when the inode matches, without
    checking that the entry's own folder is gone; Unraid /mnt/user (shfs) inodes are not stable, so stale stored inodes collide (Ambassador's
    Mission got the 'Orb of Cairado' entry, 2026-11-10). An entry whose path changed while neither title nor author word is in the new path
    is held back (not processed), goes on the review list ('moved') and a library scan restores the other folder's entry. Released when it
    fits its folder, the review is decided, or an 'identity' override exists. A confirmed other-book ASIN (identity) now also replaces
    publisher, year, genres, ISBN and the cover.

## Testing
- **pytest:** `tools/pytest_all.sh` (one process per test file; also step 2a of the gate).
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
 . Web UI was tested with a fake ABS (Flask) + Playwright.

## Running
- Container: `docker run -p 47823:47823 -v /mnt/user/appdata/audiobookshelf/abs_scripts:/config ghcr.io/xroguel1ke/abs_ratings:latest`
- Web UI without Docker: `pip install -r requirements.txt && CONFIG_DIR=/path/to/data python -m webui.app`
- Script without UI: env vars as in `userscript.sh` / README, `DATA_DIR=/path/to/data python abs_ratings.py`.

## Open work
All STATUS items are done. See docs/STATUS.md. Next: the user's first real
full run (test run first, apply selectively), done: merged into main 2026-11-09.

## Counting batches (user rule – do not weaken, do not change without the user's explicit OK)
- Every counting batch = **100 NEW library books** (never in an earlier counting batch) + **60 EN + 20 DE synthetic books** (real Audible products NOT in the library, fresh seed each batch). **When no new library books remain (since 2026-11-04): 100 EN + 80 DE synthetic books** (user decision; reusing library books is pointless). Hardcover batches use the same mix (Hardcover ON).
- Books that pass go into `tests/private/golden.json` / `tests/synthetic/` (regress gate). After every rule change the whole golden list is replayed (`tools/regress.sh`).
- Streaks: Audible 4 clean batches in a row, Hardcover 3 clean batches in a row. Any fix resets the streak. Never count a batch with a weaker mix.

## Open items
`docs/STATUS.md` is the single list of ALL open items (task + user requests) with state. Update it after every batch/chain and whenever an item changes; read it before choosing the next task.
