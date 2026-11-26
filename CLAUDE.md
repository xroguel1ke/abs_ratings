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
| `abs_verify.py` | Stand-alone verifier (2026-11-22): "is this downloaded file really book X?" for the Unraid Shelfmark auto import. Only CALLS the identification functions (search_audible_api, audible_chapters, match_chapters, volume_conflict); never writes, needs its own DATA_DIR. Changing those functions changes its verdicts: re-run the server test loop (/mnt/user/appdata/abs_verify/verify_loop.py, 0 FALSE_OK). |
| `webui/` | Flask web UI (`python -m webui.app`, port 47823). `core.py` = settings, RunManager (starts the script as subprocess), scheduler, Apprise, file readers. |
| `Dockerfile`, `.github/workflows/docker.yml`, `unraid/abs-ratings.xml` | Container: image `ghcr.io/xroguel1ke/abs_ratings:<tag>` (only `main` is built → `:latest`; since 2026-11-09 all work happens on main, the claude/* branches were merged and deleted). |

Data folder (`DATA_DIR`, container `/config` = `/mnt/user/appdata/audiobookshelf/abs_scripts` on the server):
`rating_history.json`, `failed_history.json`, `recheck_history.json`, `goodreads_cache.json`, `hardcover_cache.json`, `series_aliases.json`,
`series_index.json`, `changes.jsonl` (journal), `undone.jsonl`, `results.json` (per-book status for the UI),
`overrides.json` (manual ASIN/Goodreads/ignore), `merges.json`, `merge_review.json`, `merge_decisions.json`,
`settings.json` + `webui_runs.json` (UI), `logs/` (`run_<id>.log`, `debug_latest.log`, `console_<id>.log`),
`reports/` (`planned_changes_<run>.jsonl`, `library_report.html/json`).

## Hard rules (learned from real dry runs on ~2000 books – don't loosen without data)
- **Every ABS write goes through `abs_update()` / `abs_set_cover()`** → journaled (old → new), DRY_RUN only plans.
- **Fill only when empty:** publisher, year, narrators, authors, ISBN (Goodreads ISBN is the print edition), cover, description base.
  Exception DESCRIPTION (user 2026-11-16, replaces the rule of 2026-11-03): Audible's summary of the verified recording wins when it has
  >= DESC_MIN_LEN (200) characters in the book's language; else Hardcover's description (sure match: ASIN/ISBN/manual or title >= 0.85,
  >= 200 characters, book's language); else the existing text stays - emptied only when it is no description at all (only reviews,
  only a link, only a credit: description_junk). A foreign-language text with no replacement goes to the review list. lock_description
  keeps any text; journaled. Text comparison is text-only (_sum_norm: Audible's <p> vs ABS's <br> is no difference).
  GENRES (rework + 14 check loops 2026-11-16, user: Adventure everywhere, Science Fiction on fantasy, too few genres): with a verified
  Audible product the book step rebuilds the genres from Audible's category LADDERS (abs_series.rebuild_genres): a ladder's main genre is
  its first real level (umbrella nodes skipped, weighted), leaves get context (GENRE_CTX: Fantasy>Epic = Epic Fantasy, Fantasy>Action &
  Adventure = nothing), existing specific genres stay, generic ones only from Audible. Fantasy vs Science Fiction (Fantasy library):
  Fantasy is never dropped when Audible names it; an off-genre SF goes (generic/bare/single ladder next to strong Fantasy; Steampunk and
  Alternate History are no SF evidence; Cyberpunk/Dystopian/Post-Apocalyptic are LitRPG stand-ins on LitRPG books); an old Fantasy only
  goes for REAL Audible science fiction; Hardcover readers decide when clear (>= 5 votes, twice the other: Children of Dune).
  The series task (series_genre_fill) then, in rounds until nothing moves (ONE run): the series check (a volume off its series' big genre
  on weak evidence loses it - judged from Audible's static data, remembered in genre_series_drops.json; 'Classics' off the series goes),
  the fill from the series' majority (never generic, sub-genres only with their main, Fantasy/SF only for books with neither), and the
  book step's rules again (genre_ladders, genre_hc). Origin/format names (German literature, Novella ...) are no genres.
  Check loop tools: tests/private/loop + scratchpad genre_loop.sh (simulation from the pre-rework state, 3 passes, all checks).
  NEW BOOKS WITH BAD METADATA (test 2026-11-16, tests/private/newbooks: 18 real books reset to 'just imported' with file-name titles, no
  ASIN/narrator/tags; make_adv.py: 52 adversarial variants - wrong runtime / author / volume title - must get NO ASIN): file junk
  ([Audiobook], [2022], 64kbps, mp3, '_') is stripped from the search title (format labels like '(ungekürzt)' stay: recordings);
  a candidate's own author / narrator names are taken out of the ABS title when comparing (a narrator in the title is narrator
  evidence); a book WITHOUT ASIN and WITHOUT narrator is confirmed by title >= 0.95 + author + runtime +-2 min of a 2 h+ recording;
  the Hardcover edition ASIN path needs +-2 min too without a narrator (it took 'Caliban's War' on another volume's file); an ASIN
  found via Hardcover gets a second pass in the same run; a new book keeps the new-book rules until matched (new_pending.json);
  Hardcover description / series / genres need a matching author for a title-only search hit. Result: 3/18 -> 14/18 complete in one
  run, 0/52 false positives; books without any author are left alone on purpose.
  REALISTIC NEW BOOKS (2026-11-17, tests/private/newbooks/make_real.py: 100 library books reset to what ABS makes of the real file tags +
  folder 'Author/Series/N - Title - Narrator'; eval_real.py): 87 -> 92/100 the library's ASIN, the other 5 the same recording as another
  .com publisher listing, 0 wrong books. Fixes: (1) a book WITHOUT an ASIN may take a hit with the same narrator and the runtime off by
  <= 20 min AND <= 3 % only with narrator + series/volume number + title >= 0.95 (near_only; a long title is no evidence - regress had
  'Homestead Recipe Book' for '... Garden'; 'Homeland' 30 min off stays unmatched as in golden); (2) preferred_twin: INTO the preferred
  store one rating is enough (the 'at least as many ratings' guard of 2026-11-04 applies within a store); when audible.com's API search
  does not list the edition (region-locked US editions: Golden Fool, Blood of Dragons, Titanchild) ONE web search of that store with a
  plain UA + overrideBaseCountry (the script's full browser headers get another result list!), a miss is remembered 180 days in
  twin_web_misses.json (user: web search only as the last resort, another store is fine then); (3) 'ABS number provably wrong' is not
  applied when Audible's number belongs to another library book of that series (own reading order: The Heroes = First Law World #5);
  (4) an unnumbered Audible series that the library numbers gets no number borrowed from the folder series (Witchwood Crown was MS&T #1).
  DESCRIPTION ADS (user 2026-11-17, 'very carefully'): strip_promo_lead() on Audible's summary - a first paragraph or a bold lead whose
  EVERY sentence matches PROMO_SENTENCES (Brought to you by X / NYT-WSJ bestseller / now an original series / over N million sold /
  inspiration for the Netflix series), <= 250 chars, >= DESC_MIN_LEN left. Dry run on the library: 58 of 2043 texts, all pure ads, 0 texts
  altered after the lead. Quotes, taglines and mixed sentences are never touched (tests/test_desc.py).
  FOLDER RENAME (2026-11-17, user): 2101 book folders of Audiobooks + Non-Fiction renamed to 'NN - Title {Narrator, Narrator}' (> 3 narrators:
  {Full Cast}; ABS number unless checked otherwise; Graphic Audio untouched). ABS kept every item id (same inode). meta_fingerprint now
  uses only the cover's FILE NAME (a renamed folder changed coverPath -> 'manual update'); stored legacy fingerprints still match.
  Series numbers: ABS #x is replaced by Audible's #y only when Hardcover's featured series names the same #y and no other library book
  of the series holds it (hc_agrees_number) - Fractal Noise stays 0.5 (Hardcover 0.5 = in-world order, Audible 2 = publication order).
  GENRE PING-PONG (log check 2026-11-17): the series step checks ALL books but knew Audible's ladders only for the books of the
  current run - every scheduled 25-book run removed Classics / Fantasy / SF that the last full run had set (16 books flipped). Now:
  ladders persist in genre_ladders.json; without ladders a book loses nothing in the series check; when Audible names BOTH Fantasy and
  SF and the series says the other one, the series check swaps (only if rebuild_genres keeps the swapped genre - SAO's 'SF' is a LitRPG
  stand-in). Simulation full/partial/full/partial: 0 changes after the first full run (scratchpad genre_sim_runs.py).
  FULL CAST (user 2026-11-17): 5+ narrators (3 or 4 are no full cast); folder names follow the same rule.
  HARDCOVER METADATA (user 2026-11-16): fewer than GENRE_MAX genres / 6 tags (rating + lock_* tags do not count) -> filled up from
  Hardcover's reader-voted genres (top genre >= 10 votes, genre >= 5 votes and >= 30 % of the top; cleaned like Audible's: GENRE_RULES, canonical names, no
  duplicates, themes -> tags, Epic rule; only names the script knows; at most ONE main genre and only if the book has none; no
  'Graphic Novel'; no foreign names on English books). Hardcover's 'Tag'/'Mood' categories are opinions: not used. Added genres join
  AUD_GENRES (never 'rare'). An EMPTY series is filled from Hardcover's featured series when Audible has none (not when it is named
  like the book). Publisher/cover never from Hardcover (print edition).
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
  NEW books (not in history yet, user 2026-11-15 - file-tag titles like 'Book 20 - Volume 07 - Part 06 - Memory's Flame - The Wandering
  Inn Series'): the verified recording's Audible title (`verified_recording`: runtime ±2 min + narrator, or without an ABS narrator the
  author; or ASIN found by search + narrator + runtime in tolerance). Format labels '(Unabridged)'/'(Light Novel)' go; title + Audible
  subtitle glued stays unless the subtitle is a series label ('Book N'); Audible's marketing tail without a number is never added;
  a missing subtitle comes from Audible. Authors of a new verified book in another script / with swapped name order -> Audible's spelling.
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
- **Goodreads background mode** (user decision 2026-11-18, replaces the per-scan Goodreads settings below for the web UI): in Nov 2026
  Goodreads blocked after 2-7 requests of a scan's burst. Scans started by the web UI ALWAYS run with GOODREADS_ENABLED=false
  (core.script_env; existing Goodreads lines of a rating block stay). Goodreads is asked only by ONE long-running process
  (webui core.GoodreadsBackground -> abs_ratings.py with GR_BACKGROUND=1 -> goodreads_background()): one due book (never asked
  first, then the oldest) via rating_only_pass, then GR_MODE_MINUTES (+-20 %); at most GR_MODE_DAILY a day; a block pauses the
  mode GR_MODE_PAUSE_MIN, doubling per block in a row up to 6 h (goodreads_mode.json, survives restarts). RunManager.start()
  stops it (SIGTERM, the current book is finished) before ANY other script; it restarts when no script runs. Settings: Sources ->
  Goodreads (on/off, minutes, per day, pause); overview card '#grmode'; onboarding step 'Two separate modes'.
  In a scan, a block now pauses Goodreads for ALL workers (GR_STATE['cool_until']; log check 2026-11-18: the other workers kept
  asking into the block).
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
    explain up to 2 min. User 2026-11-24: a file WITHOUT chapters (none or one) of a verified recording takes Audible's chapters as they
    are when the length differs by <= CH_LOOSE_SECONDS (60 s; starts past the end dropped). Journal keeps chapter start/end/title; undo/apply support it.
  - tools/regress.sh runs at most 10 test containers at a time (REGRESS_JOBS); ~100 parallel containers disturbed the server.
  - ABS_BACKUP: at most one ABS backup per ABS_BACKUP_HOURS (24); 6 run backups a day had pushed every older one out of ABS's keep-limit.
  - Search fallbacks for messy file tags (2026-11-15): ABS title = Audible SUBTITLE ('The Half-Elf Demigod Part I' = Overlord 15);
    the book folder's title ('13 - The King's Secret - Narrator') when nothing was found (runtime ±2 min without a narrator); the ABS
    author being the candidate's narrator (author/narrator swapped or narrator in the author field) counts as author + narrator.
    Cover of a new book is also replaced when verified by author + title + runtime without a narrator tag; a kept cover is logged.
  - Series task `drop_duplicate_series`: one series twice on a book under two names with the same number - punctuation only, or one
    name's full words start the other's AND every book of the dropped name holds the kept one AND the kept one has own books.
    (First Law Trilogy / Mistborn Era 1 / Irwin's Journey stay - a fake-write test caught the first, looser version.)
  - Dashboard selection runs ('only new') set FOLLOW_UP_TASKS: series task + missing volumes run afterwards (they were skipped).
  - tools/run_round.sh: FOLLOW_UP_TASKS, FAKE_WRITE=1 (writes ONLY into the fake ABS: tests the library task after per-book changes).
  - Series labels (user 2026-11-15): a subtitle that only names the book's OWN series + its own number goes ('Discworld, Book 1',
    'Book 18 of the Spellmonger Series', '... (Defiance of the Fall, Book 5)' -> the rest) - last step per book, so no rule sets it again.
    Kept: other number, a sub-series the book does not hold ('Legend of Drizzt: Dark Elf Trilogy'), ranges. Series task
    harmonize_series_titles: 'series + number' titles take the form 3 of 4 (min. 4) of their series use. Library check 2026-11-15:
    806 subtitles + 9 titles, a fake-write full run vs. the old code differed ONLY there; a second run changed nothing back.
    Also per book (verified recording): a subtitle with the number of ANOTHER volume (Mushoku 5 with 'Vol. 20', System Collapse #7 with
    'Book 8') -> Audible's subtitle (blurbs, 'Light Novel' -> none); kept when ABS = Audible (sub-series numbers: Legend of Drizzt).
    Title = series name + subtitle 'Book N: X' (Dragon Heart, also 'Dragon Heart: Book 14') -> title X; not for taglines/labels.
  - Field loop 2026-11-15 (title/subtitle/narrators): last title step strips '(Light Novel)' anywhere and moves a genre tagline
    (': A LitRPG Adventure') to the subtitle; glued-subtitle cut never leaves only the series name ('Mushoku Tensei'). Subtitles:
    word/roman numbers and '(Discworld Novel 7)' count as own-series labels, marketing lines / edition labels / foreign text on an
    English book / a subtitle repeating the title go. Series task split_joined_narrators: 'A + B' narrator entry -> A, B.
  - Field loop 2026-11-15 (series/publisher/genres/description/chapters): a per-book Audible series that is only a SHORTER name
    (or an edition-marker variant) of a same-numbered series of the book is not added (covered_by_series; Mynoghra / Wax and Wayne /
    Pern publication order / Witcher Full-Cast were added and removed every run); '(Unabridged)' cut from Audible series names;
    series task tidy_series_entries (trailing spaces, lone ')', '01' -> '1'). English recording with a foreign/comic edition's
    publisher -> Audible's. Genres: comma lists split only when every part is a library genre, 'Ebooks', 'place : publisher' and
    foreign genres on English books go. Literal '\\n' in descriptions -> line break. Chapters ending > max(5 min, 3%) before the file
    are poor; Audible's are taken only when they cover the file (+-5%).
  - User decisions 2026-11-15: co-authors from Audible are added (same recording, the person has own books in the library: no pen
    names); '(Light Novel)' / ' Light Novel' leave series names (strip_series_format: Audible parse, canonical_series, series task);
    an ASIN in the ISBN field is removed; publishedYear = the BOOK's first release (Hardcover release_year, cross-checked, never after
    Audible's release); GENRE_MAX (web UI, 10): Audible's genres first, then the library's most used, single-book genres go (stable in
    one run; full books get no Audible genre re-added); number_shared_titles: 2+ volumes titled like (part of) the series name ->
    'Title, Vol. N'; shared descriptions (series task -> shared_descriptions.json) and wrong-language texts take Audible's summary;
    a subtitle with no word in common with the book (title, series, authors, Audible, description) = another edition's -> Audible's.
  - Test tools pick a free fake-ABS port (run_round: FAKE_PORT unset = free one, set but busy = abort; watch/source-state tests) and
    refuse RECORD_NO_LIVE=1 (it is a host list).
  - Genres and tags (user 2026-11-15): GENRE_MAX 6; one name per genre without '&' (Action & Adventure -> Adventure, Thriller,
    Short Stories, Graphic Novel; Sword & Sorcery stays); umbrellas go (Science Fiction & Fantasy, Literature & Fiction, Fiction);
    'Epic' next to 'Epic Fantasy' goes, 'Epic' alone stays (per book: not re-added next to Epic Fantasy); themes are English TAGS
    (_THEME_TAG: Dragons, Magic, Time Travel, Tie-In ...); order: main genre, specific ones, audience, broad (Adventure) last.
    manage_tags: genre copies leave the tags, lock_* and own tags stay, rating tags: only the HIGHEST level per source (Audible 4.5+ / 4.7+ / 4.9+, Hardcover 4.0+ / 4.2+ / 4.4+, 50+ ratings each), 'Full Cast' only when marked (full cast / dramatized /
    Hörspiel / GraphicAudio) or 5+ narrators, 'Audible 4.5+' from the rating block (>= 4.5 with >= 50 ratings, removed when lower).
  - refresh_own_fingerprints (end of every real run): books this run wrote get the fingerprint of their new state - otherwise the
    next run took our own genre/tag writes for manual updates (1884, 77 min on 2026-11-15).
  - ABS cron scans (every 30 min) that overlap a run overwrite our writes from the audio tags (cleared subtitles came back):
    wait_for_abs_scan before the book step and the series task, scan_gate (every 30 s, all workers) before books and library writes.
    The local ABS host is no longer serialized (ABS_PARALLEL=3); the per-host lock stays for Audible / Hardcover / Goodreads.
  - Web UI time left 2026-11-16 (webui/core.py _tail_eta): includes the tasks after the books - the time the last finished run of
    the same kind spent between its last book and the missing-volumes step, plus missing volumes (30 s + 1.3 s per expired Audible
    series list; its '… n/m series checked' lines refine it). The bar shows the whole run; a phase label replaces the book title.
  - Subtitles 2026-11-16: ABS fills an EMPTY subtitle from the audio file's subtitle tag at EVERY library scan (not only when a scan
    overlaps our writes). A subtitle that the file has as tag is never cleared (file_tag_subtitle) - else the same 41 ping-pong forever.
    Removing a tag from the FILE is not enough: ABS's AudioMetaTags.isEqual only compares tags still present, so the DB keeps the old
    tag; and absMetadata (metadata.json in the book folder) is read first. Fixed 2026-11-16 for the 41 books: file tag removed (backup
    /mnt/cache/abs_subtitle_backup_20261116), DB tagSubtitle removed with ABS stopped (backup absdatabase.before-subtitle-20261116.sqlite),
    then subtitle cleared via the API (ABS rewrites metadata.json; rescan only after ~10 s or it reads the old file).
  - Genres 2026-11-16: a product whose leaves are all themes / no genres gets the nearest real genre above (audible_result); the
    genre cleanup never writes an empty list. More '&' categories mapped (Mystery, Paranormal, Espionage, Fairy Tales, dropped).
  - Descriptions 2026-11-16: Audible's FULL publisher summary is only in the response group product_extended_attrs (live runs only,
    recordings keep the old query). Before, every 'summary' was the short merchandising text, so short/junk ABS texts (OpenLibrary
    'Book 1 of [..](link)', 'MacLeod Andrews (Narrator)') were never replaced. First live run replaces about 273 descriptions.
  - Speed 2026-11-16 (dry run of the whole library): the book step is bound by the Audible throttle (0.5 s per request), not by ABS.
    Search candidates come in one bulk request (aud_prefetch_asins), series member lists from series_members.json, the series task
    loads items in parallel: book step 10.5 -> 8 min, planned changes identical. Schedule '5 */6 * * *' (ABS scans at :00 / :30).
  - Check of the first big real run 2026-11-15: Hardcover's release_year can come from ONE wrongly merged edition (HWFWM 7 = 2006,
    Fellowship = 1937) or from another book found by an alternative title (Dragonsblood -> Dragon's Kin): the year is only taken from
    an ASIN/ISBN match or a close title AND when hc_year_plausible (an edition within a year, not a lone edition > 5 years before the
    rest). Case-only author variants ('XKarnation'/'Xkarnation') are merged by renaming the ABS author entry (per-book writes were
    repeated every run). Role credits with an em dash ('— Translator', '— Other') count as roles.
  - Loop 2026-11-15 part 3: review kinds 'nested' (book folders inside a book folder) and 'too_long' (audio > 1.5x Audible, > 60 min);
    genre synonyms -> one name (_GENRE_SYN, Audible's names; 'adult'/'Amazon' go), also per book (no ping-pong); a series set in the
    same run cleans the subtitle label at once; co-authors without own books -> review list (authors kind, decision remembered;
    REPLACED 2026-11-24: they are added automatically, also pen names - only names with a role (is_role_name: role_credits.json,
    or this book's Hardcover contribution role) stay out; the write waits for the Hardcover cross-check; review_sweep drops old
    'authors' entries and 'chapters' entries once the book has chapters);
  - Review list leftovers 2026-11-24 (user: solve them without manual work): a replaced wrong ASIN kept its new data (the check
    'found != asin' ran after asin = found: data dropped, strike, false 'asin' entry - 'Secrets of the Deep'); asin_plausibility:
    same author + title < 0.45 + runtime > 5 min off = 'wrong' (another book of the author: 'Phantom Deadfall' had 'Black Labyrinth');
    RE_FILE_JUNK drops bracketed release ids ('[66vbrM4]': letters AND digits, 6-12); same_recording: ABS 'Full Cast' / 'Various'
    = Audible's cast of >= 3 narrators when the runtime matches (+-2 min); Hardcover ASIN/ISBN link to another book -> title search;
    a mismatch with title < 0.5 and the same author is another book (no review entry, no rating); a description entry only when
    no text replaced it; review_sweep also clears 'description' entries.
  - Review list UI 2026-11-24 (user idea): every book entry has Apply / Discard toggles (marks kept per browser, keys A/D/U),
    'Process marked entries' sends both lists as JSON to /queue-decide (_queue_process: discards, overrides + ONE item scan,
    title/ISBN/authors via abs_apply - RunManager.start(after=[...]) chains the jobs; nothing changes while a scan runs).
    'Other': same-ASIN groups and duplicates get 'Hide' (other_hidden.json, id = ASIN + its books / titles + runtime:
    a changed group shows again; ?hidden=1 lists them with 'Show again'). Web UI runner counts script processes started
    elsewhere (other_script_pids) and finishes stale 'running' entries from their log.
  - UI pass 2026-11-24 (user: shorter, tidier, settings less cluttered, dark theme calmer): settings in 6 sections (core.SECTIONS:
    Connection, Scans = Run + Schedule, Sources, Metadata = Metadata + Series & authors, Notifications = + Calendar, System = Logs +
    Interface + 'Account and data'; SECTION_ALIASES keep old posts, JS maps old #hashes), rows = name + help left / control right,
    switches (input.sw), reset icon only when the value differs (no dotted 'default' text), SUBHEADS inside cards, DEPENDS hides
    rows while their switch is off; ADVANCED rows (set-once / fine tuning) only with the 'Advanced settings' switch (Sonarr-style).
    Overview: short library-check labels, checks without findings in one line, no duplicate Hardcover numbers. Run names shown in
    the UI language (app.run_label), singular counts, 'quick mode' trigger, journal times converted from UTC (filter dt), header
    'Next scan' chip (filter when), search right of the tabs; dark tokens: darker bg, quieter hairlines. Preview without internet:
    copy of the config + internal docker network + headless chromium (zenika/alpine-chrome).
  - Speed 2026-11-24 (quick runs 2.5-5.5 min, books only 15-30 s): library tasks load the full items 500 at a time
    (abs_series.load_full_items, POST /api/items/batch/get; single GETs only as fallback; fake_abs has the endpoint) - 60 s -> ~3 s;
    missing volumes ask the series ASIN that answered before first (76 dead alternate ASINs were asked in every run, an answer
    without a list is not cached) - every series still checked every run; a passed source check is reused CANARY_REUSE_MIN (60) min
    (source_check.json, never in record/replay, removed on a failed check; the breaker's own checks always ask).
    run status 'warning' when every failed book was a network error (web UI badge + reason).
  - tools/run_round.sh RECORD_NO_LIVE is a HOST LIST, not a flag ('1' matched every URL with a 1, web pages went live and were
    recorded -> golden deviations). For library-wide comparisons use mode 'replay' (nothing recorded).
  - Book folders: new ones are named 'N - Title {Narrator}' (user 2026-11-15; several: '{A} {B}' or '{A, B}'); the old
    'N - Title - Narrator' is still read. folder_narrators reads braces without a folder number too.
  - Loop 2026-11-15: 'new book' = not in history AND ABS addedAt within NEW_BOOK_DAYS (30) - never a whole first run. Manual update =
    newer ABS time stamp AND changed metadata fingerprint (meta_fingerprints.json; inode/path rescans stamp items too). Covers in original
    size (no '._SL1215_'; cover_state token 'ASIN@orig' upgrades old ones once). Identity switch applies once (identity_meta.json).
    Box sets also 'A / B / C' and 'Trilogie 1-3' subtitles. Selection runs (ONLY_ITEMS) never wait for Goodreads; long pauses are logged
    at once (log_pause, extra={'now': True} bypasses the per-book log buffer). SERIES_LIST_DAYS (1) web UI setting.
  - Moved-item guard (item_paths.json): ABS 2.x's folder watcher gives a NEW folder an existing entry when the inode matches, without
    checking that the entry's own folder is gone; Unraid /mnt/user (shfs) inodes are not stable, so stale stored inodes collide (Ambassador's
    Mission got the 'Orb of Cairado' entry, 2026-11-10). An entry whose path changed while neither title nor author word is in the new path
    is held back (not processed), goes on the review list ('moved') and a library scan restores the other folder's entry. Released when it
    fits its folder, the review is decided, or an 'identity' override exists. A confirmed other-book ASIN (identity) now also replaces
    publisher, year, genres, ISBN and the cover.
  - Outside ABS matches (import check 2026-11-25): ReadMeABook's 'RecentlyAdded' job (every 5 min) lets ABS fuzzy-match every new book
    without an ASIN - 478 matches in 1.5 days, 10 wrong (other volume with the same runtime, German/French edition, other narrator).
    The ASIN then looks like 'ABS (confirmed)'. Guards: `title_number_conflict()` = both TITLES are the same series title with another
    number AND ABS's own series number is the one in the ABS title ('Death: Genesis 7' vs 'Death: Genesis 3', 'Vol. 14' vs 'Vol. 1') ->
    search, same_recording() does NOT excuse it; no product of that volume found -> nothing taken, review 'asin'. NEW books: folder
    narrators ({...}, written by the import chain) that share no name with the product's narrators -> strict search for this recording
    even with a close runtime (existing books keep the old > 10 min condition). Checked on the live data: number rule 2 hits in 3010
    results (both wrong), narrator rule 6 hits in 966 new books (all wrong editions). Also: '[03] - Necropolis' = number_prefix;
    author 'By: X' / 'Written by: X' / 'Von: X' -> 'X' (abs_series.CREDIT_PREFIX_RE, library-wide author fix). tests/test_import_guard.py.
  - Chapters on the file's pauses (chapter_audio.py, 2026-11-25, user: as few books as possible with hand-set chapters): ffmpeg
    silencedetect (8 kHz, -40 dB, >= 0.6 s; ~20-30 s per 10 h book, nice 15, cached per file in cache/silences/) + Audible's list.
    A chapter break = a long pause (2-5 s), the first word ~0.8 s after its end. silence_align(): model file_t = s*(1+r)+o, pause classes
    {0.5, 1.0, p95, p98, p99} x tol {0.5, 1, 2}; accepted only with >= 80 % hits, binomial chance < 1e-12 and no rival offset > 10 s away
    (3-7 s rivals = the pauses before/after a spoken chapter title). decide(): no/one chapter -> Audible's on the pauses; own marks
    < 60 % at a pause (download split points) -> replaced; own marks at pauses -> kept (names only if they are just numbered). Needs the
    read-only mount /mnt/user/data/media/books -> /data/media/books (same paths as ABS) and ffmpeg in the image; without them it is off.
    abs_ratings: automatic for books without usable chapters when match_chapters() fails, also for an unverified recording (the pauses are
    the proof). chapter_check.py = web UI scan task 'Chapters' (kind 'chapters'): whole library / selection, Audible lists cached 30 days,
    books whose marks already sit at Audible's starts skip the audio. Real data: 5/9 review-list books aligned, 4 books with good own
    chapters kept, truth files max error 0.3 s, 0 false alignments in 156 cross pairs. tests/test_chapter_audio.py.
    piecewise_align(): fallback when one offset cannot explain it - up to 4 stretches of >= 4 chapters, each far beyond chance on its own
    (counted with the number of offsets it could pick), jumps <= length difference + 60 s (The White Song: 0 / -112 / -100 s, 40/40).
    chapter_check.py with CHAPTER_CHECK_PLAN_ONLY=true only counts (no audio read). Review entries of books deleted in ABS are dropped
    (review_drop_missing, listing before the moved-item guard). tests/test_review_missing.py.
    2026-11-26 after the first library test run (3035 books: 2090 fine, 125 planned, 521 kept, 296 not alignable): silence_align
    also starts from the rate the length difference implies (|dur/total - 1| <= 0.5 %); prop_align() = last fallback, chapter by
    chapter on the file's LONGEST pauses (top n..3n), next chapter around previous hit + Audible length x (file/Audible length),
    +-6 % (>= 20 s), leave-one-out drop of hits off the line through their neighbours (the last hit: through the two before it - the
    file may lack its end), >= 85 % hits, Poisson-binomial chance < 1e-12. Finds other speed (Charon's Claw 0.908x), missing end
    (Archmage -28 min), drift (Thud!): 116 of 290 failed books, 0 false in ~10500 cross pairs; truth files worst ~4-9 s (Audible's
    mark after the spoken title, the longest pause before it). decide(): own marks starting after the file end -> replaced; own marks
    > 3 h apart (file parts 'pt.1/pt.2') -> replaced; chapters_poor() also flags marks > 3 h apart (Reality Dysfunction 2 in 41 h).
    2026-11-26 (user): a NEW book (new_book) with not-poor own marks that do not sit at Audible's starts (chapter_check.classify rule:
    anchor_align miss, Audible >= 4 chapters) gets audio_chapters() in the normal run - the scan task no longer needed for new imports.
    Scan page: ONE 'Authors' box = AUTHOR_NORMALIZE + REMOVE_ROLE_CREDITS + REMOVE_EMPTY_AUTHORS as set in the settings (all three if none).
    2026-11-26 (user: 'as little as possible on the review list'): CA.loose_chapters = last resort for a VERIFIED recording
    (same_recording) with no/one/file-part chapters and a file within 60 s of Audible's chapter list whose pauses prove nothing (Snow
    Crash, Death of Kings, Good Omens, Four Past Midnight): Audible's list as it is, starts moved onto a pause within 3 s. REDOWNLOAD
    list `redownload.json` (redownload_flag/redownload_clear, real runs only): books whose FILE is the problem - chapter check: poor
    chapters no Audible list fits (not 'only numbered', not 'too few'), or aligned but Audible's story chapters missing at the end
    (>= 5 min; bonus/credits/excerpt chapters RE_EXTRA do not count); normal run: the 'incomplete' check. Read by the audiobook chain
    (redownload). Stale 'incomplete' review entries are swept when the chapter checks ran and no longer raise them. Runtime mismatch
    + another author (match_author False) = another book: nothing taken, not even ratings ('Imaginary Friends' had the ratings of
    'Confessions of an Imaginary Friend').
    2026-11-26 numbered download parts: CA.numbered_parts() = >= 3 marks that differ only by numbers ('And Another Thing 1-9', 'Neuromancer -
    02', 'The Warded Man 02 of 16', '002-200 Billy Summers') = no chapter names (chapters_poor 'only numbered ...' too); decide(): such marks
    with Audible's (aligned, proven) longer list -> Audible's; generic own marks all at Audible's aligned starts (+-5 s) with a longer Audible
    list -> Audible's finer named list. Library dry run chapdry5: 138 planned (107 download parts), 39 for a redownload.
  - Title = only the series name (2026-11-26, 'Ends of Magic' #4 = 'Anarchist', file tags; runtime 8 min off = no same_recording):
    title_is_series_name() - the ABS title equals its own series name, Audible files the product under the same series AND number,
    Audible's title does NOT start with the series name ('Beware of Chicken: A Xianxia ...' = marketing, kept) -> Audible's title
    (author must match, runtime within the normal tolerance). Library check: 7 books (Ends of Magic 4/5, Completionist 4, Good Guys 13 ...).

- Web UI: keep texts short (help texts <= ~100 chars, user 2026-11-15: human readability first). Series and missing volumes are ONE page (series.html, two views). Screenshots: tools/ui_shots.sh <snapshot> <port> name=/path ... (FULL=0 = viewport only); never commit them (private data).

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
- Unit tests live in `tests/test_*.py` (pytest, mocked `requests`/ABS); the web UI is also tested end-to-end with a fake ABS (Flask) + Playwright.
- CI (`.github/workflows/docker.yml`): the tests that need no private data run on every push / pull request; the image is only built when
  they pass. `tests/test_global_state.py` fails when a new module-level dict/set/list appears in `abs_ratings.py` (thread safety, see
  "Parallel books") - make it thread-local (`_TLDict`) or, if it really is run-wide, add it to the test's allowlist with a reason.

## Running
- Container: `docker run -p 47823:47823 -v /mnt/user/appdata/audiobookshelf/abs_scripts:/config ghcr.io/xroguel1ke/abs_ratings:latest`
- Web UI without Docker: `pip install -r requirements.txt && CONFIG_DIR=/path/to/data python -m webui.app`
- Script without UI: env vars (ABS_URL, API_TOKEN, LIBRARY_IDS, DRY_RUN ...), `DATA_DIR=/path/to/data python abs_ratings.py`.

## Open work
All STATUS items are done. See docs/STATUS.md. Next: the user's first real
full run (test run first, apply selectively), done: merged into main 2026-11-09.

## Counting batches (user rule – do not weaken, do not change without the user's explicit OK)
- Every counting batch = **100 NEW library books** (never in an earlier counting batch) + **60 EN + 20 DE synthetic books** (real Audible products NOT in the library, fresh seed each batch). **When no new library books remain (since 2026-11-04): 100 EN + 80 DE synthetic books** (user decision; reusing library books is pointless). Hardcover batches use the same mix (Hardcover ON).
- Books that pass go into `tests/private/golden.json` / `tests/synthetic/` (regress gate). After every rule change the whole golden list is replayed (`tools/regress.sh`).
- Streaks: Audible 4 clean batches in a row, Hardcover 3 clean batches in a row. Any fix resets the streak. Never count a batch with a weaker mix.

## Open items
`docs/STATUS.md` is the single list of ALL open items (task + user requests) with state. Update it after every batch/chain and whenever an item changes; read it before choosing the next task.
- 2026-11-26 ONE list for the audiobook chain (its proposal): `review.json` (export_review, end of every real run / chapter check) = redownload
  entries (fix 'download') + open review entries ('download' for chapters/incomplete with ASIN, else 'user'). The chain writes
  `redownload_status.json` in /config ({key: {status: searching|not_better|replaced|no_asin|skipped, note, ts}}). Review page
  (core.chain_split): redownload books fold into 'the chain takes care of these'; not_better/no_asin ones are shown to the user.
  A normal run clears a redownload flag when the chapter checks ran and the chapters fit (incomplete only when as long as Audible's).
  title_is_series_name: 'The Remaining' + Audible 'The Remaining: Trust' (#7) = the real title (taglines 'A ... Novel' etc. still not).
