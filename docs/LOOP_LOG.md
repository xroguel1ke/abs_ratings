# Loop log (branch claude/matching-loop)

Resume here after any interruption. Newest entries at the bottom.

## Fragen an den Nutzer
- Handy-Benachrichtigung nicht möglich: settings.json hat keine apprise_url, Remote Control ist inaktiv. Bitte Apprise-URL im Web-UI eintragen (oder Remote Control aktivieren). Bis dahin nur GitHub-Stand lesen.

## Experiment log (idea -> measurement -> adopted/rejected)
- Handy-Benachrichtigung nicht möglich: settings.json hat keine apprise_url, Remote Control ist inaktiv. Bitte Apprise-URL im Web-UI eintragen (oder Remote Control aktivieren). Bis dahin nur GitHub-Stand lesen.

## Phase A – full dry run 20261103-143115 (1913 books, Hardcover on, Goodreads off)
- Planned changes: description 1882, genres 1666, language 124 (106 eng->English), asin 97, series 23, publisher 14,
  title 13 / subtitle 13, authors 9, narrators 3, explicit 2, year 1.
- Other-edition ratings: 101 books; "No Audible data": 35 warnings.
- First findings (before Opus review): box set "Dämonenblut/Nachtfeuer/Perlmond Trilogie 1-3" gets series number 5;
  ISBN-like value as new ASIN for "Legend of the Arch Magus: Publisher's Pack" and "Shifting Tides: Painting the Mists";
  subtitle "The sequel to 'The Book of the New Sun'" (marketing text) set as subtitle; ~15 cosmetic series renames.
- Opus review of all risky changes: running (scratchpad/opus_review_1.md).
- Guardrail file .claude/settings.local.json NOT created (blocked by the auto-mode classifier); user chose to continue without it.

## Phase A – Opus review of full run 20261103-143115 (report: scratchpad opus_review_1.md)
Result: ASIN 91 ok / 2 wrong / 4 unsure; title 12/1/0; series 22/1/0 (4 merges pick a poor name); language 124 ok;
other-edition ratings 34 ok / 6 wrong / 3 unsure. Wrong: Reborn to Master the Blade #2 got vol-1 ASIN (also assigned to the real
vol 1), A Thousand Li: The First War -> "The First Stop", Waldsee Trilogie 1-3 got series #5, other-edition hits Tom Sawyer,
Hell Mode #3, Runebound Professor 3, Way of Kings Prime, Dark Tower #1, A Thousand Li.
Fixes done (offline-tested only: syntax + box-set regex; NOT yet replayed against recorded cases – no fixtures yet):
1. search: reject candidate when ABS and Audible number the same series differently (`series_seq_conflict`)
2. duplicate-ASIN guard (`ASIN_OWNERS`): never assign an ASIN another book already has
3. no series number for box sets/collections (`is_boxset_title`)
4. other-edition fallback: runtime within max(45 min, 25%), same box-set-ness
Open from the review: dead-ASIN check over all 8 stores + prefix proof, no-number-on-Audible-side rule, author-✗ acceptance,
.com-first on replacement, merge name choice, silent "ABS #x vs Audible #y" cases -> review list.
Not yet done: golden list, fixtures/replay, evaluate.py, fake ABS, review_queue.json (see STATUS.md).
Guardrails: .claude/settings.local.json now exists (226 deny / 6 allow, all other shares + foreign appdata folders denied).

## Notification rules (set by the user 2026-11-03)
Send via `tools/notify.sh "<title>" "<body>"` (Apprise from settings.json, works since 2026-11-03; test sent OK).
- After every round: round no., metrics, wrong hits, Hardcover quota, and the estimated overall progress "~NN % fertig" (same number as in docs/STATUS.md; rough early on, say so).
- IMMEDIATELY when manual action by the user is needed (blocked, limit, permission prompt, open question, session needs restart).
- Once when the terminal can safely be closed. Text must include how to reopen: `ssh/Server-Terminal: tmux attach -t abs`
  (if tmux is gone after a server restart: `tmux new -s abs`, `cd /mnt/user/appdata/abs_ratings_dev && claude --continue`, then "weiter" / read docs/LOOP_LOG.md).
- When the work is finished (stop criterion met).
Never print the Apprise URL.

## Standing rule: speed (user, 2026-11-03)
After every round, if realistically possible, make the NEXT round faster (e.g. cache Audible answers within a run, fewer
duplicate requests, better search params, parallel-safe prefetch). Measure before/after (books per minute), time-box ~30 min per
idea, adopt or reject and log it under "Experiment log". Never at the cost of rate-limit safety or matching accuracy.

## Round 1 (first attempt, discarded)
Found: ONLY_ITEMS mode passes `media: {}`, so ABS runtime was 0 -> all runtime checks off (also affects the web UI single-book test run!). Fix: item fetch uses `expanded=1`; snapshot re-taken expanded. Recordings of that run deleted (they were recorded with wrong runtimes only for ABS-side logic, but search results are re-recorded anyway).
Also found: port 13378 is the user's real ABS; fake ABS now uses 18999 and aborts if the port already answers.
Open question to user: is an external watchdog (mini container that notifies when LOOP_LOG.md has not changed for 2 h) wanted? Usage limit does not auto-resume this session to my knowledge -> manual `tmux attach -t abs` + "weiter" probably needed.

## Watchdog (user said yes, 2026-11-03)
Container `abs_ratings_test_watchdog` (tools/watchdog.py, read-only mounts, no autostart). Sends ONE Apprise message only when no file
activity for 2 h (repeats after 6 h), with instructions how to continue. Stop it with `docker stop abs_ratings_test_watchdog`
when the work is finished (Claude cannot: docker stop is on the deny list). Restart after a server reboot: see the docker run line in this commit's tools/watchdog.py header comment / LOOP_LOG.

## Round 1 result (100 border cases from the full run, fake ABS = read-only snapshot of the real library, DRY_RUN, Hardcover on, Goodreads off)
Run 3 (correct runtimes): 26 min, 0 errors, 84 ASIN changes (full run: 86 for the same books).
Now blocked: "A Thousand Li: The First War" -> "The First Stop" (series number differs), "Reborn ... #4" -> Audible #3 (series number differs).
Box sets: Waldsee Trilogie 1-3 and Arch Magus Publisher's Pack get no series number any more.
Other-edition ratings: 4 in this batch (the "Hell Mode" #3 -> vol 1 case still slipped through -> fix `series_seq_unconfirmed` added, replay r02 verifies).
Still open from review: Mother of Learning (author ✗ accepted), Paranormal Payback, Alice im Wunderland (title+author+runtime only, no narrator).
Bugs found with this round (also in the web UI single-book test run): runtime of the book was empty in ONLY_ITEMS mode (queue item had `media: {}`) -> all runtime checks off. Fixed with `expanded=1` + `abs_dur` in the ASIN search call.
### Experiment log
- RECORD_REUSE (serve known answers from disk): 26 min vs 30 min before -> small gain only, because `throttle()` still waits 0.5 s per call although the answer comes from disk. Adopted (harmless); real gain needs throttling only for live requests.
- Replay mode: throttle() skipped (no network) -> adopted, replay of 100 books should take well under a minute.

## Round 1 – closed (offline replay r04 vs golden: 100/100)
- Multi-evidence rule for ASIN changes found by search: `confirmed = author ✓ and title ≥ 0.85 and ((runtime ✓ and ≥1 extra) or ≥2 extra)`, extra = narrator, volume number, series number, distinctive title (≥4 words). Otherwise nothing is written and the case goes to `review_queue.json` (new). Review list so far: Paranormal Payback, Mother of Learning (author mismatch), Alice im Wunderland.
- Series-number checks: `series_seq_conflict` (reject) and `series_seq_unconfirmed` (other-edition fallback needs a matching number when ABS has #>1; fixes "Hell Mode" #3 getting volume-1 ratings).
- tools/evaluate.py + tests/private/golden.json (100 books; private because the repo is public and the golden list contains the user's library titles).
- Offline replay of 100 books: 10 s (throttle() skipped in replay).
- Open: web UI page for review_queue.json; synthetic fake-ABS books; remaining Opus fixes (dead-ASIN over all 8 stores, .com-first on replacement, merge name choice, silent "ABS #x vs Audible #y" cases into the review list).

## Review-list cases analysed (user asked: do they exist on Audible, why not fixable?)
All 3 exist on Audible and were fixable:
- Mother of Learning Arc 1: ABS author "Kurmaic" vs Audible "Kurmaić" -> author match now folds accents (`fold_accents`).
- Paranormal Payback: current ASIN B09LJY57WR is "Eyes of the Void" (Tchaikovsky) = totally different book (explains wrong narrator in ABS); candidate has exact title, author, runtime to the minute.
- Alice im Wunderland: current ASIN is "That Time I Got Reincarnated as a Slime, Vol. 5" (explains wrong narrator); candidate matches title/author/runtime.
New evidence type `old-asin-is-another-book` (title score < 0.5 AND author mismatch for the product behind the current ASIN). Replay of the 100 round-1 books: golden 100/100, review list empty.
Note: ABS narrators/metadata of such books were polluted by wrong ASINs in earlier runs -> consider a later report "metadata filled from a wrong ASIN".

## Standing rule: review list is the LAST resort (user, 2026-11-03)
Every case that ends in "not found" or in the review list is analysed first: does the book exist on Audible/Hardcover, what is the
evidence, which verification path is missing? Improve the matching logic (offline-tested against the golden list) before accepting a
review entry. A case may stay in the review list only with a written reason why it cannot be verified safely.

## Planned: ASIN plausibility check without LLM (user question 2026-11-03)
Production has no LLM, so the checks must be deterministic rules in the script:
1. Every run, every book: load the product behind the current ASIN, compare title, author, runtime, narrator, volume with ABS -> verdict ok / suspicious / wrong ("wrong" = title score < 0.5 and no shared author).
2. Verdict wrong: take NO ratings/narrators/publisher/series/genres from that ASIN; search a replacement through the multi-evidence gate; else review list ("ASIN belongs to another book").
3. Report "metadata filled from a wrong ASIN" (narrators/publisher not matching the ASIN's product) with undo via the journal.
4. Validation: record the product behind the ASIN of all ~1920 books once (~16 min), replay the rule offline against the whole library, review every "wrong" verdict by hand, then adopt.

## Standing rule: proactively hunt for this class of problem (user, 2026-11-03)
Besides fixing individual cases: in every round actively look for the same CLASS of problem (wrong ASIN, metadata polluted by a wrong ASIN,
look-alike books, author/accent variants, short generic titles) in the whole library data, not only in the batch. Prefer a sustainable
rule that works without a human (offline-validated against the golden list and the full-library recordings) before any book may land in
the manual review list. Review list = last resort, each entry with a written reason.

## Round 2 (12 remaining border cases + 88 random fresh books) – closed
First live run: 7.8 min, 0 errors, Hardcover 96/100 (daily limit left 773 before; ~620 after the re-runs). Analysis of every "not found" (rule: review list = last resort):
- House Corrino (no ASIN): exists on Audible (B003ABUSI0, 24h09). Audible search never returns it (not with 50 results either). Fix: **Hardcover edition ASIN path** – Hardcover lists the Audible ASIN + audio length of the audio edition; accepted only if the Audible product confirms author, runtime ±10 min, title, narrator not contradicting.
- Dragon's Kin (Pern 16): exists (B002V8N9QG, 502 min, Dick Hill); hidden from catalog search (even author search 150 results). Fix: **Audible series listing path** – series product `relationships` lists all children; one bulk request (`asins=`) prefilters by runtime ±10 min + author; then the normal evidence gate. Series ASINs cached in `audible_series.json` (filled from every product loaded).
- The Last Threshold: exists (B00AXZZI7W), runtime 14 % off, same narrator. Fix: runtime tolerance (15 %/20 min) accepted if narrator matches (+ one more evidence); exact-runtime candidate always beats an approximate one.
- All the Weyrs of Pern: only the 2025 Alex C. Stewart recording exists (series listing: 52 books checked, none with 950±10 min) -> golden `not_on_audible` with evidence.
- Mushoku Tensei Vol 18 (weak evidence): ABS author romanised vs Audible Japanese script -> not a mismatch when narrator + runtime (±2 min) agree.
- Reborn ... (2 ABS books claim the same ASIN, one ABS series number is wrong): exact runtime (387 min = vol 3) decides when a series-number conflict exists AND the ASIN is claimed twice.
- Lies Weeping, Emily Wilde #3, A Thousand Li: The First War, Mother of Learning, Paranormal Payback, Alice im Wunderland: now matched with their own ASIN (verified by hand, see golden.json notes).
Bugs found: ONLY_ITEMS mode lacked ASIN owners (duplicate-ASIN guard) and runtime (fixed).
Result: golden 200 books (round 1 + 2) = 200/200, review list empty, 1 legit not-on-Audible.
### Experiment log
- Test-mode throttling only before LIVE requests (recorder.py), throttle() skipped in record/replay: 100 books replay 19 s (was 26 min). Adopted.
- Hardcover edition ASIN path / series listing path: adopted (solve 3 of 4 "not found" cases of round 2, 0 regressions on 200 golden books).
Hardcover: ~620 requests left today (reserve 25).

## Requirement: author merge (user, 2026-11-03)
At the end, ABS authors must be in English (Latin script) and without duplicates caused by slightly different spelling or another script
(e.g. "Rifujin na Magonote" vs "理不尽な孫の手" = one person; accents; initials). Rules: merge only with a safe proof (same books/ASINs, same series,
shared co-author, Audible/Hardcover author record); never merge different people with similar names; always keep the Latin-script name.
Work items: (1) inventory of all authors in the snapshot (duplicates by accent/initials/script), (2) extend abs_series author normalization
(accent folding, non-Latin variants linked via same-ASIN/same-series books), (3) golden cases + offline tests, (4) STATUS line "Authors".

## Authors – implemented with proof rules (user: merge only if really the same; series can have several real authors)
Rules in `abs_series.find_author_fixes` (all offline-tested in tests/test_authors.py, synthetic data):
1. spelling variants (initials spacing, accents, "Last, First") – existing + accent folding; tie -> spelling with diacritics.
2. Same **Audible author record** behind both spellings (only single-author books; `audible_authors.json`, filled during normal runs) -> merge (Chakraborty).
3. Author in another script (e.g. Japanese) <-> Latin author: only if both have a book with the IDENTICAL title and a shared narrator. Sharing a series is NOT a proof (tested).
4. Everything else (e.g. "J. Smith" vs "John Smith") stays in the review list; different people with similar names are never merged.
Checked: Audible and Hardcover both keep the Japanese and Latin Mushoku author as separate records (no alternate names) -> no database proof, hence rule 3.
Library result (snapshot): 5 fixes (Kurmaic/Kurmaić, Tolkien, Salvatore, Schwab, 理不尽な孫の手 -> Rifujin na Magonote), Chakraborty needs rule 2 data (review until then).

## Standing rule: every new matching/merge method is re-checked against everything (user, 2026-11-03)
`tools/regress.sh` (≈12 s) is the gate for EVERY change to matching, series merge or author merge, before it is kept or committed:
1. syntax, 2. synthetic unit tests (tests/test_authors.py, tests/test_series.py: e.g. "Dune" vs "Dune, les origines" never, different authors never, overlapping numbers never),
3. `tools/lib_check.py`: series merges + author fixes over the WHOLE library vs hand-verified baseline (every difference printed, judged by hand, then `--accept`),
4. all golden rounds replayed from the recordings vs `tests/private/golden.json` (every book ever verified, "no wrong hit on other books").
New rules also get a synthetic test for the case they must NOT fire on. Series name choice: Audible's own series title wins when exactly one group name matches it (4 flips judged correct: Realm of the Elderlings, The Empyrean Series, Have Sword Will Travel Series, Reborn ... Squire).

## ASIN audit (started 2026-11-03 ~20:20)
`tools/asin_audit_fetch.py`: read-only fetch of the Audible product behind every ASIN in the snapshot (1864 ASINs, .com first, other stores for misses) -> tests/private/asin_products.json.
Purpose: find all books whose ASIN belongs to another book (like Paranormal Payback / Alice) and validate the deterministic plausibility rule offline against the whole library.

## Standing rule: bundle long runs (user, 2026-11-03)
Very long runs (full-library audits / full dry runs) only when several reasons have accumulated; no status polling while they run (background waiter only).

## Finding: Hardcover `book_mappings` (user question 2026-11-03) – measured on 70 random library books
- platform 1 (Goodreads): 67/70 books have Goodreads IDs (avg 12.7 per book = one per edition, rating is per work). -> build `https://www.goodreads.com/book/show/<id>` directly, NO search; verify title+author on the page; max 2 IDs tried; interval/limits unchanged (≥10 s, ≤~30 requests per 100-book round, GOODREADS_SAMPLE=10 in tests, stop on first block). Query: `books{ book_mappings(where:{platform_id:{_eq:1}}){external_id} }` (add to HC_BOOK_FIELDS, 1 extra field in the existing request).
- platform 32 (Audible): 16/70 have ASINs per store (us/uk/ca/au/fr/de/it/jp, format `ASIN:store`) -> extra ASIN source, store-correct (English -> :us), always verified with the Audible product (title/author/runtime/narrator).
- platform 19 (Amazon): 0/70 – not usable.
TODO (after the audit finishes): implement both, offline-test with recorded Hardcover answers, regress.sh, then a small live Goodreads sample.

## Decision: Goodreads source order (user asked, 2026-11-03)
Hardcover's public API has only the mapping to Goodreads (book_mappings platform 1), NOT Goodreads ratings (`external_data` not exposed).
Order for Goodreads ratings: (1) Hardcover Goodreads ID -> direct page, verified title+author; (2) existing ISBN link / text search as fallback (kept unchanged, now last).
Open Library (ISBN lookup, identifiers.goodreads): adopted only if it covers the ~4 % Hardcover misses (measurement pending).
Open Library: 0/15 found by ISBN (also for well-known titles) -> not adopted. Hardcover stays option 1, existing Goodreads search option 2.
Data finding: some ABS ISBN fields contain an ASIN (e.g. B0D2LJ6SJ2) -> treat as ASIN, not ISBN (plausibility/hygiene item).

## Decision (user, 2026-11-03): wrong ISBNs may be overwritten – only with proof
Relaxes the hard rule "ISBN only filled when empty". Overwrite an existing ISBN only if ALL hold: (1) old ISBN provably wrong: invalid checksum (incl. the 636 ASIN values in the ISBN field) OR resolves on Hardcover to a DIFFERENT work (title AND author contradict; a different edition of the same work is NOT wrong, e.g. print vs audio ISBN);
(2) the book itself is verified (verified ASIN or verified Hardcover hit); (3) the new ISBN comes from a verified source (Hardcover edition of that book / Goodreads page) – if none: no change, review list with reason, never delete;
(4) lock_isbn respected, journaled old -> new (undo). ASIN-looking value in the ISBN field: if the ASIN field is empty and the value is a plausible ASIN -> move it to ASIN (verified through the Audible product), ISBN gets a verified real ISBN or stays as is.
Implementation after the audit, with tests: "must NOT overwrite" cases (print ISBN of same work, locked, no verified replacement).

## Opinion on the lock feature (user question 2026-11-03)
Library: 30/1920 books locked (lock_series 23, lock_all 7). Keep lock tags (cheap, ABS-native, protects manual decisions), do not extend them.
Plan: (1) review-list decisions are stored in decision files (like merge_decisions.json) so no tag is needed; (2) later a web UI "lock report": locked books + what the script would do if unlocked (offline run with locks ignored) so obsolete locks can be released; (3) check the 23 lock_series books against the new rules after the audit.

## Experiment backlog: safe parallelism (user question 2026-11-03)
Bottleneck = polite request intervals of Audible/Hardcover/Goodreads, not CPU. Never more than 1 concurrent request per host; Goodreads never parallel.
Candidates (measure before/after, ≤30 min each, adopt only if regress.sh stays green and results are identical):
1. Pipeline Audible/Hardcover (separate limiters; book N+1 on Audible while Hardcover handles book N) – expected up to 1.5-2x on live runs.
2. Dead-ASIN lookup across the 8 Audible stores concurrently (different hosts, 1 request each, abort on first 429) – expected ~8 s -> ~2 s per such book.
3. ABS item prefetch (local server) – small.
Relevant mainly for the final full-library run (~2h15) and later scheduled runs; scheduled after the matching items.

## ASIN audit result (whole library, 1864 ASINs, 2026-11-03) + changes
- 1736 ASINs found, 128 not found in any of the 8 stores (dead/legacy ASINs; handled by the replacement search), 45 books without ASIN.
- Deterministic plausibility check (`asin_plausibility`, no LLM): 1739 ok, **2 wrong** (Alice im Wunderland, Paranormal Payback – both already fixed by the search), **5 "ASIN right, ABS title/author differ"** ("Realms of War" x5 = War of the Spider Queen vols 2-6: runtime to the minute + narrator + series number identical; ABS title is the series name, author the series editor) -> review proposal kind `title`, not auto-changed (title rule).
- First rule version flagged the 5 Realms of War as "wrong" (false positive) -> refined: runtime ±2 min + shared narrator = right ASIN.
- Volume check: 13 conflicts -> 3 after fixing zero-padding ("Vol. 02" = "Vol. 2") and subtitle-number handling; remaining: 2 real (Mushoku Vol 18/19, duplicate ASIN with vol 9/20) + 1 numbering-scheme difference (Murderbot "Book 8" vs Audible "Book 7") now ignored when `same_recording` (runtime ±2 min + shared narrator).
- Runtime mismatches >15 %/20 min: 13 (other recordings/collections; go through the existing search route, to be analysed in rounds): Sherlock Holmes Definitive Collection, Dune, The Butlerian Jihad, Legends & Lattes #1 (729 vs 438), The God-Stone War, The Archmage Unbound, Tom Sawyer, Casual Farming 3, Edgedancer, By the Grace of the Gods vol 3, Lies Weeping (fixed), + the two wrong ones.
- Integration in process_queue: verdict `wrong` -> search for the right ASIN (non-strict, ABS narrators ignored because polluted by the wrong product); nothing taken from the wrong product; review entry if no safe replacement. regress.sh caught a first version that lost Alice/Paranormal (strict narrator veto) -> fixed.
- tests/test_asin.py added (6 tests incl. "must not fire" cases); regress.sh includes it.

## TODO noted: web UI design (see ROADMAP.md "Web UI design"): Material-style, light (pastel white + cream) / dark (two pastel greys), preview with 2-3 variants first.

## Round 3 – first COUNTING round (code frozen at commit 367ca8c, 2026-11-03 21:02)
100 fresh books (never in a round, golden list or ISBN/Goodreads test), live + record, Hardcover on, Goodreads OFF (IP blocked, see GOODREADS_FINDINGS.md), DRY_RUN, fake ABS.
Counts as CLEAN only if every risky change is hand-verified correct AND nothing has to be fixed afterwards. Any fix -> counter back to 0 (clean rounds so far: 0 of 3).

## Round 3 result (counting round, NOT clean -> counter stays 0 of 3)
100 fresh books, 4:47 min, 0 errors, Hardcover 95/100 (302 daily requests left afterwards).
Needed fixes (so not clean): (1) weak-evidence cases The Companions + The Goblin Emperor (same narrator, runtime 7-9 % off, old ASIN dead): new evidence `publisher+year` (identical publisher AND release year as ABS) -> confirmed; (2) House Harkonnen (Audible catalog search never returns it, like House Corrino): series listing path now also matches series sharing a significant word ("Dune Universe" ~ "DUNE: Prelude to Dune"), up to 10 series, candidates still filtered by runtime ±10 min + author + full evidence gate -> 1004027915.
Not on Audible (documented in golden): Allanon's Quest (66 min short story; no hit via API, author listing, series listings, web search), The Book of Lost Tales x2 (narrator "Library of Congress recording" = non-commercial).
Mockingjay -> 1338589040 (title, author, narrator, series number; runtime within tolerance).
ISBN repair: 26 planned in this batch, 15 without verified replacement -> review list (kind isbn).
Series seed cache now built from the whole-library audit (395 series titles). lib_check baseline: "The Empyrean" (2 books) now keeps its name because BOTH names exist as Audible series titles (then the larger group wins) – judged correct, accepted.

## Standing rule: never idle (user, 2026-11-03)
While a process waits (Goodreads pause, long runs), start the next independent task in parallel: next batch + its evaluation, web UI pieces, tests, docs. There must always be work in progress.

## Round 4 – counting round (code frozen at commit 07e63f3, 21:17), clean rounds so far 0 of 3
TODO noted: web UI onboarding tour (first visit, restartable from settings, Material style) – see ROADMAP.md "Web UI onboarding".

## Round 4 result (HC quota left 110 -> no more live Hardcover today)
100 fresh books, 5:14 min, 0 errors. ASIN changes (4) all hand-verified correct: Talonsister (series listing path), Black Summoner Vol 2 (incl. publisher+year), The Hunger Games (Tatiana Maslany, Scholastic 2018), Six of Crows (same 7 narrators, Audible Studios 2015). Not on Audible (other recording only): The Dolphins of Pern (ABS Mel Foster/Brilliance 2014; Audible only has Louis Hill 2025, 0 ratings).
Hardcover not-found analysis (8): fixed 3 (Faraway Paladin: HC book without author + ASIN hit; Midnight Tides: flagged compilation but single series position; The Summer Tree: same) ; legit not on HC: The Zero Secret, Arch Magus Publisher's Packs (box sets), Konosuba / Boys & Their Monsters (no HC match found).
Fixes were needed -> round 4 NOT clean (counter stays 0).
## Counting rule refinement
Audible-clean and Hardcover-clean rounds are tracked separately; production-ready needs both (3 clean rounds each, all paths exercised). Today: Audible-only rounds (HARDCOVER_OFF=1) while the HC quota is used up; tomorrow Hardcover-focused tests incl. every HC hit cross-checked against Audible.

## Round 5 (Audible-only, 2:27 min, 0 errors) – NOT clean
1 ASIN change (Black Summoner vol 8 via series listing, 8 evidences) correct. Fix needed: Bloodline (ThinkTwice, ABS series "The Root of All Evil #1") – Audible title "Bloodline: The Root of All Evil" was rejected (title 0.46) although author, runtime 1368 min, narrator, publisher+year matched: a title extension that equals ABS's OWN series name now counts as the same title. Review-list proposal kind `title` for Realms of War (known class). Not clean -> counter 0.
Fix trend per round: R3 2, R4 3 (Hardcover), R5 1.
## Round 6 (Audible-only) started
TODO noted: run mode "build up, then maintain (≈30 random books/day)" and change monitoring (last_written fingerprints + journal old values; manual changes respected) – see ROADMAP.md.

## Round 6 (Audible-only, 2:21 min, 0 errors) – NOT clean
3 ASIN changes hand-verified correct (Queen of Shadows: narrator, series #4, Audible Studios 2015; City of Stairs via series listing: runtime, narrator, series, Recorded Books 2014; Dogs of War: all 3 narrators, series #1; Head of Zeus 2025 re-listing, runtime 5 % off).
Fix needed: The Renegades of Pern (Dick Hill, Brilliance 2008, ABS 720 min vs Audible 807 min = 12 %): series-listing prefilter used ±10 min; now library tolerance (15 %/20 min), all listed candidates evaluated (closest runtime first). 38 ISBN review entries are only because Hardcover was off in this round.
Fix trend: R3 2, R4 3, R5 1, R6 1. Counter stays 0.

## Corrections of my own earlier verdicts (found by the regression gate / re-audit of "not on Audible" cases)
- All the Weyrs of Pern: I had documented it as "not on Audible for this recording" – WRONG. It exists (B002V5CHNA, Mel Foster, Brilliance 2008, 1099 min, 125 ratings); the old ±10 min series-listing prefilter hid it (ABS file 950 min = 16 % shorter). Now matched (narrator, publisher+year, volume).
- The Dolphins of Pern: I had said "only the 2025 Louis Hill recording" – WRONG. B002V1NFYY (Mel Foster, Brilliance 2008, 659 min, Pern #12, 50 ratings) exists; ABS file is 528 min (20 % shorter) -> outside the ASIN tolerance, so no ASIN change, but its ratings are used as "other edition" (B004V03WHS on .de).
Lesson: "not on Audible" entries must be re-audited whenever a search path improves. `tools/regress.sh` re-runs them every time (status not_on_audible in golden). Remaining entries: Allanon's Quest, The Book of Lost Tales x2 (Library of Congress recording) – re-checked with the new code, still no candidate.
Rules changed: series-listing prefilter = runtime within max(10 min, 25 %) (candidates are only preselected; any ASIN change still needs the strict gate: runtime within 15 %/20 min AND narrator AND ≥2 further independent evidences); "other edition" ratings additionally require a narrator overlap when both are known, exact title + same series number even if the series is named differently.
Golden list: 600 books (rounds 1-6), regress 600/600.
TODO noted: certainty score per book (weights in ROADMAP.md). NOTE (user asked): so far ALL tests use the user's own library; the 30 synthetic books that are not in the library (phase A item) are not built yet -> doing it now.
## Round 7 (Audible-only, 2:21 min, 0 errors): 2 ASIN changes hand-verified (Judicator Jane, Blood of the Mantis; 6 evidences each). Two fixes -> NOT clean:
(1) Grand Design: Audible title "12 Miles Below III: Grand Design" = ABS series name + roman volume number + ABS title -> `series_prefixed_title` rule; (2) ASIN-like value in the ISBN field of a book WITHOUT ASIN is tried as ASIN hint (verified by plausibility + runtime); of 12 such books all 12 hints were Kindle ASINs (no Audible product) -> ignored; no harm.
Not on Audible (documented): The New World (Mark Lawrence, Amazon Digital Services 2023 = Kindle short story).

## Standing rule: synthetic books in EVERY batch (user, 2026-11-03)
Every batch/round also runs 30 English + 10 German books that are NOT in the user's library and exist on Audible (`tools/synth_make.py`, seed per batch, `SYNTH=<seed> tools/run_round.sh ...`; public Audible data -> `tests/synthetic/batch_<seed>.json` is committed). Each gets a typical ABS defect (no ASIN, dead ASIN, messy number prefix, "Title: Title", marketing suffix, initials author, accent-free author, runtime 8 % off, series-name variant, messy series prefix); the expected ASIN is known.
Books that pass are added to the golden list (tests/private/syn_golden_<seed>.json, replayed by regress.sh); the next batch draws 40 NEW ones. Until now (rounds 1-7) this was NOT done – that was a gap.
Batch 1 (EN only, 30): 28/30 ok. Found two gaps that never occur in the user's library: author written with initial ("R. Rowe" vs "Rob Rowe") and short title + marketing suffix ("Robot Swarms: A Gripping LitRPG Adventure") -> fixed (tests added incl. "Dune: Messiah" must never equal "Dune").

## Counting rule v2 (user, 2026-11-03)
One COUNTING BATCH = 100 fresh library books + 60 English + 20 German synthetic books (not in the library, on Audible; the synthetic part was never run in rounds 1-7, hence more of it). Production-ready needs FOUR clean batches in a row (instead of three). Clean = nothing to fix afterwards + every risky change hand-verified. Counter now: 0 of 4.
Synthetic batches 1 (30 EN: 30/30 after 2 fixes) and 2 (30 EN + 10 DE: 40/40 first time) are in `tools/regress.sh` (tests/synthetic/batch_N.json). Golden library list: 700 books.
## Batch A1 (counting) started: 100 library + SYNTH=3 (60 EN + 20 DE), Audible-only (Hardcover quota used up)

## Batch A1 (counting batch #1: 100 library + 80 synthetic [60 EN + 20 DE], Audible-only, 7:17 min, 0 errors) – NOT clean (counter 0 of 4)
Library part: 2 ASIN changes correct (Bog Standard Isekai: Scarred, Return of the Runebound Professor 3: narrator, series, volume). 3 not found -> all solved with new evidence rules, no review-list entry:
- A Thousand Li (#12): Audible title "A Thousand Li: The Fourth Wall" starts with the ABS title; same series number, runtime, neighbour ASIN -> rule "ABS title = start of Audible title + same series number".
- Shannara: title only a keyword of "The Sword of Shannara" (audible.fr listing): all title words contained + runtime to the minute (1581) + narrator Scott Brick + author + same series number (series named differently) -> accepted.
- Spice and Wolf Vol. 8 (ABS title "Vol. 8" but ABS series #9 and the file length are vol. 9): ABS contradicts itself; runtime ±2 min + narrator decide -> B0CRSM9NK5 (Vol. 9).
Synthetic part (first time!): 77/80 -> 3 gaps found that never occur in the user's own library: (1) "The Wild Robot 01 The Wild Robot: Booktrack Edition" picked the plain edition -> now the best title match among acceptable candidates wins (exact title returns at once, inexact ones are compared); (2) "03 Blutmale" (one-word title after 01-09 prefix) was not seen as messy; (3) generic public-domain title "The Prince" (dozens of recordings): last-resort query author+narrator filter (only after the normal search found nothing, otherwise it changed old results – caught by regress.sh). Now 80/80.
regress.sh caught 3 side effects of my own changes during the hour (candidate order, early break) -> fixed; golden: library 800, synthetic 150 (3 batches). Regressions that were IMPROVEMENTS were documented: Hell Mode vol 3 (587 min exact) now gets its ASIN; House Corrino alt ASIN (UK store, same product); `alt` ASINs in golden = same product in another store.
Fix count in this batch: 6 (library 3 + synthetic 3). Previous library-only rounds: 2,3,1,1,2.
## Batch A2 started (fresh 100 + SYNTH=4: 60 EN + 20 DE)
Data finding: 19 books in the library have an ABS narrator that equals the author (data error, e.g. "Hamuo" as narrator). In the matching such a "narrator" is ignored (never evidence, never a veto). TODO hygiene: replace these narrators by the Audible narrators once the ASIN is verified (journaled, lock_narrators respected).

## Batch A2 (counting #2: 100 library + 80 synthetic, Audible-only, 7:29 min, 0 errors) – NOT clean (counter 0 of 4)
Library part: 3 ASIN changes correct (Charon's Claw, Out Law, Archmage; narrator, series, publisher+year). Fixed: The Dark Tower #1 (arabic number in ABS = roman numeral on Audible: "The Dark Tower I: The Gunslinger"); Realms of War (anthology junk metadata; runtime to the minute + narrator match "Dissolution") -> review-list proposal with reason (a human decides; no safe automatic path).
Synthetic part: 78/80 -> (1) The Wonderful Wizard of Oz (AmazonClassics): WRONG recording accepted because an ABS narrator mismatch did not veto when the book had no ASIN -> known, different narrators now veto in every mode; (2) "Prince of the Doomed City 01 Entranced": one-word title after a zero-padded series prefix not seen as messy -> fixed. Now 80/80.
Side effect found by regress.sh: veto broke Hell Mode / Overlord 12 because ABS "narrator" == author (19 books in the library have this data error) -> such a "narrator" is ignored in matching (never evidence, never veto).
Fixes: library 2 + synthetic 2 (+1 side effect). Counter stays 0 of 4. Fix trend per counting batch: A1 6, A2 5.
## Batch A3 started (SYNTH=5)
TODO noted: description update from Audible when ABS text is a truncated/very short version (ROADMAP.md).

## Batch A3 (counting #3: 100 library + 80 synthetic, Audible-only, 7:16 min, 0 errors) – NOT clean (counter 0 of 4)
Library: 3 ASIN changes correct (Beware of Chicken 3, The Masterharper of Pern, Dragon Harper: ABS title "…, Book 19" vs Audible "Dragon Harper" -> tail with a volume marker whose number equals the candidate's series number counts as the same title).
Library finding (DUPLICATE): "Throne" (ASIN B0066IFMYO dead) and "Throne Hunters 5" are the same recording twice in the library (65834 s each, same author/narrator, different folders). The duplicate-ASIN guard refused to give the same ASIN twice. -> TODO "duplicate books" report (same author+narrator, duration within 2 s, different titles/folders) in the library report + web UI.
Not on Audible as a standalone: "Shadows for Silence in the Forests of Hell" (Sanderson novella, 136 min; only inside "Arcanum Unbounded", 1351 min).
Synthetic: 79/80 -> "Twisted" (generic title, authors with initials): second last-resort query keywords+narrator filter. Now 80/80.
Rule added: 4 independent signals (author ✓ + runtime to the minute + narrator + all ABS title words contained in the Audible title) are enough without series number.
Fix count: library 2 + synthetic 1. Trend: A1 6, A2 5, A3 3. Counter stays 0 of 4.
## Batch A4 started

## Batch A4 (counting #4: 100 library + 80 synthetic, Audible-only, 6:51 min, 0 errors) – NOT clean (counter 0 of 4), but much closer
Library part: 3 ASIN changes correct (The World of Ice & Fire, Moreta, Empire of the Damned), review list empty, 0 library fixes needed.
Synthetic: 79/80 -> "The Island" (very generic title, author "S. Brody", AI narrator "Virtual Voice" who narrates thousands of books): last-resort queries fetch 20 results and prefilter them by runtime (±10 %) before loading products -> 80/80.
Fix count: library 0 + synthetic 1. Trend per counting batch: A1 6, A2 5, A3 3, A4 1.
Report: duplicate recordings (same author+narrator, duration within 2 s) added to the library report (found: Throne == Throne Hunters 5).

## Batch A5 (counting #5: 100 library + 80 synthetic, Audible-only, 6:40 min, 0 errors) – NOT clean (1 fix), counter 0 of 4
Library: 2 ASIN changes correct (The Last Hero, The Book That Wouldn't Burn; 6 evidences each), 0 library fixes. Not on Audible: Nerilkas Story (checked: search, author+narrator filter, 3 Pern series listings).
Synthetic: 79/80 -> "The Soldier": a valid .co.uk listing won before the .com one; the author+narrator last-resort now runs in the PREFERRED store first (English -> .com). 80/80.
Side effect found by regress.sh (documented as improvement): 4 older golden ASINs were .co.uk variants of the same products (All the Skills, Reincarnated vol 3, SAO 5, Primal Hunter 7) -> now the .com variant (preferred store, verified: same title/runtime/date); "86—EIGHTY-SIX, Vol. 6" is a library DUPLICATE (the .com ASIN belongs to "…Vol. 6: Darkest Before the Dawn") -> no ASIN assigned twice.
Fix trend per counting batch: A1 6, A2 5, A3 3, A4 1, A5 1.
## Batch A6 started

## Batch A6 (counting #6: 100 library + 80 synthetic, Audible-only, 7:08 min, 0 errors) – FIRST CLEAN BATCH (counter 1 of 4)
Library: 4 ASIN changes hand-verified correct (Dragonseye, The Magician's Guild, City of Blades, The Baron; 4-6 independent evidences each), review list empty, nothing wrong or missing to correct. Synthetic: 80/80 first time. One case without Audible data: "Jules Verne" (ABS title = author name, real title in the series field "20.000 Meilen unter dem Meer", German; Audible.de has the Kluckert recording at 1022 min vs the ABS file 1907 min -> different cut, no safe match = correct "not on Audible"). After the batch I added an optional search improvement (title equal to author name -> series name is searched); it does not change this outcome. Rule: a batch is clean if no wrong/missing RESULT had to be corrected.
Counter: 1 of 4. Frozen code for A7: commit 4290841 + that search improvement (regress green).
## Batch A7 started (SYNTH=9)

## Batch A7 (counting #7: 100 library + 80 synthetic, Audible-only, 6:30 min, 0 errors) – CLEAN (counter 2 of 4)
Library: 2 ASIN changes hand-verified (Neverwinter, Gauntlgrym: title, author, narrator Victor Bevine, Audible Studios 2013; runtime 4 % off), no case without data, review list empty. Synthetic 80/80. Nothing to correct. Code frozen (same as A6 incl. the title-equals-author search rule).

## Quota incident + protection (2026-11-04 00:05)
Hardcover reported r=0 (daily limit 5000 used incl. the 25 reserve; reset 01:00). Cause: my parallel regression runs sent LIVE requests for queries without a recording (and recorded 9 HTTP 429 answers as "truth"). Fixes: (1) regress.sh now runs with RECORD_NO_LIVE=audible,hardcover,goodreads = pure offline replay; missing recordings are counted and reported ("blocked"), never sent; (2) the recorder never stores HTTP 429/5xx; (3) the 9 poisoned recordings were deleted. Live rounds stay sequential.
Rule: Hardcover/Goodreads live requests only from deliberately started sequential rounds, never from regress.

## Batch A8 (counting #8: 100 library + 80 synthetic, Audible-only, 6:41 min, 0 errors) – CLEAN (counter 3 of 4)
Library: 4 ASIN changes hand-verified (A Tangled Road to Justice, Blood Moon, Rise of the King, Unbound; 4-6 independent evidences each); ISHURA 04 = Spanish edition by Norma Editorial (narrator == author data error), no Audible candidate -> correct "not found". Synthetic 80/80. Review list empty. Nothing to correct.
Qualification plan: 4 clean Audible batches (A6, A7, A8 done; A9 next) + 3 clean Hardcover-path batches (H1-H3: Hardcover ID/ASIN/ISBN paths, ISBN repair, every Hardcover hit cross-checked against Audible; quota resets 01:00) + Goodreads sample verified.

## Batch A9 (counting #9: 100 library + 80 synthetic, Audible-only, 7:28 min, 0 errors) – NOT clean (1 fix), counter stays 3 of 4
Library: 4 ASIN changes correct (Skies of Pern, Tower of the Swallow [title 0.89 + author + narrator + series number + distinctive title], Night of the Hunter, Catching Fire), 0 library fixes. "Overlord" (1239 min, Chris Guerrero): Audible Vol. 1 is 481 min with the same narrator -> probably a multi-volume file, no safe match = correct not found.
Synthetic 79/80: "The Prince" (Machiavelli, Douglass Scott): the 2023 re-release B0CR6P9671 (178 min) was taken instead of the 2020 listing B085Y4MLC6 (177 min) that matches ABS publisher AND year. Fix: an exact-title hit WITHOUT publisher+year evidence (while ABS knows both) is kept pending and the remaining candidates are checked; a candidate with publisher+year wins. 80/80.
Counter: 3 of 4 (A6, A7, A8 clean). A10 runs with this frozen code.
## Batch A10 started

## CORRECTION of the streak count (honest)
After A9 I wrote "counter stays 3 of 4". WRONG: four clean batches IN A ROW are required; A9 needed a fix, so the streak A6-A7-A8 was BROKEN (counter 0 after A9).
## Batch A10 (100 library + 80 synthetic, Audible-only, 6:40 min, 0 errors) – CLEAN -> streak 1 of 4
Library: 2 ASIN changes hand-verified: Dragonsdawn (title, author, narrator, volume, publisher+year) and The Archmage Unbound (ABS file = Todd McLaren 1012 min, Tantor 2013; the old ASIN was the Podium 2022 recording by Alex Wyndham, 813 min -> ABS publisher/year had been polluted by the wrong ASIN; B00BEMKJD6 matches runtime EXACTLY + narrator). Review list: only the known class "Realms of War" title proposal. Synthetic 80/80. Nothing to correct.
Streak history: A1 x, A2 x, A3 x, A4 x, A5 x, A6 ok, A7 ok, A8 ok, A9 x, A10 ok -> current streak 1 of 4.

## Batch A11 (173 fresh library books + 80 synthetic, Audible-only, 8:13 min, 0 errors) – CLEAN -> streak 2 of 4 (A10, A11)
Library: 3 ASIN changes hand-verified (City of Miracles, Hell Mode vol 1, Dragonflight; 4-8 independent evidences each). "Snapshot" (Sanderson novella, 142 min) has no standalone audiobook on Audible = correct not found. Synthetic 80/80. Review list empty. Nothing to correct. The library pool of never-batched books is now exhausted (173 used). After this batch the Hardcover cross-check against Audible was added (applies from the next Hardcover batch).

## Batch A12 (200 synthetic books [140 EN + 60 DE], Audible-only, 11:19 min, 0 errors) – CLEAN -> streak 3 of 4 (A10, A11, A12)
200/200 correct at the first run (cases: no_asin 51, dead_asin 24, accent_free_author 18, clean_ok 18, duplicate_title 18, messy_number_prefix 18, runtime_8pct_off 17, initials_author 12, marketing_suffix 12, messy_series_prefix 6, series_name_variant 6). The never-batched library pool is exhausted (every library book was hand-verified in some batch), so fresh tests now come from synthetic books; the final full-library run happens in the closing phase.
## Batch A13 started (SYNTH=15, 140 EN + 60 DE): 4th clean batch candidate, code frozen

## Batch A13 (200 synthetic, Audible-only, 11:59 min, 0 errors) – NOT clean (2 fixes) -> streak back to 0 of 4
Streak so far: A10 ok, A11 ok, A12 ok, A13 x. Two NOT-FOUND cases (no wrong hit), both already found as candidates by the weak-candidate review (runtime to the minute + narrator + author): (1) "A Beauty Queen's Millionaire Gangster 2: A Gripping LitRPG Adventure": the volume number sits in the title HEAD, the "volume only in the Audible title" cap (0.75) wrongly applied -> volumes of the head are now counted; (2) "Carl Weber's Kingpins: Queens 02 Kingdom": the series name itself contains a colon, so the zero-padded series-prefix pattern did not match -> allowed. 200/200 after the fixes.
Observation: the synthetic generator keeps finding rare title forms (about 1 per 200 books); the decreasing trend is real (A12 had none).

## Hardcover batch H1 (100 verified library books, Hardcover ON, Goodreads off, 2:18 min, 0 errors) – NOT clean (fixes), Hardcover streak 0
Hardcover found 91 (95 after the fixes), every hit cross-checked against Audible (title+author): first run dropped 4 -> analysis: 1 real HC data error (Hardcover lists "Dragonsblood" as an alternative title of "Dragon's Kin" -> wrong hit by title search, caught), 3 harmless (2 German editions tied to the English original by ASIN/ISBN = translation; "Ascendant Insanity" vs "An Outcast in Another World: Ascendant Insanity"). Fixes: cross-check accepts translations ONLY for non-English books tied by identifier, and title words contained; Hardcover acceptance uses ABS+Audible authors (ABS sometimes lists the translator, e.g. Yami-hara); a second real mismatch is now caught: "Realms of War" (ISBN hit) vs Audible "Annihilation" (English book -> no translation excuse).
ISBN repair: 23 planned, 16 only "any other edition" incl. foreign-language ISBNs (a Polish ISBN for House of Sky and Breath) -> "other edition" now requires the SAME language (21 fixes remain: 6 audio edition with matching length, 1 edition with our ASIN, 14 same-language edition; 18 left for the review list).
Hardcover not found 5 (box set/collection, translator-as-author fixed, others).
## Hardcover batch H2 started

## Hardcover batch H2 (100 other verified library books, Hardcover ON, 2:40 min, 0 errors) – NOT clean (1 refinement), Hardcover streak 0
Found 96/100; first run the cross-check dropped 2 harmless cases ("Home Sweet Home" = Jake's Magical Market 3, "Stone Will" = Dragon Heart 1: Hardcover lists the book under its subtitle, Audible under the series name). Refinement: same series name AND same number on Hardcover and Audible counts as confirmation (test: other number = other book). Then 96 ok, 0 dropped. ASIN results with Hardcover ON equal the golden list 100/100 (H1 and H2).
## Hardcover batch H3 started

## Hardcover batch H3 (100 other verified library books, Hardcover ON, 2:58 min, 0 errors) – NOT clean (1 refinement), Hardcover streak 0
Found 93/100; ASIN results = golden 100/100; cross-check dropped 1: "The Dragonbone Chair" vs German "Der Drachenbeinthron" (Hardcover lists the German title as an alternative title) -> the Audible title matching an alternative Hardcover title confirms the hit. Then 91 ok, 0 dropped. ISBN repair: 23 fixes, 14 without verified replacement.
Pattern of the refinements so far (all with the SAME root: Hardcover/Audible name a book differently): subtitle vs series name, translation, series prefix, alternative titles. Each was resolved with an evidence-based rule + a "must still reject" test.
## Hardcover batch H4 started

## Hardcover batch H4 (100 other verified library books, Hardcover ON, 2:18 min, 0 errors) – CLEAN -> Hardcover streak 1 of 3
Found 97/100, cross-check ok 97 / dropped 0, ASIN results = golden 100/100, no rule change needed. Hand check: 14 random hits all the same work (ISBN/ASIN/search; incl. German "Der Abschiedsstein" = "Stone of Farewell", light-novel volume numbers equal). Not found (3): Konosuba Vol. 4 (3 searches, no hit with equal volume), The Family Name (Nuttall; 2 searches, no hit) = absent or differently listed at Hardcover, no wrong hit. ISBN repair: 22 planned, 15 without verified replacement -> stay on the review list. Hardcover daily limit left 4439.
## Hardcover batch H5 started

## Hardcover batch H5 (100 other verified library books, Hardcover ON, ~2.5 min, 0 errors) – CLEAN -> Hardcover streak 2 of 3
Found 96/100, ASIN results = golden 100/100, no cross-check drops, no rule change. (Loop was idle ~4 h afterwards: background start returned at once and I did not continue; watchdog notification was correct.)
## Hardcover batch H6 started

## Hardcover batch H6 (100 other verified library books, Hardcover ON, 0 errors) – CLEAN -> Hardcover streak 3 of 3 (H4, H5, H6)
Found 96/100, ASIN results = golden 100/100, no cross-check drops, no rule change. Hardcover streak goal reached; Hardcover paths remain covered by the regress gate.
## Batch A14 started (SYNTH=16, 140 EN + 60 DE, Audible-only): code frozen, Audible streak 0 of 4

## CORRECTION (2026-11-04): Hardcover streak reset to 0 of 3
H1-H6 ran with library books only (no synthetic books). Counting rule v2 requires 100 library + 60 EN + 20 DE synthetic per batch. H4-H6 do not count. New Hardcover batches H7+ use SYNTH (fresh seed per batch), Hardcover ON; started only after the running Audible batch (one concurrent request per host).

## Batch A14 (200 synthetic [140 EN + 60 DE], Audible-only, ~10 min, 0 errors) – CLEAN -> Audible streak 1 of 4 (A14)
200/200 correct at the first run. (Evaluation note: round dir a14 held an older planned_changes file from an earlier run of the same name; tools/check_round.sh now takes the newest file.)
## Hardcover batch H7 started (100 library + 60 EN + 20 DE synthetic, SYNTH=17, Hardcover ON): Hardcover streak 0 of 3

## Hardcover batch H7 (100 library + 60 EN + 20 DE synthetic, Hardcover ON, ~8 min, 0 errors) – CLEAN -> Hardcover streak 1 of 3 (H7)
Library 100/100 = golden, synthetic 80/80 = expected, Hardcover found 151/180 (29 not found, no wrong hit), cross-check dropped 0, no rule change. Hand check of 10 random Hardcover hits (library + synthetic, incl. German "Das Schwert der Wahrheit 24", generic "Reborn" with author match): all same work.
## Hardcover batch H8 started (SYNTH=18)

## Hardcover batch H8 (100 library + 60 EN + 20 DE synthetic, Hardcover ON, 0 errors) – CLEAN -> Hardcover streak 2 of 3 (H7, H8)
Library 100/100, synthetic 80/80, Hardcover found 150/180, cross-check dropped 0, no rule change. (Loop idled ~3 h after H8 finished because nothing was queued; now batches are chained: H9 -> A15 -> A16 -> A17.)
Audible chain changed: A20-A22 = 100 random verified library books + 60 EN + 20 DE synthetic (Audible-only), per counting rule v2.

## RULE CLARIFICATION (2026-11-04, user): counting batch = 100 NEW library books + 60 EN + 20 DE synthetic; passing books go into golden, changes are tested against golden. Written into CLAUDE.md.
Finding: the library has no new verified books left (1873 of 1873 golden books used in rounds 1-7/A1-A11). Hardcover batches H1-H9 and the A20-A22 plan therefore reused library books (H: 900 reused) – I had not said that clearly. Interim: 100 reused library books + 140 EN + 60 DE NEW synthetic per Audible batch; user to decide how to treat the missing new library books. Audible streak: A14 (synthetic only) is not counted -> 0 of 4. Hardcover streak H7, H8 counted with reused library books -> flagged.

## USER DECISION (2026-11-04): no new library books left -> every counting batch = 100 EN + 80 DE synthetic (real Audible products not in the library). Reusing library books brings nothing.
Passing batches stay in golden / tests/synthetic; every fix is tested against the whole golden list (tools/regress.sh). Streaks restart at 0: Audible 0 of 4, Hardcover 0 of 3 (A14, H7-H9 had a different mix and do not count). Chain A20-A22 (reused library books) cancelled.
## Batch A20 started (SYNTH=20, 100 EN + 80 DE, Audible-only): streak 0 of 4

## Batch A20 (100 EN + 80 DE synthetic, Audible-only, 0 errors) – CLEAN -> Audible streak 1 of 4 (A20)
180/180 correct at the first run. Whole-library offline replay (2 halves, current code): golden 1870/1873; 3 deviations (Lies Weeping, Dragon's Kin 16, Renegades of Pern) probably missing series-listing recordings -> live single-book check queued; 1 new ASIN change outside golden (Casual Farming 3, B0GDRXB26Y -> B0BLRVZP6N) to verify by hand.
## Batch A21 started (SYNTH=21, 100 EN + 80 DE, Audible-only): streak 1 of 4 so far

## Batch A21 (100 EN + 80 DE synthetic, Audible-only, 0 errors) – NOT clean (1 fix) -> Audible streak back to 0 of 4
179/180. "Das Schwert der Wahrheit 20" (German): got the audible.com mirror B00TWO6BJO instead of the audible.de ASIN B008AHIEHQ (same recording: 734 min, Audible Studios 2012). Not a wrong book, but the wrong store copy. Cause: 19 series-listing candidates came first; the cap on checked candidates (max(6, len(extra))) cut off the search-query hits appended after them, so the .de hit was never checked. Fix: cap = len(extra) + 6 (search hits get their own room). Regress: everything else unchanged. Live re-run as a21b.
Live check of the 3 Pern deviations of the whole-library replay: 3/3 correct live -> offline deviations were missing recordings, not regressions.
TODO (user, 2026-11-04): covers. Today a cover is only set when ABS has none (FILL_COVERS). User sees wrong covers: for VERIFIED books replace a clearly different ABS cover by the Audible cover (image comparison, lock_cover respected, old cover backed up for undo, DRY_RUN plan first). Exception to "fill only when empty" -> document in CLAUDE.md when built.
A21 root cause refined: on audible.de only ONE query ran (title+author -> 5 other volumes, loop stops at >=4 candidates); the exact-title query that returns B008AHIEHQ never ran. Fix 2: numbered titles run {"title": title} first. Live re-run a21c.
Batch A21 re-run with both fixes (a21c): 180/180. Regress after fixes: see next entry. Hardcover batch H9 (h13): 79/80 synthetic – "Shadow Country Trilogy 01-3 Shadow Country" not found (title 0.50, author ok, runtime ok) -> NOT clean, Hardcover streak 0; rule needed for "<Series> <range> <Title>" titles.
Fix 3 (H9 finding): messy titles with a range number ("Shadow Country Trilogy 01-3 Shadow Country") are parsed as series prefix + title (no single sequence). Test added. Regress: 28/28 green (one earlier run reported DEVIATIONS with no deviation line – not reproducible, ran green twice afterwards).
Streaks after A21/H9: Audible 0 of 4, Hardcover 0 of 3. Plan: build the group-confirmation rule (Realms of War) and the cover rule FIRST, then run the counting batches with frozen code. A22 (SYNTH=22) is a free exploration batch meanwhile.
Fix 4 (A22 exploration batch, 179/180): messy title with a zero-padded DECIMAL number and a one-word series ("Robots 00.1 I, Robot") -> parsed as series prefix + title (sequence 0.1); only the distinctive form 0N.N may have a one-word series. Test added. Regress green, a22b live re-run 180/180.
Fix 5 (user idea): group confirmation. 6 library books titled "Realms of War" = R.A. Salvatore's War of the Spider Queen 1-6 (same narrator, runtime to the minute, ABS series numbers 2-6 agree; #1 Dissolution sat in "Forgotten Realms: Anthologies #12" without ASIN). New group_confirm() after the run: >= 3 same-title books, distinct candidates, one common Audible series with distinct numbers -> ASIN + Audible title (locks respected, journaled). Regress round 08 added; golden updated for the Dissolution item; regress 100 % green. The Realms of War items leave the review list.
Covers (user decision): Audible cover only for a VERIFIED ASIN (same recording: runtime ±2 min + narrator, or search-confirmed ASIN + runtime ±2 min) – then also REPLACING an existing ABS cover (UPDATE_COVERS=true), old cover backed up to cover_backup/ (no backup -> no replacement), cover_state.json avoids re-uploads, lock_cover respected; empty covers are no longer filled from an unverified ASIN. Undo uploads the backup. Golden rounds: 84-88 of 100 books would get the Audible cover once. Regress green. New tools/write_undo_test.sh: real write + undo against the fake ABS (recordings only): 20 changes written (3 covers replaced, 3 backups), 20 undone, 0 failed, covers restored from the backup files.
Cover audit (partial, read-only, 288 verified books compared by image hash before the user chose "always update when verified"): 193 near-identical (distance <= 10 of 256), 47 clearly different (> 60) = ~16 % wrong/other covers in ABS. Audit stopped (no longer needed).
## Batch A23 started (SYNTH=23, 100 EN + 80 DE, Audible-only, code frozen after fixes 1-5 + covers): Audible streak 0 of 4

## Batch A23 (100 EN + 80 DE synthetic, Audible-only, frozen code, 0 errors) – CLEAN -> Audible streak 1 of 4 (A23)
180/180 at the first run. Covers: 164 planned (all books without cover and a verified ASIN).
## Hardcover batch H10 started (SYNTH=24, 100 EN + 80 DE, Hardcover ON): Hardcover streak 0 of 3

## H10 (h24, seed 24, 100 EN + 80 DE, Hardcover ON): CLEAN 180/180, HC hits 107, drops 0 -> Hardcover streak 1 of 3. Audible streak 1 of 4 (A23).
Loop mode from now (user, token saving): chains of 4 batches (A/H alternating), compact evaluation tools/eval_batch.sh, short log entries.
## Chain 1 started: a25 (Audible), h26 (HC), a27 (Audible), h28 (HC)

## Chain 1: a25 CLEAN 180/180 | h26 179/180 (Heist 01 Heist) | a27 179/180 (Fate Hollow Academy: Term 1: A Gripping LitRPG Adventure) | h28 179/180 (Planetside 03 Colonyside). No wrong match. Streaks -> Audible 0 of 4, Hardcover 0 of 3.
Fix 6: one-word series prefix counts when it equals the ABS series name. Fix 7: marketing tail after the LAST colon; volume number may sit before the last colon. Tests added, regress green (batches 26-28 now 180/180 offline).
bookbot (github.com/itsbryanman/bookbot) reviewed: weighted fuzzy score with auto-accept >= 0.75, runtime only as bonus/penalty, no hard rejects -> looser than ours. Only candidate idea: ambiguous top-2 flag. Not adopted: our gate needs author + title + runtime/narrator/number evidence at once, and 0 wrong matches in ~1870 library + >3500 synthetic books show no such case.
## Chain 2 started: a29, h30, a31, h32

## Chain 2: a29 179 | h30 179 | a31 CLEAN 180 | h32 178. No wrong book: 3x a worse listing of the SAME recording (The Heist, The Wild Robot Protects, The Thief), 1 miss ("06 Rick").
Fix 8: preferred_twin() - after an accepted hit, one targeted query (title+author+narrator of the hit) for the same recording (narrators equal, runtime +-2 min, title >= 0.85) in the preferred store (hit from another store) or with ABS publisher+year (hit without them). Fix 9: "0N" file-name prefix with a one-word rest of >= 3 letters. Live re-runs a29b/h30b 180/180, h32b 180/180 after fix 9 (regress offline); regress green. Streaks: Audible 0 of 4, Hardcover 0 of 3.
## Chain 3 started: a33, h34, a35, h36

## Chain 3: a33 CLEAN | h34 CLEAN | a35 179/180 (miss: "The Four Horsemen [Thalassa] 02 War") | h36 CLEAN. No wrong match.
Fix 10: series prefix with a one-word rest of 3-4 letters when the prefix IS the ABS series. Test, regress green. Streaks reset by the fix: Audible 0 of 4, Hardcover 0 of 3.
## Chain 4 started: a37, h38, a39, h40

## Chain 4: a37 CLEAN | h38 CLEAN* | a39 CLEAN | h40 CLEAN* (all 180/180). *Hardcover daily limit reached (23 left, reserve 25) -> h38/h40 ran Audible-only.
Audible streak: a37, h38, a39, h40 = 4 clean batches in a row with fresh 100 EN + 80 DE and frozen code (h38/h40 counted as Audible batches since Hardcover was off) -> AUDIBLE STREAK REACHED (user informed; if only A-batches count: 2 of 4).
Hardcover streak 0 of 3: next Hardcover batches after the daily limit resets. Meanwhile Phase C (Audible part): full-library dry run with current code, every risky change checked by hand.

## Phase C, Audible part: full-library dry run (1920 books, Audible live where not recorded)
Found 6 deviations vs golden that the regress gate missed (it only covered rounds 1-8): fixed with 5 changes, all re-checked against the whole library:
- twin rule: a twin needs AT LEAST as many ratings as the hit (UK listings with 94/22 ratings had replaced US ones with 8,275/979); preferred store from the PRODUCT language (German product -> .de)
- the prepended exact-title query no longer ends the query loop alone (Renegades/Dragon's Kin)
- ASINs owned by another library book are skipped INSIDE the search unless the owner has the same runtime (= duplicate recording, then refused as before: Throne)
- series listing: relevance order instead of alphabetical before the request cap (14 -> 24); children cache per (store, series ASIN) - a series unknown on .de had hidden its children on .com. Both were REAL production risks (the series file grows with every product).
- golden updated: Alice im Wunderland (.de listing), Shannara (.com listing, 9,900 ratings); synthetic expectations 29/30/32: listing with the most ratings (alt = old).
Regress now covers the WHOLE library (wl00/wl01) + all synthetic batches: 100% green.
Risky changes outside golden after the fixes: 3, all hand-verified correct (Casual Farming 3 Virtual-Voice ASIN -> narrated edition; Judicator Jane 7 new ASIN + language und -> English). Added to golden (1875).

## Phase C: the 45 library books that were not in golden analysed -> golden now covers the WHOLE library (1920: 1904 confirmed, 14 not on Audible, 2 duplicates)
36 existing ASIN confirmed, 3 other edition (ratings only), 6 not found -> analysed: 5 really not on Audible (Dragonheart/Todd McCaffrey, Imaginary Friends, Valkyrie's Dawn, Dragonsteel Prime, Book of Lost Tales), 1 FOUND after a fix: Berserk of Gluttony Vol. 1 (ABS narrator = author in Japanese name order "Isshiki Ichika" vetoed the real narrator) -> Fix: narrator==author compares names order-independently (same_person_name). Test, regress 100% green.
Library result (Audible, current code): 1904 of 1906 books that exist on Audible are matched/kept correctly (99.9%); 0 wrong.

## NOTE: Phase C fixes (twin by ratings, series relevance/cache, owned-ASIN skip, name order) came AFTER the Audible streak -> by the rule the Audible streak restarts. Confirmation chain 5 (Audible-only, final code): a41, a42, a43, a44 (100 EN + 80 DE each).

## Confirmation chain 5 (final code, Audible-only): a41 180/180 | a42 180/180 | a43 180/180 | a44 180/180 -> AUDIBLE MATCHING DONE (4 clean batches in a row with fresh 100 EN + 80 DE, final code; whole library 100% in regress).
ISBN (user decisions 2026-11-04): field = PRINT ISBN. Order: Hardcover print edition (same language) -> Goodreads print ISBN (empty only) -> LAST fallback Audible audio-edition ISBN (own small request, same recording, checksum, never the ASIN). A wrong ISBN without a better value stays (review list). Library report now also counts publisher/year/ISBN/series.

## PILOT (REAL write, user OK): 30 library books in the real ABS, 2026-11-04 15:28
Backup first: item JSON + cover of all 30 (tools/item_backup.py via tools/pilot_run.sh). Dry run against the real ABS: 30/30 = golden, 0 risky. Real run: 151 changes (16 ASIN, 8 titles, 28 covers, genres, descriptions/rating blocks, language, publisher, subtitle, narrators, year, series, explicit), 0 errors. Verification by re-reading ABS: 123/123 field values exactly as journaled (descriptions after HTML normalisation), 28 covers with 28 backups. 2nd dry run: 1 planned change (cover of Legacies of Betrayal: narrator WE filled counted as evidence) -> fixed (FILLED_NARR, seeded from the journal); 3rd dry run: 0 changes = idempotent.
Undo in the real ABS (Exodus from the Long Sun, 8 changes): 7 ok, cover FAILED: real ABS rejects uploads named .img (fake ABS did not check) -> backups now saved with the real extension (magic bytes), undo/restore detect the format; cover undo retried: OK, cover byte-identical with the backup (sha1). Fake ABS now rejects such names like the real one. Book re-applied.
ABS server backup (POST /api/backups) returned HTTP 500 twice - the run had continued; now a failed requested ABS backup STOPS a real run (ABS_BACKUP=false to skip consciously). User to check ABS: Settings -> Backups.

## Web UI (user list, all 4 done 2026-11-04): Material theme light/dark + toggle + onboarding tour; book page with reasoning (certainty parts, search evidence), ABS<->Audible comparison, change list, global search; history page with undo per field/book/run (abs_undo.py); apply a dry run (abs_apply.py: writes only values ABS still has, conflicts and locks skipped, ABS backup first, journaled + undoable) - e2e tested against the fake ABS (check 27 + 1 conflict, apply 27, undo 27).
