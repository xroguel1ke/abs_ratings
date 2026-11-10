# Status (live) – single source for ALL open items

Updated: 2026-11-05 – ALL items done (matching, Hardcover, Goodreads rhythm, web UI, roadmap, docs). Overall 100 % of the list.
Next: the user's first real full run (test run first), then the merge (docs/CHANGES.md).
Rule: update this file after every batch/chain and whenever an item changes (task requirement). Items come from the task
(`/mnt/user/appdata/abs_prompt_matching_loop.md`) [A] and from user requests in chat [U].

Legend: ✅ done · ⏳ in progress / waiting · ⚠️ partly · ❌ open

## Matching & data
- [x] 2026-11-08 check of the first full real run (1921 books): fixes for shared titles (exact runtime), another volume's data (folder
  title), twin volume guard, non-Latin series (same title enough, removal), junk shared numbers, merge target guards (non-Latin,
  case-only names), '&', re-applying reverted values. Regress green, live dry run on 108 affected books ok.
- [x] 2026-11-08 web UI: nav Overview·Scans·Books·Review list·Settings; status merged into the overview, history is a tab of Scans; library check (abs_series.library_health) after every scan; run page with end time, speed and change counters; shorter help texts.
- [x] 2026-11-08 rules: French/Spanish-looking series names on English/German books (kept when Audible uses the name, review list when Audible has no series), a number held by 3+ different works of one language is replaced by the folder number or removed.
- [ ] regress: lib_check sometimes crashes with 139 (segfault) inside the gate, never alone - find the cause
- [ ] User: update the container, run a full scan (refresh 0); then check: Konosuba 9/10 (lock_series in the Japanese series),
  The Beginning After the End (Pack 1 and 2 locked in two case-variants -> remove one lock), Pern (two numberings mixed), Solo Leveling
  folder '7 - ... Vol. 7 (Novel)' (493 min = probably not vol. 7), Mushoku Tensei vol. 8 (Audible counts #2).
| Item | State | Note |
|---|---|---|
| [A] Audible matching (ASIN repair/search, store, other edition, language, titles) | ✅ 100 % | 4 clean chains of 180 fresh books (100 EN + 80 DE), 0 wrong matches ever; whole library 1904/1906 existing books correct (99.9 %) |
| [A] Hardcover matching + cross-check against Audible | ✅ 100 % | streak 3 of 3: H14/H15/H16 (seeds 49-51) 180/180 each, 105/119/109 HC hits, 0 cross-check drops, no 429 |
| [U] Hardcover request minimising | ✅ | combined requests tried and REVERTED (Hardcover charges every search field; caused 429 in H13); one request per step with early stop, shared cool-down, burst guard |
| [U] Hardcover: title+subtitle and 'Series, Volume N: Title' comparison titles; edition-title lookup (translations) | ✅ | no wrong hits in H12; cross-check unchanged |
| [A] Phase C: 3 rounds with EVERY Hardcover hit hand-checked | ✅ | H14-H16: 331 matches, 1 wrong (Queen Takes Rose -> Queen) fixed; hand-checked matches = Hardcover golden in the regress gate |
| [U] Hardcover 'paid plan' checkbox: own daily budget 5000 (free) / 50000 (paid), counted locally (hardcover_usage.json); production default = free | ✅ | status page shows account rest + own budget |
| [U] Runtime tolerance default lower than 15 % / 20 min | ✅ | measured: 5 %/10 min loses 16, 10 %/15 min 6 of 1920 verified ASINs -> same-BOOK tolerance stays 15/20 (labelled so in the UI); recording details (narrators, publisher, year, abridged) now need runtime within 10 min (74 library books affected) |
| [U] Per-source search state: found -> REFRESH_DAYS, missing -> next day, 5 misses or 'not listed' -> pause REFRESH_DAYS; blocked = no miss; rating-only passes ask only the missing sources (no Audible request) | ✅ built | source_state.py + 10 unit tests; seeded from history/caches; real-run test on the fake ABS (state persisted only in real runs) open |
| [A] Phase C: full-library check incl. Hardcover cross-check | ✅ | Audible part (6 bugs fixed) + Hardcover part: 1854/1913 on Hardcover, 7 wrong found and fixed by number rules, library replay shows only those 7 changes |
| [A] Series merge / author normalisation: every planned merge hand-checked, 0 wrong | ✅ | baseline merges hand-checked; fresh snapshot of the real library 2026-11-05: 0 pending merges/author fixes (3 locked cases on the review list); translator-chain bug fixed |
| [A] Fake-ABS edge cases for series/authors (articles, Saga, Light Novel, translations, initials, two real authors) | ✅ | synthetic merge batches 9-11 against the fake ABS (generator tools/merge_synth.py) + unit tests |
| [U] Synthetic series/author merge batches; 3 clean in a row | ✅ | batches 9-11 clean (56/57/58 cases incl. role credits + empty authors) |
| [U] One review list with a left menu (books, series, authors, other) | ✅ | |
| [new] "Other" review cases: book in a series of another author (e.g. Bury Our Bones in "The Age of Madness") | ✅ | review list Other: remove from series (journal, undo) / keep; 11 in the library |
| [U] Remove empty authors (0 books) - re-checked before delete, never books; checkbox on the scan page + setting | ✅ | tested vs fake ABS (refuses deleting authors with books) |
| [U] Only real authors: role credits removed / never taken from Audible; learned plain illustrators; checkbox + setting | ✅ | 26 library books cleaned in the dry run |
| [new] Plain illustrators never seen with a suffix (e.g. only "Kurone Mishima") -> Hardcover contribution roles | ✅ | contributors of a verified Hardcover book (Illustrator, Translator, Contributor) are learned as role names; live probe: Kureta, David Musto, Tsukasa Kiryu, Jenny McKeon |
| [U] Series merge blocked by lock_series -> on the review list (user decides) | ✅ | 3 cases in the library |
| [A] Negative cache for not-on-Audible books (re-check after ~90 days) | ✅ | no product in any store -> parked until REFRESH_DAYS at once; other misses: 5 daily tries, then REFRESH_DAYS |
| [U] Group confirmation (same-title books of one series) | ✅ | Realms of War = Spider Queen 1-6 |
| [U] Covers: Audible cover for verified ASINs, also replacing, backup + undo | ✅ | real ABS undo tested (byte-identical) |
| [U] ISBN = print edition (HC/Goodreads), Audible audio ISBN last fallback, ASIN never | ✅ | live library test: 227 fixes + 60 fills, 2 wrong (foreign group) fixed by a registration-group check; 409 without safe replacement in the ISBN report |
| [U] Description update when ABS text is truncated/very short and Audible's is clearly complete | ✅ | 85 library books (chapter names, placeholders, truncated texts), own texts kept |
| [U] Narrator == author hygiene (remove the wrong ABS narrator) | ✅ | only for the same recording (runtime 2 min) when Audible names other narrators; 2 library books |
| [U] Duplicate books report (same recording twice) in the UI | ✅ | review list Other: same recording + same ASIN twice |
| [U] Build-up / maintenance mode (~30 random books per day) + change monitoring (respect manual edits) | ✅ | RUN_MODE auto/buildup/maintain, MAINTAIN_SAMPLE; last_written.json: rescan-reverted fields re-applied, user-changed fields never overwritten (manual_fields.json, optional lock tag, review list Other); integration test in the regress gate |
| [U] Lock feature kept but de-emphasised; lock report | ✅ | status page: lock tags with books (11 in the library) |
| [U] Parallel books (WORKERS=3): max 1 request in flight per host, MIN_INTERVAL across threads, same author in one worker, per-book log blocks | ✅ | regress identical with 1 and 3 workers (first try found a shared search context -> 2 wrong ASINs, fixed); H12 14.5 min vs ~22 |
| [U] Fix loop round 6/7 (2026-11-08): sibling-series cache (30 days), Audible sequence 'N, Subtitle' = no number, unnumbered leading series split via the ABS series list, novella range '0.1 - 0.5', accent-insensitive narrators, author without space, Part P Volume V = #P.V | ✅ | Stolen Heir / Assassin's Blade / Chain of Feathers / Blue Core 3 / Bookworm P1V3; regress + damage sim green |
| [A] Counting batches after the 2026-11-08/09 rule changes (100 EN + 80 DE synthetic, Hardcover on, new cases year prefix / ISO code / wrong narrator + book folders) | ✅ | 52-64: fixes after 53, 55, 59, 60; 61-64 clean = Audible streak 4, Hardcover streak 4 |
| [U] Library rounds 6-9 | ✅ | round 9: 0 changes, 0 errors; audit in ABS clean (3 accepted number pairs: split German volumes, two recordings of Mistborn 1) |
| [A] Night 2026-11-09: genre cleanup (188 books, stable next round), Hardcover edition label in subtitle, library-task writes stamp history, race fix, folder narrator names | ✅ | rounds 10-12 0 errors; batches 65-68 clean (streak 4 again); ASIN audit of all 1907 ASINs: 1900 ok, 7 gone (known) |
| [U] 2026-11-09 day: source health (canary + breaker), BOOK_LANGUAGES (no foreign editions, language consensus, wrong-language review list), series never mix languages, review list explained in plain words + checked same-ASIN groups, missing volumes page, sibling recordings via catalog search (Discworld #30/32/35) | ✅ | full round 0 errors; missing volumes 290 in 112 series |

## Goodreads (experimental)
| Item | State | Note |
|---|---|---|
| [A] Sparse sampling (GOODREADS_SAMPLE), stop at first block, never bypass | ✅ | |
| [U] Hardcover Goodreads-ID path first | ✅ | correct 5/5; /book/show may be guarded more strictly than search |
| [U] Limit measurement until a reliable requests/hour + per night value | ✅ | book pages: block after ~3-7; search list: ~100-150 searches in a row, then blocked for hours (test 8) -> 25 per night |
| [U] Open Library fallback for the Goodreads ID / ISBN | ✅ not needed | the search list gives the rating in one request; series rows/pages cover volumes |
| [new] Search-list-only mode: rating from the SEARCH result row, never a /book/show page (GR_BOOK_PAGES=false) | ✅ | new Next.js search layout parser (work.stats); test 6 replay 21/25 found, all hand-checked correct |
| [U] Goodreads series: series name + number + author confirm a row; series page for other volumes (1 request) | ✅ | test 7 live: 25/25 found, no block |
| [U] Goodreads book page first (robots.txt allows it), search list as fallback, AutoThrottle (Scrapling idea), 2026-11-08 | ✅ | live 8/8, 3 pages + 5 searches, no challenge; no browser / no impersonation |
| [U] Library check 2026-11-08: duplicate series (case, "(Series)", "(Light Novel)"), Japanese series names, ASINs not found on audible.com, Stormlight series broken, old logs | ✅ code | cause: ABS forced rescan 2026-11-07 (stale August metadata.json). Fixed per book by the script: series task crash, lock rule, non-Latin / folder series, library series added again, shared numbers, change monitoring (only lock_ tags protect), ROLE_NAMES race. Dry run 20 problem books ok. Needs ONE full scan (batch >= 2000) |
| [U] Production rhythm (≈25 per night?) | ✅ | test 8: block after ~125 searches within hours -> 25 per run (default), stop at the first block, continue next day |

## User findings 2026-11-05
| Item | State | Note |
|---|---|---|
| [U] Remove-ratings button (choose sources / all) | ✅ | scan-page task + book page; journaled, undo; source paused until refresh |
| [U] Progress numbering / bar jumping | ✅ | numbered by finished books |
| [U] Backup checkbox on the scan page; no backup when applying from the review list | ✅ | |
| [U] Check the user's run logs (Hardcover 'limit', Dragonsteel Prime) | ✅ | 2 real bugs (shared LAST_SEARCH, translator author links) fixed + ABS repaired with OK |
| [U] .com first for English, .de for German | ✅ | already so (measured in the user's run) |

## Safety & tests
| Item | State | Note |
|---|---|---|
| [A] DRY_RUN default, journal for every write, undo | ✅ | |
| [A] Real write + undo end-to-end against the fake ABS | ✅ | + real pilot on 30 books (user OK) |
| [A] ABS backup before real runs | ✅ | run stops when it fails (user's ABS max backup size fixed) |
| [A] Golden list + record/replay + evaluate + regress gate | ✅ | whole library + all synthetic batches |
| [A] pytest suite | ✅ | tools/pytest_all.sh (8 test files), step 2a of the regress gate |
| [A] Review list small, only real doubt cases | ✅ | 8 entries for the whole library; wrong ISBNs without a replacement moved to reports/missing_isbn.json (status page), 529 offline - re-measure with Hardcover |
| [U] Review list: accept for ISBN and title kinds | ✅ | ASIN (override), title + ISBN (abs_apply: value check, journal, undo, checksum), Hardcover link (override) |
| [A] Counting batches 69-105 (strict expect_* checks since 76) + web UI round 2026-11-09 (series page with inline missing volumes, book page, history search, instant search, notifications per event + ntfy check, rating symbol sets + rewrite) | ✅ | 97-105 clean = 9 in a row (1620 books, 0 deviations); 'new' on the missing list = newly released only |
| [A] Bug hunt 2026-11-09 evening (live library audit, web UI e2e tests, code review, counter-check batches 106-109, bulk Audible prefetch) | ✅ | 13 clean batches in a row (97-109); full dry run 1919 books in 13.5 min (was 34), 0 errors, 0 doubled blocks; done - further testing = diminishing returns |
| [U] Library audit rounds 1-10 (2026-11-09/10) + chapter matching | ✅ | rules for packs (ranges), file tags, glued/series-label titles, other book's descriptions, ISBN groups/shared ISBNs, narrator spellings, swapped roles, genres, subtitles, chapters (37 books); final test run 20261110-021319 (478 changes, 0 errors) waits for the user's apply |

## Web UI (task: 10 points, Playwright vs fake ABS, screenshots in docs/screenshots/)
| # | Item | State | Note |
|---|---|---|---|
| 1 | Start dialog with per-run settings (books, refresh days, sources, selection all / problems / new / list) | ✅ | runs page "Optionen nur für diesen Lauf"; saved settings untouched (tested) |
| 3 | Live progress (bar, ETA, book, counters) | ✅ | |
| 4 | Apply dry run: grouped by kind, filterable, checkboxes, no new Audible requests | ✅ | review page: groups, filter, checkboxes, check/apply selection; e2e tested (untick a group -> exactly those skipped) |
| 5 | Mark suspicious changes (title, ASIN change, language, new series, other edition) on top | ✅ | ⚠️ section on top of the review page (language code -> name is not risky) |
| 6 | Search over all books + test/real run per book | ✅ | |
| 7 | Comparison ABS ↔ Audible ↔ Hardcover + planned change | ✅ | 3 columns + change list on the book page |
| 8 | Reasoning per match (title, author, runtime, narrator, number, source) | ✅ | + source of the ASIN and of the Hardcover match |
| 11 | Change history with undo per change / book / run | ✅ | |
| 13 | Coverage statistics incl. ratings + Hardcover + trend over time | ✅ | + Audible/Goodreads ratings, Hardcover, change since the first snapshot (coverage_history.json) |
| 14 | Hardcover / API status: used/left today, cache size, reset time | ✅ | used/left; reset time shown when Hardcover sends a reset header (logged since today), else "daily" |
| – | Review list page | ✅ | see accept gap above |
| – | [U] Design (Material light/dark), onboarding tour, settings with left sections + save per section, confirm dialog | ✅ | |
| – | Screenshots in docs/screenshots/ (no private titles, public repo) | ✅ | 9 pages with public synthetic test books, light/dark/phone; in the README |

## Web UI language
| Item | State | Note |
|---|---|---|
| [U] English web UI, English as DEFAULT, German selectable | ✅ | Settings > Interface; webui/i18n_de.py; tests/test_webui_i18n.py in the regress gate (every string translated, no German on English pages) |
| [U] App icon instead of the star, also as browser-tab favicon | ✅ | webui/static/icon.svg |
| [U] Scan page remembers its choices (tasks, test run, options) across scans and page changes | ✅ | settings.json scan_form; never changes the settings; browser-tested |
| [U] Header: running scan as compact pill (no layout break), logout icon, password in Settings | ✅ | 1280/1024/390 px checked |
| [U] Human-readable values (series 'Name #1', lists, yes/no) in history, scan details, test-run review, book page | ✅ | + removed parts red (old), added parts green (new) |

## Finish (Phase C end)
| Item | State |
|---|---|
| README for the new UI functions | ✅ |
| Change overview for a later merge (to web-ui/main) | ✅ docs/CHANGES.md |
| Final report in LOOP_LOG + to the user (numbers, changes, limits, first-run recommendation) | ✅ 2026-11-05 |
| CLAUDE.md / ROADMAP.md up to date with all learned rules | ✅ |
