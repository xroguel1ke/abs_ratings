# Status (live) – single source for ALL open items

Updated: 2026-11-04 22:00 – English UI done (German selectable). User raised the Hardcover account to 50k/day (60/min, burst 30):
Hardcover batches can run now. Overall ≈ 72 %.
Next: Hardcover counting batches, Goodreads search-list-only mode, runtime tolerance default, app icon, Phase C remainder, final report.
Rule: update this file after every batch/chain and whenever an item changes (task requirement). Items come from the task
(`/mnt/user/appdata/abs_prompt_matching_loop.md`) [A] and from user requests in chat [U].

Legend: ✅ done · ⏳ in progress / waiting · ⚠️ partly · ❌ open

## Matching & data
| Item | State | Note |
|---|---|---|
| [A] Audible matching (ASIN repair/search, store, other edition, language, titles) | ✅ 100 % | 4 clean chains of 180 fresh books (100 EN + 80 DE), 0 wrong matches ever; whole library 1904/1906 existing books correct (99.9 %) |
| [A] Hardcover matching + cross-check against Audible | ⏳ 70 % | streak 0 of 3 with the final mix (H10 clean, then fixes reset it); daily limit; every HC hit auto cross-checked |
| [A] Phase C: 3 rounds with EVERY Hardcover hit hand-checked | ❌ | quota now available (50k) |
| [U] Hardcover 'paid plan' checkbox: own daily budget 5000 (free) / 50000 (paid), counted locally (hardcover_usage.json); production default = free | ⏳ | built, to be tested |
| [U] Runtime tolerance default lower than 15 % / 20 min | ⏳ | measured: 5 %/10 min loses 16 of 1920 verified ASINs (e.g. Pern, other recording 12-16 % longer) - too strict; measuring 10 %/15 min |
| [A] Phase C: full-library check incl. Hardcover cross-check | ⚠️ 60 % | Audible part done (6 real bugs found + fixed); Hardcover part open |
| [A] Series merge / author normalisation: every planned merge hand-checked, 0 wrong | ⚠️ 85 % | 19 merges hand-checked; write + undo vs fake ABS ok; REAL test ok (ABS removes emptied series/authors, undo re-creates them with new ids) |
| [A] Fake-ABS edge cases for series/authors (articles, Saga, Light Novel, translations, initials, two real authors) | ⚠️ | only synthetic unit tests (test_series/test_authors), no fake-ABS books |
| [U] Synthetic series/author merge batches; 3 clean in a row | ✅ | batches 9-11 clean (56/57/58 cases incl. role credits + empty authors) |
| [U] One review list with a left menu (books, series, authors, other) | ✅ | |
| [new] "Other" review cases: book in a series of another author (e.g. Bury Our Bones in "The Age of Madness") | ❌ | |
| [U] Remove empty authors (0 books) - re-checked before delete, never books; checkbox on the scan page + setting | ✅ | tested vs fake ABS (refuses deleting authors with books) |
| [U] Only real authors: role credits removed / never taken from Audible; learned plain illustrators; checkbox + setting | ✅ | 26 library books cleaned in the dry run |
| [new] Plain illustrators never seen with a suffix (e.g. only "Kurone Mishima") -> Hardcover contribution roles | ❌ | needs Hardcover quota |
| [U] Series merge blocked by lock_series -> on the review list (user decides) | ✅ | 3 cases in the library |
| [A] Negative cache for not-on-Audible books (re-check after ~90 days) | ⚠️ check | failed_history parks books after N attempts until REFRESH_DAYS – verify it matches the task |
| [U] Group confirmation (same-title books of one series) | ✅ | Realms of War = Spider Queen 1-6 |
| [U] Covers: Audible cover for verified ASINs, also replacing, backup + undo | ✅ | real ABS undo tested (byte-identical) |
| [U] ISBN = print edition (HC/Goodreads), Audible audio ISBN last fallback, ASIN never | ⚠️ 80 % | built + offline tested; live test with Hardcover open; then re-measure the 636 ISBN review entries |
| [U] Description update when ABS text is truncated/very short and Audible's is clearly complete | ❌ | |
| [U] Narrator == author hygiene (remove the wrong ABS narrator) | ❌ | search already ignores it |
| [U] Duplicate books report (same recording twice) in the UI | ❌ | data exists (library_report duplicate_recordings) |
| [U] Build-up / maintenance mode (~30 random books per day) + change monitoring (respect manual edits) | ❌ | |
| [U] Lock feature kept but de-emphasised; lock report | ⚠️ | locks respected everywhere; no report/UI |
| [U] Safe parallelism experiments (never >1 request per host) | ❌ | |

## Goodreads (experimental)
| Item | State | Note |
|---|---|---|
| [A] Sparse sampling (GOODREADS_SAMPLE), stop at first block, never bypass | ✅ | |
| [U] Hardcover Goodreads-ID path first | ✅ | correct 5/5; /book/show may be guarded more strictly than search |
| [U] Limit measurement until a reliable requests/hour + per night value | ⏳ | test 4: blocked after ~3 book pages; test 5: 6 books via search ok, block on the 7th BOOK page |
| [U] Open Library fallback for the Goodreads ID / ISBN | ❌ | measure first |
| [new] Search-list-only mode: rating from the SEARCH result row, never a /book/show page (GR_BOOK_PAGES=false) | ⏳ | probe 2026-11-04: rows carry 'x.xx avg rating — n ratings'; title-only query ranks the real book first, 'title + author' returns summaries. Patch ready, then live test ~25 books |
| [U] Production rhythm (≈25 per night?) | ❌ | after the measurements |

## Safety & tests
| Item | State | Note |
|---|---|---|
| [A] DRY_RUN default, journal for every write, undo | ✅ | |
| [A] Real write + undo end-to-end against the fake ABS | ✅ | + real pilot on 30 books (user OK) |
| [A] ABS backup before real runs | ✅ | run stops when it fails (user's ABS max backup size fixed) |
| [A] Golden list + record/replay + evaluate + regress gate | ✅ | whole library + all synthetic batches |
| [A] pytest suite | ❌ | tests are scripts run by tools/regress.sh, not bundled as pytest |
| [A] Review list small, only real doubt cases | ✅ | 8 entries for the whole library; wrong ISBNs without a replacement moved to reports/missing_isbn.json (status page), 529 offline - re-measure with Hardcover |
| [U] Review list: accept for ISBN and title kinds | ❌ | only ASIN accept works |

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
| – | Screenshots in docs/screenshots/ (no private titles, public repo) | ⚠️ | settings only |

## Web UI language
| Item | State | Note |
|---|---|---|
| [U] English web UI, English as DEFAULT, German selectable | ✅ | Settings > Interface; webui/i18n_de.py; tests/test_webui_i18n.py in the regress gate (every string translated, no German on English pages) |
| [U] App icon instead of the star, also as browser-tab favicon | ⏳ | SVG headphones + star drafted |

## Finish (Phase C end)
| Item | State |
|---|---|
| README for the new UI functions | ❌ |
| Change overview for a later merge (to web-ui/main) | ❌ |
| Final report in LOOP_LOG + to the user (numbers, changes, limits, first-run recommendation) | ❌ |
| CLAUDE.md / ROADMAP.md up to date with all learned rules | ⚠️ |
