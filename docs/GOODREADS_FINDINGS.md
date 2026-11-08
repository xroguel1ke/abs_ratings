# Goodreads findings (experiment log)
Goal: reliable Goodreads ratings with as few requests as possible, never bypassing the bot protection. Stop at the first block; next batch tries again.
Method (option 1): Hardcover `book_mappings` (platform 1) -> `https://www.goodreads.com/book/show/<id>` -> verify page title+author -> rating (work level, any edition page).
Option 2 (fallback, unchanged): ISBN link / text search.

## Correctness (test 1, 2026-11-03)
3 pages read, 3 correct: Project Hail Mary 4.51 (1,928,394 ratings, via the 2nd ID – the 1st ID 'Ave Maria' was rejected by the title check, title 0.38), Casual Farming 4 4.42 (326).
Edition language does not matter (Portuguese edition page of Project Hail Mary carries the work rating).

## Rate limit measurements
| time | interval | requests before block | result |
|---|---|---|---|
| test 1, ~20:40 (CET) | 10 s (+ page time) | 4 (3 ok, 4th = challenge) | blocked: "Bot challenge (HTTP 202, waf=challenge)" on /book/show/ page; script stopped Goodreads for the run (GR_MAX_COOLDOWNS=0) |
Observation: far earlier than the ~40 requests noted in CLAUDE.md. Possible causes: server IP already flagged, static User-Agent/no cookies, /book/show/ pages guarded more strictly than search. NOT bypassed.
Next probes (planned): wait ≥45 min, 1 request -> if ok continue at 30-60 s intervals, log when/where it blocks and how long a block lasts.

## History from the user's real logs (analysed 2026-11-03)
| period | Goodreads checks | result |
|---|---|---|
| 2026-10-20 .. 11-02, nightly runs | 25-30 per night | no block |
| 2026-11-03 02:33-05:46 (dev runs) | ~130 in ~3 h (68 in the first 35 min) | first block 03:10, more blocks later |
| 2026-11-03 ~20:40 (our test 1, 15 h later) | 4 | 3 ok, 4th = WAF challenge |
Interpretation (hypothesis): per-IP accumulation over hours; ~25-30 per night is tolerated; a burst puts the IP on a watch list that decays slowly (> 15 h). Not a general tightening by Goodreads.
Plan: pause Goodreads tests, then a few requests per day; production default ≈ 25 Goodreads requests per run (GOODREADS_SAMPLE chosen from the batch size), stop at first block.

## Probe 2 (2026-11-03 21:28, ~1 h after the block, 45 s interval, REUSE mode)
First LIVE request was blocked at once (HTTP 202 bot challenge). The 3 earlier pages came from our recordings (no request). => the block lasts > 1 h (first block ~20:40).
Next probe: tomorrow morning, ONE request. No further Goodreads traffic until then.

## Probe 3 (2026-11-03 23:03): ONE plain request to /book/show/213285211 -> HTTP 200, full page (310 KB), no challenge.
The block that started ~20:40 (test 1) was lifted within <= 2 h 23 min (probe 2 at 21:28 was still blocked). Next: 6 books via Hardcover IDs at 60 s interval (stop at the first block).

## Test 3 result (2026-11-03 23:05-23:15): 6 books via Hardcover IDs, 60 s interval, NO block; 5 pages read, all correct
Promise of Blood 4.13 (66,533), Rise of the Living Forge 5 4.67 (970), The Shadow of the Gods 4.17 (121,428), In the Labyrinth of Drakes 4.39 (10,020), Death's End 4.40 (190,322). Title/author verification passed every time (title score ≥ 0.81).
Next steps (carefully): 15 books at 45-60 s, later one small sample per run (≈25/night was fine for 13 days). The block of 20:40 lifted within ≤ 2 h 23 min.

## Test 4 (2026-11-04 13:58): 15 library books, Hardcover-ID path first, 60 s interval, only Goodreads live (Audible/Hardcover from recordings), stop at the first block
Result: 13:58:57 1st page ok (Have Sword, Will Travel 3.76/1270, correct); ~2-3 live /book/show/ requests at 60 s, then 14:01:54 WAF bot challenge (HTTP 202) -> Goodreads stopped for the run (as designed). No Goodreads traffic since test 3 (23:15 yesterday, 6 ok).
Hypothesis: /book/show/ pages (Hardcover-ID path) are guarded more strictly than /search (the user's nightly runs with 25-30 mostly-search requests ran 13 days without a block). Next probe >= 16:30: search path only (GR_TRIES=0), few books, 60 s.

## Test 5 (2026-11-04 16:32): search path only (GR_TRIES=0), 8 library books, 60 s interval, only Goodreads live
Result: 16:32-16:39 six books via SEARCH (search page + book page each, ~12 requests at 60 s) ok; 16:40 the 7th book page (/book/show) got the WAF challenge -> stopped. Test 4 (ID path, book pages only) was blocked after 2-3 pages.
Interpretation: the /book/show pages are guarded much more strictly than /search. IDEA: take the rating from the search result row (avg rating + count are shown there) when title + author verify -> ONE request per book on the less guarded endpoint, no book page. Next probe: >= 3 h later, search-page-only parsing.

## 2026-11-08: robots.txt + book page first, search as fallback (user decision)
robots.txt (User-agent: *) allows /book/show and /book/isbn, disallows /search and /work. New order: book page (cached URL,
Hardcover Goodreads ID, ISBN page) with its own budget GOODREADS_PAGES_PER_RUN=3; a challenge on a book page only switches pages
off for the run, the search list (max 25) stays the fallback. AutoThrottle after Scrapling's idea: jitter up to 40 % of the
interval, an answer slower than 3 s widens the interval x1.5 (max 6x), Retry-After wins over the fixed cool-down.
Not built: browser (image size, user decision) and TLS/fingerprint impersonation (Claude Code's safety check blocked it).
Live test (8 library books, dry run, 10 s): 3 book pages ok (one Greek edition page rejected by the title check), budget reached,
5 via the search list; 8/8 Goodreads ratings, no challenge.
