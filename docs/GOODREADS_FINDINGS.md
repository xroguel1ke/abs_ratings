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
| test 1, ~21:55 | 10 s (+ page time) | 4 (3 ok, 4th = challenge) | blocked: "Bot challenge (HTTP 202, waf=challenge)" on /book/show/ page; script stopped Goodreads for the run (GR_MAX_COOLDOWNS=0) |
Observation: far earlier than the ~40 requests noted in CLAUDE.md. Possible causes: server IP already flagged, static User-Agent/no cookies, /book/show/ pages guarded more strictly than search. NOT bypassed.
Next probes (planned): wait ≥45 min, 1 request -> if ok continue at 30-60 s intervals, log when/where it blocks and how long a block lasts.
