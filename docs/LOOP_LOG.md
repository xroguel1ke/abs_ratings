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
