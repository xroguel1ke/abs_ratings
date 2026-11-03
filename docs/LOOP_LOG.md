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
