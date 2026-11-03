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
