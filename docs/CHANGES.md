# Changes of `claude/matching-loop` (for the merge into `claude/web-ui` / `main`)

`claude/matching-loop` is 179 commits ahead of `claude/web-ui` and 0 behind, so it merges as a fast-forward.
Code: 66 files, +6128 / −475 lines. Test data: public synthetic batches in `tests/synthetic/`. Private data lives in `tests/private/`
(git-ignored).

## Matching (0 wrong matches is the rule; everything was measured on the whole library + fresh synthetic books)
- **Audible.** ASIN repair/search with an evidence gate:
  - all 8 stores, preferred store (`.com` English / `.de` German), twin listings;
  - series listings, group confirmation, messy file-name titles, language mismatch;
  - other edition: only with title and author fitting.
  - Negative cache: no product in any store = parked until `REFRESH_DAYS`.
- **Hardcover.**
  - Lookup order: identifier editions → title search → edition titles (translations).
  - Cross-checked against the Audible product. A main title beats an alternative title.
  - Number rules: Hardcover main title, ABS title, series-name titles, `Part N`.
  - Own daily budget: free 5000 / paid 50000. Shared cool-down, burst guard.
  - Contributor roles are learned (illustrators, translators).
- **Goodreads.**
  - Book page first (robots.txt allows it, budget 3 per scan), search list as fallback; AutoThrottle (jitter, slow answer, Retry-After). Parsers for the classic and the Next.js layout.
  - Series name + number + author confirm a volume, plus the series page.
  - Max 25 searches per run; stops at the first block.
- **Metadata.**
  - Recording details (narrators, publisher, year, abridged) only for the same recording (runtime within 10 min).
  - Cover for a verified ASIN, with backup and undo.
  - ISBN = the print edition (registration group must fit the language).
  - Audible description replaces only a truncated or very short ABS text.
  - Narrator = author hygiene.
- **Series and authors.**
  - Series merges with interleaving numbers; `lock_series` on either side goes to the review list.
  - Only real authors (role credits removed); empty authors removed.
  - Same-person check for Audible author records (translator chain bug).
  - "Merge as …" lets you choose the target name.

## Repair after an ABS rescan (2026-11-08)
- An ABS forced rescan with stale metadata.json files reverted ~1900 books. The script now repairs such books by itself, book by book:
  dead ASINs replaced (English -> audible.com, German -> audible.de, other stores only as fallback), non-Latin and folder-name series replaced
  by Audible's series, library series added back (Stormlight), series numbers shared by several books corrected.
- Only lock_ tags protect a field (a value changed in ABS is checked again). lock_series protects the book: its series becomes the merge target.
- Fixed: series task crash ("Series object has no attribute relation"), a thread race on role names, misleading "all due books" scan label.

## Processing
- **Per-source search state** (`source_state.json`):
  - found → `REFRESH_DAYS`;
  - missing → next day, 5 misses → pause;
  - not listed → pause at once;
  - a block is no miss.
  - Rating-only passes ask only the missing sources.
- **Parallel books** (`WORKERS=3`): max 1 request in flight per host, same author in one worker, per-book log blocks, thread-local
  search state. Progress is numbered by finished books.
- **Run mode** `auto` / `buildup` / `maintain`, **change monitoring** (`last_written.json`, `manual_fields.json`) and **quick mode**.
- **Remove-ratings task** (per source / all), journaled.

## Web UI
- English by default, German selectable.
- Pages: overview, books (certainty), book page (comparison, reasoning, manual fix, remove ratings), review list (books / series /
  authors / other: foreign-series books, fields you changed, duplicates), status (coverage + trend, linked gaps with re-check,
  Hardcover budget, lock report, CSV), scans (task checkboxes, remembered choices, backup checkbox, live progress, apply a test run
  selectively), history (red/green diff, undo).
- Settings: sections, sortable rating-block sources, icons / labels / position / minimum, genre rules, run mode, export/import.
- Design (unslop pass, after "unslop" by Damir Sokolovsky, CC BY 4.0): library-catalogue look with warm paper neutrals, one lamp-amber accent,
  Instrument Sans + Newsreader (OFL, bundled), lucide line icons instead of emoji/glyphs, a flat half-moon app mark, a moon-phase glyph
  for certainty, quiet tags instead of pills, 4 px spacing grid; version + update hint, dark mode, phone layout.

## Tests
`tools/regress.sh` is the gate:
- syntax;
- unit tests and pytest (8 files);
- per-source state and change monitoring (integration tests in real-run mode against the fake ABS);
- Hardcover golden (331 hand-checked matches);
- library-wide series/author decisions;
- every golden round (whole library + all synthetic batches) with 3 workers.

## Before the first real full run
1. ABS backup (automatic with `ABS_BACKUP`).
2. A test run of the whole library.
3. Review the planned changes ("Review test run") and apply selectively, or run for real.
