# Roadmap – Docker / Web UI

Status: ✅ done (branch `claude/web-ui`) · 🔜 next · ⏳ later

## 1. Container & updates
- ✅ Dockerfile (python 3.11-slim, pinned deps), healthcheck (`/health` → ABS `/ping`)
- ✅ GitHub Action → `ghcr.io/xroguel1ke/abs_ratings:web-ui` / `:latest` (main) + `:<tag>-<sha>`
- ✅ Unraid template `unraid/abs-ratings.xml` (port 47823, `/config` = existing abs_scripts folder)
- ⏳ Version display + "newer image available" hint (`APP_VERSION` env is already set)

## 2. Login
- ✅ Password `admin`, forced change on first login, hash only

## 3. Settings
- ✅ All script options with help texts, ABS connection test, library picker from ABS, Hardcover token + test
- ⏳ Settings export/import (without tokens)

## 4. Ratings
- ✅ Hardcover on/off, `RATING_ORDER` (text field, e.g. `audible,goodreads,hardcover`)
- 🔜 Sources on/off + drag & drop order in the UI
- ✅ Hardcover module: manual link → cached id (bulk refresh, 100 per request) → ASIN/ISBN edition → title search (strict:
  author, pure title ratio ≥0.85, volume numbers, no box sets/compilations/manga; series number confirms or rejects weak titles).
  Tested on ~200 books: ~89% found, no wrong hits left in the sample. Limits 60/min, 5000/day (stops with 25 left).
- 🔜 Format: moons/stars, counts, DE/EN labels, block top/bottom, live preview; minimum rating count; optional `rating:4.5+` tag

## 5. Schedule
- ✅ Simple (weekdays + time) or cron toggle with "next run" check, never two runs at once
- ⏳ Quick mode: poll ABS every X minutes and process new books immediately

## 6. Runs
- ✅ Start / dry run / series & report only / stop, run history, live log, changes table per run
- ✅ ABS backup before real runs (`ABS_BACKUP`, setting on by default) – verify on the real server
- 🔜 Apply a dry run: review planned changes grouped/filterable, then write exactly those (no new Audible requests)

## 7–8. Dashboard & problems
- ✅ Tiles (checked, ok, not found, other edition, no ratings yet, ignored), last/next run, Goodreads state
- ✅ Problem list with reason + filter/search, detail page with log excerpt of the last check
- ✅ Manual fix: one smart field (ASIN / Audible link any store / Goodreads / Hardcover link) or separate fields,
  apply or test first, live log + old→new table, manual values are permanent (`overrides.json`), ignore/unignore
- ⏳ Goodreads/Hardcover status tiles with numbers (budget used, cache size), queue of books waiting for re-check

## 9–10. Merged & merge review
- ✅ Merged tab (series + authors) with evidence, "✓ passt" (hides until new books are merged), "↶ rückgängig"
  (undo via journal `merge_id`, removes alias, remembered as "undone")
- ✅ Review tab: uncertain series (not enough numbers, ambiguous target, numbers don't complement) and authors
  (first name vs initial) → merge / keep separate; decisions honored by the series task
- ⏳ Choose the target name in the review (currently: the series with more books, as agreed)

## 11. Single book
- 🔜 Search any book (not only problems) and run test/real for it (backend: `ONLY_ITEMS` exists)

## 12. Logs
- ✅ List, view with live follow + filter, download single file, debug_latest.log button, ZIP of everything

## 13. Report & history
- ✅ Library report opens from the dashboard
- 🔜 Report inside the UI with links to ABS items and "process again" buttons
- 🔜 Change history per book / per run with undo per change or whole run (backend: `abs_undo.py`)

## 14. Rules
- ⏳ Genre rules (rename / block, e.g. "Movie, TV & Video Game Tie-Ins"), editable DE→EN genre map
- ⏳ Author exceptions (similar names that are different people) – partly covered by review "keep separate"
- ⏳ Duplicates page (same ASIN twice), show only

## 15. Statistics
- ⏳ CSV export of the library with ratings, rating distribution, coverage per source, history

## 16. Notifications & UI
- ✅ Apprise URL (ntfy, Discord, Telegram, mail …), test button, errors only / every run
- ✅ Light/dark, mobile friendly
- ⏳ English UI (currently German)

## Script findings still open
- Books with no search results at all (e.g. some Pern titles): check search queries with `LOG_LEVEL=DEBUG` on the real server.
- Verify `POST /api/backups` and cover upload (`POST /api/items/<id>/cover`) against the real ABS version.

## Web UI design (user wish, 2026-11-03) – TODO, after the functional web UI points
- Orientation: Google Material Design (Material 3 principles), light + dark mode (system preference + manual switch).
- Light: pastel white surfaces with a creamy contrast colour. Dark: two pastel greys (background + raised surfaces = tonal elevation, no heavy shadows).
- Claude's additions: one muted accent colour for actions/active states; clear but muted status colours (found / warning / error / review); contrast ≥ WCAG AA, especially light mode; no pure black/white.
- Before the rebuild: small preview page with 2-3 colour variants for the user to choose from.

## Web UI onboarding (user wish, 2026-11-03) – TODO, after the functional web UI points
- Short guided tour that starts on the FIRST visit (after the forced password change) and walks through the pages and their functions
  (dashboard/problems, review list, merges, runs, logs, settings, start dialog).
- Re-startable any time from the settings page ("Einführung erneut starten").
- Same design language as the rest of the UI (Material style, light/dark, see "Web UI design"); skippable, progress dots, remembers "completed" in settings.json.

## Run mode "build up, then maintain" + change monitoring (user wish, 2026-11-03) – TODO (after matching is production ready)
1. **Mode instead of a fixed schedule:** (a) build-up: process the whole library over several runs until every book has been completed once; (b) maintenance: new books always, plus ~30 random books per day (configurable) which then count as processed for N days. Scheduler in the web UI just triggers it.
2. **Change monitoring:** per book remember what the script last wrote (fingerprint of title/ASIN/series/authors/rating block, `last_written.json`). Each run compares with the library list (1 request).
   - field went back to its OLD value from `changes.jsonl` (e.g. ABS rescan restores a file-name title) -> automatic overwrite -> re-check and re-apply;
   - field has a NEW value that is neither old nor ours -> assume a manual change by the user: respect it, mark the book "manually maintained" (optionally set the lock tag), never overwrite.
   - Limit: ABS does not record who changed a field, so this is a conservative rule (doubt = user's change), not proof.
3. Web UI: "recently changed by someone else" list, setting for the daily sample size and the build-up/maintenance switch.
