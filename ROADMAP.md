# Roadmap – Docker / Web UI

Status: ✅ done (branch `claude/web-ui`) · 🔜 next · ⏳ later

## 1. Container & updates
- ✅ Dockerfile (python 3.11-slim, pinned deps), healthcheck (`/health` → ABS `/ping`)
- ✅ GitHub Action → `ghcr.io/xroguel1ke/abs_ratings:web-ui` / `:latest` (main) + `:<tag>-<sha>`
- ✅ Unraid template `unraid/abs-ratings.xml` (port 47823, `/config` = existing abs_scripts folder)
- ✅ Version display (footer) + "Update available" hint (GitHub, at most every 6 h)

## 2. Login
- ✅ Password `admin`, forced change on first login, hash only

## 3. Settings
- ✅ All script options with help texts, ABS connection test, library picker from ABS, Hardcover token + test
- ✅ Settings export/import (without tokens, password, notification URL)

## 4. Ratings
- ✅ Hardcover on/off, `RATING_ORDER` (text field, e.g. `audible,goodreads,hardcover`)
- ✅ Sources shown + order in the UI (tick + up/down)
- ✅ Hardcover module: manual link → cached id (bulk refresh, 100 per request) → ASIN/ISBN edition → title search (strict:
  author, pure title ratio ≥0.85, volume numbers, no box sets/compilations/manga; series number confirms or rejects weak titles).
  Library: 1854/1913 found, Phase C hand-checked; limits 60/min, own daily budget 5000 (free) / 50000 (paid checkbox).
- ✅ Format: moons/stars, DE/EN labels, block top/bottom, minimum rating count (live preview and a rating tag: not built)

## 5. Schedule
- ✅ Simple (weekdays + time) or cron toggle with "next run" check, never two runs at once
- ✅ Quick mode: look for never-checked books every X minutes and process them at once (QUICK_MODE_MINUTES)

## 6. Runs
- ✅ Start / dry run / series & report only / stop, run history, live log, changes table per run
- ✅ ABS backup before real runs (`ABS_BACKUP`, setting on by default, checkbox on the scan page) – verified on the real server
- ✅ Apply a dry run: review planned changes grouped/filterable, then write exactly those (no new Audible requests)

## 7–8. Dashboard & problems
- ✅ Tiles (checked, ok, not found, other edition, no ratings yet, ignored), last/next run, Goodreads state
- ✅ Problem list with reason + filter/search, detail page with log excerpt of the last check
- ✅ Manual fix: one smart field (ASIN / Audible link any store / Goodreads / Hardcover link) or separate fields,
  apply or test first, live log + old→new table, manual values are permanent (`overrides.json`), ignore/unignore
- ✅ Goodreads/Hardcover status (status page: budget used/left, reset, cache sizes); per-source search state decides what is re-checked

## 9–10. Merged & merge review
- ✅ Merged tab (series + authors) with evidence, "✓ passt" (hides until new books are merged), "↶ rückgängig"
  (undo via journal `merge_id`, removes alias, remembered as "undone")
- ✅ Review tab: uncertain series (not enough numbers, ambiguous target, numbers don't complement) and authors
  (first name vs initial) → merge / keep separate; decisions honored by the series task
- ✅ Choose the target name in the review ("Merge as ...")

## 11. Single book
- ✅ Search any book (books page, header search) and run test/real for it

## 12. Logs
- ✅ List, view with live follow + filter, download single file, debug_latest.log button, ZIP of everything

## 13. Report & history
- ✅ Library report opens from the dashboard
- ✅ Report inside the UI: status page gaps link to the books (ABS link + test/real there) with a "check these books again" button
- ✅ Change history per book / per run with undo per change, book or whole run; removed parts red, added parts green

## 14. Rules
- ✅ Genre rules (rename / block) in the settings (DE→EN map stays built in)
- ✅ Author exceptions: "keep separate" in the review list + same-person check for Audible author records
- ✅ Duplicates (same recording / same ASIN twice), show only: review list Other

## 15. Statistics
- ✅ CSV export of all books (status page); coverage per source + trend on the status page

## 16. Notifications & UI
- ✅ Apprise URL (ntfy, Discord, Telegram, mail …), test button, errors only / every run
- ✅ Light/dark, mobile friendly
- ✅ English UI by default, German selectable

## Script findings still open
- ✅ Books with no search results (Pern): found via the Audible series listing
- ✅ `POST /api/backups` and cover upload verified against the real ABS (pilot + real runs, undo byte-identical)

## Web UI design (user wish, 2026-11-03) – ✅ done
- Orientation: Google Material Design (Material 3 principles), light + dark mode (system preference + manual switch).
- Light: pastel white surfaces with a creamy contrast colour. Dark: two pastel greys (background + raised surfaces = tonal elevation, no heavy shadows).
- Claude's additions: one muted accent colour for actions/active states; clear but muted status colours (found / warning / error / review); contrast ≥ WCAG AA, especially light mode; no pure black/white.
- Before the rebuild: small preview page with 2-3 colour variants for the user to choose from.

## Web UI onboarding (user wish, 2026-11-03) – ✅ done
- Short guided tour that starts on the FIRST visit (after the forced password change) and walks through the pages and their functions
  (dashboard/problems, review list, merges, runs, logs, settings, start dialog).
- Re-startable any time from the settings page ("Einführung erneut starten").
- Same design language as the rest of the UI (Material style, light/dark, see "Web UI design"); skippable, progress dots, remembers "completed" in settings.json.

## Run mode "build up, then maintain" + change monitoring (user wish, 2026-11-03) – ✅ done
1. **Mode instead of a fixed schedule:** (a) build-up: process the whole library over several runs until every book has been completed once; (b) maintenance: new books always, plus ~30 random books per day (configurable) which then count as processed for N days. Scheduler in the web UI just triggers it.
2. **Change monitoring:** per book remember what the script last wrote (fingerprint of title/ASIN/series/authors/rating block, `last_written.json`). Each run compares with the library list (1 request).
   - field went back to its OLD value from `changes.jsonl` (e.g. ABS rescan restores a file-name title) -> automatic overwrite -> re-check and re-apply;
   - field has a NEW value that is neither old nor ours -> assume a manual change by the user: respect it, mark the book "manually maintained" (optionally set the lock tag), never overwrite.
   - Limit: ABS does not record who changed a field, so this is a conservative rule (doubt = user's change), not proof.
3. Web UI: "recently changed by someone else" list, setting for the daily sample size and the build-up/maintenance switch.

## Certainty score per book (user wish, 2026-11-03) – ✅ done (book page + sortable book list)
- 0-100 per processed book, stored in `results.json` (+ the evidence list that produced it), shown on the book page and as a sortable column in the book list (web UI).
- Weights (proposal): title 25 (exact/very similar), author 20, runtime 20 (±2 min) or 10 (inside tolerance), narrator 12, series/volume number 10, publisher+year 8, Hardcover confirmation (ISBN/ASIN) 5.
  Deductions: ASIN found by search instead of kept from ABS -5, "other edition" -20, only one rating source -5, hit via ISBN field or series listing -5, near-tolerance runtime path -5.
  Review-list cases score low automatically; unchanged, product-confirmed ASINs score high.
- The evidence list already exists in the log (`evidence: title, author, ...`) and is stored in `LAST_MATCH`; the score is computed from it, so no new matching logic is needed.

## Description update when Audible's is much more complete (user wish, 2026-11-03) – ✅ done (85 library books: chapter names, placeholders, truncated texts)
Today: description filled only when empty (rating block aside). New: replace the ABS description (without our rating block) by Audible's summary ONLY if it is provably not the user's own text:
(a) the ABS text is a prefix / truncated part of Audible's text, or (b) it is very short (< 30 % of Audible's length and < 300 characters).
Never when `lock_description` is set; journaled old -> new (undo); rating block stays on top. A long self-written description is never replaced. Tests: truncated -> replaced, own long text -> kept, locked -> kept.
Rating blocks (already so): per source, new ratings replace the old block of that source; if a source finds nothing this run, the old block of that source is kept.

- [x] Goodreads fallback without search (user idea 2026-11-04): when Hardcover has no Goodreads ID, ask Open Library (search by title+author, verified) for the Goodreads ID (identifiers.goodreads) or an ISBN -> /book/show/<id> or /book/isbn/<isbn> = 1 Goodreads request instead of 2. First measure how many Hardcover gaps Open Library fills; title/author verification on the Goodreads page as today.
- [x] Goodreads limit measurements continue in parallel (own host) until a reliable requests/hour and requests/night value exists (docs/GOODREADS_FINDINGS.md).
