"""Chapter check of the library (web UI scan task 'Chapters', user 2026-11-25): every book with an ASIN is compared with Audible's
chapter list; books without usable chapters - and books whose own marks are download split points - get Audible's chapters aligned on
the file's own pauses (chapter_audio.py). Only the audio is READ; chapters are written through abs_ratings.abs_set_chapters
(journaled, undo; a test run only plans them and can be applied afterwards).

Fast path: a book whose chapters already sit at Audible's chapter starts (abs_ratings.anchor_align) is done without touching the
audio. Audible's lists are cached for 30 days, the pauses per file for good (until the file changes).

Environment like abs_ratings.py (ABS_URL, API_TOKEN, LIBRARY_IDS, DRY_RUN, RUN_ID, ONLY_ITEMS); CHAPTER_WORKERS (3) = audio analyses at
the same time."""
import json, logging, os, re, sys, time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import abs_ratings as A
import chapter_audio as CA
from abs_series import load_full_items, expand_locks

AUD_CACHE_FILE = os.path.join(A.SCRIPT_DIR, 'cache', 'audible_chapters.json')
AUD_CACHE_DAYS = 30
# Audible chapters a file may lack without being incomplete (bonus material, credits; 2026-11-26: Witch King, Holly, Ascendant ...)
RE_EXTRA = re.compile(r"(?i)credits|bonus|excerpt|preview|sneak peek|story continues|bloopers|teaser|about the author|acknowledg|"
                      r"also by|coming soon|an interview|conversation (?:with|between)")
WORKERS = max(1, int(os.getenv('CHAPTER_WORKERS', 3) or 3))
# audio fingerprint per book at its last chapter check (CA.file_print): a replaced file is checked against its own pauses again, also when its
# (old) chapters still match Audible's list; CHAPTER_RECHECK=true does that for every book of the run
FILES_FILE = os.path.join(A.SCRIPT_DIR, 'cache', 'chapter_files.json')
CHANGED = 'the audio file was replaced since its chapters were checked'


def tags_of(d):
    md = (d.get('media') or {}).get('metadata') or {}
    raw = (d.get('tags') or []) + ((d.get('media') or {}).get('tags') or []) + (md.get('tags') or [])
    return expand_locks({t.strip() for t in raw if isinstance(t, str)})


def classify(abs_chs, dur, aud):
    """What a book needs: ('ok', why) = its marks already sit at Audible's chapter starts / nothing to compare; ('audio', why) = only the
    file's pauses can decide."""
    chs = aud[0]
    poor = A.chapters_poor(abs_chs, dur)
    if not poor:
        an = A.anchor_align(abs_chs, chs, dur)
        if an and abs(len(abs_chs) - len(chs)) <= max(1, len(chs) // 10):
            return 'ok', f"the file's chapters sit at Audible's chapter starts ({an[1]}/{an[2]})"
        if len(chs) < 4: return 'ok', 'Audible lists (almost) no chapters'
        return 'audio', f"{len(abs_chs)} own chapters, Audible {len(chs)}, marks not at Audible's starts"
    return 'audio', poor


def complete(key, dur, aud):
    """The file is as long as Audible's edition (at most 5 min shorter, the limit of abs_ratings' 'incomplete' check): an open 'incomplete'
    entry of the review list is obsolete (2026-11-26: books whose file the chain replaced stayed on the list - and in review.json for the chain)."""
    if aud and aud[0] and dur and sum(float(c[1]) for c in aud[0]) - dur <= 300: A.review_sweep(key, {'incomplete'})


def loose(d, chs0, dur, aud, sil, key):
    """CA.loose_chapters for a recording verified like abs_ratings verifies a cover (narrator + runtime +-2 min, abs_ratings.same_recording);
    the Audible product is only asked when the length already fits (a handful of books). (new, why) or None."""
    if abs(dur - sum(float(c[1]) for c in aud[0])) > CA.LOOSE_SECONDS: return None
    if len(chs0 or []) > 1 and dur / len(chs0) <= 3 * 3600: return None
    md = (d.get('media') or {}).get('metadata') or {}
    dom = ((A.rw_json(A.RESULTS_FILE) or {}).get(key) or {}).get('domain') or 'www.audible.com'
    p = A.get_audible_api(md.get('asin'), dom)
    if not isinstance(p, dict) or not A.same_recording(dur, md.get('narrators') or [], p):
        logging.info(f"   {md.get('title')}: the file is as long as Audible's chapter list, but the recording is not verified (narrator/runtime)")
        return None
    new, why = CA.loose_chapters(chs0, dur, aud, sil)
    return (new, why) if new else None


def main():
    A.setup_logging()
    A.LAST_WRITTEN.update(A.rw_json(A.LAST_WRITTEN_FILE) or {})
    t0 = time.time()
    logging.info('=' * 60)
    logging.info(f"Chapter check {A.RUN_ID}{' (test run - nothing is written)' if A.DRY_RUN else ''}")
    if not CA.available():
        logging.error("ffmpeg is missing in this container - the chapter check needs it"); return 2
    if not A.ABS_URL or not A.API_TOKEN or not A.LIBRARY_IDS:
        logging.error("ABS_URL, API_TOKEN or LIBRARY_IDS missing"); return 2
    results = A.rw_json(A.RESULTS_FILE) or {}
    cache = A.rw_json(AUD_CACHE_FILE) or {}
    only = {k for k in (os.getenv('ONLY_ITEMS') or '').split(',') if k}
    books = []
    for lib in A.LIBRARY_IDS:
        try: items = A.abs_session.get(f"{A.ABS_URL}/api/libraries/{lib}/items").json()['results']
        except Exception as e: logging.error(f"Library {lib} not loaded: {e}"); continue
        if only: items = [it for it in items if f"{lib}_{it['id']}" in only]
        for it, d, err in load_full_items(A.abs_session, A.ABS_URL, items):
            if isinstance(d, dict): books.append((lib, d))
    logging.info(f"{len(books)} books loaded")
    def flag(key, d, kind, reason, pre, **kw):
        logging.info(f"{pre}: 🔁 {'would be handed' if A.DRY_RUN else 'handed'} to the audiobook chain for a redownload ({kind}: {reason})")
        A.redownload_flag(key, d, kind, reason, **kw)

    st = {'redownload': 0, 'checked': 0, 'no_asin': 0, 'locked': 0, 'no_audible': 0, 'ok': 0, 'written': 0, 'kept': 0, 'not_aligned': 0, 'unreadable': 0}
    todo = []
    prints, seen = A.rw_json(FILES_FILE) or {}, {}
    replaced = set()  # keys of this run whose audio file changed
    force = os.getenv('CHAPTER_RECHECK', '').lower() == 'true'
    for n_, (lib, d) in enumerate(books, 1):
        if n_ % 250 == 0:
            logging.info(f"   {n_}/{len(books)} books compared with Audible's chapter list"); A.rw_json(AUD_CACHE_FILE, cache)  # progress + cache kept
        key, md, media = f"{lib}_{d['id']}", (d.get('media') or {}).get('metadata') or {}, d.get('media') or {}
        asin, dur, chs0 = md.get('asin'), float(media.get('duration') or 0), media.get('chapters') or []
        title = md.get('title') or '?'
        fp = CA.file_print(d)
        changed = force or bool(fp and prints.get(key) and prints[key] != fp)
        if fp: seen[key] = fp  # (stored at the end of a real run: a replaced file is looked at once, not in every check)
        if not asin or not dur: st['no_asin'] += 1; continue
        if 'lock_chapters' in tags_of(d): st['locked'] += 1; continue
        st['checked'] += 1
        c = cache.get(asin)
        if not c or (time.time() - c.get('ts', 0)) > AUD_CACHE_DAYS * 86400:
            dom = (results.get(key) or {}).get('domain') or 'www.audible.com'
            aud = A.audible_chapters(asin, dom)
            c = cache[asin] = {'ts': int(time.time()), 'aud': aud}
        aud = c.get('aud')
        if not aud or not aud[0]: st['no_audible'] += 1; continue
        what, why = classify(chs0, dur, aud)
        if what == 'ok' and changed: what, why = 'audio', f"{CHANGED} ({why})"
        if changed: replaced.add(key)
        if what == 'ok':  # (a file without Audible's bonus excerpt / end credits is no redownload: Witch King, Holly ... 2026-11-26)
            st['ok'] += 1; A.redownload_clear(key); complete(key, dur, aud); continue
        todo.append((key, d, title, chs0, dur, aud, why))
    A.rw_json(AUD_CACHE_FILE, cache)
    logging.info(f"{st['ok']} books already have Audible's chapters; {len(todo)} are checked against their audio ({WORKERS} at a time)")
    if os.getenv('CHAPTER_CHECK_PLAN_ONLY', '').lower() == 'true':  # count only (no audio is read)
        from collections import Counter
        for why, n_ in Counter(re.sub(r'\d+', 'N', w) for *_, w in todo).most_common(): logging.info(f"   {n_:5} {why}")
        return 0

    def work(job):
        key, d, title, chs0, dur, aud, why0 = job
        try:
            sil = CA.book_silences(d, A.SILENCE_CACHE_DIR)
            if sil is None: return job, None, None, 'the audio cannot be read here'
            new, why = CA.decide(chs0, dur, aud, sil)
            if not new and 'do not line up' in why and (lc := loose(d, chs0, dur, aud, sil, key)):
                new, why = lc  # 2026-11-26: same master without telling pauses - Audible's chapters as listed (verified recording only)
            return job, sil, new, why
        except Exception as e:  # one book must never stop the check of all others
            logging.exception(f"   {title}: chapter check failed")
            return job, None, None, f'error: {type(e).__name__}: {e}'[:200]

    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for job, sil, new, why in ex.map(work, todo):
            key, d, title, chs0, dur, aud, why0 = job
            done += 1
            pre = f"({done}/{len(todo)}) {title} [{len(chs0)} chapters, {why0}]"
            if sil is None: st['unreadable'] += 1; logging.info(f"{pre}: ⚠️ {why}"); continue
            if not new:
                st['kept' if 'kept' in why or 'no names either' in why else 'not_aligned'] += 1
                logging.info(f"{pre}: ℹ️ {why}")
                poor = A.chapters_poor(chs0, dur)
                tot, sh = sum(float(c[1]) for c in aud[0]), CA.on_pause_share(chs0, sil)
                if key in replaced and 'kept' not in why and sh < 0.6:
                    moved, mwhy = CA.realign_own(chs0, dur, sil)  # the old chapters are good, only at the old file's times
                    if not moved:
                        fch = [c for f in sorted((d.get('media') or {}).get('audioFiles') or [], key=lambda f: f.get('index') or 0)[:1]
                               for c in f.get('chapters') or []] if len((d.get('media') or {}).get('audioFiles') or []) == 1 else []
                        moved, mwhy = CA.realign_on_marks(chs0, fch, dur)
                    if moved and A.abs_set_chapters(d['id'], key, title, moved, chs0, 'chapter_check'):
                        st['written'] += 1; st['kept' if 'kept' in why or 'no names either' in why else 'not_aligned'] -= 1
                        A.redownload_clear(key); complete(key, dur, aud)
                        logging.info(f"{pre}: 📑 {'would move' if A.DRY_RUN else 'moved'} the {len(moved)} chapters onto the new file ({mwhy})"); continue
                if poor == 'chapters run past the end of the file' and ('kept' in why or sh >= 0.6) and abs(tot - dur) <= 300:
                    # the own marks sit at the file's pauses, only the last one ends after the (complete) file: its end is cut to the file's
                    # end (2026-11-26 'Stardust': the chain's better file is 1.6 min shorter than the old one - no redownload for that)
                    cut = [dict(c, end=min(float(c.get('end') or 0), dur)) for c in chs0 if float(c.get('start') or 0) < dur - 1]
                    if A.abs_set_chapters(d['id'], key, title, cut, chs0, 'chapter_check'):
                        st['written'] += 1; st['kept' if 'kept' in why or 'no names either' in why else 'not_aligned'] -= 1
                        A.redownload_clear(key); complete(key, dur, aud)
                        logging.info(f"{pre}: 📑 {'would cut' if A.DRY_RUN else 'cut'} the last chapter at the file's end ({len(cut)} chapters kept, they sit at its pauses)")
                    continue
                if poor and poor != 'only numbered chapters without names' and 'too few chapters' not in why:  # ('Guns': one chapter is all there is)
                    # user 2026-11-26: unusable chapters that no Audible list fits - the FILE is the problem: handed to the chain
                    miss = round((sum(float(c[1]) for c in aud[0]) - dur) / 60)
                    if miss >= 5:  # the file is shorter than Audible's edition: content is missing - the chain must check the runtime, not only chapters
                        # (2026-11-26 'Legends & Lattes': listed as 'chapters', the chain swapped in another 382-min file of a 439-min book)
                        flag(key, d, 'incomplete', f"the file is about {miss} min shorter than Audible's edition ({poor})", pre, missing_min=miss)
                    else: flag(key, d, 'chapters', f"{poor}; {why}", pre)
                    st['redownload'] += 1
                else: A.redownload_clear(key, {'chapters'}); complete(key, dur, aud)
                if key in replaced and 'kept' not in why and sh < 0.6:
                    # the old chapter times do not fit the new file and Audible's list cannot be laid on it: the user is told
                    logging.info(f"{pre}: ⚠️ the old chapters sit at only {sh:.0%} of the new file's pauses - review list")
                    A.add_review(key, title, 'chapters', 'replaced_file', f"the audio file was replaced and its {len(chs0)} chapters sit at only {sh:.0%} of "
                                 f"its pauses (shifted?) - Audible's chapter list does not fit the new file either; check them in ABS (Edit > Chapters)")
                continue
            if A.abs_set_chapters(d['id'], key, title, new, chs0, 'chapter_check'):
                st['written'] += 1
                logging.info(f"{pre}: 📑 {'would take' if A.DRY_RUN else 'took'} {len(new)} chapters from Audible ({why})")
                kept_t = {c['title'] for c in new}
                lost = [c[2] for c in aud[0] if c[2] not in kept_t and not RE_EXTRA.search(c[2] or '')]  # story chapters the file lacks
                gone = len(lost); miss = round((sum(float(c[1]) for c in aud[0]) - dur) / 60)
                if gone > 0 and miss >= 5:  # Audible's last chapters are not in the file: it lacks its end
                    flag(key, d, 'incomplete', f"Audible's last {gone} chapter(s) are not in the file, about {miss} min shorter than Audible's edition", pre, missing_min=miss)
                    st['redownload'] += 1
                else: A.redownload_clear(key); complete(key, dur, aud)
    if not A.DRY_RUN and seen:
        cur = A.rw_json(FILES_FILE) or {}; cur.update(seen); A.rw_json(FILES_FILE, cur)
    logging.info('-' * 60)
    logging.info(f"Checked {st['checked']} books: {st['ok']} already fine, {st['written']} chapters {'planned' if A.DRY_RUN else 'written'}, "
                 f"{st['kept']} own chapters kept, {st['not_aligned']} not safely alignable, {st['unreadable']} audio not readable, "
                 f"{st['no_audible']} without Audible chapters, {st['locked']} locked, {st['no_asin']} without ASIN, "
                 f"{st['redownload']} for a redownload (redownload.json)")
    try: A.export_review()
    except Exception as e: logging.warning(f"review.json not written ({type(e).__name__}: {e})")
    logging.info(f"Duration: {A.format_time(time.time() - t0)}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
