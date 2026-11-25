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
WORKERS = max(1, int(os.getenv('CHAPTER_WORKERS', 3) or 3))


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
    st = {'checked': 0, 'no_asin': 0, 'locked': 0, 'no_audible': 0, 'ok': 0, 'written': 0, 'kept': 0, 'not_aligned': 0, 'unreadable': 0}
    todo = []
    for n_, (lib, d) in enumerate(books, 1):
        if n_ % 250 == 0:
            logging.info(f"   {n_}/{len(books)} books compared with Audible's chapter list"); A.rw_json(AUD_CACHE_FILE, cache)  # progress + cache kept
        key, md, media = f"{lib}_{d['id']}", (d.get('media') or {}).get('metadata') or {}, d.get('media') or {}
        asin, dur, chs0 = md.get('asin'), float(media.get('duration') or 0), media.get('chapters') or []
        title = md.get('title') or '?'
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
        if what == 'ok': st['ok'] += 1; continue
        todo.append((key, d, title, chs0, dur, aud, why))
    A.rw_json(AUD_CACHE_FILE, cache)
    logging.info(f"{st['ok']} books already have Audible's chapters; {len(todo)} are checked against their audio ({WORKERS} at a time)")
    if os.getenv('CHAPTER_CHECK_PLAN_ONLY', '').lower() == 'true':  # count only (no audio is read)
        from collections import Counter
        for why, n_ in Counter(re.sub(r'\d+', 'N', w) for *_, w in todo).most_common(): logging.info(f"   {n_:5} {why}")
        return 0

    def work(job):
        key, d, title, chs0, dur, aud, why0 = job
        sil = CA.book_silences(d, A.SILENCE_CACHE_DIR)
        if sil is None: return job, None, None, 'the audio cannot be read here'
        new, why = CA.decide(chs0, dur, aud, sil)
        return job, sil, new, why

    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for job, sil, new, why in ex.map(work, todo):
            key, d, title, chs0, dur, aud, why0 = job
            done += 1
            pre = f"({done}/{len(todo)}) {title} [{len(chs0)} chapters, {why0}]"
            if sil is None: st['unreadable'] += 1; logging.info(f"{pre}: ⚠️ {why}"); continue
            if not new:
                st['kept' if 'kept' in why or 'no names either' in why else 'not_aligned'] += 1
                logging.info(f"{pre}: ℹ️ {why}"); continue
            if A.abs_set_chapters(d['id'], key, title, new, chs0, 'chapter_check'):
                st['written'] += 1
                logging.info(f"{pre}: 📑 {'would take' if A.DRY_RUN else 'took'} {len(new)} chapters from Audible ({why})")
    logging.info('-' * 60)
    logging.info(f"Checked {st['checked']} books: {st['ok']} already fine, {st['written']} chapters {'planned' if A.DRY_RUN else 'written'}, "
                 f"{st['kept']} own chapters kept, {st['not_aligned']} not safely alignable, {st['unreadable']} audio not readable, "
                 f"{st['no_audible']} without Audible chapters, {st['locked']} locked, {st['no_asin']} without ASIN")
    logging.info(f"Duration: {A.format_time(time.time() - t0)}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
