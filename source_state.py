"""Per-book, per-source search state (user rule 2026-11-04).

Every rating source (audible, hardcover, goodreads) is tracked on its own for every book:
  found   -> not asked again until REFRESH_DAYS have passed
  missing -> nothing found (uncertain): asked again in the next daily run; after MAX_MISSES in a row -> paused
  paused  -> not asked until REFRESH_DAYS have passed (5 misses, or 'absent': the source surely has no entry)
A blocked / deferred / disabled source changes nothing (no miss): the next run simply tries again.
A book is due when at least one ENABLED source is due; only the due sources are asked.
State file: source_state.json  {key: {source: {'st', 'ts', 'misses', 'why'}, 'aud': {compact Audible facts}}}
"""
from datetime import datetime, timedelta, timezone

SOURCES = ('audible', 'hardcover', 'goodreads')
MAX_MISSES = 5
RETRY_HOURS = 20  # 'next day': a daily run at the same time always qualifies


def _ts(s):
    try:
        d = datetime.fromisoformat(s)
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def is_due(entry, refresh_days, now=None):
    """entry: the state of ONE source of one book (or None = never asked)."""
    now = now or datetime.now(timezone.utc)
    if not entry: return True
    ts = _ts(entry.get('ts'))
    if ts is None: return True
    if entry.get('st') == 'missing': return now - ts >= timedelta(hours=RETRY_HOURS)
    return now - ts >= timedelta(days=refresh_days)  # found / paused


def due_sources(state, key, enabled, refresh_days, now=None):
    book = state.get(key) or {}
    return [s for s in enabled if is_due(book.get(s), refresh_days, now)]


def record(state, key, source, outcome, now=None, why=''):
    """outcome: 'found' | 'missing' | 'absent' | 'skipped' (blocked/deferred/disabled: nothing changes).
    Returns the new entry (or the unchanged one)."""
    book = state.setdefault(key, {})
    if outcome == 'skipped': return book.get(source)
    now_s = (now or datetime.now(timezone.utc)).isoformat(timespec='seconds')
    old = book.get(source) or {}
    if outcome == 'found':
        e = {'st': 'found', 'ts': now_s}
    elif outcome == 'absent':
        e = {'st': 'paused', 'ts': now_s, 'why': why or 'not listed'}
    else:
        n = (old.get('misses', 0) if old.get('st') == 'missing' else 0) + 1
        e = {'st': 'paused', 'ts': now_s, 'misses': n, 'why': f'{n} misses in a row'} if n >= MAX_MISSES else \
            {'st': 'missing', 'ts': now_s, 'misses': n}
    book[source] = e
    return e


def outcome_of(status, hits=None):
    """Map a lookup status ('found', 'not_found', 'blocked', 'deferred', 'disabled', 'fresh') to an outcome.
    not_found with 0 hits in every query = the source has no such book at all (absent)."""
    if status == 'found': return 'found'
    if status == 'not_found': return 'absent' if hits == 0 else 'missing'
    return 'skipped'


def seed(state, key, history_ts=None, audible_ok=False, gr_known=False, hc_known=False):
    """First run with this file: take over what the old files already know (history, Goodreads/Hardcover caches)."""
    if key in state or not history_ts: return
    book = state.setdefault(key, {})
    if audible_ok: book['audible'] = {'st': 'found', 'ts': history_ts}
    if gr_known: book['goodreads'] = {'st': 'found', 'ts': history_ts}
    if hc_known: book['hardcover'] = {'st': 'found', 'ts': history_ts}


AUD_FIELDS = ('asin', 'title_raw', 'subtitle_raw', 'authors_raw', 'narrators_raw', 'runtime_sec', 'domain')


def audible_facts(aud):
    """The few Audible facts a rating-only pass needs (Hardcover cross-check, Goodreads title)."""
    if not aud: return None
    out = {k: aud.get(k) for k in AUD_FIELDS if aud.get(k) is not None}
    mr = aud.get('meta_raw') or {}
    out['meta_raw'] = {'language': mr.get('language'), 'series': mr.get('series') or []}
    return out
