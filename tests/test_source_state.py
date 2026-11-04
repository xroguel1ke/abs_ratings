"""Per-source search state (user rule): found -> wait REFRESH_DAYS, missing -> next day, 5 misses or absent -> pause.
Run: python tests/test_source_state.py"""
import os, sys
from datetime import datetime, timedelta, timezone
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
import source_state as S

T0 = datetime(2026, 11, 4, 3, 0, tzinfo=timezone.utc)
day = lambda n: T0 + timedelta(days=n)
EN = ['audible', 'hardcover', 'goodreads']


def test_new_book_all_sources_due():
    assert S.due_sources({}, 'k', EN, 90, T0) == EN


def test_found_waits_refresh_days_others_stay_due():
    st = {}
    S.record(st, 'k', 'audible', 'found', T0); S.record(st, 'k', 'hardcover', 'found', T0); S.record(st, 'k', 'goodreads', 'missing', T0)
    assert S.due_sources(st, 'k', EN, 90, day(1)) == ['goodreads'], 'next day: only the missing source'
    assert S.due_sources(st, 'k', EN, 90, T0 + timedelta(hours=2)) == [], 'same day: nothing'
    assert S.due_sources(st, 'k', EN, 90, day(90)) == EN


def test_five_misses_pause_until_refresh():
    st = {}
    for i in range(4):
        e = S.record(st, 'k', 'goodreads', 'missing', day(i)); assert e['st'] == 'missing' and e['misses'] == i + 1
    e = S.record(st, 'k', 'goodreads', 'missing', day(4)); assert e['st'] == 'paused' and e['misses'] == 5
    assert not S.is_due(e, 90, day(5)) and not S.is_due(e, 90, day(93)) and S.is_due(e, 90, day(94))


def test_absent_pauses_at_once():
    st = {}
    e = S.record(st, 'k', 'hardcover', S.outcome_of('not_found', hits=0), T0)
    assert e['st'] == 'paused' and not S.is_due(e, 90, day(1))


def test_not_found_with_hits_is_a_miss():
    assert S.outcome_of('not_found', hits=7) == 'missing' and S.outcome_of('not_found') == 'missing'


def test_blocked_changes_nothing():
    st = {}
    S.record(st, 'k', 'goodreads', 'missing', T0)
    for s in ('blocked', 'deferred', 'disabled', 'fresh'):
        assert S.outcome_of(s) == 'skipped'
        S.record(st, 'k', 'goodreads', 'skipped', day(1))
    assert st['k']['goodreads'] == {'st': 'missing', 'ts': T0.isoformat(timespec='seconds'), 'misses': 1}


def test_found_resets_misses():
    st = {}
    S.record(st, 'k', 'goodreads', 'missing', T0); S.record(st, 'k', 'goodreads', 'missing', day(1))
    S.record(st, 'k', 'goodreads', 'found', day(2)); e = S.record(st, 'k', 'goodreads', 'missing', day(100))
    assert e['misses'] == 1


def test_only_enabled_sources_count():
    st = {}
    S.record(st, 'k', 'audible', 'found', T0); S.record(st, 'k', 'hardcover', 'found', T0)
    assert S.due_sources(st, 'k', ['audible', 'hardcover'], 90, day(1)) == [], 'Goodreads off: book not due'


def test_seed_takes_over_old_files():
    st = {}
    S.seed(st, 'k', '2026-10-01T00:00:00+00:00', audible_ok=True, gr_known=False, hc_known=True)
    assert S.due_sources(st, 'k', EN, 90, day(1)) == ['goodreads']
    S.seed(st, 'k', '2020-01-01T00:00:00+00:00', audible_ok=False)  # never overwrites
    assert st['k']['audible']['st'] == 'found'


def test_audible_facts_compact():
    f = S.audible_facts({'asin': 'B0X', 'title_raw': 'T', 'count': 5, 'summary': 'long', 'meta_raw': {'language': 'english', 'series': [{'title': 'S'}], 'big': 1}})
    assert f == {'asin': 'B0X', 'title_raw': 'T', 'meta_raw': {'language': 'english', 'series': [{'title': 'S'}]}}


if __name__ == '__main__':
    for n, f in sorted(globals().items()):
        if n.startswith('test_'): f(); print('ok', n)
    print('source state tests passed')
