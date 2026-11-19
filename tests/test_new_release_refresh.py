"""New releases refreshed sooner (user 2026-11-19): a book released within NEW_RELEASE_DAYS is refreshed after
NEW_RELEASE_REFRESH_DAYS instead of REFRESH_DAYS - every source, also after 5 misses. Run: python -m pytest tests/test_new_release_refresh.py"""
import os, sys, tempfile
from datetime import datetime, timedelta, timezone

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
os.environ.setdefault('DATA_DIR', tempfile.mkdtemp())
os.environ.setdefault('CONFIG_DIR', tempfile.mkdtemp())
sys.path.insert(0, ROOT)
import source_state as SST  # noqa: E402
import abs_ratings as A  # noqa: E402

NOW = datetime(2026, 11, 19, 12, tzinfo=timezone.utc)
ON = (90, 10)


def day(n): return (NOW - timedelta(days=n)).date().isoformat()
def ts(n): return (NOW - timedelta(days=n)).isoformat(timespec='seconds')
def book(released, **src): return {'aud': {'asin': 'B000000001', 'release': released}, **src}


def test_interval_per_book():
    assert SST.refresh_days_for(book(day(30)), 100, ON, NOW) == 10      # new release
    assert SST.refresh_days_for(book(day(90)), 100, ON, NOW) == 10      # still within the 90 days
    assert SST.refresh_days_for(book(day(91)), 100, ON, NOW) == 100     # older
    assert SST.refresh_days_for(book('2027-02-01'), 100, ON, NOW) == 10  # still to come (pre-order)
    assert SST.refresh_days_for(book(''), 100, ON, NOW) == 100          # Audible has no date
    assert SST.refresh_days_for({}, 100, ON, NOW) == 100                # never checked on Audible
    assert SST.refresh_days_for(book(day(30)), 100, None, NOW) == 100   # option off: as before
    assert SST.refresh_days_for(book(day(30)), 0, ON, NOW) == 0         # 0 = complete pass stays 0
    assert SST.refresh_days_for(book(day(30)), 7, ON, NOW) == 7         # never longer than the normal interval


def test_due_sources_new_release():
    found_11 = {'st': 'found', 'ts': ts(11)}
    paused_11 = {'st': 'paused', 'ts': ts(11), 'misses': 5, 'why': '5 misses in a row'}
    st = {'new': book(day(40), audible=found_11, goodreads=paused_11, hardcover={'st': 'found', 'ts': ts(5)}),
          'old': book(day(400), audible=found_11, goodreads=paused_11)}
    srcs = ['audible', 'hardcover', 'goodreads']
    assert SST.due_sources(st, 'new', srcs, 100, NOW, ON) == ['audible', 'goodreads']  # found and paused after 10 days
    assert SST.due_sources(st, 'old', srcs, 100, NOW, ON) == ['hardcover']             # (never asked) - the rest waits 100 days
    assert SST.due_sources(st, 'new', srcs, 100, NOW) == []  # option off


def test_missing_still_retried_next_day():
    st = {'new': book(day(5), goodreads={'st': 'missing', 'ts': (NOW - timedelta(hours=21)).isoformat(), 'misses': 2})}
    assert SST.due_sources(st, 'new', ['goodreads'], 100, NOW, ON) == ['goodreads']


def test_audible_facts_keep_release_date():
    f = SST.audible_facts({'asin': 'B0X', 'meta_raw': {'releaseDate': '2026-10-01T00:00:00Z', 'language': 'english'}})
    assert f['release'] == '2026-10-01'
    assert SST.audible_facts({'asin': 'B0X', 'meta_raw': {}})['release'] == ''


def test_backfill_release_dates(monkeypatch, tmp_path):
    monkeypatch.setattr(A, 'NEW_RELEASE', ON)
    monkeypatch.setattr(A, 'DRY_RUN', False)
    monkeypatch.setattr(A, 'SOURCE_STATE_FILE', str(tmp_path / 'source_state.json'))
    state = {'L_a': {'aud': {'asin': 'B00000000A', 'domain': 'www.audible.com'}},
             'L_b': {'aud': {'asin': 'B00000000B', 'domain': 'www.audible.de'}},
             'L_c': {'aud': {'asin': 'B00000000C', 'domain': 'www.audible.com', 'release': '2020-01-01'}},  # known: not asked
             'L_d': {'goodreads': {'st': 'found'}}}  # no Audible facts
    monkeypatch.setattr(A, 'SOURCE_STATE', state)
    asked = []
    def fake(host, asins):
        asked.append((host, tuple(asins)))
        return {'B00000000A': '2026-11-01'} if host == 'api.audible.com' else {}
    assert A.backfill_release_dates(fake) == 1
    assert sorted(asked) == [('api.audible.com', ('B00000000A',)), ('api.audible.de', ('B00000000B',))]
    assert state['L_a']['aud']['release'] == '2026-11-01' and state['L_b']['aud']['release'] == ''  # '' = not asked again
    asked.clear()
    assert A.backfill_release_dates(fake) == 0 and asked == []


def test_backfill_off_or_dry(monkeypatch):
    monkeypatch.setattr(A, 'SOURCE_STATE', {'L_a': {'aud': {'asin': 'B00000000A'}}})
    monkeypatch.setattr(A, 'NEW_RELEASE', None)
    assert A.backfill_release_dates(lambda h, a: 1 / 0) == 0
    monkeypatch.setattr(A, 'NEW_RELEASE', ON); monkeypatch.setattr(A, 'DRY_RUN', True)
    assert A.backfill_release_dates(lambda h, a: 1 / 0) == 0


def test_setting_reaches_the_script():
    from webui import core
    keys = [f[0] for f in core.FIELDS]
    assert {'NEW_RELEASE_REFRESH', 'NEW_RELEASE_DAYS', 'NEW_RELEASE_REFRESH_DAYS'} <= set(core.SCRIPT_KEYS)
    assert keys.index('NEW_RELEASE_REFRESH') == keys.index('REFRESH_DAYS') + 1
    env = core.script_env(dict(core.DEFAULTS, NEW_RELEASE_REFRESH=True, NEW_RELEASE_DAYS=90, NEW_RELEASE_REFRESH_DAYS=10))
    assert (env['NEW_RELEASE_REFRESH'], env['NEW_RELEASE_DAYS'], env['NEW_RELEASE_REFRESH_DAYS']) == ('true', '90', '10')
