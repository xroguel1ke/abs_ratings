"""Goodreads background mode (user 2026-11-18): scans never ask Goodreads, one process asks one book every few minutes,
a block pauses the mode; during one worker's cool-down no other worker asks Goodreads. Run: python -m pytest tests/test_goodreads_mode.py"""
import datetime as dt, os, sys, tempfile, time

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
os.environ.setdefault('DATA_DIR', tempfile.mkdtemp())
os.environ.setdefault('CONFIG_DIR', tempfile.mkdtemp())
sys.path.insert(0, ROOT)
import abs_ratings as A  # noqa: E402
from webui import core  # noqa: E402


def test_scans_never_get_goodreads():
    s = dict(core.DEFAULTS, GOODREADS_ENABLED=True)
    assert core.script_env(s, {'RUN_ID': 'x'})['GOODREADS_ENABLED'] == 'false'
    assert core.script_env(s, {'RUN_ID': 'x', 'GR_BACKGROUND': '1'})['GOODREADS_ENABLED'] == 'true'
    assert core.script_env(dict(s, GOODREADS_ENABLED=False), {'GR_BACKGROUND': '1'})['GOODREADS_ENABLED'] == 'false'
    assert 'GOODREADS_ENABLED' not in core.RUN_OVERRIDABLE and 'GOODREADS_ENABLED' not in core.scan_form(s)
    old = {'GOODREADS_MAX_PER_RUN', 'GOODREADS_PAGES_PER_RUN', 'GR_MAX_COOLDOWNS', 'GR_BLOCK_COOLDOWN', 'GR_MISSING_RECHECK_DAYS'}
    assert not old & {f[0] for f in core.FIELDS}, 'the per-scan Goodreads settings are gone from the web UI'


def test_cool_down_is_shared_by_all_workers(monkeypatch):
    calls = []
    monkeypatch.setattr(A, 'GOODREADS_ENABLED', True)
    monkeypatch.setattr(A, '_goodreads_lookup', lambda *a, **k: calls.append(a) or (None, 'not_found'))
    monkeypatch.setitem(A.GR_STATE, 'blocked', False)
    monkeypatch.setitem(A.GR_STATE, 'cool_until', time.monotonic() + 100)  # another worker waits after a block
    assert A.get_goodreads_data(None, 'Book', ['Author'], 'Author') == (None, 'deferred') and not calls
    monkeypatch.setitem(A.GR_STATE, 'cool_until', 0)
    A.get_goodreads_data(None, 'Book', ['Author'], 'Author')
    assert len(calls) == 1


def test_next_due_order(monkeypatch):
    now = dt.datetime.now(dt.timezone.utc)
    monkeypatch.setattr(A, 'LIBRARY_IDS', ['L'])
    monkeypatch.setattr(A, 'REFRESH_DAYS', 90)
    monkeypatch.setattr(A, 'RESULTS', {'L_old': {}, 'L_new': {}, 'L_fresh': {}, 'X_other': {}})
    monkeypatch.setattr(A, 'SOURCE_STATE', {
        'L_old': {'goodreads': {'st': 'found', 'ts': (now - dt.timedelta(days=200)).isoformat()}},
        'L_fresh': {'goodreads': {'st': 'found', 'ts': (now - dt.timedelta(days=2)).isoformat()}}})
    assert A.gr_next_due() == 'L_new'            # never asked first
    assert A.gr_next_due({'L_new'}) == 'L_old'   # then the oldest check; a fresh one and other libraries never
    assert A.gr_next_due({'L_new', 'L_old'}) is None


def test_block_pauses_the_mode(monkeypatch, tmp_path):
    seen, sleeps = [], []
    monkeypatch.setattr(A, 'GR_MODE_FILE', str(tmp_path / 'goodreads_mode.json'))
    monkeypatch.setattr(A, 'GR_MODE_PAUSE_MIN', 15)
    monkeypatch.setattr(A, 'scan_gate', lambda: None)
    monkeypatch.setattr(A, 'refresh_own_fingerprints', lambda: None)
    monkeypatch.setattr(A, 'gr_next_due', lambda skip=(): f"L_book{len(seen)}")
    def fake_pass(item, key, idx, total):
        seen.append(key)
        if len(seen) == 2: A.GR_STATE['blocked'] = True  # the second book meets a bot challenge
    monkeypatch.setattr(A, 'rating_only_pass', fake_pass)
    def fake_sleep(sec):
        sleeps.append(sec)
        if len(sleeps) >= 2: A._GR_STOP.set()
    monkeypatch.setattr(A, '_gr_sleep', fake_sleep)
    A._GR_STOP.clear()
    try: A.goodreads_background()
    finally: A._GR_STOP.clear()
    st = A.rw_json(A.GR_MODE_FILE)
    assert seen == ['L_book0', 'L_book1'] and st['today'] == 1 and st['blocks'] == 1
    until = dt.datetime.fromisoformat(st['pause_until'])
    assert dt.timedelta(minutes=14) < until - dt.datetime.now() <= dt.timedelta(minutes=15)
    assert 0.8 * A.GR_MODE_MINUTES * 60 <= sleeps[0] <= 1.2 * A.GR_MODE_MINUTES * 60  # the normal pace between books


def test_status_without_state():
    st = core.GR_BG.status()
    assert st['running'] is False and st['today'] == 0 and st['pause_until'] is None
