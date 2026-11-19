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


def test_restart_keeps_the_pace(monkeypatch, tmp_path):
    """After a restart (scan over, settings changed) the first book waits for the rest of the pause since the last one."""
    monkeypatch.setattr(A, 'GR_MODE_FILE', str(tmp_path / 'goodreads_mode.json'))
    monkeypatch.setattr(A, 'GR_MODE_MINUTES', 7)
    last = dt.datetime.now() - dt.timedelta(minutes=2)
    A.rw_json(A.GR_MODE_FILE, {'day': dt.date.today().isoformat(), 'today': 3, 'blocks': 0, 'last_at': last.isoformat(timespec='seconds')})
    seen, sleeps = [], []
    monkeypatch.setattr(A, 'scan_gate', lambda: None)
    monkeypatch.setattr(A, 'refresh_own_fingerprints', lambda: None)
    monkeypatch.setattr(A, 'gr_next_due', lambda skip=(): 'L_book')
    monkeypatch.setattr(A, 'rating_only_pass', lambda item, key, idx, total: seen.append((key, len(sleeps))))
    def fake_sleep(sec):
        sleeps.append(sec)
        if len(sleeps) >= 2: A._GR_STOP.set()
    monkeypatch.setattr(A, '_gr_sleep', fake_sleep)
    A._GR_STOP.clear()
    try: A.goodreads_background()
    finally: A._GR_STOP.clear()
    assert 4 * 60 < sleeps[0] <= 5 * 60 + 1  # ~5 of the 7 minutes were left
    assert seen == [('L_book', 1)]  # the book is asked only after that wait
    assert 0.8 * 7 * 60 <= sleeps[1] <= 1.2 * 7 * 60  # then the normal pace, jitter untouched


def test_settings_change_restarts_the_mode(monkeypatch):
    """A changed setting (e.g. minutes per book) stops the running process; the loop starts it again with the new values."""
    bg = core.GoodreadsBackground()
    s = {'GOODREADS_ENABLED': True, 'ABS_URL': 'http://abs', 'API_TOKEN': 't', 'GR_MODE_MINUTES': 7}
    calls = []
    monkeypatch.setattr(bg, 'running', lambda: True)
    monkeypatch.setattr(bg, 'stop', lambda *a: calls.append('stop'))
    bg.started_with = bg.fingerprint(s)
    monkeypatch.setattr(core, 'load_settings', lambda: dict(s))
    rounds = iter([0, 1])
    def fake_sleep(sec):
        try: next(rounds)
        except StopIteration: raise KeyboardInterrupt
        if sec == 20: s['GR_MODE_MINUTES'] = 6  # the user saves new settings
    monkeypatch.setattr(core.time, 'sleep', fake_sleep)
    try: bg.loop()
    except KeyboardInterrupt: pass
    assert calls == ['stop']  # unchanged settings: kept running; changed: stopped once


def test_scan_keeps_known_goodreads_page():
    """Scans never ask Goodreads: the result of a full check keeps the page the background mode found (coverage)."""
    import inspect
    src = inspect.getsource(A)
    assert "(None if gr_status == 'not_found' else (RESULTS.get(key) or {}).get('goodreads') or GR_CACHE.get(key))" in src


def test_coverage_counts_cached_goodreads_pages(monkeypatch):
    res = {'L_a': {'title': 'A', 'status': 'ok'}, 'L_b': {'title': 'B', 'status': 'ok', 'goodreads': 'u2'}, 'L_c': {'title': 'C', 'status': 'ok'}}
    real = core._load_json
    monkeypatch.setattr(core, 'results', lambda: {k: dict(v) for k, v in res.items()})
    monkeypatch.setattr(core, '_load_json', lambda p, d=None: {'L_a': 'u1', 'L_x': 'u9'} if p.endswith('goodreads_cache.json') else ({} if p.endswith('library_report.json') else real(p, d)))
    monkeypatch.setattr(core, '_coverage_trend', lambda *a: None)
    rows, _ = core.coverage()
    gr = next(r for r in rows if r[0] == 'Goodreads ratings')
    assert gr[1:3] == (2, 3)


def test_block_pause_logged_once(monkeypatch, tmp_path, caplog):
    """A block is logged once (PAUSE ... until), not a second time when the loop then waits for that pause."""
    import logging
    monkeypatch.setattr(A, 'GR_MODE_FILE', str(tmp_path / 'goodreads_mode.json'))
    monkeypatch.setattr(A, 'scan_gate', lambda: None)
    monkeypatch.setattr(A, 'refresh_own_fingerprints', lambda: None)
    monkeypatch.setattr(A, 'gr_next_due', lambda skip=(): 'L_book')
    monkeypatch.setattr(A, 'rating_only_pass', lambda *a: A.GR_STATE.update(blocked=True))
    sleeps = []
    def fake_sleep(sec):
        sleeps.append(sec)
        if len(sleeps) >= 2: A._GR_STOP.set()  # 1: normal pace after the book, 2: rest of the pause
    monkeypatch.setattr(A, '_gr_sleep', fake_sleep)
    A._GR_STOP.clear()
    with caplog.at_level(logging.INFO):
        try: A.goodreads_background()
        finally: A._GR_STOP.clear()
    msgs = [r.getMessage() for r in caplog.records]
    assert sum('PAUSE' in m for m in msgs) == 1 and not any('pauses until' in m for m in msgs)
    assert len(sleeps) == 2  # the pause is still waited out


def test_restart_into_a_pause_says_so(monkeypatch, tmp_path, caplog):
    import logging
    monkeypatch.setattr(A, 'GR_MODE_FILE', str(tmp_path / 'goodreads_mode.json'))
    until = (dt.datetime.now() + dt.timedelta(minutes=10)).isoformat(timespec='seconds')
    A.rw_json(A.GR_MODE_FILE, {'day': dt.date.today().isoformat(), 'today': 1, 'blocks': 1, 'pause_until': until})
    monkeypatch.setattr(A, '_gr_sleep', lambda sec: A._GR_STOP.set())
    A._GR_STOP.clear()
    with caplog.at_level(logging.INFO):
        try: A.goodreads_background()
        finally: A._GR_STOP.clear()
    assert any(f'pauses until {until[11:16]}' in r.getMessage() for r in caplog.records)


def test_sigterm_stops_the_mode_first(monkeypatch):
    """docker stop: the web UI ends the Goodreads process (after its book) before it exits itself."""
    from webui import app as webapp
    calls = []
    monkeypatch.setattr(webapp.core.GR_BG, 'stop', lambda timeout=90: calls.append(('stop', timeout)))
    monkeypatch.setattr(webapp.os, '_exit', lambda code: calls.append(('exit', code)))
    webapp.on_sigterm()
    assert calls == [('stop', 8), ('exit', 0)]
