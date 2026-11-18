"""Time left includes the tasks after the books (user 2026-11-16: the run sat at 100 % while the library tasks ran)."""
import datetime as dt
from webui import core


def _log(lines):
    return ''.join(f"2026-11-16 {t},000 - INFO - {m}\n" for t, m in lines)


PREV = _log([('09:00:00', 'start'), ('09:00:10', '(1/100) [ETA: …] A [ASIN: X]'), ('09:10:00', '(100/100) [ETA: 0s] B [ASIN: Y]'),
             ('09:10:30', '📚 SERIES & LIBRARY TASK'), ('09:13:00', 'Fingerprints refreshed for 3 book(s)'), ('09:13:40', '  Duration: 13m 40s')])


def _run(tmp_path, monkeypatch, cur_lines, now, expired=0):
    logs = tmp_path / 'logs'; logs.mkdir()
    (logs / 'run_prev.log').write_text(PREV, encoding='utf-8')
    cur = logs / 'run_cur.log'; cur.write_text(_log(cur_lines), encoding='utf-8')
    monkeypatch.setattr(core, 'LOG_DIR', str(logs))
    monkeypatch.setattr(core.RUNNER, 'runs', lambda: [{'id': 'cur', 'kind': 'full', 'status': 'running', 'log': 'run_cur.log'},
                                                      {'id': 'prev', 'kind': 'full', 'status': 'ok', 'log': 'run_prev.log'}])
    old = (now.astimezone(dt.timezone.utc) - dt.timedelta(days=3)).isoformat()  # relative to the test's fixed 'now' (was: the real date - failed from 2026-11-18 on)
    monkeypatch.setattr(core, '_load_json', lambda p, d: {f'v2|h|{i}': {'ts': old} for i in range(expired)} if p.endswith('series_members.json') else d)
    monkeypatch.setattr(core, 'load_settings', lambda: {'SERIES_LIST_DAYS': 1})
    class F(dt.datetime):
        @classmethod
        def now(cls, tz=None): return now.astimezone(tz) if tz else now
    monkeypatch.setattr(core, 'datetime', F)
    core._prog_cache.clear(); core._tail_cache.clear()
    return core.run_progress(str(cur), {'id': 'cur', 'kind': 'full', '_log_path': str(cur)})


def test_books_done_library_tasks_counted(tmp_path, monkeypatch):
    d = _run(tmp_path, monkeypatch, [('10:00:00', 'start'), ('10:00:10', '(1/100) [ETA: …] A [ASIN: X]'),
                                    ('10:10:00', '(100/100) [ETA: 0s] B [ASIN: Y]')], dt.datetime(2026, 11, 16, 10, 11, 0))
    # previous run: 3 min between its last book and the missing-volumes step; 1 min of it is over; + 30 s missing volumes
    assert d['phase'] == 'library' and d['eta'] == '2m 30s' and d['finish'] == '10:13' and d['pct'] < 100


def test_expired_series_lists_make_missing_volumes_longer(tmp_path, monkeypatch):
    d = _run(tmp_path, monkeypatch, [('10:00:00', 'start'), ('10:00:10', '(1/100) [ETA: …] A [ASIN: X]'),
                                    ('10:10:00', '(100/100) [ETA: 0s] B [ASIN: Y]'), ('10:13:00', 'Fingerprints refreshed for 2 book(s)')],
             dt.datetime(2026, 11, 16, 10, 13, 30), expired=300)
    assert d['phase'] == 'missing' and d['eta'] == '6m 30s'  # 30 s + 300 x 1.3 s - 30 s elapsed


def test_missing_volumes_progress_lines(tmp_path, monkeypatch):
    d = _run(tmp_path, monkeypatch, [('10:00:00', 'start'), ('10:00:10', '(1/100) [ETA: …] A [ASIN: X]'),
                                    ('10:10:00', '(100/100) [ETA: 0s] B [ASIN: Y]'), ('10:13:00', 'Fingerprints refreshed for 2 book(s)'),
                                    ('10:15:00', '   … 100/400 series checked')], dt.datetime(2026, 11, 16, 10, 15, 0), expired=300)
    assert d['eta'] == '6m 0s'  # 2 min for 100 series -> 300 left = 6 min
