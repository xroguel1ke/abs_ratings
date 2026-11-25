"""Scan page: 'Chapters' as its own task (user 2026-11-25). Run: python tests/test_webui_chapters.py"""
import json, os, sys, tempfile
cfg = tempfile.mkdtemp(); os.environ['CONFIG_DIR'] = cfg
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
json.dump({'must_change_password': False, 'ABS_URL': 'http://x', 'API_TOKEN': 't'}, open(os.path.join(cfg, 'settings.json'), 'w'))
from webui import app as W, core

calls = []
core.RUNNER.start = lambda kind, **kw: (calls.append((kind, kw)) or ('20261126-000000', None))


def client():
    c = W.app.test_client()
    with c.session_transaction() as ss: ss['auth'] = True
    return c


def test_chapters_alone_starts_the_chapter_check():
    calls.clear()
    r = client().post('/runs', data={'action': 'start', 'task_chapters': 'on', 'dry': 'on'})
    assert r.status_code == 302 and calls and calls[0][0] == 'chapters' and calls[0][1]['dry_run'] is True, calls


def test_chapters_with_metadata_runs_after_the_scan():
    calls.clear()
    client().post('/runs', data={'action': 'start', 'task_meta': 'on', 'task_chapters': 'on'})
    kind, kw = calls[0]
    assert kind == 'full' and kw['after'] and kw['after'][0][0] == 'chapters' and kw['after'][0][1]['dry_run'] is False, calls


def test_checkbox_is_on_the_scan_page_and_remembered():
    c = client()
    h = c.get('/runs').get_data(as_text=True)
    assert 'name="task_chapters"' in h
    c.post('/api/scan_form', data={'task_meta': 'on', 'task_chapters': 'on'})
    assert 'name="task_chapters" checked' in c.get('/runs').get_data(as_text=True)


if __name__ == '__main__':
    n = 0
    for k, f in list(globals().items()):
        if k.startswith('test_') and callable(f): f(); n += 1; print('ok', k)
    print(n, 'tests passed')
