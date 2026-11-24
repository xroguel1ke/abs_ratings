"""Review-list page of the web UI with synthetic data. Run: python tests/test_webui_queue.py"""
import json, os, sys, tempfile
cfg = tempfile.mkdtemp()
os.environ['CONFIG_DIR'] = cfg
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
json.dump([{'key': 'lib_1', 'title': 'Test Book One', 'kind': 'asin', 'candidate': 'B0TEST0001', 'reason': 'weak evidence for an ASIN change',
            'evidence': ['title', 'author'], 'status': 'open'},
           {'key': 'lib_2', 'title': 'Test Book Two', 'kind': 'isbn', 'candidate': 'B0TEST0002', 'reason': 'ISBN looks wrong', 'evidence': [], 'status': 'open'}],
          open(os.path.join(cfg, 'review_queue.json'), 'w'))
json.dump({'lib_1': {'title': 'Test Book One', 'status': 'not_found', 'author': 'A'}, 'lib_2': {'title': 'Test Book Two', 'status': 'ok'}}, open(os.path.join(cfg, 'results.json'), 'w'))
json.dump({'must_change_password': False}, open(os.path.join(cfg, 'settings.json'), 'w'))
from webui import app as W, core


def client():
    for f in ('review_decisions.json', 'overrides.json'):  # every test starts clean
        p = os.path.join(cfg, f)
        if os.path.exists(p): os.remove(p)
    c = W.app.test_client()
    with c.session_transaction() as s: s['auth'] = True
    return c


def test_queue_tab_lists_open_items_and_decisions_hide_them():
    c = client()
    html = c.get('/review').get_data(as_text=True)
    assert 'Test Book One' in html and 'B0TEST0001' in html and 'Test Book Two' in html, html[:400]
    qid = core.open_queue()[0]['id']
    r = c.post(f'/queue/{qid}/reject'); assert r.status_code in (302, 303)
    html = c.get('/review').get_data(as_text=True)
    assert 'Test Book One' not in html and 'Test Book Two' in html
    assert qid in json.load(open(os.path.join(cfg, 'review_decisions.json')))


def test_accept_asin_stores_override_only_for_asin_kind():
    c = client()
    asin_item = next(x for x in core.review_queue() if x['kind'] == 'asin')
    isbn_item = next(x for x in core.open_queue() if x['kind'] == 'isbn')
    c.post(f"/queue/{core.queue_id(asin_item)}/accept")
    assert core.overrides().get('lib_1', {}).get('asin') == 'B0TEST0001'
    c.post(f"/queue/{isbn_item['id']}/accept")
    assert 'lib_2' not in core.overrides()


def test_books_list_sorted_by_certainty():
    json.dump({'a': {'title': 'Low', 'status': 'ok', 'author': 'X', 'certainty': 35, 'certainty_parts': {'title': 20}}, 'b': {'title': 'High', 'status': 'ok', 'author': 'Y', 'certainty': 95, 'certainty_parts': {'title': 25}},
               'c': {'title': 'None', 'status': 'ok', 'author': 'Z'}}, open(os.path.join(cfg, 'results.json'), 'w'))
    c = client(); h = c.get('/books?sort=certainty&dir=asc').get_data(as_text=True)
    assert h.index('Low') < h.index('High') < h.index('None'), 'ascending: low first, books without score last'
    h = c.get('/books?sort=certainty&dir=desc').get_data(as_text=True); assert h.index('High') < h.index('Low')
    assert 'High' not in c.get('/books?max=50').get_data(as_text=True)
    assert 'Certainty 35 / 100' in c.get('/problem/a').get_data(as_text=True)


def test_status_page_coverage_and_api():
    os.makedirs(os.path.join(cfg, 'reports'), exist_ok=True); os.makedirs(os.path.join(cfg, 'logs'), exist_ok=True)
    json.dump({'books': 10, 'generated': 'now', 'missing': {'asin': ['A', 'B'], 'cover': []}}, open(os.path.join(cfg, 'reports', 'library_report.json'), 'w'))
    open(os.path.join(cfg, 'logs', 'run_2026-01-01_00-00-00.log'), 'w').write('Hardcover: found 5 (daily limit left: 4321)\nGoodreads blocks requests: Bot challenge')
    html = client().get('/').get_data(as_text=True)
    assert '80.0' in html and 'Complete: Cover' in html and '4321' in html and 'blocked' in html, html[-900:]  # 100 % fields share one line (2026-11-15)


def test_csv_export():
    h = client().get("/export.csv").get_data(as_text=True)
    assert h.splitlines()[0].startswith("title,author,status,asin") and "Test Book" in h or "Low" in h


def test_quick_mode_starts_a_scan_for_new_books_only():
    class R:
        def json(self): return {'results': [{'id': 'old'}, {'id': 'new1'}]}
    json.dump({'L_old': '2026-01-01T00:00:00+00:00'}, open(os.path.join(cfg, 'rating_history.json'), 'w'))
    started = []
    orig_get, orig_start, orig_running = core.abs_get, core.RUNNER.start, core.RUNNER.running
    core.abs_get = lambda s, path, timeout=30: R()
    core.RUNNER.start = lambda kind, **kw: started.append((kind, kw.get('only_items'))) or ('x', None)
    core.RUNNER.running = lambda: False
    try:
        core._quick['t'] = 0
        core.quick_poll({'QUICK_MODE_MINUTES': 5, 'LIBRARY_IDS': 'L', 'BATCH_SIZE': 300})
        assert started == [('full', ['L_new1'])], started
        core.quick_poll({'QUICK_MODE_MINUTES': 5, 'LIBRARY_IDS': 'L'})
        assert len(started) == 1, 'not again within the interval'
    finally:
        core.abs_get, core.RUNNER.start, core.RUNNER.running = orig_get, orig_start, orig_running


def test_isbn_checksum():
    assert core.isbn_valid("9780007370443") and core.isbn_valid("0-306-40615-2") and not core.isbn_valid("B0TEST0002") and not core.isbn_valid("9780007370444")


def test_run_progress_parser():
    log = os.path.join(cfg, 'logs', 'prog.log'); os.makedirs(os.path.dirname(log), exist_ok=True)
    open(log, 'w').write('2026 - INFO - (3/10) [ETA: 1m 5s] Some Book [ASIN: B0X]\n  SUCCESS on www.audible.com\n  ✅ Hardcover via ISBN: x\n  NEW ASIN Found: B0Y\n  No Audible data\n')
    p = core.run_progress(log)
    assert (p['n'], p['total'], p['pct'], p['eta'], p['title']) == (3, 10, 30.0, '1m 5s', 'Some Book'), p
    assert (p['audible_found'], p['hardcover'], p['new_asin'], p['no_audible']) == (1, 1, 1, 1), p


if __name__ == '__main__':
    for n, f in sorted(globals().items()):
        if n.startswith('test_'): f(); print('ok', n)
    print('webui queue tests passed')


def test_mark_all_then_process_at_once(monkeypatch):
    # user 2026-11-24: mark every entry (apply / discard), then process the whole list in one go; ids may contain commas
    json.dump([{'key': 'lib_1', 'title': 'Test Book One', 'kind': 'asin', 'candidate': 'B0TEST0001', 'reason': 'weak', 'evidence': [], 'status': 'open'},
               {'key': 'lib_2', 'title': 'Test Book Two', 'kind': 'series', 'candidate': 'Saga, Part 2', 'reason': 'x', 'evidence': [], 'status': 'open'},
               {'key': 'lib_3', 'title': 'Test Book Three', 'kind': 'chapters', 'candidate': 'no chapters', 'reason': 'x', 'evidence': [], 'status': 'open'}],
              open(os.path.join(cfg, 'review_queue.json'), 'w'))
    started = []
    monkeypatch.setattr(core.RUNNER, 'running', lambda: False)
    monkeypatch.setattr(core.RUNNER, 'start', lambda kind, after=None, **kw: (started.append((kind, kw, after)), ('run-x', None))[1])
    c = client()
    html = c.get('/review').get_data(as_text=True)
    assert 'Process marked entries' in html and 'class="qc" data-c="accept"' in html and 'data-kind="asin"' in html
    ids = {x['key']: x['id'] for x in core.open_queue()}
    r = c.post('/queue-decide', data={'accept': json.dumps([ids['lib_1']]), 'reject': json.dumps([ids['lib_2'], ids['lib_3']])})
    assert r.status_code in (302, 303) and '/run/run-x' in r.headers['Location'], r.headers.get('Location')
    d = json.load(open(os.path.join(cfg, 'review_decisions.json')))
    assert d[ids['lib_1']]['action'] == 'accept' and d[ids['lib_2']]['action'] == 'reject' and d[ids['lib_3']]['action'] == 'reject'
    assert core.overrides()['lib_1']['asin'] == 'B0TEST0001'
    assert started == [('item', {'dry_run': False, 'only_items': ['lib_1'], 'label': '1 × ASIN/Hardcover from the review list', 'run_settings': {'ABS_BACKUP': False}}, [])]
    assert core.open_queue() == []


def test_nothing_changes_while_a_scan_runs(monkeypatch):
    json.dump([{'key': 'lib_1', 'title': 'Test Book One', 'kind': 'asin', 'candidate': 'B0TEST0001', 'reason': 'weak', 'evidence': [], 'status': 'open'}],
              open(os.path.join(cfg, 'review_queue.json'), 'w'))
    monkeypatch.setattr(core.RUNNER, 'running', lambda: True)
    c = client(); qid = core.open_queue()[0]['id']
    c.post('/queue-decide', data={'accept': json.dumps([qid]), 'reject': '[]'})
    assert core.open_queue() and 'lib_1' not in core.overrides()


def test_other_group_hidden_as_intended(monkeypatch):
    # user 2026-11-24: The Wandering Inn + Rewrite share one ASIN on purpose - hide it for good; a new book with that ASIN shows it again
    g = [['1774240327', [{'key': 'k1', 'title': 'The Wandering Inn'}, {'key': 'k2', 'title': 'The Wandering Inn (Rewrite)'}], 'editions']]
    monkeypatch.setattr(core, 'same_asin_groups', lambda s=None: [list(x) for x in g])
    p = os.path.join(cfg, 'other_hidden.json')
    if os.path.exists(p): os.remove(p)
    c = client()
    html = c.get('/review').get_data(as_text=True)
    assert 'The Wandering Inn (Rewrite)' in html and '>Hide<' in html.replace(' Hide<', '>Hide<')
    gid = core.other_group_id('asin', g[0][0], g[0][1])
    c.post('/other-hide', data={'gid': gid, 'label': 'Wandering Inn'})
    html = c.get('/review').get_data(as_text=True)
    assert 'The Wandering Inn (Rewrite)' not in html and '1 hidden - show' in html
    assert 'The Wandering Inn (Rewrite)' in c.get('/review?hidden=1').get_data(as_text=True)
    g[0][1].append({'key': 'k3', 'title': 'The Wandering Inn (Copy)'})  # the group changed: shown again
    assert 'The Wandering Inn (Copy)' in c.get('/review').get_data(as_text=True)
    c.post('/other-hide', data={'gid': gid, 'show': '1'}); assert gid not in core.other_hidden()


def test_follow_up_job_starts_after_the_run():
    r = core.RunManager(); calls = []
    r.start = lambda kind, after=None, **kw: calls.append((kind, kw, after))
    class P:
        pid = 1
        def wait(self): return 0
    r.proc = P(); open(os.path.join(cfg, 'c.log'), 'w').close()
    r._wait({'id': 'x', 'kind': 'apply', 'start': '2026-01-01T00:00:00', 'log': 'nolog.log', 'console': 'c.log'}, open(os.path.join(cfg, 'c.log'), 'a'), {},
            [('item', {'only_items': ['k']}), ('item', {'only_items': ['z']})])
    assert calls == [('item', {'only_items': ['k']}, [('item', {'only_items': ['z']})])]
