"""Web UI write actions end to end against the fake ABS (tools/fake_abs.py, synthetic books - no private data, no Audible):
series rename -> undo from the history, field lock, 'change all rating symbols', a second start while a run is busy.
Run: python -m pytest tests/test_webui_e2e.py"""
import json, os, socket, subprocess, sys, tempfile, time
import requests

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
cfg = tempfile.mkdtemp()
os.environ['CONFIG_DIR'] = cfg
sys.path.insert(0, ROOT)
LIB = 'lib1'
OLD_BLOCK = ('⭐ Ratings &amp; Infos<br />Audible (120):<br />🏆 ⭐⭐⭐⭐✨ 4.6 / 5 - Overall<br />Hardcover (20):<br />'
             '🏆 ⭐⭐⭐⭐ 4.1 / 5 - Rating<br />⭐<br />')


def book(i, title, series, seq, desc):
    md = {'title': title, 'subtitle': None, 'authors': [{'id': 'au1', 'name': 'Test Author'}], 'narrators': ['Test Narrator'],
          'series': [{'id': f's{i}', 'name': series, 'sequence': seq}], 'description': desc, 'language': 'English', 'asin': None, 'isbn': None,
          'genres': [], 'publisher': None, 'publishedYear': None}
    item = {'id': f'it{i}', 'libraryId': LIB, 'path': f'/books/Test Author/{series}/{seq} - {title}', 'updatedAt': 1000,
            'media': {'metadata': md, 'tags': [], 'duration': 36000}}
    lst = {'id': f'it{i}', 'libraryId': LIB, 'updatedAt': 1000, 'path': item['path'],
           'media': {'tags': [], 'duration': 36000, 'metadata': {'title': title, 'authorName': 'Test Author', 'narratorName': 'Test Narrator',
                                                                 'seriesName': f'{series} #{seq}', 'description': desc, 'language': 'English'}}}
    return item, lst


def setup_module(_):
    global FAKE, PORT, LOG
    books = [book(1, 'First Light', 'Test Saga', '1', OLD_BLOCK + 'Story one.'), book(2, 'Second Dawn', 'Test Saga', '2', OLD_BLOCK + 'Story two.'),
             book(3, 'Third Night', 'Test saga', '3', 'No ratings here.')]
    snap = {'libraries': {'libraries': [{'id': LIB, 'name': 'Books', 'mediaType': 'book'}]},
            'lists': {LIB: {'results': [b[1] for b in books], 'total': 3}}, 'items': {b[0]['id']: b[0] for b in books}}
    sp = os.path.join(cfg, 'snap.json'); json.dump(snap, open(sp, 'w', encoding='utf-8'), ensure_ascii=False)
    with socket.socket() as s_: s_.bind(('127.0.0.1', 0)); PORT = s_.getsockname()[1]
    LOG = os.path.join(cfg, 'patches.jsonl')
    FAKE = subprocess.Popen([sys.executable, os.path.join(ROOT, 'tools', 'fake_abs.py'), '--snapshot', sp, '--port', str(PORT), '--patch-log', LOG],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        try: requests.get(f'http://127.0.0.1:{PORT}/ping', timeout=1); break
        except requests.RequestException: time.sleep(0.1)
    json.dump({'must_change_password': False, 'ABS_URL': f'http://127.0.0.1:{PORT}', 'API_TOKEN': 'test', 'LIBRARY_IDS': LIB,
               'RATING_ICONS': 'hearts', 'ABS_BACKUP': False, 'GOODREADS_ENABLED': False, 'HARDCOVER_ENABLED': False},
              open(os.path.join(cfg, 'settings.json'), 'w'))


def teardown_module(_):
    FAKE.terminate()


def client():
    from webui import app as W
    c = W.app.test_client()
    with c.session_transaction() as s: s['auth'] = True
    return c


def wait_run():
    from webui import core
    for _ in range(600):
        if not core.RUNNER.running(): break
        time.sleep(0.1)
    time.sleep(0.3)  # the waiter thread stores the summary
    return core.RUNNER.runs()[0]


def patches():
    if not os.path.exists(LOG): return []
    return [json.loads(l) for l in open(LOG, encoding='utf-8') if l.strip()]


def item(iid): return requests.get(f'http://127.0.0.1:{PORT}/api/items/{iid}', timeout=5).json()['media']


def test_1_series_page_and_rename_then_undo():
    c = client()
    h = c.get('/series').get_data(as_text=True)
    assert 'Test Saga' in h and 'Test saga' in h
    r = c.post('/series/rename', data={'lib': LIB, 'old': 'Test saga', 'new': 'Test Saga'})
    assert r.status_code == 302, r.status_code
    run = wait_run()
    assert run['status'] in ('ok', 'done', 'finished') or run.get('rc') == 0, run
    assert [x['name'] for x in item('it3')['metadata']['series']] == ['Test Saga'], item('it3')['metadata']['series']
    # undo the rename from the history page of that run
    c.post(f"/history/{run['id']}/undo", data={})
    wait_run()
    assert [x['name'] for x in item('it3')['metadata']['series']] == ['Test saga'], item('it3')['metadata']['series']


def test_2_lock_field_on_book_page():
    c = client()
    r = c.post(f'/book/{LIB}_it2/field/series/lock')
    assert r.status_code == 302
    assert 'lock_series' in (item('it2').get('tags') or []), item('it2')
    c.post(f'/book/{LIB}_it2/field/series/unlock')
    assert 'lock_series' not in (item('it2').get('tags') or [])


def test_3_rewrite_symbols_and_busy_runner():
    c = client()
    c.post('/ratings/rewrite-symbols', data={})
    r2 = c.post('/series/rename', data={'lib': LIB, 'old': 'Test Saga', 'new': 'Other'})  # while the first run is busy (or already done)
    run = wait_run()
    d1, d3 = item('it1')['metadata']['description'], item('it3')['metadata']['description']
    assert '─── Ratings' in d1 and '❤️❤️❤️❤️💗 4.6' in d1 and d1.count('Ratings') == 1 and d1.endswith('Story one.'), d1
    assert d3 == 'No ratings here.'
    assert all(p['kind'] != 'backup' for p in patches()), 'no ABS backup without the option'
    assert r2.status_code == 302


def test_4_remove_ratings_whole_library():
    from webui import core
    rid, err = core.RUNNER.start('full', dry_run=False, label='remove', run_settings={'REMOVE_RATINGS': 'Hardcover', 'ABS_BACKUP': False})
    assert not err, err
    wait_run()
    for i, tail in (('it1', 'Story one.'), ('it2', 'Story two.')):
        d = item(i)['metadata']['description']
        assert 'Hardcover' not in d and 'Audible (120)' in d and d.count('Ratings') == 1 and d.endswith(tail), d
    assert item('it3')['metadata']['description'] == 'No ratings here.'


def test_5_apply_selected_changes_of_a_test_run():
    from webui import core
    rid = '20990101-000000'
    os.makedirs(core.REPORT_DIR, exist_ok=True)
    ch = [{'run': rid, 'ts': '2099-01-01T00:00:00', 'item': 'it1', 'key': f'{LIB}_it1', 'title': 'First Light', 'field': 'title', 'old': 'First Light',
           'new': 'First Light (new)', 'source': 'test', 'dry_run': True},
          {'run': rid, 'ts': '2099-01-01T00:00:00', 'item': 'it2', 'key': f'{LIB}_it2', 'title': 'Second Dawn', 'field': 'title', 'old': 'Second Dawn',
           'new': 'Second Dawn (new)', 'source': 'test', 'dry_run': True}]
    with open(os.path.join(core.REPORT_DIR, f'planned_changes_{rid}.jsonl'), 'w', encoding='utf-8') as f:
        for e in ch: f.write(json.dumps(e) + '\n')
    core.RUNNER._save_run({'id': rid, 'kind': 'full', 'label': 'test run', 'dry_run': True, 'status': 'ok', 'start': '2099-01-01T00:00:00', 'end': '2099-01-01T00:00:01'})
    c = client()
    assert c.get(f'/run/{rid}/review').status_code == 200
    c.post(f'/run/{rid}/apply', data={'mode': 'apply', 'selection': '1', 'sel': ['1']})  # only the second change is ticked
    wait_run()
    assert item('it1')['metadata']['title'] == 'First Light'
    assert item('it2')['metadata']['title'] == 'Second Dawn (new)'
    # the applied change is in the history search and 'undo all shown' undoes exactly it
    h = c.get('/history/search', query_string={'q': 'Second Dawn'}).get_data(as_text=True)
    assert 'Second Dawn' in h and '(new)' in h, h[-2000:]
    c.post('/history/search', data={'q': 'Second Dawn', 'field': 'title'})
    wait_run()
    assert item('it2')['metadata']['title'] == 'Second Dawn'
    assert item('it1')['metadata']['title'] == 'First Light'


def test_6_same_field_twice_in_one_test_run():
    from webui import core
    rid = '20990101-000001'
    ch = [{'run': rid, 'ts': '2099-01-01T00:00:00', 'item': 'it1', 'key': f'{LIB}_it1', 'title': 'First Light', 'field': 'subtitle', 'old': None,
           'new': 'Step One', 'source': 'test', 'dry_run': True},
          {'run': rid, 'ts': '2099-01-01T00:00:01', 'item': 'it1', 'key': f'{LIB}_it1', 'title': 'First Light', 'field': 'subtitle', 'old': 'Step One',
           'new': 'Step Two', 'source': 'test2', 'dry_run': True}]
    with open(os.path.join(core.REPORT_DIR, f'planned_changes_{rid}.jsonl'), 'w', encoding='utf-8') as f:
        for e in ch: f.write(json.dumps(e) + '\n')
    core.RUNNER._save_run({'id': rid, 'kind': 'full', 'label': 'test run 2', 'dry_run': True, 'status': 'ok', 'start': '2099-01-01T00:00:00', 'end': '2099-01-01T00:00:01'})
    client().post(f'/run/{rid}/apply', data={'mode': 'apply'})
    wait_run()
    assert item('it1')['metadata']['subtitle'] == 'Step Two', item('it1')['metadata']
    last = [json.loads(l) for l in open(os.path.join(cfg, 'changes.jsonl'), encoding='utf-8')][-1]
    assert last['field'] == 'subtitle' and last['old'] is None and last['new'] == 'Step Two', last


def _apply_plan(rid, changes):
    from webui import core
    with open(os.path.join(core.REPORT_DIR, f'planned_changes_{rid}.jsonl'), 'w', encoding='utf-8') as f:
        for e in changes: f.write(json.dumps(dict(e, run=rid, dry_run=True, ts='2099-01-01T00:00:00')) + '\n')
    core.RUNNER._save_run({'id': rid, 'kind': 'full', 'label': rid, 'dry_run': True, 'status': 'ok', 'start': '2099-01-01T00:00:00', 'end': '2099-01-01T00:00:01'})
    client().post(f'/run/{rid}/apply', data={'mode': 'apply'})
    return wait_run()['id']


def test_7_undo_of_an_old_change_keeps_a_newer_one():
    base = {'item': 'it3', 'key': f'{LIB}_it3', 'title': 'Third Night', 'field': 'publisher', 'source': 'test'}
    run_a = _apply_plan('20990101-000002', [dict(base, old=None, new='Pub A')])
    time.sleep(1.1)  # run ids are per second
    _apply_plan('20990101-000003', [dict(base, old='Pub A', new='Pub B')])
    assert item('it3')['metadata']['publisher'] == 'Pub B'
    client().post(f'/history/{run_a}/undo', data={})
    wait_run()
    assert item('it3')['metadata']['publisher'] == 'Pub B', 'the newer change must survive the undo of the older one'


def test_8_apply_run_does_not_inherit_an_old_scan_alert():
    with open(os.path.join(cfg, 'last_run.env'), 'w', encoding='utf-8') as f:  # an earlier scan that ended with an alert
        f.write("ABS_ICON='alert'\nABS_SUBJECT='old scan failed'\nABS_REPORT_BODY='old body'\n")
    os.utime(os.path.join(cfg, 'last_run.env'), (time.time() - 3600, time.time() - 3600))
    time.sleep(1.1)
    base = {'item': 'it3', 'key': f'{LIB}_it3', 'title': 'Third Night', 'field': 'publishedYear', 'source': 'test'}
    _apply_plan('20990101-000004', [dict(base, old=None, new='2001')])
    from webui import core
    run = core.RUNNER.runs()[0]
    assert run['kind'] == 'apply' and run['status'] == 'ok' and 'old' not in (run.get('summary') or '') + (run.get('subject') or ''), run
