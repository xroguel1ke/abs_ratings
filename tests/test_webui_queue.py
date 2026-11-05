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
    html = client().get('/status').get_data(as_text=True)
    assert '80.0' in html and '100.0' in html and '4321' in html and 'blocked' in html, html[-900:]


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
