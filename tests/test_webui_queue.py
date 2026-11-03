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
    html = c.get('/?tab=queue').get_data(as_text=True)
    assert 'Test Book One' in html and 'B0TEST0001' in html and 'Test Book Two' in html, html[:400]
    qid = core.open_queue()[0]['id']
    r = c.post(f'/queue/{qid}/reject'); assert r.status_code in (302, 303)
    html = c.get('/?tab=queue').get_data(as_text=True)
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


if __name__ == '__main__':
    for n, f in sorted(globals().items()):
        if n.startswith('test_'): f(); print('ok', n)
    print('webui queue tests passed')
