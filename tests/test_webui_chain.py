"""Review list: books whose FILE is the problem are the audiobook chain's job (user 2026-11-26). Run: python tests/test_webui_chain.py"""
import json, os, sys, tempfile
cfg = tempfile.mkdtemp()
os.environ['CONFIG_DIR'] = cfg
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
json.dump([{'key': 'lib_1', 'title': 'Charon Book', 'kind': 'incomplete', 'candidate': '110', 'reason': 'the audio file is 110 min shorter', 'status': 'open'},
           {'key': 'lib_2', 'title': 'Asin Book', 'kind': 'asin', 'candidate': 'B0TEST0002', 'reason': 'weak evidence', 'evidence': [], 'status': 'open'}],
          open(os.path.join(cfg, 'review_queue.json'), 'w'))
json.dump({'lib_1': {'title': 'Charon Book', 'status': 'ok'}, 'lib_2': {'title': 'Asin Book', 'status': 'ok'}}, open(os.path.join(cfg, 'results.json'), 'w'))
json.dump({'must_change_password': False}, open(os.path.join(cfg, 'settings.json'), 'w'))
RD = {'lib_1': {'title': 'Charon Book', 'authors': ['R. A. S.'], 'kind': 'incomplete', 'reason': 'Audible\'s last 2 chapter(s) are not in the file'},
      'lib_3': {'title': 'Parts Book', 'authors': ['X'], 'kind': 'chapters', 'reason': 'only a few marks hours apart (file parts)'}}
json.dump(RD, open(os.path.join(cfg, 'redownload.json'), 'w'))
from webui import app as W, core


def client():
    c = W.app.test_client()
    with c.session_transaction() as s: s['auth'] = True
    return c


def test_chain_books_are_folded_away():
    st = os.path.join(cfg, 'redownload_status.json')
    json.dump({'lib_1': {'status': 'searching', 'ts': '2026-11-26T11:00:00'}}, open(st, 'w'))
    q, busy, gave = core.chain_split(core.open_queue())
    assert [x['key'] for x in q] == ['lib_2'] and {e['key'] for e in busy} == {'lib_1', 'lib_3'} and not gave
    html = client().get('/review').get_data(as_text=True)
    assert 'Asin Book' in html and 'Parts Book' in html and 'Charon Book' in html, ('Asin Book' in html, 'Parts Book' in html, 'Charon Book' in html, html[:300])  # folded, still listed
    assert '<details class="card"><summary><b>' in html


def test_chain_gave_up_is_shown_to_the_user():
    json.dump({'lib_3': {'status': 'not_better', 'note': 'only the same release found', 'ts': '2026-11-26T11:00:00'}}, open(os.path.join(cfg, 'redownload_status.json'), 'w'))
    q, busy, gave = core.chain_split(core.open_queue())
    assert [e['key'] for e in gave] == ['lib_3'] and [e['key'] for e in busy] == ['lib_1']
    html = client().get('/review').get_data(as_text=True)
    assert 'only the same release found' in html


if __name__ == '__main__':
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith('test_') and callable(fn): fn(); n += 1; print('ok', name)
    print(n, 'tests passed')
