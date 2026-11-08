"""Goodreads lookup order (no network): book page first with its own budget, a bot challenge on a book page only switches
pages off and the search list is the fallback; a block on the search stops Goodreads. Run: python tests/test_goodreads_flow.py"""
import os, sys
os.environ.setdefault('DATA_DIR', '/tmp/gr_flow_test'); os.environ.setdefault('ABS_URL', 'http://127.0.0.1:1'); os.environ.setdefault('API_TOKEN', 'x')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import abs_ratings as A

PAGE = {'name': 'Project Hail Mary', 'author': 'Andy Weir', 'val': '4.51', 'count': 1000}


def setup(pages=None, search=None):
    A.GR_STATE.clear(); A.GR_STATE.update({'blocked': False, 'cooldowns': 0, 'searches': 0, 'pages': 0, 'pages_blocked': False})
    calls = {'pages': [], 'search': 0}

    def page(url, require_book_page=False):
        calls['pages'].append(url)
        return pages(url) if callable(pages) else pages

    def srch(*a, **k):
        calls['search'] += 1
        assert k.get('list_only') is True  # the fallback never opens book pages
        if isinstance(search, Exception): raise search
        return search
    A.scrape_gr_details, A._search_goodreads = page, srch
    A.GR_BOOK_PAGES, A.GOODREADS_PAGES_PER_RUN, A.GOODREADS_MAX_PER_RUN = True, 3, 25
    return calls


def look(hc_ids=('1',), isbn=None):
    return A.get_goodreads_data(isbn, 'Project Hail Mary', ['Andy Weir'], 'Andy Weir', hc_ids=list(hc_ids), force=True)


def test_book_page_first():
    c = setup(pages=dict(PAGE), search={'val': '1'})
    d, st = look()
    assert st == 'found' and d['source'] == 'Hardcover-ID' and c['search'] == 0


def test_challenge_on_book_page_falls_back_to_search():
    def blocked(url, **k): raise A.SourceBlockedException('Bot challenge (HTTP 202)')
    c = setup(pages=blocked, search={'val': '4.5', 'count': 9})
    d, st = look()
    assert st == 'found' and c['search'] == 1 and A.GR_STATE['pages_blocked'] and not A.GR_STATE['blocked']
    look()  # next book: no page request any more, search only
    assert len(c['pages']) == 1 and c['search'] == 2


def test_page_budget_then_search():
    c = setup(pages=None, search=None)  # pages give nothing -> search each time
    for _ in range(3): look(hc_ids=('1', '2'))
    assert len(c['pages']) == 3 and c['search'] == 3  # budget 3 pages over all books


def test_wrong_page_is_not_taken():
    c = setup(pages={'name': 'Ave Maria', 'author': 'Someone Else', 'val': '3.1', 'count': 5}, search=None)
    d, st = look()
    assert d is None and st == 'not_found' and c['search'] == 1


def test_search_block_stops_goodreads():
    A.GR_MAX_COOLDOWNS = 0
    setup(pages=None, search=A.SourceBlockedException('HTTP 429'))
    d, st = look(hc_ids=())
    assert st == 'blocked' and A.GR_STATE['blocked']


def test_retry_after_parsing():
    class R:
        def __init__(self, v): self.headers = {'Retry-After': v} if v else {}
    assert A.retry_after_seconds(R('120')) == 120 and A.retry_after_seconds(R(None)) is None and A.retry_after_seconds(R('soon')) is None


if __name__ == '__main__':
    for n, f in list(globals().items()):
        if n.startswith('test_'): f(); print('ok', n)
