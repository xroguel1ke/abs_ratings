"""Library tasks faster (user 2026-11-24: 'das dauert immer recht lang am ende jedes runs'). Run: python -m pytest tests/test_library_speed.py"""
import os, sys, json
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/libspeed_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import abs_series as S

SRC = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'abs_ratings.py'), encoding='utf-8').read()


class R:
    def __init__(self, code, data): self.status_code, self._d = code, data
    def json(self): return self._d


class Sess:
    """ABS: batch/get returns the full items except 'gone2' (the single request answers it); 'fail' makes the batch fail."""
    def __init__(self, fail=False): self.fail, self.posts, self.gets = fail, 0, []
    def post(self, url, json=None, timeout=None):
        self.posts += 1
        if self.fail: raise OSError('down')
        return R(200, {'libraryItems': [{'id': i, 'media': {'metadata': {'title': i}}} for i in json['libraryItemIds'] if i != 'gone2']})
    def get(self, url):
        self.gets.append(url.rsplit('/', 1)[-1])
        return R(200, {'id': url.rsplit('/', 1)[-1], 'media': {'metadata': {'title': 'single'}}})


def test_full_items_come_in_batches_of_500_with_single_fallback():
    items = [{'id': f'i{n}'} for n in range(1200)] + [{'id': 'gone2'}]
    s = Sess(); out = S.load_full_items(s, 'http://abs', items)
    assert s.posts == 3 and s.gets == ['gone2']  # 1201 items: 3 batch requests, one item one by one
    assert [it['id'] for it, _, _ in out] == [it['id'] for it in items]  # listing order kept
    assert out[-1][1]['media']['metadata']['title'] == 'single' and out[0][1]['id'] == 'i0'
    s = Sess(fail=True); out = S.load_full_items(s, 'http://abs', items[:10])
    assert s.posts == 1 and len(s.gets) == 10 and all(d for _, d, _ in out)  # batch down: the old way


def test_series_list_asks_the_working_asin_first():
    # 76 dead series ASINs were asked again in every run (an answer without a list is not cached); the ASIN that answered before goes first
    assert 'cands.sort(key=lambda c: not (cache.get(f"v2|{c[0]}|{c[1]}") or {}).get(\'members\'))' in SRC
    cache = {'v2|api.audible.com|B0GOOD': {'members': [{'asin': 'x'}]}}
    cands = [(h, sa) for h in ['api.audible.com', 'api.audible.co.uk'] for sa in sorted({'B0AAAA', 'B0GOOD'})]
    cands.sort(key=lambda c: not (cache.get(f"v2|{c[0]}|{c[1]}") or {}).get('members'))
    assert cands[0] == ('api.audible.com', 'B0GOOD') and cands[1:] == [('api.audible.com', 'B0AAAA'), ('api.audible.co.uk', 'B0AAAA'), ('api.audible.co.uk', 'B0GOOD')]


def test_passed_source_check_is_reused_for_an_hour(monkeypatch):
    import abs_ratings as A
    calls = []
    monkeypatch.setattr(A.SH, 'run_canaries', lambda *a, **k: calls.append(k.get('only')) or {})
    monkeypatch.setattr(A, 'SOURCE_CANARY', True); monkeypatch.setattr(A, 'USE_AUDIBLE_API', True)
    f = os.path.join(A.SCRIPT_DIR, 'source_check.json')
    if os.path.exists(f): os.remove(f)
    A.source_canaries(); A.source_canaries()
    assert calls == [None], calls  # second run within the hour: reused
    A.source_canaries(only='audible'); assert calls == [None, 'audible']  # the breaker always asks
    d = json.load(open(f)); d['ok_at'] = '2000-01-01T00:00:00+00:00'; json.dump(d, open(f, 'w'))
    A.source_canaries(); assert calls[-1] is None and len(calls) == 3  # older than an hour: checked again
    monkeypatch.setattr(A.SH, 'run_canaries', lambda *a, **k: calls.append('x') or {'hardcover': 'broken'})
    monkeypatch.setattr(A, 'source_down', lambda s_, why: None); monkeypatch.setattr(A.time, 'sleep', lambda s: None)
    json.dump({'ok_at': '2000-01-01T00:00:00+00:00', 'hardcover': True}, open(f, 'w'))
    A.source_canaries(); assert not os.path.exists(f)  # a failed check is never reused
