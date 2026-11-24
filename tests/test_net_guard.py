"""Network outages (user 2026-11-24, 'Syl: Nucleus': 'No route to host' for 20+ min -> 'not found' + strike). Run: python -m pytest tests/test_net_guard.py"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/net_guard_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import pytest, requests
from requests.adapters import HTTPAdapter
import net_guard as G

REFUSED = requests.exceptions.ConnectionError("HTTPSConnectionPool(host='api.audible.com', port=443): Max retries exceeded (Caused by "
                                              "NewConnectionError('Failed to establish a new connection: [Errno 113] No route to host'))")


class Fake:
    def __init__(self, fails, exc=REFUSED): self.fails, self.calls, self.exc = fails, 0, exc
    def __call__(self, adapter, request, **kw):
        self.calls += 1
        if self.calls <= self.fails: raise self.exc
        r = requests.Response(); r.status_code, r._content, r.url = 200, b'{}', request.url
        return r


@pytest.fixture
def guard(monkeypatch):
    """A fresh guard around a fake transport; probes and sleeps are fakes too."""
    orig = HTTPAdapter.send
    state = {'up': True, 'probes': 0}
    def probe(addr, timeout):
        state['probes'] += 1
        if not state['up']: raise OSError('no route')
        class S:
            def close(self): pass
        return S()
    monkeypatch.setattr(G, '_probe_connect', probe)
    monkeypatch.setattr(G, '_sleep', lambda s: None)
    monkeypatch.setattr(G, 'WAIT_MINUTES', 0.0005)
    G.GAVE_UP.clear(); G._SEEN_OK.clear(); G.book_start()
    def use(fake):
        HTTPAdapter.send = fake; G._installed[0] = False; G.install('http://abs.local:13378')
    yield use, state
    HTTPAdapter.send = orig; G._installed[0] = True; G.GAVE_UP.clear(); G.book_start()


def test_short_blip_is_waited_out(guard):
    use, state = guard
    f = Fake(fails=1); use(f)
    assert requests.get('https://api.audible.com/1.0/catalog/products/X').status_code == 200
    assert f.calls == 2 and not G.book_failed() and not G.any_down()


def test_long_outage_gives_up_and_stops(guard):
    use, state = guard
    state['up'] = False; f = Fake(fails=99); use(f)
    with pytest.raises(requests.exceptions.ConnectionError):
        requests.get('https://api.audible.com/1.0/catalog/products/X')
    assert G.book_failed() and G.any_down()
    n = f.calls
    with pytest.raises(requests.exceptions.ConnectionError):  # no second wait: fails at once while the host counts as down
        requests.get('https://api.audible.com/1.0/catalog/products/Y')
    assert f.calls == n


def test_unknown_host_and_unknown_name_fail_at_once(guard):
    use, state = guard
    f = Fake(fails=1); use(f)
    with pytest.raises(requests.exceptions.ConnectionError):
        requests.get('https://some-dead-site.example/x')  # not a source of the script, never answered: old behaviour
    assert state['probes'] == 0 and not G.book_failed()
    nx = requests.exceptions.ConnectionError("Failed to resolve 'api.audible.com' ([Errno -2] Name or service not known)")
    f2 = Fake(fails=1, exc=nx); use(f2)
    with pytest.raises(requests.exceptions.ConnectionError):
        requests.get('https://api.audible.com/x')
    assert state['probes'] == 0


def test_tls_error_is_no_outage(guard):
    use, state = guard
    f = Fake(fails=1, exc=requests.exceptions.SSLError('bad cert')); use(f)
    with pytest.raises(requests.exceptions.SSLError):
        requests.get('https://api.audible.com/x')
    assert state['probes'] == 0


def test_host_seen_in_this_run_waits_too(guard):
    use, state = guard
    G._SEEN_OK.add('covers.example'); f = Fake(fails=1); use(f)
    assert requests.get('https://covers.example/a.jpg').status_code == 200 and f.calls == 2


def test_give_up_expires(guard, monkeypatch):
    use, state = guard
    G.GAVE_UP['api.audible.com'] = ('no route', 0.0)  # long ago
    assert G.down('api.audible.com') is None and not G.any_down()


def test_no_strike_and_no_result_after_an_outage(monkeypatch):
    import abs_ratings as A
    G.book_start(); G._TL.failed = 'No route to host'
    hist, failed, rec = {}, {}, {}
    A.record_attempt('k1', hist, failed, rec, complete=False)
    assert failed == {} and hist == {}
    A.RESULTS['k1'] = {'status': 'ok', 'title': 'old'}
    A.set_result('k1', 'l', 'i', 'T', ['A'], None, 'not_found', 'ASIN not found')
    assert A.RESULTS['k1'] == {'status': 'ok', 'title': 'old'}
    G.book_start()
    monkeypatch.setattr(A, 'DRY_RUN', True)  # without an outage the strike path runs as before (DRY_RUN: nothing persisted)
    A.record_attempt('k1', hist, failed, rec, complete=False)
    A.RESULTS.pop('k1', None)


def test_wired_into_the_run():
    src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'abs_ratings.py'), encoding='utf-8').read()
    assert "net_guard.install(os.getenv('ABS_URL', ''))" in src
    assert "or net_guard.any_down(): return\n        net_guard.book_start()" in src  # every book starts clean, an outage stops the queue
    assert src.count('net_guard.book_start()') == 2  # per-book run and Goodreads mode
    dock = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Dockerfile'), encoding='utf-8').read()
    assert 'net_guard.py' in dock


def test_requests_get_reuses_one_session_per_thread(monkeypatch):
    # DS-Lite session limit (2026-11-24): requests.get must not open a new connection pool per call; cookies do not carry over
    G.install()
    assert requests.api.request is G.pooled_request
    seen = []
    def fake_request(self, method, url, **kw):
        seen.append(self); self.cookies.set('sid', 'x')
        r = requests.Response(); r.status_code = 200; return r
    monkeypatch.setattr(requests.Session, 'request', fake_request)
    requests.get('https://api.audible.com/a'); requests.post('https://api.audible.com/b', json={})
    assert len(seen) == 2 and seen[0] is seen[1]
    assert not seen[0].cookies  # cleared after every call (like a fresh session)
    import threading
    other = []
    t = threading.Thread(target=lambda: (requests.get('https://api.audible.com/c'), other.append(seen[-1]))); t.start(); t.join()
    assert other[0] is not seen[0]  # one session per thread
