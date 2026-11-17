"""Login: only own paths as redirect target after the login, and a pause after too many wrong passwords.
Run: python -m pytest tests/test_webui_login.py"""
import os, sys, tempfile

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
os.environ['CONFIG_DIR'] = tempfile.mkdtemp()
sys.path.insert(0, ROOT)
from webui import app as W  # noqa: E402

W.app.config['TESTING'] = True


def client():
    W._LOGIN_FAILS.clear()
    return W.app.test_client()


def test_safe_next():
    with W.app.test_request_context():
        home = W.url_for('dashboard')
        assert W.safe_next('/books?q=x') == '/books?q=x'
        for bad in ('https://evil.example/', '//evil.example/', '/\\evil.example', 'evil.example', '', None, '/books\r\nX: y'):
            assert W.safe_next(bad) == home, bad


def test_login_redirect_stays_on_site():
    c = client()
    r = c.post('/login?next=https://evil.example/', data={'password': 'admin'})
    assert r.status_code == 302 and 'evil.example' not in r.headers['Location']
    c = client()
    r = c.post('/login?next=/books', data={'password': 'admin'})
    assert r.status_code == 302 and r.headers['Location'].endswith('/books')


def test_lock_after_wrong_passwords(monkeypatch):
    c = client()
    for _ in range(W.LOGIN_MAX_FAILS):
        r = c.post('/login', data={'password': 'wrong'})
        assert r.status_code == 200
    r = c.post('/login', data={'password': 'admin'})  # right password, but locked: no session
    assert r.status_code == 200 and b'please wait' in r.data
    with c.session_transaction() as s: assert not s.get('auth')
    t0 = W.time.monotonic()  # the pause is over -> the right password works again
    monkeypatch.setattr(W.time, 'monotonic', lambda: t0 + W.LOGIN_LOCK_SECONDS + 1)
    r = c.post('/login', data={'password': 'admin'})
    assert r.status_code == 302


def test_success_resets_counter():
    c = client()
    for _ in range(W.LOGIN_MAX_FAILS - 1): c.post('/login', data={'password': 'wrong'})
    assert c.post('/login', data={'password': 'admin'}).status_code == 302
    assert not W._LOGIN_FAILS
