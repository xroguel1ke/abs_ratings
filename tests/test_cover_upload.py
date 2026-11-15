"""Cover upload: longer timeout, a timeout is no crash, a failed original is only info when the smaller cover follows (run 2026-11-15).
Run: python -m pytest tests/test_cover_upload.py"""
import os, sys, logging
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/cover_upload_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import abs_ratings as A


class R:
    def __init__(self, code, text=''): self.status_code, self.text = code, text


def setup(monkeypatch, post):
    seen = []
    monkeypatch.setattr(A, 'DRY_RUN', False)
    monkeypatch.setattr(A, 'journal_change', lambda *a: seen.append(a))
    monkeypatch.setattr(A.abs_session, 'post', post)
    return seen


def test_timeout_is_handled(monkeypatch, caplog):
    kw = {}
    def post(url, **k):
        kw.update(k); raise A.requests.exceptions.ReadTimeout('Read timed out')
    seen = setup(monkeypatch, post)
    with caplog.at_level(logging.INFO):
        assert A.abs_set_cover('i1', 'k', 'T', 'http://x/a.jpg', 'audible_cover') is False
    assert kw['timeout'] == 120 and not seen
    assert any(r.levelno == logging.ERROR and 'Read timed out' in r.getMessage() for r in caplog.records)


def test_fallback_failure_is_info(monkeypatch, caplog):
    setup(monkeypatch, lambda url, **k: R(400, 'Failed to download image from url'))
    with caplog.at_level(logging.INFO):
        assert A.abs_set_cover('i1', 'k', 'T', 'http://x/a.jpg', 'audible_cover', fallback=True) is False
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert any('trying the smaller one' in r.getMessage() for r in caplog.records)


def test_success_journals(monkeypatch):
    seen = setup(monkeypatch, lambda url, **k: R(200))
    assert A.abs_set_cover('i1', 'k', 'T', 'http://x/a.jpg', 'audible_cover') is True and len(seen) == 1
