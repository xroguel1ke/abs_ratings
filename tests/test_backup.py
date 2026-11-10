"""ABS backup before a real run: at most one per ABS_BACKUP_HOURS. Run: python -m pytest tests/test_backup.py"""
import os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/backup_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import abs_ratings as A


class R:
    def __init__(self, code=200, js=None): self.status_code, self._js, self.text = code, js or {}, ''
    def json(self): return self._js


def sess(monkeypatch, backups):
    posts = []

    class S:
        def get(self, url, **kw): return R(js={'backups': backups})
        def post(self, url, **kw): posts.append(url); return R()
    monkeypatch.setattr(A, 'abs_session', S())
    return posts


def test_recent_backup_skips(monkeypatch):
    posts = sess(monkeypatch, [{'createdAt': (time.time() - 3 * 3600) * 1000}])
    assert A.create_abs_backup() and posts == []


def test_old_backup_creates(monkeypatch):
    posts = sess(monkeypatch, [{'createdAt': (time.time() - 30 * 3600) * 1000}])
    assert A.create_abs_backup() and len(posts) == 1


def test_no_backups_creates(monkeypatch):
    posts = sess(monkeypatch, [])
    assert A.create_abs_backup() and len(posts) == 1
