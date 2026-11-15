"""Manual-update detection: a newer ABS time stamp alone (scan / mover) is no manual update. Run: python -m pytest tests/test_manual_update.py"""
import os, sys, tempfile, copy
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/manual_update_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import abs_ratings as A

LIB = 'lib1'


def item(title='Book', updated=None):
    return {'id': 'x', 'path': '/data/a/Author/Book', 'updatedAt': int(updated.timestamp() * 1000), 'media': {
        'metadata': {'title': title, 'authorName': 'Author', 'asin': 'B000000001'}, 'tags': [], 'duration': 3600, 'coverPath': '/c.jpg'}}


def run(monkeypatch, items, fp_store=None):
    d = tempfile.mkdtemp()
    monkeypatch.setattr(A, 'SCRIPT_DIR', d)
    monkeypatch.setattr(A, 'ITEM_PATHS_FILE', os.path.join(d, 'item_paths.json'))
    monkeypatch.setattr(A, 'META_FP_FILE', os.path.join(d, 'fp.json'))
    monkeypatch.setattr(A, 'REVIEW_FILE', os.path.join(d, 'review.json'))
    monkeypatch.setattr(A, 'DRY_RUN', False)
    monkeypatch.setattr(A, 'REFRESH_DAYS', 60)
    if fp_store is not None: A.rw_json(A.META_FP_FILE, fp_store)

    class R:
        def __init__(self, js): self._js = js
        def json(self): return self._js

    class S:
        def get(self, url, **kw): return R({'results': copy.deepcopy(items)})
        def post(self, *a, **kw): return R({})
    monkeypatch.setattr(A, 'abs_session', S())
    last = datetime.now(timezone.utc) - timedelta(days=2)
    return A.build_queue(LIB, {f'{LIB}_x': last.isoformat(timespec='seconds')}, {})


def test_newer_stamp_same_metadata_is_no_manual_update(monkeypatch):
    it = item(updated=datetime.now(timezone.utc) - timedelta(hours=1))
    q = run(monkeypatch, [it], {f'{LIB}_x': A.meta_fingerprint(it)})
    assert not [x for x in q if x.get('_manual_trigger')]


def test_changed_metadata_is_a_manual_update(monkeypatch):
    old = item(title='Old', updated=datetime.now(timezone.utc) - timedelta(days=3))
    new = item(title='New title', updated=datetime.now(timezone.utc) - timedelta(hours=1))
    q = run(monkeypatch, [new], {f'{LIB}_x': A.meta_fingerprint(old)})
    assert [x for x in q if x.get('_manual_trigger')]


def test_unknown_fingerprint_keeps_the_old_behaviour(monkeypatch):
    q = run(monkeypatch, [item(updated=datetime.now(timezone.utc) - timedelta(hours=1))], {})
    assert [x for x in q if x.get('_manual_trigger')]
    assert A.rw_json(A.META_FP_FILE).get(f'{LIB}_x')  # stored for the next run


def test_fingerprint_ignores_inode_and_path():
    a = item(updated=datetime.now(timezone.utc)); b = copy.deepcopy(a); b['path'] = '/data/b/Author/Book'; b['ino'] = '123'
    assert A.meta_fingerprint(a) == A.meta_fingerprint(b)
