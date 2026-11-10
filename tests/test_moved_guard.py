"""Moved-item guard: ABS giving a NEW folder the entry of another book (inode takeover). Run: python -m pytest tests/test_moved_guard.py"""
import os, sys, tempfile
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/moved_guard_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import abs_ratings as A

LIB = 'lib1'
ORB = '/data/audiobooks/Katherine Addison/The Chronicles of Osreth/1.1 - The Orb of Cairado - Zehra Jane Naqvi'
AMB = '/data/audiobooks/Trudi Canavan/Kyralia Universe/1 - The Ambassadors Mission - Richard Aspel'


def item(iid, path, title, author):
    return {'id': iid, 'path': path, 'media': {'metadata': {'title': title, 'authorName': author}}}


def setup(monkeypatch):
    d = tempfile.mkdtemp()
    monkeypatch.setattr(A, 'ITEM_PATHS_FILE', os.path.join(d, 'item_paths.json'))
    monkeypatch.setattr(A, 'REVIEW_FILE', os.path.join(d, 'review_queue.json'))
    monkeypatch.setattr(A, 'SCRIPT_DIR', d)
    monkeypatch.setattr(A, 'DRY_RUN', False)
    monkeypatch.setattr(A, 'OVERRIDES', {})
    posts = []

    class S:
        def post(self, url, **kw): posts.append(url)
    monkeypatch.setattr(A, 'abs_session', S())
    return posts


def test_takeover_is_held_back(monkeypatch):
    posts = setup(monkeypatch)
    assert len(A.guard_moved_items(LIB, [item('x', ORB, 'The Orb of Cairado', 'Katherine Addison')])) == 1  # first run: snapshot only
    out = A.guard_moved_items(LIB, [item('x', AMB, 'The Orb of Cairado', 'Katherine Addison')])
    assert out == []  # not processed
    q = A.rw_json(A.REVIEW_FILE)
    assert q and q[0]['kind'] == 'moved' and q[0]['candidate'] == AMB
    assert posts and posts[0].endswith(f'/api/libraries/{LIB}/scan')
    # next run: still the other book's metadata -> still held back, no second scan
    assert A.guard_moved_items(LIB, [item('x', AMB, 'The Orb of Cairado', 'Katherine Addison')]) == []
    assert len(posts) == 1
    # fixed (title/author now fit the folder): processed again
    assert len(A.guard_moved_items(LIB, [item('x', AMB, "The Ambassador's Mission", 'Trudi Canavan')])) == 1


def test_identity_override_releases(monkeypatch):
    setup(monkeypatch)
    A.guard_moved_items(LIB, [item('x', ORB, 'The Orb of Cairado', 'Katherine Addison')])
    A.guard_moved_items(LIB, [item('x', AMB, 'The Orb of Cairado', 'Katherine Addison')])
    monkeypatch.setattr(A, 'OVERRIDES', {f'{LIB}_x': {'asin': 'B0055EBH6W', 'identity': True}})
    assert len(A.guard_moved_items(LIB, [item('x', AMB, 'The Orb of Cairado', 'Katherine Addison')])) == 1


def test_real_moves_pass(monkeypatch):
    posts = setup(monkeypatch)
    A.guard_moved_items(LIB, [item('a', '/data/audiobooks/Joe Abercrombie/Best Served Cold', 'Best Served Cold', 'Joe Abercrombie'),
                              item('b', '/data/audiobooks/Unsorted/Red Country', 'Red Country', 'Joe Abercrombie')])
    out = A.guard_moved_items(LIB, [item('a', '/data/audiobooks/Joe Abercrombie/The First Law World/4 - Best Served Cold', 'Best Served Cold', 'Joe Abercrombie'),
                                    item('b', '/data/audiobooks/Joe Abercrombie/The First Law World/6 - Red Country', 'Red Country', 'Joe Abercrombie')])
    assert len(out) == 2 and not posts and not A.rw_json(A.REVIEW_FILE)


def test_dry_run_keeps_snapshot(monkeypatch):
    posts = setup(monkeypatch)
    A.guard_moved_items(LIB, [item('x', ORB, 'The Orb of Cairado', 'Katherine Addison')])
    monkeypatch.setattr(A, 'DRY_RUN', True)
    assert A.guard_moved_items(LIB, [item('x', AMB, 'The Orb of Cairado', 'Katherine Addison')]) == []
    assert not posts and A.rw_json(A.ITEM_PATHS_FILE)[f'{LIB}_x'][0] == ORB
