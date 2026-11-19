"""Folder covers to /metadata (spin-up chat 2026-11-19): a book whose coverPath points into the library (/data/...) and that got no new
cover in this run gets the same image uploaded as a file - ABS keeps it in /metadata/items/<id>/, so apps no longer wake the array disk.
Run: python -m pytest tests/test_cover_to_metadata.py"""
import os, sys, logging
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/cover_meta_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import abs_ratings as A

JPG = b'\xff\xd8\xff\xe0' + b'x' * 100


class R:
    def __init__(self, code, content=b'', js=None, text=''):
        self.status_code, self.content, self._js, self.text = code, content, js, text
    def json(self):
        if self._js is None: raise ValueError('no json')
        return self._js


def setup(monkeypatch, post, get=None, dry=False):
    seen, posts = [], []
    monkeypatch.setattr(A, 'DRY_RUN', dry)
    monkeypatch.setattr(A, 'COVERS_SET', set())
    monkeypatch.setattr(A, 'journal_change', lambda *a: seen.append(a))
    monkeypatch.setattr(A.abs_session, 'get', get or (lambda url, **k: R(200, JPG)))
    monkeypatch.setattr(A.abs_session, 'post', lambda url, **k: posts.append((url, k)) or post(url, **k))
    return seen, posts


def ok(url, **k): return R(200, js={'success': True, 'cover': '/metadata/items/i1/cover.jpg'})


def test_folder_cover_is_uploaded(monkeypatch):
    seen, posts = setup(monkeypatch, ok)
    assert A.cover_to_metadata('i1', 'L_i1', 'T', '/data/media/books/A/B/cover.jpg') is True
    url, k = posts[0]
    assert url.endswith('/api/items/i1/cover') and k['files']['cover'][0] == 'cover.jpg' and k['files']['cover'][1] == JPG
    assert k['headers'] == {'Content-Type': None}
    (iid, key, title, field, old, new, source, extra), = seen
    assert (field, old, new, source, extra) == ('cover', '/data/media/books/A/B/cover.jpg', '/metadata/items/i1/cover.jpg', 'cover_to_metadata', {'cover_move': True})


def test_multipart_header_reaches_abs():
    # the session's JSON content type must not survive, or ABS gets no file (no multipart boundary)
    p = A.abs_session.prepare_request(A.requests.Request('POST', 'http://abs.local/api/items/i1/cover', files={'cover': ('cover.jpg', JPG, 'image/jpeg')},
                                                         headers={'Content-Type': None}))
    assert p.headers['Content-Type'].startswith('multipart/form-data; boundary=')


def test_nothing_to_do(monkeypatch):
    seen, posts = setup(monkeypatch, ok)
    assert A.cover_to_metadata('i1', 'k', 'T', '/metadata/items/i1/cover.jpg') is False  # already in /metadata
    assert A.cover_to_metadata('i1', 'k', 'T', None) is False                            # no cover at all
    A.COVERS_SET.add('i2')
    assert A.cover_to_metadata('i2', 'k', 'T', '/data/x/cover.jpg') is False             # got a cover from Audible in this run
    assert not posts and not seen


def test_extension_and_png(monkeypatch):
    png = b'\x89PNG\r\n\x1a\n' + b'x' * 50
    seen, posts = setup(monkeypatch, ok, get=lambda url, **k: R(200, png))
    A.cover_to_metadata('i1', 'k', 'T', '/data/x/folder.PNG')
    assert posts[0][1]['files']['cover'][0] == 'cover.png' and posts[0][1]['files']['cover'][2] == 'image/png'
    posts.clear()
    A.cover_to_metadata('i3', 'k', 'T', '/data/x/cover')  # no extension: from the bytes
    assert posts[0][1]['files']['cover'][0] == 'cover.png'


def test_failure_keeps_folder_cover(monkeypatch, caplog):
    seen, _ = setup(monkeypatch, lambda url, **k: R(400, text='Invalid image type'))
    with caplog.at_level(logging.INFO):
        assert A.cover_to_metadata('i1', 'k', 'T', '/data/x/cover.jpg') is False
    assert not seen and any('stays in the book folder' in r.getMessage() for r in caplog.records)
    # ABS answers 200 but not with a /metadata path: not counted as done
    seen, _ = setup(monkeypatch, lambda url, **k: R(200, js={'success': True, 'cover': '/data/x/cover.jpg'}))
    assert A.cover_to_metadata('i1', 'k', 'T', '/data/x/cover.jpg') is False and not seen
    # the folder file is missing (e-book 'Crossroads of Twilight'): no upload
    seen, posts = setup(monkeypatch, ok, get=lambda url, **k: R(404))
    assert A.cover_to_metadata('i1', 'k', 'T', '/data/x/cover.jpg') is False and not posts and not seen


def test_dry_run_only_plans(monkeypatch):
    seen, posts = setup(monkeypatch, ok, dry=True)
    assert A.cover_to_metadata('i1', 'k', 'T', '/data/x/cover.jpg') is True
    assert not posts and seen[0][3] == 'cover' and seen[0][7] == {'cover_move': True}


def test_url_cover_marks_the_book(monkeypatch):
    monkeypatch.setattr(A, 'COVERS_SET', set())
    monkeypatch.setattr(A, 'DRY_RUN', False)
    monkeypatch.setattr(A, 'journal_change', lambda *a: None)
    monkeypatch.setattr(A.abs_session, 'post', lambda url, **k: R(200))
    assert A.abs_set_cover('i9', 'k', 'T', 'http://x/a.jpg', 'audible_cover') is True
    assert 'i9' in A.COVERS_SET  # the folder copy is skipped for this book afterwards


def test_undo_sends_nothing():
    # ABS copies any folder image into /metadata again (validateCoverPath), so a move has no real undo: abs_undo skips it, no request
    src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'abs_undo.py'), encoding='utf-8').read()
    i = src.index("e.get('cover_move')")
    branch = src[i:src.index('elif', i)]
    assert 'cannot be undone' in branch and 'continue' in branch and 'sess.' not in branch and 'requests.' not in branch


def test_locks_do_not_stop_the_copy():
    # spin-up chat 2026-11-19: lock_cover / lock_all keep the image - the bytes stay the same, only the place changes
    src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'abs_ratings.py'), encoding='utf-8').read()
    job = src.index("cover_job = (iid, key, title,")
    line = src[job:src.index('\n', job)]
    assert job < src.index("if 'lock_all' in tags:", job) and 'lock' not in line and 'tags' not in line


def test_web_ui_switch():
    import webui.core as C
    f = next(x for x in C.FIELDS if x[0] == 'COVERS_TO_METADATA')
    assert f[2] == 'bool' and f[3] is True and f[5] == 'Metadata'  # on by default, in Settings -> Metadata
    src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'abs_ratings.py'), encoding='utf-8').read()
    assert 'if cover_job and COVERS_TO_METADATA:' in src  # off = the step is not run at all
