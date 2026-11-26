"""Replaced audio files (user 2026-11-26): the chain swaps files in place, ABS keeps the OLD chapter times - quick mode sees the new file
(size + length) and starts a chapter check of exactly those books, once per new file. Run: python tests/test_chapter_files.py (or pytest)"""
import json, os, sys, tempfile
cfg = tempfile.mkdtemp()
os.environ['CONFIG_DIR'] = cfg
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import chapter_audio as CA
from webui import core


def _item(i, size, dur): return {'id': i, 'media': {'size': size, 'duration': dur}}


def test_same_fingerprint_in_web_ui_and_script():
    for it in (_item('a', 628969226, 56955.246), _item('b', 0, 10.0), _item('c', 5, None), {}, None, {'media': {'size': 'x', 'duration': 3}}):
        assert core.file_print(it) == CA.file_print(it), it
    assert CA.file_print(_item('a', 628969226, 56955.246)) == '628969226|56955'


def test_quick_mode_rechecks_replaced_files_once():
    items = [_item('i1', 1000, 3600.0), _item('i2', 2000, 7200.0)]
    started = []

    class R:
        def running(self): return False
        def start(self, kind, **kw): started.append((kind, kw.get('only_items')))

    class Resp:
        def json(self): return {'results': items}

    json.dump({'lib_i1': 1, 'lib_i2': 1}, open(os.path.join(cfg, 'rating_history.json'), 'w'))  # both known: no 'new book' run
    old_r, old_get = core.RUNNER, core.abs_get
    core.RUNNER, core.abs_get = R(), lambda *a, **k: Resp()
    s = {'QUICK_MODE_MINUTES': 15, 'LIBRARY_IDS': 'lib', 'MATCH_CHAPTERS': True, 'DRY_RUN': False}
    try:
        def poll(): core._quick['t'] = 0; core.quick_poll(s)
        poll()  # first sight: fingerprints stored, nothing started
        assert started == [] and json.load(open(core.CHAPTER_FILES)) == {'lib_i1': '1000|3600', 'lib_i2': '2000|7200'}
        items[1] = _item('i2', 2500, 7210.0)  # the chain swapped i2's file
        poll()
        assert started == [('chapters', ['lib_i2'])], started
        poll()  # the chapter check has not stored the new print (yet): not started again for the same file
        assert len(started) == 1
        poll_dry = dict(s, DRY_RUN=True); items[0] = _item('i1', 1100, 3600.0); core._quick['t'] = 0; core.quick_poll(poll_dry)
        assert len(started) == 1  # a test-run setting would never store the print: no endless re-runs
    finally:
        core.RUNNER, core.abs_get = old_r, old_get


def test_old_chapters_follow_the_new_files_marks():
    import random
    rnd = random.Random(7)
    old_starts = [0.0, 67.0] + [1450.0 + 1000.0 * i + rnd.uniform(-200, 200) for i in range(30)]
    old = [{'start': s, 'end': (old_starts[i + 1] if i + 1 < len(old_starts) else 31600.0), 'title': f'Chapter {i}'} for i, s in enumerate(old_starts)]
    dur = 31650.0
    # 'Snow Crash': the new file's parts start 23-27 s after the old chapters at first, ~5 s later on (two releases, one cut differently)
    marks = [{'start': 0.0}] + [{'start': s + (25 + rnd.uniform(-2, 2) if i < 18 else 5 + rnd.uniform(-2, 2))} for i, s in enumerate(old_starts[2:])]
    new, why = CA.realign_on_marks(old, marks, dur)
    assert new and len(new) == len(old) and 'steady offset' in why, why
    assert [c['title'] for c in new] == [c['title'] for c in old]
    assert all(abs(n['start'] - m['start']) < 0.01 for n, m in zip(new[2:], marks[1:]))
    assert new[-1]['end'] == dur
    # 'Forever Peace': parts every 15 min have nothing to do with the old chapters
    parts = [{'start': 900.0 * i} for i in range(35)]
    assert CA.realign_on_marks(old, parts, dur)[0] is None
    assert CA.realign_on_marks(old, marks[:5], dur)[0] is None  # too few marks: no proof


if __name__ == '__main__':
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith('test_') and callable(fn): fn(); n += 1; print('ok', name)
    print(n, 'tests passed')
