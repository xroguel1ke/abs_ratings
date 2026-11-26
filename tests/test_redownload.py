"""Redownload list for the audiobook chain (user 2026-11-26): books whose FILE is the problem. Run: python tests/test_redownload.py (or pytest)"""
import os, sys, tempfile
os.environ.setdefault('DATA_DIR', tempfile.mkdtemp(prefix='redl_')); os.environ.setdefault('ABS_URL', 'http://127.0.0.1:1'); os.environ.setdefault('API_TOKEN', 'x')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import abs_ratings as A
import chapter_check as CC

ITEM = {'id': 'i1', 'relPath': 'R. A. Salvatore/Icewind Dale/01 - The Crystal Shard {Victor Bevine}',
        'media': {'duration': 41480.0, 'metadata': {'title': 'The Crystal Shard', 'authors': [{'name': 'R. A. Salvatore'}], 'narrators': ['Victor Bevine'], 'asin': 'B01'}}}


def _fresh(tmp):
    A.REDOWNLOAD_FILE = os.path.join(tmp, 'redownload.json'); A.DRY_RUN = False


def test_flag_and_clear():
    _fresh(tempfile.mkdtemp())
    A.redownload_flag('lib_i1', ITEM, 'chapters', 'chapters run past the end of the file', missing_min=31)
    e = A.rw_json(A.REDOWNLOAD_FILE)['lib_i1']
    assert e['title'] == 'The Crystal Shard' and e['authors'] == ['R. A. Salvatore'] and e['narrators'] == ['Victor Bevine'] and e['kind'] == 'chapters'
    assert e['path'].endswith('{Victor Bevine}') and e['duration_min'] == 691 and e['missing_min'] == 31 and e['since']
    since = e['since']
    A.redownload_flag('lib_i1', ITEM, 'incomplete', 'x')  # updated, 'since' stays
    assert A.rw_json(A.REDOWNLOAD_FILE)['lib_i1']['since'] == since
    A.redownload_clear('lib_i1', {'chapters'})  # another kind: stays
    assert 'lib_i1' in A.rw_json(A.REDOWNLOAD_FILE)
    A.redownload_clear('lib_i1')
    assert 'lib_i1' not in A.rw_json(A.REDOWNLOAD_FILE)


def test_test_run_writes_nothing():
    _fresh(tempfile.mkdtemp()); A.DRY_RUN = True
    try:
        A.redownload_flag('lib_i1', ITEM, 'chapters', 'x')
        assert not os.path.exists(A.REDOWNLOAD_FILE)
    finally: A.DRY_RUN = False


def test_review_json_is_one_list_for_the_chain():
    tmp = tempfile.mkdtemp(); _fresh(tmp)
    A.REVIEW_FILE, A.RESULTS_FILE, A.REVIEW_EXPORT_FILE = (os.path.join(tmp, f) for f in ('review_queue.json', 'results.json', 'review.json'))
    A.rw_json(A.REVIEW_FILE, [{'key': 'lib_i1', 'title': 'The Crystal Shard', 'kind': 'incomplete', 'reason': 'short', 'status': 'open'},
                              {'key': 'lib_i2', 'title': 'Other', 'kind': 'asin', 'candidate': 'B0X', 'reason': 'weak', 'status': 'open'},
                              {'key': 'lib_i3', 'title': 'Old', 'kind': 'asin', 'status': 'obsolete'}])
    A.rw_json(A.RESULTS_FILE, {'lib_i2': {'title': 'Other', 'author': 'Someone', 'asin': 'B0Y'}})
    A.redownload_flag('lib_i1', ITEM, 'chapters', 'chapters run past the end of the file')
    A.export_review()
    out = A.rw_json(A.REVIEW_EXPORT_FILE)
    assert set(out) == {'lib_i1', 'lib_i2'}
    assert out['lib_i1']['fix'] == 'download' and out['lib_i1']['narrators'] == ['Victor Bevine'] and out['lib_i1']['review'][0]['kind'] == 'incomplete'
    assert out['lib_i2']['fix'] == 'user' and out['lib_i2']['asin'] == 'B0Y' and out['lib_i2']['authors'] == ['Someone']


def test_bonus_chapters_are_no_missing_story():
    for t in ('Bonus Excerpt: Queen Demon', 'End Credits', 'Closing Credits', 'The Story Continues in Defiance', 'Bloopers', 'Excerpt from Mr. Mercedes',
              'Bonus: Conversation between the authors'):
        assert CC.RE_EXTRA.search(t), t
    for t in ('Epilogue', 'Chapter 22: The Prophecy Comes True', 'Chapter Eleven', 'Part B'):
        assert not CC.RE_EXTRA.search(t), t


if __name__ == '__main__':
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith('test_') and callable(fn): fn(); n += 1; print('ok', name)
    print(n, 'tests passed')
