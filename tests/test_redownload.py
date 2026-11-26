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


def test_complete_file_drops_incomplete_review_entry():
    tmp = tempfile.mkdtemp(); _fresh(tmp)
    A.REVIEW_FILE = os.path.join(tmp, 'review_queue.json')
    A.rw_json(A.REVIEW_FILE, [{'key': 'lib_i1', 'kind': 'incomplete', 'status': 'open', 'run': 'old'},
                              {'key': 'lib_i1', 'kind': 'asin', 'status': 'open', 'run': 'old'},
                              {'key': 'lib_i2', 'kind': 'incomplete', 'status': 'open', 'run': 'old'}])
    aud = ([['Ch 1', 3000.0, 'Chapter 1'], ['Ch 2', 3000.0, 'Chapter 2']], None)
    CC.complete('lib_i2', 5400.0, aud)  # 10 min short: stays
    CC.complete('lib_i1', 5800.0, aud)  # replaced file, 200 s short: the entry is obsolete, other kinds stay
    q = {(x['key'], x['kind']): x['status'] for x in A.rw_json(A.REVIEW_FILE)}
    assert q == {('lib_i1', 'incomplete'): 'obsolete', ('lib_i1', 'asin'): 'open', ('lib_i2', 'incomplete'): 'open'}, q


def test_fitting_chapters_drop_only_the_replaced_file_entry():
    tmp = tempfile.mkdtemp(); _fresh(tmp)
    A.REVIEW_FILE = os.path.join(tmp, 'review_queue.json')
    A.rw_json(A.REVIEW_FILE, [{'key': 'lib_i1', 'kind': 'chapters', 'candidate': 'replaced_file', 'status': 'open'},
                              {'key': 'lib_i1', 'kind': 'chapters', 'candidate': 'no chapters', 'status': 'open'},
                              {'key': 'lib_i2', 'kind': 'chapters', 'candidate': 'replaced_file', 'status': 'open'}])
    CC.settled('lib_i1')  # 'Forever Peace': the chain put the old file back, its chapters fit again
    q = [(x['key'], x['candidate'], x['status']) for x in A.rw_json(A.REVIEW_FILE)]
    assert q == [('lib_i1', 'replaced_file', 'obsolete'), ('lib_i1', 'no chapters', 'open'), ('lib_i2', 'replaced_file', 'open')], q


if __name__ == '__main__':
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith('test_') and callable(fn): fn(); n += 1; print('ok', name)
    print(n, 'tests passed')
