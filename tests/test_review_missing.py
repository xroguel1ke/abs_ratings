"""Review entries of books deleted in ABS leave the review list (2026-11-25). Run: python tests/test_review_missing.py"""
import json, os, sys, tempfile
d = tempfile.mkdtemp(prefix='revmiss_')
os.environ.update(DATA_DIR=d, ABS_URL='http://127.0.0.1:1', API_TOKEN='x', DRY_RUN='false')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import abs_ratings as A


def test_open_entries_of_gone_books_become_obsolete():
    q = [{'key': 'L_gone', 'kind': 'chapters', 'status': 'open'}, {'key': 'L_alive', 'kind': 'chapters', 'status': 'open'},
         {'key': 'L_gone', 'kind': 'asin', 'status': 'done'}]
    A.rw_json(A.REVIEW_FILE, q)
    assert A.review_drop_missing({'L_gone'}) == 1
    q2 = A.rw_json(A.REVIEW_FILE)
    assert [x['status'] for x in q2] == ['obsolete', 'open', 'done']


def test_dry_run_changes_nothing():
    A.rw_json(A.REVIEW_FILE, [{'key': 'L_x', 'kind': 'asin', 'status': 'open'}])
    A.DRY_RUN = True
    try: assert A.review_drop_missing({'L_x'}) == 0
    finally: A.DRY_RUN = False
    assert A.rw_json(A.REVIEW_FILE)[0]['status'] == 'open'


if __name__ == '__main__':
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith('test_') and callable(fn): fn(); n += 1; print('ok', name)
    print(n, 'tests passed')
