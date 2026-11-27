"""Per-book state files are merged on write (2026-11-27: a second process dumped its whole stale copy of results.json and
dropped the import run's new books Moon Over Soho and The Lost Plot)."""
import json, os, sys, tempfile
os.environ.setdefault('DATA_DIR', '/tmp/merge_test'); os.environ.setdefault('ABS_URL', 'http://127.0.0.1:1'); os.environ.setdefault('API_TOKEN', 'x')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import abs_ratings as A


def _file(tmp, data):
    p = os.path.join(tmp, 'results.json')
    with open(p, 'w') as f: json.dump(data, f)
    return p


def test_two_writers_keep_each_others_books():
    with tempfile.TemporaryDirectory() as tmp:
        p = _file(tmp, {'old': {'s': 1}, 'both': {'s': 1}, 'gone': {'s': 1}})
        A.RESULTS_FILE = p
        mine = A.rw_json(p)                                  # this process loads the file
        with open(p) as f: other = json.load(f)              # another process loads it too ...
        other['new_book'] = {'s': 'ok'}; other['both'] = {'s': 'other'}
        with open(p, 'w') as f: json.dump(other, f)          # ... and saves its new book first
        mine['mine'] = {'s': 'ok'}; mine['old']['s'] = 2; del mine['gone']
        A.rw_json(p, mine)
        with open(p) as f: on_disk = json.load(f)
        assert on_disk['new_book'] == {'s': 'ok'}, 'the other process\'s new book stays'
        assert on_disk['both'] == {'s': 'other'}, 'a key we never touched keeps the other writer\'s value'
        assert on_disk['mine'] == {'s': 'ok'} and on_disk['old'] == {'s': 2} and 'gone' not in on_disk
        assert mine['new_book'] == {'s': 'ok'}, 'our copy learns the other book (a later save must not drop it)'
        mine['mine']['s'] = 'again'; A.rw_json(p, mine)
        with open(p) as f: on_disk = json.load(f)
        assert on_disk['new_book'] == {'s': 'ok'} and on_disk['mine'] == {'s': 'again'}


def test_other_files_are_written_whole():
    with tempfile.TemporaryDirectory() as tmp:
        p = os.path.join(tmp, 'other.json')
        with open(p, 'w') as f: json.dump({'a': 1}, f)
        d = A.rw_json(p); d.pop('a'); A.rw_json(p, d)
        with open(p) as f: assert json.load(f) == {}
