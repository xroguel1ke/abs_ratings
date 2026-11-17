"""State files: an unreadable JSON file counts as empty (as before) but a copy is kept and a warning logged;
writes go through a temp file (no half-written file). Run: python -m pytest tests/test_state_files.py"""
import json, logging, os, sys, tempfile
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', tempfile.mkdtemp())
import abs_ratings as A  # noqa: E402


def test_missing_file_is_empty(tmp_path):
    assert A.rw_json(str(tmp_path / 'none.json')) == {}


def test_roundtrip(tmp_path):
    p = str(tmp_path / 'a.json')
    A.rw_json(p, {'x': 1})
    assert A.rw_json(p) == {'x': 1}
    assert [f for f in os.listdir(tmp_path)] == ['a.json']  # no temp file left


def test_corrupt_file_kept_and_logged(tmp_path, caplog):
    p = str(tmp_path / 'history.json')
    open(p, 'w', encoding='utf-8').write('{"half": ')
    with caplog.at_level(logging.WARNING):
        assert A.rw_json(p) == {}
        assert A.rw_json(p) == {}  # a second read keeps no second copy
    kept = [f for f in os.listdir(tmp_path) if f.startswith('history.json.corrupt-')]
    assert len(kept) == 1 and open(tmp_path / kept[0], encoding='utf-8').read() == '{"half": '
    assert any('history.json not readable' in r.getMessage() for r in caplog.records)


def test_atomic_dump(tmp_path):
    p = str(tmp_path / 'b.json')
    A._atomic_dump(p, {'k': ['v']})
    assert json.load(open(p, encoding='utf-8')) == {'k': ['v']} and os.listdir(tmp_path) == ['b.json']


def test_atomic_dump_failure_leaves_old_file(tmp_path):
    p = str(tmp_path / 'c.json')
    A._atomic_dump(p, {'old': 1})
    try: A._atomic_dump(p, {'bad': object()})  # not serializable: fails while writing
    except TypeError: pass
    assert json.load(open(p, encoding='utf-8')) == {'old': 1} and os.listdir(tmp_path) == ['c.json']
