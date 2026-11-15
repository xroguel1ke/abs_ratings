"""New books with messy file tags (2026-11-15 runs). Run: python -m pytest tests/test_new_books.py"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/new_books_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import abs_ratings as A

AUD = {'runtime_sec': 485 * 60, 'authors_raw': ['Samantha Shannon'], 'narrators_raw': ['Arty Froushan', 'Gemma Whelan']}


def test_verified_without_narrator_tag():
    assert A.verified_recording(485 * 60 + 30, [], ['Samantha Shannon'], AUD)          # author + runtime, no narrator in the tags
    assert not A.verified_recording(485 * 60, ['Someone Else'], ['Samantha Shannon'], AUD)  # a known other narrator: no
    assert not A.verified_recording(470 * 60, [], ['Samantha Shannon'], AUD)            # runtime off by 15 min
    assert A.verified_recording(485 * 60, [], ['Gemma Whelan'], AUD)                    # the author field holds the narrator


def test_new_book_title_rule():
    assert not A.new_book_title_ok('Beware of Chicken 2', 'Beware of Chicken 2: A Xianxia Cultivation Novel')  # marketing tail stays out
    assert A.new_book_title_ok('Konosuba', "Konosuba: God's Blessing on This Wonderful World!, Vol. 13")       # the volume number counts
    assert A.new_book_title_ok("Book 20 - Volume 07 - Part 06 - Memory's Flame - The Wandering Inn Series", "Memory's Flame")
    assert A.new_book_title_ok('Zero Enigma', "The King's Secret")


def test_folder_book_title():
    f = lambda p: A.folder_book_title({'relPath': p})
    assert f("Christopher G. Nuttall/The Zero Enigma/13 - The King's Secret - Saskia Maarleveld") == "The King's Secret"
    assert f("Natsume Akatsuki/Konosuba/13 - Konosuba God's Blessing on This Wonderful World!, Vol. 13 - Arnie Pantoja + Faye Mata") == \
        "Konosuba God's Blessing on This Wonderful World!, Vol. 13"
    assert f('Samantha Shannon/The Roots of Chaos/2 - A Day of Fallen Night - Ellie Kendrick + Hanako Footman') == 'A Day of Fallen Night'
    assert f('Martha Wells/The Murderbot Diaries/08 - Platform Decay') == 'Platform Decay'
    assert f('Shinobumaru/The Haunted Bookstore_ Gateway to a Parallel Universe/Volume 02 {Alexa Magro}') is None


def test_same_tail_twice():
    t = 'Defiance of the Fall 4: A LitRPG Adventure: A LitRPG Adventure'
    assert A.glued_subtitle(t, 'A LitRPG Adventure', 'Defiance of the Fall 4: A LitRPG Adventure', None) == 'title'
    assert A.glued_subtitle('Mistborn: The Final Empire', None, 'Mistborn', None) is None  # a normal title stays
    assert A.glued_subtitle('Dune: Dune', None, 'Dune', None) is None  # needs three parts
    # run 2026-11-15: after the first cut the title is still Audible's glued one - the volume number before the colon cuts it once more
    assert A.glued_subtitle('Defiance of the Fall 4: A LitRPG Adventure', 'A LitRPG Adventure', 'Defiance of the Fall 4: A LitRPG Adventure', None) == 'title'
    assert A.glued_subtitle('Mistborn: The Final Empire', 'The Final Empire', 'Mistborn: The Final Empire', None) is None  # no volume number
    assert A.glued_subtitle('Dune 2: Messiah', 'Other', 'Dune 2: Messiah', None) is None  # the ABS subtitle is another one


def test_box_set_detection():
    assert A.is_boxset_title('Dämonenblut / Nachtfeuer / Perlmond')
    assert not A.is_boxset_title('Fire / Ice')  # two titles: no evidence
    assert not A.is_boxset_title('AC/DC: The Story')
    assert A.is_boxset_subtitle('Die Chroniken von Waldsee Trilogie 1-3')
    assert not A.is_boxset_subtitle('Black Magician Trilogy, Book 3')


def test_temp_track_excluded(monkeypatch, tmp_path):
    monkeypatch.setattr(A, 'REVIEW_FILE', str(tmp_path / 'review.json'))
    monkeypatch.setattr(A, 'SCRIPT_DIR', str(tmp_path))
    calls = []

    class R:
        status_code = 200
        def json(self): return {}

    class S:
        def patch(self, url, json=None, **kw): calls.append(('patch', url, json)); return R()
        def post(self, url, **kw): calls.append(('post', url)); return R()
    monkeypatch.setattr(A, 'abs_session', S())
    monkeypatch.setattr(A, 'DRY_RUN', False)
    item = {'media': {'audioFiles': [
        {'ino': '1', 'index': 1, 'duration': 60830.8, 'metadata': {'filename': 'The Hyena and the Hawk Echoes of the Fall, Book 3.mp4'}},
        {'ino': '2', 'index': 2, 'duration': 58777.0, 'metadata': {'filename': 'The Hyena and the Hawk Echoes of the Fall, Book 3.tmp.mp4'}}]}}
    assert round(A.exclude_temp_tracks('i', 'l_i', 'T', item)) == 60831
    assert calls[0][0] == 'patch' and calls[0][2]['orderedFileData'] == [{'ino': '1', 'exclude': False}, {'ino': '2', 'exclude': True}]
    assert A.rw_json(A.REVIEW_FILE)[0]['kind'] == 'tmpfile'
    only_tmp = {'media': {'audioFiles': [{'ino': '2', 'index': 1, 'duration': 5, 'metadata': {'filename': 'x.tmp.mp4'}}]}}
    assert A.exclude_temp_tracks('i', 'l_i', 'T', only_tmp) is None  # never the only file
