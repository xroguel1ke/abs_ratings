"""Review list leftovers 2026-11-24 (user: 'checke ob man die nicht auch ohne manuelles eingreifen sauber lösen könnte').
Run: python -m pytest tests/test_review_leftovers.py"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/leftovers_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import abs_ratings as A

SRC = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'abs_ratings.py'), encoding='utf-8').read()


def test_other_book_of_the_same_author_is_a_wrong_asin():
    # 'Phantom Deadfall' (Ruins of the Earth 3) carried the ASIN of 'Black Labyrinth' (Ruins of the Galaxy 5): same author and narrator, 25 min off
    v, why = A.asin_plausibility('Phantom Deadfall', ['J.N. Chaney', 'Christopher Hopper'], 'Black Labyrinth', None, ['J.N. Chaney', 'Jason Anspach'],
                                 ['R.C. Bray'], ['R.C. Bray'], 37752, 37752 + 1500)
    assert v == 'wrong', why
    # the same recording keeps its ASIN, whatever the title says (ABS title off, runtime +-5 min)
    assert A.asin_plausibility('Phantom Deadfall', ['J.N. Chaney'], 'Black Labyrinth', None, ['J.N. Chaney'], ['R.C. Bray'], ['R.C. Bray'], 37752, 37800)[0] != 'wrong'
    # a similar title stays ok (another edition of the same book)
    assert A.asin_plausibility('The Dark Portal', ['E.G. Foley'], 'The Dark Portal', 'The Gryphon Chronicles, Book 3', ['E.G. Foley'], [], [], 34490, 30000)[0] == 'ok'


def test_release_id_in_brackets_is_file_junk():
    assert A.strip_title_junk('Phantom Deadfall [66vbrM4]', keep_format=True)[0] == 'Phantom Deadfall'
    assert A.strip_title_junk('Mother of Learning [Book 1]', keep_format=True)[0].startswith('Mother of Learning [Book 1]')  # words, no id
    assert A.strip_title_junk('Red Rising [Unabridged]', keep_format=True)[0] == 'Red Rising [Unabridged]'  # format label kept as before


def test_full_cast_is_the_same_recording_when_runtime_matches():
    aud = {'runtime_sec': 42300, 'narrators_raw': ['Shaun Taylor-Corbett', 'Nat Cassidy', "Kelley Rae O'Donnell", 'Stephanie Willing']}
    assert A.same_recording(42293, ['Full Cast'], aud)
    assert not A.same_recording(42293 + 600, ['Full Cast'], aud)  # runtime still decides
    assert not A.same_recording(42293, ['Full Cast'], dict(aud, narrators_raw=['Nat Cassidy']))  # one narrator is no cast
    assert not A.same_recording(42293, ['Someone Else'], aud)


def test_replaced_wrong_asin_keeps_the_new_products_data():
    # 'Secrets of the Deep': the right ASIN was found, then 'found != asin' (asin already = found) threw its data away -> strike + review entry
    assert 'if plaus == "wrong" and not asin_from_search:' in SRC
    assert 'if plaus == "wrong" and not (found and found != asin)' not in SRC


def test_hardcover_identifier_to_another_book_searches_by_title():
    assert "hc_data.get('source') in ('ASIN', 'ISBN') and not ov.get('hardcover')" in SRC
    assert "if not other_: add_review(key, title, 'hardcover'" in SRC  # another book of the same author: no question
    assert "({'description'} if 'lock_description' not in tags else set())" in SRC
    assert "if not summary and base_desc == cur_desc and (other_d := description_of_other_book(" in SRC
