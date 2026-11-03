"""ASIN plausibility / volume rules with synthetic data. Run: python tests/test_asin.py (or pytest)"""
import os, sys
os.environ.setdefault('DATA_DIR', '/tmp/asin_test'); os.environ.setdefault('ABS_URL', 'http://127.0.0.1:1'); os.environ.setdefault('API_TOKEN', 'x')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import abs_ratings as A


def test_wrong_asin_is_detected():
    v, _ = A.asin_plausibility('Paranormal Payback', ['Jim Butcher'], 'Eyes of the Void', 'The Final Architecture, Book 2', ['Adrian Tchaikovsky'],
                               ['Sophie Aldred'], ['Sophie Aldred'], 38640, 74640)
    assert v == 'wrong', v


def test_same_recording_with_other_title_is_not_wrong():
    v, _ = A.asin_plausibility('Realms of War', ['R.A. Salvatore'], 'Insurrection', 'War of the Spider Queen, Book 2', ['Thomas M. Reid'],
                               ['Rosalyn Landor'], ['Rosalyn Landor'], 43560, 43560)
    assert v == 'asin_ok_metadata_differs', v


def test_normal_book_is_ok_and_accent_author_matches():
    assert A.asin_plausibility('Mother of Learning', ['Domagoj Kurmaic'], 'Mother of Learning: Arc 1', None, ['Domagoj Kurmaić'])[0] == 'ok'
    assert A.asin_plausibility('Dragon Heart', ['Ben Scott'], 'Dragon Heart: Book 12', None, ['Ben Scott'])[0] == 'ok'


def test_title_differs_but_author_matches_is_not_wrong():
    assert A.asin_plausibility('Die zwei Türme', ['J.R.R. Tolkien'], 'Der Herr der Ringe: Die zwei Türme', None, ['J. R. R. Tolkien'])[0] == 'ok'


def test_volume_padding_and_subtitle_numbers():
    assert not A.volume_conflict('Nowhere Stars, Vol. 02', 'Nowhere Stars 2', None)
    assert not A.volume_conflict("Legend: Publisher's Pack 4", "Legend: Publisher's Pack 4", 'Books 1-3')
    assert A.volume_conflict('Mushoku Tensei, Vol. 18', 'Mushoku Tensei, Vol. 9', None)


def test_same_recording_helper():
    aud = {'runtime_sec': 36000, 'narrators_raw': ['A Narrator']}
    assert A.same_recording(36060, ['A Narrator'], aud)
    assert not A.same_recording(40000, ['A Narrator'], aud)
    assert not A.same_recording(36000, ['Someone Else'], aud)


def test_initial_author_and_marketing_suffix():
    assert A.match_author(['R. Rowe'], 'Rob Rowe') and A.match_author(['Rob Rowe'], 'R. Rowe')
    assert not A.match_author(['R. Rowe'], 'Tim Rowe') and not A.match_author(['J. Smith'], 'Jane Doe')
    assert A.search_match_score('Robot Swarms: A Gripping LitRPG Adventure', 'Robot Swarms', None) >= 0.85
    assert A.search_match_score('Dune: Messiah', 'Dune', None) < 0.85, 'a real subtitle must never equal the short title'
    assert A.search_match_score('Dune: Messiah', 'Dune: Messiah', None) == 1.0


def test_certainty_score_parts():
    aud = {'title_raw': 'Dragon Heart', 'subtitle_raw': None, 'authors_raw': ['Ben Scott'], 'runtime_sec': 36000, 'narrators_raw': ['A Narrator'],
           'meta_raw': {'publisher': {'name': 'Pub House'}, 'releaseDate': '2021-05-01', 'series': [{'name': 'Dragon Series', 'part': '3'}]}}
    s, parts = A.certainty_score('Dragon Heart 3', ['Ben Scott'], ['A Narrator'], 36060, 'Pub House', 2021, [{'name': 'Dragon Series', 'sequence': '3'}], aud, {'source': 'ISBN'})
    assert s == 100 and parts['runtime'] == 20 and parts['number'] == 10, (s, parts)
    s2, _ = A.certainty_score('Dragon Heart 3', ['Ben Scott'], ['Other'], 40000, None, None, [], dict(aud, related_edition=True), None, True, True)
    assert s2 < s - 30, s2
    assert A.certainty_score('X', ['A'], [], 1, None, None, [], None)[0] == 0


def test_messy_number_prefix_single_word():
    assert (A.parse_messy_title('03 Blutmale') or {}).get('title') == 'Blutmale'
    assert A.parse_messy_title('007 Skyfall') is None and A.parse_messy_title('1984') is None and A.parse_messy_title('Dune') is None


def test_isbn_checks():
    assert A.isbn_bad_reason('B0D2LJ6SJ2') == 'value is an ASIN, not an ISBN'
    assert A.isbn_bad_reason('9780553580334') is None            # real ISBN-13 (House Corrino)
    assert A.isbn_bad_reason('978-0-553-58033-4') is None
    assert A.isbn_bad_reason('9780553580335') == 'check digit is wrong'
    assert A.isbn_bad_reason('0553110845') is None               # real ISBN-10
    assert A.isbn_bad_reason('') is None and A.isbn_bad_reason(None) is None
    assert A.isbn_bad_reason('12345') == 'not a valid ISBN format'


if __name__ == '__main__':
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn): fn(); n += 1; print('ok', name)
    print(n, 'tests passed')
