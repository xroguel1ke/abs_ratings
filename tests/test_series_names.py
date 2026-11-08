"""Series names the script replaces (user rules 2026-11-08). Run: python tests/test_series_names.py (or pytest)"""
import os, sys
os.environ.setdefault('DATA_DIR', '/tmp/series_names_test'); os.environ.setdefault('ABS_URL', 'http://127.0.0.1:1'); os.environ.setdefault('API_TOKEN', 'x')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import abs_ratings as A


def test_non_latin_names():
    for n in ('鍛冶屋ではじめる異世界スローライフ [Kajiya de Hajimeru Isekai Slow Life] (Novel)', '나 혼자만 레벨업', 'МЕТРО', '四畳半シリーズ'):
        assert A.non_latin_name(n), n
    for n in ('The Witcher', 'Die Chroniken von Waldsee', 'Osten Ard Saga', 'Mushoku Tensei', 'Overlord (Light Novel)', 'Élan ♀ Light Novel'):
        assert not A.non_latin_name(n), n


def test_folder_series_name():
    it = {'relPath': 'Robin Hobb/Elolaivat/1 - Ship of Magic - Lucy Tregear'}
    assert A.folder_series_name(it, 'Elolaivat') and A.folder_series_name(it, 'elolaivat ')
    assert not A.folder_series_name(it, 'Robin Hobb')  # the author folder never counts
    assert not A.folder_series_name(it, 'The Liveship Traders')
    assert not A.folder_series_name({'relPath': 'Author/Book'}, 'Book')  # the book folder itself is not a series folder


def test_series_in_library_exact():
    A.SERIES_INDEX.clear()
    A.SERIES_INDEX['l'] = {'|stormlight archive': {'name': 'The Stormlight Archive', 'names': ['The Stormlight Archive'], 'authors': ['brandon sanderson']}}
    key = next(iter(A.SERIES_INDEX['l']))
    from abs_series import series_key, series_variant
    A.SERIES_INDEX['l'] = {f"{','.join(sorted(series_variant('The Stormlight Archive')))}|{series_key('The Stormlight Archive')}": A.SERIES_INDEX['l'][key]}
    assert A.series_in_library('l', 'The Stormlight Archive', ['Brandon Sanderson'], exact=True)
    assert not A.series_in_library('l', 'Stormlight Archive', ['Brandon Sanderson'], exact=True)  # another spelling is not added
    assert not A.series_in_library('l', 'The Stormlight Archive', ['Someone Else'])


if __name__ == '__main__':
    for n, f in list(globals().items()):
        if n.startswith('test_'): f(); print('ok', n)
