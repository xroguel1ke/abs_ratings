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




def _members(m):
    A._series_members = lambda host, sa: m


def test_abs_number_hunters_code_is_never_disproved():  # book 1 and 2 share the title (regress reg_syn_21)
    _members([('B1', '1', "The Hunter's Code"), ('B2', '2', "The Hunter's Code")])
    abs_s = [{'name': "The Hunter's Code", 'sequence': '2'}]
    cand_s = [{'name': "The Hunter's Code", 'part': '1', 'asin': 'S1'}]
    assert A.abs_number_disproved(abs_s, 'B1', "The Hunter's Code", cand_s, 'h') is None


def test_abs_number_from_another_numbering_is_disproved():  # Discworld #11 for The Light Fantastic (Audible #2, #11 = Reaper Man)
    _members([('C1', '1', 'The Colour of Magic'), ('L2', '2', 'The Light Fantastic'), ('L2old', '2', 'The Light Fantastic'), ('R11', '11', 'Reaper Man')])
    abs_s = [{'name': 'Discworld', 'sequence': '11'}]
    cand_s = [{'name': 'Discworld', 'part': '2', 'asin': 'S'}]
    assert A.abs_number_disproved(abs_s, 'L2', 'The Light Fantastic', cand_s, 'h') == {'discworld': '2'}


def test_abs_number_not_in_series_is_disproved():  # Heritage of Shannara #16 (chronological) vs Audible #3
    _members([('D2', '2', 'The Druid of Shannara'), ('E3', '3', 'The Elf Queen of Shannara')])
    assert A.abs_number_disproved([{'name': 'Heritage of Shannara', 'sequence': '16'}], 'E3', 'The Elf Queen of Shannara',
                                  [{'name': 'The Heritage of Shannara', 'part': '3', 'asin': 'S'}], 'h')


def test_no_series_list_means_no_proof():
    _members(None)
    assert A.abs_number_disproved([{'name': 'Discworld', 'sequence': '11'}], 'L2', 'The Light Fantastic',
                                  [{'name': 'Discworld', 'part': '2', 'asin': 'S'}], 'h') is None


if __name__ == '__main__':
    for n, f in list(globals().items()):
        if n.startswith('test_'): f(); print('ok', n)
