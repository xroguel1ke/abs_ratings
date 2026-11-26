"""Guards against outside ABS matches (2026-11-25 import check): ReadMeABook's 'RecentlyAdded' job let ABS fuzzy-match new books
and the ASIN of ANOTHER volume / edition arrived before abs_ratings saw the book. Run: python tests/test_import_guard.py (or pytest)"""
import os, sys
os.environ.setdefault('DATA_DIR', '/tmp/import_guard_test'); os.environ.setdefault('ABS_URL', 'http://127.0.0.1:1'); os.environ.setdefault('API_TOKEN', 'x')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import abs_ratings as A
import abs_series as S


def test_same_series_title_with_other_number_is_another_volume():
    # Death: Genesis 7 got Genesis 3's ASIN (same narrator, runtime 1 min apart)
    assert A.title_number_conflict('Death: Genesis 7: An Isekai LitRPG (Unabridged)', [{'name': 'Death - Genesis', 'sequence': '7'}],
                                   'Death: Genesis 3: An Isekai LitRPG')
    # Tanya the Evil Vol. 14 got Vol. 1's ASIN
    assert A.title_number_conflict('The Saga of Tanya the Evil, Vol. 14', [{'name': 'The Saga of Tanya the Evil', 'sequence': '14'}],
                                   'The Saga of Tanya the Evil, Vol. 1')
    assert A.title_number_conflict('Dungeon Life 02', [{'name': 'Dungeon Life', 'sequence': '2'}], 'Dungeon Life 4')


def test_number_rule_must_not_fire():
    # numbering difference only in the subtitle / series field (Murderbot 'Book 8' vs Audible 'Book 7'): no number in the titles
    assert not A.title_number_conflict('System Collapse', [{'name': 'The Murderbot Diaries', 'sequence': '8'}], 'System Collapse')
    # same number
    assert not A.title_number_conflict('Death: Genesis 3', [{'name': 'Death: Genesis', 'sequence': '3'}], 'Death: Genesis 3: An Isekai LitRPG')
    # ABS's own series number does not back the ABS title number (the ABS title itself may be the odd one)
    assert not A.title_number_conflict('Mushoku Tensei, Vol. 18', [{'name': 'Mushoku Tensei', 'sequence': '9'}], 'Mushoku Tensei, Vol. 9')
    # different titles with numbers: another rule's job, not this one
    assert not A.title_number_conflict('Catch 22', [{'name': 'X', 'sequence': '22'}], 'Slaughterhouse 5')
    # years are no volume numbers
    assert not A.title_number_conflict('The Best of 2023', [{'name': 'Best of', 'sequence': '2'}], 'The Best of 2024')
    # no series number in ABS: no proof
    assert not A.title_number_conflict('Death: Genesis 7', [], 'Death: Genesis 3')


def test_title_that_is_only_the_series_name():
    # 2026-11-26: 'Ends of Magic' #4 (file tags) is 'Anarchist' on Audible
    assert A.title_is_series_name('Ends of Magic', [{'name': 'Ends of Magic', 'sequence': '4'}], 'Anarchist', [{'name': 'Ends of Magic', 'part': '4'}])
    # another number on Audible: no
    assert not A.title_is_series_name('Ends of Magic', [{'name': 'Ends of Magic', 'sequence': '4'}], 'Aspirant', [{'name': 'Ends of Magic', 'part': '5'}])
    # book 1 whose Audible title only adds marketing: the ABS title stays
    assert not A.title_is_series_name('Beware of Chicken', [{'name': 'Beware of Chicken', 'sequence': '1'}], 'Beware of Chicken: A Xianxia Cultivation Novel',
                                      [{'name': 'Beware of Chicken', 'part': '1'}])
    # a real title that is not the series name: not this rule
    assert not A.title_is_series_name('Anarchist', [{'name': 'Ends of Magic', 'sequence': '4'}], 'Anarchist: Book 4', [{'name': 'Ends of Magic', 'part': '4'}])


def test_file_parts_are_poor_chapters():
    assert A.chapters_poor([{'start': 0, 'end': 61566, 'title': 'X_pt.1'}, {'start': 61566, 'end': 148023, 'title': 'X_pt.2'}], 148023) == 'only a few marks hours apart (file parts)'
    # real chapters of a long book are not
    assert A.chapters_poor([{'start': i * 1800, 'end': (i + 1) * 1800, 'title': f'The {chr(65 + i % 26)}{i}'} for i in range(80)], 144000) is None


def test_bracket_number_prefix_is_messy():
    m = A.parse_messy_title('[03] - Necropolis')
    assert m and m['title'] == 'Necropolis' and m['sequence'] == '3', m
    assert A.parse_messy_title('Necropolis') is None


def test_credit_prefix_author_is_fixed():
    recs = [{'id': str(i), 'title': t, 'authors': ['By: Anthony J. Melchiorri'], 'narrators': [], 'tags': set()} for i, t in enumerate(['Eternal Frontier', 'Rebel World'])]
    recs.append({'id': '9', 'title': 'Other', 'authors': ['Brandon Sanderson'], 'narrators': [], 'tags': set()})
    fixes, _, _ = S.find_author_fixes(recs)
    assert fixes == {'By: Anthony J. Melchiorri': 'Anthony J. Melchiorri'}, fixes
    # a real name is never touched; 'By' without a colon is a possible name part
    recs2 = [{'id': '1', 'title': 'A', 'authors': ['Byron Bystander'], 'narrators': [], 'tags': set()}]
    assert S.find_author_fixes(recs2)[0] == {}


def test_credit_prefix_respects_keep_decision():
    recs = [{'id': '1', 'title': 'A', 'authors': ['By: Anthony J. Melchiorri'], 'narrators': [], 'tags': set()}]
    mid = S.merge_id('author', '', 'By: Anthony J. Melchiorri', 'Anthony J. Melchiorri')
    assert S.find_author_fixes(recs, {mid: {'action': 'keep'}})[0] == {}


if __name__ == '__main__':
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith('test_') and callable(fn): fn(); n += 1; print('ok', name)
    print(n, 'tests passed')
