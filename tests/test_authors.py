"""Author merge tests with synthetic data (no library content). Run: python tests/test_authors.py   (or pytest)"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import abs_series as S


def rec(i, title, authors, narrators=(), series=()):
    return {'id': i, 'lib': 'l', 'title': title, 'authors': list(authors), 'narrators': list(narrators),
            'series': [{'name': n, 'sequence': q} for n, q in series], 'tags': set()}


def fixes(records, links=None):
    return S.find_author_fixes(records, {}, links)[0]


def test_initials_and_accents_merge():
    r = [rec(1, 'A', ['R. A. Salvatore']), rec(2, 'B', ['R.A. Salvatore']), rec(3, 'C', ['R.A. Salvatore']),
         rec(4, 'D', ['Domagoj Kurmaic']), rec(5, 'E', ['Domagoj Kurmaić'])]
    f = fixes(r)
    assert f == {'R. A. Salvatore': 'R.A. Salvatore', 'Domagoj Kurmaic': 'Domagoj Kurmaić'}, f


def test_different_authors_in_one_series_are_never_merged():
    r = [rec(1, 'Shared World 1', ['Anna Autor'], ['N1'], [('Shared World', '1')]),
         rec(2, 'Shared World 2', ['Boris Schreiber'], ['N1'], [('Shared World', '2')]),
         rec(3, 'Shared World 3', ['Chen Wei'], ['N1'], [('Shared World', '3')])]
    assert fixes(r) == {}


def test_other_script_needs_identical_title_and_shared_narrator():
    same_work = [rec(1, 'Vol. 18', ['理不尽な孫の手'], ['Cliff Kirk'], [('Mushoku', '18')]), rec(2, 'Vol. 18', ['Rifujin na Magonote'], ['Cliff Kirk'], [('Mushoku', '9')])]
    assert fixes(same_work) == {'理不尽な孫の手': 'Rifujin na Magonote'}
    only_same_series = [rec(1, 'Vol. 18', ['理不尽な孫の手'], ['Cliff Kirk'], [('Mushoku', '18')]), rec(2, 'Vol. 9', ['Rifujin na Magonote'], ['Cliff Kirk'], [('Mushoku', '9')])]
    assert fixes(only_same_series) == {}, 'same series alone is no proof'
    other_narrator = [rec(1, 'Vol. 18', ['理不尽な孫の手'], ['A'], []), rec(2, 'Vol. 18', ['Rifujin na Magonote'], ['B'], [])]
    assert fixes(other_narrator) == {}


def test_two_latin_candidates_for_other_script_is_not_merged():
    r = [rec(1, 'T', ['山田太郎'], ['N']), rec(2, 'T', ['Taro Yamada'], ['N']), rec(3, 'T', ['Jiro Sato'], ['N'])]
    assert fixes(r) == {}


def test_audible_author_record_is_proof():
    r = [rec(1, 'City', ['S.A. Chakraborty']), rec(3, 'Copper', ['S.A. Chakraborty']), rec(2, 'Fate', ['Shannon Chakraborty'])]  # majority spelling wins
    assert fixes(r) == {}, 'without the proof the pair only goes to the review list'
    assert fixes(r, {'S.A. Chakraborty': {'X1'}, 'Shannon Chakraborty': {'X1'}}) == {'Shannon Chakraborty': 'S.A. Chakraborty'}
    assert fixes(r, {'S.A. Chakraborty': {'X1'}, 'Shannon Chakraborty': {'X2'}}) == {}, 'different Audible records'


if __name__ == '__main__':
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn): fn(); n += 1; print('ok', name)
    print(n, 'tests passed')
