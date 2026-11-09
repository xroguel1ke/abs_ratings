"""Missing volumes (missing_series.compare): only real gaps."""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import missing_series as MS

LIB = [{'num': '1', 'title': 'The Final Empire', 'asin': 'A1'}, {'num': '3', 'title': 'The Hero of Ages', 'asin': 'OTHER-EDITION'}]
AUD = [{'asin': 'A1', 'num': '1', 'title': 'The Final Empire', 'language': 'English'},
       {'asin': 'A2', 'num': '2', 'title': 'The Well of Ascension', 'language': 'English', 'release': '2009-01-01'},
       {'asin': 'A3', 'num': '3', 'title': 'The Hero of Ages', 'language': 'English'},          # other edition in ABS: present
       {'asin': 'A2b', 'num': '2', 'title': 'The Well of Ascension (Dramatized)', 'language': 'English'},
       {'asin': 'B', 'num': '1-3', 'title': 'Mistborn Trilogy Boxed Set', 'language': 'English'},
       {'asin': 'N', 'num': '3.5', 'title': 'Secret History', 'language': 'English'},
       {'asin': 'X', 'num': '', 'title': 'Companion', 'language': 'English'},
       {'asin': 'F', 'num': '4', 'title': 'L\'Alliage de la justice', 'language': 'French'},
       {'asin': 'A7', 'num': '7', 'title': 'The Lost Metal', 'language': 'English', 'release': '2099-11-15'}]


def test_only_real_gaps():
    m = MS.compare(LIB, AUD, language='English', today='2026-11-09')
    assert [(x['num'], x['title'], x['upcoming']) for x in m] == [('2', 'The Well of Ascension', False), ('7', 'The Lost Metal', True)], m


def test_novellas_optional_and_title_counts():
    m = MS.compare(LIB + [{'num': None, 'title': 'The Well of Ascension', 'asin': None}], AUD, language='English', with_novellas=True, today='2026-11-09')
    assert [x['num'] for x in m] == ['3.5', '7'], m


def test_omnibus_editions_cover_volumes():
    aud = [{'asin': f'G{i}', 'num': str(i), 'title': f'Book {i} title'} for i in range(1, 18)]
    assert [x['num'] for x in MS.compare([{'num': '1-16.5', 'title': 'Galaxy Outlaws'}], aud)] == ['17']
    waldsee = [{'num': None, 'title': 'Dämonenblut / Nachtfeuer / Perlmond: Die Chroniken von Waldsee Trilogie 1-3'}]
    aud = [{'asin': 'W1', 'num': '1', 'title': 'Dämonenblut'}, {'asin': 'W2', 'num': '2', 'title': 'Nachtfeuer'}, {'asin': 'W4', 'num': '4', 'title': 'Nauraka - Volk der Tiefe'}]
    assert [x['num'] for x in MS.compare(waldsee, aud)] == ['4']
    assert MS.compare([{'num': None, 'title': 'Sherlock Holmes: The Definitive Collection'}], [{'asin': 'S', 'num': '1', 'title': 'A Study in Scarlet'}]) == []
    assert MS.compare([{'num': '3', 'title': 'To Green Angel Tower'}], [{'asin': 'T', 'num': '4', 'title': 'To Green Angel Tower, Part 2'}]) == []
    assert [x['num'] for x in MS.compare([{'num': '12', 'title': 'Overlord, Vol. 12: The Paladin of the Sacred Kingdom Part I'}],
                                         [{'asin': 'O', 'num': '13', 'title': 'Overlord, Vol. 13'}])] == ['13']  # vol. 12 does not hold vol. 13


def test_dramatized_series_keeps_its_editions():
    m = MS.compare([], [{'asin': 'D', 'num': '1', 'title': 'Dune (Dramatized Adaptation)'}], lib_variant=frozenset({'dramatized'}))
    assert [x['num'] for x in m] == ['1'], m


if __name__ == '__main__':
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn): fn(); n += 1; print('ok', name)
    print(n, 'tests passed')
