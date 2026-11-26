"""Narrator spelling of the same recording (2026-11-26 'Rebel': 'Stephen Kaplan' in ABS, 'Steven Kaplan' on Audible and in the folder).
Run: python tests/test_narrator_spelling.py (or pytest)"""
import os, sys, tempfile
os.environ.setdefault('DATA_DIR', tempfile.mkdtemp(prefix='narr_')); os.environ.setdefault('ABS_URL', 'http://127.0.0.1:1'); os.environ.setdefault('API_TOKEN', 'x')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import abs_ratings as A


def test_one_word_spelled_differently():
    for a, b in (('Stephen Kaplan', 'Steven Kaplan'), ('Cliff Kurt', 'Cliff Kirk'), ('Ramón de Ocampo', 'Ramon de Ocampa'), ('Jon Lindstrom', 'John Lindstrom')):
        assert A.narr_variant(a, b), (a, b)
    for a, b in (('Stephen Kaplan', 'Stephen Fry'), ('Kate Reading', 'Kate Rudd'), ('Ray Porter', 'Ray Porter'), ('Cher', 'Chet'),
                 ('Michael Kramer', 'Kate Reading'), ('Al Smith', 'Ed Smith'), ('Steven Pacey', 'Stephen Pacey Jr.')):
        assert not A.narr_variant(a, b), (a, b)


def test_respell_takes_audibles_list_only_when_every_name_fits():
    assert A.narr_respell(['Nile Bullock', 'Stephen Kaplan'], ['Nile Bullock', 'Steven Kaplan']) == ['Nile Bullock', 'Steven Kaplan']
    assert A.narr_respell(['Stephen Kaplan', 'Nile Bullock'], ['Nile Bullock', 'Steven Kaplan']) == ['Nile Bullock', 'Steven Kaplan']
    assert A.narr_respell(['Nile Bullock', 'Steven Kaplan'], ['Nile Bullock', 'Steven Kaplan']) is None  # nothing to change
    assert A.narr_respell(['Nile Bullock', 'Mariel Stern'], ['Nile Bullock', 'Steven Kaplan']) is None   # another person
    assert A.narr_respell(['Stephen Kaplan'], ['Nile Bullock', 'Steven Kaplan']) is None                 # not the same cast
    assert A.narr_respell([], ['Steven Kaplan']) is None


if __name__ == '__main__':
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith('test_') and callable(fn): fn(); n += 1; print('ok', name)
    print(n, 'tests passed')
