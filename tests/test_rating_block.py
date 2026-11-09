"""Rating block format settings (icons, labels, position, minimum count, shown sources). Run: python tests/test_rating_block.py"""
import os, sys, importlib
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/rb_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import abs_ratings as A
AUD = {'count': 120, 'overall': 4.6, 'performance': 4.8, 'story': 4.5}
GR = {'count': 5, 'val': '4.1'}


def build(**env):
    for k, v in env.items(): setattr(A, k, v)
    return A.build_description('Text of the book.', {'Audible': AUD, 'Goodreads': GR}, {})


def test_default_unchanged():
    d = build(RATING_ICONS='moons', RATING_LABELS='en', RATING_POSITION='top', RATING_MIN_COUNT=0, RATING_ORDER=['Audible', 'Goodreads', 'Hardcover'])
    assert d.startswith('⭐ Ratings & Infos<br>Audible (120):<br>🏆 🌕🌕🌕🌕🌗 4.6 / 5 - Overall') and d.endswith('⭐<br>Text of the book.'), d


def test_stars_german_bottom_min_count_and_hidden_source():
    d = build(RATING_ICONS='stars', RATING_LABELS='de', RATING_POSITION='bottom', RATING_MIN_COUNT=10, RATING_ORDER=['Audible', 'Goodreads'])
    assert d.startswith('Text of the book.<br><br>⭐ Bewertungen & Infos<br>Audible (120):<br>🏆 ★★★★⯪ 4.6 / 5 - Gesamt'), d
    assert 'Goodreads' not in d, 'below the minimum count'
    again = A.build_description(d, {'Audible': AUD}, A.parse_rating_block(d))  # block at the bottom is found and replaced, not doubled
    assert again.count('Bewertungen & Infos') == 1 and again.startswith('Text of the book.'), again
    d2 = build(RATING_ICONS='moons', RATING_LABELS='en', RATING_POSITION='top', RATING_MIN_COUNT=0, RATING_ORDER=['Goodreads'])
    assert 'Audible' not in d2 and 'Goodreads (5)' in d2, 'only the shown sources'


if __name__ == '__main__':
    for n, f in sorted(globals().items()):
        if n.startswith('test_'): f(); print('ok', n)
    print('rating block tests passed')
