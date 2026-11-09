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
    assert d.startswith('─── Ratings & Infos ───<br>Audible (120):<br>🏆 🌕🌕🌕🌕🌗 4.6 / 5 - Overall') and d.endswith('───────────────<br>Text of the book.'), d


def test_stars_german_bottom_min_count_and_hidden_source():
    d = build(RATING_ICONS='stars', RATING_LABELS='de', RATING_POSITION='bottom', RATING_MIN_COUNT=10, RATING_ORDER=['Audible', 'Goodreads'])
    assert d.startswith('Text of the book.<br><br>─── Bewertungen & Infos ───<br>Audible (120):<br>🏆 ★★★★⯪ 4.6 / 5 - Gesamt'), d
    assert 'Goodreads' not in d, 'below the minimum count'
    again = A.build_description(d, {'Audible': AUD}, A.parse_rating_block(d))  # block at the bottom is found and replaced, not doubled
    assert again.count('Bewertungen & Infos') == 1 and again.startswith('Text of the book.'), again
    d2 = build(RATING_ICONS='moons', RATING_LABELS='en', RATING_POSITION='top', RATING_MIN_COUNT=0, RATING_ORDER=['Goodreads'])
    assert 'Audible' not in d2 and 'Goodreads (5)' in d2, 'only the shown sources'


def test_yellow_stars_and_old_frame():
    # yellow stars inside the block must not end it early (old regex stopped at the first star -> block doubled next run)
    d = build(RATING_ICONS='yellow_stars', RATING_LABELS='en', RATING_POSITION='top', RATING_MIN_COUNT=0, RATING_ORDER=['Audible', 'Goodreads'])
    again = A.build_description(d, {'Audible': AUD}, A.parse_rating_block(d))
    assert again.count('Ratings & Infos') == 1 and again.endswith('<br>Text of the book.') and 'Goodreads (5)' in again, again
    old = '⭐ Ratings & Infos<br>Audible (120):<br>🏆 ⭐⭐⭐⭐✨ 4.6 / 5 - Overall<br>Goodreads (5):<br>🏆 ⭐⭐⭐⭐ 4.1 / 5 - Rating<br>⭐<br>Text of the book.'
    assert A.strip_rating_block(old) == 'Text of the book.' if hasattr(A, 'strip_rating_block') else True
    assert A.RE_RATING_BLOCK.sub('', old) == 'Text of the book.', A.RE_RATING_BLOCK.sub('', old)
    assert set(A.parse_rating_block(old)) == {'Audible', 'Goodreads'}
    new = A.build_description(old, {'Audible': AUD}, A.parse_rating_block(old))
    assert new.startswith('─── Ratings & Infos ───') and new.count('Ratings & Infos') == 1 and new.endswith('<br>Text of the book.'), new
    sw = {c: A.RATING_SYMBOL_SETS['hearts'][i] for st in A.RATING_SYMBOL_SETS.values() for i, c in enumerate(st) if c}
    sw.update({c: A.RATING_SYMBOL_SETS['hearts'][i] for i, c in enumerate(A.RATING_SYMBOL_SETS['hearts']) if c})
    blk = A.RE_RATING_BLOCK.search(old).group(0); nb = A.rewrite_block(blk, sw)
    assert nb.startswith('─── Ratings & Infos ───<br>') and '🏆 ❤️❤️❤️❤️💗 4.6' in nb and '🏆 ❤️❤️❤️❤️ 4.1' in nb and nb.rstrip('<br>').endswith('───────────────'), nb
    for k in ('hearts', 'traffic', 'green_squares', 'orange_circles'): assert k in A.RATING_SYMBOL_SETS, k


if __name__ == '__main__':
    for n, f in sorted(globals().items()):
        if n.startswith('test_'): f(); print('ok', n)
    print('rating block tests passed')
