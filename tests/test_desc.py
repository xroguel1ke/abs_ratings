"""Description replacement (user rule): only a truncated or very short ABS text, never an own long one. Run: python tests/test_desc.py"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/desc_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import abs_ratings as A

AUD = '<p>When the last dragon dies, the young smith Aren must leave his village and cross the burning plains to find the one ' \
      'weapon that can stop the coming storm. A tale of courage, loss and the price of hope, told over three long winters.</p>'


def test_truncated_beginning_is_replaced():
    assert A.description_replaceable('When the last dragon dies, the young smith Aren must leave his village and cross the burning...', AUD)


def test_very_short_text_is_replaced():
    assert A.description_replaceable('A fantasy novel.', AUD)


def test_own_long_text_is_kept():
    own = 'My own notes: I bought this after the convention, the narrator is great, chapter 12 is my favourite, ' * 3
    assert A.description_replaceable(own, AUD) is None


def test_equal_or_longer_is_kept():
    assert A.description_replaceable(AUD, AUD) is None and A.description_replaceable(AUD + ' extra', AUD) is None


def test_genre_rules():
    A.GENRE_RULES = {'movie, tv & video game tie-ins': '-', 'sci-fi': 'Science Fiction'}
    assert A.map_genre('Movie, TV & Video Game Tie-Ins', 'www.audible.com') is None
    assert A.map_genre('Sci-Fi', 'www.audible.com') == 'Science Fiction' and A.map_genre('Fantasy', 'www.audible.com') == 'Fantasy'
    A.GENRE_RULES = {}


def test_hardcover_roles_are_learned_authors_kept():
    book = {'contributions': [{'contribution': 'Author', 'author': {'name': 'Okina Baba'}}, {'contribution': 'Contributor', 'author': {'name': 'Tsukasa Kiryu'}},
                              {'contribution': 'Translator', 'author': {'name': 'Jenny McKeon'}}, {'author': {'name': 'Old Format Author'}}]}
    A.ROLE_NAMES.clear(); A.hc_learn_roles(book)
    assert A.ROLE_NAMES == {'Tsukasa Kiryu', 'Jenny McKeon'}, A.ROLE_NAMES
    assert A.hc_authors(book) == ['Okina Baba', 'Old Format Author'], 'contributors are no authors; entries without a role (recordings) count as authors'
    A.ROLE_NAMES.clear()


if __name__ == '__main__':
    for n, f in sorted(globals().items()):
        if n.startswith('test_'): f(); print('ok', n)
    print('description tests passed')
