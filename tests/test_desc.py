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


def test_foreign_language_description_2026_11_16():
    fr = "Il y a bien longtemps que les humains sont arrivés sur les Terres Bannies, qu'ils ont disputées victorieusement aux clans de géants qui les peuplaient. Mais une guerre se prépare dans les ombres et elle est menée par des forces que les hommes ont oubliées."
    es = "Restábamos ya solo cinco candidatos. El ídolo se había movido. Había posado el pie que había mantenido alzado, aplastando una cabeza, y ahora levantaba su otra extremidad inferior con la espada en la mano."
    en = "Croaker has fallen and, following the Company's disastrous defeat at Dejagore, Lady is one of the few survivors, determined to avenge the Company and herself against the Shadowmasters."
    assert A.text_language(fr) == 'French' and A.text_language(es) == 'Spanish' and A.text_language(en) == 'English'
    assert A.desc_language_wrong(fr, en, 'English') and A.desc_language_wrong(es, en, 'English')
    assert not A.desc_language_wrong(en, en, 'English') and not A.desc_language_wrong(fr, '', 'English')
    assert A.text_language('Dragonsteel Prime, the 2001 version') is None  # too short: unclear


def test_reviews_only_description_2026_11_16():
    rev = 'Review "The way this story evolved lead to me being more and more excited to read the next chapter. I blitzed through this book." - 5 stars. Richard White.'
    assert A.description_is_reviews(rev)
    assert not A.description_is_reviews('As the dust settles on the battlefield of Carthage, Alexander finds himself struggling. "A great read" - 5 stars.')


def test_description_junk_2026_11_16():
    assert A.description_junk('Book 1 of [Fellowship of the Ring](https://openlibrary.org/works/OL14933414W/The_Fellowship_of_the_Ring)')
    assert A.description_junk('MacLeod Andrews (Narrator)')
    assert A.description_junk('Review "Loved every page of it, could not put it down at all." - 5 stars. Jane Doe')
    assert A.description_junk('https://www.example.com/book')
    assert not A.description_junk('Sanderson Curiosities: Dragonsteel Prime, the 2001 Alternate Version')
    assert not A.description_junk('Il y a bien longtemps que les humains sont arrivés sur les Terres Bannies.')  # wrong language is no nonsense
    assert not A.description_junk('More than a quarter of a century after The Sword of Shannara ... Description taken from https://www.amazon.com/x and a long text ' * 2)


def test_hardcover_description_fallback_2026_11_16():
    txt = ('A black sun is rising. Young Corban watches enviously as boys become warriors under King Brenin’s rule, learning the art of '
           'sword and shield. He will learn the true meaning of courage, honour and loyalty while the world around him is at war.\n\nThe second paragraph.')
    hc = {'source': 'search', 'title': 'Malice', 'description': txt}
    out = A.hc_description_ok(hc, 'Malice', 'English')
    assert out.startswith('<p>A black sun') and '</p><p>The second paragraph.</p>' in out
    assert A.hc_description_ok(hc, 'Malice', 'German') == ''                       # language must be the book's
    assert A.hc_description_ok(dict(hc, title='Malice Aforethought'), 'Malice', 'English') == ''  # not a sure match
    assert A.hc_description_ok(dict(hc, description='Too short.'), 'Malice', 'English') == ''
    assert A.hc_sure_match({'source': 'ISBN', 'title': 'Other'}, 'Malice')


def test_hardcover_fills_genres_and_theme_tags_2026_11_16():
    votes = [('Fantasy', 9), ('Epic Fantasy', 3), ('Adventure', 2), ('Grimdark', 1), ('Sci-fi', 2), ('["Fantasy"', 2), ('Science Fiction & Fantasy', 3),
             ('Action & Adventure', 2), ('Dragons', 3), ('Epic', 2)]
    # cleaned like Audible's: 'Sci-fi' -> Science Fiction, umbrella 'Science Fiction & Fantasy' dropped, 'Action & Adventure' = Adventure
    # (no duplicate), 'Dragons' is a theme (tag, no genre), 'Epic' not next to Epic Fantasy, junk and single votes dropped
    assert A.hc_fill_genres(['Fantasy'], votes, 6) == ['Epic Fantasy', 'Adventure']  # no 2nd main genre (Sci-fi)
    assert A.hc_fill_genres(['Adventure'], votes, 6) == ['Fantasy', 'Epic Fantasy']  # only the top-voted main genre
    assert A.hc_fill_genres([], [('Graphic Novel', 5), ('Fantasy', 3)], 6) == ['Fantasy']
    assert A.hc_fill_genres(['fantasy', 'LitRPG', 'A', 'B', 'C'], votes, 6) == ['Epic Fantasy']  # only up to 6, 'fantasy' = Fantasy
    assert A.hc_fill_genres(['A', 'B', 'C', 'D', 'E', 'F'], votes, 6) == []
    assert A.hc_fill_theme_tags(['Audible 4.5+', 'lock_title'], votes) == ['Dragons']   # rating / lock tags do not count
    assert A.hc_fill_theme_tags(['dragons'], votes) == []                                # no duplicate
    assert A.hc_fill_theme_tags(['a', 'b', 'c', 'd', 'e', 'f'], votes) == []             # 6 real tags: full


def test_hardcover_genres_foreign_names_2026_11_16():
    assert A.hc_fill_genres(['Fantasy'], [('Action & Abenteuer', 4), ('LitRPG', 3)], 6, 'English') == ['LitRPG']


def test_hardcover_genres_only_known_names_2026_11_16():
    from abs_series import genre_key as gk
    assert A.hc_fill_genres(['Fantasy'], [('Imaginary wars and battles', 5), ('LitRPG', 3)], 6, 'English', {gk('LitRPG'), gk('Fantasy')}) == ['LitRPG']


def test_triple_line_breaks_collapsed_2026_11_16():
    out = A.build_description('Text one.<br /><br /><br />Text two.<br><br>Three.', {}, {})
    assert 'Text one.<br><br>Text two.<br><br>Three.' in out
