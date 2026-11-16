"""Genre rework 2026-11-16 (user: Adventure on nearly every book, Science Fiction on fantasy, too few genres)."""
import abs_series as S

L = lambda *ls: [x.split(' > ') for x in ls]


def test_context_and_generic_leaves():
    lads = L('Science Fiction & Fantasy > Fantasy > Epic', 'Science Fiction & Fantasy > Fantasy > Action & Adventure',
             'Science Fiction & Fantasy > Fantasy > Historical', 'Science Fiction & Fantasy > Fantasy > Paranormal & Urban > Contemporary')
    assert S.rebuild_genres(['Adventure', 'Epic', 'LitRPG', 'Historical'], lads) == ['Fantasy', 'Epic Fantasy', 'Historical Fantasy', 'Contemporary Fantasy', 'LitRPG']


def test_off_genre_science_fiction_goes():
    # 'Science Fiction > Dystopian' next to three fantasy ladders (The Faithful and the Fallen): Science Fiction AND Dystopian go
    lads = L('Science Fiction & Fantasy > Fantasy > Epic', 'Science Fiction & Fantasy > Fantasy > Historical',
             'Science Fiction & Fantasy > Fantasy > Action & Adventure', 'Science Fiction & Fantasy > Science Fiction > Dystopian')
    assert S.rebuild_genres(['Science Fiction', 'Dystopian', 'Fantasy'], lads) == ['Fantasy', 'Epic Fantasy', 'Historical Fantasy']
    # 'Science Fiction > Adventure' (generic) next to 'Fantasy > Humorous' (Overlord)
    lads = L('Science Fiction & Fantasy > Fantasy > Humorous', 'Science Fiction & Fantasy > Science Fiction > Adventure')
    assert 'Science Fiction' not in S.rebuild_genres([], lads)
    # Cyberpunk as LitRPG stand-in
    lads = L('Science Fiction & Fantasy > Fantasy > Epic', 'Science Fiction & Fantasy > Science Fiction > Cyberpunk')
    assert S.rebuild_genres(['LitRPG'], lads) == ['Fantasy', 'Epic Fantasy', 'LitRPG']
    assert 'Science Fiction' in S.rebuild_genres([], lads)  # no LitRPG: 1:1, both stay
    # real science fiction stays (Expanse)
    lads = L('Science Fiction & Fantasy > Science Fiction > Space Opera', 'Science Fiction & Fantasy > Science Fiction > Hard Science Fiction')
    assert S.rebuild_genres(['Fantasy', 'Adventure'], lads) == ['Science Fiction', 'Space Opera', 'Hard Science Fiction']


def test_light_novel_rule_and_idempotent():
    lads = L('Science Fiction & Fantasy > Fantasy > Epic', 'Teen & Young Adult > Science Fiction & Fantasy > Fantasy')
    g = S.rebuild_genres(['Light Novel', 'Manga', 'Graphic Novel', 'Adventure'], lads)
    assert g == ['Fantasy', 'Epic Fantasy', 'Young Adult', 'Light Novel'] and S.rebuild_genres(g, lads) == g


def test_series_fill():
    calls = []
    ctx = {'abs_update': lambda *a, **k: calls.append(a) or True, 'dry_run': False, 'genre_max': 6, 'genre_drops': {'L_3': ['cyberpunk']}}
    rec = lambda i, g: {'id': str(i), 'lib': 'L', 'title': f'b{i}', 'genres': g, 'tags': set(), 'series': [{'name': 'Saga'}]}
    rs = [rec(1, ['Fantasy', 'LitRPG', 'Dark Fantasy', 'Cyberpunk']), rec(2, ['Fantasy', 'LitRPG', 'Dark Fantasy', 'Cyberpunk', 'Adventure']),
          rec(3, ['Fantasy']), rec(4, ['Science Fiction', 'LitRPG', 'Dark Fantasy', 'Cyberpunk'])]
    S.series_genre_fill(ctx, rs)
    # book 3: LitRPG + Dark Fantasy from the other volumes; not Cyberpunk (off-genre for it), not Adventure (generic), not a 2nd main genre
    assert rs[2]['genres'] == ['Fantasy', 'LitRPG', 'Dark Fantasy']
