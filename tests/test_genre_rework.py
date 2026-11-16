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


def test_series_fill_only_clean_genres():
    calls = []
    ctx = {'abs_update': lambda *a, **k: calls.append(a) or True, 'dry_run': False, 'genre_max': 6}
    rec = lambda i, g: {'id': str(i), 'lib': 'L', 'title': f'b{i}', 'genres': g, 'tags': set(), 'series': [{'name': 'Body'}]}
    rs = [rec(1, ['Fantasy', 'Humorous', 'LitRPG']), rec(2, ['Literature & Fiction']), rec(3, ['Literature & Fiction'])]
    S.series_genre_fill(ctx, rs)
    assert rs[0]['genres'] == ['Fantasy', 'Humorous', 'LitRPG']  # the umbrella category of the other volumes is no genre to copy


def test_series_fill_book_without_real_genre():
    ctx = {'abs_update': lambda *a, **k: True, 'dry_run': False, 'genre_max': 6}
    rec = lambda i, g: {'id': str(i), 'lib': 'L', 'title': f'b{i}', 'genres': g, 'tags': set(), 'series': [{'name': 'Body'}]}
    rs = [rec(1, ['Fantasy', 'Humorous', 'LitRPG']), rec(2, ['Literature & Fiction']), rec(3, ['Literature & Fiction'])]
    S.series_genre_fill(ctx, rs)
    assert rs[1]['genres'] == ['Fantasy', 'Humorous', 'LitRPG']  # the umbrella goes in the same run
    assert rs[0]['genres'] == ['Fantasy', 'Humorous', 'LitRPG']


def test_series_fill_no_extra_main_genre_on_normal_books():
    ctx = {'abs_update': lambda *a, **k: True, 'dry_run': False, 'genre_max': 6}
    rec = lambda i, g: {'id': str(i), 'lib': 'L', 'title': f'b{i}', 'genres': g, 'tags': set(), 'series': [{'name': 'S'}]}
    rs = [rec(1, ['Fantasy', 'Humorous', 'LitRPG']), rec(2, ['Fantasy', 'Humorous', 'LitRPG']), rec(3, ['Fantasy'])]
    S.series_genre_fill(ctx, rs)
    assert rs[2]['genres'] == ['Fantasy', 'LitRPG']  # Humorous is a main genre: the book step would drop it again if Audible does not name it


def test_one_run_series_fill_then_audible_rules():
    # the book gets LitRPG from its series; its Audible ladders (Fantasy > Epic, Science Fiction > Cyberpunk) then drop SF + Cyberpunk at once
    lads = L('Science Fiction & Fantasy > Fantasy > Epic', 'Science Fiction & Fantasy > Science Fiction > Cyberpunk')
    drops = {}
    ctx = {'abs_update': lambda *a, **k: True, 'dry_run': False, 'genre_max': 6, 'genre_ladders': {'L_3': lads}, 'genre_drops': drops}
    rec = lambda i, g: {'id': str(i), 'lib': 'L', 'title': f'b{i}', 'genres': g, 'tags': set(), 'series': [{'name': 'S'}]}
    first = S.rebuild_genres([], lads)
    assert first == ['Fantasy', 'Science Fiction', 'Epic Fantasy', 'Cyberpunk']
    rs = [rec(1, ['Fantasy', 'Epic Fantasy', 'LitRPG']), rec(2, ['Fantasy', 'Epic Fantasy', 'LitRPG']), rec(3, list(first))]
    S.series_genre_fill(ctx, rs)
    assert rs[2]['genres'] == ['Fantasy', 'Epic Fantasy', 'LitRPG'] and set(drops['L_3']) >= {'sciencefiction', 'cyberpunk'}
    assert S.rebuild_genres(rs[2]['genres'], lads) == rs[2]['genres']  # the next run's book step changes nothing


def test_subgenres_of_the_other_big_genre_go():
    sf = L('Science Fiction & Fantasy > Science Fiction > Space Opera')
    assert S.rebuild_genres(['Epic Fantasy', 'Epic', 'Space Opera'], sf) == ['Science Fiction', 'Space Opera']  # Dune
    fa = L('Science Fiction & Fantasy > Fantasy > Epic')
    assert S.rebuild_genres(['Space Opera', 'Cyberpunk', 'LitRPG'], fa) == ['Fantasy', 'Epic Fantasy', 'LitRPG']


def test_fantasy_never_dropped_by_majority():
    lads = L('Science Fiction & Fantasy > Fantasy > Action & Adventure', 'Science Fiction & Fantasy > Fantasy > Epic',
             'Science Fiction & Fantasy > Science Fiction > Cyberpunk', 'Science Fiction & Fantasy > Science Fiction > First Contact')
    g = S.rebuild_genres(['LitRPG'], lads)
    assert 'Fantasy' in g and 'Epic Fantasy' in g  # Defiance of the Fall
    lads = L('Science Fiction & Fantasy > Fantasy', 'Science Fiction & Fantasy > Science Fiction > Hard Science Fiction')
    assert 'Fantasy' in S.rebuild_genres([], lads)  # Audible names Fantasy: kept per book (Murderbot's is removed by the series check)


def test_genre_loop_rules_2026_11_16():
    # nested 'Romance > Fantasy' is fantasy evidence (Can't Spell Treason Without Tea)
    assert S.big_evidence(L('Romance > Fantasy'), 'fantasy') and S.big_named(L('Romance > Fantasy'), 'fantasy')
    # 'Fantasy > Epic' is real evidence, 'Fantasy > Action & Adventure' is not
    assert S.big_evidence(L('Science Fiction & Fantasy > Fantasy > Epic'), 'fantasy')
    assert not S.big_evidence(L('Science Fiction & Fantasy > Fantasy > Action & Adventure'), 'fantasy')
    # LitRPG stand-ins: Post-Apocalyptic / Dystopian / Cyberpunk are no science fiction on a LitRPG book
    lads = L('Science Fiction & Fantasy > Fantasy > Epic', 'Science Fiction & Fantasy > Science Fiction > Post-Apocalyptic')
    g = S.rebuild_genres(['LitRPG'], lads)
    assert 'Science Fiction' not in g and 'Post Apocalyptic' in g and 'Fantasy' in g
    # Steampunk / Alternate History are no evidence: an old Fantasy stays, the science fiction goes (Second Chance Swordsman, Temeraire)
    g = S.rebuild_genres(['Fantasy'], L('Science Fiction & Fantasy > Science Fiction > Steampunk'))
    assert 'Fantasy' in g and 'Science Fiction' not in g and 'Steampunk' in g
    # old Fantasy goes for REAL science fiction (Hunger Games)
    assert 'Fantasy' not in S.rebuild_genres(['Fantasy'], L('Science Fiction & Fantasy > Science Fiction > Dystopian'))
    # an old Fantasy stays when Audible names neither (Piranesi)
    assert 'Fantasy' in S.rebuild_genres(['Fantasy'], L('Literature & Fiction > Genre Fiction > Literary Fiction'))
    # Hardcover as arbiter: Children of Dune (Audible: Fantasy > Epic + Science Fiction > Space Opera; readers 23 : 0)
    lads = L('Science Fiction & Fantasy > Fantasy > Epic', 'Science Fiction & Fantasy > Science Fiction > Space Opera')
    assert S.rebuild_genres([], lads, hc={'Science Fiction': 23, 'Fantasy': 0}) == ['Science Fiction', 'Space Opera']
    # first real level is the main genre: 'Teen & Young Adult > Romance > Science Fiction & Fantasy > Fantasy' -> Romance + Fantasy Romance
    m, sp, _ = S.genres_from_audible(L('Teen & Young Adult > Romance > Science Fiction & Fantasy > Fantasy'))
    assert m == ['Romance'] and 'Fantasy Romance' in sp
    # origin / format names are no genres
    assert S.canonical_genre('German literature') is None and S.canonical_genre('Novella') is None and S.theme_tag('40k') == 'Warhammer 40k'


def test_series_check_drops_off_series_big_genre():
    sf = L('Science Fiction & Fantasy > Science Fiction > Hard Science Fiction')
    lads = {f'L_{i}': sf for i in range(2, 6)}
    lads['L_1'] = L('Science Fiction & Fantasy > Fantasy', 'Science Fiction & Fantasy > Science Fiction > Hard Science Fiction')
    ctx = {'abs_update': lambda *a, **k: True, 'dry_run': False, 'genre_max': 6, 'genre_ladders': lads, 'genre_series_drops': {}}
    rec = lambda i, g: {'id': str(i), 'lib': 'L', 'title': f'b{i}', 'genres': g, 'tags': set(), 'series': [{'name': 'Murderbot'}]}
    rs = [rec(1, ['Fantasy', 'Science Fiction', 'Hard Science Fiction'])] + [rec(i, ['Science Fiction', 'Hard Science Fiction']) for i in range(2, 6)]
    S.series_genre_fill(ctx, rs)
    assert rs[0]['genres'] == ['Science Fiction', 'Hard Science Fiction'] and 'fantasy' in ctx['genre_series_drops']['L_1']
