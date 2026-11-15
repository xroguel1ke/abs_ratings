"""Folder series + sub-series numbering (Kyralia Universe, 2026-11-10). Run: python -m pytest tests/test_series_numbers.py"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
import abs_series as S


def rec(i, title, year, series, path, author='Trudi Canavan', tags=()):
    return {'id': i, 'lib': 'l', 'title': title, 'authors': [author], 'year': year, 'tags': set(tags), 'path': path,
            'series': [{'name': n, 'sequence': q} for n, q in series]}


def kyralia(amb_series=(('Traitor Spy Trilogy', '1'),), amb_tags=()):
    d = 'Trudi Canavan/Kyralia Universe/'
    return [rec(1, "The Magicians' Guild", '2007', [('Kyralia Universe', '1'), ('Black Magician Trilogy', '1')], d + "1 - The Magicians' Guild"),
            rec(2, 'The Novice', '2008', [('Kyralia Universe', '2'), ('Black Magician Trilogy', '2')], d + '2 - The Novice'),
            rec(3, 'The High Lord', '2009', [('Kyralia Universe', '3'), ('Black Magician Trilogy', '3')], d + '3 - The High Lord'),
            rec(4, "The Magician's Apprentice", '2013', [('Kyralia Universe', '0.5')], d + "0.5 - The Magician's Apprentice"),
            rec(5, "The Ambassador's Mission", '2011', list(amb_series), d + '1 - The Ambassadors Mission', tags=amb_tags),
            rec(6, 'The Rogue', '2011', [('Kyralia Universe', None), ('Traitor Spy Trilogy', '2')], d + '2 - The Rogue'),
            rec(7, 'The Traitor Queen', '2012', [('Kyralia Universe', None), ('Traitor Spy Trilogy', '3')], d + '3 - The Traitor Queen')]


def ctx():
    calls = []
    return {'dry_run': False, 'abs_update': lambda *a: calls.append(a) or True}, calls


def seq(records, title, series='Kyralia Universe'):
    r = next(r for r in records if r['title'] == title)
    return next((x['sequence'] for x in r['series'] if x['name'] == series), 'absent')


def test_folder_series_then_follow_up_numbers():
    recs = kyralia(); c, calls = ctx()
    assert S.fill_folder_series(c, recs) == 1
    assert seq(recs, "The Ambassador's Mission") is None  # added without a number
    S.fill_from_subseries(c, recs)
    assert (seq(recs, "The Ambassador's Mission"), seq(recs, 'The Rogue'), seq(recs, 'The Traitor Queen')) == ('4', '5', '6')
    assert seq(recs, "The Magician's Apprentice") == '0.5'


def test_later_prequel_does_not_block_the_follow_up():
    recs = kyralia(amb_series=(('Kyralia Universe', None), ('Traitor Spy Trilogy', '1'))); c, _ = ctx()
    S.fill_from_subseries(c, recs)
    assert seq(recs, 'The Rogue') == '5'


def test_folder_series_respects_lock_and_other_authors():
    recs = kyralia(amb_tags=('lock_series',)); c, _ = ctx()
    assert S.fill_folder_series(c, recs) == 0
    recs = kyralia(); recs[4]['authors'] = ['Somebody Else']; c, _ = ctx()
    assert S.fill_folder_series(c, recs) == 0


def test_folder_series_needs_every_other_book():
    recs = kyralia(); recs[5]['series'] = [('Traitor Spy Trilogy', '2')] and [{'name': 'Traitor Spy Trilogy', 'sequence': '2'}]; c, _ = ctx()
    assert S.fill_folder_series(c, recs) == 0


def test_same_series_twice_on_a_book():
    d = 'Fuurou/So You Want to Live the Slow Life! A Guide to Life in the Beastly Wilds/'
    long_ = 'So You Want to Live the Slow Life! A Guide to Life in the Beastly Wilds'
    recs = [rec(1, 'Vol. 1', '2023', [('So You Want to Live the Slow Life?', '1')], d + '1 - v1', author='Fuurou'),
            rec(2, 'Vol. 2', '2023', [('So You Want to Live the Slow Life?', '2')], d + '2 - v2', author='Fuurou'),
            rec(3, 'Vol. 3', '2024', [('So You Want to Live the Slow Life?', '3'), (long_, '3')], d + '3 - v3', author='Fuurou'),
            rec(4, 'Vol. 4', '2025', [('So You Want to Live the Slow Life?', '4'), (long_, '4')], d + '4 - v4', author='Fuurou')]
    c, _ = ctx()
    assert S.drop_duplicate_series(c, recs) == 2
    assert all([x['name'] for x in r['series']] == ['So You Want to Live the Slow Life?'] for r in recs)


def test_umbrella_with_other_numbers_stays():
    recs = kyralia(); c, _ = ctx()
    assert S.drop_duplicate_series(c, recs) == 0  # 'Kyralia Universe' vs 'Black Magician Trilogy': no name prefix
    recs = [rec(1, 'a', '2020', [('The Long Saga Name', '1'), ('The Long Saga Name Novellas', '2')], 'A/S/1 - a')]
    assert S.drop_duplicate_series(c, recs) == 0  # different numbers: a real sub-series


def test_one_word_short_name():
    recs = [rec(i, f'v{i}', '2020', [("Konosuba: God's Blessing on This Wonderful World!", str(i))], f'N/Konosuba/{i} - v', author='N') for i in range(1, 4)]
    recs.append(rec(13, 'v13', '2026', [("Konosuba: God's Blessing on This Wonderful World!", '13'), ('Konosuba', '13')], 'N/Konosuba/13 - v', author='N'))
    c, _ = ctx()
    assert S.drop_duplicate_series(c, recs) == 1 and [x['name'] for x in recs[-1]['series']] == ["Konosuba: God's Blessing on This Wonderful World!"]


def test_real_sub_series_stay():
    # fake-write test 2026-11-15: these were removed by the first version of the rule
    fl = [rec(1, 'The Blade Itself', '2006', [('First Law World', '1'), ('The First Law Trilogy', '1')], 'J/First Law World/1 - a', author='J'),
          rec(2, 'Before They Are Hanged', '2007', [('First Law World', '2'), ('The First Law Trilogy', '2')], 'J/First Law World/2 - b', author='J'),
          rec(3, 'Best Served Cold', '2009', [('First Law World', '4')], 'J/First Law World/4 - c', author='J')]
    c, _ = ctx()
    assert S.drop_duplicate_series(c, fl) == 0
    ij = [rec(i, f'c{i}', '2024', [("Irwin's Journey", str(i)), ("Irwin's Journey: The Cardsmith", str(i))], f'J/Irwin/{i} - c', author='J') for i in (1, 2, 3)]
    assert S.drop_duplicate_series(c, ij) == 0  # always together: umbrella + sub-series
    mb = [rec(i, f'm{i}', '2006', [('The Mistborn Saga', str(i)), ('Mistborn, Era 1: Original Trilogy', str(i))], f'B/Mistborn/{i} - m', author='B') for i in (1, 2, 3)]
    mb.append(rec(4, 'm4', '2011', [('The Mistborn Saga', '4')], 'B/Mistborn/4 - m', author='B'))
    assert S.drop_duplicate_series(c, mb) == 0


def test_same_name_twice_punctuation():
    n1, n2 = 'The Haunted Bookstore - Gateway to a Parallel Universe', 'The Haunted Bookstore_ Gateway to a Parallel Universe'
    recs = [rec(1, 'v1', '2023', [(n1, '1')], 'S/x/1', author='S'), rec(2, 'v2', '2024', [(n1, '2'), (n2, '2')], 'S/x/2', author='S')]
    c, _ = ctx()
    assert S.drop_duplicate_series(c, recs) == 1 and [x['name'] for x in recs[1]['series']] == [n1]
