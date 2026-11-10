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
