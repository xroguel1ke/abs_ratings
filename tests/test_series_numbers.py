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


def test_name_contained_in_the_other():
    t = 'Tales of Witness (Malazan Universe)'
    recs = [rec(1, 'Legacies of Betrayal', '2026', [(t, '3')], 'S/x/3 - a', author='E'),
            rec(2, 'No Life Forsaken', '2025', [('Witness', '2'), (t, '2')], 'S/x/2 - b', author='E')]
    c, _ = ctx()
    assert S.drop_duplicate_series(c, recs) == 1 and [x['name'] for x in recs[1]['series']] == [t]


def test_folder_only_series_of_a_new_book():
    import time
    c, _ = ctx(); c['audible_series_names'] = {'Tales of Witness (Malazan Universe)'}; c['new_book_days'] = 30
    folder = 'Malazan (In-World Chronological Order)'
    new = rec(2, 'No Life Forsaken', '2025', [(folder, '2'), ('Tales of Witness (Malazan Universe)', '2')], f'Steven Erikson/{folder}/2 - No Life Forsaken', author='E')
    new['added'] = time.time() * 1000
    old = rec(3, 'Old', '2020', [(folder, '1'), ('Tales of Witness (Malazan Universe)', '1')], f'Steven Erikson/{folder}/1 - Old', author='E')
    old['added'] = (time.time() - 400 * 86400) * 1000
    assert S.drop_folder_only_series(c, [new]) == 1 and [x['name'] for x in new['series']] == ['Tales of Witness (Malazan Universe)']
    assert S.drop_folder_only_series(c, [old]) == 0  # not a new book


def test_sub_series_inside_umbrella_name_stays():
    oa = [rec(1, 'The Dragonbone Chair', '1988', [('Osten Ard', '1')], 'T/Osten Ard/1 - a', author='T'),
          rec(2, 'The Witchwood Crown', '2017', [('Osten Ard', '5'), ('The Last King of Osten Ard', '5')], 'T/Osten Ard/5 - b', author='T')]
    c, _ = ctx()
    assert S.drop_duplicate_series(c, oa) == 0  # fake-write check 2026-11-15: the sub-series must stay


def test_series_label_subtitles():
    dof = lambda n: [{'name': 'Defiance of the Fall', 'sequence': str(n)}]
    c = S.clean_series_label_subtitle
    assert c('Defiance of the Fall, Book 6', dof(6)) == ''
    assert c('A LitRPG Adventure (Defiance of the Fall, Book 5)', dof(5)) == 'A LitRPG Adventure'
    assert c('A LitRPG Adventure', dof(5)) == 'A LitRPG Adventure'
    assert c('Defiance of the Fall, Book 6', dof(7)) == 'Defiance of the Fall, Book 6'  # another number: not this book's label
    assert c('Dark Elf Trilogy, Book 3', dof(3)) == 'Dark Elf Trilogy, Book 3'  # another series
    assert c('Apollo 13', [{'name': 'Apollo', 'sequence': '13'}]) == 'Apollo 13'  # no word before the number


def test_series_title_form():
    names = ['Defiance of the Fall 8', 'Defiance of the Fall 9', 'Defiance of the Fall, Book 10', 'Defiance of the Fall 11', 'Defiance of the Fall']
    seqs = ['8', '9', '10', '11', '1']
    recs = [rec(i, t, '2023', [('Defiance of the Fall', q)], f'J/D/{q} - x', author='J') for i, (t, q) in enumerate(zip(names, seqs))]
    c, calls = ctx()
    assert S.harmonize_series_titles(c, recs) == 1 and recs[2]['title'] == 'Defiance of the Fall 10' and recs[4]['title'] == 'Defiance of the Fall'
    mixed = [rec(i, t, '2020', [('X', str(i))], 'A/X', author='A') for i, t in enumerate(['X 1', 'X 2', 'X, Book 3', 'X, Book 4'], 1)]
    assert S.harmonize_series_titles(c, mixed) == 0  # no clear form
    locked = [rec(i, f'Y {i}', '2020', [('Y', str(i))], 'A/Y', author='A') for i in (1, 2, 3)] + [rec(4, 'Y, Book 4', '2020', [('Y', '4')], 'A/Y', author='A', tags=('lock_title',))]
    assert S.harmonize_series_titles(c, locked) == 0


def test_series_label_subtitle_variants():
    c = S.clean_series_label_subtitle
    one = lambda n, q: [{'name': n, 'sequence': q}]
    assert c('Book 18 of the Spellmonger Series', one('The Spellmonger', '18')) == ''
    assert c('Volume 6', one('My Quiet Blacksmith Life in Another World', '6')) == ''
    assert c('Volume 4: My Quiet Blacksmith Life in Another World, Book 4', one('My Quiet Blacksmith Life in Another World', '4')) == ''
    assert c('Gentleman Bastards, Book 2', one('Gentleman Bastard', '2')) == ''
    assert c('Shadows and Crowns Series, Book 3', one('Shadows and Crowns', '3')) == ''
    assert c('Light Novel (Mushoku Tensei: Jobless Reincarnation, Book 28)', one('Mushoku Tensei: Jobless Reincarnation', '28')) == ''
    assert c('The Red Maidens (Black Summoner, Book 6)', one('Black Summoner', '6')) == 'The Red Maidens'
    # kept (checked against the whole library 2026-11-15)
    assert c('Legend of Drizzt: Dark Elf Trilogy, Book 3', one('Legend of Drizzt', '3')) != ''  # names a sub-series
    assert c('The Murderbot Diaries, Book 8', one('Murderbot Diaries', '7')) != ''  # another number
    assert c('The Beginning After the End, Book 3, 4', one('The Beginning After the End', '3-4')) != ''  # a range
    assert c('Book 9: Land of Pain', one('Dragon Heart', '9')) != ''  # the volume's own title
    assert c('Sons of Ares #1', one('Red Rising Saga', '1')) != ''


def test_title_form_keeps_separator():
    recs = [rec(i, f'Cinnamon Bun: Volume {i}', '2022', [('Cinnamon Bun', str(i))], 'A/C', author='A') for i in (1, 2, 4, 5)]
    recs.append(rec(3, 'Cinnamon Bun 3', '2022', [('Cinnamon Bun', '3')], 'A/C', author='A'))
    c, _ = ctx()
    assert S.harmonize_series_titles(c, recs) == 1 and recs[-1]['title'] == 'Cinnamon Bun: Volume 3'
