"""Series merge rules from CLAUDE.md with synthetic data. Run: python tests/test_series.py (or pytest)"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import abs_series as S


def rec(i, title, author, series, seq, tags=()):
    return {'id': i, 'lib': 'l', 'title': title, 'authors': [author], 'narrators': ['N'], 'series': [{'name': series, 'sequence': seq}], 'tags': set(tags)}


def names(records, preferred=None):
    merges, rejected = S.find_merges(records, {}, preferred)
    return sorted((m['target'].name, sorted(s.name for s in m['members'])) for m in merges), rejected


def test_same_name_after_normalization_merges_complementary_numbers():
    r = [rec(1, 'a', 'X', 'The Big Saga', '1'), rec(2, 'b', 'X', 'The Big Saga', '2'), rec(3, 'c', 'X', 'Big Series', '3')]
    m, _ = names(r)
    assert m == [('The Big Saga', ['Big Series'])], m


def test_contained_name_is_never_merged():
    r = [rec(1, 'a', 'Herbert', 'Dune', '1'), rec(2, 'b', 'Herbert', 'Dune', '2'), rec(3, 'c', 'Herbert', 'Dune, les origines', '3')]
    assert names(r)[0] == []


def test_overlapping_numbers_are_never_merged():
    r = [rec(1, 'a', 'X', 'Star Cycle', '1'), rec(2, 'b', 'X', 'Star Cycle', '2'), rec(3, 'c', 'X', 'The Star Cycle', '1')]
    assert names(r)[0] == []


def test_different_authors_are_never_merged():
    r = [rec(1, 'a', 'Anna', 'Sky Road', '1'), rec(2, 'b', 'Anna', 'Sky Road', '2'), rec(3, 'c', 'Boris', 'Sky Road Series', '3')]
    assert names(r)[0] == []


def test_lock_series_book_keeps_its_series_as_target():  # user rule 2026-11-08 (was: lock on either side blocks the merge)
    r = _series('Coven Crest Academy', 'X', [1, 2]) + _series('Coven Crest Academy Series', 'X', [3, 4], 200)
    r[2]['tags'] = {'lock_series'}
    assert names(r)[0] == [('Coven Crest Academy Series', ['Coven Crest Academy'])], names(r)[0]


def test_locked_side_becomes_target():  # user rule 2026-11-08: lock_series protects the book, not the whole merge
    r = [rec(1, 'a', 'X', 'The Big Saga', '1'), rec(2, 'b', 'X', 'The Big Saga', '2'), rec(3, 'c', 'X', 'Big Series', '3', tags=['lock_series'])]
    assert names(r)[0] == [('Big Series', ['The Big Saga'])], names(r)[0]


def test_locks_in_two_series_go_to_review():
    r = [rec(1, 'a', 'X', 'The Big Saga', '1', tags=['lock_series']), rec(2, 'b', 'X', 'The Big Saga', '2'), rec(3, 'c', 'X', 'Big Series', '3', tags=['lock_series'])]
    m, rej = names(r)
    assert m == [] and any('lock_series' in x['reason'] for x in rej), (m, rej)


def test_trailing_space_name_is_never_the_target():
    r = [rec(i, f'w{i}', 'Ring', 'The Titan ', str(i)) for i in range(3, 9)] + [rec(1, 'w1', 'Ring', 'The Titan', '1')]
    assert names(r)[0] == [('The Titan', ['The Titan '])], names(r)[0]


def test_audible_name_becomes_target():
    r = [rec(1, 'a', 'X', 'Realms of Fire', '1'), rec(2, 'b', 'X', 'Realms of Fire', '2'), rec(3, 'c', 'X', 'Realm of Fire', '3')]
    assert names(r)[0] == [('Realms of Fire', ['Realm of Fire'])]
    assert names(r, {'Realm of Fire'})[0] == [('Realm of Fire', ['Realms of Fire'])]


def test_your_target_name_wins():  # review list 'merge as ...' (choose the target name)
    r = [rec(1, 'a', 'X', 'The Big Saga', '1'), rec(2, 'b', 'X', 'The Big Saga', '2'), rec(3, 'c', 'X', 'Big Series', '3')]
    mid = S.merge_id('series', 'l', 'Big Series', 'The Big Saga')
    merges, _ = S.find_merges(r, {mid: {'action': 'merge', 'target': 'Big Series'}}, None)
    assert [(m['target'].name, [x.name for x in m['members']]) for m in merges] == [('Big Series', ['The Big Saga'])]


def test_book_of_another_author_in_a_series_goes_to_review():
    r = [rec(1, 'Best Served Cold', 'Joe Abercrombie', 'The Age of Madness', '1'), rec(2, 'A Little Hatred', 'Joe Abercrombie', 'The Age of Madness', '2'),
         rec(3, 'The Trouble with Peace', 'Joe Abercrombie', 'The Age of Madness', '3'), rec(4, 'Bury Our Bones', 'V. E. Schwab', 'The Age of Madness', None)]
    out = S.foreign_series_books(r)
    assert [(x['title'], x['main_author']) for x in out] == [('Bury Our Bones', 'Joe Abercrombie')], out
    assert S.foreign_series_books(r, {out[0]['id']: {'action': 'keep'}}) == [], 'your decision hides it'
    r2 = [rec(1, 'a', 'A', 'Shared World', '1'), rec(2, 'b', 'B', 'Shared World', '2'), rec(3, 'c', 'C', 'Shared World', '3')]
    assert S.foreign_series_books(r2) == [], 'a series of several authors is no case'


def test_duplicate_recordings_in_report():
    r1 = rec(1, 'Throne', 'Phil Tucker', 'Throne Hunters', '5'); r2 = rec(2, 'Throne Hunters 5', 'Phil Tucker', 'Throne Hunters', '5'); r3 = rec(3, 'Other', 'Phil Tucker', 'Throne Hunters', '6')
    for r, d in ((r1, 65834), (r2, 65835), (r3, 70000)): r.update({'duration': d, 'asin': None, 'cover': None, 'description': '', 'language': 'English', 'genres': [], 'isbn': None, 'media_tags': []})
    ctx = {'dry_run': True, 'report_dir': '/tmp/claude-0/rep_test', 'extra': {}}
    os.makedirs(ctx['report_dir'], exist_ok=True)
    rep = S.build_report(ctx, [r1, r2, r3], [], [], {})
    assert len(rep['duplicate_recordings']) == 1 and rep['duplicate_recordings'][0]['titles'] == ['Throne', 'Throne Hunters 5'], rep['duplicate_recordings']


def _series(name, author, nums, start=100):
    return [rec(start + i, f'{name} {n}', author, name, str(n)) for i, n in enumerate(nums)]


def test_interleaved_numbers_merge_loosely_similar_names():  # user rule 2026-11-04
    r = _series('Ember Realm Chronicles', 'X', [1, 2, 3, 7, 8, 10]) + _series('Ember Realm', 'X', [4, 5, 6, 9, 11, 12, 13], 200)
    m, _ = names(r)
    assert len(m) == 1 and set([m[0][0]] + m[0][1]) == {'Ember Realm Chronicles', 'Ember Realm'}, m


def test_loose_names_without_interleaving_are_not_merged():
    for a, b in (([1, 2, 3], [4, 5, 6]), ([1, 2], [5, 6]), ([1, 3], [2])):  # continuation only, gap, one side too small
        r = _series('Ember Realm Chronicles', 'X', a) + _series('Ember Realm', 'X', b, 200)
        assert names(r)[0] == [], (a, b)


def test_interleaved_but_other_author_or_edition_never_merges():
    r = _series('Ember Realm Chronicles', 'X', [1, 2, 3, 7, 8]) + _series('Ember Realm', 'Y', [4, 5, 6, 9], 200)
    assert names(r)[0] == []
    r = _series('Ember Realm', 'X', [1, 2, 3, 7, 8]) + _series('Ember Realm (Full Cast)', 'X', [4, 5, 6, 9], 200)
    assert names(r)[0] == []


def test_swapped_target_keeps_a_relation():  # crash 2026-11-07: 'Series' object has no attribute 'relation'
    r = [rec(i, f'w{i}', 'Cobble', 'Benjamin Ashwood', str(i)) for i in range(2, 7)] + [rec(1, 'w1', 'Cobble', 'Benjamin Ashwood Series', '1')]
    merges, _ = S.find_merges(r, {}, {'Benjamin Ashwood Series'})
    assert merges and merges[0]['target'].name == 'Benjamin Ashwood Series'
    assert all(getattr(s, 'relation', None) for s in merges[0]['members']), [vars(s) for s in merges[0]['members']]


if __name__ == '__main__':
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn): fn(); n += 1; print('ok', name)
    print(n, 'tests passed')
