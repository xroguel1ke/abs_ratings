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


def test_lock_series_blocks_merge():
    r = [rec(1, 'a', 'X', 'The Big Saga', '1'), rec(2, 'b', 'X', 'The Big Saga', '2'), rec(3, 'c', 'X', 'Big Series', '3', tags=['lock_series'])]
    assert names(r)[0] == []


def test_audible_name_becomes_target():
    r = [rec(1, 'a', 'X', 'Realms of Fire', '1'), rec(2, 'b', 'X', 'Realms of Fire', '2'), rec(3, 'c', 'X', 'Realm of Fire', '3')]
    assert names(r)[0] == [('Realms of Fire', ['Realm of Fire'])]
    assert names(r, {'Realm of Fire'})[0] == [('Realm of Fire', ['Realms of Fire'])]


if __name__ == '__main__':
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn): fn(); n += 1; print('ok', name)
    print(n, 'tests passed')
