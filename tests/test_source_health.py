"""Source health checks (source_health.py) with fake answers: canary field checks, breaker suspect/trip/clear and streak restore."""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import source_health as SH

GOOD = {'title_raw': 'The Final Empire', 'authors_raw': ['Brandon Sanderson'], 'narrators_raw': ['Michael Kramer'], 'runtime_sec': 88000,
        'count': 90000, 'meta_raw': {'series': [{'name': 'Mistborn', 'part': '1'}], 'language': 'english'}}


def test_audible_product_fields():
    assert SH.check_audible_product(GOOD) == []
    renamed = dict(GOOD, runtime_sec=None, narrators_raw=[])  # API renamed runtime_length_min / narrators
    assert SH.check_audible_product(renamed) == ['narrators', 'runtime']
    assert SH.check_audible_product('missing') and SH.check_audible_product(None)


def test_canaries():
    assert SH.run_canaries(lambda a, d: GOOD) == {}
    down = SH.run_canaries(lambda a, d: dict(GOOD, count=0))
    assert 'ratings' in down['audible'], down
    hc_ok = {'search': {'results': {'hits': [{'document': {'id': 1, 'title': 'The Final Empire', 'rating': 4.4, 'ratings_count': 9, 'author_names': ['B']}}]}}}
    assert SH.run_canaries(lambda a, d: GOOD, lambda q: hc_ok) == {}
    assert 'hardcover' in SH.run_canaries(lambda a, d: GOOD, lambda q: {'search': {'results': {'found': 3}}})  # format changed
    assert 'hardcover' in SH.run_canaries(lambda a, d: GOOD, lambda q: None)
    assert SH.run_canaries(lambda a, d: None, lambda q: hc_ok, only='hardcover') == {}  # only the suspected source is checked


def test_breaker_needs_history_and_streak():
    state = {f'k{i}': {'hardcover': {'st': 'found'}} for i in range(60)}
    b = SH.Breaker(state, ['hardcover', 'goodreads'], streak=3)
    assert b.armed('hardcover') and not b.armed('goodreads')  # goodreads: no history -> the canary has to do it
    prev = [state[f'k{i}']['hardcover'].copy() for i in range(3)]
    assert not b.note('k0', 'hardcover', 'absent', prev[0])
    assert not b.note('k1', 'hardcover', 'found', prev[1])  # a hit resets the streak
    hits = [b.note(f'k{i}', 'hardcover', 'absent', prev[0]) for i in range(3)]
    assert hits == [False, False, True], hits
    for i in range(3): state[f'k{i}']['hardcover'] = {'st': 'paused', 'why': 'not listed'}  # what the caller stored meanwhile
    why = b.trip(state, 'hardcover', 'format changed')
    assert 'format changed' in why and all(state[f'k{i}']['hardcover'] == {'st': 'found'} for i in range(3)), state
    assert not b.note('k5', 'hardcover', 'absent', None)  # tripped: no more counting


def test_breaker_clear_when_check_book_fine():
    state = {f'k{i}': {'audible': {'st': 'found'}} for i in range(60)}
    b = SH.Breaker(state, ['audible'], streak=2)
    b.note('a', 'audible', 'missing', None); assert b.note('b', 'audible', 'missing', None)
    b.clear('audible')
    assert not b.note('c', 'audible', 'missing', None) and 'audible' not in b.tripped


if __name__ == '__main__':
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn): fn(); n += 1; print('ok', name)
    print(n, 'tests passed')
