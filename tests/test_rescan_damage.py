"""Rules against ABS metadata of another volume (ABS rescan 2026-11-07) and merge-target guards. Run: python tests/test_rescan_damage.py"""
import os, sys
os.environ.setdefault('DATA_DIR', '/tmp/rescan_damage_test'); os.environ.setdefault('ABS_URL', 'http://127.0.0.1:1'); os.environ.setdefault('API_TOKEN', 'x')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import abs_ratings as A
import abs_series as S


def _owners(*titles):
    A.TITLE_OWNERS.clear()
    for i, t in enumerate(titles): A.TITLE_OWNERS.setdefault(('L', A.normalize_title_text(t)), set()).add((f'L_{i}', 'x'))


def test_title_shared():
    _owners('Solo Leveling, Vol. 7 (Novel)', 'Solo Leveling, Vol. 7 (Novel)', 'Dune')
    assert A.title_shared('L_0', 'Solo Leveling, Vol. 7 (Novel)')
    assert not A.title_shared('L_2', 'Dune')
    A.TITLE_OWNERS[('L', 'threshold')] = {('L_5', 'a'), ('L_6', 'b')}
    assert not A.title_shared('L_5', 'Threshold')  # two authors
    A.TITLE_OWNERS[('L', 'judicator jane')] = {('L_7', 'a'), ('L_8', 'a')}; A.KEY_DUR.update({'L_7': 30000, 'L_8': 60000})
    assert A.title_shared('L_7', 'Judicator Jane') and not A.title_shared('L_7', 'Judicator Jane', 31000)  # the other one is far longer
    assert A.title_shared('L_7', 'Judicator Jane', 59000)


def test_folder_conflict_found():
    _owners('Solo Leveling, Vol. 7 (Novel)', 'Solo Leveling, Vol. 7 (Novel)')
    it = {'relPath': 'Chugong/Solo Leveling/2 - Solo Leveling, Vol. 2 - Ki Hong Lee'}
    meta = {'series': [{'name': 'Solo Leveling (Novel)', 'sequence': '7'}]}
    assert A.folder_volume_conflict(it, 'L_0', 'Solo Leveling, Vol. 7 (Novel)', meta) == {'title': 'Solo Leveling, Vol. 2', 'num': '2'}


def test_folder_conflict_not_for_the_real_volume():
    _owners('Solo Leveling, Vol. 7 (Novel)', 'Solo Leveling, Vol. 7 (Novel)')
    it = {'relPath': 'Chugong/Solo Leveling/7 - Solo Leveling, Vol. 7 - Ki Hong Lee'}
    assert A.folder_volume_conflict(it, 'L_0', 'Solo Leveling, Vol. 7 (Novel)', {'series': [{'name': 'Solo Leveling', 'sequence': '7'}]}) is None


def test_folder_conflict_needs_shared_title():  # Discworld folder numbers are the user's own order: title unique -> nothing
    _owners('Equal Rites')
    it = {'relPath': 'Terry Pratchett/Discworld/1 - Equal Rites - Full Cast'}
    assert A.folder_volume_conflict(it, 'L_0', 'Equal Rites', {'series': [{'name': 'Discworld', 'sequence': '3'}]}) is None


def test_folder_conflict_bare_title():
    _owners('Mushoku Tensei', 'Mushoku Tensei', 'Mushoku Tensei')
    it = {'relPath': 'Rifujin na Magonote/Mushoku Tensei - Jobless Reincarnation/08 - Mushoku Tensei, Vol. 8 - Cliff Kirk'}
    r = A.folder_volume_conflict(it, 'L_1', 'Mushoku Tensei', {'series': [{'name': 'Mushoku Tensei Web Novel', 'sequence': '3'}]})
    assert r == {'title': 'Mushoku Tensei, Vol. 8', 'num': '8'}, r


def test_ampersand_series_key():
    assert S.series_key('Memory, Sorrow & Thorn') == S.series_key('Memory, Sorrow, and Thorn')


class _Ser(S.Series if hasattr(S, 'Series') else object): pass


def _rec(i, title, series, lang='English', tags=()):
    return {'id': i, 'lib': 'L', 'title': title, 'authors': ['Natsume Akatsuki'], 'series': series, 'tags': set(tags), 'language': lang}


def test_non_latin_never_target_unless_locked():
    jp = "この素晴らしい世界に祝福を! Konosuba Light Novel"
    en = "Konosuba: God's Blessing on This Wonderful World! (Light Novel)"
    recs = [_rec(f'b{i}', f'Vol {i}', [{'name': en, 'sequence': str(i)}]) for i in range(1, 9)] + \
           [_rec('b9', 'Vol 9', [{'name': jp, 'sequence': '9'}]), _rec('b10', 'Vol 10', [{'name': jp, 'sequence': '10'}])]
    merges, _ = S.find_merges(recs, {'series|L|' + jp + '->' + en: {'action': 'merge'}, 'series|L|' + en + '->' + jp: {'action': 'merge'}})
    assert merges and all(S.is_latin(m['target'].name) for m in merges), [m['target'].name for m in merges]
    recs[-1]['tags'] = {'lock_series'}; recs[-2]['tags'] = {'lock_series'}
    merges, rej = S.find_merges(recs, {'series|L|' + jp + '->' + en: {'action': 'merge'}, 'series|L|' + en + '->' + jp: {'action': 'merge'}})
    assert not any(not S.is_latin(m['target'].name) for m in merges)


def test_case_only_variant_with_locked_books_goes_to_review():
    recs = [_rec('p1', 'Pack 1', [{'name': 'The Beginning After The End', 'sequence': '1-2'}], tags={'lock_series'}),
            _rec('p2', 'Pack 2', [{'name': 'The Beginning After the End', 'sequence': '3-4'}], tags={'lock_series'})] + \
           [_rec(f'x{i}', f'B{i}', [{'name': 'The Beginning after the End', 'sequence': str(i)}]) for i in range(5, 12)]
    for r in recs: r['authors'] = ['TurtleMe']
    merges, rej = S.find_merges(recs)
    assert not merges, [(m['target'].name, [x.name for x in m['members']]) for m in merges]


def test_eta_text_uses_finished_books():
    import time as _t
    A.RUN_T0[0] = _t.time() - 100; A.DONE_TS.clear()
    for i in range(30): A.DONE_TS.append(_t.time() - 60 + 2 * i)  # one book every 2 s lately
    txt = A.eta_text(50, 150)  # 100 left at ~0.5 books/s -> a few minutes, not hours or seconds
    assert txt.endswith('s') and 'h' not in txt, txt


if __name__ == '__main__':
    for n, f in list(globals().items()):
        if n.startswith('test_'): f(); print('ok', n)
