"""Folder series next to Audible's series (user 2026-11-22): 'Camp Half-Blood Chronicles' (the folder) stayed next to 'Percy Jackson and
the Olympians' on the new volumes 3 and 4, while volumes 1, 2 and 5 in the same folder only carry Percy Jackson. A folder series is kept
only when the other volumes in the folder carry it too ('Disciples of the Horned One' in 'Soul Force Saga').
Run: python -m pytest tests/test_folder_series_only_name.py"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/folder_series_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import abs_ratings as A

L = 'lib1'


def listing(monkeypatch, books):
    monkeypatch.setattr(A, 'FOLDER_SERIES', {})
    for i, (rel, series) in enumerate(books):
        A.note_listing_item(L, {'id': f'i{i}', 'relPath': rel, 'media': {'metadata': {'title': rel.split('/')[-1], 'seriesName': series}}})


def test_percy_jackson_folder_name_goes(monkeypatch):
    d = 'Rick Riordan/Camp Half-Blood Chronicles'
    listing(monkeypatch, [(f'{d}/01 - The Lightning Thief', 'Percy Jackson and the Olympians #1'),
                          (f'{d}/02 - The Sea of Monsters', 'Percy Jackson and the Olympians #2'),
                          (f'{d}/03 - The Titans Curse', 'Camp Half-Blood Chronicles #3, Percy Jackson and the Olympians #3')])
    item = {'relPath': f'{d}/03 - The Titans Curse'}
    assert A.folder_name_only(L, item, 'Camp Half-Blood Chronicles', ['Percy Jackson and the Olympians']) is True


def test_real_sub_series_stays(monkeypatch):
    d = 'J.F. Wisher/Disciples of the Horned One'
    listing(monkeypatch, [(f'{d}/01 - Darkness Rising', 'Disciples of the Horned One #1, Soul Force Saga #1'),
                          (f'{d}/02 - Raging Sea', 'Disciples of the Horned One #2, Soul Force Saga #2'),
                          (f'{d}/03 - Harvest of Souls', 'Disciples of the Horned One #3')])
    item = {'relPath': f'{d}/03 - Harvest of Souls'}
    assert A.folder_name_only(L, item, 'Disciples of the Horned One', ['Soul Force Saga']) is False


def test_first_volume_alone_has_no_evidence(monkeypatch):
    # a whole new series at once: no other volume says anything - the folder series stays (as before)
    d = 'X/Some Saga'
    listing(monkeypatch, [(f'{d}/01 - One', 'Some Saga #1'), (f'{d}/02 - Two', 'Some Saga #2')])
    assert A.folder_name_only(L, {'relPath': f'{d}/01 - One'}, 'Some Saga', ['Audible Name']) is False


def test_other_folders_do_not_count(monkeypatch):
    listing(monkeypatch, [('Rick Riordan/Percy Jackson/01 - A', 'Percy Jackson and the Olympians #1'),
                          ('Rick Riordan/Camp Half-Blood Chronicles/03 - C', 'Camp Half-Blood Chronicles #3')])
    item = {'relPath': 'Rick Riordan/Camp Half-Blood Chronicles/03 - C'}
    assert A.folder_name_only(L, item, 'Camp Half-Blood Chronicles', ['Percy Jackson and the Olympians']) is False


def test_rule_is_used_in_both_places():
    src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'abs_ratings.py'), encoding='utf-8').read()
    assert "drop_fd = [b for b in bad_fd if folder_name_only(lib_id, item_data, b['name'], aud_names)]" in src  # book has both already
    assert "and not folder_name_only(lib_id, item_data, b['name'], [x['name'] for x in added])]" in src     # replacement (new book)
