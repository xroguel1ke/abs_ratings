"""Title cleanup helpers. Run: python -m pytest tests/test_titles.py"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/titles_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import abs_ratings as A


def test_glued_subtitle():
    g = A.glued_subtitle
    # subtitle glued into the title and also in the subtitle field (library 2026-11-09)
    assert g('Sojourn: Legend of Drizzt: Dark Elf Trilogy, Book 3', 'Legend of Drizzt: Dark Elf Trilogy, Book 3', 'Sojourn', 'Legend of Drizzt: Dark Elf Trilogy, Book 3') == 'title'
    assert g('Defiance of the Fall 4: A LitRPG Adventure: A LitRPG Adventure', 'A LitRPG Adventure', 'Defiance of the Fall 4', 'A LitRPG Adventure') == 'title'
    # the subtitle field names a series the book is not in: replaced too
    assert g('Moreta: Dragonlady of Pern: Dragonriders of Pern, Book 7', 'Victor of Tucson, Book 4', 'Moreta: Dragonlady of Pern', 'Dragonriders of Pern, Book 7', ['Dragonriders of Pern']) == 'subtitle'
    assert g('X: Y: Y', None, 'X', 'Y') == 'subtitle'
    assert g("Legend of the Arch Magus: Publisher's Pack: Legend of the Arch Magus, Book 1, 2", None, "Legend of the Arch Magus: Publisher's Pack",
             'Legend of the Arch Magus, Book 1, 2', ['Legend of the Arch Magus']) == 'subtitle'
    # normal titles stay
    assert g('Mistborn: The Final Empire', None, 'Mistborn', 'The Final Empire') is None
    assert g('Mistborn: The Final Empire', 'My own', 'Mistborn', 'The Final Empire') is None
    assert g('Mistborn', 'The Final Empire', 'Mistborn', 'The Final Empire') is None


def test_search_score_title_with_audible_subtitle():
    s = A.search_match_score
    assert s('Homeland: Legend of Drizzt', 'Homeland', 'Legend of Drizzt: Dark Elf Trilogy, Book 1') >= 0.85
    assert s('Dune: Messiah', 'Dune', 'Dune Chronicles, Book 1') < 0.85
    assert s('Homeland: Legend', 'Homeland', 'Legend of Drizzt') < 0.85  # one tail word is too little


def test_series_name_appended_to_title():
    h = A.series_tail_head
    assert h('Hogfather: Discworld', ['Discworld'], 'Hogfather') == 'Hogfather'
    assert h('Mark of the Crijik 4: The Burning Lake: Mark of the Crijik', ['Mark of the Crijik'], 'Mark of the Crijik 4: The Burning Lake') == 'Mark of the Crijik 4: The Burning Lake'
    assert h('Casual Farming 2: Sowing Season', ['Sowing Season'], 'Casual Farming 2: A Slow Living LitRPG') == 'Casual Farming 2'
    assert h('Dune: Messiah', ['Dune'], 'Dune Messiah') is None  # the tail is not the series name
    assert h('Discworld', ['Discworld'], 'Discworld') is None


def test_title_series_tail():
    # 2026-11-28: Audible's own title carries the series label ('Shaman's Crossing, Book One of the Soldier Son Trilogy') - the series field holds it
    f, S = A.title_series_tail, [{'name': 'Soldier Son Trilogy', 'sequence': '1'}]
    assert f("Shaman's Crossing, Book One of the Soldier Son Trilogy", S) == "Shaman's Crossing"
    assert f("Shaman's Crossing: Soldier Son Trilogy, Book 1", S) == "Shaman's Crossing"
    assert f("Shaman's Crossing (Soldier Son Trilogy #1)", S) == "Shaman's Crossing"
    assert f("Shaman's Crossing - Book 1 of the Soldier Son Trilogy", S) == "Shaman's Crossing"
    assert f('The Eye of the World: Book One of The Wheel of Time', [{'name': 'The Wheel of Time', 'sequence': '1'}]) == 'The Eye of the World'
    assert f('Sojourn: Legend of Drizzt: Dark Elf Trilogy, Book 3', [{'name': 'Legend of Drizzt', 'sequence': '6'}, {'name': 'Dark Elf Trilogy', 'sequence': '3'}]) == 'Sojourn'
    assert f('Moreta: Dragonlady of Pern: Dragonriders of Pern, Book 7', [{'name': 'Dragonriders of Pern', 'sequence': '7'}]) == 'Moreta: Dragonlady of Pern'
    # another volume's number, only the series name left, no series named, normal titles: kept
    assert f("Shaman's Crossing, Book Two of the Soldier Son Trilogy", S) is None
    assert f('Soldier Son Trilogy, Book 1', S) is None
    assert f('Defiance of the Fall, Book 10', [{'name': 'Defiance of the Fall', 'sequence': '10'}]) is None
    assert f('Mushoku Tensei: Jobless Reincarnation, Vol. 5', [{'name': 'Mushoku Tensei: Jobless Reincarnation', 'sequence': '5'}]) is None
    assert f('Choose Your Enemies: Ciaphas Cain: Warhammer 40,000, Book 10', [{'name': 'Ciaphas Cain', 'sequence': '10'}]) is None
    assert f('The Murderbot Diaries 1: All Systems Red', [{'name': 'The Murderbot Diaries', 'sequence': '1'}]) is None
    assert f('Mistborn: The Final Empire', [{'name': 'Mistborn', 'sequence': '1'}]) is None
    assert f("Shaman's Crossing", S) is None and f("Shaman's Crossing, Book One of the Soldier Son Trilogy", []) is None


def test_title_series_tail_only_recent_books():
    src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'abs_ratings.py')).read()
    assert 'if recent_ and new_t and (lbl_t := title_series_tail(new_t, ser_now)): new_t = lbl_t' in src


def test_cover_of_the_accepted_asin_for_recent_books():
    # user 2026-11-28 ('1922': metadata synced, the old cover stayed): a recently added book takes the cover of its accepted ASIN
    src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'abs_ratings.py')).read()
    assert "cover_ok_ = cover_verified or bool(recent_ and asin and not lang_conflict and not aud_data.get('related_edition'))" in src
    assert '(cover_ok_ and (UPDATE_COVERS or id_switch) and has_cover and COVER_STATE.get(iid) != cover_token(asin))' in src
