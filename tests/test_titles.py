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
