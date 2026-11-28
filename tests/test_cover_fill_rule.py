"""Cover rules (user 2026-11-22, 'Battle of the Labyrinth' had no cover: runtime 6.6 min off Audible's, so the strict check failed):
a book WITHOUT a cover takes the cover of every accepted ASIN (the match that is good enough for ratings and metadata);
REPLACING an existing cover keeps the strict verified-recording rule for the library; user 2026-11-28 ('1922'): a recently added book
takes the cover of its accepted ASIN ("if the book is the right one, just change it").
Run: python -m pytest tests/test_cover_fill_rule.py"""
import os

SRC = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'abs_ratings.py'), encoding='utf-8').read()


def block():
    i = SRC.index("fill_ok = bool(asin)")
    return SRC[i:SRC.index("if 'lock_cover' in tags", i)]


def test_fill_needs_only_an_accepted_asin():
    b = block()
    assert "fill_ok = bool(asin) and not aud_data.get('related_edition')" in b
    assert "(FILL_COVERS and not has_cover and fill_ok)" in b
    fill = b[b.index("(FILL_COVERS and not has_cover and fill_ok)"):]
    fill = fill[:fill.index(' or')]
    assert 'cover_verified' not in fill  # no runtime ±2 min needed when nothing is overwritten


def test_replace_keeps_the_strict_rule():
    b = block()
    assert "(cover_ok_ and (UPDATE_COVERS or id_switch) and has_cover" in b
    assert 'not lang_conflict' in b
    # strict for the library: only a RECENT book gets past cover_verified
    assert "cover_ok_ = cover_verified or bool(lib_ok_ and asin and not lang_conflict and not aud_data.get('related_edition'))" in SRC


def test_cover_block_only_runs_with_metadata_sync():
    # a skipped metadata sync (other language, runtime far off: 'Edgedancer') never reaches the cover code
    head = SRC.rfind("if aud_data and aud_data.get('meta_raw') and not skip_meta:", 0, SRC.index("fill_ok = bool(asin)"))
    assert head > 0


def test_missing_cover_is_logged():
    assert "No cover set:" in block()
