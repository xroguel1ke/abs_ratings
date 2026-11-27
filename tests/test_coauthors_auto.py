"""Co-authors (user 2026-11-24: 90 review entries 'Audible lists X as co-author' - 'ich habe keine lust das alles selbst zu entscheiden').
Audible's co-authors of the same recording are added automatically; translators, illustrators, editors never. Run: python -m pytest tests/test_coauthors_auto.py"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/coauthors_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import abs_ratings as A

SRC = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'abs_ratings.py'), encoding='utf-8').read()


def test_role_names_are_recognised_loosely():
    A.ROLE_NAMES.update({'Mitz Vah', 'Christopher Tolkien'})
    assert A.is_role_name('Mitz Mitz Vah')  # Audible's spelling on 'That Time I Got Reincarnated as a Slime'
    assert A.is_role_name('Christopher Tolkien') and A.is_role_name('christopher tolkien - editor')
    assert not A.is_role_name('Janci Patterson') and not A.is_role_name('') and not A.is_role_name(None)


def test_co_author_is_no_review_entry_any_more():
    assert "as co-author of this recording (a real co-author, or a pen name" not in SRC
    assert 'co_late = (' in SRC and 'if co_late and' in SRC
    # the write comes after the Hardcover cross-check (its contributions name illustrators / translators)
    assert SRC.index('hc_learn_roles(hc_data)\n                        # user 2026-11-15: publishedYear') < SRC.index('if co_late and')
    assert "hc_role_.get(author_key_s(an))" in SRC


def test_obsolete_author_and_chapter_entries_leave_the_list():
    assert "({'authors'} if aud_data and aud_data.get('meta_raw') and not skip_meta else set())" in SRC
    assert "({'chapters'} if ch_done_ or len((item_data.get('media') or {}).get('chapters') or []) > 1 else set())" in SRC


def test_narrator_in_author_field_with_narrator_set_gets_audibles_author():
    # import 2026-11-27 (Space Team): tags 'Phil Thron' as author AND narrator, Audible: author Barry J. Hutchison, narrator Phil Thron
    assert "and set(_nk(authors)) <= set(_nk(meta.get('narrators'))) and not set(_nk(authors)) & set(_nk(a_auth)):" in SRC
    assert 'log_updates.append(f"Author field held the narrator: authors -> {a_auth}")' in SRC
    # no review entry 'Audible names other authors' once the author field was corrected
    assert "if not abs_updates.get('authors') and not match_author(authors, ', '.join(aud_auth))" in SRC
