"""A text that is Audible's own summary of the book's ASIN is no 'shared description' (2026-11-16: 19 books listed in every run)."""
import abs_series as S

TXT = 'Naofumi Iwatani, an uncharismatic otaku who spends his days on games and manga, suddenly finds himself summoned to a parallel universe to become one of four heroes who must save the world from destruction.'
norm = lambda t: ' '.join(''.join(c if c.isalnum() else ' ' for c in t.lower()).split())


def test_own_audible_text_is_kept_other_book_text_is_flagged():
    recs = [{'lib': 'L', 'id': '1', 'asin': 'A1', 'description': TXT}, {'lib': 'L', 'id': '3', 'asin': 'A3', 'description': TXT}]
    both_own = {'A1': ['Vol 1', norm(TXT)], 'A3': ['Vol 3', norm(TXT)]}
    assert S.shared_descriptions({'dry_run': True, 'summary_index': both_own}, recs) == []
    only_one = {'A1': ['Vol 1', norm(TXT)], 'A3': ['Vol 3', 'a completely different summary of volume three and its own plot']}
    assert S.shared_descriptions({'dry_run': True, 'summary_index': only_one}, recs) == ['L_3']
    assert S.shared_descriptions({'dry_run': True}, recs) == ['L_1', 'L_3']  # no index: as before
