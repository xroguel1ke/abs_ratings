"""Chapter matching (Audible chapters for poorly chaptered files). Run: python -m pytest tests/test_chapters.py"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/chapters_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import abs_ratings as A

AUD = ([(0.0, 40.0, 'Opening Credits'), (40.0, 1000.0, 'Chapter 1: Start'), (1040.0, 1200.0, 'Chapter 2: Middle'), (2240.0, 960.0, 'End Credits')], 4.0, 5.0)


def test_poor_detection():
    assert A.chapters_poor([], 3200) == 'no chapters'
    assert A.chapters_poor([{'start': 0, 'end': 3200, 'title': 'Book'}], 3200) == 'a single chapter'
    assert A.chapters_poor([{'start': 0, 'end': 100, 'title': 'Chapter 1'}, {'start': 100, 'end': 200, 'title': 'Track 02'}], 200)
    assert A.chapters_poor([{'start': 0, 'end': 100, 'title': 'Prologue'}, {'start': 100, 'end': 200, 'title': 'Chapter 1'}], 200) is None


def test_exact_length():
    new, why = A.match_chapters([], 3202.0, AUD)
    assert new and len(new) == 4 and new[1]['start'] == 40.0 and new[-1]['end'] == 3202.0 and new[2]['title'] == 'Chapter 2: Middle', why


def test_own_marks_with_offset():
    marks = [{'start': 0, 'end': 10, 'title': '1'}, {'start': 10, 'end': 1010, 'title': '2'}, {'start': 1010, 'end': 2210, 'title': '3'}, {'start': 2210, 'end': 3110, 'title': '4'}]
    new, why = A.match_chapters(marks, 3110.0, AUD)
    assert new and [c['start'] for c in new] == [0, 10, 1010, 2210] and new[1]['title'] == 'Chapter 1: Start', why


def test_intro_cut():
    new, why = A.match_chapters([], 3196.0, AUD)  # 4 s intro missing
    assert new and abs(new[1]['start'] - 36.0) < 0.01, why


def test_unexplained_difference_is_left_alone():
    new, why = A.match_chapters([], 3260.0, AUD)
    assert new is None
    new, why = A.match_chapters([], 3500.0, AUD)
    assert new is None


def test_anchor_marks_past_the_end():
    # marks made for the longer Audible file (run 30 s past the end) but sitting exactly at Audible's starts
    marks = [{'start': 0, 'end': 40, 'title': '1'}, {'start': 40, 'end': 1040, 'title': '2'}, {'start': 1040, 'end': 2240, 'title': '3'}, {'start': 2240, 'end': 3200, 'title': '4'}]
    new, why = A.match_chapters(marks, 3170.0, AUD)
    assert new and [round(c['start']) for c in new] == [0, 40, 1040, 2240] and new[-1]['end'] == 3170.0 and new[1]['title'] == 'Chapter 1: Start', why


def test_anchor_needs_matching_marks():
    marks = [{'start': 0, 'end': 500, 'title': '1'}, {'start': 500, 'end': 1500, 'title': '2'}, {'start': 1500, 'end': 3170, 'title': '3'}]
    new, why = A.match_chapters(marks, 3170.0, AUD)
    assert new is None, why
