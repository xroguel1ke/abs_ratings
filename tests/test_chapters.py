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
    new, why = A.match_chapters([], 3290.0, AUD)  # 90 s more than Audible: not 'a little' any more
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


def test_chapters_ending_early_are_poor():
    # a chapter every hour (2 marks in 20 h are file parts - another rule, 2026-11-26)
    named = lambda end: [{'start': 0, 'end': 600, 'title': 'Prologue'}] + [{'start': 600 + i * 3600, 'end': min(end, 600 + (i + 1) * 3600), 'title': f'Chapter {i + 1}'}
                                                                          for i in range(int((end - 600) // 3600) + 1) if 600 + i * 3600 < end]
    assert A.chapters_poor(named(59820), 72300) == 'chapters end long before the file does'  # 997 of 1205 min
    assert A.chapters_poor(named(72000), 72300) is None  # 5 min short: rounding / credits


def test_small_difference_without_chapters_takes_audibles():
    # user 2026-11-24: 3 books by hand, ~30 s off - a file WITHOUT chapters takes Audible's as they are (like ABS's 'Find chapters')
    for dur in (3230.0, 3170.0, 3150.0):
        new, why = A.match_chapters([], dur, AUD)
        assert new and [c['start'] for c in new] == [0.0, 40.0, 1040.0, 2240.0] and new[-1]['end'] == dur and new[1]['title'] == 'Chapter 1: Start', (dur, why)
    one = [{'start': 0, 'end': 3230, 'title': 'Book'}]
    assert A.match_chapters(one, 3230.0, AUD)[0], 'a single chapter counts as none'
    own = [{'start': 0, 'end': 500, 'title': '1'}, {'start': 500, 'end': 1500, 'title': '2'}, {'start': 1500, 'end': 3230, 'title': '3'}]
    assert A.match_chapters(own, 3230.0, AUD)[0] is None, "the file's own (different) marks are not overwritten loosely"
    short = ([(0.0, 40.0, 'a'), (40.0, 3100.0, 'b'), (3140.0, 60.0, 'c')], 0, 0)
    new, why = A.match_chapters([], 3150.0, short)  # -50 s: every start lies inside the file
    assert new and [c['start'] for c in new] == [0.0, 40.0, 3140.0] and new[-1]['end'] == 3150.0, why
    new, why = A.match_chapters([], 3141.0, short)  # -59 s: the last start (3140) lies past the end of the file and is dropped
    assert new and len(new) == 2 and new[-1]["end"] == 3141.0, why
