"""New books with messy file tags (2026-11-15 runs). Run: python -m pytest tests/test_new_books.py"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/new_books_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import abs_ratings as A

AUD = {'runtime_sec': 485 * 60, 'authors_raw': ['Samantha Shannon'], 'narrators_raw': ['Arty Froushan', 'Gemma Whelan']}


def test_verified_without_narrator_tag():
    assert A.verified_recording(485 * 60 + 30, [], ['Samantha Shannon'], AUD)          # author + runtime, no narrator in the tags
    assert not A.verified_recording(485 * 60, ['Someone Else'], ['Samantha Shannon'], AUD)  # a known other narrator: no
    assert not A.verified_recording(470 * 60, [], ['Samantha Shannon'], AUD)            # runtime off by 15 min
    assert A.verified_recording(485 * 60, [], ['Gemma Whelan'], AUD)                    # the author field holds the narrator


def test_new_book_title_rule():
    assert not A.new_book_title_ok('Beware of Chicken 2', 'Beware of Chicken 2: A Xianxia Cultivation Novel')  # marketing tail stays out
    assert A.new_book_title_ok('Konosuba', "Konosuba: God's Blessing on This Wonderful World!, Vol. 13")       # the volume number counts
    assert A.new_book_title_ok("Book 20 - Volume 07 - Part 06 - Memory's Flame - The Wandering Inn Series", "Memory's Flame")
    assert A.new_book_title_ok('Zero Enigma', "The King's Secret")


def test_folder_book_title():
    f = lambda p: A.folder_book_title({'relPath': p})
    assert f("Christopher G. Nuttall/The Zero Enigma/13 - The King's Secret - Saskia Maarleveld") == "The King's Secret"
    assert f("Natsume Akatsuki/Konosuba/13 - Konosuba God's Blessing on This Wonderful World!, Vol. 13 - Arnie Pantoja + Faye Mata") == \
        "Konosuba God's Blessing on This Wonderful World!, Vol. 13"
    assert f('Samantha Shannon/The Roots of Chaos/2 - A Day of Fallen Night - Ellie Kendrick + Hanako Footman') == 'A Day of Fallen Night'
    assert f('Martha Wells/The Murderbot Diaries/08 - Platform Decay') == 'Platform Decay'
    assert f('Shinobumaru/The Haunted Bookstore_ Gateway to a Parallel Universe/Volume 02 {Alexa Magro}') is None
