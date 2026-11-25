"""Chapters on the file's pauses (chapter_audio.py, 2026-11-25) with synthetic pause lists. Run: python tests/test_chapter_audio.py"""
import os, random, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import chapter_audio as CA


def book(n=25, seed=1, dur_ch=1500.0):
    """Audible chapter list + a file whose pauses hold the chapter breaks (3.4 s) and many sentence pauses (0.6-1.6 s)."""
    rnd = random.Random(seed)
    lens = [dur_ch * rnd.uniform(0.6, 1.4) for _ in range(n)]
    starts, t = [], 0.0
    for l in lens: starts.append(t); t += l
    chs = [(s, l, f"Chapter {i + 1}: The {['Road', 'Fire', 'Sea', 'Wall'][i % 4]}") for i, (s, l) in enumerate(zip(starts, lens))]
    return chs, t, rnd


def pauses(chs, total, rnd, offset=0.0, rate=0.0, long_=3.4):
    sil = []
    t = 5.0
    while t < total * (1 + rate) + offset:
        d = rnd.uniform(0.6, 1.6); sil.append((t, t + d)); t += d + rnd.uniform(3, 12)
    for s, _, _ in chs[1:]:
        x = s * (1 + rate) + offset
        sil.append((x - long_ + 0.8, x + 0.8))  # the chapter's first word 0.8 s after the pause
    return sorted(sil)


def test_offset_and_drift_are_found():
    chs, total, rnd = book()
    sil = pauses(chs, total, rnd, offset=-43.0, rate=0.0004)
    r = CA.silence_align(chs, total - 113, sil)
    assert r and r['hit'] >= 0.9 * r['total'], r
    exp = [s * 1.0004 - 43.0 for s, _, _ in chs[1:]]
    assert max(abs(a - b) for a, b in zip(r['starts'][1:], exp)) < 1.0


def test_exact_file_aligns_with_tiny_error():
    chs, total, rnd = book(seed=2)
    r = CA.silence_align(chs, total, pauses(chs, total, rnd))
    assert r and abs(r['offset']) < 0.5 and r['chance'] < 1e-12


def test_other_books_pauses_never_align():
    chs, total, _ = book(seed=3)
    for seed in range(4, 14):
        chs2, total2, rnd2 = book(seed=seed)
        assert CA.silence_align(chs, total2, pauses(chs2, total2, rnd2)) is None, seed


def test_only_sentence_pauses_never_align():
    chs, total, rnd = book(seed=5)
    sil = [p for p in pauses(chs, total, rnd) if p[1] - p[0] < 2]  # the chapter breaks removed
    assert CA.silence_align(chs, total, sil) is None


def test_decide_keeps_own_marks_at_pauses_and_replaces_split_points():
    chs, total, rnd = book(seed=6)
    sil = pauses(chs, total, rnd, offset=20.0)
    aud = (chs, 2.0, 3.0)
    # no chapters: Audible's chapters on the pauses, Audible's names
    new, why = CA.decide([], total + 20, aud, sil)
    assert new and new[0]['title'].startswith('Chapter 1') and abs(new[1]['start'] - (chs[1][0] + 20)) < 1, why
    # own marks every 30 min (download split points, not at pauses) -> replaced
    split = [{'start': i * 1800.0 + 7.7, 'end': (i + 1) * 1800.0 + 7.7, 'title': f'Part {i + 1}'} for i in range(int(total // 1800))]
    split[0]['start'] = 0.0
    new2, why2 = CA.decide(split, total + 20, aud, sil)
    assert new2 and 'only' in why2, why2
    # own named chapters at the pauses -> kept
    own = [{'start': 0.0 if i == 0 else c[0] + 20, 'end': 0, 'title': f'My name {i}'} for i, c in enumerate(chs)]
    new3, why3 = CA.decide(own, total + 20, aud, sil)
    assert new3 is None and 'kept' in why3, why3


def test_offset_jump_mid_book_is_found():
    # The White Song 2026-11-25: the release lacks ~112 s after chapter 13 and has 12 s more after chapter 25
    chs, total, rnd = book(n=40, seed=7)
    sil = []
    t = 5.0
    while t < total:
        d = rnd.uniform(0.6, 1.6); sil.append((t, t + d)); t += d + rnd.uniform(3, 12)
    off = lambda i: 0.0 if i < 14 else (-112.0 if i < 26 else -100.0)
    for i, (s, _, _) in enumerate(chs[1:], start=1): x = s + off(i); sil.append((x - 3.4 + 0.8, x + 0.8))
    sil.sort()
    assert CA.silence_align(chs, total - 100, sil) is None  # one offset cannot explain it
    new, why = CA.decide([], total - 100, (chs, 0, 0), sil)
    assert new and '-112' in why and abs(new[20]['start'] - (chs[20][0] - 112)) < 1, why
    # the same jumps on ANOTHER book's pauses: never
    chs2, total2, rnd2 = book(n=40, seed=8)
    assert CA.decide([], total2, (chs2, 0, 0), sil)[0] is None


def test_parse_silences():
    lines = ['[silencedetect @ 0x1] silence_start: 12.5', '[silencedetect @ 0x1] silence_end: 15.9 | silence_duration: 3.4', 'junk']
    assert CA.parse_silences(lines) == [(12.5, 15.9)]


if __name__ == '__main__':
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith('test_') and callable(fn): fn(); n += 1; print('ok', name)
    print(n, 'tests passed')
