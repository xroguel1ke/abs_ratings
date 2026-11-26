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
    assert r and abs(r['offset']) < 1.0 and r['chance'] < 1e-12


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
    own = [{'start': 0.0 if i == 0 else c[0] + 20, 'end': 0, 'title': f'My name {chr(65 + i % 26)}{i}'} for i, c in enumerate(chs)]
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


def speed_book(seed, factor=0.92, jitter=0.02, cut=1200.0):
    """Another speed (x factor) with +-jitter tempo per chapter and the last `cut` seconds missing; chapter breaks = the longest pauses."""
    chs, total, rnd = book(n=30, seed=seed)
    starts, t = [0.0], 0.0
    for s, l, _ in chs[:-1]: t += l * factor * rnd.uniform(1 - jitter, 1 + jitter); starts.append(t)
    dur = (chs[-1][0] + chs[-1][1]) * factor - cut
    sil, x = [], 5.0
    while x < dur:
        d = rnd.uniform(0.6, 1.6); sil.append((x, x + d)); x += d + rnd.uniform(3, 12)
    sil += [(st - 3.4 + 0.8, st + 0.8) for st in starts[1:] if st < dur]
    return chs, dur, sorted(sil), starts


def test_other_speed_and_missing_end_is_found_chapter_by_chapter():
    # 2026-11-26: Charon's Claw runs at 0.908 x Audible's times, Archmage lacks 28 min at the end - one offset (+-0.2 % drift) cannot fit
    chs, dur, sil, truth = speed_book(seed=21)
    assert CA.silence_align(chs, dur, sil) is None
    r = CA.prop_align(chs, dur, sil)
    assert r and r['hit'] >= 0.9 * r['total'], r
    assert max(abs(a - b) for a, b in zip(r['starts'], truth) if b < dur) < 1.0
    new, why = CA.decide([], dur, (chs, 0, 0), sil)
    assert new and 'chapter by chapter' in why and len(new) == sum(1 for b in truth if b < dur - 1), why


def test_chapter_by_chapter_never_fits_another_book():
    chs, _, _, _ = speed_book(seed=22)
    for seed in range(23, 33):
        _, dur2, sil2, _ = speed_book(seed=seed)
        assert CA.prop_align(chs, dur2, sil2) is None, seed


def test_own_marks_past_the_end_are_replaced():
    chs, dur, sil, truth = speed_book(seed=24)
    own = [{'start': c[0], 'end': c[0] + c[1], 'title': c[2]} for c in chs]  # Audible's list of the longer edition, copied as is
    new, why = CA.decide(own, dur, (chs, 0, 0), sil)
    assert new and 'after its end' in why, why


def test_file_parts_hours_apart_are_replaced():
    # 2026-11-26: The Reality Dysfunction (41 h) came with two marks 'pt.1' / 'pt.2' - the download's file parts
    chs, total, rnd = book(n=40, seed=31, dur_ch=3600.0)
    sil = pauses(chs, total, rnd)
    own = [{'start': 0.0, 'end': total / 2, 'title': 'Book_pt.1'}, {'start': total / 2, 'end': total, 'title': 'Book_pt.2'}]
    new, why = CA.decide(own, total, (chs, 0, 0), sil)
    assert new and 'file parts' in why and len(new) == 40, why


def test_same_master_without_telling_pauses_takes_audible_as_listed():
    # Snow Crash 2026-11-26: file 10 s off Audible's list, only file parts, chapter breaks without long pauses
    chs = [(i * 1500.0, 1500.0, f"Chapter {i + 1} name") for i in range(20)]
    dur = 30000.0 - 10
    sil = [(i * 37.0, i * 37.0 + 0.7) for i in range(1, 800)] + [(4500.5, 5501.5 - 1000)]  # sentence pauses only (one long one near ch 4)
    sil.sort()
    new, why = CA.loose_chapters([{'start': 0, 'end': 15000, 'title': 'pt1'}, {'start': 15000, 'end': dur, 'title': 'pt2'}], dur, (chs, 0, 0), sil)
    assert new and len(new) == 20 and 'as listed' in why, why
    assert all(abs(c['start'] - i * 1500.0) <= 3 for i, c in enumerate(new)), [c['start'] for c in new]
    # the file's own real chapters: not this rule
    own = [{'start': i * 1000.0, 'end': (i + 1) * 1000.0, 'title': f'Own {i}'} for i in range(30)]
    assert CA.loose_chapters(own, dur, (chs, 0, 0), sil)[0] is None
    # another length (another master): no
    assert CA.loose_chapters([], 30000.0 + 200, (chs, 0, 0), sil)[0] is None


def test_numbered_file_parts_are_no_chapter_names():
    mk = lambda ts: [{'start': i * 100.0, 'end': (i + 1) * 100.0, 'title': t} for i, t in enumerate(ts)]
    assert CA.numbered_parts(mk([f'And Another Thing {i}-9' for i in range(1, 10)]))
    assert CA.numbered_parts(mk([f'The Whispering Skull {i:02d}' for i in range(1, 6)]))
    assert not CA.numbered_parts(mk(['Chapter One', 'Chapter Two', 'Chapter Three']))  # words, not numbers: names
    assert not CA.numbered_parts(mk(['1', '2', '3']))  # plain numbers: RE_GENERIC's job
    assert not CA.numbered_parts(mk(['Prologue', 'Chapter 1', 'Chapter 2']))
    assert not CA.numbered_parts(mk(['Part 1 - The Storm', 'Part 2 - The Calm', 'Part 3 - The End']))


def test_numbered_parts_at_audible_starts_get_audibles_finer_list():
    random.seed(7)
    total = 15 * 1800.0
    chs = [(i * 1800.0, 1800.0, f"Named chapter {i + 1}") for i in range(15)]
    sil = sorted([(c[0] - 2.5, c[0] - 0.4) for c in chs[1:]] + [(random.uniform(10, total - 10),) * 2 for _ in range(0)] +
                 [(x, x + 0.6) for x in [random.uniform(10, total - 10) for _ in range(400)]])
    own = [{'start': chs[i][0], 'end': chs[i + 3][0] if i + 3 < 15 else total, 'title': f'Chapter {i // 3 + 1}'} for i in range(0, 15, 3)]
    new, why = CA.decide(own, total, (chs, 0, 0), sil)
    assert new and len(new) == 15 and ('finer list' in why or 'download parts' in why), why
    # download parts named by number ('And Another Thing 1-5'), cut at pauses but NOT at chapter starts: Audible's named list too
    parts = [{'start': 0.0 if i == 0 else chs[i][0] + 600.0, 'end': 0, 'title': f'And Another Thing {i // 3 + 1}-5'} for i in range(0, 15, 3)]
    sil2 = sorted(sil + [(p['start'] - 1.5, p['start'] - 0.2) for p in parts[1:]])
    new2, why2 = CA.decide(parts, total, (chs, 0, 0), sil2)
    assert new2 and len(new2) == 15 and 'download parts' in why2, why2


def test_non_utf8_ffmpeg_output_does_not_raise():
    # 2026-11-26: a tag in Latin-1 made ffmpeg's stderr undecodable and stopped the whole library check
    class P: stderr = b'title: caf\xe9\n[silencedetect @ 0x1] silence_start: 1.0\n[silencedetect @ 0x1] silence_end: 3.5 | silence_duration: 2.5\n'
    run_, ff_ = CA.subprocess.run, CA.FFMPEG
    CA.subprocess.run, CA.FFMPEG = (lambda *a, **k: P()), '/usr/bin/ffmpeg'
    try: assert CA.file_silences('/x.m4b') == [(1.0, 3.5)]
    finally: CA.subprocess.run, CA.FFMPEG = run_, ff_


def test_parse_silences():
    lines = ['[silencedetect @ 0x1] silence_start: 12.5', '[silencedetect @ 0x1] silence_end: 15.9 | silence_duration: 3.4', 'junk']
    assert CA.parse_silences(lines) == [(12.5, 15.9)]


if __name__ == '__main__':
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith('test_') and callable(fn): fn(); n += 1; print('ok', name)
    print(n, 'tests passed')
