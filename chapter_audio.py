"""Chapters on the file's own pauses (user 2026-11-25: as few books as possible where the chapters have to be set by hand).

Audible's chapter list (Audnexus) fits a file exactly only when it IS Audible's file. Other releases of the same recording are cut a
little differently (intro/outro, a few seconds of drift), so the marks land in the middle of a sentence. Here the audio is analysed:
ffmpeg's silencedetect lists the pauses of the file, and Audible's chapter starts are moved onto them. A chapter break is a long
pause (typically 2-5 s, the next chapter's first word ~0.8 s after its end); sentence pauses are shorter and far more frequent.

Safety: an alignment is only accepted when it is practically impossible by chance (binomial tail of the pause density < 1e-12),
>= 80 % of the inner chapter starts hit a pause, and no clearly different offset (> 10 s away) explains them as well. Checked on real
books (2026-11-25): files with verified marks max error 0.3 s, a synthetic +47 s shift found again, 0 false alignments in 156 pairs of
one book's chapters against another book's pauses.

The audio is only READ (read-only mount, same paths as audiobookshelf: /data/media/books/...). Pauses are cached per file
(size + mtime), a file is decoded once."""
import bisect, hashlib, json, logging, math, os, re, shutil, subprocess

FFMPEG = shutil.which('ffmpeg')
SILENCE_ARGS = 'aresample=8000,silencedetect=noise=-40dB:d=0.6'  # 8 kHz mono is enough for pauses and halves the work
FILE_TIMEOUT = int(os.getenv('CHAPTER_AUDIO_TIMEOUT', 1800) or 1800)  # seconds per audio file (a 40 h m4b takes ~2 min)
RE_GENERIC = re.compile(r'(?i)^\s*(?:(?:chapter|kapitel|track|part|teil|cd|disc|section|file|titel)\s*)?[\divxlc]+(?:\s*[-:.]\s*\d+)?\s*$|^\s*$')


def available():
    return bool(FFMPEG)


def audio_files(item_data):
    """[(path, duration)] of the book's playable audio files in play order, or None when one of them cannot be read here."""
    afs = [a for a in ((item_data or {}).get('media') or {}).get('audioFiles') or [] if not a.get('exclude')]
    afs.sort(key=lambda a: a.get('index') or 0)
    out = []
    for a in afs:
        p = (a.get('metadata') or {}).get('path')
        if not p or not os.path.isfile(p): return None
        out.append((p, float(a.get('duration') or 0)))
    return out or None


def parse_silences(lines):
    out, st = [], None
    for ln in lines:
        m = re.search(r'silence_(start|end): (-?[\d.]+)', ln)
        if not m: continue
        if m.group(1) == 'start': st = float(m.group(2))
        elif st is not None: out.append((max(0.0, st), float(m.group(2)))); st = None
    return out


def file_silences(path):
    """Pauses of ONE file via ffmpeg (low priority, audio only)."""
    cmd = [FFMPEG, '-nostdin', '-hide_banner', '-nostats', '-vn', '-sn', '-dn', '-i', path, '-af', SILENCE_ARGS, '-f', 'null', '-']
    try:
        p = subprocess.run(['nice', '-n', '15'] + cmd if shutil.which('nice') else cmd, capture_output=True, timeout=FILE_TIMEOUT)
    except (subprocess.TimeoutExpired, OSError) as e:
        logging.warning(f"        ⚠️ Pause analysis failed for {os.path.basename(path)}: {e}"); return None
    # bytes: ffmpeg prints the file's tags, which are not always UTF-8 (2026-11-26: one book stopped the whole library check)
    return parse_silences(p.stderr.decode('utf-8', errors='replace').splitlines())


def book_silences(item_data, cache_dir):
    """Pauses of the whole book on its play timeline (several files: offset by the earlier files' durations). Cached per file."""
    files = audio_files(item_data)
    if not files or not FFMPEG: return None
    os.makedirs(cache_dir, exist_ok=True)
    out, off = [], 0.0
    for path, dur in files:
        st = os.stat(path)
        ck = os.path.join(cache_dir, hashlib.sha1(f"{path}|{st.st_size}|{int(st.st_mtime)}".encode()).hexdigest() + '.json')
        sil = None
        try:
            with open(ck, encoding='utf-8') as f: sil = [tuple(x) for x in json.load(f)]
        except (OSError, ValueError): pass
        if sil is None:
            sil = file_silences(path)
            if sil is None: return None
            try:
                with open(ck + '.tmp', 'w', encoding='utf-8') as f: json.dump([[round(a, 3), round(b, 3)] for a, b in sil], f)
                os.replace(ck + '.tmp', ck)
            except OSError: pass
        out += [(a + off, b + off) for a, b in sil]
        off += dur
    return out


def _tail(n, k, p):
    """P(X >= k) for X ~ Binomial(n, p): how likely k chapter starts hit a pause by chance."""
    if k <= 0: return 1.0
    p = min(max(p, 1e-9), 1 - 1e-9)
    return min(1.0, sum(math.exp(math.lgamma(n + 1) - math.lgamma(i + 1) - math.lgamma(n - i + 1) + i * math.log(p) + (n - i) * math.log(1 - p))
                        for i in range(k, n + 1)))


def _marks(sil, min_pause):
    # where in the pause Audible's mark sits varies by book (near the end / the middle); the middle, at most 1.5 s before the end, was
    # the best compromise on 16 books with known marks (2026-11-26: mean error 1.0 s, marks > 3 s off 25 instead of 52 with 'end - 0.8')
    return sorted(max((s + e) / 2, e - 1.5) for s, e in sil if e - s >= min_pause)


def silence_align(chs, dur, sil, need=0.8, max_chance=1e-12):
    """Audible's chapter starts moved onto the file's pauses. chs = [(start, length, title)], dur = file length, sil = [(start, end)].
    Model: file time = s * (1 + r) + o (o = intro cut differently, r = a slow drift). Tried for several pause classes (all pauses /
    only the long ones) and tolerances. Returns {'starts', 'hit', 'total', 'offset', 'rate', 'chance', 'tol', 'min_pause'} or None."""
    inner = [c[0] for c in chs[1:]]
    n = len(inner)
    if n < 3 or not sil or not dur: return None
    total = chs[-1][0] + chs[-1][1]
    lens = sorted(e - s for s, e in sil)
    q = lambda f: lens[min(len(lens) - 1, int(len(lens) * f))]
    best = None
    for min_pause in sorted({0.5, 1.0, q(0.95), q(0.98), q(0.99)}):
        marks = _marks(sil, min_pause)
        if len(marks) < n * need: continue
        density = len(marks) / dur

        def score(o, r, tol):
            hit, pos, pairs = 0, [], []
            for s in inner:
                t = s * (1 + r) + o
                i = bisect.bisect_left(marks, t)
                d, j = min(((abs(marks[j] - t), j) for j in (i - 1, i) if 0 <= j < len(marks)), default=(1e9, -1))
                if d <= tol: hit += 1; pos.append(marks[j]); pairs.append((s, marks[j]))
                else: pos.append(None)
            return hit, pos, pairs

        window = abs(dur - total) + 90
        for tol in (0.5, 1.0, 2.0):
            cands = set()
            for jj in sorted(set(list(range(min(4, n))) + [n // 2, n - 1])):
                x = inner[jj]
                lo, hi = bisect.bisect_left(marks, x - window), bisect.bisect_right(marks, x + window)
                for m in marks[lo:hi]: cands.add(round(m - x, 1))
            scored = []
            for o in cands:
                hit, pos, pairs = score(o, 0.0, tol)
                r = 0.0
                if len(pairs) >= 3:  # slow drift: least-squares line through the matched pairs, then count again
                    mx = sum(a for a, _ in pairs) / len(pairs); my = sum(b for _, b in pairs) / len(pairs)
                    vx = sum((a - mx) ** 2 for a, _ in pairs)
                    if vx > 0:
                        sl = sum((a - mx) * (b - my) for a, b in pairs) / vx
                        if abs(sl - 1) <= 0.002:
                            r2, o2 = sl - 1, my - sl * mx
                            h2, p2, _ = score(o2, r2, tol)
                            if h2 > hit: hit, pos, o, r = h2, p2, o2, r2
                scored.append((hit, o, r, pos))
            if not scored: continue
            scored.sort(key=lambda z: -z[0])
            hit, o, r, pos = scored[0]
            p_rand = min(1.0, density * 2 * tol)
            chance = _tail(n, hit, p_rand)
            if hit < need * n or chance > max_chance: continue
            # another offset > 10 s away that is ALSO far beyond chance: the pauses do not pin the chapters down
            # (3-7 s apart = the two pauses around a spoken chapter title - both put the mark at the chapter)
            if any(abs(o2 - o) > 10 and h2 >= 0.5 * hit and _tail(n, h2, p_rand) <= 1e-6 for h2, o2, _, _ in scored[1:]): continue
            if best is None or chance < best['chance']:
                starts = [0.0]
                for p, s in zip(pos, inner):
                    x = p if p is not None and p > starts[-1] + 0.5 else s * (1 + r) + o  # two close Audible marks on ONE pause: model
                    starts.append(x)
                if any(b <= a for a, b in zip(starts, starts[1:])): continue
                best = {'starts': [round(x, 3) for x in starts], 'hit': hit, 'total': n, 'offset': round(o, 2), 'rate': round(r, 6),
                        'chance': chance, 'tol': tol, 'min_pause': round(min_pause, 2)}
    return best


def piecewise_align(chs, dur, sil, max_seg=4, min_seg=4, tol=1.0, need=0.8, max_chance=1e-12):
    """Like silence_align, but the offset may JUMP a few times: a release that lacks or adds a minute somewhere in the middle (The White
    Song 2026-11-25: offset 0 for 13 chapters, -112 s for 12, -100 s for 15). Dynamic programming over the chapters, one offset per
    stretch, a jump costs 4 hits. Every stretch (>= min_seg chapters) must on its own be far beyond chance - counted with the number of
    offsets it could have picked from - and all of them together < max_chance. Returns the silence_align dict (+ 'segments') or None."""
    inner = [c[0] for c in chs[1:]]
    n = len(inner)
    if n < 2 * min_seg or not sil or not dur: return None
    total = chs[-1][0] + chs[-1][1]
    lens = sorted(e - s for s, e in sil)
    q = lambda f: lens[min(len(lens) - 1, int(len(lens) * f))]
    window = abs(dur - total) + 150
    best = None
    for min_pause in sorted({1.0, q(0.98), q(0.99)}):
        marks = _marks(sil, min_pause)
        if len(marks) < n * need: continue
        density = len(marks) / dur
        p_rand = min(1.0, density * 2 * tol)
        offs = sorted({round(round((m - s) * 2) / 2, 1) for s in inner
                       for m in marks[bisect.bisect_left(marks, s - window):bisect.bisect_right(marks, s + window)]})
        if not offs: continue
        def near(t):
            i = bisect.bisect_left(marks, t)
            d, j = min(((abs(marks[j] - t), j) for j in (i - 1, i) if 0 <= j < len(marks)), default=(1e9, -1))
            return marks[j] if d <= tol else None
        hit = [[near(s + o) is not None for o in offs] for s in inner]
        PEN = 4
        dp = [[0] * len(offs) for _ in inner]; back = [[0] * len(offs) for _ in inner]
        for b in range(len(offs)): dp[0][b] = int(hit[0][b])
        for i in range(1, n):
            pb = max(range(len(offs)), key=lambda b: dp[i - 1][b])
            for b in range(len(offs)):
                stay, jump = dp[i - 1][b], dp[i - 1][pb] - PEN
                dp[i][b], back[i][b] = (stay, b) if stay >= jump else (jump, pb)
                dp[i][b] += int(hit[i][b])
        b = max(range(len(offs)), key=lambda x: dp[n - 1][x]); path = [0] * n
        for i in range(n - 1, -1, -1): path[i] = b; b = back[i][b]
        segs, st_ = [], 0
        for i in range(1, n + 1):
            if i == n or path[i] != path[st_]: segs.append((st_, i, offs[path[st_]])); st_ = i
        if len(segs) < 2 or len(segs) > max_seg or any(e - s < min_seg for s, e, _ in segs): continue
        if any(abs(o2 - o1) > abs(dur - total) + 60 for (_, _, o1), (_, _, o2) in zip(segs, segs[1:])): continue
        hits_ = sum(sum(hit[i][path[i]] for i in range(s, e)) for s, e, _ in segs)
        if hits_ < need * n: continue
        chances = [len(offs) * _tail(e - s, sum(hit[i][path[i]] for i in range(s, e)), p_rand) for s, e, _ in segs]
        prod = math.prod(min(1.0, c) for c in chances)
        if max(chances) > 1e-6 or prod > max_chance: continue
        starts = [0.0]
        for i, s in enumerate(inner):
            m = near(s + offs[path[i]])
            starts.append(m if m is not None and m > starts[-1] + 0.5 else s + offs[path[i]])
        if any(b2 <= a2 for a2, b2 in zip(starts, starts[1:])): continue
        if best is None or prod < best['chance']:
            best = {'starts': [round(x, 3) for x in starts], 'hit': hits_, 'total': n, 'offset': segs[0][2], 'rate': 0.0, 'chance': prod,
                    'tol': tol, 'min_pause': round(min_pause, 2), 'segments': [(s, e, o) for s, e, o in segs]}
    return best


def refine(chs, al, sil, tol=2.0):
    """Exact positions after an accepted alignment. It is the SAME recording, so Audible's mark sits inside the same pause of this file -
    but not always at the same depth (sometimes the middle of the pause, sometimes near its end): snapping every chapter to one fixed
    point of its pause shifted whole books by 2-4 s (check 2026-11-26, 14 books with known marks). Instead: per stretch (one offset), the
    common shift that puts the most Audible marks INSIDE their pause, the middle of that shift range; Audible's own positions are kept
    relative to each other. Returns the new starts."""
    inner = [c[0] for c in chs[1:]]
    spans = sorted((s, e) for s, e in sil if e - s >= 0.5)
    st_ = [s for s, _ in spans]
    segs = al.get('segments') or [(0, len(inner), al['offset'])]
    rate = al.get('rate') or 0.0
    model = [0.0] * len(inner)
    for a, b, o in segs:
        for i in range(a, b): model[i] = inner[i] * (1 + rate) + (o if al.get('segments') else al['offset'])
    out = list(model)
    for a, b, _ in segs:
        ranges = []
        for i in range(a, b):
            t = model[i]
            k = bisect.bisect_right(st_, t + tol) - 1
            cand = [spans[j] for j in (k - 1, k, k + 1) if 0 <= j < len(spans) and spans[j][0] - tol <= t <= spans[j][1] + tol]
            if cand:
                ps, pe = min(cand, key=lambda p: 0 if p[0] <= t <= p[1] else min(abs(p[0] - t), abs(p[1] - t)))
                ranges.append((ps - t, pe - t))
        if len(ranges) < 3: continue
        ev = sorted([(x, 1) for x, _ in ranges] + [(y, -1) for _, y in ranges], key=lambda z: (z[0], -z[1]))
        cur = bestc = 0; lo = hi = 0.0
        for j, (x, d) in enumerate(ev):
            cur += d
            if d == 1 and cur > bestc:
                bestc, lo = cur, x
                hi = next(x2 for x2, d2 in ev[j + 1:] if d2 == -1)
        if bestc < 0.6 * len(ranges): continue
        delta = (lo + hi) / 2
        for i in range(a, b): out[i] = model[i] + delta
    starts = [0.0] + out
    return [round(x, 3) for x in starts] if all(b2 > a2 for a2, b2 in zip(starts, starts[1:])) else al['starts']


def on_pause_share(abs_chs, sil, tol=1.0):
    """Share of the file's own inner chapter marks that sit at a pause (>= 1 s): real chapter marks do, split points of a
    download (every 30 min, per file) mostly do not."""
    inner = [float(c.get('start') or 0) for c in (abs_chs or [])[1:]]
    if not inner or not sil: return 0.0
    marks = _marks(sil, 1.0)
    spans = sorted((s, e) for s, e in sil if e - s >= 1.0)
    starts_ = [s for s, _ in spans]
    def ok(t):
        i = bisect.bisect_left(marks, t)
        if any(0 <= j < len(marks) and abs(marks[j] - t) <= tol for j in (i - 1, i)): return True
        k = bisect.bisect_right(starts_, t) - 1
        return k >= 0 and spans[k][0] - tol <= t <= spans[k][1] + tol  # a mark anywhere inside the pause
    return sum(1 for t in inner if ok(t)) / len(inner)


def build(chs, starts, dur):
    """ABS chapter list from Audible's titles on the aligned starts (marks past the end of the file are dropped)."""
    keep = [i for i, st in enumerate(starts) if st < dur - 1]
    starts = [starts[i] for i in keep]; titles = [chs[i][2] for i in keep]
    return [{'id': i, 'start': round(max(0.0, st), 3), 'end': round(starts[i + 1] if i + 1 < len(starts) else dur, 3), 'title': t or f"Chapter {i + 1}"}
            for i, (st, t) in enumerate(zip(starts, titles))]


def decide(abs_chs, dur, aud, sil):
    """(new chapter list or None, reason). aud = (chs, intro, outro) from Audible. Books WITHOUT usable chapters get Audible's chapters
    on the pauses. A book WITH chapters keeps them unless they do not sit at pauses (download split points) or only carry numbers -
    then Audible's (aligned) chapters are better."""
    chs = aud[0] if aud else None
    if not chs or len(chs) < 4: return None, "Audible lists too few chapters"
    al = silence_align(chs, dur, sil) or piecewise_align(chs, dur, sil)
    if not al: return None, "Audible's chapters do not line up with the pauses of the file"
    new = build(chs, al['starts'], dur)
    seg = (', offsets ' + ' / '.join(f"{o:+.0f} s" for _, _, o in al['segments'])) if al.get('segments') else f", offset {al['offset']:+.1f} s"
    why = f"{al['hit']}/{al['total']} chapter starts on the file's pauses ({seg[2:]}, chance {al['chance']:.0e})"
    if len(abs_chs or []) <= 1: return new, why
    share = on_pause_share(abs_chs, sil)
    generic = all(RE_GENERIC.match(str(c.get('title') or '')) for c in abs_chs)
    if share < 0.6:  # the own marks are split points, not chapters: Audible's chapters on the pauses are better
        return new, why + f"; the file's own {len(abs_chs)} marks: only {share:.0%} at a pause"
    if generic and all(RE_GENERIC.match(c['title'] or '') for c in new):
        return None, "the file's own marks sit at pauses and Audible's chapters have no names either"
    if generic and abs(len(abs_chs) - len(new)) <= max(1, len(new) // 10):
        # the same marks, only numbered: Audible's names on the file's OWN times (they already sit at the pauses)
        own = [float(c.get('start') or 0) for c in abs_chs]
        if all(any(abs(o - s) <= 3 for o in own) for s in al['starts'][1:]):
            out = [dict(c, title=new[min(range(len(new)), key=lambda k: abs(new[k]['start'] - float(c.get('start') or 0)))]['title']) for c in abs_chs]
            return [{'id': i, 'start': float(c.get('start') or 0), 'end': float(c.get('end') or 0), 'title': c['title']} for i, c in enumerate(out)], \
                   why + "; names only (the file's own marks are kept)"
    if generic and share < 0.85 and len(new) >= len(abs_chs):
        return new, why + f"; the file's own marks are only numbered ({share:.0%} at a pause)"
    return None, f"the file's own {len(abs_chs)} chapters sit at its pauses ({share:.0%}): kept"
