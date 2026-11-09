"""Missing volumes of series the library has (user request 2026-11-09): only what REALLY is missing.

compare(lib_books, members) decides per series, offline (tests/test_missing_series.py). lib_books: the library books of one series
[{'num', 'title', 'asin', 'variant'}]; members: Audible's series list [{'asin', 'num' (str), 'title', 'release', 'language'}].

A volume counts as present when ANY library book of the series holds its number (another edition / recording of the same volume is
fine), or a library book has its ASIN or its title. Never listed: numberless entries, ranges and box sets ('1-3', 'Books 1-3',
'Box Set'), dramatized / full-cast / GraphicAudio editions when the library series is the plain edition, another language,
fractional numbers (novellas, 0.5 / 2.5) unless with_novellas. A volume released in the future is listed with its date."""
import re
from datetime import date

BOX_RE = re.compile(r"\b\d+\s*[-–]\s*\d+\b|\b(box\s*set|boxed set|omnibus|sammelband|gesamtausgabe|bundle|publisher.?s pack|complete (series|collection)|"
                    r"collection|trilog(y|ie))\b|\bbooks?\s+\d+\s*(?:-|–|&|and|,)\s*\d+", re.I)
EDITION_RE = re.compile(r"dramati[sz]ed|full[- ]cast|graphic\s*audio|radio (play|drama)|hörspiel|bbc radio", re.I)


def norm_title(t):
    t = re.sub(r"\([^)]*\)|\[[^]]*\]", " ", str(t or '').lower())
    return re.sub(r"[^a-z0-9äöüß]+", " ", t).strip()


def num_of(q):
    q = str(q or '').strip()
    return float(q) if re.fullmatch(r"\d+(\.\d+)?", q) else None


COMPLETE_RE = re.compile(r"\b(complete|definitive|collected|gesamtausgabe|all (the )?(novels|stories))\b", re.I)


def compare(lib_books, members, lib_variant=frozenset(), language=None, with_novellas=False, today=None):
    """-> sorted list of missing volumes [{'num', 'title', 'asin', 'release', 'upcoming'}] (one per number).
    Library omnibus editions count: a number range ('1-16.5': Galaxy Outlaws 1-16), the volume titles inside a box set title
    ('Dämonenblut / Nachtfeuer / Perlmond'), a whole-series collection ('The Definitive Collection'); 'X, Part 2' is covered by X."""
    today = today or date.today().isoformat()
    if any(COMPLETE_RE.search(b.get('title') or '') and num_of(b.get('num')) is None for b in lib_books): return []
    have_nums = {n for n in (num_of(b.get('num')) for b in lib_books) if n is not None}
    for b in lib_books:
        if m_ := re.fullmatch(r"\s*(\d+)(?:\.\d+)?\s*[-–]\s*(\d+)(?:\.\d+)?\s*", str(b.get('num') or '')):
            have_nums |= {float(i) for i in range(int(m_.group(1)), int(m_.group(2)) + 1)}
    have_asins = {b.get('asin') for b in lib_books if b.get('asin')}
    have_titles = {}
    for b in lib_books:
        if b.get('title'):
            e_ = have_titles.setdefault(norm_title(b.get('title')), [False, set()])
            e_[0] |= bool(BOX_RE.search(b.get('title') or '') or '/' in (b.get('title') or ''))
            e_[1].add(num_of(b.get('num')))
    def held(t, n=None):
        nt = norm_title(t)
        if not nt: return False
        base = re.sub(r"\s+part \d+$", "", nt)  # 'To Green Angel Tower, Part 2' = the library's 'To Green Angel Tower'
        for h, (omnibus, h_nums) in have_titles.items():
            if nt == h or base == h: return True
            # a contained / prefix title only for a library book WITHOUT another number: 'Life Reset' (#1) holds no 'Life Reset: Conquest' (#5)
            if not omnibus and not (None in h_nums or n in h_nums): continue
            # contained titles count only without another NUMBER around them ('Defiance of the Fall 6' is not the library's
            # 'Defiance of the Fall'), except inside an omnibus title ('Dämonenblut / Nachtfeuer / Perlmond ... Trilogie 1-3')
            if len(nt) >= 5 and f" {nt} " in f" {h} " and (omnibus or not re.search(r"\d", h.replace(nt, '', 1))): return True
            if len(h) >= 5 and nt.startswith(h + ' ') and not re.search(r"\d", nt[len(h):]): return True  # 'Title' = Audible's 'Title: A Novel'
        return False
    by_num = {}
    for m in members:
        n = num_of(m.get('num'))
        if n is None or (n != int(n) and not with_novellas): continue
        if BOX_RE.search(m.get('title') or '') or (EDITION_RE.search(m.get('title') or '') and not lib_variant): continue
        if language and m.get('language') and str(m['language']).lower() != str(language).lower(): continue
        if n in have_nums or m.get('asin') in have_asins or held(m.get('title'), n): by_num[n] = None; continue
        rel_ = str(m.get('release') or '')[:10]
        if n in by_num and not (by_num[n] and by_num[n]['upcoming'] and rel_ and '2100' > rel_ <= today): continue
        # first listing per number (other editions are no extra gaps); a RELEASED listing replaces an announced placeholder of the same number
        rel = str(m.get('release') or '')[:10]
        announced = rel >= '2100'  # Audible's placeholder date for an announced volume without a release date
        by_num[n] = {'num': (str(int(n)) if n == int(n) else str(n)), 'title': m.get('title') or '', 'asin': m.get('asin'),
                     'release': '' if announced else rel, 'upcoming': announced or bool(rel and rel > today)}
    return [v for k, v in sorted(by_num.items()) if v]
