"""
ABS-only maintenance task (no Audible/Goodreads requests):

1. Series merge: finds series that are really the same series (Audible often uses different
   names within one series) and merges the smaller one into the series that had more entries.
   The merge is remembered in series_aliases.json, so the Audible sync maps the old name to the
   merged series from then on (no lock tag needed). series_index.json lists all existing series,
   so new books join an existing series instead of creating a split one.
1b. Author spellings: 'R. A. Salvatore' / 'R.A. Salvatore' are unified to the spelling used by
   more books (per book, so it can be undone).
2. Library report: reports/library_report.html (+ .json) with series gaps, duplicates,
   missing fields, author spelling variants, unmerged similar series ...

Called from abs_ratings.py at the end of a run (or alone with SERIES_ONLY=true).
All writes go through ctx['abs_update'], so they are journaled and respect DRY_RUN.
"""
import re, json, html, logging, difflib, os, unicodedata
from collections import defaultdict
from datetime import datetime

# Markers that make a series a different "edition" (never merged with the plain series)
VARIANT_MARKERS = {
    'fullcast': [r'full[\s-]*cast'],
    'dramatized': [r'dramati[sz](ed|ation)', r'audio\s*drama', r'radio\s*drama', r'h(ö|oe)rspiel'],
    'graphicaudio': [r'graphic\s*audio'],
    'abridged': [r'\babridged\b', r'gek(ü|ue)rzt'],
    'manga': [r'\bmanga\b', r'\bcomics?\b', r'graphic\s*novel'],
}
GENERIC_WORDS = {'series', 'saga', 'reihe', 'serie', 'trilogy', 'trilogie', 'cycle', 'zyklus', 'sequence',
                 'novels', 'novel', 'books', 'book', 'romane', 'roman', 'collection', 'edition', 'editions'}
LEADING_ARTICLES = {'the', 'a', 'an', 'der', 'die', 'das', 'le', 'la'}
# Format labels that don't make a different series ('Hell Mode' = 'Hell Mode Light Novel')
FORMAT_PHRASES = [r'light\s*novels?', r'\bunabridged\b', r'\bungek(ü|ue)rzt\b', r'\baudiobooks?\b', r'\bh(ö|oe)rbuch\b']
FUZZY_NAME_RATIO = 0.9


METADATA_LOCKS = {'lock_title', 'lock_series', 'lock_language', 'lock_genres', 'lock_publisher', 'lock_year', 'lock_narrators',
                  'lock_authors', 'lock_cover', 'lock_chapters', 'lock_explicit', 'lock_abridged', 'lock_isbn', 'lock_asin'}


def expand_locks(tags):
    """'lock_metadata' = every metadata lock (ratings are still written)."""
    return tags | METADATA_LOCKS if 'lock_metadata' in tags else tags


def series_variant(name):
    n = name.lower()
    return frozenset(k for k, pats in VARIANT_MARKERS.items() if any(re.search(p, n) for p in pats))


def series_key(name):
    """Normalized series name: no edition markers, punctuation, leading article or generic words."""
    n = name.lower()
    for pats in VARIANT_MARKERS.values():
        for p in pats: n = re.sub(p, ' ', n)
    for p in FORMAT_PHRASES: n = re.sub(p, ' ', n)
    n = n.replace('&', ' and ')  # 'Memory, Sorrow & Thorn' = 'Memory, Sorrow, and Thorn'
    n = re.sub(r'[\(\)\[\]]', ' ', n)
    n = re.sub(r'[^\w\s]', ' ', n)
    words = n.split()
    while words and words[0] in LEADING_ARTICLES: words = words[1:]
    while words and words[-1] in GENERIC_WORDS: words = words[:-1]
    return ' '.join(words)


def parse_seq(seq):
    """'3' -> 3.0, '2.5' -> 2.5, '1-3' / 'Book 4' -> first number, None if no number."""
    if seq is None: return None
    m = re.search(r'\d+(?:\.\d+)?', str(seq))
    return float(m.group()) if m else None


def fmt_seqs(nums):
    """[1,2,3,5,6,7] -> '1-3, 5-7'"""
    nums = sorted(set(nums))
    if not nums: return '-'
    out, start, prev = [], nums[0], nums[0]
    for x in nums[1:] + [None]:
        if x is not None and x == prev + 1 and float(x).is_integer() and float(prev).is_integer():
            prev = x; continue
        out.append(f"{_n(start)}-{_n(prev)}" if start != prev else _n(start))
        if x is not None: start = prev = x
    return ', '.join(out)


def _n(x): return str(int(x)) if float(x).is_integer() else str(x)


# Role credits that are not authors ('Kevin Steinbach - translator', 'X - Illustrator', 'Y - Übersetzer'): Audible lists them in the authors list
ROLE_CREDIT_RE = re.compile(r'\s+[-–—]\s+(?:\S.*?\s+[-–—]\s+)?(other|illustrat\w*|illustrations?|translat\w*|translated by|edited by|editor|editors|introductions?|foreword|afterword|preface|contributor|adapt\w*|cover\s+\w+|'
                            r'übersetz\w*|übertragen\w*|herausgeb\w*|hrsg\.?|zeichn\w*|illustriert\w*|vorwort|nachwort|bearbeit\w*)\s*$', re.I)


def is_role_credit(name): return bool(name and ROLE_CREDIT_RE.search(name))


def fold_accents(s):
    """'Kurmaić' -> 'Kurmaic'"""
    return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))

def is_latin(name):
    return not any(c.isalpha() and "LATIN" not in unicodedata.name(c, "") for c in (name or ""))  # letters only: "86—EIGHTY-SIX" (em dash) is Latin

FOREIGN_WORDS = {'les', 'le', 'la', 'du', 'aux', 'et', 'une', 'un', 'el', 'los', 'las', 'del', 'y', 'il', 'gli', 'degli', 'della', 'dei', 'delle', 'di', 'os', 'dos', 'het', 'een',
                 'de', 'des', 'livre', 'tome', 'chroniques', 'cronache', 'crónicas', 'cronicas', 'ciclo', 'cycle de', 'saga de', 'trilogie', 'trilogia', 'kronikka', 'och', 'av'}
ENDE_WORDS = {'the', 'of', 'and', 'a', 'an', 'der', 'die', 'das', 'und', 'von', 'vom', 'im', 'zum', 'zur', 'ein', 'eine', 'in', 'to', 'for'}

GERMAN_WORDS = {'der', 'die', 'das', 'dem', 'den', 'des', 'und', 'von', 'vom', 'zum', 'zur', 'im', 'unter', 'einer', 'eines', 'zweiten', 'vergessenen', 'chroniken'}

def foreign_for_book(name, book_lang):
    """Series name that does not fit the BOOK's language: not English/German at all (foreign_series_name, Nordic letters å ø æ), or a
    German name on an English book ('Das Spiel der Götter' on Gardens of the Moon). An English name on a German book is fine."""
    if foreign_series_name(name) or re.search(r'[åøæÅØÆ]', name or ''): return True
    lang = str(book_lang or '').strip().lower()
    if lang in ('english', 'eng', 'en'):
        w = re.findall(r"[a-zäöüß]+", (name or '').lower())
        strong = GERMAN_WORDS - {'die', 'den', 'des', 'im'}  # 'Die Hard', 'Den of Thieves' are English
        return (any(x in strong for x in w) or sum(x in GERMAN_WORDS for x in w) >= 2) and not any(x in ('the', 'of', 'and', 'a', 'an') for x in w)
    return False

def foreign_series_name(name):
    """A series name that is neither English nor German: non-Latin script ('四畳半シリーズ') or two or more French/Spanish/Italian/...
    function words and none of English/German ('Les Annales de la Compagnie Noire' on English Black Company books)."""
    if not is_latin(name): return True
    w = re.findall(r"[a-zà-ÿ]+", (name or '').lower())
    f, e = sum(x in FOREIGN_WORDS for x in w), sum(x in ENDE_WORDS for x in w)
    if re.search(r"(?:^|\s)[ld]['’][a-zà-ÿ]", (name or '').lower()): f += 2  # French elision: "L'Assassin royal", "d'Arkadie"
    return f >= 2 and f > e  # 'Il Ciclo di The Cosmere' (2 vs 1) is foreign; 'The Fall of La Ciudad' is not

def distinct_works(titles):
    """How many different WORKS a set of normalized titles names: a title contained in another is the same work
    ('mistborn', 'the final empire', 'mistborn the final empire' = 1)."""
    groups = []
    for t in sorted({t for t in titles if t}, key=len, reverse=True):
        if not any(t in g or g in t for g in groups): groups.append(t)
    return len(groups)

LATIN_LANGS = {'english', 'eng', 'en', 'german', 'deu', 'ger', 'de', 'deutsch', '', None}
def latin_books(g):
    """The books of a merge group are English/German (or have no language): their series names must be Latin."""
    return all((str(r.get('language') or '').strip().lower() or None) in LATIN_LANGS for s in g for r, _ in s.entries)

def plausible_same_person(a, b):
    """Could two author names be ONE person written differently? Same surname, the same name parts in another order
    ('Baba Okina'), a near-identical spelling, or one of them in a non-Latin script (romanisation). A translator and the
    author ('Caitlin Kelly' / 'Okina Baba') are not: an Audible author record seen behind both is no proof then."""
    if not a or not b: return False
    if not is_latin(a) or not is_latin(b): return True
    ta = [t for t in re.sub(r'[^a-z ]', ' ', fold_accents(a).lower()).split() if len(t) > 1]
    tb = [t for t in re.sub(r'[^a-z ]', ' ', fold_accents(b).lower()).split() if len(t) > 1]
    if not ta or not tb: return False
    if ta[-1] == tb[-1] or set(ta) == set(tb): return True
    return difflib.SequenceMatcher(None, ' '.join(ta), ' '.join(tb)).ratio() >= 0.85

def author_key(name):
    """'Chambers, Becky' / 'Becky Chambers' / 'J.M. Clarke' / 'JM Clarke' / 'Kurmaić' -> comparable key (accents ignored)."""
    n = fold_accents(name).lower().strip()
    if ',' in n:
        last, first = [x.strip() for x in n.split(',', 1)]
        n = f"{first} {last}"
    n = re.sub(r'\b([a-z])\.\s*(?=[a-z]\b)', r'\1', n)  # J. R. R. / J.R.R. -> jrr (initials joined)
    n = re.sub(r'\b([a-z])\.\s*', r'\1 ', n)              # ... but kept apart from the last name
    return ' '.join(re.sub(r'[^\w\s]', ' ', n).split())


class Series:
    def __init__(self, name):
        self.name = name
        self.key = series_key(name)
        self.variant = series_variant(name)
        self.entries = []  # (record, seq_string)

    @property
    def seqs(self): return [s for s in (parse_seq(q) for _, q in self.entries) if s is not None]

    @property
    def item_ids(self): return {r['id'] for r, _ in self.entries}

    @property
    def authors(self): return {author_key(a) for r, _ in self.entries for a in r['authors']}


def name_relation(a, b):
    """How strongly two series names match: 'exact' (same after removing articles, generic words like
    'Series'/'Saga' and format labels like 'Light Novel') or 'similar' (spelling variant, e.g.
    'Realm'/'Realms'). A name that merely contains the other ('Dune' / 'Dune, les origines',
    'Discworld' / 'Discworld: Witches') is a sub-series and never matches."""
    if not a.key or not b.key: return None
    if a.key == b.key: return 'exact'
    # Similar: only spelling differences, same number of words (no extra word = no sub-series)
    if len(a.key.split()) == len(b.key.split()) and \
            difflib.SequenceMatcher(None, a.key, b.key).ratio() >= FUZZY_NAME_RATIO: return 'similar'
    return None


LOOSE_NAME_RATIO = 0.8  # names this similar (or one inside the other) merge only with interleaving book numbers


def loosely_similar(a, b):
    """'spelling' = names only loosely alike (ratio >= 0.8), 'contained' = all words of one inside the other (a sub-series like
    'Discworld: Witches' / 'Discworld' - normally never merged), None = not similar."""
    if not a.key or not b.key: return None
    wa, wb = set(a.key.split()), set(b.key.split())
    if difflib.SequenceMatcher(None, a.key, b.key).ratio() >= LOOSE_NAME_RATIO: return 'spelling'
    if (wa <= wb or wb <= wa) and max(len(w) for w in (wa & wb or {''})) >= 4: return 'contained'
    return None


def interleaved(g_seqs, c_seqs):
    """Strong evidence for one series (user rule 2026-11-04): both sides have >= 2 numbers, together they form ONE gapless run, and the
    sides alternate at least twice (1-3,7-8,10 | 4-6,9,11-13). Whole numbers only."""
    g = {int(x) for x in g_seqs if float(x).is_integer()}; c = {int(x) for x in c_seqs if float(x).is_integer()}
    if len(g) < 2 or len(c) < 2 or g & c: return False
    u = sorted(g | c)
    if u[-1] - u[0] + 1 != len(u): return False
    side = ['g' if x in g else 'c' for x in u]
    return sum(1 for i in range(1, len(side)) if side[i] != side[i - 1]) >= 2


UNCERTAIN = ('not enough numbered books', "numbers don't complement", 'matches more than one series', "numbers don't interleave", 'contains book(s) with lock_series')  # -> review list

def series_info(s):
    """Titles (with number) and authors of a Series object, for the web UI."""
    return {'titles': [f"#{q} {r['title']}" if q else r['title'] for r, q in sorted(s.entries, key=lambda e: parse_seq(e[1]) or 0)][:15],
            'authors': sorted({a for r, _ in s.entries for a in r['authors']})[:4]}


def pair_info(a, b):
    ia, ib = series_info(a), series_info(b)
    return {'series_books': fmt_seqs(a.seqs), 'other_books': fmt_seqs(b.seqs), 'series_titles': ia['titles'], 'other_titles': ib['titles'],
            'series_authors': ia['authors'], 'other_authors': ib['authors']}


def merge_id(kind, lib, member, target): return f"{kind}|{lib}|{member}->{target}"

def foreign_series_books(records, decisions=None):
    """Review list 'Other': a book in a series (>= 3 books) whose authors share NOBODY with the other books, while those all share the
    series' main author ('Bury Our Bones in the Midnight Soil' (Schwab) in 'The Age of Madness' (Abercrombie)). Often a wrong series - or
    the narrator stands as author. Nothing is changed automatically; you decide (remove the series from the book / keep)."""
    decisions = decisions or {}
    by = defaultdict(list)
    for r in records:
        for s in r['series']: by[(r['lib'], s['name'])].append((r, s.get('sequence')))
    out = []
    for (lib, name), books in by.items():
        if len(books) < 3: continue
        cnt = defaultdict(int)
        for r, _ in books:
            for a in {author_key(x) for x in r['authors']}: cnt[a] += 1
        for r, seq in books:
            mine = {author_key(x) for x in r['authors']}
            others = [b for b, _ in books if b is not r]
            if not mine or any(mine & {author_key(x) for x in b['authors']} for b in others): continue
            if not all(any(cnt[author_key(x)] >= 0.8 * len(others) for x in b['authors']) for b in others): continue  # the rest has ONE main author
            main = max((a for b in others for a in b['authors']), key=lambda a: cnt[author_key(a)])
            mid = f"other|{lib}|{r['id']}|{name}"
            if decisions.get(mid, {}).get('action') in ('keep', 'removed'): continue
            reason = ('the book\'s authors look like its narrators' if mine <= {author_key(x) for x in r.get('narrators') or []} else 'book of another author in this series')
            out.append({'id': mid, 'kind': 'other', 'lib': lib, 'key': f"{lib}_{r['id']}", 'item': r['id'], 'title': r['title'], 'series': name, 'sequence': seq,
                        'authors': r['authors'], 'main_author': main, 'reason': reason, 'series_books': len(books)})
    return sorted(out, key=lambda x: (x['series'].lower(), x['title'].lower()))

def merge_check(group, cand, decisions=None):
    """Can 'cand' be merged into 'group' (list of Series, first = target)? Returns (ok, relation, reason).
    Your decisions from the web UI (merge / keep separate / undone) win over the rules."""
    target = group[0]
    rel = name_relation(target, cand)
    dec = (decisions or {}).get(merge_id('series', getattr(cand, 'lib', ''), cand.name, target.name), {}).get('action')
    if dec in ('keep', 'undone'): return False, rel or 'manual', 'kept separate (your decision)'
    if dec == 'merge': return True, 'your decision', 'ok'
    loose = False
    if not rel:
        loose = loosely_similar(target, cand)
        if not loose: return False, None, 'names not similar'
        rel = 'similar (numbers interleave)'
    if target.variant != cand.variant:
        return False, rel, f"different edition ({', '.join(sorted(target.variant)) or 'plain'} vs {', '.join(sorted(cand.variant)) or 'plain'})"
    g_authors = set().union(*(s.authors for s in group))
    if not (g_authors & cand.authors): return False, rel, 'no shared author'
    # A manually locked book means the user decided on this series name: don't touch the series
    # (on EITHER side: a book you locked in the target series also means 'don't touch these names')
    # User rule 2026-11-08: lock_series protects the BOOK. If all locked books sit in ONE series of the group, that series becomes
    # the target and only unlocked books move (find_merges swaps the target). Locked books in two or more series -> review list.
    with_locks = [s for s in group + [cand] if any('lock_series' in r['tags'] for r, _ in s.entries)]
    locked = [r['title'] for s in with_locks for r, _ in s.entries if 'lock_series' in r['tags']] if len(with_locks) > 1 else []
    lock_msg = f"contains book(s) with lock_series: {', '.join(locked[:3])}" if locked else ''  # only a merge that is OTHERWISE allowed becomes a review case
    # Every single book of the smaller series must share an author with the target
    foreign = [r['title'] for r, _ in cand.entries if not ({author_key(a) for a in r['authors']} & g_authors)]
    if foreign: return False, rel, f"book(s) by other authors: {', '.join(foreign[:3])}"
    # Books that are in both series (duplicate entries) don't count as conflicts
    g_ids = set().union(*(s.item_ids for s in group))
    g_seqs = {parse_seq(q) for s in group for r, q in s.entries if parse_seq(q) is not None and r['id'] not in cand.item_ids}
    c_seqs = {parse_seq(q) for r, q in cand.entries if parse_seq(q) is not None and r['id'] not in g_ids}
    if overlap := g_seqs & c_seqs:
        return False, rel, f"book numbers overlap ({fmt_seqs(overlap)})"
    if loose:  # only loosely similar names: the numbers must interleave into one gapless run, otherwise it goes to the review list
        if not interleaved(g_seqs, c_seqs):
            # a sub-series (name inside the other) or a side without own numbers is no doubt case, it simply stays separate;
            # only spelling variants with numbers on both sides go to the review list
            if loose == 'contained' or len(g_seqs) < 2 or len(c_seqs) < 2:
                return False, None, f"sub-series or no number evidence ({fmt_seqs(g_seqs)} vs {fmt_seqs(c_seqs)})"
            return False, rel, f"numbers don't interleave ({fmt_seqs(g_seqs)} vs {fmt_seqs(c_seqs)}) and the names are only loosely similar"
        return (False, rel, lock_msg) if locked else (True, rel, 'ok')
    if rel != 'exact':
        # Fuzzy names need the book numbers to complement each other (fill gaps or continue the series)
        if not c_seqs or not g_seqs: return False, rel, 'not enough numbered books to prove it is the same series'
        lo, hi = min(g_seqs), max(g_seqs)
        fills = any(lo < x < hi or x == hi + 1 or x == lo - 1 for x in c_seqs)
        if not fills: return False, rel, f"numbers don't complement ({fmt_seqs(g_seqs)} vs {fmt_seqs(c_seqs)})"
    return (False, rel, lock_msg) if locked else (True, rel, 'ok')


def collect_records(ctx):
    """Load every book of the configured libraries with full metadata (ABS only)."""
    sess, url = ctx['session'], ctx['abs_url']
    records = []
    for lib in ctx['library_ids']:
        try: items = sess.get(f"{url}/api/libraries/{lib}/items").json()['results']
        except Exception as e:
            logging.error(f"Series task: cannot load library {lib}: {e}"); continue
        logging.info(f"   Loading {len(items)} items of library {lib} ...")
        # speed 2026-11-15: the full items are fetched in parallel (one at a time took ~1 min for 2000 books); order kept
        from concurrent.futures import ThreadPoolExecutor
        def _get(it):
            try: return it, sess.get(f"{url}/api/items/{it['id']}").json(), None
            except Exception as e: return it, None, e
        with ThreadPoolExecutor(max_workers=int(ctx.get('load_workers') or 3)) as ex_:
            loaded = list(ex_.map(_get, items))
        for it, d, err in loaded:
            if err is not None or not isinstance(d, dict):
                logging.warning(f"   Could not load item {it.get('id')}: {err}"); continue
            media = d.get('media') or {}
            md = media.get('metadata') or {}
            if d.get('mediaType', 'book') != 'book': continue
            raw_tags = (d.get('tags') or []) + (media.get('tags') or []) + (md.get('tags') or [])
            records.append({
                'id': d['id'], 'lib': lib, 'title': md.get('title') or '?', 'path': d.get('relPath') or '',
                'authors': [a.get('name') if isinstance(a, dict) else a for a in (md.get('authors') or []) if a],
                'narrators': md.get('narrators') or [], 'asin': md.get('asin'), 'isbn': md.get('isbn'), 'publisher': md.get('publisher'), 'year': md.get('publishedYear'), 'subtitle': md.get('subtitle'),
                'series': [{'name': s.get('name'), 'sequence': s.get('sequence')} for s in (md.get('series') or []) if s.get('name')],
                'media_tags': list(media.get('tags') or []), 'tags': expand_locks({t.strip() for t in raw_tags if isinstance(t, str)}),
                'description': md.get('description') or '', 'language': md.get('language'), 'genres': md.get('genres') or [],
                'cover': media.get('coverPath'), 'duration': media.get('duration'), 'added': d.get('addedAt'),
            })
    return records


def _make_target(g, new):
    """Make 'new' the merge target of group g. The old target becomes a member and takes over the relation of the new one
    (the relation is symmetric). Without this a swapped target had no .relation and the whole series task crashed (2026-11-07)."""
    old = g[0]
    g.remove(new); g.insert(0, new)
    old.relation = getattr(new, 'relation', None) or 'name'

def find_merges(records, decisions=None, preferred_names=None):
    """Returns (merges, rejected). merges: list of dicts {lib, target, members:[Series], evidence}.
    preferred_names: exact series titles used by Audible; if exactly one name of a merge group is one of them, it becomes the target
    (instead of simply the largest group's name)."""
    merges, rejected = [], []
    by_lib = defaultdict(dict)
    for r in records:
        for s in r['series']:
            ser = by_lib[r['lib']].setdefault(s['name'], Series(s['name']))
            ser.lib = r['lib']
            ser.entries.append((r, s.get('sequence')))
    for lib, series_map in by_lib.items():
        # Largest series first: they become merge targets ("the series that had more entries")
        ordered = sorted(series_map.values(), key=lambda s: (-len(s.entries), s.name.lower()))
        groups = []  # list of [target, member, member ...]
        for ser in ordered:
            fits, related = [], []
            for g in groups:
                ok, rel, reason = merge_check(g, ser, decisions)
                if ok: fits.append((g, rel))
                elif rel: related.append((g[0], reason))
            if len(fits) > 1 and any(r == 'your decision' for _, r in fits):  # your choice resolves ambiguity
                fits = [f for f in fits if f[1] == 'your decision'][:1]
            if len(fits) == 1:
                fits[0][0].append(ser)
                ser.relation = fits[0][1]
            else:
                if len(fits) > 1:
                    for g, _ in fits:  # one review entry per possible target
                        rejected.append({'lib': lib, 'series': ser.name, 'other': g[0].name, 'reason': 'matches more than one series (ambiguous)',
                                         **pair_info(ser, g[0])})
                for tgt, reason in related:
                    rejected.append({'lib': lib, 'series': ser.name, 'other': tgt.name, 'reason': reason, **pair_info(ser, tgt)})
                groups.append([ser])
        for g in groups:
            if len(g) > 1:
                hits = [s for s in g if s.name in (preferred_names or ())]
                if len(hits) == 1 and hits[0] is not g[0]:  # Audible's own name wins
                    _make_target(g, hits[0])
                # the name YOU chose in the review list ('merge as ...') wins over everything
                wanted = {d.get('target') for k, d in (decisions or {}).items() if d.get('action') == 'merge' and d.get('target') and k.startswith(f'series|{lib}|')}
                mine = [s for s in g if s.name in wanted]
                if len(mine) == 1 and mine[0] is not g[0]: _make_target(g, mine[0])
                # a clean spelling beats the same name with stray spaces ('The Titan ' -> 'The Titan')
                if g[0].name != g[0].name.strip() and (clean := [s for s in g[1:] if s.name == g[0].name.strip()]): _make_target(g, clean[0])
                # the series that holds the lock_series books is the target: locked books are never moved (wins over everything)
                lk = [s for s in g if any('lock_series' in r['tags'] for r, _ in s.entries)]
                if len(lk) == 1 and lk[0] is not g[0]: _make_target(g, lk[0])
                # user rule 2026-11-08: English/German books never end up in a series with a Japanese/Korean/... name
                # (Konosuba: the locked books sat in the Japanese-named series and the merge moved the fixed books back into it)
                if not is_latin(g[0].name) and any(is_latin(x.name) for x in g[1:]) and latin_books(g):
                    if g[0] in lk:
                        for x in g[1:]:
                            rejected.append({'lib': lib, 'series': x.name, 'other': g[0].name, 'reason': 'contains book(s) with lock_series: '
                                             'the locked books sit in a series with a non-Latin name', **pair_info(x, g[0])})
                        continue
                    _make_target(g, max((x for x in g[1:] if is_latin(x.name)), key=lambda x: len(x.entries)))
                # ABS finds series names case-insensitively: 'The Beginning after the End' cannot be written as 'The Beginning After
                # the End' while both exist. The apply step empties such a spelling first (ABS deletes the empty series), then writes
                # the target name. That only works if no other spelling of the same name keeps books (locked ones) -> review list.
                low = g[0].name.strip().lower()
                stay = [x for x in series_map.values() if x is not g[0] and x.name.strip().lower() == low and x.name.strip() != g[0].name.strip()
                        and (x not in g or any('lock_series' in r['tags'] for r, _ in x.entries))]
                if stay:
                    for x in g[1:]:
                        rejected.append({'lib': lib, 'series': x.name, 'other': g[0].name, 'reason': 'contains book(s) with lock_series: ABS treats '
                                         f"names that differ only in upper/lower case as ONE name and '{stay[0].name}' keeps locked books", **pair_info(x, g[0])})
                    continue
                merges.append({'lib': lib, 'target': g[0], 'members': g[1:]})
    return merges, rejected


def apply_merges(ctx, merges):
    """Rename the merged series on every book and lock the series field of the whole group."""
    abs_update, dry, lock = ctx['abs_update'], ctx['dry_run'], ctx.get('lock_after_merge', False)
    aliases = ctx.setdefault('aliases', {})
    changed = 0
    for m in merges:
        tgt = m['target']
        all_seqs = tgt.seqs + [x for s in m['members'] for x in s.seqs]
        logging.info(f"   🔗 MERGE{' (DRY_RUN)' if dry else ''}: into '{tgt.name}' ({len(tgt.entries)} books, #{fmt_seqs(tgt.seqs)})")
        for s in m['members']:
            logging.info(f"        <- '{s.name}' ({len(s.entries)} books, #{fmt_seqs(s.seqs)}) [name: {s.relation}, shared author, no number conflict]")
        logging.info(f"        => '{tgt.name}' afterwards: #{fmt_seqs(all_seqs)}"
                     f"{' (alias saved)' if not dry else ''}")
        member_names = {s.name for s in m['members']}
        mid_of = {s.name: merge_id('series', m['lib'], s.name, tgt.name) for s in m['members']}
        record_merge(ctx, 'series', m['lib'], tgt.name, [(s.name, fmt_seqs(s.seqs), len(s.entries), s.relation, series_info(s)) for s in m['members']],
                     fmt_seqs(tgt.seqs), series_info(tgt))
        if not dry:
            for n in member_names: aliases.setdefault(m['lib'], {})[n] = tgt.name
        group_records = {r['id']: r for s in [tgt] + m['members'] for r, _ in s.entries}
        # spellings that differ from the target only in upper/lower case: empty them first (see find_merges)
        case_names = {s.name for s in m['members'] if s.name.strip().lower() == tgt.name.strip().lower()}
        for rid, r in group_records.items():
            if 'lock_series' in r['tags'] or not any(x['name'] in case_names for x in r['series']): continue
            first = [x for x in r['series'] if x['name'] not in case_names]
            logging.info(f"        ✏️ '{r['title']}': leaves '{next(x['name'] for x in r['series'] if x['name'] in case_names)}' first (ABS ignores upper/lower case in names)")
            mid = next((mid_of[x['name']] for x in r['series'] if x['name'] in mid_of), None)
            abs_update(r['id'], f"{r['lib']}_{rid}", r['title'], {'series': first}, {'series': r['series']}, 'series_merge',
                       extra={'merge_id': mid} if mid else None)
        for rid, r in group_records.items():
            key = f"{r['lib']}_{rid}"
            if 'lock_series' in r['tags'] and any(x['name'] in member_names for x in r['series']):
                logging.info(f"        🔒 '{r['title']}' has lock_series, not changed")
                continue
            new_series, seen = [], {}
            for x in r['series']:
                name = tgt.name if x['name'] in member_names else x['name']
                if name in seen:  # Same series twice after renaming: keep the entry with a number
                    if not new_series[seen[name]]['sequence'] and x.get('sequence'): new_series[seen[name]]['sequence'] = x['sequence']
                    continue
                seen[name] = len(new_series); new_series.append({'name': name, 'sequence': x.get('sequence')})
            updates = {}
            if new_series != r['series']:
                updates['series'] = new_series
            new_tags = None
            if lock and 'lock_series' not in r['tags']:
                new_tags = sorted(set(r['media_tags']) | {'lock_series'})
            if not updates and new_tags is None: continue
            what = []
            if updates: what.append(f"series: {', '.join(x['name'] + (' #' + str(x['sequence']) if x['sequence'] else '') for x in new_series)}")
            if new_tags is not None: what.append("+tag lock_series")
            logging.info(f"        ✏️ '{r['title']}': {' | '.join(what)}")
            mid = next((mid_of[x['name']] for x in r['series'] if x['name'] in mid_of), None)
            if abs_update(r['id'], key, r['title'], updates, {'series': r['series']}, 'series_merge',
                          tags=new_tags, old_tags=r['media_tags'] if new_tags is not None else None,
                          extra={'merge_id': mid} if mid else None):
                changed += 1
    return changed


def record_merge(ctx, kind, lib, target, members, target_books, target_info=None):
    """merges.json: every automatic or decided merge (web UI review list). members: (name, books, count, relation[, info]);
    info / target_info: {'titles': [...], 'authors': [...]} so the UI can show which books are meant."""
    hist, dec, now = ctx.setdefault('merge_history', {}), ctx.setdefault('decisions', {}), datetime.now().strftime('%Y-%m-%d %H:%M')
    for m_ in members:
        name, books, count, relation = m_[:4]; info = m_[4] if len(m_) > 4 else {}
        mid = merge_id(kind, lib, name, target)
        old = hist.get(mid)
        if old and old.get('member_books') != books and dec.get(mid, {}).get('action') == 'confirmed' and not ctx['dry_run']:
            dec.pop(mid, None)  # new books were merged: show it again
        hist[mid] = {'id': mid, 'kind': kind, 'lib': lib, 'target': target, 'member': name, 'member_books': books,
                     'member_count': count, 'target_books': target_books, 'relation': relation,
                     'member_titles': info.get('titles', []), 'member_authors': info.get('authors', []),
                     'target_titles': (target_info or {}).get('titles', []), 'target_authors': (target_info or {}).get('authors', []),
                     'status': 'planned' if ctx['dry_run'] and (not old or old.get('status') == 'planned') else 'applied',
                     'first_seen': (old or {}).get('first_seen', now), 'last_seen': now}


# ================= AUTHORS =================

def find_author_fixes(records, decisions=None, links=None, role_names=()):
    """Spelling variants of the same author ('R. A. Salvatore' / 'R.A. Salvatore'): a spelling with accents wins over the same name
    without them, otherwise the spelling used by more books (tie: alphabetical). Returns ({variant: canonical}, counts, review list).
    Uncertain pairs ('J. Smith' / 'John Smith') go to the review list; your decisions win."""
    decisions = decisions or {}
    count = defaultdict(lambda: defaultdict(int))
    for r in records:
        for a in r['authors']: count[author_key(a)][a] += 1
    fixes = {}
    for key, spellings in count.items():
        if len(spellings) < 2: continue
        # the spelling WITH accents wins over the accent-less one ('Gablé' over 'Gable': the accent is information), then the most used one
        canon = sorted(spellings.items(), key=lambda x: (0 if fold_accents(x[0]) != x[0] else 1, -x[1], x[0]))[0][0]
        for sp in spellings:
            if sp != canon and decisions.get(merge_id('author', '', sp, canon), {}).get('action') not in ('keep', 'undone'):
                fixes[sp] = canon
    # Proof 1: the same Audible author record (author ASIN) was seen behind both spellings (single-author books only, see abs_ratings.note_author_links).
    by_asin = defaultdict(set)
    for name, asins in (links or {}).items():
        for a in asins: by_asin[a].add(name)
    for asin, group in by_asin.items():
        present = [n for n in group if n in {sp for sps in count.values() for sp in sps}]
        if len({author_key(n) for n in present}) < 2: continue
        canon = max(present, key=lambda n: (fold_accents(n) != n, sum(count[author_key(n)].values()) if author_key(n) in count else 0, n))
        roles = {author_key(x) for x in role_names or ()}
        for n in present:
            if not plausible_same_person(n, canon) or author_key(n) in roles or author_key(canon) in roles:
                logging.info(f"   Audible author record {asin} behind '{n}' and '{canon}', but they do not look like one person (translator?): not merged")
                continue
            if author_key(n) != author_key(canon) and n not in fixes and decisions.get(merge_id('author', '', n, canon), {}).get('action') not in ('keep', 'undone'):
                fixes[n] = canon
                logging.info(f"   Same Audible author record ({asin}): '{n}' -> '{canon}'")
    # Proof 2: author in another script (e.g. Japanese) <-> Latin-script author: only if both have a book with the IDENTICAL title and a shared narrator
    # (the same work credited under two spellings). Sharing a series is NOT enough (series can have several real authors).
    by_author = defaultdict(list)
    for r in records:
        for a in r['authors']: by_author[a].append(r)
    tkey = lambda t: re.sub(r'[^a-z0-9]+', ' ', fold_accents(t or '').lower()).strip()
    for a in sorted(n for n in by_author if not is_latin(n)):
        if a in fixes: continue
        found = set()
        for b in by_author:
            if b == a or not is_latin(b): continue
            for ra in by_author[a]:
                if any(tkey(rb['title']) == tkey(ra['title']) and set(rb.get('narrators') or []) & set(ra.get('narrators') or []) for rb in by_author[b]):
                    found.add(b)
        if len({author_key(b) for b in found}) == 1:
            canon = max(found, key=lambda b: sum(count[author_key(b)].values()))
            if decisions.get(merge_id('author', '', a, canon), {}).get('action') not in ('keep', 'undone'):
                fixes[a] = canon
                logging.info(f"   Author in another script: '{a}' -> '{canon}' (identical title + shared narrator)")
    # Review: same last name, first name written out vs. initial ('J. Smith' / 'John Smith')
    total = {k: sum(v.values()) for k, v in count.items()}
    by_last = defaultdict(list)
    for k in count:
        parts = k.split()
        if len(parts) >= 2: by_last[parts[-1]].append(k)
    review = []
    for last, keys in by_last.items():
        for i, a in enumerate(keys):
            for b in keys[i + 1:]:
                fa, fb = a.split()[0], b.split()[0]
                if len(fa) == len(fb) or not (fa[0] == fb[0] and (len(fa) <= 2 or len(fb) <= 2)): continue
                if len(a.split()) != len(b.split()): continue
                big, small = (a, b) if total[a] >= total[b] else (b, a)
                canon = max(count[big], key=count[big].get); variant = max(count[small], key=count[small].get)
                mid = merge_id('author', '', variant, canon)
                act = decisions.get(mid, {}).get('action')
                if act == 'merge' and decisions[mid].get('target') == variant:  # 'merge as <variant>': the other direction
                    for sp in count[big]: fixes[sp] = variant
                elif act == 'merge':
                    for sp in count[small]: fixes[sp] = canon
                elif act not in ('keep', 'undone'):
                    review.append({'id': mid, 'kind': 'author', 'lib': '', 'series': variant, 'other': canon,
                                   'reason': 'first name vs. initial', 'series_books': total[small], 'other_books': total[big],
                                   'series_titles': sorted({r['title'] for r in records if variant in r['authors']})[:15],
                                   'other_titles': sorted({r['title'] for r in records if canon in r['authors']})[:15]})
    review = [r for r in review if r["series"] not in fixes]  # already resolved by proof
    return fixes, count, review


def merge_case_variant_authors(ctx, fixes):
    """Spellings that differ only in case ('XKarnation' / 'Xkarnation') are two ABS author entries, but a book update with the other
    spelling is matched to the same entry: the per-book fix was written again in every run (check 2026-11-15, 6 runs). The entry is
    renamed instead - ABS then merges it into the existing one. Returns the variants handled this way."""
    case_only = {v: c for v, c in fixes.items() if v != c and v.lower() == c.lower()}
    if not case_only or ctx['dry_run'] or not ctx.get('session'): return set()  # test run: the per-book plan shows it as before
    done = set()
    for lib in ctx.get('library_ids') or []:
        try: lst = ctx['session'].get(f"{ctx['abs_url']}/api/libraries/{lib}/authors", timeout=60).json().get('authors') or []
        except Exception as e:
            logging.warning(f"   Author list of {lib} not loaded: {e}"); continue
        for a in lst:
            if a.get('name') in case_only:
                try:
                    r = ctx['session'].patch(f"{ctx['abs_url']}/api/authors/{a['id']}", json={'name': case_only[a['name']]}, timeout=60)
                    if r.ok: done.add(a['name']); logging.info(f"   ✍️ AUTHOR entry '{a['name']}' renamed to '{case_only[a['name']]}' (only the case differed: ABS merges both)")
                    else: logging.warning(f"   Author entry '{a['name']}' not renamed (HTTP {r.status_code})")
                except Exception as e: logging.warning(f"   Author entry '{a['name']}' not renamed: {e}")
    return done


def apply_author_fixes(ctx, records, fixes, count):
    abs_update, dry = ctx['abs_update'], ctx['dry_run']
    changed = 0
    merged_ = merge_case_variant_authors(ctx, fixes)
    if merged_:
        for r in records: r['authors'] = [fixes[a] if a in merged_ else a for a in r['authors']]
        fixes = {v: c for v, c in fixes.items() if v not in merged_}
    for variant, canon in sorted(fixes.items()):
        logging.info(f"   ✍️ AUTHOR{' (DRY_RUN)' if dry else ''}: '{variant}' ({count[author_key(variant)][variant]} books) -> '{canon}' ({count[author_key(canon)][canon]} books)")
        titles = lambda n: sorted({r['title'] for r in records if n in r['authors']})[:15]
        record_merge(ctx, 'author', '', canon, [(variant, '', count[author_key(variant)][variant], 'spelling', {'titles': titles(variant)})], '', {'titles': titles(canon)})
    for r in records:
        if not any(a in fixes for a in r['authors']): continue
        if 'lock_authors' in r['tags']:
            logging.info(f"        🔒 '{r['title']}' has lock_authors, not changed"); continue
        new_auth = []
        for a in r['authors']:
            a = fixes.get(a, a)
            if a not in new_auth: new_auth.append(a)
        logging.info(f"        ✏️ '{r['title']}': authors {r['authors']} -> {new_auth}")
        mid = next((merge_id('author', '', a, fixes[a]) for a in r['authors'] if a in fixes), None)
        if abs_update(r['id'], f"{r['lib']}_{r['id']}", r['title'], {'authors': [{'name': a} for a in new_auth]},
                      {'authors': [{'name': a} for a in r['authors']]}, 'author_normalize', extra={'merge_id': mid}):
            changed += 1
            r['authors'] = new_auth  # later steps (role credits) must build on the fixed names, not overwrite them
    return changed




# ================= AUTHOR CLEAN-UP (user rules 2026-11-04) =================

def role_base(name):
    """'You Shiina - illustrator' -> 'You Shiina'."""
    return ROLE_CREDIT_RE.sub('', name or '').strip()


def genre_key(g):
    """'Post Apocalyptic' = 'Post-Apocalyptic' = 'PostApocalyptic', 'Science fiction' = 'Science Fiction': one genre."""
    return re.sub(r'[^a-z0-9]', '', fold_accents(str(g or '')).lower())


# genre loop 2026-11-15 (user: one name per genre): synonyms -> one name (Audible's where it has one). '-' = no genre at all.
_GENRE_SYN = {
    # user 2026-11-15: one name per genre, no '&' names (except the established 'Sword & Sorcery'), no umbrella categories
    'Humorous': ['Humor', 'Humour', 'comedy', 'Comedy & Humor', 'Humorous Fiction', 'Humour fiction'],
    'Science Fiction': ['Science fiction', 'Sci-fi', 'Sci-Fi', 'SciFi', 'Science-Fiction'],
    'LitRPG': ['litrpg', 'LitRpg', 'Lit RPG', 'GameLit', 'gamelit', 'Fantasy LitRPG'],
    'Young Adult': ['Young Adult Fiction', 'Teen & Young Adult', 'Young adult fantasy', 'teen', 'YA', 'Jugendliche & Heranwachsende'],
    'Dystopian': ['Dystopia', 'Dystopias'],
    'Post Apocalyptic': ['Apocalyptic & Post-Apocalyptic', 'Post-apocalyptic fiction', 'Apocalyptic'],
    'Light Novel': ['light-novels', 'Light Novels'],
    'Short Stories': ['Short stories', 'Anthologies', 'Collections', 'Anthology', 'Anthologies & Short Stories'],
    'Thriller': ['Thrillers & Suspense', 'Thriller & Suspense', 'Suspense', 'Thrillers', 'Crime Thrillers'],
    'Graphic Novel': ['Comics', 'Graphic novels', 'Comix', 'Comics & Graphic Novels'],
    'Military': ['Military Fiction', 'War & Military'],
    'Fantasy Romance': ['Fantasy romance', 'Romantasy'],
    'Fantasy': ['Fantasy fiction', 'Fantasy Fiction', 'Fantasy & Magic', 'Fantasy & Magie'],
    'Epic Fantasy': ['Fantasy - Epic', 'Epische Fantasy'],
    'Urban Fantasy': ['Urban', 'Paranormal & Urban', 'Zeitgenössische Fantasy'],
    'Romance': ['Liebesromane'],
    'Sword & Sorcery': ['Schwerter & Magie'],
    'Adventure': ['Action & Adventure', 'Action', 'Action and Adventure', 'Sea Adventures'],
    'Mystery': ['Mysteries', 'Detective and mystery stories', 'International Mystery & Crime'],
    'Paranormal': ['Paranormal & Supernatural'],
    'Espionage': ['Spies & Politics'],
    'Fairy Tales': ['Fairy Tales, Folk Tales & Myths'],
    "Children's Audiobooks": ['childrens', "Children's", 'Juvenile Fiction', 'Middle Grade', 'Juvenile Literature', 'Kinder-Hörbücher'],
    'Anime & Manga': ['anime'],
    'Mythology': ['Greek & Roman', 'Norse'],
    'Dramatized': ['Dramatizations', 'Audio Performances & Dramatizations'],
    '-': ['adult', 'Amazon', 'Book Club', 'smut', 'Large type books', 'Unfinished', 'Science Fiction & Fantasy', 'Sci-Fi & Fantasy', 'Literature & Fiction',
          'Fiction', 'Novels', 'Literature', 'Fiction & Literature', 'Genre Fiction', 'Social & Life Skills', 'Growing Up & Facts of Life', 'Death & Dying',
          'Physical & Emotional Abuse', 'Difficult Situations', 'Family Life', 'Emotions & Feelings', 'Explore the World', 'Self Esteem & Self Image',
          'Clean & Wholesome', 'Small Town & Rural', 'Asian American & Pacific Islander', 'Words, Language & Grammar', 'Arts & Entertainment',
          'Metaphysical & Visionary', 'Family', 'Bullying', 'Belletristik', 'Literatur & Belletristik', 'Mystery, Thriller & Suspense', 'Holidays', 'Modern', 'Dark', 'second chance', 'Multicultural', 'European', 'Southern',
          'Literary History & Criticism', 'Writing & Publishing', 'Politics & Government', 'Roman'],  # audit 2026-11-16: no fiction genres
}
# themes are no genres (user 2026-11-15: 'Dragons & Mythical Creatures' is a tag) -> an English tag
_THEME_TAG = {
    'Dragons & Mythical Creatures': 'Dragons', 'Drachen & Fabelwesen': 'Dragons', 'Zeitreisen': 'Time Travel', 'Mit Videospiel-, Film- & Fernsehbezug': 'Tie-In', 'Mit Videospiel-': 'Tie-In',
    'Film- & Fernsehbezug': 'Tie-In',
    'dragons': 'Dragons', 'Dragons': 'Dragons', 'Magic': 'Magic', 'Time Travel': 'Time Travel',
    'First Contact': 'First Contact', 'Aliens': 'Aliens', 'Movie, TV & Video Game Tie-Ins': 'Tie-In', 'Movie, TV & Video Game Tie-ins': 'Tie-In',
    'Vampires': 'Vampires', 'Elves': 'Elves', 'Monsters': 'Monsters', 'demons': 'Demons', 'Shapeshifters': 'Shapeshifters', 'Cats': 'Cats',
    'Animals': 'Animals', 'Superhero': 'Superheroes', 'Superheroes': 'Superheroes', 'Martial arts': 'Martial Arts', 'Genetic Engineering': 'Genetic Engineering',
    'Artificial Intelligence': 'Artificial Intelligence', 'Space Exploration': 'Space Exploration', 'Coming of Age': 'Coming of Age', 'LGBTQ': 'LGBTQ',
    'Lgbt': 'LGBTQ', 'LGBTQ+': 'LGBTQ', 'Queer': 'LGBTQ', 'Gay': 'LGBTQ', 'Lesbian': 'LGBTQ', 'Cozy': 'Cozy', 'cozy fantasy': 'Cozy', 'Cozy Fantasy': 'Cozy',
    'Cosy': 'Cozy', 'Harem': 'Harem', 'Friendship': 'Friendship', 'Medieval': 'Medieval', 'Forgotten Realms': 'Forgotten Realms',
    'Dungeons and dragons': 'Dungeons & Dragons', 'Star wars': 'Star Wars', 'gods': 'Gods', 'fae': 'Fae', 'Pirates': 'Pirates', 'Spicy': 'Spicy', 'Space': 'Space', 'Books & Libraries': 'Books & Libraries', 'Found Family': 'Found Family',
}
_GENRE_CANON = {genre_key(v): k for k, vs in _GENRE_SYN.items() for v in vs}
_GENRE_SYN_KEYS = {genre_key(k): k for k in _GENRE_SYN if k != '-'}  # canonical key -> its spelling
_THEME_KEYS = {genre_key(k): v for k, v in _THEME_TAG.items()}
MAIN_GENRES = {genre_key(x) for x in ('Fantasy', 'Science Fiction', 'Horror', 'Mystery', 'Thriller', 'Romance', 'Historical Fiction', 'Literary Fiction',
                                       'Classics', 'Humorous', 'Crime', 'Westerns', 'Poetry')}
AUDIENCE_GENRES = {genre_key(x) for x in ('Young Adult', "Children's Audiobooks", 'New adult')}
BROAD_GENRES = {genre_key(x) for x in ('Adventure', 'Contemporary', 'Historical')}  # on most books: only for places left over


# genre rework 2026-11-16 (user: 'Adventure' on nearly every book, 'Science Fiction' on fantasy, few genres): Audible's categories are
# ladders ('Science Fiction & Fantasy > Fantasy > Epic'); only the leaf was taken, without its context
GENERIC_GENRES = {genre_key(x) for x in ('Adventure', 'Epic', 'Contemporary', 'Historical', 'Military')}  # say nothing without context
GENRE_CTX = {  # (parent, leaf) -> genre; None = no genre ('Fantasy > Action & Adventure' says no more than 'Fantasy')
    ('Fantasy', 'Epic'): 'Epic Fantasy', ('Fantasy', 'Historical'): 'Historical Fantasy', ('Fantasy', 'Military'): 'Military Fantasy',
    ('Science Fiction', 'Military'): 'Military Science Fiction', ('Paranormal & Urban', 'Contemporary'): 'Contemporary Fantasy',
    ('Paranormal & Urban', 'Urban'): 'Urban Fantasy', ('Fantasy', 'Action & Adventure'): None, ('Science Fiction', 'Adventure'): None,
    ('Science Fiction', 'Action & Adventure'): None, ('Romance', 'Fantasy'): 'Fantasy Romance', ('Romance', 'Paranormal'): 'Paranormal Romance',
    ('Teen & Young Adult', 'Literature & Fiction'): None,
}
GENRE_TOP = {'Teen & Young Adult': 'Young Adult', "Children's Audiobooks": "Children's Audiobooks"}  # audience roots
FORMAT_GENRES = {genre_key(x) for x in ('Manga', 'Graphic Novel')}  # wrong next to 'Light Novel' on an audiobook


def genres_from_audible(ladders, cur=None):
    """Audible's category ladders -> (main genres, specific genres, genres to drop) with context; themes are left to theme_tag().
    User 2026-11-16: no 'Science Fiction' on fantasy books that have nothing to do with it - publishers add one off-genre category
    ('Science Fiction > Dystopian' on The Faithful and the Fallen, 'Science Fiction > Cyberpunk' on LitRPG: Audible has no LitRPG category,
    'Science Fiction > Adventure' on Overlord). Between Fantasy and Science Fiction:
    (a) one with only generic / bare ladders goes when the other has a real sub-category,
    (b) one with a single ladder goes when the other has 2+ (with its sub-category),
    (c) Science Fiction whose only sub-category is Cyberpunk goes on a LitRPG book."""
    mains, spec, lad_main = [], [], []
    for lad in ladders or []:
        n = [str(x or '').strip() for x in lad if str(x or '').strip()]
        if not n: continue
        if n[0] in GENRE_TOP: spec.append(GENRE_TOP[n[0]])
        ctx = GENRE_CTX.get((n[-2], n[-1]), '?') if len(n) >= 2 else '?'
        leaf_g = None if ctx is None else (ctx if ctx != '?' else canonical_genre(n[-1]))
        main_here = None
        for x in n:
            c = canonical_genre(x)
            if c and genre_key(c) in MAIN_GENRES:
                if c not in mains: mains.append(c)
                if main_here is None and genre_key(c) in ('fantasy', 'sciencefiction'): main_here = c
        real = bool(leaf_g) and genre_key(leaf_g) not in GENERIC_GENRES and leaf_g != main_here or theme_tag(n[-1]) is not None
        if leaf_g and genre_key(leaf_g) not in MAIN_GENRES: spec.append(leaf_g)
        elif leaf_g and leaf_g != main_here and leaf_g not in mains: mains.append(leaf_g)  # 'Fantasy > Humorous'
        lad_main.append((main_here, real, leaf_g if leaf_g and genre_key(leaf_g) not in MAIN_GENRES else None))
    drop = set()
    F, SF = 'Fantasy', 'Science Fiction'
    if F in mains and SF in mains:
        nr = {m: sum(1 for mh, r, _ in lad_main if mh == m and r) for m in (F, SF)}
        leaves = {m: {genre_key(l) for mh, _, l in lad_main if mh == m and l} for m in (F, SF)}
        minor = None
        if nr[F] and not nr[SF]: minor = SF
        elif nr[SF] and not nr[F]: minor = F
        elif nr[F] >= 2 and nr[SF] == 1: minor = SF
        elif nr[SF] >= 2 and nr[F] == 1: minor = F
        elif leaves[SF] <= {'cyberpunk'} and 'litrpg' in {genre_key(g) for g in cur or [] if isinstance(g, str)}: minor = SF
        if minor:
            drop = {genre_key(minor)} | (leaves[minor] - leaves[F if minor == SF else SF])
            mains = [m for m in mains if m != minor]
            spec = [g for g in spec if genre_key(g) not in drop]
    return mains, spec, drop


def light_novel_rule(genres):
    """'Light Novel' stays; 'Manga' / 'Graphic Novel' next to it are the wrong format for an audiobook (user 2026-11-16)."""
    keys = {genre_key(g) for g in genres if isinstance(g, str)}
    if 'lightnovel' not in keys: return list(genres)
    return [g for g in genres if not (isinstance(g, str) and genre_key(g) in FORMAT_GENRES)]


def rebuild_genres(cur, ladders, gmax=6):
    """The book's genres from a VERIFIED Audible product: Audible's main genres (others go - 'Science Fiction' on a fantasy book came
    from old data), Audible's specific genres with context, then the existing specific genres (LitRPG, Dark Fantasy, Light Novel ... from
    earlier sources, the series fill or Hardcover). Generic ones (Adventure, Epic ...) only when Audible itself gives them. Themes are
    no genres. Idempotent: rebuild_genres(rebuild_genres(x)) == rebuild_genres(x) (no ping-pong with the series task)."""
    mains, spec, drop = genres_from_audible(ladders, cur)
    if not mains and not spec: return list(cur or [])
    out = list(mains) + list(spec)
    for g in cur or []:
        if not isinstance(g, str): continue
        k = genre_key(g)
        if k in MAIN_GENRES:
            if not mains: out.append(g)  # Audible names no main genre: the existing ones stay
            continue
        if k in GENERIC_GENRES or k in drop: continue  # drop: the off-genre category's leaf ('Dystopian' from 'Science Fiction > Dystopian')
        out.append({'epic': 'Epic Fantasy'}.get(k, g))
    seen, res = set(), []
    for g in out:
        k = genre_key(g)
        if not k or k in seen or theme_tag(g): continue
        seen.add(k); res.append(g)
    if {'epic', 'epicfantasy'} <= seen: res = [g for g in res if genre_key(g) != 'epic']
    return light_novel_rule(res)[:gmax]


def series_genre_fill(ctx, records):
    """Fewer than GENRE_MAX genres -> the genres most books of the same series have (user 2026-11-16: volumes of one series share their
    genres; Hardcover's votes are too thin). Never a generic genre, a main genre only for a book without one, no Manga / Graphic Novel next
    to Light Novel, lock_genres respected. Runs after the genre cleanup."""
    from collections import Counter
    abs_update, dry = ctx['abs_update'], ctx['dry_run']
    gmax = int(ctx.get('genre_max') or 6)
    by_series = defaultdict(list)
    for r in records:
        for x in r['series']:
            if x.get('name'): by_series[(r['lib'], series_key(x['name']))].append(r)
    changed, orig = 0, {id(r): list(r['genres']) if isinstance(r.get('genres'), list) else None for r in records}
    for _round in range(4):  # the siblings' fill changes the consensus: repeat until nothing moves (else it settles one run later)
      moved = False
      for r in records:
        if not isinstance(r.get('genres'), list) or len(r['genres']) >= gmax or r['tags'] & {'lock_genres', 'lock_all', 'lock_metadata'}: continue
        sib = {id(o): o for x in r['series'] if x.get('name') for o in by_series[(r['lib'], series_key(x['name']))] if o is not r}
        if len(sib) < 2: continue
        cnt = Counter(genre_key(g) for o in sib.values() for g in dict.fromkeys(o['genres']) if isinstance(g, str))
        names = {}
        for o in sib.values():
            for g in o['genres']:
                if isinstance(g, str): names.setdefault(genre_key(g), g)
        have = {genre_key(g) for g in r['genres'] if isinstance(g, str)}
        blocked = set((ctx.get('genre_drops') or {}).get(f"{r['lib']}_{r['id']}") or ())  # off-genre for this book by Audible (book step)
        has_main = bool(have & MAIN_GENRES)
        # a book with no real genre at all ('Literature & Fiction' only: Audible has nothing better) takes them from ONE other volume too
        need = 1 if not any(isinstance(g, str) and canonical_genre(g) == g for g in r['genres']) else max(2, (len(sib) + 1) // 2)
        add = []
        for k, n in cnt.most_common():
            if len(r['genres']) + len(add) >= gmax: break
            if n < need or k in have or k in blocked or k in GENERIC_GENRES or theme_tag(names[k]): continue
            if canonical_genre(names[k]) is None or genre_key(canonical_genre(names[k])) != k: continue  # 2026-11-16: only clean genres
            # ('Literature & Fiction' of two volumes came back right after the cleanup removed it: every run)
            if k in ('fantasy', 'sciencefiction') and have & {'fantasy', 'sciencefiction'}: continue  # never Fantasy <-> Science Fiction
            # a further main genre only for a book that had no real genre (Audible gave none, so the book step keeps it); otherwise the
            # book step would drop a main genre Audible does not name and this step would add it again (ping-pong)
            if k in MAIN_GENRES and has_main and need != 1: continue
            if k in FORMAT_GENRES and 'lightnovel' in have: continue
            if k == 'epic' and 'epicfantasy' in have: continue
            add.append(names[k]); have.add(k); has_main = has_main or k in MAIN_GENRES
        if add:
            new_ = r['genres'] + add
            if need == 1: new_ = [g for g in new_ if not (isinstance(g, str) and canonical_genre(g) is None)]  # umbrella goes once real genres are there
            if {'epic', 'epicfantasy'} <= {genre_key(g) for g in new_ if isinstance(g, str)}: new_ = [g for g in new_ if not (isinstance(g, str) and genre_key(g) == 'epic')]
            r['genres'] = new_; moved = True
      if not moved: break
    # user 2026-11-16: all in ONE run - the filled genres get the book step's Audible rules right away (a book that got 'LitRPG' from its
    # series loses the Cyberpunk-only 'Science Fiction' now, not one run later); the rebuild never adds back what the fill may not
    lads_all = ctx.get('genre_ladders') or {}
    drops = ctx.get('genre_drops') if isinstance(ctx.get('genre_drops'), dict) else None
    for r in records:
        o, k_ = orig.get(id(r)), f"{r['lib']}_{r['id']}"
        if o is None or r['genres'] == o or k_ not in lads_all: continue
        new_ = rebuild_genres(r['genres'], lads_all[k_], gmax)
        if drops is not None:
            dr_ = genres_from_audible(lads_all[k_], new_)[2]
            if dr_: drops[k_] = sorted(set(drops.get(k_) or ()) | dr_)
        r['genres'] = new_
    for r in records:
        o = orig.get(id(r))
        if o is not None and r['genres'] != o and abs_update(r['id'], f"{r['lib']}_{r['id']}", r['title'], {'genres': r['genres']}, {'genres': o}, 'genre_series'):
            changed += 1
            logging.info(f"   🏷️ GENRES{' (DRY_RUN)' if dry else ''}: '{r['title']}': +{', '.join(g for g in r['genres'] if g not in o)} (most volumes of the series have them)")
    return changed


def canonical_genre(g):
    """One name for a genre ('Humor' / 'comedy' -> 'Humorous', 'Action & Adventure' -> 'Adventure'); None = no genre ('adult', umbrella
    categories like 'Science Fiction & Fantasy', themes - see theme_tag)."""
    k = genre_key(g)
    if k in _THEME_KEYS: return None
    c = _GENRE_CANON.get(k)
    if c == '-': return None
    return c or _GENRE_SYN_KEYS.get(k) or g


def theme_tag(g):
    """'Dragons & Mythical Creatures' -> 'Dragons', 'Magic' -> 'Magic' (a theme, kept as a tag), else None."""
    return _THEME_KEYS.get(genre_key(g))


_CB_STOP = {'the', 'a', 'an', 'of', 'and', 'series', 'saga', 'trilogy', 'cycle', 'novels', 'novel', 'books', 'book', 'chronicles', 'pre', 'era',
            'order', 'publication', 'chronological', 'light'}


def covered_by_series(name, seq, series):
    """The name is a SHORTER name of a series the book already has with the same number: all its words are in that one name
    ('Apocalypse Bringer Mynoghra' #4 / 'Apocalypse Bringer Mynoghra: World Conquest ...' #4, 'Wax and Wayne' #1 / 'Mistborn, Era 2:
    Wax & Wayne' #1, 'Pern Novels [publication order]' #5 / 'Pern' #5). Series loop 2026-11-15: added per book, removed again by the
    series task every run. A LONGER name ('Discworld: Tiffany Aching' next to 'Tiffany Aching') is still added: drop_duplicate_series
    then keeps the longer one for the whole series."""
    w = lambda n: {x for x in re.sub(r"[^a-z0-9\s]", ' ', fold_accents((n or '').lower()).replace("'", '').replace('&', ' and ')).split() if x not in _CB_STOP}
    num = lambda q: float(q) if re.fullmatch(r'\d+(?:\.\d+)?', str(q or '').strip()) else None
    mine = w(name)
    # also the same name with an edition marker ('The Witcher (Full-Cast Editions)' #7 next to 'The Witcher' #7: the task drops it)
    return bool(mine) and num(seq) is not None and any((mine <= w(x.get('name')) or series_key(name) == series_key(x.get('name') or ''))
                                                       and num(x.get('sequence')) == num(seq) for x in series or [])


def strip_series_format(name):
    """'Spice and Wolf (Light Novel)' / 'How a Realist Hero Rebuilt the Kingdom Light Novel' / 'The Sword of Shannara (Unabridged)' ->
    the name without the format label (user 2026-11-15: '(Light Novel)' out of series names too)."""
    n = re.sub(r'(?i)\s*\((?:light\s*novels?|unabridged|ungek(?:ü|ue)rzt|abridged)\)', '', name or '')
    n = re.sub(r'(?i)\s+light\s*novels?\s*$', '', n)
    return re.sub(r'\s{2,}', ' ', n).strip() or (name or '')


def tidy_series_entries(ctx, records):
    """Series loop 2026-11-15: 'The Devils ' / 'Battle Mage Farmer ' (a trailing space makes a second series in ABS), 'Qi=MC^2)' (a
    lone closing bracket), numbers '01' -> '1', '01–07' -> '1-7'. lock_series respected."""
    abs_update, dry = ctx['abs_update'], ctx['dry_run']
    def nm(n):
        n = strip_series_format(re.sub(r'\s{2,}', ' ', (n or '').strip()))
        if n.endswith(')') and '(' not in n: n = n[:-1].rstrip()
        return n
    def sq(q):
        if q is None: return q
        q = str(q).strip().replace('–', '-').replace('—', '-')
        return re.sub(r'(?<![\d.])0+(\d)', r'\1', q)
    changed = 0
    for r in records:
        if r['tags'] & {'lock_series', 'lock_all', 'lock_metadata'}: continue
        new = [dict(x, name=nm(x['name']), sequence=sq(x.get('sequence'))) for x in r['series']]
        if new != r['series'] and abs_update(r['id'], f"{r['lib']}_{r['id']}", r['title'], {'series': new}, {'series': r['series']}, 'series_spelling'):
            changed += 1
            logging.info(f"   📚 SERIES{' (DRY_RUN)' if dry else ''}: '{r['title']}': spelling {[(x['name'], x.get('sequence')) for x in r['series']]} -> {[(x['name'], x.get('sequence')) for x in new]}")
            r['series'] = new
    return changed


def shared_descriptions(ctx, records):
    """Keys whose description text (without the rating block) another library book of another ASIN has too - the per-book step then
    takes this recording's Audible summary (description loop 2026-11-15: Dragon Heart 1 = 4, Pendulum of Fate 1 = 2)."""
    def body(d):
        d = re.sub(r'(?s)─── .*? ───.*?─{6,}(?:<br\s*/?>)?', '', d or '')
        return re.sub(r'\W+', '', re.sub(r'<[^>]+>', ' ', d).lower())[:400]
    groups = defaultdict(list)
    for r in records:
        b = body(r.get('description'))
        if len(b) >= 120: groups[(r['lib'], b)].append(r)
    # 2026-11-16: a text that IS Audible's summary of this very ASIN stays (Audible uses one blurb for several volumes: 6 Harry Potter
    # full-cast editions, Shield Hero 1/3/4) - it was listed in every run with 'takes its own summary next run' and never changed
    idx = ctx.get('summary_index') or {}
    sn = lambda t: re.sub(r'[^a-z0-9]+', ' ', html.unescape(re.sub(r'<[^>]+>', ' ', t or '')).lower()).strip()
    def own_(r):
        a = (idx.get(r.get('asin') or '') or [None, ''])[1]
        t = sn(re.sub(r'(?s)─── .*? ───.*?─{6,}(?:<br\s*/?>)?', '', r.get('description') or ''))
        return bool(a) and len(t) >= 60 and t[:300] == a[:300]
    keys = sorted(f"{r['lib']}_{r['id']}" for lst in groups.values() if len({r.get('asin') for r in lst}) > 1 for r in lst if not own_(r))
    if ctx.get('shared_desc_file') and not ctx['dry_run']: _save(ctx['shared_desc_file'], keys)
    if keys: logging.info(f"   📝 Descriptions shared by several books: {len(keys)} (each takes its own Audible summary in the next run)")
    n_own = sum(1 for lst in groups.values() if len({r.get('asin') for r in lst}) > 1 for r in lst if own_(r))
    if n_own: logging.info(f"   ℹ️ {n_own} book(s) share a text that is Audible's own summary of their edition (Audible uses one text for several volumes): kept")
    return keys


def split_joined_narrators(ctx, records):
    """Narrator loop 2026-11-15: 'Dillon Sickels + Mary Robinette Kowal' as ONE narrator next to both names (9 books, from the folder's
    'A + B' list) -> the single names, each once, order kept. lock_narrators respected."""
    abs_update, dry = ctx['abs_update'], ctx['dry_run']
    changed = 0
    for r in records:
        if not any(' + ' in (n or '') for n in r['narrators']) or r['tags'] & {'lock_narrators', 'lock_all', 'lock_metadata'}: continue
        new, seen = [], set()
        for n in r['narrators']:
            for x in (p.strip() for p in str(n).split(' + ')):
                k = re.sub(r'[^a-z]', '', fold_accents(x).lower())
                if x and k not in seen: seen.add(k); new.append(x)
        if new != r['narrators'] and abs_update(r['id'], f"{r['lib']}_{r['id']}", r['title'], {'narrators': new}, {'narrators': r['narrators']}, 'joined_narrators'):
            changed += 1
            logging.info(f"   🎙️ NARRATORS{' (DRY_RUN)' if dry else ''}: '{r['title']}': {r['narrators']} -> {new}")
            r['narrators'] = new
    return changed


_FOREIGN_GENRE = re.compile(r"(?i)(?:acci[oó]n|aventuras?\b|fantas[ií]a\b|ficci[oó]n|literatura\b|azione|avventura|\bepica\b|fantascienza|créatures|"
                            r"mythiques|[ée]pique|epopeyas|\bet\b|science-fiction et|abenteuer|belletristik|\bliteratur\b|jugendliche|heranwachsende|kinderb|krimi|"
                            r"sachbuch|geschichte|fantastique|policier|paranormal y|y urbana|ciencia)")


def normalize_genres(ctx, records):
    """Genre hygiene (audit 2026-11-09: 659 genres, 128 books with the same genre twice in another spelling): spellings of one genre
    become the library's most used spelling, a genre listed twice in one book once; number-only genres (timestamps like
    '1750488324482') and genres in a non-Latin script on English/German books are removed. Order kept, lock_genres respected."""
    from collections import Counter
    abs_update, dry = ctx['abs_update'], ctx['dry_run']
    split_ = lambda g: [x.strip() for x in re.split(r'\s*;\s*|\s*\|\s*|\s+/\s+|(?<=\S):(?=\S)', g) if x.strip()]
    cnt = Counter(x for r in records for g in r['genres'] if isinstance(g, str) for x in split_(g))
    best = {}
    for g, n in cnt.items():
        k, cur = genre_key(g), best.get(genre_key(g))
        # 2026-11-16: a capitalized spelling wins over a more used lowercase one ('Erotica' over 'erotica' on 3 books)
        if k and (cur is None or (g[:1].isupper(), n, g.count(' '), g) > (cur[:1].isupper(), cnt[cur], cur.count(' '), cur)): best[k] = g
    changed = 0
    gmax = int(ctx.get('genre_max') or 0)
    cnt_all = Counter(genre_key(x) for r in records for g in r['genres'] if isinstance(g, str) for x in split_(g))  # books per genre
    props = []
    for r in records:
        if not r['genres'] or 'lock_genres' in r['tags'] or 'lock_all' in r['tags']: continue
        en_de = str(r.get('language') or '').strip().lower() in ('english', 'german', 'eng', 'deu', 'ger', 'en', 'de')
        out, seen = [], set()
        parts_ = []
        for g in r['genres']:
            # round 5 (2026-11-09): several genres glued into one ('Epic; Action & Adventure', 'A|B', 'Science Fiction & Fantasy / Science
            # Fiction / Military', 'X:Y:Z') are split; 'Audiobook' (1341 books), 'General', 'Unknown' are no genres
            ps_ = [x.strip() for x in re.split(r'\s*;\s*|\s*\|\s*|\s+/\s+|(?<=\S):(?=\S)', g) if x.strip()] if isinstance(g, str) else [g]
            # genre loop 2026-11-15: 'Action & Adventure, Dark Fantasy, Fantasy' (one entry) - comma lists of capitalized genres split too
            for x in ps_:
                cs_ = [y.strip() for y in x.split(', ')] if isinstance(x, str) else [x]
                # only when every part is a genre of its own in the library ('Movie, TV & Video Game Tie-Ins' is ONE genre)
                parts_ += cs_ if len(cs_) > 1 and all(y and cnt.get(y, 0) >= 1 for y in cs_) else [x]
        en_ = str(r.get('language') or '').strip().lower() in ('english', 'eng', 'en')
        r['_themes'] = {t for t in (theme_tag(g) for g in parts_ if isinstance(g, str)) if t}  # -> tags (manage_tags)
        for g in parts_:
            if isinstance(g, str) and g.strip().lower() in ('audiobook', 'audiobooks', 'hörbuch', 'hörbücher', 'general', 'unknown', 'books', 'genre',
                                                            'ebooks', 'ebook', 'e-books', 'kindle', 'audible'): continue
            if isinstance(g, str) and re.search(r'\S\s+:\s+\S', g): continue  # 'Boston : Allen & Unwin' (place : publisher of a catalogue record)
            if isinstance(g, str) and en_ and _FOREIGN_GENRE.search(g): continue  # 'Acción y aventura', 'Literatur & Belletristik' on an English book
            if isinstance(g, str):
                g = canonical_genre(g)
                if g is None: continue
            k = genre_key(g) if isinstance(g, str) else ''
            if not k or k.isdigit() or k in seen or (en_de and not is_latin(g)): continue
            g_ = _GENRE_SYN_KEYS.get(k) or best.get(k, g.strip())
            if isinstance(g_, str) and g_[:1].islower() and g_.isascii(): g_ = ' '.join(w[:1].upper() + w[1:] for w in g_.split(' '))  # 'speculative fiction'
            seen.add(k); out.append(g_)
        out = light_novel_rule(out)  # 2026-11-16: no Manga / Graphic Novel next to Light Novel
        if {'epic', 'epicfantasy'} <= {genre_key(g) for g in out if isinstance(g, str)}:
            # user 2026-11-15: 'Epic' next to 'Epic Fantasy' is a duplicate -> Epic Fantasy stays; 'Epic' alone stays 'Epic'
            out = [g for g in out if not (isinstance(g, str) and genre_key(g) == 'epic')]
        if gmax and out:
            # user 2026-11-15: at most GENRE_MAX genres (Audible's first, then the others in their order) and no genre that only this
            # book has (unless it came from Audible) - at least 3 genres stay
            aud_g = {genre_key(x) for x in ctx.get('audible_genres') or ()}
            rare = [g for g in out if isinstance(g, str) and cnt_all.get(genre_key(g), 0) <= 1 and genre_key(g) not in aud_g]
            if rare and len(out) - len(rare) >= 3: out = [g for g in out if g not in rare]
            if len(out) > gmax:
                # Audible's first, then the genres the library uses most ('LitRPG' on 610 books stays before a one-off tag)
                # user 2026-11-15: the main genre first (Fantasy, Science Fiction, Horror ...), then the specific ones (LitRPG, Epic
                # Fantasy, Cyberpunk ...), then the audience (Young Adult); inside each: Audible's, then the library's most used
                cls_ = lambda g: 0 if genre_key(g) in MAIN_GENRES else (2 if genre_key(g) in AUDIENCE_GENRES else (3 if genre_key(g) in BROAD_GENRES else 1))
                keep_ = set(sorted(out, key=lambda g: (cls_(g) if isinstance(g, str) else 1, 0 if isinstance(g, str) and genre_key(g) in aud_g else 1,
                                                       -cnt_all.get(genre_key(g) if isinstance(g, str) else '', 0)))[:gmax])
                out = [g for g in out if g in keep_]  # the book's own order
        props.append((r, out))
    if gmax:
        # rarity on the RESULT (a genre left on one book after the cap goes now, not one run later - stable in one run)
        aud_g = {genre_key(x) for x in ctx.get('audible_genres') or ()}
        for _ in range(5):
            cnt_new = Counter(genre_key(g) for r, out in props for g in out if isinstance(g, str))
            moved = False
            for i, (r, out) in enumerate(props):
                rare = [g for g in out if isinstance(g, str) and cnt_new.get(genre_key(g), 0) <= 1 and genre_key(g) not in aud_g]
                if rare and len(out) - len(rare) >= 3: props[i] = (r, [g for g in out if g not in rare]); moved = True
            if not moved: break
    for r, out in props:
        if out and out != r['genres']:
            if abs_update(r['id'], f"{r['lib']}_{r['id']}", r['title'], {'genres': out}, {'genres': r['genres']}, 'genre_cleanup'):
                changed += 1
                gone = [g for g in r['genres'] if g not in out]
                logging.info(f"   🏷️ GENRES{' (DRY_RUN)' if dry else ''}: '{r['title']}': {', '.join(map(str, gone))} -> {len(out)} genres")
                r['genres'] = out  # the series fill after this step works on the cleaned list
    return changed


RATING_TAGS = (4.5, 4.7, 4.9)  # Audible overall rating tags (at least 50 ratings; user 2026-11-15: 4.9 instead of 4.8)
HC_RATING_TAGS = (4.0, 4.2, 4.4)  # Hardcover rating tags (user 2026-11-15; at least 50 ratings like Audible)
FULL_CAST_RE = re.compile(r'(?i)full[\s-]*cast|dramati[sz](?:ed|ation)|audio\s*drama|radio\s*drama|h(?:ö|oe)rspiel|graphic\s*audio')


def manage_tags(ctx, records):
    """Tags (user 2026-11-15): genre copies go (ABS put Audible's categories into the tags - filter by genre instead), themes that are
    no genres come in as English tags ('Dragons', 'Magic'), 'Full Cast' only for a real full cast (marked as full cast / dramatized /
    Hörspiel / GraphicAudio, or 5+ narrators - two narrators are no full cast). lock_* tags and the user's own tags stay; 'Audible 4.5+'
    is set per book (ratings)."""
    from collections import Counter
    abs_update, dry = ctx['abs_update'], ctx['dry_run']
    managed = set(_THEME_TAG.values()) | {'Full Cast'} | {f'Audible {lim}+' for lim in RATING_TAGS} | {f'Hardcover {lim}+' for lim in HC_RATING_TAGS}
    known = {genre_key(g) for r in records for g in r['genres'] if isinstance(g, str)} | set(_GENRE_CANON) | set(_GENRE_SYN_KEYS) | set(_THEME_KEYS) \
            | {genre_key(g) for g in ctx.get('audible_genres') or ()} | {genre_key(x) for x in (
                'Genre Fiction', 'Literatur & Belletristik', 'Klassiker', 'Action & Abenteuer', 'Abenteuer', 'Dystopien', 'Family & Relationships',
                'Animals & Nature', 'Christian Fiction', 'Collections & Anthologies', 'Anthologien & Kurzgeschichten', 'Kulturen & Geografie',
                'Mit Videospiel-', 'Film- & Fernsehbezug', 'Belletristik', 'Mystery, Thriller & Suspense', 'Kinder-Hörbücher', 'Jugendliche & Heranwachsende',
                'Science-Fiction et fantasy', 'Épique', 'Urbain et paranormal', 'Contemporain', "Women's Adventure", 'Historisch')}
    changed = 0
    for r in records:
        if r['tags'] & {'lock_all', 'lock_metadata', 'lock_tags'}: continue
        cur = [t for t in (r.get('media_tags') or []) if isinstance(t, str)]
        new = []
        for t in cur:
            if t.startswith('lock_') or t in managed: new.append(t)
            elif genre_key(t) in known:
                if (tt := theme_tag(t)) and tt not in new: new.append(tt)  # a theme copied into the tags stays as a theme tag
            else: new.append(t)  # the user's own tag
        new = list(dict.fromkeys(new))  # once each ('Dragons' from the book step and from a theme genre)
        for tt in sorted(r.get('_themes') or ()):
            if tt not in new: new.append(tt)
        # 'Audible 4.5+' (user 2026-11-15): Audible's overall rating of the rating block >= 4.5 with at least 50 ratings; gone when it drops
        m_ = re.search(r'Audible \((\d+)\):(?:<br\s*/?>|\s)*\S*\s*\S*\s*([\d.]+) / 5 - (?:Overall|Gesamt)', r.get('description') or '')
        # user 2026-11-15: cumulative - a 4.8 book has 'Audible 4.5+', 'Audible 4.7+' and 'Audible 4.8+' (filter 4.7+ = everything >= 4.7)
        # user 2026-11-15: only the HIGHEST level per source (a 4.9 book has 'Audible 4.9+' only), at least 50 ratings for both sources
        h_ = re.search(r'Hardcover \((\d+)\):(?:<br\s*/?>|\s)*\S*\s*\S*\s*([\d.]+) / 5 - (?:Rating|Bewertung)', r.get('description') or '')
        for src_, mm_, levels in (('Audible', m_, RATING_TAGS), ('Hardcover', h_, HC_RATING_TAGS)):
            best = max((l for l in levels if mm_ and int(mm_.group(1)) >= 50 and float(mm_.group(2)) >= l), default=None)
            for l in levels:
                tag_ = f'{src_} {l}+'
                if l == best and tag_ not in new: new.append(tag_)
                elif l != best and tag_ in new: new.remove(tag_)
        marker = ' '.join([r['title'] or '', r.get('subtitle') or '', r.get('path') or ''] + [x['name'] or '' for x in r['series']] + [str(g) for g in r['genres']] + list(r['narrators']))
        fc = bool(FULL_CAST_RE.search(marker)) or len({n.strip().lower() for n in r['narrators'] if n and n.strip()}) >= 5
        if fc and 'Full Cast' not in new: new.append('Full Cast')
        elif not fc and 'Full Cast' in new: new.remove('Full Cast')
        if new != cur and abs_update(r['id'], f"{r['lib']}_{r['id']}", r['title'], {}, {}, 'tag_cleanup', tags=new, old_tags=cur):
            changed += 1
            gone, added = [t for t in cur if t not in new], [t for t in new if t not in cur]
            logging.info(f"   🔖 TAGS{' (DRY_RUN)' if dry else ''}: '{r['title']}': {('-' + ', -'.join(gone)) if gone else ''}{' ' if gone and added else ''}{('+' + ', +'.join(added)) if added else ''}")
            r['media_tags'] = new
    return changed


LANG_NAMES = {'en': 'English', 'eng': 'English', 'english': 'English', 'de': 'German', 'deu': 'German', 'ger': 'German', 'german': 'German'}


def fix_languages(ctx, records, allowed):
    """Language consensus (user 2026-11-09: 'only German and English books, everything else is an error'). A book whose language is
    unknown ('und'), not one of the library's languages, or contradicts its series is set to the language of its series when
    (a) at least 2 other books of that series (with a known library language) exist and 90 % of them have that one language, and
    (b) one of them shares a narrator with it
    (the same narrator reads the series in that language: The Dolphins of Pern 'German', read by Mel Foster like the English Pern books).
    lock_language respected; only languages from BOOK_LANGUAGES."""
    if not allowed: return 0
    abs_update, dry = ctx['abs_update'], ctx['dry_run']
    lang = lambda r: LANG_NAMES.get(str(r.get('language') or '').strip().lower(), str(r.get('language') or '').strip())
    nk = lambda xs: {re.sub(r'[^a-z]', '', fold_accents(x).lower()) for x in xs or [] if x}
    by = {}
    for r in records:
        for s in r['series']: by.setdefault((r['lib'], series_key(s['name'])), []).append(r)
    changed = 0
    for r in records:
        if 'lock_language' in r['tags'] or 'lock_all' in r['tags'] or 'lock_metadata' in r['tags']: continue
        cur = lang(r)
        for s in r['series']:
            others = [o for o in by.get((r['lib'], series_key(s['name'])), []) if o['id'] != r['id'] and lang(o) in allowed]  # 'und'/wrong ones don't vote
            if len(others) < 2: continue
            from collections import Counter
            target, n_ = Counter(lang(o) for o in others).most_common(1)[0]
            if n_ < 0.9 * len(others): continue  # a mixed series (translations side by side) decides nothing
            others = [o for o in others if lang(o) == target]
            if target == cur or not any(nk(o['narrators']) & nk(r['narrators']) for o in others): continue
            if abs_update(r['id'], f"{r['lib']}_{r['id']}", r['title'], {'language': target}, {'language': r.get('language')}, 'language_consensus'):
                changed += 1; r['language'] = target
                logging.info(f"   🌐 LANGUAGE{' (DRY_RUN)' if dry else ''}: '{r['title']}': '{cur or '-'}' -> '{target}' (series '{s['name']}': all {len(others)} other books are {target}, same narrator)")
            break
    return changed


FOLDER_NUM_RE = re.compile(r"^(?:book\s*)?0*(\d+(?:\.\d+)?)\s+-\s+")


_LBL_WORD = r'(?:Books?|Volume|Vol\.|Band|Teil|Part)'
_LBL_SEP = rf'([,:]?\s+{_LBL_WORD}\s+|\s*#\s*|\s+)'


def _lbl_words(n):
    w = re.sub(r"[^a-z0-9\s]", ' ', fold_accents((n or '').lower()).replace("'", '')).split()
    while w and w[0] in LEADING_ARTICLES: w = w[1:]
    return ' '.join(w)


def _same_series_name(a, b):
    """a = the name in a label / title, b = the book's series name. 'The Murderbot Diaries' = 'Murderbot Diaries', 'Shadows and Crowns Series' = 'Shadows and Crowns', 'Gentleman Bastards' =
    'Gentleman Bastard' (article, generic word, a plural s). Never a part of a name: 'Legend of Drizzt: Dark Elf Trilogy' != 'Legend of Drizzt'."""
    k = lambda x: re.sub(r's\b', '', series_key(fold_accents(x or '').replace('’', "'").replace("'", '')))
    if bool(k(a)) and k(a) == k(b): return True
    # the part of a two-part series name: 'Jobless Reincarnation' / 'Mushoku Tensei' of 'Mushoku Tensei: Jobless Reincarnation'
    parts = [x for x in re.split(r'\s*:\s+', b or '') if len(x.split()) >= 2]
    return len(parts) == 2 and any(k(a) == k(x) for x in parts)


def _own_number(n, series, name=None):
    """n is the number of one of the book's series (that series' name ~ name, when a name is given) -> that series, else None."""
    for x in series or []:
        q = str(x.get('sequence') or '').strip()
        if q and re.fullmatch(r'\d+(?:\.\d+)?', q) and float(q) == float(n) and (name is None or _same_series_name(name, x.get('name'))):
            return x
    return None


def series_label(text, series, strict=False):
    """'Defiance of the Fall, Book 10' / 'Defiance of the Fall 10' / 'X #3' / 'Cinnamon Bun: Volume 3': the text is only one of the book's
    OWN series names + its own number -> (series name as written in the text, number, form). form = the separator as written
    (' ', ', Book ', ': Volume ', ' #'). strict: a word or '#' must stand before the number (a subtitle 'Apollo 13' is no label)."""
    m = re.match(rf'^\s*(.+?){_LBL_SEP}(\d+(?:\.\d+)?)\s*$', text or '', re.I)
    if not m: return None
    name, sep, n = m.group(1).strip(), m.group(2), m.group(3)
    if strict and not sep.strip(' ,:'): return None
    if not _own_number(n, series, name): return None
    form = re.sub(r'\s+', ' ', sep)
    form = form if form.strip() else ' '
    return name, n, re.sub(rf'(?i){_LBL_WORD}', lambda w: w.group(0)[0].upper() + w.group(0)[1:].lower(), form)


_WORD_NUM = {w: i for i, w in enumerate('one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen '
                                           'seventeen eighteen nineteen twenty'.split(), 1)}


def _roman(t):
    v = {'i': 1, 'v': 5, 'x': 10, 'l': 50, 'c': 100}; t = t.lower(); n = 0
    for i, ch in enumerate(t):
        n += -v[ch] if i + 1 < len(t) and v[ch] < v[t[i + 1]] else v[ch]
    return n


def _label_text(t):
    """Subtitle label spellings -> the plain form: '(Discworld Novel 7)' -> 'Discworld Book 7', 'Book Nine Of The Spellmonger Series'
    -> 'Book 9 of The Spellmonger Series', 'The Wheel Of Time, Book II' -> '..., Book 2', "Book Two of 'The Wheel of Time'", 'Expanse Bk 9'."""
    t = (t or '').strip().strip(',').strip()
    if re.fullmatch(r'\(([^()]+)\)', t): t = t[1:-1].strip()
    t = re.sub(r'(?i)\s*\(light novel\)', '', t)  # 'Jobless Reincarnation (Light Novel) Vol. 25'
    t = re.sub(r"['‘’\"“”]", '', t)
    t = re.sub(r'(?i)\b(?:novel|bk\.?|number|no\.)(?=\s+\w)', 'Book', t)
    t = re.sub(rf'(?i)\b({_LBL_WORD}|Book)\s+({"|".join(_WORD_NUM)})\b', lambda m: f"{m.group(1)} {_WORD_NUM[m.group(2).lower()]}", t)
    t = re.sub(rf'(?i)\b({_LBL_WORD}|Book)\s+([ivxlc]{{1,6}})\b', lambda m: f"{m.group(1)} {_roman(m.group(2))}", t)
    return t


def _subtitle_label(t, series):
    """The whole text only names the book's own series and number: 'Discworld, Book 1', 'Book 18 of the Spellmonger Series',
    'Book 2' / 'Volume 6' alone, 'Volume 4: My Quiet Blacksmith Life in Another World, Book 4', subtitle loop 2026-11-15: word and
    roman numbers, '(Discworld Novel 7)', 'Book Sixteen in The Wandering Inn Series', 'Number 3 in the Series', 'Expanse Bk 9'."""
    t = _label_text(t)
    if series_label(t, series, strict=True): return True
    if (m := re.match(rf'^{_LBL_WORD}\s+(\d+(?:\.\d+)?)\s+(?:of|in)\s+(.+)$', t, re.I)) and (
            _own_number(m.group(1), series, m.group(2)) or (re.fullmatch(r'(?i)the\s+series', m.group(2).strip()) and _own_number(m.group(1), series))): return True
    if (m := re.match(rf'^{_LBL_WORD}\s+(\d+(?:\.\d+)?)$', t, re.I)) and _own_number(m.group(1), series): return True
    if (m := re.match(rf'^({_LBL_WORD}\s+\d+(?:\.\d+)?)\s*:\s*(.+)$', t, re.I)) and _subtitle_label(m.group(1), series) and _subtitle_label(m.group(2), series): return True
    return False


_MARKETING = re.compile(r"(?i)\b(?:best-?sell\w*|award|winner|audies|netflix|prime original|now a major|sensation|instant (?:sunday|new york)|"
                        r"sunday times|new york times|meets the|prepare for|gripping|groundbreaking|charming|heart-warming|riotous)\b")
_EDITION = re.compile(r"(?i)^\W*(?:(?:collector'?s|deluxe|special|anniversary|illustrated|5x8|library|limited|signed|hardcover|paperback)\s*)+"
                      r"(?:edition)?(?:\s+hardcover)?\W*$|^\W*graphic novel\W*$|\[(?:paperback|hardcover|kindle)[^\]]*\]")


def clean_series_label_subtitle(sub, series, lang=None):
    """A subtitle that only names the book's own series and number goes ('Defiance of the Fall, Book 6' -> ''), a trailing one in
    brackets too ('A LitRPG Adventure (Defiance of the Fall, Book 5)' -> 'A LitRPG Adventure'). user 2026-11-15: the series field holds
    it already. Kept: another number ('The Murderbot Diaries, Book 8' on #7), a sub-series the book's series do not name ('Legend of
    Drizzt: Dark Elf Trilogy, Book 3' on 'Legend of Drizzt' #3), ranges ('Book 3, 4').
    Subtitle loop 2026-11-15, also gone: marketing lines ('An instant Sunday Times bestseller ...', 'WINNER OF THE AUDIES ...'),
    edition labels ("Collector's Edition", '5x8 Edition', 'Graphic Novel', '[Paperback]'), a format label alone ('Light Novel'), and on
    an English book a subtitle in another language ('La révolte' on Mockingjay)."""
    if not sub: return sub
    if _subtitle_label(sub, series): return ''
    if _MARKETING.search(sub) or _EDITION.search(sub): return ''
    if re.fullmatch(r'(?i)\W*(?:light novel|novel|unabridged|ungek(?:ü|ue)rzt|audiobook|hörbuch)\W*', sub): return ''
    if str(lang or '').lower() in ('english', 'eng', 'en'):
        w = re.findall(r"[a-zà-ÿ]+", sub.lower())
        f, e = sum(x in FOREIGN_WORDS for x in w), sum(x in ENDE_WORDS for x in w)
        if e == 0 and (f >= 2 or (f >= 1 and re.search(r'[à-ÿ]', sub.lower()))) or foreign_series_name(sub): return ''
    m = re.match(r'^(.*\S)\s*\(([^()]+)\)\s*$', sub)
    if m and _subtitle_label(m.group(2), series):
        rest = m.group(1).strip(' ,:-')
        return '' if re.fullmatch(r'(?i)light novel|novel|unabridged|ungek(?:ü|ue)rzt', rest) else rest  # 'Light Novel (Mushoku Tensei, Book 28)'
    return sub


def number_shared_titles(ctx, records):
    """Volumes of one series that are only titled with (a part of) the series name get their number (user 2026-11-15: 'Reborn to
    Master the Blade' 1 + 2 and 'Reborn to Master the Blade: From Hero-king ...' 3 + 4 -> 'Reborn to Master the Blade, Vol. 1' ...).
    Needs 2+ such volumes with different numbers; the shortest of their titles is used for all. A single volume titled like its series
    stays ('The Dark Tower' #7, 'Mistborn' #1). lock_title respected."""
    abs_update, dry = ctx['abs_update'], ctx['dry_run']
    num = lambda q: float(q) if re.fullmatch(r'\d+(?:\.\d+)?', str(q or '').strip()) else None
    groups = defaultdict(list)
    for r in records:
        for x in r['series']:
            sn, t = _lbl_words(x['name']), _lbl_words(r['title'])
            if t and num(x.get('sequence')) is not None and (t == sn or sn.startswith(t + ' ')) and not re.search(r'\d', r['title']):
                groups[(r['lib'], sn)].append((r, x))
    changed = 0
    for lst in groups.values():
        if len(lst) < 2 or len({num(x.get('sequence')) for _, x in lst}) < len(lst): continue
        base = min((r['title'] for r, _ in lst), key=len).strip().rstrip(':,')
        for r, x in lst:
            if r['tags'] & {'lock_title', 'lock_all', 'lock_metadata'}: continue
            q = str(x['sequence']).strip()
            new = f"{base}, Vol. {q}"
            if new != r['title'] and abs_update(r['id'], f"{r['lib']}_{r['id']}", r['title'], {'title': new}, {'title': r['title']}, 'shared_title_number'):
                changed += 1
                logging.info(f"   📚 TITLE{' (DRY_RUN)' if dry else ''}: '{r['title']}' -> '{new}' (several volumes had this title)")
                r['title'] = new
    return changed


def harmonize_series_titles(ctx, records):
    """Volumes titled only 'series name + number' get the form most of their series uses (user 2026-11-15: 'Defiance of the Fall, Book 10'
    among 'Defiance of the Fall 9', '... 11' -> 'Defiance of the Fall 10'). Needs at least 4 such titles in the series and 3 of 4 in one
    form. lock_title respected."""
    abs_update, dry = ctx['abs_update'], ctx['dry_run']
    groups = defaultdict(list)
    for r in records:
        for x in r['series']:
            lb = series_label(r['title'], [x])
            if lb: groups[(r['lib'], _lbl_words(x['name']))].append((r, lb))
    changed = 0
    for lst in groups.values():
        forms = defaultdict(list)
        for r, lb in lst: forms[lb[2]].append(lb[0])
        top = max(forms, key=lambda f: len(forms[f]))
        if len(lst) < 4 or len(forms[top]) < 3 or len(forms[top]) * 4 < len(lst) * 3: continue
        name = max(set(forms[top]), key=forms[top].count)  # the spelling the series uses
        for r, lb in lst:
            if lb[2] == top or r['tags'] & {'lock_title', 'lock_all', 'lock_metadata'}: continue
            new = f"{name}{top}{lb[1]}"
            if new != r['title'] and abs_update(r['id'], f"{r['lib']}_{r['id']}", r['title'], {'title': new}, {'title': r['title']}, 'series_title_form'):
                changed += 1
                logging.info(f"   📚 TITLE{' (DRY_RUN)' if dry else ''}: '{r['title']}' -> '{new}' (form of the other volumes)")
                r['title'] = new
    return changed


def fill_folder_numbers(ctx, records):
    """A book sits in series S without a number, and its folder is 'Author/S/N - Title' (the series folder IS the series): the folder
    number N becomes the series number - only in a series that is otherwise numbered (>= 75 %), only when N is unique in that folder (Elfhame/1 holds The Cruel Prince AND The Stolen Heir:
    the numbers belong to sub-series, the umbrella stays unnumbered) and no other book of S holds N. lock_series respected."""
    abs_update, dry = ctx['abs_update'], ctx['dry_run']
    folder_nums, held, counts = {}, {}, {}
    for r in records:
        for s in r['series']:
            c_ = counts.setdefault((r['lib'], series_key(s['name'])), [0, 0]); c_[0] += 1; c_[1] += bool(s.get('sequence'))
        parts = [p for p in str(r.get('path') or '').replace('\\', '/').split('/') if p.strip()]
        if len(parts) >= 3 and (m := FOLDER_NUM_RE.match(parts[-1])):
            folder_nums.setdefault((r['lib'], parts[-3].lower(), parts[-2].lower()), []).append(m.group(1))
        for s in r['series']:
            if s.get('sequence'): held.setdefault((r['lib'], series_key(s['name'])), set()).add(_n(float(s['sequence'])) if re.fullmatch(r"\d+(\.\d+)?", str(s['sequence'])) else s['sequence'])
    changed = 0
    for r in records:
        if 'lock_series' in r['tags'] or 'lock_all' in r['tags'] or 'lock_metadata' in r['tags']: continue
        parts = [p for p in str(r.get('path') or '').replace('\\', '/').split('/') if p.strip()]
        if len(parts) < 3 or not (m := FOLDER_NUM_RE.match(parts[-1])): continue
        num = _n(float(m.group(1)))
        if folder_nums.get((r['lib'], parts[-3].lower(), parts[-2].lower()), []).count(m.group(1)) != 1: continue
        new, did = [], None
        for s in r['series']:
            tot, numbered = counts.get((r['lib'], series_key(s['name'])), [0, 0])
            # only a GAP in a numbered series (an umbrella like 'The Cosmere' is unnumbered on purpose; the folder order is the user's)
            if not s.get('sequence') and series_key(s['name']) == series_key(parts[-2]) and num not in held.get((r['lib'], series_key(s['name'])), set()) \
                    and numbered >= 2 and numbered >= 0.75 * tot:
                s = dict(s, sequence=num); did = s['name']
            new.append(s)
        if did and abs_update(r['id'], f"{r['lib']}_{r['id']}", r['title'], {'series': new}, {'series': r['series']}, 'folder_number'):
            changed += 1; held.setdefault((r['lib'], series_key(did)), set()).add(num); r['series'] = new
            logging.info(f"   🔢 NUMBER{' (DRY_RUN)' if dry else ''}: '{r['title']}': '{did}' #{num} (book folder '{parts[-1][:40]}')")
    return changed


def drop_duplicate_series(ctx, records):
    """The SAME series twice on one book under two names (user 2026-11-15: 'So You Want to Live the Slow Life?' #3 and 'So You Want to Live
    the Slow Life! A Guide to Life in the Beastly Wilds' #3; 'Konosuba' #13 from the folder next to 'Konosuba: God's Blessing on This
    Wonderful World!' #13): two series on one book look like umbrella + sub-series, so the merge never joined them.
    A name D goes only when (1) its full words (punctuation and a leading article aside - NOT the generic-word stripping of series_key:
    'The First Law Trilogy' is no prefix of 'First Law World') start with, or are the start of, the other name K, (2) EVERY book of D
    also holds K with the same number, and (3) K also has books without D (K stands on its own; 'Irwin's Journey' and 'Irwin's
    Journey: The Cardsmith' always together = umbrella + sub-series, both stay). Fake-write test 2026-11-15: First Law Trilogy,
    Mistborn Era 1 and Irwin's Journey must stay. lock_series respected."""
    abs_update, dry = ctx['abs_update'], ctx['dry_run']
    num = lambda q: float(q) if re.fullmatch(r"\d+(\.\d+)?", str(q or '').strip()) else None
    def words(n):
        w = re.sub(r"[^a-z0-9\s]", ' ', fold_accents((n or '').lower()).replace("'", '')).split()  # '_' is punctuation too
        while w and w[0] in LEADING_ARTICLES: w = w[1:]
        return ' '.join(w)
    changed = 0
    raw_n = {}
    for r in records:
        for x in r['series']: raw_n[(r['lib'], x['name'])] = raw_n.get((r['lib'], x['name']), 0) + 1
    for r in records:  # the same name twice, only punctuation differs ('The Haunted Bookstore - Gateway...' / '..._ Gateway...'), same number
        if 'lock_series' in r['tags'] or 'lock_all' in r['tags'] or 'lock_metadata' in r['tags']: continue
        pos, new = {}, []
        for x in r['series']:
            k_ = (words(x['name']), num(x.get('sequence')))
            if k_[1] is not None and k_ in pos:  # keep the spelling more books use
                if raw_n.get((r['lib'], x['name']), 0) > raw_n.get((r['lib'], new[pos[k_]]['name']), 0): new[pos[k_]] = dict(x)
                continue
            pos[k_] = len(new); new.append(dict(x))
        if len(new) < len(r['series']) and abs_update(r['id'], f"{r['lib']}_{r['id']}", r['title'], {'series': new}, {'series': r['series']}, 'duplicate_series'):
            changed += 1
            logging.info(f"   📚 SERIES{' (DRY_RUN)' if dry else ''}: '{r['title']}': one series name twice (punctuation) -> {', '.join(x['name'] for x in new)}")
            r['series'] = new
    books_of = {}
    for r in records:
        for x in r['series']: books_of.setdefault((r['lib'], words(x['name'])), []).append((r, num(x.get('sequence'))))
    drop = {}
    for (lib, kd), lst in books_of.items():
        for (lib2, kk), lst2 in books_of.items():
            if lib2 != lib or kk == kd or not kd or not kk: continue
            # one name starts the other (either way: 'Slow Life?' / 'Slow Life! A Guide to ...'), or the DROPPED name is the shorter one
            # inside the kept ('Witness' in 'Tales of Witness (Malazan Universe)'). Never a longer name inside a shorter one: 'The Last
            # King of Osten Ard' is a real sub-series of 'Osten Ard' with the same numbers (fake-write check 2026-11-15)
            if not (kk.startswith(kd + ' ') or kd.startswith(kk + ' ') or f' {kd} ' in f' {kk} '): continue
            ids_k = {id(r) for r, _ in lst2}
            if not all(any(series_key(y['name']) and words(y['name']) == kk and num(y.get('sequence')) == q and q is not None for y in r['series']) for r, q in lst): continue
            if not any(id(r) not in {id(r_) for r_, _ in lst} for r, _ in lst2): continue  # K has books of its own
            drop[(lib, kd)] = kk
    for (lib, kd), kk in drop.items():
        books = [r for r, _ in books_of[(lib, kd)]]
        if any('lock_series' in r['tags'] or 'lock_all' in r['tags'] or 'lock_metadata' in r['tags'] for r in books): continue
        for r in books:
            gone = [x['name'] for x in r['series'] if words(x['name']) == kd]
            new = [dict(x) for x in r['series'] if words(x['name']) != kd]
            if gone and abs_update(r['id'], f"{r['lib']}_{r['id']}", r['title'], {'series': new}, {'series': r['series']}, 'duplicate_series'):
                changed += 1
                logging.info(f"   📚 SERIES{' (DRY_RUN)' if dry else ''}: '{r['title']}': removed '{gone[0]}' (same series as '{next(x['name'] for x in new if words(x['name']) == kk)}', same number)")
                r['series'] = new
    return changed


def drop_folder_only_series(ctx, records):
    """A NEW book (added within new_book_days) holds a series that only came from its folder name: the name of the book's parent
    folder, Audible does not list it, no other book has it - while the book also has a series Audible lists. (2026-11-15: 'No Life
    Forsaken' sat in 'Malazan (In-World Chronological Order)/2 - ...' and got that series #2 next to Audible's 'Tales of Witness' #2;
    user: series must be right even when a folder is named wrong.) lock_series respected."""
    abs_update, dry = ctx['abs_update'], ctx['dry_run']
    aud = {series_key(n) for n in (ctx.get('audible_series_names') or ())}
    days = float(ctx.get('new_book_days') or 0)
    if not aud or not days: return 0
    import time as _t
    count = {}
    for r in records:
        for x in r['series']: count[(r['lib'], series_key(x['name']))] = count.get((r['lib'], series_key(x['name'])), 0) + 1
    changed = 0
    for r in records:
        if 'lock_series' in r['tags'] or 'lock_all' in r['tags'] or 'lock_metadata' in r['tags']: continue
        if not r.get('added') or _t.time() - r['added'] / 1000 > days * 86400: continue
        parts = [p for p in str(r.get('path') or '').replace('\\', '/').split('/') if p.strip()]
        if len(parts) < 3: continue
        fk = series_key(parts[-2])
        gone = [x for x in r['series'] if series_key(x['name']) == fk and fk not in aud and count.get((r['lib'], fk), 0) == 1]
        keep = [x for x in r['series'] if x not in gone]
        if not gone or not any(series_key(x['name']) in aud for x in keep): continue
        if abs_update(r['id'], f"{r['lib']}_{r['id']}", r['title'], {'series': [dict(x) for x in keep]}, {'series': r['series']}, 'folder_only_series'):
            changed += 1
            logging.info(f"   📚 SERIES{' (DRY_RUN)' if dry else ''}: '{r['title']}': removed '{gone[0]['name']}' (only from the folder name, Audible does not list it)")
            r['series'] = keep
    return changed


def fill_folder_series(ctx, records):
    """A book in a series folder 'Author/S/Book' without series S while EVERY other book of that folder (at least 2) has S and shares
    an author with it -> S is added without a number (user 2026-11-10: The Ambassador's Mission sat in 'Kyralia Universe' next to The
    Rogue and The Traitor Queen but only had Audible's 'Traitor Spy Trilogy'; the sub-series rule then numbers it). lock_series respected."""
    abs_update, dry = ctx['abs_update'], ctx['dry_run']
    dirs = {}
    for r in records:
        parts = [p for p in str(r.get('path') or '').replace('\\', '/').split('/') if p.strip()]
        if len(parts) >= 3: dirs.setdefault((r['lib'], '/'.join(parts[:-1]).lower()), []).append((r, parts[-2]))
    changed = 0
    for (lib, _), group in dirs.items():
        for r, sfolder in group:
            if 'lock_series' in r['tags'] or 'lock_all' in r['tags'] or 'lock_metadata' in r['tags']: continue
            k = series_key(sfolder)
            if any(series_key(x['name']) == k for x in r['series']): continue
            others = [o for o, _ in group if o is not r]
            names = [next((x['name'] for x in o['series'] if series_key(x['name']) == k), None) for o in others]
            if len(others) < 2 or None in names: continue
            mine = {author_key(a) for a in r['authors']}
            if not mine or not all(mine & {author_key(a) for a in o['authors']} for o in others): continue
            name = max(set(names), key=names.count)
            new = [dict(x) for x in r['series']] + [{'name': name, 'sequence': None}]
            if abs_update(r['id'], f"{r['lib']}_{r['id']}", r['title'], {'series': new}, {'series': r['series']}, 'folder_series'):
                changed += 1; r['series'] = new
                logging.info(f"   📚 SERIES{' (DRY_RUN)' if dry else ''}: '{r['title']}': + '{name}' (every other book of its folder has it)")
    return changed


def fill_from_subseries(ctx, records):
    """Umbrella number from a sub-series (user 2026-11-10, First Law World): a book in umbrella U WITHOUT a number but in sub-series S
    with number k, while another book of S has U #M at S #m (Last Argument of Kings: First Law World #3 = First Law Trilogy #3) ->
    U #(M - m + k) (The Blade Itself = First Law Trilogy #1 -> First Law World #1). Only when every book of S that has both numbers
    gives the same offset, the result is a whole number not held by another book of U, and no lock_series."""
    abs_update, dry = ctx['abs_update'], ctx['dry_run']
    num = lambda q: float(q) if re.fullmatch(r"\d+(\.\d+)?", str(q or '').strip()) else None
    held, offs = {}, {}
    for r in records:
        sers = [(series_key(x['name']), num(x.get('sequence'))) for x in r['series']]
        for k_, q in sers:
            if q is not None: held.setdefault((r['lib'], k_), set()).add(q)
        for ku, qu in sers:
            for ks, qs in sers:
                if ku != ks and qu is not None and qs is not None:
                    offs.setdefault((r['lib'], ku, ks), set()).add(qu - qs)
    changed = 0
    for r in records:
        if 'lock_series' in r['tags'] or 'lock_all' in r['tags'] or 'lock_metadata' in r['tags']: continue
        new, did = [dict(x) for x in r['series']], []
        for x in new:
            ku = series_key(x['name'])
            if num(x.get('sequence')) is not None or x.get('sequence'): continue
            cands = set()
            for y in new:
                ks, qs = series_key(y['name']), num(y.get('sequence'))
                o = offs.get((r['lib'], ku, ks))
                if qs is None or ks == ku or not o: continue
                if len(o) != 1: cands = None; break  # the sub-series does not map onto the umbrella with ONE offset
                cands.add(next(iter(o)) + qs)
            if not cands or len(cands) != 1: continue
            q = cands.pop()
            if q != int(q) or q < 1 or q in held.get((r['lib'], ku), set()): continue
            x['sequence'] = str(int(q)); did.append(f"{x['name']} #{int(q)}")
            held.setdefault((r['lib'], ku), set()).add(q)
        if did and abs_update(r['id'], f"{r['lib']}_{r['id']}", r['title'], {'series': new}, {'series': r['series']}, 'subseries_number'):
            changed += 1; r['series'] = new
            logging.info(f"   🔢 NUMBER{' (DRY_RUN)' if dry else ''}: '{r['title']}': {', '.join(did)} (from its sub-series)")
    # A whole sub-series without umbrella numbers that came out AFTER every numbered book of the umbrella continues it (user 2026-11-10:
    # Kyralia Universe 1-3 = Black Magician Trilogy, then Traitor Spy 1-3 -> 4-6 even when book 1 is missing): umbrella max + sub number.
    # Only when the umbrella's whole numbers are gapless 1..N, exactly ONE such sub-series is left and every one of its books is newer.
    yr = lambda r: int(str(r.get('year'))[:4]) if re.match(r'\d{4}', str(r.get('year') or '')) else None
    by_u = {}
    for r in records:
        for x in r['series']:
            by_u.setdefault((r['lib'], series_key(x['name'])), []).append(r)
    for (lib, ku), books in by_u.items():
        if any('lock_series' in r['tags'] or 'lock_all' in r['tags'] for r in books): continue
        ints = sorted({int(q) for r in books for x in r['series'] if series_key(x['name']) == ku and (q := num(x.get('sequence'))) is not None and q == int(q) and q >= 1})
        if not ints or ints != list(range(1, ints[-1] + 1)): continue
        # whole numbers only: a later prequel (The Magician's Apprentice #0.5, 2013) does not hide that Traitor Spy (2011) followed #1-3
        years_num = [yr(r) for r in books if any(series_key(x['name']) == ku and (q := num(x.get('sequence'))) and q == int(q) and q >= 1 for x in r['series'])]
        if not years_num or None in years_num: continue
        rest = [r for r in books if any(series_key(x['name']) == ku and not x.get('sequence') for x in r['series'])]
        subs = {}
        for r in rest:
            sub = [(series_key(x['name']), x['name'], num(x.get('sequence'))) for x in r['series'] if series_key(x['name']) != ku and num(x.get('sequence'))]
            if len(sub) != 1: subs = None; break
            subs.setdefault(sub[0][0], []).append((r, sub[0][2]))
        if not subs or len(subs) != 1: continue
        (ks, members), = subs.items()
        if any(yr(r) is None or yr(r) <= max(years_num) for r, _ in members): continue
        if any(q != int(q) for _, q in members) or len({q for _, q in members}) != len(members): continue
        for r, q in members:
            newq = ints[-1] + int(q)
            new = [dict(x, sequence=str(newq)) if series_key(x['name']) == ku and not x.get('sequence') else dict(x) for x in r['series']]
            if abs_update(r['id'], f"{r['lib']}_{r['id']}", r['title'], {'series': new}, {'series': r['series']}, 'subseries_number'):
                changed += 1; r['series'] = new
                logging.info(f"   🔢 NUMBER{' (DRY_RUN)' if dry else ''}: '{r['title']}': umbrella #{newq} (its sub-series follows #1-{ints[-1]})")
    return changed


def wrong_languages(records, allowed):
    """Review list (user 2026-11-09): books whose language is unknown or not one of BOOK_LANGUAGES after every automatic fix."""
    if not allowed: return []
    out = []
    for r in records:
        if 'lock_all' in r['tags']: continue
        l = LANG_NAMES.get(str(r.get('language') or '').strip().lower(), str(r.get('language') or '').strip())
        if l not in allowed:
            out.append({'id': f"lang|{r['lib']}|{r['id']}", 'key': f"{r['lib']}_{r['id']}", 'title': r['title'], 'language': r.get('language') or '',
                        'authors': r['authors'][:2], 'narrators': r['narrators'][:2], 'path': r.get('path') or '',
                        'locked': 'lock_language' in r['tags']})
    return sorted(out, key=lambda x: x['title'].lower())


def remove_role_credits(ctx, records, known_roles=()):
    """Only real authors in the author field: role credits ('X - illustrator', 'Y - translator', 'Z - Übersetzer', ...) are removed
    when at least one real author remains. A plain name counts as a role credit when it was seen WITH a role suffix somewhere
    (this library or Audible, e.g. 'Kurone Mishima - illustrator') AND it is nowhere the first author of a book. lock_authors respected."""
    abs_update, dry = ctx['abs_update'], ctx['dry_run']
    roles = {author_key(role_base(a)) for r in records for a in r['authors'] if is_role_credit(a)} | {author_key(n) for n in known_roles}
    first = {author_key(r['authors'][0]) for r in records if r['authors'] and not is_role_credit(r['authors'][0])}
    changed = 0
    for r in records:
        if not r['authors'] or 'lock_authors' in r['tags']: continue
        drop = [a for i, a in enumerate(r['authors']) if is_role_credit(a) or (i > 0 and author_key(a) in roles and author_key(a) not in first)]
        keep = [a for a in r['authors'] if a not in drop]
        if not drop or not keep: continue  # never leave a book without author
        if abs_update(r['id'], f"{r['lib']}_{r['id']}", r['title'], {'authors': [{'name': a} for a in keep]},
                      {'authors': [{'name': a} for a in r['authors']]}, 'role_credit_removed'):
            changed += 1
            logging.info(f"   🎨 ONLY AUTHORS{' (DRY_RUN)' if dry else ''}: '{r['title']}': removed {', '.join(drop)}")
    return changed


def remove_empty_authors(ctx):
    """Delete ABS author objects that have NO book (left-overs). Every candidate is re-checked right before deleting
    (GET /api/authors/<id>?include=items must list 0 books); books are never touched. Journaled (not undoable: an empty author)."""
    s, url, dry = ctx['session'], ctx['abs_url'].rstrip('/'), ctx['dry_run']
    removed = 0
    ids = ctx['library_ids'] if isinstance(ctx['library_ids'], (list, tuple, set)) else str(ctx['library_ids']).replace(';', ',').split(',')
    for lib in [str(l).strip() for l in ids if str(l).strip()]:
        try: authors = s.get(f"{url}/api/libraries/{lib}/authors", timeout=60).json().get('authors') or []
        except Exception as e: logging.warning(f"   Could not load authors of {lib}: {e}"); continue
        for a in authors:
            if a.get('numBooks') not in (0, None) or not a.get('id'): continue
            try:
                d = s.get(f"{url}/api/authors/{a['id']}", params={'include': 'items'}, timeout=30).json()
                if d.get('libraryItems') or d.get('numBooks'): continue  # has books after all: never delete
            except Exception: continue
            if not dry:
                r = s.delete(f"{url}/api/authors/{a['id']}", timeout=30)
                if r.status_code != 200: logging.warning(f"   Could not delete empty author '{a.get('name')}' (HTTP {r.status_code})"); continue
            ctx['journal'](f"author:{a['id']}", '', a.get('name') or '?', 'author', a.get('name'), None, 'empty_author_removed')
            removed += 1
            logging.info(f"   🧹 EMPTY AUTHOR{' (DRY_RUN)' if dry else ''} removed: '{a.get('name')}' (0 books)")
    return removed


def write_series_index(ctx, records, merges):
    """series_index.json: normalized series key -> name used in ABS (+ authors), after merges.
    Used by the Audible sync to put new books into existing series."""
    renamed = {(m['lib'], s.name): m['target'].name for m in merges for s in m['members']} if not ctx['dry_run'] else {}
    books = defaultdict(lambda: defaultdict(lambda: {'count': 0, 'authors': set()}))
    for r in records:
        for s in r['series']:
            name = renamed.get((r['lib'], s['name']), s['name'])
            k = f"{','.join(sorted(series_variant(name)))}|{series_key(name)}"
            e = books[(r['lib'], k)][name]
            e['count'] += 1
            e['authors'].update(author_key(a) for a in r['authors'])
    index = defaultdict(dict)
    for (lib, k), names in books.items():
        if not k.split('|', 1)[1]: continue
        name, e = sorted(names.items(), key=lambda x: (-x[1]['count'], x[0]))[0]
        index[lib][k] = {'name': name, 'names': sorted(names), 'authors': sorted(set().union(*(v['authors'] for v in names.values())))}
    try:
        with open(ctx['index_file'], 'w', encoding='utf-8') as f: json.dump(index, f, indent=1, ensure_ascii=False)
    except OSError as e: logging.error(f"   Could not write series index: {e}")


# ================= REPORT =================

def build_report(ctx, records, merges, rejected, extra):
    """Collect findings (ABS data only) and write reports/library_report.html + .json."""
    rep = {'generated': datetime.now().strftime('%Y-%m-%d %H:%M'), 'dry_run': ctx['dry_run'], 'books': len(records)}
    member_to_target = {(m['lib'], s.name): m['target'].name for m in merges for s in m['members']}

    # Series after (planned) merges: gaps and books without number
    series = defaultdict(list)
    for r in records:
        for s in r['series']:
            name = member_to_target.get((r['lib'], s['name']), s['name'])
            series[(r['lib'], name)].append((r, s.get('sequence')))
    gaps, no_seq = [], []
    for (lib, name), entries in sorted(series.items(), key=lambda x: x[0][1].lower()):
        nums = sorted({int(n) for n in (parse_seq(q) for _, q in entries) if n is not None and float(n).is_integer()})
        if len(nums) >= 2:
            missing = [x for x in range(nums[0], nums[-1] + 1) if x not in nums]
            if missing: gaps.append({'series': name, 'have': fmt_seqs(nums), 'missing': fmt_seqs(missing)})
        for r, q in entries:
            if parse_seq(q) is None and len(entries) > 1: no_seq.append({'series': name, 'title': r['title']})

    # Duplicates (same ASIN twice in one library)
    by_asin = defaultdict(list)
    for r in records:
        if r['asin']: by_asin[(r['lib'], r['asin'])].append(r['title'])
    dups = [{'asin': a, 'titles': t} for (_, a), t in by_asin.items() if len(t) > 1]

    # Duplicate RECORDINGS: same author + same narrator + duration within 2 s, different title/folder (the same book twice in the library)
    rec_groups = defaultdict(list)
    for r in records:
        if r.get('duration') and r['authors'] and r['narrators']:
            rec_groups[(r['lib'], tuple(sorted(author_key(a) for a in r['authors'])), tuple(sorted(n.lower() for n in r['narrators'])))].append(r)
    dup_rec = []
    for grp in rec_groups.values():
        grp.sort(key=lambda x: x['duration'])
        for i, a_ in enumerate(grp):
            for b_ in grp[i + 1:]:
                if b_['duration'] - a_['duration'] > 2: break
                dup_rec.append({'titles': [a_['title'], b_['title']], 'seconds': int(a_['duration']), 'asins': [a_['asin'], b_['asin']]})

    # Author spelling variants across the library
    variants = defaultdict(set)
    for r in records:
        for a in r['authors']: variants[author_key(a)].add(a)
    author_var = [sorted(v) for v in variants.values() if len(v) > 1]

    def missing(pred): return sorted(r['title'] for r in records if pred(r) and 'lock_all' not in r['tags'])
    no_desc_re = re.compile(r'(?s)(?:⭐|─{3,})\s*(?:Ratings|Bewertungen)\b.*?<br\s*/?>\s*(?:⭐|─{3,})(?=\s*(?:<|$))')
    rep.update({
        'merges': [{'target': m['target'].name, 'target_books': fmt_seqs(m['target'].seqs),
                    'merged': [{'name': s.name, 'books': fmt_seqs(s.seqs), 'count': len(s.entries), 'match': s.relation} for s in m['members']]} for m in merges],
        'not_merged': rejected, 'series_gaps': gaps, 'series_without_number': no_seq, 'duplicate_asins': dups, 'duplicate_recordings': dup_rec,
        'author_variants_library': author_var,
        'author_fixes': [{'from': k, 'to': v} for k, v in sorted(extra.get('author_fixes', {}).items())], 'author_variants_audible': extra.get('author_notes', []),
        'unmapped_genres': sorted(extra.get('unmapped_genres', [])),
        'health': library_health(records, os.path.join(os.path.dirname(ctx['report_dir']), 'results.json')),
        'missing': {
            'asin': missing(lambda r: not r['asin']), 'cover': missing(lambda r: not r['cover']),
            'description': missing(lambda r: not re.sub(r'<[^>]+>|\s', '', no_desc_re.sub('', r['description']))),
            'narrators': missing(lambda r: not r['narrators']), 'language': missing(lambda r: not r['language']),
            'genres': missing(lambda r: not r['genres']), 'authors': missing(lambda r: not r['authors']),
            'publisher': missing(lambda r: not r.get('publisher')), 'year': missing(lambda r: not r.get('year')), 'isbn': missing(lambda r: not r.get('isbn')),
            'series': missing(lambda r: not r.get('series')),
        },
    })
    os.makedirs(ctx['report_dir'], exist_ok=True)
    with open(os.path.join(ctx['report_dir'], 'library_report.json'), 'w', encoding='utf-8') as f:
        json.dump(rep, f, indent=2, ensure_ascii=False)
    with open(os.path.join(ctx['report_dir'], 'library_report.html'), 'w', encoding='utf-8') as f:
        f.write(render_html(rep))
    return rep


LANG_STORE = {'english': 'com', 'eng': 'com', 'en': 'com', 'german': 'de', 'deu': 'de', 'ger': 'de', 'de': 'de', 'deutsch': 'de'}

def library_health(records, results_path=None):
    """Checks of the whole library after every scan (ABS data + results.json, no Audible request) - what was checked by hand on
    2026-11-08: no ASIN, an ASIN on several books, ASIN not from the preferred store, non-Latin series names, a series number held by
    3+ different books, the book folder naming another volume, series spellings that differ only in case/spaces."""
    try: results = json.load(open(results_path, encoding='utf-8')) if results_path and os.path.exists(results_path) else {}
    except (OSError, ValueError): results = {}
    out = defaultdict(list)
    key = lambda r: f"{r['lib']}_{r['id']}"
    add = lambda k, r, why='': out[k].append({'key': key(r), 'title': r['title'], 'why': why})
    by_asin, by_title, seq_titles, names = defaultdict(list), defaultdict(list), defaultdict(set), defaultdict(set)
    for r in records:
        if 'lock_all' in r['tags']: continue
        lang = str(r.get('language') or '').strip().lower()
        if not r['asin']: add('no_asin', r)
        else: by_asin[(r['lib'], r['asin'])].append(r)
        want = LANG_STORE.get(lang)
        log = ' '.join((results.get(key(r)) or {}).get('log') or [])
        dom = re.findall(r'SUCCESS on www\.audible\.([a-z.]+)', log)
        if want and dom and dom[-1] != want and 'lock_asin' not in r['tags']: add('wrong_store', r, f".{dom[-1]} ({r.get('language')})")
        bad_ser = [x['name'] for x in r['series'] if foreign_for_book(x['name'], r.get('language'))]
        if want and bad_ser: add('non_latin_series', r, ', '.join(bad_ser) + (' (lock_series)' if 'lock_series' in r['tags'] else ''))
        if r['authors']: by_title[(r['lib'], author_key(r['authors'][0]), re.sub(r'\W+', ' ', r['title'].lower()).strip())].append(r)
        for x in r['series']:
            names[(r['lib'], x['name'].strip().lower())].add(x['name'])
            if x.get('sequence'): seq_titles[(r['lib'], x['name'], str(x['sequence']), LANG_STORE.get(lang, lang))].add(re.sub(r'\W+', ' ', r['title'].lower()).strip())  # per language: a translation in two parts is no junk
    for (_, a), rs in by_asin.items():
        if len(rs) > 1:
            for r in rs: add('shared_asin', r, a)
    junk = {k[:3] for k, t in seq_titles.items() if distinct_works(t) >= 3}
    for r in records:
        hit = [f"{x['name']} #{x['sequence']}" for x in r['series'] if (r['lib'], x['name'], str(x.get('sequence'))) in junk]
        if hit and 'lock_all' not in r['tags']: add('junk_number', r, ', '.join(hit))
    for rs in by_title.values():
        if len(rs) < 2: continue
        for r in rs:
            parts = [p for p in r.get('path', '').split('/') if p]
            m = re.match(r'^(?:book\s*)?(\d+(?:\.\d+)?)\s+-\s+', parts[-1]) if len(parts) >= 3 else None
            if not m: continue
            nums = {parse_seq(x.get('sequence')) for x in r['series']} - {None}
            nums |= {float(v) for v in re.findall(r'(?i)\b(?:vol(?:ume)?\.?|book|part)\s*(\d+)\b', r['title'])}
            if nums and float(m.group(1)) not in nums: add('folder_conflict', r, f"folder {m.group(1)}, ABS {', '.join(f'{n:g}' for n in sorted(nums))}")
    for (lib, low), ns in names.items():
        if len(ns) > 1:
            for r in records:
                if r['lib'] == lib and any(x['name'] in ns for x in r['series']): add('case_variants', r, ' / '.join(sorted(ns)))
    return {k: v for k, v in out.items()}


def render_html(rep):
    e = lambda x: html.escape(str(x))
    def table(rows, cols):
        if not rows: return '<p class="ok">Nothing found.</p>'
        head = ''.join(f'<th>{e(c)}</th>' for c in cols)
        body = ''.join('<tr>' + ''.join(f'<td>{e(r.get(c, "") if isinstance(r, dict) else r)}</td>' for c in cols) + '</tr>' for r in rows)
        return f'<table><tr>{head}</tr>{body}</table>'
    def lst(items):
        if not items: return '<p class="ok">Nothing found.</p>'
        return '<ul>' + ''.join(f'<li>{e(i)}</li>' for i in items) + '</ul>'
    merges = ''.join(f"<div class='card'><b>{e(m['target'])}</b> (#{e(m['target_books'])})<ul>" +
                     ''.join(f"<li>&larr; {e(x['name'])} (#{e(x['books'])}, {x['count']} books, name match: {e(x['match'])})</li>" for x in m['merged']) +
                     "</ul></div>" for m in rep['merges']) or '<p class="ok">No series to merge.</p>'
    miss = ''.join(f"<details><summary>No {e(k)}: {len(v)}</summary>{lst(v)}</details>" for k, v in rep['missing'].items())
    return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>ABS Library Report</title><style>
:root{{--bg:#fff;--fg:#1d1d1f;--muted:#6e6e73;--line:#e5e5ea;--card:#f5f5f7;--ok:#1a7f37}}
@media (prefers-color-scheme:dark){{:root{{--bg:#1c1c1e;--fg:#f2f2f7;--muted:#a1a1a6;--line:#38383a;--card:#2c2c2e;--ok:#4cc26a}}}}
body{{background:var(--bg);color:var(--fg);font:15px/1.5 system-ui,sans-serif;max-width:1000px;margin:0 auto;padding:16px}}
h1{{font-size:1.5em}} h2{{margin-top:2em;border-bottom:1px solid var(--line)}} .muted{{color:var(--muted)}} .ok{{color:var(--ok)}}
table{{border-collapse:collapse;width:100%;display:block;overflow-x:auto}} td,th{{border-bottom:1px solid var(--line);padding:4px 8px;text-align:left;vertical-align:top}}
.card{{background:var(--card);border-radius:8px;padding:8px 12px;margin:8px 0}} summary{{cursor:pointer;padding:4px 0}}
</style></head><body>
<h1>ABS Library Report</h1><p class="muted">{e(rep['generated'])} · {rep['books']} books{' · DRY RUN (nothing was changed)' if rep['dry_run'] else ''}</p>
<h2>Series merges ({len(rep['merges'])})</h2>{merges}
<h2>Similar series not merged ({len(rep['not_merged'])})</h2>{table(rep['not_merged'], ['series', 'other', 'reason'])}
<h2>Gaps in series ({len(rep['series_gaps'])})</h2><p class="muted">Missing numbers between the lowest and highest book you have. Can be books you don't own.</p>{table(rep['series_gaps'], ['series', 'have', 'missing'])}
<h2>Books without series number ({len(rep['series_without_number'])})</h2>{table(rep['series_without_number'], ['series', 'title'])}
<h2>Duplicate ASINs ({len(rep['duplicate_asins'])})</h2>{table([{'asin': d['asin'], 'titles': ' | '.join(d['titles'])} for d in rep['duplicate_asins']], ['asin', 'titles'])}
<h2>Duplicate recordings ({len(rep.get('duplicate_recordings', []))}) - the same book twice (same author, narrator, length within 2 s)</h2>{table([{'titles': ' | '.join(d['titles']), 'seconds': d['seconds'], 'asins': ' | '.join(str(a) for a in d['asins'])} for d in rep.get('duplicate_recordings', [])], ['titles', 'seconds', 'asins'])}
<h2>Duplicate recordings ({len(rep.get('duplicate_recordings', []))}) – the same book twice (same author, narrator, length within 2 s)</h2>{table([{'titles': ' | '.join(d['titles']), 'seconds': d['seconds'], 'asins': ' | '.join(str(a) for a in d['asins'])} for d in rep.get('duplicate_recordings', [])], ['titles', 'seconds', 'asins'])}
<h2>Author spellings unified ({len(rep['author_fixes'])})</h2>{table(rep['author_fixes'], ['from', 'to'])}
<h2>Author spelling ABS vs Audible ({len(rep['author_variants_audible'])})</h2>{table([{'title': n['title'], 'abs': ', '.join(n['abs']), 'audible': n['audible']} for n in rep['author_variants_audible']], ['title', 'abs', 'audible'])}
<h2>Missing fields</h2>{miss}
<h2>Untranslated audible.de genres ({len(rep['unmapped_genres'])})</h2>{lst(rep['unmapped_genres'])}
</body></html>"""


def _load(path):
    try:
        with open(path, encoding='utf-8') as f: return json.load(f)
    except (OSError, ValueError, TypeError): return {}

def _save(path, data):
    if not path: return
    try:
        with open(path + '.tmp', 'w', encoding='utf-8') as f: json.dump(data, f, indent=1, ensure_ascii=False)
        os.replace(path + '.tmp', path)
    except OSError as e: logging.error(f"   Could not write {path}: {e}")


def run(ctx):
    """Entry point. ctx: session, abs_url, library_ids, dry_run, abs_update, report_dir, merge, report, extra."""
    logging.info("=" * 60)
    logging.info(f"📚 SERIES & LIBRARY TASK (ABS only, no Audible/Goodreads requests){' - DRY_RUN' if ctx['dry_run'] else ''}")
    records = collect_records(ctx)
    logging.info(f"   {len(records)} books loaded.")
    ctx['decisions'] = _load(ctx.get('decisions_file'))
    ctx['merge_history'] = _load(ctx.get('merges_file'))
    merges, rejected = find_merges(records, ctx['decisions'], ctx.get('audible_series_names'))
    changed = authors_changed = 0
    if ctx['merge']:
        logging.info(f"   Series merges found: {len(merges)}")
        changed = apply_merges(ctx, merges)
        for r in rejected:
            logging.info(f"   ⏭️ Not merged: '{r['series']}' / '{r['other']}': {r['reason']}")
        if not ctx['dry_run'] and ctx.get('aliases'):
            try:
                with open(ctx['aliases_file'], 'w', encoding='utf-8') as f: json.dump(ctx['aliases'], f, indent=2, ensure_ascii=False)
            except OSError as e: logging.error(f"   Could not write series aliases: {e}")
    write_series_index(ctx, records, merges if ctx['merge'] else [])
    fixes, author_review = {}, []
    if ctx.get('author_normalize'):
        fixes, count, author_review = find_author_fixes(records, ctx['decisions'], ctx.get('author_links'), ctx.get('role_names') or ())
        logging.info(f"   Author spelling variants: {len(fixes)} (review: {len(author_review)})")
        authors_changed = apply_author_fixes(ctx, records, fixes, count)
    if ctx.get('remove_role_credits'):
        authors_changed += remove_role_credits(ctx, records, ctx.get('role_names') or ())
    if ctx.get('remove_empty_authors'):
        ctx['empty_authors_removed'] = remove_empty_authors(ctx)
    if ctx.get('book_languages'):
        ctx['languages_fixed'] = fix_languages(ctx, records, ctx['book_languages'])
    if ctx.get('merge'):
        ctx['series_tidied'] = tidy_series_entries(ctx, records)
        ctx['duplicate_series'] = drop_duplicate_series(ctx, records)
        ctx['folder_only_series'] = drop_folder_only_series(ctx, records)
        ctx['folder_series'] = fill_folder_series(ctx, records)
        ctx['folder_numbers'] = fill_folder_numbers(ctx, records)
        ctx['subseries_numbers'] = fill_from_subseries(ctx, records)
        ctx['shared_titles'] = number_shared_titles(ctx, records)
        ctx['series_titles'] = harmonize_series_titles(ctx, records)
    ctx['narrators_split'] = split_joined_narrators(ctx, records)
    ctx['shared_desc'] = len(shared_descriptions(ctx, records))
    if ctx.get('genre_cleanup'):
        ctx['genres_cleaned'] = normalize_genres(ctx, records)
        ctx['genres_series'] = series_genre_fill(ctx, records)
        ctx['tags_changed'] = manage_tags(ctx, records)
        logging.info(f"   Genre spellings cleaned: {ctx['genres_cleaned']} books")
    # Web UI files: merge history, uncertain cases, decisions (a confirmed merge may have been reopened)
    series_review = []
    for r in rejected:
        if r['reason'].startswith(UNCERTAIN):
            mid = merge_id('series', r['lib'], r['series'], r['other'])
            if ctx['decisions'].get(mid, {}).get('action') in ('keep', 'undone'): continue
            series_review.append(dict(r, id=mid, kind='series'))
    other_review = foreign_series_books(records, ctx['decisions'])
    _save(ctx.get('merges_file'), ctx['merge_history'])
    _save(ctx.get('review_file'), {'series': series_review, 'authors': author_review, 'other': other_review,
                                   'languages': wrong_languages(records, ctx.get('book_languages') or []),
                                   'generated': datetime.now().strftime('%Y-%m-%d %H:%M')})
    if not ctx['dry_run']: _save(ctx.get('decisions_file'), ctx['decisions'])
    if ctx['report']:
        extra = dict(ctx.get('extra', {}), author_fixes=fixes)
        rep = build_report(ctx, records, merges if ctx['merge'] else [], rejected, extra)
        logging.info(f"   📄 Report written: {os.path.join(ctx['report_dir'], 'library_report.html')} "
                     f"(gaps: {len(rep['series_gaps'])}, duplicates: {len(rep['duplicate_asins'])}, "
                     f"author variants: {len(rep['author_variants_library'])})")
    return {'merges': len(merges), 'books_changed': changed, 'author_fixes': len(fixes), 'authors_changed': authors_changed}
