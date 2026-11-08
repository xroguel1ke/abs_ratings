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
                  'lock_authors', 'lock_cover', 'lock_explicit', 'lock_abridged', 'lock_isbn', 'lock_asin'}


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
ROLE_CREDIT_RE = re.compile(r'\s+[-–]\s+(illustrat\w*|illustrations?|translat\w*|translated by|edited by|editor|editors|introductions?|foreword|afterword|preface|contributor|adapt\w*|cover\s+\w+|'
                            r'übersetz\w*|übertragen\w*|herausgeb\w*|hrsg\.?|zeichn\w*|illustriert\w*|vorwort|nachwort|bearbeit\w*)\s*$', re.I)


def is_role_credit(name): return bool(name and ROLE_CREDIT_RE.search(name))


def fold_accents(s):
    """'Kurmaić' -> 'Kurmaic'"""
    return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))

def is_latin(name):
    return not re.search(r"[^\x00-\u024F]", name or "")

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
        for it in items:
            try: d = sess.get(f"{url}/api/items/{it['id']}").json()
            except Exception as e:
                logging.warning(f"   Could not load item {it.get('id')}: {e}"); continue
            media = d.get('media') or {}
            md = media.get('metadata') or {}
            if d.get('mediaType', 'book') != 'book': continue
            raw_tags = (d.get('tags') or []) + (media.get('tags') or []) + (md.get('tags') or [])
            records.append({
                'id': d['id'], 'lib': lib, 'title': md.get('title') or '?',
                'authors': [a.get('name') if isinstance(a, dict) else a for a in (md.get('authors') or []) if a],
                'narrators': md.get('narrators') or [], 'asin': md.get('asin'), 'isbn': md.get('isbn'), 'publisher': md.get('publisher'), 'year': md.get('publishedYear'), 'subtitle': md.get('subtitle'),
                'series': [{'name': s.get('name'), 'sequence': s.get('sequence')} for s in (md.get('series') or []) if s.get('name')],
                'media_tags': list(media.get('tags') or []), 'tags': expand_locks({t.strip() for t in raw_tags if isinstance(t, str)}),
                'description': md.get('description') or '', 'language': md.get('language'), 'genres': md.get('genres') or [],
                'cover': media.get('coverPath'), 'duration': media.get('duration'),
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


def apply_author_fixes(ctx, records, fixes, count):
    abs_update, dry = ctx['abs_update'], ctx['dry_run']
    changed = 0
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
    no_desc_re = re.compile(r'(?s)⭐\s*Ratings.*?⭐')
    rep.update({
        'merges': [{'target': m['target'].name, 'target_books': fmt_seqs(m['target'].seqs),
                    'merged': [{'name': s.name, 'books': fmt_seqs(s.seqs), 'count': len(s.entries), 'match': s.relation} for s in m['members']]} for m in merges],
        'not_merged': rejected, 'series_gaps': gaps, 'series_without_number': no_seq, 'duplicate_asins': dups, 'duplicate_recordings': dup_rec,
        'author_variants_library': author_var,
        'author_fixes': [{'from': k, 'to': v} for k, v in sorted(extra.get('author_fixes', {}).items())], 'author_variants_audible': extra.get('author_notes', []),
        'unmapped_genres': sorted(extra.get('unmapped_genres', [])),
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
                                   'generated': datetime.now().strftime('%Y-%m-%d %H:%M')})
    if not ctx['dry_run']: _save(ctx.get('decisions_file'), ctx['decisions'])
    if ctx['report']:
        extra = dict(ctx.get('extra', {}), author_fixes=fixes)
        rep = build_report(ctx, records, merges if ctx['merge'] else [], rejected, extra)
        logging.info(f"   📄 Report written: {os.path.join(ctx['report_dir'], 'library_report.html')} "
                     f"(gaps: {len(rep['series_gaps'])}, duplicates: {len(rep['duplicate_asins'])}, "
                     f"author variants: {len(rep['author_variants_library'])})")
    return {'merges': len(merges), 'books_changed': changed, 'author_fixes': len(fixes), 'authors_changed': authors_changed}
