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


def fold_accents(s):
    """'Kurmaić' -> 'Kurmaic'"""
    return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))

def is_latin(name):
    return not re.search(r"[^\x00-\u024F]", name or "")

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


UNCERTAIN = ('not enough numbered books', "numbers don't complement", 'matches more than one series')

def merge_id(kind, lib, member, target): return f"{kind}|{lib}|{member}->{target}"

def merge_check(group, cand, decisions=None):
    """Can 'cand' be merged into 'group' (list of Series, first = target)? Returns (ok, relation, reason).
    Your decisions from the web UI (merge / keep separate / undone) win over the rules."""
    target = group[0]
    rel = name_relation(target, cand)
    dec = (decisions or {}).get(merge_id('series', getattr(cand, 'lib', ''), cand.name, target.name), {}).get('action')
    if dec in ('keep', 'undone'): return False, rel or 'manual', 'kept separate (your decision)'
    if dec == 'merge': return True, 'your decision', 'ok'
    if not rel: return False, None, 'names not similar'
    if target.variant != cand.variant:
        return False, rel, f"different edition ({', '.join(sorted(target.variant)) or 'plain'} vs {', '.join(sorted(cand.variant)) or 'plain'})"
    g_authors = set().union(*(s.authors for s in group))
    if not (g_authors & cand.authors): return False, rel, 'no shared author'
    # A manually locked book means the user decided on this series name: don't touch the series
    locked = [r['title'] for r, _ in cand.entries if 'lock_series' in r['tags']]
    if locked: return False, rel, f"contains book(s) with lock_series: {', '.join(locked[:3])}"
    # Every single book of the smaller series must share an author with the target
    foreign = [r['title'] for r, _ in cand.entries if not ({author_key(a) for a in r['authors']} & g_authors)]
    if foreign: return False, rel, f"book(s) by other authors: {', '.join(foreign[:3])}"
    # Books that are in both series (duplicate entries) don't count as conflicts
    g_ids = set().union(*(s.item_ids for s in group))
    g_seqs = {parse_seq(q) for s in group for r, q in s.entries if parse_seq(q) is not None and r['id'] not in cand.item_ids}
    c_seqs = {parse_seq(q) for r, q in cand.entries if parse_seq(q) is not None and r['id'] not in g_ids}
    if overlap := g_seqs & c_seqs:
        return False, rel, f"book numbers overlap ({fmt_seqs(overlap)})"
    if rel != 'exact':
        # Fuzzy names need the book numbers to complement each other (fill gaps or continue the series)
        if not c_seqs or not g_seqs: return False, rel, 'not enough numbered books to prove it is the same series'
        lo, hi = min(g_seqs), max(g_seqs)
        fills = any(lo < x < hi or x == hi + 1 or x == lo - 1 for x in c_seqs)
        if not fills: return False, rel, f"numbers don't complement ({fmt_seqs(g_seqs)} vs {fmt_seqs(c_seqs)})"
    return True, rel, 'ok'


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
                'narrators': md.get('narrators') or [], 'asin': md.get('asin'), 'isbn': md.get('isbn'),
                'series': [{'name': s.get('name'), 'sequence': s.get('sequence')} for s in (md.get('series') or []) if s.get('name')],
                'media_tags': list(media.get('tags') or []), 'tags': expand_locks({t.strip() for t in raw_tags if isinstance(t, str)}),
                'description': md.get('description') or '', 'language': md.get('language'), 'genres': md.get('genres') or [],
                'cover': media.get('coverPath'), 'duration': media.get('duration'),
            })
    return records


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
                        rejected.append({'lib': lib, 'series': ser.name, 'other': g[0].name,
                                         'reason': 'matches more than one series (ambiguous)'})
                for tgt, reason in related:
                    rejected.append({'lib': lib, 'series': ser.name, 'other': tgt.name, 'reason': reason})
                groups.append([ser])
        for g in groups:
            if len(g) > 1:
                hits = [s for s in g if s.name in (preferred_names or ())]
                if len(hits) == 1 and hits[0] is not g[0]:  # Audible's own name wins
                    g.remove(hits[0]); g.insert(0, hits[0])
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
        record_merge(ctx, 'series', m['lib'], tgt.name, [(s.name, fmt_seqs(s.seqs), len(s.entries), s.relation) for s in m['members']],
                     fmt_seqs(tgt.seqs))
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


def record_merge(ctx, kind, lib, target, members, target_books):
    """merges.json: every automatic or decided merge (shown in the web UI 'Merged' tab)."""
    hist, dec, now = ctx.setdefault('merge_history', {}), ctx.setdefault('decisions', {}), datetime.now().strftime('%Y-%m-%d %H:%M')
    for name, books, count, relation in members:
        mid = merge_id(kind, lib, name, target)
        old = hist.get(mid)
        if old and old.get('member_books') != books and dec.get(mid, {}).get('action') == 'confirmed' and not ctx['dry_run']:
            dec.pop(mid, None)  # new books were merged: show it again
        hist[mid] = {'id': mid, 'kind': kind, 'lib': lib, 'target': target, 'member': name, 'member_books': books,
                     'member_count': count, 'target_books': target_books, 'relation': relation,
                     'status': 'planned' if ctx['dry_run'] and (not old or old.get('status') == 'planned') else 'applied',
                     'first_seen': (old or {}).get('first_seen', now), 'last_seen': now}


# ================= AUTHORS =================

def find_author_fixes(records, decisions=None, links=None):
    """Spelling variants of the same author ('R. A. Salvatore' / 'R.A. Salvatore'): the spelling used
    by more books wins (tie: alphabetical). Returns ({variant: canonical}, counts, review list).
    Uncertain pairs ('J. Smith' / 'John Smith') go to the review list; your decisions win."""
    decisions = decisions or {}
    count = defaultdict(lambda: defaultdict(int))
    for r in records:
        for a in r['authors']: count[author_key(a)][a] += 1
    fixes = {}
    for key, spellings in count.items():
        if len(spellings) < 2: continue
        canon = sorted(spellings.items(), key=lambda x: (-x[1], 0 if fold_accents(x[0]) != x[0] else 1, x[0]))[0][0]
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
        canon = max(present, key=lambda n: (sum(count[author_key(n)].values()) if author_key(n) in count else 0, fold_accents(n) != n, n))
        for n in present:
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
                if act == 'merge':
                    for sp in count[small]: fixes[sp] = canon
                elif act not in ('keep', 'undone'):
                    review.append({'id': mid, 'kind': 'author', 'lib': '', 'series': variant, 'other': canon,
                                   'reason': 'first name vs. initial', 'series_books': total[small], 'other_books': total[big]})
    review = [r for r in review if r["series"] not in fixes]  # already resolved by proof
    return fixes, count, review


def apply_author_fixes(ctx, records, fixes, count):
    abs_update, dry = ctx['abs_update'], ctx['dry_run']
    changed = 0
    for variant, canon in sorted(fixes.items()):
        logging.info(f"   ✍️ AUTHOR{' (DRY_RUN)' if dry else ''}: '{variant}' ({count[author_key(variant)][variant]} books) -> '{canon}' ({count[author_key(canon)][canon]} books)")
        record_merge(ctx, 'author', '', canon, [(variant, '', count[author_key(variant)][variant], 'spelling')], '')
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
    return changed


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
        'not_merged': rejected, 'series_gaps': gaps, 'series_without_number': no_seq, 'duplicate_asins': dups,
        'author_variants_library': author_var,
        'author_fixes': [{'from': k, 'to': v} for k, v in sorted(extra.get('author_fixes', {}).items())], 'author_variants_audible': extra.get('author_notes', []),
        'unmapped_genres': sorted(extra.get('unmapped_genres', [])),
        'missing': {
            'asin': missing(lambda r: not r['asin']), 'cover': missing(lambda r: not r['cover']),
            'description': missing(lambda r: not re.sub(r'<[^>]+>|\s', '', no_desc_re.sub('', r['description']))),
            'narrators': missing(lambda r: not r['narrators']), 'language': missing(lambda r: not r['language']),
            'genres': missing(lambda r: not r['genres']), 'authors': missing(lambda r: not r['authors']),
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
        fixes, count, author_review = find_author_fixes(records, ctx['decisions'], ctx.get('author_links'))
        logging.info(f"   Author spelling variants: {len(fixes)} (review: {len(author_review)})")
        authors_changed = apply_author_fixes(ctx, records, fixes, count)
    # Web UI files: merge history, uncertain cases, decisions (a confirmed merge may have been reopened)
    series_review = []
    for r in rejected:
        if r['reason'].startswith(UNCERTAIN):
            mid = merge_id('series', r['lib'], r['series'], r['other'])
            if ctx['decisions'].get(mid, {}).get('action') in ('keep', 'undone'): continue
            series_review.append(dict(r, id=mid, kind='series'))
    _save(ctx.get('merges_file'), ctx['merge_history'])
    _save(ctx.get('review_file'), {'series': series_review, 'authors': author_review,
                                   'generated': datetime.now().strftime('%Y-%m-%d %H:%M')})
    if not ctx['dry_run']: _save(ctx.get('decisions_file'), ctx['decisions'])
    if ctx['report']:
        extra = dict(ctx.get('extra', {}), author_fixes=fixes)
        rep = build_report(ctx, records, merges if ctx['merge'] else [], rejected, extra)
        logging.info(f"   📄 Report written: {os.path.join(ctx['report_dir'], 'library_report.html')} "
                     f"(gaps: {len(rep['series_gaps'])}, duplicates: {len(rep['duplicate_asins'])}, "
                     f"author variants: {len(rep['author_variants_library'])})")
    return {'merges': len(merges), 'books_changed': changed, 'author_fixes': len(fixes), 'authors_changed': authors_changed}
