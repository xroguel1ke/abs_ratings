"""Source health (user request 2026-11-09): notice when Audible / Hardcover / Goodreads change their API or website, instead of
silently treating every book as 'not found'.

1. Canary check before the per-book part: one known book per source must come back with every field the matching rules need
   (title, authors, narrators, runtime, ratings, series). A failed check switches the source off for this run - nothing is counted
   as a miss, nothing is written from it - and the run summary / notification says why.
2. Breaker during the run: a source that normally finds most books (>= BREAKER_RATE in source_state.json) and suddenly finds
   nothing for BREAKER_STREAK books in a row is switched off; the misses of that streak are taken back (the books stay due and are
   asked again in the next run instead of being paused for REFRESH_DAYS).

No network code here: the callers pass the fetch functions in, so the checks are testable offline (tests/test_source_health.py)."""
import threading

BREAKER_STREAK = 15
BREAKER_RATE = 0.7
BREAKER_MIN_BOOKS = 50  # base rate only counts with enough history; a new library relies on the canary

# Known books whose answers have every field (popular, long-lived listings). Values are only checked for presence / plausibility.
AUDIBLE_CANARIES = [('B002V0QCYU', 'www.audible.com'),  # The Final Empire (Mistborn 1), Michael Kramer
                    ('B0092PNN4O', 'www.audible.de')]   # Der Name des Windes, Stefan Kaminski
HARDCOVER_CANARY = {'search': 'The Final Empire Brandon Sanderson', 'title_word': 'final empire'}


def check_audible_product(p):
    """Problems of one Audible API result (the dict get_audible_api returns), [] if every needed field is there."""
    if not isinstance(p, dict): return [f"no product ({p!r})"]
    bad = []
    if not (p.get('title_raw') or '').strip(): bad.append('title')
    if not p.get('authors_raw'): bad.append('authors')
    if not p.get('narrators_raw'): bad.append('narrators')
    if not (p.get('runtime_sec') or 0) > 3600: bad.append('runtime')
    if not int(p.get('count') or 0) > 0: bad.append('ratings')
    if not ((p.get('meta_raw') or {}).get('series')): bad.append('series')
    if not ((p.get('meta_raw') or {}).get('language')): bad.append('language')
    return bad


def check_hardcover_hits(data, title_word):
    """Problems of a Hardcover search answer for the canary query, [] if a hit with title, rating and id is there."""
    hits = (((data or {}).get('search') or {}).get('results') or {}).get('hits') if isinstance(data, dict) else None
    if data is None: return ['no answer']
    if not isinstance(hits, list): return ['search result format (no hits list)']
    docs = [h.get('document') or {} for h in hits if isinstance(h, dict)]
    top = [d for d in docs if title_word in str(d.get('title') or '').lower()]
    if not top: return [f"canary book not in the {len(docs)} hits"]
    d = top[0]
    bad = [f for f, ok in (('id', d.get('id')), ('rating', d.get('rating') is not None), ('ratings_count', d.get('ratings_count')),
                           ('author_names', d.get('author_names'))) if not ok]
    return bad


def run_canaries(get_audible, hc_search=None, log=print, only=None):
    """-> {source: reason} of the sources that failed. get_audible(asin, domain) -> product dict | 'missing' | None;
    hc_search(query) -> raw GraphQL data | None (None = Hardcover off)."""
    down = {}
    for asin, dom in (AUDIBLE_CANARIES if only in (None, 'audible') else []):
        try: p = get_audible(asin, dom)
        except Exception as e: p = f"{type(e).__name__}: {e}"
        bad = check_audible_product(p)
        if bad:
            down['audible'] = f"check book {asin} on {dom}: {', '.join(bad)} missing or unreadable"
            break
    if hc_search and only in (None, 'hardcover'):
        try: data = hc_search(HARDCOVER_CANARY['search'])
        except Exception as e: data = None; log(f"Hardcover check failed: {type(e).__name__}: {e}")
        bad = check_hardcover_hits(data, HARDCOVER_CANARY['title_word'])
        if bad: down['hardcover'] = f"check search '{HARDCOVER_CANARY['search']}': {', '.join(bad)}"
    return down


class Breaker:
    """Counts books in a row that a normally reliable source did not find. state: source_state.json dict."""
    def __init__(self, state, sources, streak=BREAKER_STREAK, rate=BREAKER_RATE, min_books=BREAKER_MIN_BOOKS):
        self.lock, self.streak_n, self.tripped = threading.Lock(), streak, {}
        self.rate, self.runs = {}, {s: [] for s in sources}
        for s in sources:
            es = [b.get(s) for b in state.values() if isinstance(b, dict) and isinstance(b.get(s), dict)]
            if len(es) >= min_books: self.rate[s] = sum(1 for e in es if e.get('st') == 'found') / len(es)

    def armed(self, source): return self.rate.get(source, 0) >= BREAKER_RATE

    def note(self, key, source, outcome, prev):
        """Call BEFORE the outcome is stored, with the previous entry. Returns True when the streak is long enough to SUSPECT the
        source; the caller then re-checks it with the canary and calls trip() (source broken) or clear() (just unlucky books:
        twenty Japanese-only light novels in a row are no API change)."""
        with self.lock:
            if source in self.tripped or source not in self.runs: return False
            if outcome == 'found': self.runs[source] = []; return False
            if outcome not in ('missing', 'absent'): return False
            self.runs[source].append((key, prev))
            return self.armed(source) and len(self.runs[source]) >= self.streak_n

    def clear(self, source):
        with self.lock: self.runs[source] = []

    def trip(self, state, source, detail=''):
        """Source broken: the misses of the streak are taken back (these books are asked again next run). Returns the reason."""
        with self.lock:
            run = self.runs.get(source) or []
            for k, old in run:
                book = state.get(k)
                if book is None: continue
                if old is None: book.pop(source, None)
                else: book[source] = old
            why = (f"{len(run)} books in a row without a result (normally {self.rate.get(source, 0):.0%} found)"
                   + (f"; check: {detail}" if detail else ''))
            self.tripped[source], self.runs[source] = why, []
            return why
