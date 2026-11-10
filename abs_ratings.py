import os
import re
import time
import requests
from bs4 import BeautifulSoup
import re, json, random, difflib, logging, urllib.parse, unicodedata
import html as html_lib
import recorder
import source_state as SST; recorder.install()
import source_health as SH
import missing_series as MS
import threading, queue as _queue_mod
from concurrent.futures import ThreadPoolExecutor
import atexit
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ================= PARALLEL BOOKS (thread safety) =================
WORKERS = max(1, int(os.getenv('WORKERS', 3)))  # books processed at the same time; requests stay 1 per host
IO_LOCK = threading.RLock()       # every read-modify-write of a shared state file / dict that is also dumped
_TL = threading.local()           # per-thread: current item log, last search evidence, lookup hit counts, stats, log buffer
_HOST_LOCKS, _THROTTLE_LOCKS = {}, {}

class _TLDict:
    """A dict that is separate for every thread (LAST_MATCH, LOOKUP_HITS): code keeps using it like the old global dict."""
    def __init__(self, name, default=None): self._n, self._d = name, default or {}
    def _get(self):
        d = getattr(_TL, self._n, None)
        if d is None: d = dict(self._d); setattr(_TL, self._n, d)
        return d
    def __getitem__(self, k): return self._get()[k]
    def __setitem__(self, k, v): self._get()[k] = v
    def __contains__(self, k): return k in self._get()
    def __iter__(self): return iter(self._get())
    def __len__(self): return len(self._get())
    def __bool__(self): return bool(self._get())
    def get(self, k, d=None): return self._get().get(k, d)
    def clear(self): self._get().clear()
    def update(self, *a, **kw): self._get().update(*a, **kw)
    def keys(self): return self._get().keys()
    def items(self): return self._get().items()
    def copy(self): return dict(self._get())

class _Stats(dict):
    """Run counters: every worker counts in its own dict (no lost '+= 1'), merge() adds them up after the workers finished.
    Flags shared by all workers (aborted_ratelimit) live in the main dict."""
    SHARED = {'aborted_ratelimit'}
    def _local(self):
        return getattr(_TL, 'stats', None)
    def __getitem__(self, k):
        loc = self._local()
        if loc is None or k in self.SHARED: return dict.__getitem__(self, k)
        return loc.get(k, 0)
    def __setitem__(self, k, v):
        loc = self._local()
        if loc is None or k in self.SHARED: dict.__setitem__(self, k, v)
        else: loc[k] = v
    def get(self, k, d=None):
        loc = self._local()
        if loc is None or k in self.SHARED: return dict.get(self, k, d)
        return loc.get(k, d if d is not None else 0)
    def merge(self, loc):
        with IO_LOCK:
            for k, v in loc.items():
                if isinstance(v, (int, float)) and not isinstance(v, bool): dict.__setitem__(self, k, dict.get(self, k, 0) + v)

def _host_serialized(orig):
    """At most ONE request in flight per host, whatever thread sends it (wraps requests.Session.request)."""
    def request(self, method, url, *a, **kw):
        host = urllib.parse.urlsplit(str(url)).hostname or ''
        lock = _HOST_LOCKS.setdefault(host, threading.Lock())
        with lock: return orig(self, method, url, *a, **kw)
    request._host_serialized = True
    return request
if not getattr(requests.Session.request, '_host_serialized', False):
    requests.Session.request = _host_serialized(requests.Session.request)

class _BookLogRouter(logging.Handler):
    """Forwards records to the real handlers; inside a worker the records of one book are buffered and written as one block."""
    def __init__(self, handlers):
        super().__init__(); self.real = handlers
    def emit(self, record):
        buf = getattr(_TL, 'logbuf', None)
        if buf is not None: buf.append(record); return
        for h in self.real: h.handle(record)
    def flush_book(self):
        buf = getattr(_TL, 'logbuf', None)
        if not buf: return
        _TL.logbuf = []
        with IO_LOCK:
            # '(n/total)' = books FINISHED so far (in queue order the workers finish out of order: 174, 214, 135 ... made the progress jump)
            for r in buf:
                if isinstance(r.msg, str) and (m := re.match(r'^\((\d+)/(\d+)\)', r.msg)):
                    BOOKS_DONE[0] += 1
                    rest = re.sub(r'^ \[ETA: [^\]]*\]', lambda _: f" [ETA: {eta_text(BOOKS_DONE[0], int(m.group(2)))}]", r.msg[m.end():])
                    r.msg = f"({BOOKS_DONE[0]}/{m.group(2)})" + rest
                    break
            for r in buf:
                for h in self.real: h.handle(r)
LOG_ROUTER = None
BOOKS_DONE = [0]  # finished books of the current run_books() (progress numbering with parallel workers)
RUN_T0 = [None]   # start of the current run_books()
DONE_TS = __import__('collections').deque(maxlen=150)  # finish times of the last books (rolling speed)

def eta_text(done, total):
    """Remaining time from the books FINISHED so far. Was: elapsed / queue index at the START of a book - with parallel workers the
    index is the queue position (an author group with index 1230 runs early), so the ETA jumped between 4 s and 40 h. Now: speed of
    the last 150 finished books (a slow phase - Goodreads cool-down, many searches - shows up quickly), blended with the run average."""
    now = time.time()
    DONE_TS.append(now)
    if not RUN_T0[0] or done < 3: return '…'
    overall = done / max(1.0, now - RUN_T0[0])
    rate = overall
    if len(DONE_TS) >= 20 and DONE_TS[-1] > DONE_TS[0]:
        rate = 0.7 * ((len(DONE_TS) - 1) / (DONE_TS[-1] - DONE_TS[0])) + 0.3 * overall
    return format_time(max(0, total - done) / rate) if rate > 0 else '…'


# ================= CONFIGURATION =================
def clean_env(value, allowed):
    """Remove invisible characters that sneak in via copy & paste (zero-width spaces, line breaks, quotes...)."""
    raw = os.getenv(value, '') or ''
    cleaned = re.sub(f'[^{allowed}]', '', raw)
    return cleaned, len(raw) - len(cleaned)

ABS_URL, _URL_REMOVED = clean_env('ABS_URL', r'A-Za-z0-9:/._\-\[\]%')
ABS_URL = ABS_URL.rstrip('/')
API_TOKEN, _TOKEN_REMOVED = clean_env('API_TOKEN', r'A-Za-z0-9._\-')  # JWT: only these characters
LIBRARY_IDS = [l.strip() for l in os.getenv('LIBRARY_IDS', '').split(',') if l.strip()]

# DATA_DIR: where history, caches, logs and reports live (the container sets it to /config).
SCRIPT_DIR = os.getenv('DATA_DIR') or os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")
REPORT_DIR = os.path.join(SCRIPT_DIR, "reports")
HISTORY_FILE = os.path.join(SCRIPT_DIR, "rating_history.json")
FAILED_FILE = os.path.join(SCRIPT_DIR, "failed_history.json")
RECHECK_FILE = os.path.join(SCRIPT_DIR, "recheck_history.json")  # key -> UTC time of an early re-check
GR_CACHE_FILE = os.path.join(SCRIPT_DIR, "goodreads_cache.json")  # key -> Goodreads book URL
RUN_ID = os.getenv('RUN_ID') or datetime.now().strftime('%Y%m%d-%H%M%S')  # the web UI passes its own id
RESULTS_FILE = os.path.join(SCRIPT_DIR, "results.json")      # last result per book (status, reason, log excerpt)
OVERRIDES_FILE = os.path.join(SCRIPT_DIR, "overrides.json")  # manual fixes from the web UI: asin, goodreads, ignore
ONLY_ITEMS = [x.strip() for x in os.getenv('ONLY_ITEMS', '').split(',') if x.strip()]  # 'libid_itemid' -> single-book mode
ABS_BACKUP = os.getenv('ABS_BACKUP', 'false').lower() == 'true'  # create an ABS backup before a real run
BOOK_LANGUAGES = []  # filled after LANGUAGE_MAP (below): languages the library holds; an Audible product in another language is never taken
MISSING_SERIES = os.getenv('MISSING_SERIES', 'true').lower() == 'true'  # report missing volumes of the library's series (reports/missing_in_series.json)
MISSING_NOVELLAS = os.getenv('MISSING_NOVELLAS', 'false').lower() == 'true'
SERIES_BOOKS = {}  # (lib, series name lower) -> [{'num', 'title', 'asin', 'lang', 'author'}] from the listing
GENRE_CLEANUP = os.getenv('GENRE_CLEANUP', 'true').lower() == 'true'  # one spelling per genre, no number-only / foreign-script genres (abs_series.normalize_genres)
REMOVE_ROLE_CREDITS = os.getenv('REMOVE_ROLE_CREDITS', 'true').lower() == 'true'  # only real authors: remove 'X - illustrator', 'Y - translator', ...
REMOVE_EMPTY_AUTHORS = os.getenv('REMOVE_EMPTY_AUTHORS', 'true').lower() == 'true'  # delete ABS author objects with 0 books
ROLE_NAMES_FILE = os.path.join(SCRIPT_DIR, 'role_credits.json')  # names seen with a role suffix on Audible ('Kurone Mishima - illustrator')
ROLE_NAMES = set()
SERIES_ALIASES_FILE = os.path.join(SCRIPT_DIR, "series_aliases.json")  # {library: {"alias name": "series name"}} (editable)
SERIES_INDEX_FILE = os.path.join(SCRIPT_DIR, "series_index.json")      # Series that exist in ABS (written by the series task)
CHANGES_FILE = os.path.join(SCRIPT_DIR, "changes.jsonl")  # Every applied change (old -> new), used by abs_undo.py
PLANNED_FILE = os.path.join(REPORT_DIR, f"planned_changes_{RUN_ID}.jsonl")  # DRY_RUN: what would have been changed
ENV_OUTPUT_FILE = os.path.join(SCRIPT_DIR, "last_run.env")

REFRESH_DAYS = int(os.getenv('REFRESH_DAYS', 90))
MAX_BATCH_SIZE = int(os.getenv('BATCH_SIZE', 150))
MAX_FAIL_ATTEMPTS = 5
MAX_CONSECUTIVE_RL = 3
RECOVERY_PAUSE = 60
BASE_SLEEP = float(os.getenv('SLEEP_TIMER', 0))  # Extra pause between items. Every source has its own throttle
                                                 # (MIN_INTERVAL), so 0 is safe; the pause only slows the run down.
SEARCH_PENALTY_SLEEP = float(os.getenv('SEARCH_PENALTY_SLEEP', 0))  # Optional extra pause after ASIN searches
# Minimum seconds between two requests to the same source
MIN_INTERVAL = {
    'audible_api': float(os.getenv('AUDIBLE_API_INTERVAL', 0.5)),  # per marketplace (each store is its own server)
    'audible_web': float(os.getenv('AUDIBLE_WEB_INTERVAL', 3)),
    'goodreads': float(os.getenv('GOODREADS_INTERVAL', 10)),
    'hardcover': float(os.getenv('HARDCOVER_INTERVAL', 1.1)),  # API limit: 60 requests/min, 5000/day
}
USE_AUDIBLE_API = os.getenv('USE_AUDIBLE_API', 'true').lower() == 'true'
GOODREADS_ENABLED = os.getenv('GOODREADS_ENABLED', 'true').lower() == 'true'  # false = no Goodreads requests at all
GR_BOOK_PAGES = os.getenv('GR_BOOK_PAGES', 'true').lower() == 'true'  # true = book page first (robots.txt allows it), search list as fallback
GOODREADS_PAGES_PER_RUN = int(os.getenv('GOODREADS_PAGES_PER_RUN', 3))  # book pages per run (measured: challenge after 3-7); a challenge only switches pages off
HARDCOVER_TOKEN, _ = clean_env('HARDCOVER_TOKEN', r'A-Za-z0-9._\- ')
HARDCOVER_TOKEN = re.sub(r'(?i)^\s*bearer\s+', '', HARDCOVER_TOKEN).strip()
HARDCOVER_ENABLED = os.getenv('HARDCOVER_ENABLED', 'true').lower() == 'true' and bool(HARDCOVER_TOKEN)
# own daily budget (counted locally, persisted): free plan 5000/day, paid plan 50000/day. Both 60 requests/min.
HARDCOVER_PAID = os.getenv('HARDCOVER_PAID', 'false').lower() == 'true'
HC_DAILY_LIMIT = int(os.getenv('HARDCOVER_DAILY_LIMIT') or (50000 if HARDCOVER_PAID else 5000))
SOURCE_STATE_FILE = os.path.join(SCRIPT_DIR, 'source_state.json')  # per book + source: found / missing / paused (source_state.py)
SOURCE_STATE = {}
# change monitoring: what this script wrote last (per book + field) and fields the user changed himself afterwards
LAST_WRITTEN_FILE = os.path.join(SCRIPT_DIR, 'last_written.json')   # {key: {field: {'ours': listing-style value, 'old': ...}}}
MANUAL_FIELDS_FILE = os.path.join(SCRIPT_DIR, 'manual_fields.json') # {key: {field: {'value', 'ts', 'title'}}} never overwritten again
LAST_WRITTEN, MANUAL_FIELDS = {}, {}
RUN_MODE = os.getenv('RUN_MODE', 'auto').lower()                     # auto | buildup | maintain
MAINTAIN_SAMPLE = int(os.getenv('MAINTAIN_SAMPLE', 30))              # maintenance: random due books per run (new / reverted books always)
MANUAL_LOCK_TAG = os.getenv('MANUAL_LOCK_TAG', 'false').lower() == 'true'  # also set lock_<field> when the user changed a field
WATCH_FIELDS = ('title', 'subtitle', 'asin', 'authors', 'narrators', 'series', 'publisher', 'publishedYear', 'isbn', 'language')
LOOKUP_HITS = _TLDict('lookup_hits', {'hardcover': None, 'goodreads': None, 'audible': None})  # result count of the last lookup (0 = no such book at all), per thread
HC_USAGE_FILE = os.path.join(SCRIPT_DIR, 'hardcover_usage.json')  # {'day': 'YYYY-MM-DD' (UTC), 'requests': n}
HC_CACHE_FILE = os.path.join(SCRIPT_DIR, "hardcover_cache.json")  # key -> {'id': Hardcover book id, 'slug': ...}
# Order of the sources in the rating block (unknown names are ignored, missing ones are appended)
RATING_SOURCES = ['Audible', 'Goodreads', 'Hardcover']
RATING_ORDER = [s for s in (x.strip().capitalize() for x in os.getenv('RATING_ORDER', 'audible,goodreads,hardcover').split(',')) if s in RATING_SOURCES]
RATING_ORDER = RATING_ORDER or list(RATING_SOURCES)  # only the listed sources are SHOWN in the block (all enabled ones are still asked)
RATING_ICONS = os.getenv('RATING_ICONS', 'moons').lower()        # moons | stars | circles | squares | diamonds (all with half steps)
REWRITE_RATING_SYMBOLS = os.getenv('REWRITE_RATING_SYMBOLS', 'false').lower() == 'true'  # task: rewrite the symbols of every rating block
# (full, half, empty) per symbol set; the order of the keys is the order in the web UI
RATING_SYMBOL_SETS = {'moons': ('🌕', '🌗', '🌑'), 'stars': ('★', '⯪', '☆'), 'circles': ('●', '◐', '○'), 'squares': ('■', '◧', '□'), 'diamonds': ('◆', '⬖', '◇'),
                      # colourful emoji (user 2026-11-09): emoji have no half symbol, the middle one stands for the half step
                      'yellow_stars': ('⭐', '✨', ''),  # no fitting empty yellow star: only the earned ones are shown (user 2026-11-09)
                      'hearts': ('❤️', '💗', '🤍'), 'traffic': ('🟢', '🟡', '⚪'), 'green_squares': ('🟩', '🟨', '⬜'),
                      'orange_circles': ('🟠', '🟡', '⚪')}
RATING_SYMBOL_SETS['stars_dark'] = RATING_SYMBOL_SETS['yellow_stars']  # short-lived option, same look now
RATING_LABELS = os.getenv('RATING_LABELS', 'en').lower()         # en | de
RATING_POSITION = os.getenv('RATING_POSITION', 'top').lower()    # top | bottom of the description
RATING_MIN_COUNT = int(os.getenv('RATING_MIN_COUNT', 0) or 0)    # a source with fewer ratings is not shown
LABELS = {'en': {'head': 'Ratings & Infos', 'overall': 'Overall', 'performance': 'Performance', 'story': 'Story', 'rating': 'Rating'},
          'de': {'head': 'Bewertungen & Infos', 'overall': 'Gesamt', 'performance': 'Sprecher', 'story': 'Geschichte', 'rating': 'Bewertung'}}
# Audible marketplaces. ABS may have matched a book in any of them (e.g. audible.co.uk), so all are checked
# before an ASIN counts as broken. AUDIBLE_MARKETPLACES limits/orders them.
ALL_MARKETS = ['com', 'co.uk', 'ca', 'com.au', 'de', 'fr', 'it', 'es']
AUDIBLE_MARKETS = [m.strip() for m in os.getenv('AUDIBLE_MARKETPLACES', ','.join(ALL_MARKETS)).split(',') if m.strip() in ALL_MARKETS]
AUDIBLE_API_HOSTS = {f"www.audible.{m}": f"api.audible.{m}" for m in ALL_MARKETS}
MARKET_BY_LANG = {'German': 'de', 'French': 'fr', 'Italian': 'it', 'Spanish': 'es', 'English': 'com'}
# Preferred store per book language: an ASIN from another store is swapped for the edition of this store
# if it can be matched unambiguously (title, author, runtime, narrator). Otherwise the existing ASIN stays.
PREFERRED_STORE = {'English': 'www.audible.com', 'German': 'www.audible.de'}
AUDIBLE_API_GROUPS = "rating,series,product_attrs,product_desc,category_ladders,contributors,media"
DRY_RUN = os.getenv('DRY_RUN', 'False').lower() == 'true'
ABS_TIMEOUT = int(os.getenv('ABS_TIMEOUT', 30))  # Seconds per ABS API request
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
# Runtime check: ABS file duration vs. Audible runtime. Both limits must be exceeded to count as a different edition.
RUNTIME_TOLERANCE_PCT = float(os.getenv('RUNTIME_TOLERANCE_PCT', 15))  # 'same BOOK' (another recording may be 12-16 % longer):
RUNTIME_TOLERANCE_MIN = float(os.getenv('RUNTIME_TOLERANCE_MIN', 20))  # measured 2026-11-04 - 5 %/10 min loses 16 of 1920 verified ASINs, 10 %/15 min 6
EDITION_FILL_MIN = float(os.getenv('EDITION_FILL_MIN', 10))  # recording details (narrators, publisher, year, abridged) only when the runtime is this close
FILL_COVERS = os.getenv('FILL_COVERS', 'true').lower() == 'true'      # Set Audible cover when ABS has none
UPDATE_COVERS = os.getenv('UPDATE_COVERS', 'true').lower() == 'true'  # Replace an existing cover by the Audible cover when the book is VERIFIED (same recording); old cover backed up for undo
COVER_STATE_FILE = os.path.join(SCRIPT_DIR, "cover_state.json")  # item id -> ASIN whose Audible cover was set (no re-upload every run)
COVER_STATE = {}
FILLED_NARR_FILE = os.path.join(SCRIPT_DIR, "filled_narrators.json")  # item keys whose narrators WE copied from Audible (no self-confirmation)
FILLED_NARR = set()
GENRE_LANGUAGE = os.getenv('GENRE_LANGUAGE', 'en').lower()            # 'en' = translate audible.de genres, 'keep' = as delivered
SERIES_MERGE = os.getenv('SERIES_MERGE', 'true').lower() == 'true'    # ABS-only series merge task at the end
LIBRARY_REPORT = os.getenv('LIBRARY_REPORT', 'true').lower() == 'true'
SERIES_ONLY = os.getenv('SERIES_ONLY', 'false').lower() == 'true'     # Only run the ABS-only series/report task
# Remove ratings task (web UI): 'audible,goodreads,hardcover' or 'all' - only this task runs (no searches, no metadata)
REMOVE_RATINGS = {x.strip().capitalize() for x in os.getenv('REMOVE_RATINGS', '').replace('all', 'audible,goodreads,hardcover').split(',') if x.strip()}
AUTHOR_NORMALIZE = os.getenv('AUTHOR_NORMALIZE', 'true').lower() == 'true'  # Unify author spellings (series task)
SERIES_LOCK_AFTER_MERGE = os.getenv('SERIES_LOCK_AFTER_MERGE', 'false').lower() == 'true'  # Old behaviour: tag lock_series
NO_RATINGS_RECHECK_DAYS = int(os.getenv('NO_RATINGS_RECHECK_DAYS', 7))  # New releases without ratings yet
GR_MISSING_RECHECK_DAYS = int(os.getenv('GR_MISSING_RECHECK_DAYS', 30))  # Audible OK, Goodreads not found
GR_BLOCKED_RECHECK_DAYS = 1  # Goodreads blocked/unreachable during the run
GR_BLOCK_COOLDOWN = int(os.getenv('GR_BLOCK_COOLDOWN', 900))  # Pause before retrying after a Goodreads block
GR_MAX_COOLDOWNS = int(os.getenv('GR_MAX_COOLDOWNS', 0))  # Cool-downs per run before Goodreads is skipped (0 = stop at the first block, next batch tries again)
GOODREADS_SAMPLE = max(1, int(os.getenv('GOODREADS_SAMPLE', 1)))  # Goodreads only for every Nth book (the others are re-checked later); 1 = every book
GR_ID_TRIES = int(os.getenv('GR_ID_TRIES', 2))  # max Goodreads pages tried per book via Hardcover IDs
# Max Goodreads searches per run (cached URLs don't count). Keeps the first full pass under Goodreads'
# bot detection: the rest is deferred to the next run. 0 = unlimited.
GOODREADS_MAX_PER_RUN = int(os.getenv('GOODREADS_MAX_PER_RUN', 25))  # measured 2026-11-05: ~100-150 searches in a row, then a bot block for hours

# --- HEADERS & CONSTANTS ---
FIXED_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

GERMAN_LANG_CODES = ['de', 'deu', 'ger', 'german', 'deutsch']

LANGUAGE_MAP = {
    'englisch': 'English', 'english': 'English', 'en': 'English', 'eng': 'English', 'en-us': 'English', 'en-gb': 'English',
    'deutsch': 'German', 'german': 'German', 'de': 'German', 'deu': 'German', 'ger': 'German', 'de-de': 'German',
    'french': 'French', 'französisch': 'French', 'fr': 'French', 'fra': 'French', 'fre': 'French',
    'spanish': 'Spanish', 'spanisch': 'Spanish', 'es': 'Spanish', 'spa': 'Spanish',
    'italian': 'Italian', 'italienisch': 'Italian', 'it': 'Italian', 'ita': 'Italian',
    'dutch': 'Dutch', 'niederländisch': 'Dutch', 'nl': 'Dutch', 'nld': 'Dutch', 'dut': 'Dutch',
    'japanese': 'Japanese', 'japanisch': 'Japanese', 'ja': 'Japanese', 'jpn': 'Japanese',
}

BOOK_LANGUAGES[:] = [LANGUAGE_MAP.get(x.strip().lower(), x.strip()[:1].upper() + x.strip()[1:]) for x in os.getenv('BOOK_LANGUAGES', '').split(',') if x.strip()]

# audible.de category names -> audible.com names (best effort; unmapped names are kept and listed in the report)
GENRE_MAP_DE = {
    'science-fiction & fantasy': 'Science Fiction & Fantasy', 'science fiction & fantasy': 'Science Fiction & Fantasy',
    'science-fiction': 'Science Fiction', 'fantasy': 'Fantasy', 'epische fantasy': 'Epic', 'episch': 'Epic',
    'high fantasy': 'High Fantasy', 'dark fantasy': 'Dark Fantasy', 'urban fantasy': 'Urban', 'humorvolle fantasy': 'Humorous',
    'abenteuer': 'Adventure', 'action & abenteuer': 'Action & Adventure', 'action und abenteuer': 'Action & Adventure',
    'dystopien': 'Dystopian', 'dystopie': 'Dystopian', 'postapokalyptisch': 'Post-Apocalyptic', 'postapokalypse': 'Post-Apocalyptic',
    'space opera': 'Space Opera', 'militär': 'Military', 'militär-science-fiction': 'Military', 'cyberpunk': 'Cyberpunk',
    'zeitreisen': 'Time Travel', 'erstkontakt': 'First Contact', 'außerirdische': 'Alien Invasion',
    'drachen & fabelwesen': 'Dragons & Mythical Creatures', 'märchen & sagen': 'Fairy Tales', 'mythologie': 'Myths & Legends',
    'krimis & thriller': 'Mystery, Thriller & Suspense', 'krimi & thriller': 'Mystery, Thriller & Suspense',
    'thriller': 'Thriller & Suspense', 'krimis': 'Mystery', 'krimi': 'Mystery', 'psychothriller': 'Psychological',
    'horror': 'Horror', 'paranormal': 'Paranormal', 'historisch': 'Historical', 'historische romane': 'Historical Fiction',
    'literatur & belletristik': 'Literature & Fiction', 'belletristik': 'Literature & Fiction', 'romane': 'Literature & Fiction',
    'klassiker': 'Classics', 'zeitgenössisch': 'Contemporary', 'zeitgenössische literatur': 'Contemporary',
    'humor': 'Comedy & Humor', 'comedy & humor': 'Comedy & Humor', 'liebesromane': 'Romance', 'romantik': 'Romance', 'erotik': 'Erotica',
    'kinderhörbücher': "Children's Audiobooks", 'kinder': "Children's Audiobooks", 'jugendhörbücher': 'Teen & Young Adult',
    'jugendliche & junge erwachsene': 'Teen & Young Adult',
    'biografien & erinnerungen': 'Biographies & Memoirs', 'biografien': 'Biographies & Memoirs', 'geschichte': 'History',
    'wissenschaft & technik': 'Science & Engineering', 'wissenschaft': 'Science', 'gesundheit & wellness': 'Health & Wellness',
    'religion & spiritualität': 'Religion & Spirituality', 'politik & sozialwissenschaften': 'Politics & Social Sciences',
    'wirtschaft & karriere': 'Business & Careers', 'ratgeber': 'Relationships, Parenting & Personal Development',
    'hörspiele': 'Radio & TV', 'kurzgeschichten': 'Short Stories', 'anthologien & kurzgeschichten': 'Anthologies & Short Stories',
}
UNMAPPED_GENRES = set()

def _genre_rules():
    """GENRE_RULES (settings, one per line): 'Old => New' renames, 'Old => -' blocks a genre coming from Audible."""
    out = {}
    for line in os.getenv('GENRE_RULES', '').replace('\\n', '\n').splitlines():
        if '=>' in line:
            a, b = (x.strip() for x in line.split('=>', 1))
            if a: out[a.lower()] = b
    return out
GENRE_RULES = _genre_rules()

def apply_genre_rules(name):
    """None = blocked."""
    r = GENRE_RULES.get((name or '').strip().lower())
    if r is None: return name
    return None if r in ('', '-') else r

def map_genre(name, domain):
    """Translate audible.de genre names to English (GENRE_LANGUAGE=en); own rules (GENRE_RULES) afterwards."""
    return apply_genre_rules(_map_genre(name, domain))

def _map_genre(name, domain):
    if GENRE_LANGUAGE != 'en' or 'audible.de' not in str(domain): return name
    mapped = GENRE_MAP_DE.get(name.strip().lower())
    if mapped: return mapped
    UNMAPPED_GENRES.add(name)
    return name

HEADERS_BASE = {
    "User-Agent": FIXED_USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Referer": "https://www.google.com/",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-User": "?1"
}

# Goodreads only, opt-in (user decision 2026-11-08): curl_cffi sends the TLS/HTTP2 signature of a real Chrome together
# with matching headers. No browser, no JavaScript: a WAF JS challenge (HTTP 202) still stops book pages as before.
# Budgets, throttle and the stop at a block stay unchanged.
GR_IMPERSONATE = os.getenv('GR_IMPERSONATE', 'false').lower() == 'true'
_GR_CURL, _GR_CURL_LOCK = None, threading.Lock()
GR_CURL_ERRORS = ()
if GR_IMPERSONATE:  # load the native curl_cffi library ONLY when the option is on (it was loaded always; runs crashed with SIGSEGV, 2026-11-08)
    try:
        from curl_cffi.requests.errors import RequestsError as _CurlRequestsError
        GR_CURL_ERRORS = (_CurlRequestsError,)
    except Exception:  # curl_cffi missing: the option falls back to plain requests (see gr_impersonated_get)
        pass

def gr_impersonated_get(url, params, accept_language):
    """GET with a Chrome browser signature (curl_cffi). Headers (User-Agent, sec-ch-ua ...) come from the impersonation
    profile so they fit the TLS signature; only the language is ours. Without curl_cffi: plain requests + a warning once."""
    global _GR_CURL
    with _GR_CURL_LOCK:  # one Goodreads request in flight; a curl session is not thread-safe
        if _GR_CURL is None:
            try:
                from curl_cffi import requests as creq
                _GR_CURL = creq.Session(impersonate='chrome')
                logging.info("        🧭 Goodreads: browser signature on (curl_cffi, Chrome profile)")
            except ImportError:
                _GR_CURL = False
                logging.warning("        ⚠️ GR_IMPERSONATE=true, but curl_cffi is not installed - plain requests are used")
        if _GR_CURL is False:
            return requests.get(url, headers=get_headers(), params=params, timeout=20)
        return _GR_CURL.get(url, params=params, headers={'Accept-Language': accept_language}, timeout=20)

HEADERS_ABS = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
class TimeoutSession(requests.Session):
    """requests.Session that applies a default timeout to every request."""
    def request(self, *args, **kwargs):
        kwargs.setdefault('timeout', ABS_TIMEOUT)
        return super().request(*args, **kwargs)

abs_session = TimeoutSession()
abs_session.headers.update(HEADERS_ABS)

# Regex
RE_ASIN = re.compile(r'ASIN[:\s]*(B0\w+)')
RE_ISBN_JSON = re.compile(r'"isbn"\s*:\s*"([0-9]{10,13})"')
RE_ASIN_JSON = re.compile(r'"asin"\s*:\s*"([A-Z0-9]{10})"')
RE_URL_ASIN = re.compile(r'(?:creativeASIN|asin)=([A-Z0-9]{10})')
# Old blocks are searched only inside our rating block. ABS stores '<br />' and '&amp;'.
RE_BLOCK_HEAD = re.compile(r'^(Audible|Goodreads|Hardcover) \(')  # first line of one source inside the rating block
# Block frame: '─── Ratings & Infos ───' ... '───────────' (user 2026-11-09: the old '⭐' frame looked like a rating with the yellow-star set).
# The old frame is still recognised; its end is a LINE of its own ('<br>⭐'), so a yellow-star rating line never ends the block early.
BLOCK_RULE = '─' * 3
BLOCK_END = '─' * 15
RE_RATING_BLOCK = re.compile(r'(?s)(?:<br\s*/?>\s*)*(?:⭐|─{3,})\s*(?:Ratings|Bewertungen)\b.*?<br\s*/?>\s*(?:⭐|─{3,})(?=\s*(?:<|$))(?:\s|<br\s*/?>)*')  # top or bottom, EN or DE head
RE_CLEAN_TITLE = re.compile(r'(?i)\b(unabridged|abridged|audiobook|graphic audio|dramatized adaptation)\b|[\(\[].*?[\)\]]')
RE_VOL = re.compile(r'(?i)(?:\b(?:book|vol\.?|volume|part|no\.?)|#)\s*(\d+)')
RE_TEXT_RATING = re.compile(r'([0-9]+[.,]?[0-9]*)\s*(?:out of|von)\s*5\s*(?:stars|Sternen)', re.IGNORECASE)
RE_TEXT_COUNT = re.compile(r'\((?:[0-9]{1,3}(?:[.,][0-9]{3})*|[0-9]+)\s*(?:ratings|Bewertungen|votes)?\)', re.IGNORECASE)

# --- NORMALIZATION CONSTANTS ---
NUMBER_MAP = {
    'one': '1', 'two': '2', 'three': '3', 'four': '4', 'five': '5', 'six': '6', 'seven': '7', 'eight': '8', 'nine': '9', 'ten': '10',
    'eins': '1', 'zwei': '2', 'drei': '3', 'vier': '4', 'fünf': '5', 'sechs': '6', 'sieben': '7', 'acht': '8', 'neun': '9', 'zehn': '10'
}
RE_NOISE = re.compile(r'(?i)\b(?:book|vol\.?|volume|part|no\.?|nr\.?|band|teil|buch|reihe|serie|series|episode|chapter|kapitel)\b')

RE_RAW_STORY = re.compile(r'story-value="([0-9.]+)"')
RE_RAW_PERFORMANCE = re.compile(r'performance-value="([0-9.]+)"')
RE_RAW_OVERALL = re.compile(r'value="([0-9.]+)"') 
RE_RAW_COUNT = re.compile(r'count="(\d+)"')

stats = _Stats({k: 0 for k in ["processed", "success", "failed", "no_data", "skipped", "partial", "cooldown", "recycled", "asin_found", "isbn_added", "isbn_repaired", "asin_migrated", "meta_updated", "manual_retrigger", "no_ratings_yet", "gr_missing", "recheck", "gr_deferred", "cover_added", "chapters", "desc_filled", "series_merged", "other_edition", "hc_found", "hc_missing", "rating_only"]})
stats['aborted_ratelimit'] = False
SUMMARY_INDEX_FILE = os.path.join(SCRIPT_DIR, 'summary_index.json')  # asin -> [title, start of Audible's summary] (description of another book)
SUMMARY_INDEX = {}
IDENTITY_DESC_FILE = os.path.join(SCRIPT_DIR, 'identity_desc.json')  # books whose description was replaced after a confirmed other-book ASIN
IDENTITY_DESC = set()
NEW_VOLUMES = []  # missing volumes that became available since the last report (run summary / notification)
SOURCE_DOWN = {}  # source -> reason: failed canary check or tripped breaker (source_health.py); nothing is counted or written from it
BREAKER = None
SOURCE_CANARY = os.getenv('SOURCE_CANARY', 'true').lower() == 'true'  # tests replaying recordings switch it off (no live canary request)
GR_STATE = {'blocked': False, 'cooldowns': 0, 'searches': 0, 'pages': 0, 'pages_blocked': False}
GR_CACHE = {}  # Filled in main()  # Set once Goodreads blocks us; GR is then skipped for the rest of the run
reports = {"audible": {}, "goodreads": {}, "isbn": {}}
AUTHOR_NOTES = []  # Author spelling differences between ABS and Audible (report only)
SERIES_ALIASES, SERIES_INDEX = {}, {}  # Loaded in main()
SEQ_TITLES = {}  # (lib, series name lower, number) -> normalized titles holding it (filled by build_queue)

def _first_author(names):
    return re.sub(r'[^a-z]', '', fold_accents(str(names or '').split(',')[0]).lower())

def title_shared(key, title, cand_rt=None):
    """Another library book has the same title (an ABS rescan gave five Solo Leveling books 'Vol. 7', fourteen Mushoku Tensei books
    the bare series name): the title cannot tell the volumes apart, only the runtime can."""
    own = TITLE_OWNERS.get((key.split('_')[0], normalize_title_text(title or '')), set()) if key else set()
    mine = {a for k, a in own if k == key}
    # same title AND same author ('Threshold' by two authors is no doubt); with cand_rt: only an owner whose runtime would ALSO fit the
    # candidate makes it ambiguous (Judicator Jane 1 vs 6: the other book is far longer -> the candidate can only be this one)
    return any(k != key and (a in mine or not mine) and (cand_rt is None or not KEY_DUR.get(k) or not runtime_mismatch(KEY_DUR[k], cand_rt)) for k, a in own)

def folder_volume_conflict(item_data, key, title, meta):
    """ABS metadata of ANOTHER volume (an ABS rescan with an old metadata.json gave Solo Leveling 2, 3, 5 and 8 the title, ASIN and number
    of volume 7): the title is shared by several library books AND the book folder ('2 - Solo Leveling, Vol. 2 - Ki Hong Lee') names
    another title with a number that neither the ABS title nor the ABS series number has. Returns {'title', 'num'} of the folder or None."""
    if not title_shared(key, title): return None
    parts = [p_ for p_ in str((item_data or {}).get('relPath') or '').replace('\\', '/').split('/') if p_.strip()]
    if len(parts) < 3: return None  # author/series/book: only books inside a series folder
    m = re.match(r'^(?:book\s*)?(\d+(?:\.\d+)?)\s+-\s+(.+)$', parts[-1].strip(), re.I)
    if not m: return None
    f_title = re.sub(r'\s*\{[^}]*\}', '', m.group(2).split(' - ')[0]).strip()
    try: f_num = float(m.group(1))
    except ValueError: return None
    if not f_title or normalize_title_text(f_title) == normalize_title_text(title): return None
    nums = set()
    for x in meta.get('series') or []:
        try: nums.add(float(str(x.get('sequence')).strip()))
        except (TypeError, ValueError): pass
    for v in extract_volume(title or ''):
        try: nums.add(float(v))
        except ValueError: pass
    if not nums or f_num in nums: return None
    return {'title': f_title, 'num': strip_lead0(m.group(1))}

def tag_volume_conflict(item_data, title, meta):
    """The embedded tags of the audio file (title / album, set by the publisher or the download tool) name ANOTHER volume than the
    ABS title: 'Publisher's Pack 3: Books 5-6' (grouping '#5') in a file whose ABS title and folder say 'Publisher's Pack 1' (2026-11-09).
    Returns {'title', 'num'} like folder_volume_conflict (the search then needs the runtime to ±2 min) or None."""
    files = ((item_data or {}).get('media') or {}).get('audioFiles') or []
    tags_ = (files[0].get('metaTags') or {}) if files else {}
    t_title = (tags_.get('tagTitle') or tags_.get('tagAlbum') or '').strip()
    if not t_title or normalize_title_text(t_title) == normalize_title_text(title): return None
    head = lambda t: normalize_title_text(re.split(r'\s*[:(]\s*', t)[0])
    if head(t_title) != head(title): return None  # another book altogether - only another VOLUME of the same title counts
    t_vols, a_vols = extract_volume(t_title.split(':')[0] + ':' + t_title.split(':')[1] if t_title.count(':') else t_title), extract_volume(title)
    if not t_vols or not a_vols or t_vols & a_vols: return None
    gm = re.search(r'#\s*(\d+(?:\.\d+)?)', tags_.get('tagGrouping') or '')
    nums = {str(x.get('sequence')).strip() for x in meta.get('series') or [] if x.get('sequence')}
    if gm and gm.group(1) in nums: return None  # the grouping agrees with ABS: no proof
    return {'title': re.sub(r'(:\s*[^:]+)\1$', r'\1', t_title), 'num': gm.group(1) if gm else None}

def seq_shared(lib, name, seq):
    """Two or more DIFFERENT books of the library hold this number in this series (an ABS rescan put three Witcher books at #0.5)."""
    q = str(seq or '').strip()
    return bool(q) and len(SEQ_TITLES.get((lib, str(name or '').strip().lower(), q), ())) > 1

def search_series(lib, series):
    """ABS series for the ASIN search: a number that two or more DIFFERENT books of the same series share (e.g. three Witcher
    books at #0.5 after an ABS rescan) is no evidence and must not reject the right product -> number unknown."""
    out = []
    for x in series or []:
        q = str(x.get('sequence') or '').strip()
        if seq_shared(lib, x.get('name'), q):
            logging.info(f"        ℹ️ Series '{x.get('name')}' #{q} is shared by several books in ABS: number not used as evidence")
            x = dict(x, sequence=None)
        out.append(x)
    return out

SERIES_FACTS = {}  # (lib, series name lower) -> {'n': books, 'authors': first authors, 'folders': series folders, 'name'} (from the listing)

def series_cleanup(lib, item_data, series, aud_series, book_authors, book_variant, verified, book_lang=None):
    """Series that do not belong to the book (user findings 2026-11-08), decided with Audible's list for the SAME book.
    series: current [{'name','sequence'}]; aud_series: Audible's [{'name','sequence'}] (already same edition). Returns (new list, notes)."""
    from abs_series import series_key as _k, series_variant as _v, author_key
    if not verified: return series, []
    same = lambda a, b: a.strip().lower() == b.strip().lower() or (_k(a) == _k(b) and _v(a) == _v(b))
    fact = lambda n: SERIES_FACTS.get((lib, n.strip().lower())) or {'n': 1, 'authors': set(), 'folders': set()}
    listed = lambda n: any(same(n, a['name']) for a in aud_series)
    parts = [p.strip().lower() for p in str(item_data.get('relPath') or '').replace('\\', '/').split('/') if p.strip()]
    folder = parts[1] if len(parts) >= 3 else None
    me = {_first_author(', '.join(book_authors))}
    out, notes = [dict(x) for x in series], []
    german_book = map_language(book_lang) == 'German'
    from abs_series import foreign_for_book as _ffb
    fm = re.match(r'^(?:book\s*)?(\d+(?:\.\d+)?)\s+-\s+', parts[-1]) if parts else None
    # round 2 (2026-11-09): the book sits in a folder named like Audible's series of the same recording ("R.A. Salvatore's War Of The
    # Spider Queen (Complete Series)/02 - Insurrection") -> that series is added even when the book has another one; an unnumbered
    # series Audible does not list whose books ALL sit in that folder is a stale name for it ('Forgotten Realms: Anthologies') and goes
    plain_ = lambda x: re.sub(r'[\W_]+', ' ', (x or '').lower()).strip()
    f_key = plain_(re.sub(r'\s*\((?:the\s+)?(?:complete\s+)?(?:series|saga|collection|reihe)\)\s*$', '', folder or '', flags=re.I)) if folder else ''
    for a in aud_series:
        # the folder names exactly this series (no normalisation: 'The Witcher' is not 'The Witcher (Full-Cast Editions)')
        if f_key and plain_(a['name']) == f_key and not (_v(a['name']) - book_variant) and not any(same(y['name'], a['name']) for y in out):
            add = {'name': canonical_series(lib, a['name'], book_authors, book_variant)[0], 'sequence': a.get('sequence')}
            out.append(add); notes.append(f"'{add['name']}'{' #' + str(add['sequence']) if add.get('sequence') else ''} added (Audible's series = the book's folder)")
            for y in list(out):
                fy = fact(y['name'])
                if y is not add and not y.get('sequence') and not listed(y['name']) and fy.get('folders') == {folder} and not same(y['name'], folder):
                    out.remove(y); notes.append(f"'{y['name']}' removed (unnumbered, Audible does not list it, all its books are in the folder of '{add['name']}')")
    for x in list(out):
        n = x['name']
        if x.get('sequence') and map_language(book_lang) == 'English' and clash_seq(lib, n, x['sequence'], book_lang) \
                and not any(same(n, a['name']) and _seq_num(a.get('sequence')) == _seq_num(x['sequence']) for a in aud_series):
            # another work has this number in this series and Audible does not confirm it for this book (English books only: a German
            # translation in two parts legitimately shares a number). Audible's number of this series, else the folder number, else none;
            # Audible's sub-series that carries the same number is added ('A Little Hatred' = The Age of Madness #1).
            old_q = x['sequence']
            own = next((a for a in aud_series if same(n, a['name']) and a.get('sequence')), None)
            if own: x['sequence'] = own['sequence']
            elif fm and folder and same(n, folder) and (strip_lead0(fm.group(1))) != str(old_q): x['sequence'] = strip_lead0(fm.group(1))
            else: x['sequence'] = None
            notes.append(f"'{n}' #{old_q} is held by another book too and Audible does not confirm it -> #{x['sequence'] or '-'}")
            for a in aud_series:
                if re.fullmatch(r'\d+(?:\.\d+)?', str(a.get('sequence') or '').strip()) and _seq_num(a.get('sequence')) == _seq_num(old_q) \
                        and not same(n, a['name']) and not any(same(y['name'], a['name']) for y in out):
                    add = {'name': canonical_series(lib, a['name'], book_authors, book_variant)[0], 'sequence': a.get('sequence')}
                    out.append(add); notes.append(f"'{add['name']}' #{add['sequence']} added (the sub-series the number came from)")
        if not aud_series and folder and (non_latin_name(n) or _ffb(n, book_lang)):
            # Audible lists no series: the library series the other books of this FOLDER are in ('Les Annales de la Compagnie Noire' on
            # 'Glen Cook/The Black Company/1 - The Black Company' -> 'The Chronicles of the Black Company'), number from the folder
            libs = [v for k_, v in SERIES_FACTS.items() if k_[0] == lib and folder in v['folders'] and v['n'] >= 2 and v['name'] != n
                    and (v['authors'] & me) and not non_latin_name(v['name']) and not _ffb(v['name'], book_lang)]
            if len(libs) == 1:
                x['name'] = libs[0]['name']
                if fm: x['sequence'] = strip_lead0(fm.group(1))
                notes.append(f"'{n}' -> '{x['name']}' (not English/German; the series of the other books in this folder)")
                continue
        if german_book and re.search(r'(?i)\b(der|die|das|des|und|von|vom|im)\b', n): continue  # a German book keeps its German series name
        if listed(n):
            a = next(a['name'] for a in aud_series if same(n, a['name']))
            lib_a = SERIES_FACTS.get((lib, a.strip().lower()))
            if a != n and lib_a and fact(n)['n'] <= 2 and lib_a['n'] > fact(n)['n']:  # 'Remembrance of Earth's Past' -> the library's 'The Remembrance ...'
                x['name'] = lib_a['name']; notes.append(f"'{n}' -> '{lib_a['name']}' (Audible's series, spelled as in the library)")
            continue
        others = [y for y in out if y is not x]
        others_ok = any(listed(y['name']) or (folder and same(y['name'], folder)) for y in others)
        from abs_series import foreign_for_book as _ffb
        variant_dup = any(y is not x and _k(y['name']) == _k(n) and _v(n) - book_variant for y in out)  # 'X (Omnibus)'/'X Manga' next to 'X'
        if aud_series and others_ok and (variant_dup or non_latin_name(n) or _ffb(n, book_lang)):  # NOT by book count: 'Skyward Flight' is real
            missing = [a for a in aud_series if not any(same(y['name'], a['name']) for y in out)]
            near = [a for a in missing if non_latin_name(n) or _ffb(n, book_lang) or difflib.SequenceMatcher(None, _k(n), _k(a['name'])).ratio() >= 0.75]
            if missing and not near: continue  # an own sub-series ('Hoid's Travails') next to a missing Audible series: leave it
            missing = near
            if missing:  # 'Realms of the Elderlings' #14 while Audible's 'Realm of the Elderlings' is not on the book: take Audible's
                a = missing[0]; canon = canonical_series(lib, a['name'], book_authors, book_variant)[0]
                x['name'], x['sequence'] = canon, a.get('sequence') or x.get('sequence')
                notes.append(f"'{n}' -> '{canon}' (Audible does not list '{n}', but lists '{a['name']}' which the book lacks)")
            else:
                out.remove(x); notes.append(f"'{n}' removed (Audible does not list it; the book is in {', '.join(repr(y['name']) for y in others)})")
        elif not aud_series and others_ok and fact(n)['n'] == 1 and not (folder and same(n, folder)):
            out.remove(x); notes.append(f"'{n}' removed (single-book series; the book is in its folder series)")
        elif german_book and aud_series and others_ok and not listed(n) and not (folder and same(n, folder)):
            # 'Lightbringer' (original English numbering) next to Audible's 'Licht-Saga' on the German edition: one series, the German one
            out.remove(x); notes.append(f"'{n}' removed (German book: Audible lists {', '.join(repr(a['name']) for a in aud_series)}, the book is in it)")
        elif fact(n)['n'] == 1 and not (fact(n)['authors'] - me - {''}) and any(
                k_[0] == lib and v['name'] != n and _k(v['name']) == _k(n) and v['authors'] and not (v['authors'] & me) for k_, v in SERIES_FACTS.items()):
            out.remove(x); notes.append(f"'{n}' removed (named like another author's series, Audible does not list it)")
        elif aud_series and not others and fact(n)['n'] <= 2 and not listed(n):
            a = aud_series[0]; canon = canonical_series(lib, a['name'], book_authors, book_variant)[0]
            if non_latin_name(n) or _ffb(n, book_lang):
                x['name'], x['sequence'] = canon, a.get('sequence') or x.get('sequence')
                notes.append(f"'{n}' -> '{canon}' (only series of the book, not English/German, Audible does not list it)")
            elif not any(same(y['name'], canon) for y in out):  # ADD: 'Chains of the Fallen' is a real sub-series of Audible's 'Soul Force Saga'
                out.append({'name': canon, 'sequence': a.get('sequence')})
                notes.append(f"'{canon}' #{a.get('sequence') or '-'} added (Audible's series; '{n}' stays)")
        elif folder and fact(n)['folders'] == {folder} and any(same(a['name'], folder) for a in aud_series) and not any(same(y['name'], folder) for y in out):
            a = next(a for a in aud_series if same(a['name'], folder))
            add = {'name': canonical_series(lib, a['name'], book_authors, book_variant)[0], 'sequence': a.get('sequence')}
            if not any(same(y['name'], add['name']) for y in out):  # ADD only: 'Mistborn, Era 2' stays next to 'The Mistborn Saga'
                out.append(add); notes.append(f"'{add['name']}' #{add['sequence'] or '-'} added (folder and Audible name this series)")
    # user rule 2026-11-09: a series never mixes languages. A non-English book sits in a series that library books of ANOTHER language
    # hold, Audible does not list that series for this book (same edition) but lists its own series -> Audible's series replaces it
    # (Der Drachenbeinthron: 'Memory, Sorrow & Thorn' -> 'Das Geheimnis der Großen Schwerter'; Malice (French) -> 'Le livre des Terres Bannies')
    bl = map_language(book_lang)
    if bl and aud_series and book_lang_allowed(bl):
        for x in list(out):
            if listed(x['name']) or x not in out: continue
            langs_ = fact(x['name']).get('langs') or {}
            other = sum(v for l_, v in langs_.items() if l_ and l_ != bl)
            if not other: continue
            # an ENGLISH book only leaves a series that is (almost) entirely in another language (The Silmarillion in the German 'Middle Earth')
            if bl == 'English' and (other < 2 or langs_.get('English', 0) > 1): continue
            a = next((a for a in aud_series if a.get('name') and not same(a['name'], x['name'])), None)
            if not a: continue
            canon = canonical_series(lib, a['name'], book_authors, book_variant)[0]
            if same(canon, x['name']): continue  # an alias maps Audible's name back to this series: nothing to split
            i_ = out.index(x)
            if any(same(y['name'], canon) for y in out) or (bl == 'English' and any(listed(y['name']) for y in out if y is not x)):
                out.pop(i_)  # Audible's series is already there (English: another Audible-listed series of the book stays, the foreign one goes)
            else: out[i_] = {'name': canon, 'sequence': a.get('sequence') or x.get('sequence')}
            notes.append(f"'{x['name']}' -> '{canon}' #{a.get('sequence') or x.get('sequence') or '-'} ({other} book(s) of that series are in another language; Audible's {bl} series)")
    return out, notes

def folder_narrators(item_data):
    """Narrators named in the book folder '1 - Fool's Errand - Nick Taylor' / '... - A + B' (the downloader's naming): the folder
    cannot be overwritten by an ABS match, so it is evidence when ABS narrator and ASIN were replaced together."""
    base = str((item_data or {}).get('relPath') or '').replace('\\', '/').split('/')[-1]
    parts = [p.strip() for p in base.split(' - ')]
    if len(parts) < 3 or not re.match(r'^(?:book\s*)?\d', parts[0], re.I): return []
    # everything after the title: '... - Brian Sutherland + Donald Katz - introduction' (a role suffix splits at ' - ' too, batch 84)
    from abs_series import is_role_credit as _irc, role_base as _rbase
    tail = parts[-1]
    if len(parts) >= 4 and _irc(f"X - {parts[-1]}"): tail = ' - '.join(parts[-2:])  # the last part is only a role ('introduction')
    names = [n.strip() for n in re.split(r'\s*\+\s*|\s*,\s*|\s+&\s+', re.sub(r'\{[^}]*\}', '', tail)) if n.strip() and not re.search(r'(?i)full cast', n)]
    names = [_rbase(n) if _irc(n) else n for n in names]
    # '09 - Volume 09 - The Town of Strife II': the last part is a TITLE, not a narrator (regress wl01 2026-11-09) -> person names only
    # function words only in lower case ('Eric A. Shelman' is a name: an initial 'A.' is no article - batch 60), an article only first
    person = lambda n: 1 <= len(n.split()) <= 4 and not re.search(r'\d', n) and not re.search(
        r"\b(?:the|of|and|a|an|in|to|on|for|der|die|das|und|des)\b|^(?:The|Der|Die|Das|A|An)\s|(?i:\b(?:vol|volume|book|part|teil|band)\b)|\b[IVX]{2,}\b", n)
    if len(names) > 1 and '+' in tail:  # an explicit 'A + B' list is the downloader's narrator list: keep the person names in it
        return [n for n in names if person(n)]  # ('Casey Wayman + Mind Control and Manipulation Studio' -> Casey Wayman, batch 86)
    return names if names and all(person(n) for n in names) else []

def series_by_membership(lib, asin, host, book_authors, title=None, all_hits=False):
    """Audible lists no series for this product, but an Audible series of a library series of the same author has this book as a member
    - by ASIN, or by the same title (an older listing of the same book: 'The Black Company' is #1 of Audible's 'Chronicles of the Black
    Company'). Names are compared normalized ('The Chronicles ...' = 'Chronicles ...'). Returns (library series name, number) or None."""
    from abs_series import series_key as _k
    if not asin and not title: return None
    me, nt = {_first_author(', '.join(book_authors))}, normalize_title_text(title or '')
    hits = []
    for (l_, low), v in list(SERIES_FACTS.items()):
        if l_ != lib or not (v['authors'] & me) or v['n'] < (1 if all_hits else 2) or non_latin_name(v['name']): continue
        for s_asin in set().union(*[set(a) for t, a in list(SERIES_ASINS.items()) if _k(t) == _k(v['name'])] or [set()]):
            for a, q, t in (_series_members(host, s_asin) or []):
                if a == asin or (nt and normalize_title_text(t or '') == nt): hits.append((v['name'], q))
    hits = list(dict.fromkeys(h for h in hits if h[1]))
    if all_hits:  # one number per series, else that series is ambiguous
        by = {}
        for n_, q_ in hits: by.setdefault(n_, set()).add(q_)
        return [(n_, next(iter(qs))) for n_, qs in by.items() if len(qs) == 1]
    return hits[0] if len({h[0] for h in hits}) == 1 and len({h[1] for h in hits}) == 1 else None

SIBLING_FILE = os.path.join(SCRIPT_DIR, 'sibling_series.json')  # {asin: {'ts': iso, 'extra': [...]}}: series of other recordings (30 days)
SIBLING_CACHE, SIBLING_DAYS = {}, 30

def sibling_series(asin, domain, series_list, title, authors=None):
    """Cached wrapper (round 6 2026-11-08 was ~25 % slower: series lists + sibling products for every book): an ASIN is asked again
    after SIBLING_DAYS only."""
    with IO_LOCK:
        if not SIBLING_CACHE: SIBLING_CACHE.update(rw_json(SIBLING_FILE) or {'_': {}})
        c = SIBLING_CACHE.get(asin)
    if c:
        try:
            if datetime.now(timezone.utc) - datetime.fromisoformat(c['ts']) < timedelta(days=SIBLING_DAYS) and (c.get('v') == 2 or not authors): return c['extra']
        except (KeyError, ValueError): pass
    extra = _sibling_series(asin, domain, series_list, title, authors)
    with IO_LOCK:
        SIBLING_CACHE[asin] = {'ts': datetime.now(timezone.utc).isoformat(timespec='seconds'), 'extra': extra, 'v': 2 if authors else 1}
    return extra

def _sibling_series(asin, domain, series_list, title, authors=None):
    """Series of OTHER recordings of the same work: Audible lists the new Lucy Tregear recording of 'Ship of Magic' only in 'Liveship
    Traders'; the older recording in the same Audible series list (same title, same number) is also 'Realm of the Elderlings' #4.
    A series belongs to the work, not the recording. Returns extra series dicts like md_raw['series'] (name, part, asin)."""
    host = AUDIBLE_API_HOSTS.get(domain or 'www.audible.com')
    have = {(s.get('name') or '').strip().lower() for s in series_list or []}
    extra, nt = [], normalize_title_text(title or '')
    for s in series_list or []:
        if not s.get('asin') or not host: continue
        for a, q, t in (_series_members(host, s['asin']) or []):
            if a == asin or _seq_num(q) != _seq_num(s.get('part')) or normalize_title_text(t or '') != nt: continue
            p = get_audible_api(a, domain)
            for x in ((p or {}).get('meta_raw') or {}).get('series') or [] if isinstance(p, dict) else []:
                nm = (x.get('name') or '').strip()
                if nm and nm.lower() not in have:
                    have.add(nm.lower()); extra.append({'name': nm, 'part': x.get('part'), 'asin': x.get('asin'), 'sibling': a})
    if not extra and host and authors and nt:
        # the series list holds only THIS edition (the full-cast 'Tiffany Aching' list has no standard recordings): other recordings of
        # the same title by the same author from the catalog search, e.g. the standard 'The Wee Free Men' = Discworld #30 (2026-11-09)
        throttle('audible_api', host)
        try:
            r = requests.get(f"https://{host}/1.0/catalog/products", params={"title": title, "author": authors[0], "num_results": 20,
                             "response_groups": "series,contributors,product_desc,product_attrs"}, headers={"User-Agent": FIXED_USER_AGENT, "Accept": "application/json"}, timeout=15)
            prods = (r.json().get('products') or []) if r.status_code == 200 else []
        except (requests.RequestException, ValueError): prods = []
        for p in prods:
            if p.get('asin') == asin or normalize_title_text(p.get('title') or '') != nt: continue
            if not match_author(authors, ', '.join(a_.get('name') or '' for a_ in p.get('authors') or [])): continue
            if is_boxset_title(p.get('title') or ''): continue
            for x in p.get('series') or []:
                nm = (x.get('title') or '').strip()
                if nm and nm.lower() not in have:
                    have.add(nm.lower()); extra.append({'name': nm, 'part': aud_part(x.get('sequence')), 'asin': x.get('asin'), 'sibling': p.get('asin')})
    return extra

def canonical_series(lib, name, book_authors, book_variant=None):
    """Map a series name delivered by Audible to the name used in ABS:
    1. series_aliases.json (merges done earlier), 2. an existing ABS series with the same normalized
    name, same edition and a shared author. Prevents Audible from splitting series again."""
    from abs_series import series_key, series_variant, author_key
    if target := SERIES_ALIASES.get(lib, {}).get(name): return target, 'alias'
    idx_key = f"{','.join(sorted(series_variant(name)))}|{series_key(name)}"
    ent = SERIES_INDEX.get(lib, {}).get(idx_key)
    # Only map if exactly one ABS series has this normalized name and the Audible name itself doesn't exist
    # ('Dune' and 'The Dune Sequence' both exist -> keep 'Dune')
    if ent and len(ent.get('names', [])) == 1 and name not in ent['names'] \
            and ({author_key(a) for a in book_authors} & set(ent.get('authors', []))):
        return ent['name'], 'existing'
    if book_variant and not series_variant(name):  # 'The DemonWars Saga' for a dramatized book: the library's dramatized series of that name
        ent = SERIES_INDEX.get(lib, {}).get(f"{','.join(sorted(book_variant))}|{series_key(name)}")
        if ent and len(ent.get('names', [])) == 1 and ({author_key(a) for a in book_authors} & set(ent.get('authors', []))):
            return ent['name'], 'existing'
    return name, None

def non_latin_name(name):
    """Series name that is not English/German: non-Latin script (Japanese, Korean, Cyrillic ...) or French/Spanish/... function words
    ('Les Annales de la Compagnie Noire'). User rule 2026-11-08: series names are English or German."""
    from abs_series import foreign_series_name
    return foreign_series_name(str(name or ''))

def non_latin_script(name):
    """Only a non-Latin SCRIPT (no doubt it is not an English/German name; a French-looking name may be a real English series name)."""
    return any(c.isalpha() and 'LATIN' not in unicodedata.name(c, '') for c in str(name or ''))

SEQ_WORKS = {}  # (lib, series name lower, number, language) -> normalized titles (junk numbers: abs_series.distinct_works >= 3)

def strip_lead0(q):
    """'08' -> '8', '0.5' stays '0.5' (lstrip('0') made it '.5'), '0' stays '0'."""
    return re.sub(r'^0+(?=\d)', '', str(q))

def clash_seq(lib, name, seq, lang):
    """Two or more different WORKS of the same language hold this number in this series ('First Law World' #1 = The Blade Itself and
    A Little Hatred: the umbrella series took the sub-series numbers)."""
    from abs_series import distinct_works
    q = str(seq or '').strip()
    return bool(q) and distinct_works(SEQ_WORKS.get((lib, str(name or '').strip().lower(), q, map_language(lang) or ''), ())) >= 2

def junk_seq(lib, name, seq, lang):
    """Three or more different WORKS of the same language hold this number in this series ('Shannara (Chronological Order)' #1 on 4
    books, three Publisher's Packs at #1): the number is no information."""
    from abs_series import distinct_works
    q = str(seq or '').strip()
    return bool(q) and distinct_works(SEQ_WORKS.get((lib, str(name or '').strip().lower(), q, map_language(lang) or ''), ())) >= 3

def folder_series_name(item_data, name):
    """The series name is only a folder of the book's path (ABS 'folder structure' metadata), e.g. relPath
    'Robin Hobb/Elolaivat/1 - Ship of Magic' -> 'Elolaivat'. The first folder (author) does not count."""
    parts = [p.strip().lower() for p in str((item_data or {}).get('relPath') or '').replace('\\', '/').split('/') if p.strip()][:-1]
    return bool(name) and str(name).strip().lower() in parts[1:]

def series_in_library(lib, name, authors=None, exact=False):
    """An ABS series with this normalized name (same edition) exists in the library (series_index.json, written by the series
    task); with authors: and it holds a book of one of these authors."""
    from abs_series import series_key, series_variant, author_key
    ent = SERIES_INDEX.get(lib, {}).get(f"{','.join(sorted(series_variant(name)))}|{series_key(name)}")
    if not ent or (exact and name not in (ent.get('names') or [ent.get('name')])): return False
    return not authors or bool({author_key(a) for a in authors} & set(ent.get('authors', [])))

def certainty_score(search_title, authors, narrators, abs_dur, publisher, year, abs_series, aud, hc=None, asin_from_search=False, review_open=False):
    """0-100: how sure are we that the product behind the ASIN is THIS book. Parts are the independent pieces of evidence:
    title 25, author 20, runtime 20 (±2 min) / 10 (inside tolerance), narrator 12, series/volume number 10, publisher+year 8, Hardcover ISBN/ASIN confirmation 5.
    Deductions: ASIN found by search -5, ratings of another edition -20, open review entry -15. Returns (score, {part: points})."""
    if not aud or not aud.get('title_raw'): return 0, {'no Audible product': 0}
    parts = {}
    t = max(search_match_score(search_title, aud['title_raw'], aud.get('subtitle_raw')), title_similarity(clean_title(search_title), clean_title(aud['title_raw'])))
    parts['title'] = round(25 * min(1.0, t))
    parts['author'] = 20 if match_author(authors, ', '.join(aud.get('authors_raw') or [])) else 0
    rt = aud.get('runtime_sec')
    parts['runtime'] = 0 if not (abs_dur and rt) else 20 if abs(abs_dur - rt) <= 120 else 10 if not runtime_mismatch(abs_dur, rt) else 0
    n = lambda xs: {re.sub(r'[^a-z]', '', fold_accents(x).lower()) for x in (xs or []) if x}
    parts['narrator'] = 12 if n(narrators) & n(aud.get('narrators_raw')) else 0
    cand_series = (aud.get('meta_raw') or {}).get('series')
    num = volume_of(aud['title_raw'], aud.get('subtitle_raw')) & extract_volume(search_title) if (volume_of(aud['title_raw'], aud.get('subtitle_raw')) and extract_volume(search_title)) else set()
    parts['number'] = 10 if num or any(an is not None and cn is not None and an == cn for _, an in _series_pairs(abs_series) for _, cn in _series_pairs(cand_series)) else 0
    cp, cy = ((aud.get('meta_raw') or {}).get('publisher') or {}).get('name'), str(parse_release_year((aud.get('meta_raw') or {}).get('releaseDate')) or '')
    parts['publisher+year'] = 8 if cp and publisher and cy and year and re.sub(r'[^a-z0-9]', '', cp.lower()) == re.sub(r'[^a-z0-9]', '', publisher.lower()) and cy == str(year) else 0
    parts['hardcover'] = 5 if hc and hc.get('source') in ('ISBN', 'ASIN') else 0
    score = sum(parts.values())
    if asin_from_search: score -= 5; parts['asin from search'] = -5
    if aud.get('related_edition'): score -= 20; parts['other edition'] = -20
    if review_open: score -= 15; parts['open review entry'] = -15
    return max(0, min(100, score)), parts

def asin_plausibility(abs_title, abs_authors, aud_title, aud_sub, aud_authors, abs_narrators=(), aud_narrators=(), abs_dur=None, aud_dur=None):
    """Deterministic check (no LLM): does the product behind the ASIN look like THIS book?
    Returns (verdict, reasons): 'wrong' = title AND author contradict (the ASIN belongs to another book), 'suspicious' = one of them is off, 'ok'."""
    t = max(search_match_score(abs_title, aud_title, aud_sub), title_similarity(clean_title(abs_title), clean_title(aud_title or "")))
    auth = match_author(abs_authors, ", ".join(aud_authors or [])) if (abs_authors and aud_authors) else None  # None = cannot tell
    reasons = [f"title {t:.2f}", "author " + {True: "✓", False: "✗", None: "?"}[auth]]
    if t < 0.5 and auth is False:
        norm_n = lambda n: re.sub(r"[^a-z]", "", n.lower())
        narr_hit = bool({norm_n(n) for n in abs_narrators or [] if n} & {norm_n(n) for n in aud_narrators or [] if n})
        if narr_hit and abs_dur and aud_dur and abs(abs_dur - aud_dur) <= 120:  # same recording: the ASIN is right, ABS title/author are what is off
            return "asin_ok_metadata_differs", reasons + ["narrator ✓", "runtime ±2 min"]
        return "wrong", reasons
    if (t < 0.5 and auth is not True) or (auth is False and t < 0.85): return "suspicious", reasons
    return "ok", reasons

def language_verdict(abs_title, abs_narrators, aud, lang_locked, abs_series_names=()):
    """ABS and Audible language differ. 'asin_wrong' if the ASIN belongs to another language edition
    (language locked, title doesn't match, or no common narrator), else 'abs_wrong'."""
    if lang_locked: return 'asin_wrong'
    aud_t, aud_s = aud.get('title_raw') or '', aud.get('subtitle_raw') or ''
    sim = max(title_similarity(abs_title, aud_t), title_similarity(abs_title, f"{aud_t} {aud_s}"))
    if sim < 0.6: return 'asin_wrong'
    # Same title in both languages ('Dune', 'Blood Moon'): narrator and series decide
    norm = lambda n: re.sub(r'[^a-z]', '', fold_accents(n).lower())
    a_narr, b_narr = {norm(n) for n in abs_narrators if n}, {norm(n) for n in (aud.get('narrators_raw') or []) if n}
    if a_narr and b_narr: return 'abs_wrong' if a_narr & b_narr else 'asin_wrong'
    from abs_series import series_key
    a_ser = {series_key(x) for x in abs_series_names if x}
    b_ser = {series_key(x.get('name') or '') for x in ((aud.get('meta_raw') or {}).get('series') or []) if x.get('name')}
    if a_ser and b_ser: return 'abs_wrong' if a_ser & b_ser else 'asin_wrong'
    return 'unclear'

def volume_conflict(abs_title, aud_title, aud_sub):
    """True if ABS and Audible name different volume numbers ('Vol. 9' vs 'Vol. 18') -> wrong ASIN."""
    a = extract_volume(abs_title or '')
    b = volume_of(aud_title, aud_sub)
    return bool(a and b and not a & b)

def same_recording(abs_dur, abs_narrators, aud):
    """Same recording: runtime within 2 min AND a shared narrator. Then a differing volume NUMBER is a numbering scheme difference
    ('Book 8' in ABS vs 'Book 7' on Audible), not a wrong ASIN."""
    if not (abs_dur and aud and aud.get('runtime_sec') and abs(abs_dur - aud['runtime_sec']) <= 120): return False
    n = lambda xs: {re.sub(r'[^a-z]', '', fold_accents(x).lower()) for x in (xs or []) if x}
    return bool(n(abs_narrators) & n(aud.get('narrators_raw')))

BOXSET_RE = re.compile(r"\b\d+\s*[-–]\s*\d+\b|\b(trilog(y|ie)|box\s*set|boxed set|omnibus|sammelband|gesamtausgabe|publisher.?s pack|complete (series|collection)|collection)\b|\bbooks?\s+\d+\s*(?:-|–|&|and|,)\s*\d+", re.I)
def is_boxset_title(t): return bool(t and BOXSET_RE.search(t))

ASIN_OWNERS = {}  # asin -> set of item keys (ABS library + ASINs planned in this run); an ASIN is never given to a second book
ISBN_OWNERS = {}  # isbn -> set of item keys (an ISBN is never given to a second book)
TITLE_OWNERS = {}  # (lib, normalized title) -> item keys: a title held by several books says nothing about WHICH volume it is
KEY_LANG = {}  # item key -> book language (who of two books sharing an ISBN holds the one of its own language)
KEY_ASIN = {}  # item key -> ABS ASIN (an ISBN shared by two books with different ASINs is wrong on at least one)
KEY_DUR = {}  # item key -> ABS duration (s): tells a duplicate (owner = same recording) from an owner with a wrong ASIN

def aud_part(q):
    """Audible series sequence -> number. '1, The Stolen Heir' (The Folk of the Air) is the position inside a sub-series, not this
    series' number (it gave The Stolen Heir 'Folk of the Air #1' next to The Cruel Prince #1, 2026-11-08): no number."""
    return None if re.match(r'\s*\d+(?:\.\d+)?\s*,\s*[^\d\s]', str(q or '')) else q

def _seq_num(v):
    m = re.search(r"\d+(?:\.\d+)?", str(v or ""))
    return float(m.group(0)) if m else None

def _series_pairs(series):
    from abs_series import series_key
    out = []
    for s in series or []:
        k = series_key(s.get("name") or s.get("title") or "")
        if k: out.append((k, _seq_num(s.get("sequence") if s.get("sequence") not in (None, "") else s.get("part"))))
    return out

def _novella_range(v):
    """'0.1 - 0.5' (Audible: The Assassin's Blade = Throne of Glass novellas 0.1-0.5) -> (0.1, 0.5). Only ranges spanning less than
    one volume: a box set '1-3' is several books and stays a different number."""
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*[-\u2013]\s*(\d+(?:\.\d+)?)\s*", str(v or ""))
    return (float(m.group(1)), float(m.group(2))) if m and 0 < float(m.group(2)) - float(m.group(1)) < 1 else None

def part_scheme_number(abs_title, cand_title, cand_series):
    """Audible counts 'Part 1, Volume 3' as #1.3 while ABS counts volumes (#3): no conflict when both titles are IDENTICAL and name
    the part and volume that Audible's number encodes (the title identifies the volume; Ascendance of a Bookworm, 2026-11-08)."""
    na, nc = normalize_title_text(abs_title or ''), normalize_title_text(cand_title or '')
    if not nc or not (na == nc or na.startswith(nc + ' ')): return False  # ABS may append the subtitle ('...: Ascendance of a Bookworm, Book 3')
    rx = r'\bpart\s+(\d+)\W+(?:volume|vol\.?|book)\s+(\d+)\b'
    m, ma = re.search(rx, cand_title or '', re.I), re.search(rx, abs_title or '', re.I)
    if not m or not ma or (int(m.group(1)), int(m.group(2))) != (int(ma.group(1)), int(ma.group(2))): return False
    want = f"{int(m.group(1))}.{int(m.group(2))}"
    return any(str((x.get('sequence') if x.get('sequence') not in (None, '') else x.get('part')) or '').strip() == want for x in cand_series or [])

def series_seq_conflict(abs_series, cand_series):
    """True if ABS and a candidate both state a number for the same series and the numbers differ (-> other volume).
    An ABS number inside a candidate's novella range ('0.1 - 0.5' vs 0.5) is no conflict."""
    rng = {}
    for s in cand_series or []:
        if r_ := _novella_range(s.get("sequence") if s.get("sequence") not in (None, "") else s.get("part")):
            from abs_series import series_key
            rng[series_key(s.get("name") or s.get("title") or "")] = r_
    for ak, an in _series_pairs(abs_series):
        if an is None: continue
        hits = [(ck, cn) for ck, cn in _series_pairs(cand_series) if cn is not None and (ak == ck or difflib.SequenceMatcher(None, ak, ck).ratio() >= 0.9)]
        # one agreeing entry is enough: Audible lists 'Dune' #1 AND 'The Dune Sequence' #12 (both key 'dune') for the same book
        if any(cn == an or (ck in rng and rng[ck][0] <= an <= rng[ck][1]) for ck, cn in hits): continue
        if hits: return True
    return False

_SERIES_SEQ, _ASIN_TITLE = {}, {}  # (host, series asin) -> [(child asin, sequence)]; (host, asin) -> title (cached per run)

def _series_members(host, series_asin):
    """Children of an Audible series with their numbers and titles (2 requests per series, cached)."""
    if (host, series_asin) not in _SERIES_SEQ:
        hdr = {"User-Agent": FIXED_USER_AGENT, "Accept": "application/json"}
        throttle('audible_api', host)
        try:
            r = requests.get(f"https://{host}/1.0/catalog/products/{series_asin}", params={"response_groups": "relationships"}, headers=hdr, timeout=15)
            rel = ((r.json().get('product') or {}).get('relationships') or []) if r.status_code == 200 else None
        except (requests.RequestException, ValueError): rel = None
        if rel is None: return None  # unknown: never used as proof
        kids = [(x['asin'], str(x.get('sequence') or '').strip()) for x in rel if x.get('relationship_to_product') == 'child' and x.get('asin')]
        need = [a for a, _ in kids if (host, a) not in _ASIN_TITLE]
        for i in range(0, len(need), 50):
            throttle('audible_api', host)
            try:
                r = requests.get(f"https://{host}/1.0/catalog/products", params={"asins": ",".join(need[i:i + 50]), "response_groups": "product_desc"},
                                 headers=hdr, timeout=20)
                prods = (r.json().get('products') or []) if r.status_code == 200 else None
            except (requests.RequestException, ValueError): prods = None
            if prods is None: return None
            for p in prods: _ASIN_TITLE[(host, p.get('asin'))] = p.get('title') or ''
        _SERIES_SEQ[(host, series_asin)] = kids
    return [(a, q, _ASIN_TITLE.get((host, a), '')) for a, q in _SERIES_SEQ[(host, series_asin)]]

def abs_number_disproved(abs_series, cand, cand_title, cand_series, host):
    """ABS states another number for this series than Audible does. The ABS number is PROVABLY wrong (user wish 2026-11-08) only if
    Audible's own series list has no OTHER volume with this title (The Hunter's Code 1 and 2 share title and narrator) and the ABS
    number belongs to another title there or does not exist in that series. Returns {series key: Audible number} or None.
    The caller additionally requires the same recording (identical title, runtime ±2 min, a shared narrator)."""
    fixes = {}
    nt = normalize_title_text(cand_title)
    for ak, an in _series_pairs(abs_series):
        for s in cand_series or []:
            ck, cn = (_series_pairs([s]) or [(None, None)])[0]
            if an is None or cn is None or an == cn or not (ak == ck or difflib.SequenceMatcher(None, ak, ck).ratio() >= 0.9): continue
            if not s.get('asin'): return None
            members = _series_members(host, s['asin'])
            if not members: return None
            num = lambda q: float(q) if re.fullmatch(r'\d+(\.\d+)?', str(q or '').strip()) else None
            # another VOLUME with this title (other number: The Hunter's Code 1/2) -> no proof; the same number = another edition, fine
            if any(a != cand and normalize_title_text(t) == nt and num(q) != cn for a, q, t in members): return None
            at_abs = [t for a, q, t in members if num(q) == an]
            if any(normalize_title_text(t) == nt for t in at_abs): return None
            fixes[ck] = s.get('part') or s.get('sequence')
    return fixes or None

def series_number_hit(abs_series, cand_series):
    """ABS and the candidate list the same series (name equal after normalisation) with the SAME number."""
    return any(an is not None and cn is not None and an == cn and (ak == ck or difflib.SequenceMatcher(None, ak, ck).ratio() >= 0.9)
               for ak, an in _series_pairs(abs_series) for ck, cn in _series_pairs(cand_series))

def series_number_loose(abs_series, cand_series):
    """Same NUMBER in some series of ABS and of the candidate (series named differently: 'The Original Shannara Trilogy' vs 'The Sword of Shannara (Unabridged)')."""
    return any(an is not None and cn is not None and an == cn for _, an in _series_pairs(abs_series) for _, cn in _series_pairs(cand_series))

def series_seq_unconfirmed(abs_series, cand_series):
    """True if ABS says 'volume 2 or higher' of a series but the candidate shows no matching number
    (e.g. ABS 'Hell Mode #3' vs Audible 'Hell Mode' without number -> probably volume 1)."""
    cands = _series_pairs(cand_series)
    for ak, an in _series_pairs(abs_series):
        if an is None or an <= 1: continue
        if not any(cn == an and (ak == ck or difflib.SequenceMatcher(None, ak, ck).ratio() >= 0.9) for ck, cn in cands): return True
    return False

class RateLimitException(Exception):
    def __init__(self, msg, is_hard=False): super().__init__(msg); self.is_hard = is_hard

class SourceBlockedException(Exception):
    """A non-critical source (Goodreads) blocks us. Skip the source instead of aborting the run."""

# ================= UTILS =================

_last_request = {}
def throttle(source, host=None):
    """Wait until MIN_INTERVAL[source] seconds have passed since the last request to that source (with host: to that host -
    the Audible marketplaces are separate servers, so .com and .de do not slow each other down)."""
    if recorder.REPLAY_DIR: return  # replay: nothing goes out
    if recorder.RECORD_DIR and not (source == 'goodreads' and GR_IMPERSONATE): return  # recorder.py waits before LIVE requests;
    # impersonated Goodreads requests bypass the recorder, so they must be spaced here (2026-11-08: a test burst of 12 pages in 30 s)
    tkey = f"{source}:{host}" if host else source
    with _THROTTLE_LOCKS.setdefault(tkey, threading.Lock()):  # parallel books: the spacing holds across all threads
        wait = MIN_INTERVAL.get(source, 0) - (time.monotonic() - _last_request.get(tkey, 0))
        if wait > 0: time.sleep(wait + random.uniform(0, MIN_INTERVAL[source] * 0.4 if source == 'goodreads' else min(0.5, MIN_INTERVAL.get(source, 0) / 2)))
        _last_request[tkey] = time.monotonic()

GR_BASE_INTERVAL = MIN_INTERVAL['goodreads']
def gr_autothrottle(r):
    """AutoThrottle (idea from Scrapling): a slow Goodreads answer (server under load or starting to rate-limit) widens
    the interval x1.5 (max 6x the setting), fast answers narrow it back slowly; never below GOODREADS_INTERVAL."""
    el = r.elapsed.total_seconds() if hasattr(r.elapsed, 'total_seconds') else float(r.elapsed or 0)  # curl_cffi: float seconds
    cur = MIN_INTERVAL['goodreads']
    new = min(cur * 1.5, GR_BASE_INTERVAL * 6) if el > 3 else max(GR_BASE_INTERVAL, cur * 0.9)
    if abs(new - cur) >= 1 or (new == GR_BASE_INTERVAL and cur != new):
        logging.info(f"        🐢 Goodreads answered in {el:.1f}s -> interval {cur:.0f}s -> {new:.0f}s")
    MIN_INTERVAL['goodreads'] = new

def retry_after_seconds(r):
    """Retry-After header (seconds or HTTP date) -> seconds, else None."""
    v = (r.headers.get('Retry-After') or '').strip()
    if not v: return None
    if v.isdigit(): return int(v)
    try:
        from email.utils import parsedate_to_datetime
        return max(0, int((parsedate_to_datetime(v) - datetime.now(timezone.utc)).total_seconds()))
    except Exception:
        return None

def book_lang_allowed(l):
    """BOOK_LANGUAGES (user 2026-11-09: 'only German and English books, everything else is an error'): empty = every language."""
    return not BOOK_LANGUAGES or map_language(l) in BOOK_LANGUAGES

def map_language(raw):
    if not raw: return None
    raw = str(raw).strip()
    return LANGUAGE_MAP.get(raw.lower(), raw[:1].upper() + raw[1:])

def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    f = os.path.join(LOG_DIR, f"run_{RUN_ID}.log" if os.getenv('RUN_ID') else f"run_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
    logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler(f, encoding='utf-8'), logging.StreamHandler()])
    global LOG_ROUTER
    root = logging.getLogger()
    LOG_ROUTER = _BookLogRouter(list(root.handlers))
    root.handlers = [LOG_ROUTER]
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    return f



# ================= CHANGE JOURNAL =================

def _plain(v):
    """Make ABS values journal-friendly (series/authors lose their internal ids)."""
    if isinstance(v, list):
        return [({k: x.get(k) for k in ('name', 'sequence') if k in x} if ('name' in x or 'sequence' in x) else dict(x)) if isinstance(x, dict) else x for x in v]  # chapters keep start/end/title
    return v

def journal_change(iid, key, title, field, old, new, source, extra=None):
    """Append one change to changes.jsonl (or the planned-changes file in DRY_RUN)."""
    entry = {'run': RUN_ID, 'ts': datetime.now(timezone.utc).isoformat(timespec='seconds'), 'item': iid, 'key': key,
             'title': title, 'field': field, 'old': _plain(old), 'new': _plain(new), 'source': source, 'dry_run': DRY_RUN}
    if extra: entry.update(extra)
    path = PLANNED_FILE if DRY_RUN else CHANGES_FILE
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with IO_LOCK, open(path, 'a', encoding='utf-8') as f: f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError as e: logging.error(f"Journal write failed: {e}")

def listing_value(field, v):
    """A metadata value in the form of the ABS library listing (one request per run compares against it)."""
    if v is None: return ''
    if isinstance(v, str): pass  # already the listing form ('A #1, B #2' / 'Name, Name'); iterating it gave '' for series (2026-11-08)
    elif field in ('authors', 'narrators'):
        v = ', '.join((x.get('name') if isinstance(x, dict) else str(x)) for x in (v or []) if x)
    elif field == 'series':
        v = ', '.join(f"{x.get('name')} #{x.get('sequence')}" if x.get('sequence') not in (None, '') else str(x.get('name')) for x in (v or []) if isinstance(x, dict) and x.get('name'))
    return re.sub(r'\s+', ' ', str(v)).strip().lower()

def listing_field(item, field):
    md = (item.get('media') or {}).get('metadata') or {}
    return {'authors': md.get('authorName'), 'narrators': md.get('narratorName'), 'series': md.get('seriesName')}.get(field, md.get(field))

def abs_update(iid, key, title, updates, old_meta, source, tags=None, old_tags=None, extra=None):
    """The only way this script writes metadata/tags to ABS. Journals every field (old -> new).
    In DRY_RUN nothing is sent, the change is only written to the planned-changes file."""
    # User decision 2026-11-08: only lock_<field> tags protect a field. A value changed in ABS (by you or by an ABS rescan -
    # ABS cannot tell them apart) is checked again and corrected like any other value.
    if not updates and tags is None: return True
    if updates and key and not str(source).startswith(('applied:', 'review', 'manual', 'series_page', 'book_page')):
        # a change YOU undid (history / undo) is your decision: the script never writes the same value again (2026-11-09) - unless you apply it yourself
        journal_last(key, 'asin')
        for f_ in [f_ for f_, v_ in updates.items() if listing_value(f_, v_) in _UNDONE_VAL.get((key, f_), ())]:
            logging.info(f"        ↩️ {f_}: '{listing_value(f_, updates[f_])}' was undone by you before - not written again (lock_{f_} to keep it for good)")
            updates = {k_: v_ for k_, v_ in updates.items() if k_ != f_}
        if not updates and tags is None: return False
    if not DRY_RUN:
        payload = {}
        if updates: payload['metadata'] = updates
        if tags is not None: payload['tags'] = tags
        r = abs_session.patch(f"{ABS_URL}/api/items/{iid}/media", json=payload)
        if r.status_code != 200:
            logging.error(f"        ❌ ABS update failed for '{title}' (HTTP {r.status_code}): {r.text[:200]}")
            return False
    for field, new in (updates or {}).items():
        journal_change(iid, key, title, field, (old_meta or {}).get(field), new, source, extra)
        if field in WATCH_FIELDS and not DRY_RUN:
            with IO_LOCK: LAST_WRITTEN.setdefault(key, {})[field] = {'ours': listing_value(field, new), 'old': listing_value(field, (old_meta or {}).get(field))}
    if tags is not None: journal_change(iid, key, title, 'tags', old_tags, tags, source, extra)
    return True

def image_ext(data):
    """File extension from the image bytes (magic numbers)."""
    if data[:3] == b'\xff\xd8\xff': return 'jpg'
    if data[:8] == b'\x89PNG\r\n\x1a\n': return 'png'
    if data[:4] == b'RIFF' and data[8:12] == b'WEBP': return 'webp'
    return 'jpg'

def abs_set_cover(iid, key, title, url, source, replace=False):
    """Set a cover from URL. replace=True: ABS already has a cover -> it is backed up first (cover_backup/<item>_<run>.img) and the
    journal's 'old' holds the backup path, so undo uploads it again. Without a backup nothing is replaced. Undo of a new cover removes it."""
    old = None
    if not DRY_RUN:
        if replace:
            try:
                rb = abs_session.get(f"{ABS_URL}/api/items/{iid}/cover", params={'raw': 1}, timeout=60)
                if rb.status_code != 200 or not rb.content: raise OSError(f"HTTP {rb.status_code}")
                os.makedirs(os.path.join(SCRIPT_DIR, 'cover_backup'), exist_ok=True)
                ext = image_ext(rb.content)  # ABS accepts only png, jpg, jpeg, webp file names on upload (undo)
                old = os.path.join(SCRIPT_DIR, 'cover_backup', f"{iid}_{RUN_ID}.{ext}")
                with open(old, 'wb') as f: f.write(rb.content)
            except Exception as e:
                logging.warning(f"        ⚠️ Old cover of '{title}' could not be backed up ({e}): cover NOT replaced")
                return False
        r = abs_session.post(f"{ABS_URL}/api/items/{iid}/cover", json={"url": url})
        if r.status_code != 200:
            logging.error(f"        ❌ Cover upload failed for '{title}' (HTTP {r.status_code}): {r.text[:200]}")
            return False
    elif replace: old = '(existing cover, backed up on a real run)'
    journal_change(iid, key, title, 'cover', old, url, source)
    return True

MATCH_CHAPTERS = os.getenv('MATCH_CHAPTERS', 'true').lower() == 'true'  # take Audible's chapters for poorly chaptered files (user 2026-11-10)
CHAPTER_REGION = {'www.audible.com': 'us', 'www.audible.de': 'de', 'www.audible.co.uk': 'uk', 'www.audible.ca': 'ca', 'www.audible.com.au': 'au',
                  'www.audible.fr': 'fr', 'www.audible.it': 'it', 'www.audible.es': 'es', 'www.audible.in': 'in', 'www.audible.co.jp': 'jp'}
RE_GENERIC_CH = re.compile(r'(?i)^\s*(?:chapter|kapitel|track|part|teil|ch\.?|cd\s*\d+\s*-?\s*track|file|datei|section|\d+\s*[-.]?)?\s*\d{0,4}\s*$')

def chapters_poor(chs, dur):
    """ABS chapters worth replacing: none, a single one, only generic names ('Chapter 7', 'Track 03', '12'), or running past the end."""
    if not chs or len(chs) <= 1: return 'no chapters' if not chs else 'a single chapter'
    if dur and any(float(c.get('end') or 0) > dur + 30 for c in chs): return 'chapters run past the end of the file'  # a few seconds over the end is usual (rounding)
    if all(RE_GENERIC_CH.match(str(c.get('title') or '')) for c in chs): return 'only numbered chapters without names'
    return None

def audible_chapters(asin, domain):
    """Audible's chapter list (Audnexus, the source ABS itself uses for 'Find chapters'): ([(start_s, length_s, title)], intro_s, outro_s)."""
    reg = CHAPTER_REGION.get(domain or 'www.audible.com', 'us')
    throttle('audible_api', 'api.audnex.us')
    try:
        r = requests.get(f"https://api.audnex.us/books/{asin}/chapters", params={'region': reg}, headers={"User-Agent": FIXED_USER_AGENT}, timeout=20)
        j = r.json() if r.status_code == 200 else None
    except (requests.RequestException, ValueError): j = None
    if not j or not j.get('chapters'): return None
    chs = [(c['startOffsetMs'] / 1000, c['lengthMs'] / 1000, (c.get('title') or '').strip()) for c in j['chapters'] if c.get('lengthMs') is not None]
    return chs, (j.get('brandIntroDurationMs') or 0) / 1000, (j.get('brandOutroDurationMs') or 0) / 1000

def match_chapters(abs_chs, dur, aud):
    """New chapter list or (None, reason). Reliable only:
    (1) the file length equals Audible's (<= 5 s): Audible's chapters as they are;
    (2) the file has its own markers and their LENGTHS match Audible's one by one (inner chapters <= 3 s, the whole list shifted by
        a fixed offset - intro/outro cut differently): Audible's names on the file's own times;
    (3) no usable markers, the difference (<= 2 min) is exactly Audible's brand intro and/or outro (<= 3 s): shifted by the intro."""
    chs, intro, outro = aud
    total = chs[-1][0] + chs[-1][1]
    diff = dur - total
    mk = lambda starts, titles: [{'id': i, 'start': round(max(0.0, st), 3), 'end': round(starts[i + 1] if i + 1 < len(starts) else dur, 3), 'title': t or f"Chapter {i + 1}"}
                                 for i, (st, t) in enumerate(zip(starts, titles))]
    titles = [c[2] for c in chs]
    if intro and abs(diff) >= 1.5 and (abs(-diff - intro) <= 1 or abs(-diff - intro - outro) <= 1):  # the file lacks Audible's intro: shift by it
        return mk([0.0] + [c[0] - intro for c in chs[1:]], titles), f"the file lacks Audible's intro ({diff:+.1f} s)"
    if abs(diff) <= 5:
        return mk([c[0] for c in chs], titles), f"length matches Audible ({diff:+.1f} s)"
    an = anchor_align(abs_chs, chs, dur)
    if an:  # Audible's starts moved by the measured offset, cut at the end of the file (marks past the end are dropped)
        off = an[0]
        starts = [0.0] + [c[0] + off for c in chs[1:]]
        keep = [i for i, st in enumerate(starts) if st < dur - 1]
        return mk([starts[i] for i in keep], [titles[i] for i in keep]), f"the file's chapter marks sit at Audible's chapter starts ({an[1]}/{an[2]}, offset {off:+.1f} s)"
    if abs(diff) > 120: return None, f"length differs by {diff:+.0f} s"
    if abs_chs and len(abs_chs) == len(chs) and len(chs) >= 3 and max(float(c.get('end') or 0) for c in abs_chs) <= dur + 2:  # broken marks (past the end) are no anchor
        al = [float(c['end']) - float(c['start']) for c in abs_chs]
        if all(abs(a - b[1]) <= 3 for a, b in list(zip(al, chs))[1:-1]) and abs(al[0] - chs[0][1]) <= 125 and abs(al[-1] - chs[-1][1]) <= 125:
            return [{'id': i, 'start': float(c['start']), 'end': float(c['end']), 'title': titles[i] or c.get('title')} for i, c in enumerate(abs_chs)], \
                   "the file's own chapter marks match Audible's chapter lengths"
    for shift, why in ((intro + outro, 'intro + outro'), (intro, 'intro'), (outro, 'outro')):
        if shift and abs(-diff - shift) <= 3:
            off = intro if why != 'outro' else 0.0
            return mk([0.0] + [c[0] - off for c in chs[1:]], titles), f"the file lacks Audible's {why} ({diff:+.1f} s)"
    return None, f"length differs by {diff:+.0f} s and no reliable alignment"

def anchor_align(abs_chs, chs, dur, need=0.9):
    """The file's own chapter marks sit where Audible's chapters start, up to one constant offset (Audible's intro cut differently):
    >= need of Audible's chapter starts have a file mark within 2 s after the offset. Returns (offset, matched, total) or None.
    Proves the chapter structure even when the marks run past the end of the file (marks made for the longer Audible file)."""
    starts = sorted(float(c.get('start') or 0) for c in abs_chs or [])
    inner = [c[0] for c in chs[1:]]
    if len(starts) < 3 or len(inner) < 2: return None
    near = lambda x: min(starts, key=lambda y: abs(y - x))
    diffs = sorted(near(x) - x for x in inner)
    off = diffs[len(diffs) // 2]
    hit = sum(1 for x in inner if abs(near(x + off) - (x + off)) <= 2)
    return (off, hit, len(inner)) if hit >= need * len(inner) and abs(off) <= 150 else None

def abs_set_chapters(iid, key, title, new, old, source):
    """Write chapters (own ABS endpoint), journaled like every write; undo writes the old list back."""
    if not DRY_RUN:
        r = abs_session.post(f"{ABS_URL}/api/items/{iid}/chapters", json={'chapters': new})
        if r.status_code != 200:
            logging.error(f"        ❌ Chapter update failed for '{title}' (HTTP {r.status_code}): {r.text[:150]}"); return False
    journal_change(iid, key, title, 'chapters', [{k: c.get(k) for k in ('start', 'end', 'title')} for c in old or []],
                   [{k: c.get(k) for k in ('start', 'end', 'title')} for c in new], source)
    return True

def runtime_mismatch(abs_sec, aud_sec):
    """True if ABS file duration and Audible runtime differ by more than both tolerances (other edition)."""
    if not abs_sec or not aud_sec: return False
    diff = abs(float(abs_sec) - float(aud_sec))
    return diff > RUNTIME_TOLERANCE_MIN * 60 and diff > float(aud_sec) * RUNTIME_TOLERANCE_PCT / 100

def rw_json(path, data=None):
    try:
        if data is None: 
            return json.load(open(path, 'r', encoding='utf-8')) if os.path.exists(path) else {}
        else:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with IO_LOCK:  # parallel books: one writer at a time, and a dict changed by another thread is dumped again
                for _try in range(5):
                    try: payload = json.dumps(data, indent=4, ensure_ascii=False); break
                    except RuntimeError: time.sleep(0.01)
                else: return None
                tmp_path = path + f".tmp{threading.get_ident()}"
                with open(tmp_path, 'w', encoding='utf-8') as f:
                    f.write(payload)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, path)
    except: return {} if data is None else None

def update_report(src, key, title, author, ident, reason, success):
    if success: reports[src].pop(key, None)
    else: reports[src][key] = {"key": key, "title": title, "author": author, "identifier": ident, "reason": reason, "last_check": datetime.now().strftime("%Y-%m-%d")}

def save_reports():
    for k, v in reports.items(): rw_json(os.path.join(REPORT_DIR, f"missing_{k}.json"), sorted(v.values(), key=lambda x: x['title']))

def write_env_file(log_file, start_time, error=None):
    dur = f"{int((datetime.now() - start_time).total_seconds() // 60)}m {int((datetime.now() - start_time).total_seconds() % 60)}s"
    if error: sub, icon, head = "ABS Ratings: Start failed ❌", "alert", "Error"
    elif stats['aborted_ratelimit']: sub, icon, head = "ABS Ratings: Aborted 🛑", "alert", "Rate Limit detected!"
    elif SOURCE_DOWN: sub, icon, head = "ABS Ratings: Source problem ⚠️", "alert", f"{', '.join(s.capitalize() for s in SOURCE_DOWN)} switched off"
    elif stats['failed'] > 0: sub, icon, head = "ABS Ratings: Error ❌", "alert", "Error occurred!"
    elif any(stats[k] > 0 for k in ['success', 'recycled', 'asin_found', 'asin_migrated', 'isbn_added', 'meta_updated', 'series_merged']): sub, icon, head = "ABS Ratings: Success ✅", "normal", "Update complete"
    else: sub, icon, head = "ABS Ratings: Info ℹ️", "normal", "No changes"
    
    body = f"Proc: {stats['processed']} | New: {stats['success']} | ManUpd: {stats['manual_retrigger']} | MetaUpd: {stats['meta_updated']} | ASIN+: {stats['asin_found']} | Mig: {stats['asin_migrated']} | ISBN+: {stats['isbn_added']} | Fix: {stats['isbn_repaired']} | NoRatYet: {stats['no_ratings_yet']} | GR-: {stats['gr_missing']} | Cover+: {stats['cover_added']} | Series merged: {stats['series_merged']} | Err: {stats['failed']}"
    if GR_STATE['blocked']: body += " | ⚠️ Goodreads blocked"
    if stats['gr_deferred']: body += f" | GR deferred: {stats['gr_deferred']}"
    if stats['aborted_ratelimit']: body += " | ⚠️ ABORTED (Rate Limit)"
    for s_, why in SOURCE_DOWN.items(): body += f" | ⚠️ {s_.capitalize()} off: {why}"
    if NEW_VOLUMES: body += f" | 📚 New volumes of your series: {', '.join(NEW_VOLUMES[:5])}{' …' if len(NEW_VOLUMES) > 5 else ''}"
    if DRY_RUN: body += " | DRY RUN"
    if error: body = error.replace("'", "")
    
    try:
        with open(ENV_OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write(f"ABS_SUBJECT='{sub}'\nABS_ICON='{icon}'\nABS_HEADER='{head}'\nABS_DURATION='{dur}'\nABS_REPORT_BODY='{body}'\nABS_LOG_FILE='{os.path.basename(log_file)}'\n")
    except: pass

def safe_float(v): return float(str(v).replace(',', '.')) if v else 0.0

def is_valid_rating(v):
    try:
        val = safe_float(v)
        return 0.1 <= val <= 5.0
    except: return False

def clean_title(t): return RE_CLEAN_TITLE.sub('', t).split(':')[0].split(' - ')[0].strip() if t else ""

def normalize_title_text(t):
    if not t: return ""
    t = RE_CLEAN_TITLE.sub(' ', t)
    t = t.lower()
    t = re.sub(r"[^\w\s]", ' ', t)  # all punctuation (commas, apostrophes, colons ...)
    for word, digit in NUMBER_MAP.items():
        t = re.sub(r'\b' + word + r'\b', digit, t)
    t = RE_NOISE.sub('', t)
    return re.sub(r'\s+', ' ', t).strip()

def moon_rating(v):
    full_s, half_s, empty_s = RATING_SYMBOL_SETS.get(RATING_ICONS, RATING_SYMBOL_SETS['moons'])
    v = safe_float(v)
    if v == 0: return empty_s * 5
    full, decimal = int(v), v - int(v)
    if decimal >= 0.75:
        full += 1
        half = 0
    elif decimal >= 0.25:
        half = 1
    else:
        half = 0
    full = min(full, 5); half = half if full < 5 else 0
    return full_s * full + half_s * half + empty_s * (5 - full - half)

def parse_release_year(rel_date):
    """Extract a 4-digit year from Audible dates like 'YYYY-MM-DD', 'MM-DD-YYYY' or 'MM-DD-YY'."""
    if not rel_date: return None
    parts = re.split(r'[-./]', str(rel_date).strip())
    if len(parts[0]) == 4 and parts[0].isdigit(): year = parts[0]
    elif parts[-1].isdigit() and len(parts[-1]) == 4: year = parts[-1]
    elif parts[-1].isdigit() and len(parts[-1]) == 2:
        yy = int(parts[-1])
        # Pivot: years up to the current year's two digits are 20xx, everything above is 19xx
        year = str((2000 if yy <= datetime.now().year % 100 else 1900) + yy)
    else: return None
    return year if 1900 <= int(year) <= datetime.now().year + 2 else None

def normalize_isbn(v):
    """Return a cleaned ISBN-10/13 or None (rejects ASINs and garbage)."""
    if not v: return None
    c = re.sub(r'[\s-]', '', str(v)).upper()
    if re.fullmatch(r'\d{13}', c) and c.startswith(('978', '979')): return c
    if re.fullmatch(r'\d{9}[\dX]', c): return c
    return None

def isbn_checksum_ok(c):
    """True for a valid ISBN-10 or ISBN-13 (cleaned string)."""
    if not c: return False
    if len(c) == 13 and c.isdigit(): return (10 - sum(int(d) * (1 if i % 2 == 0 else 3) for i, d in enumerate(c[:12])) % 10) % 10 == int(c[12])
    if len(c) == 10 and re.fullmatch(r'\d{9}[\dX]', c): return sum((10 - i) * (10 if d == 'X' else int(d)) for i, d in enumerate(c)) % 11 == 0
    return False

def isbn_bad_reason(v):
    """Why an ABS ISBN value is provably wrong (None = looks fine or empty). An ASIN or a number with a wrong check digit is no ISBN."""
    if not v or not str(v).strip(): return None
    raw = re.sub(r'[\s-]', '', str(v)).upper()
    if re.fullmatch(r'B0[A-Z0-9]{8}', raw): return 'value is an ASIN, not an ISBN'
    c = normalize_isbn(raw)
    if not c: return 'not a valid ISBN format'
    if not isbn_checksum_ok(c): return 'check digit is wrong'
    return None

RE_DUP_TITLE = re.compile(r'^\s*(.+?)\s*:\s*\1\s*(?::\s*(.+))?$', re.IGNORECASE)
# 'The Book of the New Sun 05 The Urth of the New Sun': number directly between two names, separated by spaces only.
# 'Casual Farming 3: A Slow Living LitRPG' / 'ANIMA, Vol. 4: Light Novel' (colon after the number) are normal titles.
RE_SERIES_PREFIX = re.compile(r'^\s*(.{4,}?)\s+(0\d{1,2}(?:-\d{1,2}|\.\d{1,2})?)\s+([^:].{2,})$')  # zero-padded like file names ('05'); the series name itself may contain a colon ('Carl Weber\'s Kingpins: Queens 02 Kingdom')

RE_LEADING_NUM = re.compile(r'^\s*(0\d{1,2})\s*[-.:]?\s+(.{3,})$')  # '01 Nightside the Long Sun' (zero-padded only)

def series_tail_head(title, abs_series, aud_title):
    """'X: <one of the book's series names>' whose head X starts like Audible's title (same recording) -> X, else None."""
    n = lambda x: normalize_title_text(x or '')
    for sn in abs_series or []:
        if sn and ':' in (title or '') and n(title).endswith(' ' + n(sn)) and re.search(r'(?i):\s*' + re.escape(sn.strip()) + r'\s*$', title):
            head = re.sub(r'(?i)\s*:\s*' + re.escape(sn.strip()) + r'\s*$', '', title).strip()
            if head and n(head) != n(title) and (n(head) + ' ').startswith(n(aud_title) + ' ') or n(aud_title).startswith(n(head) + ' ') and head:
                return head
    return None

def glued_subtitle(title, abs_sub, aud_title, aud_sub, abs_series=None):
    """The ABS title is Audible's title + ': ' + Audible's subtitle (once or the subtitle twice) -> 'title' (title only) or 'subtitle' (title
    and the subtitle field: it is empty or names a series the book is not in - 'Moreta' had 'Victor of Tucson, Book 4'). Else None.
    The ABS subtitle must be Audible's, a foreign series label, or empty with the subtitle twice in the title: an own subtitle (or none:
    'Mistborn: The Final Empire' is a normal title) keeps the title as it is."""
    n = lambda x: normalize_title_text(x or '')
    if not (title and aud_title and aud_sub) or n(aud_title) == n(title) or ':' not in title: return None
    if abs_sub and n(title) == n(f"{aud_title}: {abs_sub}"): return 'title'  # 'Casual Farming: Sowing Season, Book 1' + that subtitle
    if n(title) not in (n(f"{aud_title}: {aud_sub}"), n(f"{aud_title}: {aud_sub}: {aud_sub}")): return None
    if n(abs_sub) == n(aud_sub): return 'title'  # the subtitle is shown twice in ABS (title + subtitle field)
    if not abs_sub:
        if n(title) == n(f"{aud_title}: {aud_sub}: {aud_sub}"): return 'subtitle'
        # Audible's subtitle is only a series label of the book's own series ('Legend of the Arch Magus, Book 1, 2'): never part of the title
        m0 = re.match(r'^(.+?),?\s+(?:Books?|Volumes?|Vol\.|Band|Teil|Part)\s+[\d,\s&and-]+$', aud_sub.strip(), re.I)
        if m0 and any(title_similarity(m0.group(1), x) >= 0.85 for x in abs_series or [] if x): return 'subtitle'
        return None  # 'Mistborn: The Final Empire' alone is a normal title
    m = re.match(r'^(.+?),?\s+(?:Book|Volume|Vol\.|Band|Teil|Part)\s+\d+(?:\.\d+)?$', abs_sub.strip(), re.I)
    if m and not any(title_similarity(m.group(1), x) >= 0.85 for x in abs_series or [] if x) and title_similarity(m.group(1), aud_sub) < 0.85: return 'subtitle'
    return None

def parse_messy_title(t, abs_series_names=None):
    """Detect broken (file name style) titles.
    'Series 05 Title' -> title='Title', series='Series', sequence='5'
    'X: X: Subtitle'  -> title='X', subtitle='Subtitle'
    Returns None if the title looks normal."""
    if not t: return None
    if m := RE_DUP_TITLE.match(t):
        return {'title': m.group(1).strip(), 'subtitle': (m.group(2) or '').strip() or None, 'kind': 'duplicate'}
    if m := RE_SERIES_PREFIX.match(t):
        series, seq, rest = m.group(1).strip(' -:,'), m.group(2), m.group(3).strip()
        if re.search(r'(?i)\b(vol\.?|volume|book|band|teil|part|no\.?)$', series): return None  # 'X Vol. 4 Y' is a normal title
        # Guard against normal titles with numbers ("Catch 22 ..."): the series part needs at least 2 words
        is_abs_series = normalize_title_text(series) in {normalize_title_text(n) for n in abs_series_names or [] if n}  # the prefix IS the ABS series
        if (len(series.split()) >= 2 or re.fullmatch(r"0\d\.\d{1,2}", seq) or is_abs_series) and (len(rest.split()) >= 2 or (re.fullmatch(r"0\d", seq) and (len(rest) >= 5 or (is_abs_series and len(rest) >= 3)))):  # 'The Four Horsemen [Thalassa] 02 War'  # 'Prince of the Doomed City 01 Entranced'
            return {'title': rest, 'series': series, 'sequence': None if '-' in seq else str(float(seq)).rstrip('0').rstrip('.'), 'kind': 'series_prefix'}  # '01-3' = a range (omnibus): no single volume number
    if m := re.match(r'^\s*((?:19|20)\d\d)\s+-\s+(\S.{2,})$', t):  # '2019 - This Is How You Lose the Time War' (year from the file name)
        if len(m.group(2).strip()) >= 3: return {'title': m.group(2).strip(), 'kind': 'year_prefix'}  # 'Atlantic' or '1873' alone too (the search hit must still confirm)
    if m := RE_LEADING_NUM.match(t):
        rest_ = m.group(2).strip()
        if len(rest_.split()) >= 2 or (re.fullmatch(r'0[1-9]', m.group(1)) and len(rest_) >= 3):  # '03 Blutmale', '06 Rick' yes ('0N' = file-name prefix; the match still needs the full evidence), '007 Skyfall' no
            return {'title': rest_, 'sequence': str(int(m.group(1))), 'kind': 'number_prefix'}
    return None

def title_similarity(a, b):
    na, nb = normalize_title_text(a), normalize_title_text(b)
    if not na or not nb: return 0.0
    score = difflib.SequenceMatcher(None, na, nb).ratio()
    if (len(na) > 3 and na in nb) or (len(nb) > 3 and nb in na): score += 0.15
    # One title fully contained in the other (e.g. 'From Percy Jackson - Camp Half-Blood Confidential: ...'
    # vs 'Camp Half-Blood Confidential'): strong signal if the shorter one has at least 3 words.
    short, long_ = sorted((na, nb), key=len)
    if len(short.split()) >= 3 and f" {short} " in f" {long_} ": score = max(score, 0.85)
    return score

def extract_volume(text):
    raw = set(RE_VOL.findall(text)) | ({m.group(1)} if (m := re.search(r'\b(\d+)$', text.strip())) else set())
    return {strip_lead0(x) for x in raw}  # 'Vol. 02' == 'Vol. 2'

def volume_of(title, sub):
    """Volume numbers of an Audible product: title and subtitle separately (a number at the end of the SUBTITLE is not the volume)."""
    return extract_volume(title or '') | extract_volume(sub or '')

def format_time(seconds):
    if seconds < 60: 
        return f"{int(seconds)}s"
    if seconds >= 3600:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"
    return f"{int(seconds // 60)}m {int(seconds % 60)}s"

def narr_keys(names):
    """Comparable narrator keys; a joint credit counts per person too: 'Bridget & Jake Bordeaux' (Audible) = 'Bridget' + 'Jake Bordeaux'
    (book folder 'Bridget & Jake Bordeaux' split at '&', batch 93) - the full name AND its parts."""
    out = set()
    for n in names or []:
        if not n: continue
        k = re.sub(r'[^a-z]', '', fold_accents(n).lower())
        if k: out.add(k)
        parts = re.split(r'\s+(?:&|and|und)\s+', n)
        if len(parts) > 1: out |= {re.sub(r'[^a-z]', '', fold_accents(x).lower()) for x in parts if len(x.strip()) >= 3}
    return out

def fold_accents(s):
    """'Kurmaić' -> 'Kurmaic' so that spelling without diacritics still matches."""
    return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))

def match_author(abs_authors, web_author):
    if not abs_authors or not web_author: return False
    web_clean = [fold_accents(w).strip().lower() for w in web_author.split(',')]
    for abs_auth in abs_authors:
        abs_clean = fold_accents(abs_auth).lower()
        for wa in web_clean:
            if abs_clean in wa or wa in abs_clean: return True
            if len(sq := re.sub(r'[^a-z0-9]', '', abs_clean)) >= 6 and sq == re.sub(r'[^a-z0-9]', '', wa): return True  # 'Inadvisably Compelled' = 'InadvisablyCompelled'
            a_tok = set(re.split(r'[^a-z0-9]+', abs_clean))
            wa_tok = set(re.split(r'[^a-z0-9]+', wa))
            if len(a_tok.intersection(wa_tok)) >= 2: return True
            if len(a_tok.intersection(wa_tok)) == 1 and len(a_tok) == 1: return True
            a_list, w_list = [t for t in re.split(r'[^a-z0-9]+', abs_clean) if t], [t for t in re.split(r'[^a-z0-9]+', wa) if t]
            if len(a_list) >= 2 and len(w_list) >= 2 and a_list[-1] == w_list[-1] and a_list[0] != w_list[0] and (
                    (len(a_list[0]) == 1 and w_list[0].startswith(a_list[0])) or (len(w_list[0]) == 1 and a_list[0].startswith(w_list[0]))):
                return True  # 'R. Rowe' = 'Rob Rowe' (initial + same last name; other evidence still has to confirm)
    return False

def find_rating_recursive(obj):
    if isinstance(obj, dict):
        if 'rating' in obj and isinstance(obj['rating'], dict) and 'value' in obj['rating']:
             if is_valid_rating(obj['rating'].get('value')): return obj['rating']
        for k, v in obj.items():
            if res := find_rating_recursive(v): return res
    elif isinstance(obj, list):
        for item in obj:
            if res := find_rating_recursive(item): return res
    return None

def get_headers(domain=None):
    h = HEADERS_BASE.copy()
    if domain and "audible.de" in domain:
        h["Accept-Language"] = "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7"
    else:
        h["Accept-Language"] = "en-US,en;q=0.9"
    return h

def page_title(soup):
    return soup.title.get_text(strip=True)[:80] if soup and soup.title else ''

def fetch_url(url, params=None, domain=None, source=None):
    """GET a page and return (response, soup) or (None, None).
    source='goodreads' turns blocks into SourceBlockedException (skip Goodreads, keep running);
    for Audible blocks raise RateLimitException (pause / abort)."""
    def blocked(msg, hard=False):
        if source == 'goodreads': raise SourceBlockedException(msg)
        raise RateLimitException(msg, hard)

    r = None
    impersonate = source == 'goodreads' and GR_IMPERSONATE and not recorder.REPLAY_DIR  # replays never go live
    for attempt in (1, 2):  # One retry for transient network errors
        throttle('goodreads' if source == 'goodreads' else 'audible_web')
        try:
            if impersonate:
                r = gr_impersonated_get(url, params, get_headers(domain)['Accept-Language'])
            else:
                r = requests.get(url, headers=get_headers(domain), params=params, timeout=20)
            break
        except GR_CURL_ERRORS as e:
            logging.warning(f"        🌐 Request failed (try {attempt}/2, browser signature): {url} ({type(e).__name__})")
            if attempt == 1: time.sleep(3)
        except (requests.ConnectionError, requests.Timeout) as e:
            logging.warning(f"        🌐 Request failed (try {attempt}/2): {url} ({type(e).__name__})")
            if attempt == 1: time.sleep(3)
        except requests.RequestException as e:
            logging.warning(f"        🌐 Request failed: {url} ({type(e).__name__}: {e})")
            break
    if r is None: return None, None

    status, final_url = r.status_code, r.url
    if source == 'goodreads':
        if status in (429, 503) and (ra := retry_after_seconds(r)) is not None: GR_STATE['retry_after'] = ra
        if not recorder.REPLAY_DIR: gr_autothrottle(r)
    if status == 429: blocked(f"HTTP 429 on {final_url}", True)
    if status in (403, 503): blocked(f"HTTP {status} on {final_url}")
    # AWS WAF (Goodreads/Amazon) answers bot challenges with 202 + empty/JS body
    if status == 202 or r.headers.get('x-amzn-waf-action'):
        blocked(f"Bot challenge (HTTP {status}, waf={r.headers.get('x-amzn-waf-action')}) on {final_url}")
    if any(m in final_url for m in ('/user/sign_in', '/ap/signin', '/sign_in')):
        blocked(f"Redirected to login page: {final_url}")

    soup = BeautifulSoup(r.text, 'lxml')
    title = page_title(soup)
    if 'captcha' in title.lower() or 'robot check' in title.lower(): blocked(f"Captcha detected on {final_url}")
    if status != 200:
        logging.warning(f"        🌐 HTTP {status} for {final_url} (title: '{title}')")
        return None, None
    if len(r.text) < 2000:
        logging.warning(f"        🌐 Suspiciously small page ({len(r.text)} bytes) for {final_url} (title: '{title}')")
    return r, soup

def scrape_search_result_fallback(domain, asin):
    try:
        r, soup = fetch_url(f"https://{domain}/search", params={"keywords": asin, "ipRedirectOverride": "true"}, domain=domain)
        if not soup: return None
        item = soup.find('li', attrs={'data-asin': asin}) or (soup.find('div', attrs={'data-asin': asin}).find_parent('li') if soup.find('div', attrs={'data-asin': asin}) else None)
        if item:
            ratings = {}
            if rate_txt := item.find('span', class_=re.compile(r'ratingLabel|ratingText')):
                if m := re.search(r'(\d+[.,]?\d*)', rate_txt.get_text()):
                     if is_valid_rating(m.group(1).replace(',', '.')):
                           ratings['overall'] = m.group(1).replace(',', '.')
            if count_txt := item.find('span', class_=re.compile(r'ratingsLabel|ratingCount')):
                if m := re.search(r'([\d,.]+)', count_txt.get_text()): ratings['count'] = int(re.sub(r'[^\d]', '', m.group(1)))
            if not ratings.get('count') or not ratings.get('overall'):
                full_text = item.get_text()
                if not ratings.get('overall'):
                    if m := RE_TEXT_RATING.search(full_text):
                        if is_valid_rating(m.group(1).replace(',', '.')):
                            ratings['overall'] = m.group(1).replace(',', '.')
                if not ratings.get('count'):
                    if m := RE_TEXT_COUNT.search(full_text):
                        val = re.sub(r'[^\d]', '', m.group(0))
                        if val: ratings['count'] = int(val)
            if ratings.get('overall') and ratings.get('count'): return ratings
    except: pass
    return None

# ================= CORE LOGIC =================

def audible_domains(language):
    """All marketplaces for a product lookup, the one matching the book language first."""
    first = MARKET_BY_LANG.get(map_language(language) if language else None, 'com')
    order = [first] + [m for m in AUDIBLE_MARKETS if m != first]
    return [f"www.audible.{m}" for m in order if m in AUDIBLE_MARKETS or m == first]

def search_domains(language):
    """Marketplaces used for an ASIN search (fewer, searches are expensive)."""
    lang = map_language(language) if language else None
    first = MARKET_BY_LANG.get(lang, 'com')
    second = 'co.uk' if first == 'com' else 'com'
    return [f"www.audible.{m}" for m in dict.fromkeys([first, second])]

def web_domains(language):
    """The web scraper fallback only knows audible.com and audible.de."""
    return ["www.audible.de", "www.audible.com"] if map_language(language) == 'German' else ["www.audible.com", "www.audible.de"]

_AUD_ISBN = {}
def audible_isbn(asin, domain):
    """ISBN of THIS audio edition (Audible response group product_details) - own small request, only as the LAST fallback for an empty or
    wrong ISBN field (the user prefers the print ISBN; the main product request stays unchanged). Cached per run."""
    if not asin or not domain or domain not in AUDIBLE_API_HOSTS: return None
    if (domain, asin) not in _AUD_ISBN:
        throttle('audible_api', AUDIBLE_API_HOSTS[domain])
        try:
            r = requests.get(f"https://{AUDIBLE_API_HOSTS[domain]}/1.0/catalog/products/{asin}", params={"response_groups": "product_details"},
                             headers={"User-Agent": FIXED_USER_AGENT, "Accept": "application/json"}, timeout=15)
            v = ((r.json().get('product') or {}).get('isbn') if r.status_code == 200 else None)
        except (requests.RequestException, ValueError): v = None
        _AUD_ISBN[(domain, asin)] = re.sub(r'[^0-9Xx]', '', str(v)).upper() if v else None
    return _AUD_ISBN[(domain, asin)]


def audio_isbn_fallback(asin, aud, abs_dur, narrators, asin_from_search, key, current_isbn):
    """Audible audio-edition ISBN as the last fallback: same recording only (runtime +-2 min AND shared narrator or a search-confirmed
    ASIN), valid checksum, never the ASIN itself (Audible sometimes stores the ASIN as ISBN), never one another book owns."""
    if not (HARDCOVER_ENABLED and not HC_STATE.get('disabled')): return None  # Hardcover (print ISBN) not asked in this run: wait, never fall back early
    if not (asin and aud and abs_dur and aud.get('runtime_sec') and abs(abs_dur - aud['runtime_sec']) <= 120): return None
    if not (same_recording(abs_dur, narrators, aud) or asin_from_search): return None
    i = normalize_isbn(audible_isbn(aud.get('asin') or asin, aud.get('domain')))
    if not i or not isbn_checksum_ok(i) or i == str(asin).upper() or i == normalize_isbn(current_isbn) or (ISBN_OWNERS.get(i, set()) - {key}): return None
    return i


def get_audible_api(asin, domain):
    """Query the Audible catalog API for one marketplace.
    Returns a result dict in the same shape as the web scraper, 'missing' if the product
    is not available there, or None on errors (caller falls back to web scraping)."""
    host = AUDIBLE_API_HOSTS.get(domain)
    if not host: return None
    if (host, asin) in AUD_PREFETCH:  # loaded in bulk at the start of the run (aud_prefetch)
        p = AUD_PREFETCH[(host, asin)]
        return audible_result(p, asin, domain) if p.get('title') else 'missing'
    throttle('audible_api', host)
    url = f"https://{host}/1.0/catalog/products/{asin}"
    try:
        r = requests.get(url, params={"response_groups": AUDIBLE_API_GROUPS, "image_sizes": "500,1215"},
                         headers={"User-Agent": FIXED_USER_AGENT, "Accept": "application/json"}, timeout=15)
    except requests.RequestException as e:
        logging.warning(f"        🌐 Audible API request failed ({host}): {type(e).__name__}: {e}")
        return None
    if r.status_code == 429: raise RateLimitException(f"HTTP 429 on {host} (API)", True)
    if r.status_code == 404: return 'missing'
    if r.status_code != 200:
        logging.warning(f"        🌐 Audible API HTTP {r.status_code} on {host}")
        return None
    try: p = r.json().get('product') or {}
    except ValueError:
        logging.warning(f"        🌐 Audible API returned no JSON on {host}")
        return None
    if not p.get('title'): return 'missing'  # Product not sold in this marketplace
    return audible_result(p, asin, domain)


AUD_PREFETCH = {}  # (api host, asin) -> catalog product of this run (bulk request, 50 per request)


def aud_prefetch(items):
    """The product of every queued book from its preferred store in bulk (50 ASINs per request instead of one request per book -
    each single request waits for the per-host throttle). A product the bulk answer does not contain is asked singly as before."""
    by_host = {}
    for it in items:
        md = (it.get('media') or {}).get('metadata') or {}
        a = str(md.get('asin') or '').strip()
        if not re.fullmatch(r'[A-Z0-9]{10}', a): continue
        dom = f"www.audible.{MARKET_BY_LANG.get(map_language(md.get('language')) if md.get('language') else None, 'com')}"
        if dom in AUDIBLE_API_HOSTS: by_host.setdefault(AUDIBLE_API_HOSTS[dom], set()).add(a)
    n = 0
    for host, asins in by_host.items():
        asins = sorted(asins)
        for i in range(0, len(asins), 50):
            throttle('audible_api', host)
            try:
                r = requests.get(f"https://{host}/1.0/catalog/products", params={"asins": ",".join(asins[i:i + 50]), "response_groups": AUDIBLE_API_GROUPS,
                                 "image_sizes": "500,1215"}, headers={"User-Agent": FIXED_USER_AGENT, "Accept": "application/json"}, timeout=30)
                prods = r.json().get('products') if r.status_code == 200 else None
            except (requests.RequestException, ValueError): prods = None
            if prods is None: break  # throttled / blocked / not recorded: the books are asked one by one as before
            for p in prods:
                if p.get('asin') in asins[i:i + 50]: AUD_PREFETCH[(host, p['asin'])] = p; n += 1
    if n: logging.info(f"Audible: {n} product(s) loaded in bulk ({sum(len(v) for v in by_host.values())} queued books with an ASIN).")


def audible_result(p, asin, domain):
    """Catalog product -> result dict (same shape as the web scraper)."""
    res = {'domain': domain, 'source': 'API', 'page_ok': True, 'title_raw': p.get('title'), 'asin': p.get('asin') or asin,
           'authors_raw': [a.get('name') for a in (p.get('authors') or []) if a.get('name')],
           'authors_asin': [{'name': a.get('name'), 'asin': a.get('asin')} for a in (p.get('authors') or []) if a.get('name') and a.get('asin')],
           'runtime_sec': int(p.get('runtime_length_min') or 0) * 60}
    if p.get('subtitle'): res['subtitle_raw'] = p['subtitle']
    from abs_series import is_role_credit, role_base
    ROLE_NAMES.update(role_base(a) for a in res['authors_raw'] if is_role_credit(a))  # learn illustrators/translators for the clean-up
    rating = p.get('rating') or {}
    def dist(name):
        d = rating.get(name) or {}
        return d.get('display_average_rating') or d.get('average_rating'), int(d.get('num_ratings') or 0)
    overall, count = dist('overall_distribution')
    performance, _ = dist('performance_distribution')
    story, _ = dist('story_distribution')
    res['count'] = count
    if count and is_valid_rating(overall): res['overall'] = str(overall)
    if count and is_valid_rating(performance): res['performance'] = str(performance)
    if count and is_valid_rating(story): res['story'] = str(story)

    # Convert to the 'meta_raw' shape the metadata sync already understands
    categories = []
    ladders = [l.get('ladder') or [] for l in (p.get('category_ladders') or [])]
    if any(len(l) >= 2 for l in ladders): ladders = [l for l in ladders if len(l) >= 2]  # skip bare top-level categories
    for lad in ladders:
        ladder = {'ladder': lad}
        # Only the finest level of each ladder ('Space Opera' instead of 'Science Fiction & Fantasy' > 'Science Fiction' > 'Space Opera')
        leaf = ((ladder.get('ladder') or [{}])[-1] or {}).get('name')
        if leaf and leaf not in categories: categories.append(leaf)
    res['narrators_raw'] = [n.get('name') for n in (p.get('narrators') or []) if n.get('name')]
    res['summary'] = p.get('publisher_summary') or p.get('merchandising_summary')
    if res['summary'] and p.get('asin'):
        with IO_LOCK: SUMMARY_INDEX[p['asin']] = [p.get('title') or '', _sum_norm(res['summary'])[:800]]
    res['adult'] = bool(p.get('is_adult_product'))
    imgs = {k: v for k, v in (p.get('product_images') or {}).items() if str(k).isdigit() and v}
    if imgs: res['cover_url'] = imgs[max(imgs, key=int)]
    res['meta_raw'] = {
        'language': p.get('language'),
        'publisher': {'name': p.get('publisher_name')} if p.get('publisher_name') else None,
        'releaseDate': p.get('release_date') or p.get('issue_date'),
        'format': p.get('format_type'),
        'categories': [{'name': c} for c in categories],
        'series': [{'name': s.get('title'), 'part': aud_part(s.get('sequence')), 'asin': s.get('asin')} for s in (p.get('series') or []) if s.get('title')],
    }
    note_series(p.get('series'))
    return res

def get_audible_data(asin, language, skip=()):
    """Audible ratings + metadata from the catalog API. All marketplaces (minus 'skip') are asked IN PARALLEL (separate servers); the
    first hit in store order wins (the book language's store first). Only when the API could not answer (errors) the product pages are
    scraped; a product that every store answers as 'not available' is gone - no page scraping (it only ever gave a soft 404)."""
    if not asin: return None
    if not USE_AUDIBLE_API: return get_audible_data_web(asin, language)
    domains = [d for d in audible_domains(language) if d not in skip]
    with ThreadPoolExecutor(max_workers=max(1, len(domains))) as ex:
        answers = list(ex.map(lambda d: get_audible_api(asin, d), domains))
    api_error, missing = False, [d.replace('www.audible.', '.') for d in skip]
    for domain, res in zip(domains, answers):
        if res is None: api_error = True; continue
        if res == 'missing':
            missing.append(domain.replace('www.audible.', '.'))
            continue
        logging.info(f"      -> Audible API: found on {domain}" + (f" (not on {', '.join(missing)})" if missing else ""))
        if res['count'] > 0 and res.get('overall'):
            logging.info(f"        ✅ SUCCESS on {domain} (Count: {res['count']}, Rating: {round(safe_float(res['overall']), 2)})")
        else:
            logging.info(f"        ℹ️ Product exists on {domain}, but has no ratings yet.")
        return res
    if not api_error:
        logging.info(f"      -> Audible API: ASIN not available on {', '.join(missing) or '-'} (gone from Audible)")
        return None
    logging.info(f"      -> Audible API: ASIN not available on {', '.join(missing) or '-'} (API errors), falling back to web scraping...")
    return get_audible_data_web(asin, language)

def get_audible_data_web(asin, language):
    if not asin: return None
    domains = web_domains(language)

    best_result = None

    for domain in domains:
        logging.info(f"      -> Checking {domain}...")
        url = f"https://{domain}/pd/{asin}?ipRedirectOverride=true"
        cookies = {}
        if "audible.de" in domain: cookies["audible_site_preference"] = "de"
        elif "audible.com" in domain: cookies["audible_site_preference"] = "us"
        headers = get_headers(domain)

        try:
            throttle('audible_web')
            r = requests.get(url, headers=headers, cookies=cookies, timeout=15)
            txt_lower = r.text.lower()
            soup = BeautifulSoup(r.text, 'lxml')
            title_lower = (soup.title.text if soup.title else "").lower()
            
            soft_404_markers = ["looks like this title is no longer available", "titel ist leider nicht verfügbar", "no results for", "keine ergebnisse für"]
            if any(marker in txt_lower for marker in soft_404_markers) or "search" in title_lower:
                logging.info(f"        ⚠️ Soft-404 (Not Available/No Results/Search Page) on {domain}")
                if domain in ["www.audible.com", "www.audible.de"]:
                    if fb := scrape_search_result_fallback(domain, asin):
                        logging.info(f"        ✅ Found via Search Fallback (Soft-404) (Count: {fb['count']})")
                        fb['domain'] = domain
                        return fb
                continue

            if r.status_code == 404 or "/pderror" in r.url:
                logging.info(f"        ❌ 404/Error on {domain}")
                if domain in ["www.audible.com", "www.audible.de"]:
                    if fb := scrape_search_result_fallback(domain, asin):
                        logging.info(f"        ✅ Found via Search Fallback (404) (Count: {fb['count']})")
                        fb['domain'] = domain
                        return fb
                continue

            ratings = {'domain': domain}
            raw_text = r.text
            try:
                if link_us := soup.find('link', attrs={'hreflang': 'en-us'}):
                    href = link_us.get('href', '').strip()
                    if "www.audible.com/" in href and (m := re.search(r'([A-Z0-9]{10})', href)):
                        ratings['variant_asin_us'] = m.group(1)
                if link_de := soup.find('link', attrs={'hreflang': 'de-de'}):
                    href = link_de.get('href', '').strip()
                    if "www.audible.de/" in href and (m := re.search(r'([A-Z0-9]{10})', href)):
                        ratings['variant_asin_de'] = m.group(1)
            except: pass
            
            try:
                for json_script in soup.find_all('script', type='application/json'):
                    if json_script.string and '"duration"' in json_script.string:
                        try:
                            md = json.loads(json_script.string)
                            if isinstance(md, list): md = md[0]
                            if isinstance(md, dict) and 'duration' in md:
                                ratings['meta_raw'] = md
                                break
                        except: continue
            except: pass

            if h1 := soup.find('h1', slot='title'): ratings['title_raw'] = h1.get_text(strip=True)
            if h2 := soup.find('h2', slot='subtitle'): ratings['subtitle_raw'] = h2.get_text(strip=True)

            if sum_tag := soup.find('adbl-rating-summary'):
                if is_valid_rating(sum_tag.get('performance-value')): ratings['performance'] = sum_tag.get('performance-value')
                if is_valid_rating(sum_tag.get('story-value')): ratings['story'] = sum_tag.get('story-value')
                if st := sum_tag.find('adbl-star-rating'): 
                    if is_valid_rating(st.get('value')): ratings['overall'] = st.get('value')
                    ratings['count'] = st.get('count')

            if not ratings.get('count') or not ratings.get('overall'):
                for s in soup.find_all('script', type='application/ld+json'):
                    try:
                        d = json.loads(s.string)
                        for i in (d if isinstance(d, list) else [d]):
                            if 'aggregateRating' in i: 
                                val = i['aggregateRating'].get('ratingValue')
                                if is_valid_rating(val): ratings['overall'] = val
                                c_val = i['aggregateRating'].get('ratingCount') or i['aggregateRating'].get('reviewCount')
                                if c_val: ratings['count'] = int(c_val)
                    except: pass

            if not ratings.get('count') or not ratings.get('overall'):
                for s in soup.find_all('script', type='application/json'):
                    try:
                        d = json.loads(s.string)
                        if 'rating' in d and isinstance(d['rating'], dict):
                            val = d['rating'].get('value')
                            cnt = d['rating'].get('count')
                            if is_valid_rating(val): ratings['overall'] = val
                            if cnt: ratings['count'] = int(cnt)
                            if ratings.get('overall') and ratings.get('count'): break
                    except: pass

            if not ratings.get('count'):
                if m := RE_RAW_STORY.search(raw_text):
                     if is_valid_rating(m.group(1)): ratings['story'] = m.group(1)
                if m := RE_RAW_PERFORMANCE.search(raw_text):
                     if is_valid_rating(m.group(1)): ratings['performance'] = m.group(1)
                if m := RE_RAW_OVERALL.search(raw_text):
                     if is_valid_rating(m.group(1)): ratings['overall'] = m.group(1)
                if m := RE_RAW_COUNT.search(raw_text): ratings['count'] = m.group(1)

            count = int(ratings.get('count', 0))
            overall_val = safe_float(ratings.get('overall'))
            
            if count > 0 and overall_val > 0:
                ov = round(overall_val, 2)
                logging.info(f"        ✅ SUCCESS on {domain} (Count: {count}, Rating: {ov})")
                return ratings
            else:
                logging.info(f"        ⚠️ Page OK (200), but 0 Ratings/Invalid Rating found. Attempting Search Fallback...")
                if fb := scrape_search_result_fallback(domain, asin):
                     logging.info(f"        ✅ Found via Search Fallback (Page OK but empty) (Count: {fb['count']})")
                     fb['domain'] = domain
                     if ratings.get('meta_raw'): fb['meta_raw'] = ratings['meta_raw'] 
                     return fb
                # Product page exists (has a title) but nobody has rated it yet -> new release, not a broken ASIN
                page_ok = bool(ratings.get('title_raw') or ratings.get('meta_raw'))
                if best_result is None or (page_ok and not best_result.get('page_ok')):
                    best_result = {'count': 0, 'source': 'Empty', 'domain': domain, 'page_ok': page_ok}
                    for k in ('meta_raw', 'title_raw', 'subtitle_raw'):
                        if ratings.get(k): best_result[k] = ratings[k]

        except Exception as e:
            logging.error(f"        ⚠️ Request failed: {e}")
            continue

    if best_result: return best_result
    return None

# Words that make up Audible's marketing suffixes (': A LitRPG Adventure', ': A Xianxia Cultivation Novel')
MARKETING_WORDS = {'litrpg', 'gamelit', 'progression', 'cultivation', 'xianxia', 'wuxia', 'isekai', 'adventure', 'adventures',
                   'novel', 'fantasy', 'series', 'saga', 'thriller', 'romance', 'epic', 'slice', 'life', 'apocalypse',
                   'apocalyptic', 'dungeon', 'core', 'system', 'harem', 'mystery', 'cozy', 'scifi', 'sci', 'fi', 'space',
                   'opera', 'military', 'portal', 'urban', 'dark', 'progressive', 'monster', 'tamer', 'crafting', 'base',
                   'building', 'kingdom', 'reincarnation', 'magic', 'academy', 'superhero', 'post', 'survival'}

def search_match_score(abs_title, found_title, found_sub):
    """Title score for search candidates. Unlike title_similarity():
    - a subtitle never adds containment points ('The Hunger Games' vs 'Mockingjay' + 'The Hunger Games, Book 3')
    - a LONGER Audible title only counts if the ABS title has a volume number that matches
      ('A Thousand Li' vs 'A Thousand Li: The Fourth Wall' = other book; 'Beware of Chicken 4' vs
      'Beware of Chicken 4: A Xianxia Cultivation Novel' = same book)."""
    na, nf = normalize_title_text(abs_title), normalize_title_text(found_title)
    if not na or not nf: return 0.0
    score = max(difflib.SequenceMatcher(None, na, nf).ratio(),
                difflib.SequenceMatcher(None, na, normalize_title_text(f"{found_title} {found_sub or ''}")).ratio())
    if na == nf: return 1.0
    if len(nf.split()) >= 3 and f" {nf} " in f" {na} ": score = max(score, 0.85)          # Audible title inside ABS title
    if nf.startswith(na + " ") and re.search(r'\d', na):                                    # ABS title + subtitle on Audible
        score = max(score, 0.85)  # the number in the ABS title is part of the match, so it is the same volume
    if nf.startswith(na + " ") and not extract_volume(found_title):
        extra = set(nf[len(na):].split())
        if extra & MARKETING_WORDS: score = max(score, 0.85)  # 'A Farm in the Afterlife: A Slice-of-Life LitRPG Adventure'
    if len(na.split()) >= 3 and nf.endswith(" " + na): score = max(score, 0.85)  # 'Vainqueur the Dragon II: The Year of the Rogues'
    for sep in (':', ' - '):  # 'Robot Swarms: A Gripping LitRPG Adventure' vs 'Robot Swarms': the tail is marketing (needs a marketing word, so 'Dune: Messiah' never equals 'Dune')
        if sep in (abs_title or ''):
            for head, tail in {tuple(abs_title.split(sep, 1)), tuple(abs_title.rsplit(sep, 1))}:  # first AND last separator ('Fate Hollow Academy: Term 1: A Gripping LitRPG Adventure')
                if normalize_title_text(head) == nf and set(normalize_title_text(tail).split()) & MARKETING_WORDS: score = max(score, 0.9)
    if ':' in (abs_title or '') and found_sub:  # the ABS title carries Audible's subtitle (start): 'Homeland: Legend of Drizzt' = 'Homeland' + 'Legend of Drizzt: Dark Elf Trilogy, Book 1'
        head, tail = (normalize_title_text(x) for x in abs_title.split(':', 1))
        if head == nf and len(tail.split()) >= 2 and (normalize_title_text(found_sub) + ' ').startswith(tail + ' '): score = max(score, 0.9)
    head_last = (abs_title or '').rsplit(':', 1)[0] if ':' in (abs_title or '') else ''  # everything before the LAST colon ('Fate Hollow Academy: Term 1')
    if extract_volume(found_title) and not (extract_volume(abs_title) | extract_volume(re.split(r'\s*[:\u2013-]\s+', abs_title or '')[0]) | extract_volume(head_last)):  # the number may sit in the title head ('Gangster 2: A Gripping LitRPG Adventure')
        score = min(score, 0.75)  # 'Hell Mode' vs 'Hell Mode, Volume 3': could be any volume
    return score

LAST_SEARCH = _TLDict('last_search', {'related': None})  # best same-title/same-author candidate of the last search (other edition), per thread

# ---------------- Audible series listing path (finds products the catalog search does not return) ----------------
SERIES_ASIN_FILE = os.path.join(SCRIPT_DIR, 'audible_series.json')  # {series title: [series asin]}, filled from every product we load
SERIES_ASINS = {}      # raw series title -> set(series asin)
_SERIES_CHILDREN = {}  # (api host, series asin) -> [child asin]  (per store: a series ASIN unknown on .de must not hide its children on .com)
try:
    for _t, _a in (json.load(open(SERIES_ASIN_FILE, encoding='utf-8')) if os.path.exists(SERIES_ASIN_FILE) else {}).items():
        SERIES_ASINS[_t] = set(_a)
except (OSError, ValueError): pass

def note_series(series_list):
    for s in series_list or []:
        if s.get('asin') and s.get('title'): SERIES_ASINS.setdefault(s['title'], set()).add(s['asin'])

def save_series_asins():
    try:
        os.makedirs(os.path.dirname(SERIES_ASIN_FILE) or '.', exist_ok=True)
        with open(SERIES_ASIN_FILE, 'w', encoding='utf-8') as f: json.dump({t: sorted(a) for t, a in list(SERIES_ASINS.items())}, f, ensure_ascii=False)
    except OSError: pass
atexit.register(save_series_asins)

AUTHOR_LINKS_FILE = os.path.join(SCRIPT_DIR, 'audible_authors.json')  # ABS author spelling -> Audible author record IDs (proof that two spellings are one person)
AUTHOR_LINKS = {}
try:
    for _n, _a in (json.load(open(AUTHOR_LINKS_FILE, encoding='utf-8')) if os.path.exists(AUTHOR_LINKS_FILE) else {}).items(): AUTHOR_LINKS[_n] = set(_a)
except (OSError, ValueError): pass

def note_author_links(abs_authors, aud_data):
    """Only for single-author books: the ABS author and the Audible author of the SAME verified product are the same person."""
    aa = (aud_data or {}).get('authors_asin') or []
    names = [a for a in (abs_authors or []) if a]
    if len(names) == 1 and len(aa) == 1 and aa[0].get('asin'):
        from abs_series import plausible_same_person
        if plausible_same_person(names[0], aa[0].get('name') or ''):  # ABS lists the translator sometimes: that is no second spelling
            AUTHOR_LINKS.setdefault(names[0], set()).add(aa[0]['asin'])

def save_author_links():
    try:
        with open(AUTHOR_LINKS_FILE, 'w', encoding='utf-8') as f: json.dump({n: sorted(a) for n, a in list(AUTHOR_LINKS.items())}, f, ensure_ascii=False)
    except OSError: pass
atexit.register(save_author_links)

def series_candidates(abs_series, authors_list, duration, domain):
    """ASINs of books in the same Audible series as ABS's series. Audible's catalog search misses some (older) products, a series
    listing does not. Children are prefiltered with ONE bulk request: runtime within 10 min and matching author."""
    from abs_series import series_key
    keys = [series_key(x.get('name') or '') for x in (abs_series or []) if x.get('name')]
    exact, ranked = [], []
    words = lambda s: {w for w in s.split() if len(w) >= 4}
    for title, asins in list(SERIES_ASINS.items()):  # copy: another worker may add a series meanwhile
        sk = series_key(title)
        if any(k and (sk == k or sk.startswith(k + ' ')) for k in keys): exact.append((sk != (keys[0] if keys else ''), len(sk), sorted(asins))); continue
        # looser: ABS 'Dune Universe' vs Audible 'DUNE: Prelude to Dune' share a significant word (candidates are filtered hard afterwards)
        shared = max((len(words(sk) & words(k)) for k in keys if k), default=0)
        if shared: ranked.append((-shared, len(sk), sorted(asins)))
    # most relevant first (exact name, then most shared words, then shorter name): the request cap below must never drop the right
    # series just because its ASIN sorts late (the series file grows with every product loaded)
    wanted = list(dict.fromkeys(a for _, _, asins in sorted(exact) + sorted(ranked)[:10] for a in asins))
    if not wanted or not duration: return []
    host, hdr = AUDIBLE_API_HOSTS[domain], {"User-Agent": FIXED_USER_AGENT, "Accept": "application/json"}
    children = []
    for sa in wanted[:24]:  # one request per series ASIN, cached for the whole run
        if (host, sa) not in _SERIES_CHILDREN:
            throttle('audible_api', host)
            try:
                r = requests.get(f"https://{host}/1.0/catalog/products/{sa}", params={"response_groups": "relationships"}, headers=hdr, timeout=15)
                rel = ((r.json().get('product') or {}).get('relationships') or []) if r.status_code == 200 else []
            except (requests.RequestException, ValueError): rel = []
            _SERIES_CHILDREN[(host, sa)] = [x['asin'] for x in rel if x.get('relationship_to_product') == 'child' and x.get('asin')]
        children += [c for c in _SERIES_CHILDREN[(host, sa)] if c not in children]
    out = []
    for i in range(0, len(children), 50):
        throttle('audible_api', host)
        try:
            r = requests.get(f"https://{host}/1.0/catalog/products", params={"asins": ",".join(children[i:i + 50]), "response_groups": "product_attrs,contributors,media"},
                             headers=hdr, timeout=20)
            prods = (r.json().get('products') or []) if r.status_code == 200 else []
        except (requests.RequestException, ValueError): prods = []
        for p in prods:
            rt = int(p.get('runtime_length_min') or 0) * 60
            if rt and abs(rt - duration) <= max(600, 0.25 * duration) and match_author(authors_list, ', '.join(a.get('name') or '' for a in (p.get('authors') or []))):
                out.append((abs(rt - duration), p["asin"]))
    return [a for _, a in sorted(out)]  # closest runtime first


def same_person_name(a, b):
    """Same name regardless of word order and accents ('Isshiki Ichika' = 'Ichika Isshiki': Japanese name order)."""
    tok = lambda s: sorted(re.sub(r"[^a-z ]", " ", fold_accents(s or "").lower()).split())
    return bool(tok(a)) and tok(a) == tok(b)

LAST_MATCH = _TLDict('last_match')  # evidence of the last accepted search hit: {asin, confirmed, evidence[]} (per thread)
SEARCH_CTX = _TLDict('search_ctx')  # {'old_title','old_authors',...}: what the current ASIN in ABS points to (set by the caller, per thread)


def preferred_twin(lang, key):
    """After an accepted search hit: the SAME recording as another listing that fits better - in the preferred store of the
    PRODUCT language (English -> .com, German -> .de) when the hit came from another store, or with ABS's publisher+year when
    the hit lacks them. The ratings are the point of the tool: a twin is only taken with AT LEAST as many ratings as the hit
    (a UK publisher listing with 94 ratings never replaces the US one with 8,275).
    Twin = same narrators, matching authors, runtime within 2 min of the hit, title score >= 0.85 vs the hit, not owned by
    another book. One targeted query (title + author + narrator of the hit). Returns (asin, why) or None."""
    m = LAST_MATCH
    if not (m.get('asin') and m.get('confirmed') and m.get('narrators') and m.get('authors') and m.get('runtime')): return None
    plang = map_language(m.get('lang')) if m.get('lang') else (map_language(lang) if lang else None)
    pref = PREFERRED_STORE.get(plang) if plang else None
    other_store = bool(pref and m.get('domain') != pref)
    want_pub = bool(not m.get('pub_year') and SEARCH_CTX.get('pub') and SEARCH_CTX.get('year'))
    if not (other_store or want_pub): return None
    domain = pref if other_store else m.get('domain')
    norm = lambda xs: {re.sub(r'[^a-z]', '', fold_accents(x).lower()) for x in xs if x}
    throttle('audible_api', AUDIBLE_API_HOSTS[domain])
    try:
        r = requests.get(f"https://{AUDIBLE_API_HOSTS[domain]}/1.0/catalog/products",
                         params={"num_results": 20, "title": m['title'], "author": m['authors'][0], "narrator": m['narrators'][0], "response_groups": "product_attrs"},
                         headers={"User-Agent": FIXED_USER_AGENT, "Accept": "application/json"}, timeout=15)
        prods = (r.json().get('products') or []) if r.status_code == 200 else []
    except (requests.RequestException, ValueError): return None
    best = None
    for prod in prods:
        c = prod.get('asin')
        if not c or c == m['asin'] or (ASIN_OWNERS.get(c, set()) - {key}): continue
        p = get_audible_api(c, domain)
        if not isinstance(p, dict) or not p.get('runtime_sec') or abs(p['runtime_sec'] - m['runtime']) > 120: continue
        if norm(p.get('narrators_raw') or []) != norm(m['narrators']) or not match_author(m['authors'], ", ".join(p.get('authors_raw') or [])): continue
        if search_match_score(m['title'], p.get('title_raw') or '', p.get('subtitle_raw')) < 0.85 and search_match_score(p.get('title_raw') or '', m['title'], None) < 0.85: continue
        tv, want = volume_of(p.get('title_raw'), p.get('subtitle_raw')), (extract_volume(m['title']) or SEARCH_CTX.get('want_vol') or set())
        if tv and want and not tv & want: continue  # 'Mushoku Tensei: ..., Vol. 21' is no twin of the bare-titled vol. 22 (same narrator, similar runtime)
        pub = ((p.get('meta_raw') or {}).get('publisher') or {}).get('name') or ''
        yr = str(parse_release_year((p.get('meta_raw') or {}).get('releaseDate')) or '')
        pub_ok = bool(SEARCH_CTX.get('pub') and re.sub(r'[^a-z0-9]', '', pub.lower()) == re.sub(r'[^a-z0-9]', '', str(SEARCH_CTX['pub']).lower()) and yr == str(SEARCH_CTX.get('year')))
        if want_pub and not pub_ok and not other_store: continue  # same store: only a publisher+year match is a better listing
        cnt = int(p.get('count') or 0)
        if cnt < m.get('count', 0): continue  # fewer ratings than the hit: never a better listing
        if best is None or (cnt, pub_ok) > (best[3], best[2]): best = (c, p.get('title_raw'), pub_ok, cnt)
    if not best: return None
    return best[0], f"same recording as {m['asin']} ({m.get('domain')}): {'preferred store ' + domain if other_store else ''}{' + ' if other_store and best[2] else ''}{'ABS publisher+year' if best[2] else ''}"
REVIEW_FILE = os.path.join(SCRIPT_DIR, 'review_queue.json')

def add_review(key, title, kind, candidate, reason, evidence=None, ctx=None):
    """Uncertain automatic decision: nothing is written, the user decides in the review list (review_queue.json)."""
    with IO_LOCK: return _add_review(key, title, kind, candidate, reason, evidence, ctx)

def _add_review(key, title, kind, candidate, reason, evidence=None, ctx=None):
    decided = rw_json(os.path.join(SCRIPT_DIR, 'review_decisions.json')) or {}
    if f"{kind}|{key}|{candidate}" in decided: return  # user already decided (accepted or rejected)
    q = rw_json(REVIEW_FILE) or []
    if not isinstance(q, list): q = []
    for x in q:
        if x.get('key') == key and x.get('kind') == kind and x.get('candidate') == candidate:
            if x.get('status', 'open') == 'open':
                x['seen'] = RUN_ID  # still true in this run (review_sweep keeps it)
                if ctx: x['ctx'], x['run'] = ctx, RUN_ID  # refresh the context for group_confirm()
                rw_json(REVIEW_FILE, q)
            return
    q.append({'key': key, 'title': title, 'kind': kind, 'candidate': candidate, 'reason': reason, 'evidence': evidence or [],
              'run': RUN_ID, 'created_run': RUN_ID, 'ts': datetime.now(timezone.utc).isoformat(timespec='seconds'), 'status': 'open'})
    if ctx: q[-1]['ctx'] = ctx
    rw_json(REVIEW_FILE, q)

def review_sweep(key, kinds):
    """Open review entries of this book that this run did NOT raise again are obsolete (the book was fixed, locked or changed since - the
    list kept 66 entries of which most were long done, 2026-11-08). Only for the kinds this run really checked; not in a test run."""
    if DRY_RUN: return
    with IO_LOCK:
        q = rw_json(REVIEW_FILE) or []
        n = 0
        for x in q if isinstance(q, list) else []:
            if x.get('key') == key and x.get('kind') in kinds and x.get('status', 'open') == 'open' and RUN_ID not in (x.get('seen'), x.get('run')):
                x['status'], x['obsolete_run'] = 'obsolete', RUN_ID; n += 1
        if n:
            rw_json(REVIEW_FILE, q)
            logging.info(f"      -> 🧹 {n} review entr{'y' if n == 1 else 'ies'} of this book no longer apply (removed from the review list)")

def group_confirm():
    """Several ABS books with the SAME title (>= 3) whose candidates each match runtime (to the minute) and narrator and are DIFFERENT volumes
    of ONE Audible series (distinct numbers; where ABS names the same series its number agrees) confirm each other: the shared ABS title is a
    series/collection name, not a book title ('Realms of War' = War of the Spider Queen 1-6). The books get the candidate ASIN and the Audible title.
    Respects lock_asin / lock_title; every change is journaled."""
    q = rw_json(REVIEW_FILE) or []
    if not isinstance(q, list): return
    groups = {}
    for e in q:
        c = e.get('ctx') or {}
        if e.get('run') == RUN_ID and e.get('status') == 'open' and e.get('kind') in ('asin', 'title') and c.get('cand_asin'):
            groups.setdefault(normalize_title_text(e.get('title') or ''), {}).setdefault(e['key'], []).append(e)
    changed = False
    for gtitle, members in groups.items():
        if not gtitle or len(members) < 3: continue
        asins = {k: es[0]['ctx']['cand_asin'] for k, es in members.items()}
        if len(set(asins.values())) != len(asins): continue  # two books, one candidate: not a group of different volumes
        if any(ASIN_OWNERS.get(a, set()) - {k} for k, a in asins.items()): continue
        data = {k: get_audible_data(a, members[k][0]['ctx'].get('lang')) for k, a in asins.items()}
        if any(not isinstance(d, dict) or not d.get('title_raw') for d in data.values()): continue
        pairs = {k: dict(_series_pairs((d.get('meta_raw') or {}).get('series'))) for k, d in data.items()}
        common = set.intersection(*(set(p) for p in pairs.values())) if pairs else set()
        good = None
        for sk in sorted(common):
            nums = [pairs[k][sk] for k in members]
            if any(n is None for n in nums) or len(set(nums)) != len(nums): continue
            if all(not (an is not None and an != pairs[k][sk]) for k, es in members.items() for ak, an in es[0]['ctx'].get('abs_series', []) if ak == sk):
                good = sk; break
        if not good: continue
        logging.info(f"  🧩 Group confirmation: {len(members)} books titled '{gtitle}' are volumes {sorted(pairs[k][good] for k in members)} of ONE Audible series ('{good}'), each with runtime to the minute + narrator.")
        for k, es in members.items():
            c, d, asin_ = es[0]['ctx'], data[k], asins[k]
            tags_ = set(c.get('tags') or [])
            updates, old = {}, {}
            if any(e['kind'] == 'asin' for e in es) and 'lock_asin' not in tags_: updates['asin'], old['asin'] = asin_, None
            if 'lock_title' not in tags_ and d.get('title_raw') and d['title_raw'] != es[0]['title']: updates['title'], old['title'] = d['title_raw'], es[0]['title']
            if updates and abs_update(c['iid'], k, es[0]['title'], updates, old, 'group_confirm'):
                if 'asin' in updates: ASIN_OWNERS.setdefault(asin_, set()).add(k)
                logging.info(f"     💾 {es[0]['title']} -> {', '.join(f'{f}: {v}' for f, v in updates.items())}{' (DRY_RUN)' if DRY_RUN else ''}")
            for e in es: e['status'], e['resolved_by'] = 'auto_accepted', 'group_confirm'
            changed = True
    if changed: rw_json(REVIEW_FILE, q)

def roman(n):
    out = ""
    for v, s in ((1000, "m"), (900, "cm"), (500, "d"), (400, "cd"), (100, "c"), (90, "xc"), (50, "l"), (40, "xl"), (10, "x"), (9, "ix"), (5, "v"), (4, "iv"), (1, "i")):
        while n >= v: out += s; n -= v
    return out

def series_prefixed_title(abs_title, found_title, abs_series):
    """'Grand Design' (series '12 Miles Below' #3) <-> '12 Miles Below III: Grand Design': Audible title = ABS series name + volume number
    (arabic, roman or 'Book N') + ABS title."""
    na, nf = normalize_title_text(abs_title), normalize_title_text(found_title)
    if not na or not nf.endswith(" " + na): return False
    prefix = nf[:-len(na)].strip()
    for s in abs_series or []:
        name = normalize_title_text(re.sub(r"(?i)\s+series$", "", s.get("name") or ""))
        try: num = float(s.get("sequence")); nums = {str(int(num))} | ({roman(int(num))} if num.is_integer() and 0 < num < 4000 else set())
        except (TypeError, ValueError): nums = set()
        if name and nums and prefix in {f"{name} {n}" for n in nums}: return True
    return False

def search_audible_api(title, authors_list, duration, lang, raw_title=None, required_lang=None, strict=False,
                       only_domain=None, narrators=None, abs_series=None, extra_candidates=None, only_extra=False, only_queries=None):
    """Find an ASIN via the Audible catalog search API. Every candidate is verified by loading
    the product (title, author, runtime). Returns the ASIN, None (no match) or False (API error)."""
    narrators = [n for n in (narrators or []) if not any(same_person_name(n, a) for a in (authors_list or []))]  # an ABS "narrator" equal to the author is a data error, not evidence
    prim_auth = authors_list[0] if authors_list else ""
    short = re.split(r'\s*[:(]\s*|\s+-\s+', title)[0].strip()  # 'Dragon's Kin: Dragonriders of Pern, Book 16' -> 'Dragon's Kin'
    queries = [{"keywords": f"{title} {prim_auth}".strip()}]
    title_first = bool(extract_volume(title))
    if title_first: queries.insert(0, {"title": title})  # numbered title: the exact-title query first (keyword queries return the OTHER volumes and the loop stops after 4 candidates)
    if prim_auth: queries.append({"title": title, "author": prim_auth})
    if short and short != title and (len(short) >= 4 or (prim_auth and len(short) >= 2)):  # 'WAR' + author is precise enough (batch 80)
        queries.append({"title": short, "author": prim_auth} if prim_auth else {"keywords": short})
    if raw_title and raw_title != title: queries.append({"keywords": raw_title})
    queries.append({"keywords": title})
    if narrators and narrators[0]: queries.append({"keywords": f"{title} {prim_auth} {narrators[0]}".strip()})  # generic titles ('The Prince'): the narrator surfaces the right recording
    nq = {"author": prim_auth, "narrator": narrators[0], "num_results": 20} if (narrators and narrators[0] and prim_auth) else None  # last resort only (see end of function)
    nq2 = {"keywords": title, "narrator": narrators[0], "num_results": 20} if (narrators and narrators[0]) else None
    if only_queries: queries, title_first = list(only_queries), False
    LAST_SEARCH['related'] = None
    if not only_queries: LAST_SEARCH['weak'] = None
    api_ok = False
    near_pick = None  # accepted only via 'same narrator, runtime inside tolerance'; used if no candidate with exact runtime follows
    best_pick = None  # accepted with an INEXACT title: keep looking, a later candidate may match the title exactly ('The Wild Robot' vs '...: Booktrack Edition')
    pending_pick = None  # exact title accepted WITHOUT publisher+year although ABS knows both: keep looking for a re-release that matches (2020 vs 2023 listing of the same recording)
    if only_extra: queries = []  # series-listing path: no search queries, only the given candidates
    domains = [only_domain] if only_domain else search_domains(required_lang or lang)
    if only_extra: domains = domains[:1]
    for domain in domains:
        host, candidates = AUDIBLE_API_HOSTS[domain], list(extra_candidates or [])
        for qi, q in enumerate(queries):
            throttle('audible_api', host)
            try:
                r = requests.get(f"https://{host}/1.0/catalog/products", params={"num_results": 5, **q, "response_groups": "product_attrs"},
                                 headers={"User-Agent": FIXED_USER_AGENT, "Accept": "application/json"}, timeout=15)
                if r.status_code == 429: raise RateLimitException(f"HTTP 429 on {host} (API search)", True)
                if r.status_code != 200: LOOKUP_HITS['audible'] = None; continue
                api_ok = True
                if LOOKUP_HITS.get('audible') is not None: LOOKUP_HITS['audible'] += len(r.json().get('products') or [])
                logging.debug(f"        search {host} {q}: {len(r.json().get('products') or [])} results")
                for prod in r.json().get('products') or []:
                    if only_queries and duration and prod.get("runtime_length_min") and abs(prod["runtime_length_min"] * 60 - duration) > max(600, 0.1 * duration): continue  # last resort: only candidates with a plausible runtime are fetched
                    if prod.get('asin') and prod['asin'] not in candidates: candidates.append(prod['asin'])
            except (requests.RequestException, ValueError) as e:
                LOOKUP_HITS['audible'] = None  # no answer: unknown, never 'not on Audible'
                logging.warning(f"        🌐 Audible API search failed ({host}): {type(e).__name__}")
            if len(candidates) >= 4 and not (title_first and qi == 0): break  # the prepended exact-title query never ends the loop alone (it returns other volumes for 'Renegades of Pern: ..., Book 10')
        if not candidates and not only_extra: logging.info(f"        ↪️ No search results on {domain} ({len(queries)} queries)")
        # series-listing candidates are all checked (closest runtime first); the hits of the search queries are appended AFTER them and get
        # their own room (otherwise a long series listing cuts them off: the .de hit of a German volume was never looked at)
        for cand in candidates[:len(extra_candidates or []) + max(6, 12 if only_queries else 0)]:
            p = get_audible_api(cand, domain)
            owners_ = (ASIN_OWNERS.get(cand, set()) - {SEARCH_CTX['key']}) if SEARCH_CTX.get('key') else set()
            if owners_ and isinstance(p, dict) and p.get('runtime_sec') and not any(KEY_DUR.get(o) and abs(KEY_DUR[o] - p['runtime_sec']) <= 120 for o in owners_):
                # another library book owns it but does NOT match its runtime (that owner's ASIN is probably wrong): keep searching.
                # An owner WITH the same runtime = duplicate recording in the library: the candidate goes on and the caller refuses it (review list).
                logging.info(f"        ↪️ Skipped [{cand}] (ASIN belongs to another library book whose runtime differs)"); continue
            if not isinstance(p, dict): continue
            cand_lang = map_language((p.get('meta_raw') or {}).get('language'))
            if required_lang and cand_lang and cand_lang != required_lang:
                logging.info(f"        ↪️ Rejected '{p.get('title_raw')}' [{cand}] (language {cand_lang}, need {required_lang})")
                continue
            if cand_lang and not book_lang_allowed(cand_lang):
                logging.info(f"        ↪️ Rejected '{p.get('title_raw')}' [{cand}] (language {cand_lang}: not one of your book languages)")
                continue
            found_title = p.get('title_raw') or ''
            t_score = search_match_score(title, found_title, p.get('subtitle_raw'))
            if t_score < 0.85 and abs_series:  # 'Bloodline' vs 'Bloodline: The Root of All Evil': the extension is exactly ABS's own series name
                na, nf = normalize_title_text(title), normalize_title_text(found_title)
                rest = nf[len(na):].strip() if nf.startswith(na + ' ') else ''
                if rest and rest in {normalize_title_text(re.sub(r'(?i)\s+series$', '', s.get('name') or '')) for s in abs_series}: t_score = 0.85
            if t_score < 0.85 and abs_series and series_prefixed_title(title, found_title, abs_series): t_score = 0.9
            if t_score < 0.85 and ':' in (title or ''):  # 'Dragon Harper: Dragonriders of Pern, Book 19' vs 'Dragon Harper'
                head_, tail_ = title.split(':', 1)
                mv = RE_VOL.search(tail_)
                if mv and normalize_title_text(head_) == normalize_title_text(found_title) and any(
                        cn is not None and cn == float(mv.group(1)) for _, cn in _series_pairs((p.get('meta_raw') or {}).get('series'))):
                    t_score = 0.85  # tail = series info + a volume number that equals the candidate's series number
            if t_score < 0.85 and (mm := re.match(r'^(.*?)\s*#?\s*(\d{1,2})\s*$', title or '')):  # arabic number in ABS = roman numeral on Audible
                stem, num_, nf_all = normalize_title_text(mm.group(1)), int(mm.group(2)), normalize_title_text(found_title)
                nxt = nf_all[len(stem) + 1:].split()[:1] if stem and nf_all.startswith(stem + ' ') else []
                if nxt and nxt[0] in (roman(num_), str(num_)): t_score = 0.85
            if t_score < 0.85 and abs_series:
                na_, nf_ = normalize_title_text(title), normalize_title_text(found_title)
                if na_ and nf_.startswith(na_ + ' ') and series_number_hit(abs_series, (p.get('meta_raw') or {}).get('series')): t_score = 0.85  # ABS title = start of the Audible title + same series number
            f_nums, t_nums = volume_of(found_title, p.get('subtitle_raw')), extract_volume(title)
            seq_nums = {str(int(float(s.get('sequence')))) for s in (abs_series or []) if re.fullmatch(r'\d+(\.0)?', str(s.get('sequence') or ''))}
            self_inconsistent = bool(seq_nums & f_nums and duration and p.get('runtime_sec') and abs(duration - p['runtime_sec']) <= 120
                                     and {re.sub(r'[^a-z]', '', fold_accents(n).lower()) for n in (narrators or []) if n} & {re.sub(r'[^a-z]', '', fold_accents(n).lower()) for n in (p.get('narrators_raw') or []) if n})
            if only_extra and abs_series and t_score < 0.99:
                # series-listing candidates also come from LOOSELY named series ('Terrifying Towns: Ghoulsboro' #3 for ABS 'Terrifying Towns:
                # Haunted Hollow' #1, same runtime by chance - batch 79): without an exact title the candidate must be IN the ABS series
                from abs_series import series_key as _skl
                abs_k = {_skl(x.get('name') or '') for x in abs_series if x.get('name')}
                nk_ = lambda xs: {re.sub(r'[^a-z]', '', fold_accents(n).lower()) for n in (xs or []) if n}
                same_rec_ = bool(duration and p.get('runtime_sec') and abs(duration - p['runtime_sec']) <= 120 and nk_(narrators) & nk_(p.get('narrators_raw')))
                if not same_rec_ and not abs_k & {_skl(x.get('name') or '') for x in ((p.get('meta_raw') or {}).get('series') or [])}:
                    # (the same recording - runtime 2 min AND a shared narrator - may sit in an Audible series named otherwise: House Harkonnen)
                    logging.info(f"        ↪️ Rejected '{found_title}' [{cand}] (series listing: not in the book's series)"); continue
            # 'Part 2', 'Teil 2' and '(1 of 2)' (dramatized adaptations in two parts, batch 106): the brackets are dropped by the title normalisation
            part_ = lambda t: set(re.findall(r'(?i)\b(?:part|pt\.?|teil)\s*(\d+)\b', t or '')) | set(re.findall(r'(?i)\((\d+)\s+(?:of|von)\s+\d+\)', t or ''))
            if part_(title) and part_(found_title) and not part_(title) & part_(found_title):
                # 'The Desert Prince (Part 1 of 3)' vs '(Part 2 of 3)': the title normalisation drops the brackets, the part number decides
                logging.info(f"        ↪️ Rejected '{found_title}' [{cand}] (part {','.join(sorted(part_(found_title)))} vs {','.join(sorted(part_(title)))})")
                continue
            if f_nums and t_nums and not f_nums & t_nums and not self_inconsistent:  # different volume: never the same book (unless ABS contradicts itself and runtime + narrator agree)
                logging.info(f"        ↪️ Rejected '{found_title}' [{cand}] (volume {','.join(sorted(f_nums))} vs {','.join(sorted(t_nums))})")
                continue
            dup_resolved = False
            if series_seq_conflict(abs_series, (p.get('meta_raw') or {}).get('series')) \
                    and not part_scheme_number(title, found_title, (p.get('meta_raw') or {}).get('series')):  # ABS and Audible number the same series differently
                # Exception: two ABS books claim the same ASIN (one ABS number must be wrong) and this sibling product matches the file
                # length to the minute -> the runtime decides, not the (unreliable) ABS series number.
                exact_rt = bool(duration and p.get('runtime_sec') and abs(duration - p['runtime_sec']) <= 120)
                # NOTE 2026-11-08: 'same title + runtime ±2 min + narrator' is NOT enough to ignore the ABS number - The Hunter's Code
                # 1 and 2 share title and narrator and differ by 2 minutes (regress reg_syn_21). Do not add that exception again.
                same_rec = exact_rt and normalize_title_text(title) == normalize_title_text(found_title) and bool(
                    {re.sub(r'[^a-z]', '', fold_accents(n).lower()) for n in (narrators or []) if n} & {re.sub(r'[^a-z]', '', fold_accents(n).lower()) for n in (p.get('narrators_raw') or []) if n})
                proven = abs_number_disproved(abs_series, cand, found_title, (p.get('meta_raw') or {}).get('series'), host) if same_rec else None
                if proven:
                    fx_ = SEARCH_CTX.get('seq_fix') or {}; fx_[cand] = proven; SEARCH_CTX['seq_fix'] = fx_  # used only if THIS candidate becomes the ASIN
                    logging.info(f"        🔢 '{found_title}' [{cand}]: ABS series number is provably wrong (Audible's series list has this book at "
                                 f"{', '.join(f'#{v}' for v in proven.values())}, the ABS number belongs to another title) - number corrected later")
                if not ((SEARCH_CTX.get('dup_claim') and exact_rt) or proven):
                    logging.info(f"        ↪️ Rejected '{found_title}' [{cand}] (series number differs from ABS)")
                    continue
                dup_resolved = True
            auth_ok = match_author(authors_list, ", ".join(p.get('authors_raw') or []))
            dur_ok = bool(duration and p.get('runtime_sec') and abs(duration - p['runtime_sec']) < 600)
            dur_conflict = bool(duration and p.get('runtime_sec') and not dur_ok)
            norm_n = lambda n: re.sub(r'[^a-z]', '', fold_accents(n).lower())
            a_narr, c_narr = narr_keys(narrators), narr_keys(p.get('narrators_raw'))
            # Same book, possibly another recording (runtime/narrator differ): remembered as a ratings-only fallback
            rel_dur_ok = not (duration and p.get('runtime_sec')) or abs(duration - p['runtime_sec']) <= max(2700, 0.25 * duration)
            if t_score >= 0.85 and auth_ok and int(p.get('count') or 0) > 0 and rel_dur_ok and is_boxset_title(found_title) == is_boxset_title(title) \
                    and not (a_narr and c_narr and not a_narr & c_narr) \
                    and (not series_seq_unconfirmed(abs_series, (p.get("meta_raw") or {}).get("series"))
                         or (t_score >= 0.99 and any(an is not None and cn is not None and an == cn and an >= 2  # exact title + same number, series named differently
                                                     for _, an in _series_pairs(abs_series) for _, cn in _series_pairs((p.get("meta_raw") or {}).get("series"))))):
                rel, diff = LAST_SEARCH['related'], abs((duration or 0) - (p.get('runtime_sec') or 0))
                if rel is None or diff < rel['_diff']:
                    LAST_SEARCH['related'] = dict(p, _diff=diff)
            norm_n = lambda n: re.sub(r'[^a-z]', '', fold_accents(n).lower())
            a_narr, c_narr = narr_keys(narrators), narr_keys(p.get('narrators_raw'))
            # Author written in another script (ABS 'Rifujin na Magonote' vs Audible '理不尽な孫の手'): no contradiction if narrator and runtime (+-2 min) agree
            if not auth_ok and a_narr and c_narr and a_narr & c_narr and duration and p.get('runtime_sec') and abs(duration - p['runtime_sec']) <= 120:
                cand_auth = ", ".join(p.get('authors_raw') or [])
                if re.search(r'[^\x00-ɏ]', cand_auth) and not any(re.search(r'[^\x00-ɏ]', a) for a in authors_list):
                    auth_ok = True
                    logging.info(f"        ℹ️ Author '{cand_auth}' is written in another script; narrator and runtime agree, so it is not treated as a mismatch")
            # author and narrator swapped in ABS (file tags: Overlord 13 had the narrator as author and the author as narrator, round 5):
            # the ABS narrator is the candidate's author AND the ABS author its narrator -> one recording, roles crossed
            swapped_ = bool(a_narr and c_narr and narr_keys(p.get('authors_raw')) & a_narr and narr_keys(authors_list) & c_narr)
            if swapped_:
                auth_ok = True
                logging.info(f"        ℹ️ '{found_title}' [{cand}]: author and narrator are swapped in ABS - the roles match crosswise")
            if a_narr and c_narr and not a_narr & c_narr and not swapped_:  # other recording (e.g. UK vs US narrator): known and different narrators veto the hit, with or without an existing ASIN
                logging.info(f"        ↪️ Rejected '{found_title}' [{cand}] (narrator {', '.join(p.get('narrators_raw') or [])} differs)")
                continue
            # same narrator + runtime inside the library tolerance (15 % / 20 min): same recording, ABS file just differs a little
            dur_near_n = bool(duration and p.get("runtime_sec") and not runtime_mismatch(duration, p["runtime_sec"]) and a_narr and c_narr and a_narr & c_narr)
            na_w, nf_w = set(normalize_title_text(title).split()), set(normalize_title_text(found_title).split())
            exact_rt = bool(duration and p.get('runtime_sec') and abs(duration - p['runtime_sec']) <= 120)
            if duration and p.get('runtime_sec') and not exact_rt and (SEARCH_CTX.get('need_exact_rt') or title_shared(SEARCH_CTX.get('key'), title, p['runtime_sec'])):
                logging.info(f"        ↪️ Rejected '{found_title}' [{cand}] (the ABS title is shared by several library books: runtime must match to ±2 min, "
                             f"differs by {abs(duration - p['runtime_sec']) / 60:.0f} min)")
                continue
            ser_same = any(an is not None and cn is not None and an == cn and (ak == ck or difflib.SequenceMatcher(None, ak, ck).ratio() >= 0.9)
                           for ak, an in _series_pairs(abs_series) for ck, cn in _series_pairs((p.get('meta_raw') or {}).get('series')))
            if t_score < 0.85 and auth_ok and exact_rt and a_narr and c_narr and a_narr & c_narr and ser_same and not (LAST_SEARCH.get('weak') or {}).get('series_ok'):
                # 'Oathbound Healer' = 'Beneath the Dragoneye Moons' #1: author, narrator, runtime to 2 min, series AND number agree - only the title is
                # another one. LAST resort (the caller takes it only if the normal search found nothing: an exact-title listing of the same recording wins)
                LAST_SEARCH['weak'] = {'asin': cand, 'title': found_title, 'title_score': round(t_score, 2), 'author_ok': True, 'series_ok': True, 'domain': domain,
                                       'runtime_sec': p['runtime_sec'], 'narrators': p.get('narrators_raw') or [], 'authors': p.get('authors_raw') or [],
                                       'lang': (p.get('meta_raw') or {}).get('language'),
                                       'cand': {'title': found_title, 'authors': p.get('authors_raw') or [], 'narrators': p.get('narrators_raw') or [], 'runtime_min': round(p['runtime_sec'] / 60),
                                                'series': [f"{n} #{q}" if q else n for n, q in ((x.get('name') or x.get('title'), x.get('part') or x.get('sequence')) for x in ((p.get('meta_raw') or {}).get('series') or []))], 'store': domain}}
                logging.info(f"        ↪️ '{found_title}' [{cand}]: title differs, but author, narrator, runtime (±2 min) and series number agree - kept as last resort")
                continue
            if (t_score < 0.85 and auth_ok and exact_rt and a_narr and c_narr and a_narr & c_narr and na_w and na_w <= nf_w):  # 4 independent signals
                t_score = 0.85  # 'Shannara' vs 'The Sword of Shannara': title words contained, but runtime to the minute + narrator + author + series number agree
            if strict:  # replacing an existing ASIN: everything must match
                accept = t_score >= 0.85 and auth_ok and (dur_ok or dur_near_n)
            else:
                accept = t_score >= 0.8 and auth_ok and not dur_conflict or t_score >= 0.85 and dur_ok
            if accept:
                narr_hit = bool(a_narr and c_narr and a_narr & c_narr)
                vol_hit = bool(f_nums and t_nums and f_nums & t_nums)
                ser_hit = any(an is not None and cn is not None and an == cn and (ak == ck or difflib.SequenceMatcher(None, ak, ck).ratio() >= 0.9)
                              for ak, an in _series_pairs(abs_series) for ck, cn in _series_pairs((p.get('meta_raw') or {}).get('series')))
                exact_long = len(normalize_title_text(title).split()) >= 4  # long, distinctive title (a short generic one such as 'Alice im Wunderland' is not enough)
                evidence = [n for n, v in (('title', t_score >= 0.85), ('author', auth_ok), ('runtime', dur_ok), ('narrator', narr_hit),
                                           ('volume-number', vol_hit), ('series-number', ser_hit), ('distinctive-title', exact_long)) if v]
                old_wrong = bool(SEARCH_CTX.get('old_title') and search_match_score(title, SEARCH_CTX['old_title'], '') < 0.5
                                 and not match_author(authors_list, SEARCH_CTX.get('old_authors') or ''))  # current ASIN is a different book
                if old_wrong: evidence.append('old-asin-is-another-book')
                if dup_resolved: evidence.append('duplicate-asin-resolved-by-exact-runtime')
                cand_pub = ((p.get('meta_raw') or {}).get('publisher') or {}).get('name')
                cand_year = str(parse_release_year((p.get('meta_raw') or {}).get('releaseDate')) or '')
                pub_year = bool(cand_pub and SEARCH_CTX.get('pub') and cand_year and SEARCH_CTX.get('year')
                                and re.sub(r'[^a-z0-9]', '', cand_pub.lower()) == re.sub(r'[^a-z0-9]', '', SEARCH_CTX['pub'].lower()) and cand_year == str(SEARCH_CTX['year']))
                if pub_year: evidence.append('publisher+year')
                extra = sum([narr_hit, vol_hit, ser_hit, exact_long, old_wrong, dup_resolved, pub_year])
                ser_loose = (not ser_hit) and any(an is not None and cn is not None and an == cn and an >= 2  # same number, series named differently
                                                  for _, an in _series_pairs(abs_series) for _, cn in _series_pairs((p.get('meta_raw') or {}).get('series')))
                if ser_loose and not dur_ok: evidence.append('series-number(other name)')
                confirmed = auth_ok and t_score >= 0.85 and ((dur_ok and extra >= 1) or (dur_near_n and not dur_ok and extra + ser_loose >= 2) or extra >= 2)
                ev_txt = ", ".join(evidence)
                LAST_MATCH.clear(); LAST_MATCH.update({'asin': cand, 'confirmed': confirmed, 'evidence': evidence, 'title': found_title, 'domain': domain,
                                   'runtime': p.get('runtime_sec'), 'narrators': p.get('narrators_raw') or [], 'authors': p.get('authors_raw') or [], 'pub_year': pub_year,
                                   'count': int(p.get('count') or 0), 'lang': (p.get('meta_raw') or {}).get('language')})
                msg = (f"        ✅ API match on {domain}: '{found_title}' [{cand}] (title {t_score:.2f}, author {'✓' if auth_ok else '✗'}, "
                       f"runtime {'✓' if dur_ok else '≈ (same narrator, inside tolerance)'}) evidence: {ev_txt}")
                if strict and not dur_ok and dur_near_n:  # keep looking: a candidate with exact runtime beats an approximate one
                    if near_pick is None: near_pick = (cand, dict(LAST_MATCH), msg)
                    continue
                if t_score < 0.99:
                    if best_pick is None or (round(t_score, 2), dur_ok) > best_pick[0]: best_pick = ((round(t_score, 2), dur_ok), cand, dict(LAST_MATCH), msg)
                    continue
                if not pub_year and SEARCH_CTX.get('pub') and SEARCH_CTX.get('year'):
                    if pending_pick is None: pending_pick = (cand, dict(LAST_MATCH), msg)
                    continue
                logging.info(msg)
                return cand
            if duration and p.get('runtime_sec') and abs(duration - p['runtime_sec']) <= 120 and a_narr and c_narr and a_narr & c_narr and not LAST_SEARCH.get('weak'):
                LAST_SEARCH['weak'] = {'asin': cand, 'title': found_title, 'title_score': round(t_score, 2), 'author_ok': bool(auth_ok),
                                       'cand': {'title': found_title, 'authors': p.get('authors_raw') or [], 'narrators': p.get('narrators_raw') or [],
                                                'runtime_min': round(p['runtime_sec'] / 60), 'series': [f"{n} #{q}" if q else n for n, q in ((x.get('name') or x.get('title'), x.get('part') or x.get('sequence')) for x in ((p.get('meta_raw') or {}).get('series') or []))],
                                                'store': domain}}  # runtime to the minute + narrator agree, title/author do not: a human decides
            logging.info(f"        ↪️ Rejected '{found_title}' [{cand}] (title {t_score:.2f}, author {'✓' if auth_ok else '✗'}, runtime {'✓' if dur_ok else '✗' if dur_conflict else '?'})")
        if domain == domains[0] and (nq or nq2) and not best_pick and not near_pick and not only_queries and not only_extra:  # preferred store first: author+narrator filter there before another store wins
            rel_b = LAST_SEARCH['related']
            res = search_audible_api(title, authors_list, duration, lang, raw_title, required_lang, strict, domain, narrators, abs_series, only_queries=[q_ for q_ in (nq, nq2) if q_])
            if res: return res
            if rel_b and not LAST_SEARCH['related']: LAST_SEARCH['related'] = rel_b
        if best_pick: break  # do not search further stores when this store already has an acceptable hit
    if pending_pick:
        LAST_MATCH.clear(); LAST_MATCH.update(pending_pick[1])
        logging.info(pending_pick[2])
        return pending_pick[0]
    if best_pick:
        LAST_MATCH.clear(); LAST_MATCH.update(best_pick[2])
        logging.info(best_pick[3])
        return best_pick[1]
    if near_pick:
        LAST_MATCH.clear(); LAST_MATCH.update(near_pick[1])
        logging.info(near_pick[2])
        return near_pick[0]
    if (nq or nq2) and not only_queries and not only_extra:  # nothing found: author + narrator filter (generic titles like 'The Prince' hide the right recording)
        rel_before = LAST_SEARCH['related']
        res = search_audible_api(title, authors_list, duration, lang, raw_title, required_lang, strict, only_domain, narrators, abs_series, only_queries=[q_ for q_ in (nq, nq2) if q_])
        if res: return res
        if rel_before and not LAST_SEARCH['related']: LAST_SEARCH['related'] = rel_before
    if api_ok and not only_queries and not only_extra and queries and (nt_ := {normalize_title_text(t_) for t_ in (title, raw_title) if t_} - {''}):
        # nothing accepted: a long run of look-alike titles ('Terrifying Towns: ...' x 14) can push the EXACT title beyond the 5 hits
        # looked at. One deeper look at the first query (titles included); only exact-title products are checked (batch 79, 2026-11-09)
        dom0 = domains[0]; host0 = AUDIBLE_API_HOSTS[dom0]
        throttle('audible_api', host0)
        try:
            r2 = requests.get(f"https://{host0}/1.0/catalog/products", params={"num_results": 25, **queries[0], "response_groups": "product_desc"},
                              headers={"User-Agent": FIXED_USER_AGENT, "Accept": "application/json"}, timeout=15)
            exact_ = [x['asin'] for x in ((r2.json().get('products') or []) if r2.status_code == 200 else [])
                      if x.get('asin') and normalize_title_text(x.get('title') or '') in nt_]  # the search title (cleaned '06 X' = 'X', batch 97) or the raw one
        except (requests.RequestException, ValueError): exact_ = []
        if exact_:
            res = search_audible_api(title, authors_list, duration, lang, raw_title, required_lang, strict, dom0, narrators, abs_series,
                                     extra_candidates=exact_, only_extra=True)
            if res: return res
    return None if api_ok else False

def find_missing_asin(title, authors_list, duration, lang, raw_title=None, required_lang=None, strict=False,
                      only_domain=None, narrators=None, abs_series=None):
    logging.info(f"      -> 🔎 Searching Replacement ASIN for '{title}'{f' on {only_domain}' if only_domain else ''}"
                 f"{' (strict: title, author, runtime and narrator must match)' if strict else ''}...")
    if narrators and authors_list and all(any(same_person_name(n, a) for a in authors_list) for n in narrators if n):
        narrators = None  # the 'narrator' is only the author (file tags of a self-read book or a tagging error): no evidence (Overlord 13)
    if USE_AUDIBLE_API:
        res = search_audible_api(title, authors_list, duration, lang, raw_title, required_lang, strict, only_domain, narrators, abs_series)
        if only_domain: return res or None  # preferred-store search: no web fallback, keep the existing ASIN
        if res is not False: return res
        logging.info("        ↪️ API search unavailable, falling back to web search...")
    return find_missing_asin_web(title, authors_list, duration, lang)

def find_missing_asin_via_series(title, authors_list, duration, lang, raw_title=None, required_lang=None, strict=False, narrators=None, abs_series=None):
    """Same verification as the search, but the candidates come from the Audible series listing (books the search does not return)."""
    domain = search_domains(required_lang or lang)[0]
    extras = series_candidates(abs_series, authors_list, duration, domain)
    if not extras: return None
    logging.info(f"      -> 📚 Audible series listing: {len(extras)} book(s) of the same series with matching runtime and author")
    res = search_audible_api(title, authors_list, duration, lang, raw_title, required_lang, strict, domain, narrators, abs_series,
                             extra_candidates=extras, only_extra=True)
    return res or None

def find_missing_asin_web(title, authors_list, duration, lang):
    doms = ["www.audible.com", "www.audible.de"]
    if lang and str(lang).strip().lower() in GERMAN_LANG_CODES: doms = ["www.audible.de", "www.audible.com"]
    prim_auth = authors_list[0] if authors_list else ""
    strategies = [
        {"params": {"title": title, "author_author": prim_auth, "ipRedirectOverride": "true"}, "mode": "Strict"},
        {"params": {"title": title, "ipRedirectOverride": "true"}, "mode": "TitleOnly"}
    ]

    for d in doms:
        for strat in strategies:
            r, soup = fetch_url(f"https://www.audible.de/search" if "audible.de" in d else f"https://{d}/search", params=strat["params"], domain=d)
            if not soup: continue
            for item in soup.find_all('li', class_=re.compile(r'productListItem')):
                asin = item.get('data-asin') or (item.find('div', attrs={'data-asin': True}) or {}).get('data-asin')
                if not asin: continue
                ft = item.find('h3', class_=re.compile(r'bc-heading'))
                if not ft: continue
                found_title = ft.get_text(strip=True)
                t_score = difflib.SequenceMatcher(None, title.lower(), found_title.lower()).ratio()
                if t_score < 0.7: continue

                dur_match = False
                found_dur_sec = 0
                if rt := item.find('li', class_=re.compile(r'runtimeLabel')):
                    h = re.search(r'(\d+)\s*(?:Std|hr|h)', rt.text)
                    m = re.search(r'(\d+)\s*(?:Min|m)', rt.text)
                    found_dur_sec = (int(h.group(1))*3600 if h else 0) + (int(m.group(1))*60 if m else 0)
                    if found_dur_sec > 0:
                        if duration and abs(duration - found_dur_sec) < 300: dur_match = True
                    else: dur_match = True 

                found_auth = ""
                if auth_tag := item.find('li', class_=re.compile(r'authorLabel')):
                    found_auth = auth_tag.get_text(strip=True).replace('By:', '').strip()
                auth_match = match_author(authors_list, found_auth)

                if t_score > 0.7 and auth_match:
                    if duration and found_dur_sec > 0 and not dur_match:
                          logging.info(f"        ⚠️ Skipped candidate '{found_title}': Author matches, but duration differs > 15m (ABS: {int(duration)}s vs Web: {found_dur_sec}s).")
                          continue
                    return asin
                if t_score > 0.8 and dur_match and duration:
                    logging.info(f"        ✅ Accepted '{found_title}' based on Title+Duration match (Author mismatch ignored: '{prim_auth}' vs '{found_auth}')")
                    return asin
    return None

def scrape_gr_details(url, require_book_page=False):
    r, soup = fetch_url(url, source='goodreads')
    if not soup: return None
    # An ID search without an exact hit returns a result list. Scraping that would pick up
    # the rating of an arbitrary first result, so only accept real book pages here.
    if require_book_page and "/book/show/" not in r.url: return None
    res = {'url': r.url, 'source': 'GR'}
    res['page_title'] = page_title(soup)
    if mini := soup.find('span', class_='minirating'):
        txt = mini.get_text()
        if m := re.search(r'(\d+[.,]\d+)\s+avg rating', txt):
            if is_valid_rating(m.group(1).replace(',', '.')): res['val'] = m.group(1).replace(',', '.')
        if m := re.search(r'([\d,.]+)\s+ratings', txt): res['count'] = int(re.sub(r'[^\d]', '', m.group(1)))
    if not res.get('count') or not res.get('val'):
        for s in soup.find_all('script', type='application/ld+json'):
            try:
                d = json.loads(s.string)
                if 'aggregateRating' in d:
                    val = d['aggregateRating'].get('ratingValue')
                    if is_valid_rating(val): res['val'] = val
                    c_votes = d['aggregateRating'].get('ratingCount')
                    c_reviews = d['aggregateRating'].get('reviewCount')
                    if c_votes: res['count'] = int(c_votes)
                    elif c_reviews: res['count'] = int(c_reviews) 
                if d.get('isbn'): res['isbn'] = d['isbn']
                if d.get('name'): res['name'] = d['name']
                if d.get('author'): res['author'] = ", ".join(a.get('name', '') for a in (d['author'] if isinstance(d['author'], list) else [d['author']]) if isinstance(a, dict))
            except: pass
    if 'val' not in res:
        if m := re.search(r'(\d+[.,]\d+)\s+avg rating', soup.get_text()): res['val'] = m.group(1).replace(',', '.')
    if 'count' not in res:
        if m := re.search(r'([\d,.]+)\s+ratings', soup.get_text()): res['count'] = int(re.sub(r'[^\d]', '', m.group(1)))
    if 'isbn' not in res and (meta_isbn := (soup.find('meta', property="books:isbn") or {}).get('content')): res['isbn'] = meta_isbn
    if 'isbn' not in res and (m := RE_ISBN_JSON.search(r.text)): res['isbn'] = m.group(1)
    if 'asin' not in res:
        if m := RE_ASIN_JSON.search(r.text) or RE_URL_ASIN.search(r.text): res['asin'] = m.group(1)
        if not res.get('asin'):
            if m := re.search(r'ASIN[:\s]*(B0\w+)', soup.get_text()): res['asin'] = m.group(1)
    if 'val' not in res:
        logging.info(f"        🔍 GR page loaded but no rating found (layout change?): {r.url} (title: '{page_title(soup)}')")
        return None
    return res

def get_goodreads_data(isbn, title, authors, prim_auth, cached_url=None, hc_ids=None, force=False):
    """Returns (data, status) with status in 'found', 'not_found', 'blocked', 'deferred', 'disabled'.
    cached_url: Goodreads book page found in an earlier run (1 request instead of a search)."""
    if not GOODREADS_ENABLED:
        logging.info("      -> ⏭️ Goodreads disabled (GOODREADS_ENABLED=false)")
        return None, 'disabled'
    if GR_STATE['blocked']:
        logging.info("      -> ⏭️ Goodreads skipped (blocked earlier in this run)")
        return None, 'blocked'
    GR_STATE['seen'] = GR_STATE.get('seen', 0) + 1
    if GOODREADS_SAMPLE > 1 and not force and GR_STATE['seen'] % GOODREADS_SAMPLE != 0:
        logging.info(f"      -> ⏭️ Goodreads: not this book's turn (every {GOODREADS_SAMPLE}. book); it is re-checked later")
        stats['gr_deferred'] += 1
        return None, 'deferred'
    logging.info("      -> Checking www.goodreads.com")
    while True:
        try:
            return _goodreads_lookup(isbn, title, authors, prim_auth, cached_url, hc_ids)
        except SourceBlockedException as e:
            if GR_STATE['cooldowns'] < GR_MAX_COOLDOWNS:
                GR_STATE['cooldowns'] += 1
                pause = max(GR_BLOCK_COOLDOWN, GR_STATE.pop('retry_after', 0) or 0)  # the server's Retry-After wins when longer
                logging.warning(f"        ⏸️ Goodreads blocks requests: {e}. Cool-down {pause}s, then retrying ({GR_STATE['cooldowns']}/{GR_MAX_COOLDOWNS})...")
                time.sleep(pause)
                continue
            GR_STATE['blocked'] = True
            logging.warning(f"        🛑 Goodreads still blocks requests: {e}. Skipping Goodreads for the rest of this run.")
            return None, 'blocked'

def gr_page_matches(d, title, authors):
    """The Goodreads page must be THIS book: title similar and (author matches or page lists no author)."""
    t = max(title_similarity(clean_title(title), clean_title(d.get('name') or d.get('page_title') or '')),
            search_match_score(title, d.get('name') or d.get('page_title') or '', ''))
    a_ok = match_author(authors, d['author']) if d.get('author') else None
    return t >= 0.8 and a_ok is not False, t, a_ok

def _gr_budget_left():
    """Search budget of this run (book pages have their own, GOODREADS_PAGES_PER_RUN)."""
    if GOODREADS_MAX_PER_RUN and GR_STATE['searches'] >= GOODREADS_MAX_PER_RUN:
        if GR_STATE['searches'] == GOODREADS_MAX_PER_RUN:
            logging.info(f"        ⏭️ Goodreads search budget for this run reached ({GOODREADS_MAX_PER_RUN}). Remaining books follow in the next runs.")
            GR_STATE['searches'] += 1  # log only once
        return False
    return True

def _gr_book_pages(isbn, title, authors, hc_ids, cached_url):
    """Primary path (allowed by Goodreads' robots.txt): the book page itself, from a known URL, a Hardcover Goodreads ID
    or the ISBN page. Every page counts against GOODREADS_PAGES_PER_RUN; Goodreads guards these pages more strictly
    than the search (measured 2026-11-04: challenge after 3-7 pages), so the budget stays small.
    Raises SourceBlockedException on a bot challenge (the caller then falls back to the search list)."""
    cands = []
    if cached_url: cands.append((cached_url, 'Cache'))
    cands += [(f"https://www.goodreads.com/book/show/{g}", 'Hardcover-ID') for g in (hc_ids or [])[:GR_ID_TRIES]]
    if q := normalize_isbn(isbn): cands.append((f"https://www.goodreads.com/book/isbn/{q}", 'ISBN Lookup'))
    seen = set()
    for url, src in cands:
        gid = (m.group(1) if (m := RE_GR_BOOK_ID.search(url)) else url)
        if gid in seen: continue
        seen.add(gid)
        if GR_STATE['pages'] >= GOODREADS_PAGES_PER_RUN:
            if GR_STATE['pages'] == GOODREADS_PAGES_PER_RUN:
                logging.info(f"        ⏭️ Goodreads book-page budget for this run reached ({GOODREADS_PAGES_PER_RUN}); search list only from here")
                GR_STATE['pages'] += 1
            return None
        GR_STATE['pages'] += 1
        d = scrape_gr_details(url, require_book_page=True)
        if not d: continue
        ok, t, a_ok = gr_page_matches(d, title, authors)
        logging.info(f"        🔗 Goodreads page via {src}: '{d.get('name') or d.get('page_title')}' title {t:.2f}, author {'?' if a_ok is None else '✓' if a_ok else '✗'}")
        if ok and d.get('count'):
            d['source'] = src
            logging.info(f"        ✅ Goodreads via {src} (Count: {d.get('count')}, Rating: {round(safe_float(d.get('val')), 2)})")
            return d
    return None

def _goodreads_lookup(isbn, title, authors, prim_auth, cached_url, hc_ids=None):
    known = {str(g) for g in (hc_ids or [])}
    if cached_url and (m := RE_GR_BOOK_ID.search(cached_url)): known.add(m.group(1))
    # 1) book page (robots.txt allows /book/show and /book/isbn) - small budget per run
    if GR_BOOK_PAGES and not GR_STATE.get('pages_blocked') and GOODREADS_PAGES_PER_RUN > 0:
        try:
            if d := _gr_book_pages(isbn, title, authors, hc_ids, cached_url):
                return d, 'found'
        except SourceBlockedException as e:
            GR_STATE['pages_blocked'] = True
            logging.warning(f"        ⚠️ Goodreads blocks book pages: {e}. Book pages off for this run, search list as fallback.")
    # 2) fallback: the search list (rating from the result row; known ids only confirm a row). A block here stops Goodreads.
    if not _gr_budget_left():
        stats['gr_deferred'] += 1
        return None, 'deferred'
    GR_STATE['searches'] += 1
    d = _search_goodreads(isbn, title, authors, prim_auth, known_ids=known, list_only=True)
    return d, ('found' if d else 'not_found')

RE_GR_LIST_RATING = re.compile(r'(\d[.,]\d{1,2})\s*avg rating\s*[—–-]?\s*([\d,.]+)\s*ratings?', re.IGNORECASE)
RE_GR_BOOK_ID = re.compile(r'/book/show/(\d+)')

def list_rating(link):
    """Read 'x.xx avg rating — n ratings' from the result container of a search hit.
    Only containers that belong to exactly one book are used, so we never take a neighbour's rating."""
    node = link
    for _ in range(8):
        node = node.parent
        if node is None: return None
        ids = {m.group(1) for a in node.find_all('a', href=RE_GR_BOOK_ID) if (m := RE_GR_BOOK_ID.search(a['href']))}
        if len(ids) > 1: return None
        if m := RE_GR_LIST_RATING.search(node.get_text(" ", strip=True)):
            val, cnt = m.group(1).replace(',', '.'), int(re.sub(r'[^\d]', '', m.group(2)) or 0)
            return (val, cnt) if is_valid_rating(val) and cnt > 0 else None
    return None

def parse_gr_search_results(soup):
    """Return [(title, absolute_url, author, (rating, count) or None)] from a Goodreads search page.
    Tries the classic table layout first, then a layout-independent fallback that pairs every
    /book/show/ link with the nearest /author/show/ link."""
    results, seen = [], set()
    def add(link, author):
        href = urllib.parse.urljoin("https://www.goodreads.com", link.get('href', '')).split('?')[0]
        t = link.get_text(" ", strip=True)
        if not t or href in seen: return
        seen.add(href); results.append((t, href, author, list_rating(link)))

    rows = soup.find_all('tr', itemtype=re.compile(r'schema\.org/Book'))
    for row in rows:
        link = row.find('a', class_='bookTitle') or row.find('a', href=re.compile(r'/book/show/'))
        auth = row.find('a', class_='authorName') or row.find('a', href=re.compile(r'/author/show/'))
        if link: add(link, auth.get_text(" ", strip=True) if auth else "")
    if results: return results

    for link in soup.find_all('a', href=re.compile(r'/book/show/\d+')):
        node, auth = link, None
        for _ in range(6):  # walk up to the result container
            node = node.parent
            if node is None: break
            if auth := node.find('a', href=re.compile(r'/author/show/')): break
        add(link, auth.get_text(" ", strip=True) if auth else "")
    return results

RE_GR_NEXT_BOOK = re.compile(r'"__typename":"Book",')

def _unjs(s):
    try: return json.loads(f'"{s}"')
    except ValueError: return s

def parse_gr_next_results(html):
    """New Goodreads search layout (Next.js, seen 2026-11-04): the result books are embedded as JSON inside JS strings.
    Returns [(title, url, author, (rating, count) or None)] like parse_gr_search_results. work.stats = all editions,
    the same number the classic list shows."""
    if '"__typename' not in html and '\\"__typename' not in html: return []
    text = html.replace('\\"', '"')
    out, seen = [], set()
    for chunk in RE_GR_NEXT_BOOK.split(text)[1:]:
        chunk = chunk[:6000]  # one book object
        m_url = re.search(r'"webUrl":"(https://www\.goodreads\.com/book/show/\d+[^"]*)"', chunk)
        m_title = re.search(r'"legacyId":\d+,"title":"((?:[^"\\]|\\.)*)"', chunk)
        if not (m_url and m_title): continue
        url = m_url.group(1).split('?')[0]
        if url in seen: continue
        seen.add(url)
        m_auth = re.search(r'"primaryContributorEdge":\{.*?"name":"((?:[^"\\]|\\.)*)"', chunk)
        m_st = re.search(r'"averageRating":([\d.]+),"ratingsCount":(\d+)', chunk)
        rating = (m_st.group(1), int(m_st.group(2))) if m_st and int(m_st.group(2)) > 0 and is_valid_rating(m_st.group(1)) else None
        ser = [(_unjs(m.group(2)), m.group(1), m.group(3)) for m in re.finditer(
            r'"seriesPlacement":"([^"]*)","series":\{"__typename":"Series","id":"[^"]*","title":"((?:[^"\\]|\\.)*)","webUrl":"(https://www\.goodreads\.com/series/[^"]+)"', chunk.split('"primaryContributorEdge"')[0])]
        GR_ROW_SERIES[url] = ser
        out.append((_unjs(m_title.group(1)), url, _unjs(m_auth.group(1)) if m_auth else '', rating))
    return out

GR_ROW_SERIES = _TLDict('gr_row_series')  # result url -> [(series name, placement, series page url)] of the last parsed page (per thread)
GR_CTX = _TLDict('gr_ctx')                # {'series': [(name, number)]} of the book being looked up (set by the caller, per thread)
HC_CTX = _TLDict('hc_ctx')                # {'lang': ABS language of the book being looked up} (per thread)

def _num_eq(a, b):
    try: return float(str(a).strip()) == float(str(b).strip())
    except (TypeError, ValueError): return False

def gr_series_match(name, placement, ours):
    """Same series (name nearly equal after cleaning) AND exactly the same volume number."""
    clean = lambda x: re.sub(r'\s*\([^)]*\)\s*$', '', x or '').strip()
    return any(n and title_similarity(clean(n), clean(name)) >= 0.9 and _num_eq(num, placement) for n, num in ours)

def gr_series_page(url, number, authors):
    """Goodreads series page (one request): the volume with exactly this number, if its author matches. Returns (title, url, (val, count)) or None."""
    r, soup = fetch_url(url, source='goodreads')
    if not soup: return None
    hits = []
    for m in re.finditer(r'data-react-class="ReactComponents.SeriesList" data-react-props="([^"]*)"', r.text):
        try: d = json.loads(html_lib.unescape(m.group(1)))
        except ValueError: continue
        heads = d.get('seriesHeaders') or []
        for i, e in enumerate(d.get('series') or []):
            b = e.get('book') or {}
            head = heads[i] if i < len(heads) and isinstance(heads[i], str) else ''
            num = (re.search(r'(\d+(?:\.\d+)?)', head) or re.search(r'#(\d+(?:\.\d+)?)\)', b.get('title') or '') or [None, None])[1]
            if not _num_eq(num, number): continue
            if not match_author(authors, (b.get('author') or {}).get('name') or ''): continue
            if b.get('ratingsCount') and is_valid_rating(b.get('avgRating')):
                hits.append((b.get('title'), urllib.parse.urljoin('https://www.goodreads.com', b.get('bookUrl') or ''), (str(b['avgRating']), int(b['ratingsCount']))))
    hits = list({h[1]: h for h in hits}.values())
    return hits[0] if len(hits) == 1 else None  # two books with the same number (e.g. 1 and 1.0 editions): ambiguous -> nothing

_gr_sample_saved = {'done': False}
def save_gr_sample(html, q):
    """Store the first unparseable search page per run so the layout can be inspected."""
    if _gr_sample_saved['done']: return
    _gr_sample_saved['done'] = True
    try:
        path = os.path.join(LOG_DIR, "goodreads_search_sample.html")
        with open(path, 'w', encoding='utf-8') as f: f.write(html)
        logging.info(f"        💾 Saved unparseable GR search page for '{q}' to {path}")
    except OSError: pass

def _search_goodreads(isbn, title, authors, prim_auth, known_ids=None, list_only=False):
    """list_only: never load a book page (an ISBN lookup and a direct search hit both redirect to one) - only the result list."""
    known_ids = known_ids or set()
    LOOKUP_HITS['goodreads'] = 0
    # Audible ASINs practically never resolve on Goodreads, so only the ISBN is tried
    for q_id, src in [(normalize_isbn(isbn), 'ISBN Lookup')]:
        if q_id and not list_only:
            # /book/isbn/<isbn> redirects straight to the book page (1 request instead of search + page)
            if d := scrape_gr_details(f"https://www.goodreads.com/book/isbn/{q_id}", require_book_page=True):
                d['source'] = src
                logging.info(f"        ✅ Found via {src} (Count: {d.get('count')}, Rating: {round(safe_float(d.get('val')), 2)})")
                return d
    base_title = clean_title(title)
    searches = []
    # title first: 'title + author' mostly returns third-party summaries ('Summary of X by Y') for popular books
    for q in ([title, base_title, f"{base_title} {prim_auth}"] if list_only else [f"{title} {prim_auth}", f"{base_title} {prim_auth}", title, base_title]):
        q = q.strip()
        if q and q not in searches: searches.append(q)  # No duplicate requests
    norm_target = normalize_title_text(title)
    series_pages = {}  # our series seen in the results, but with other volume numbers -> series page as the last step

    for q in searches:
        r, soup = fetch_url(f"https://www.goodreads.com/search", params={"q": q}, source='goodreads')
        if not soup: LOOKUP_HITS['goodreads'] = None; continue  # no answer: unknown, never 'absent'
        if "/book/show/" in r.url:
            if LOOKUP_HITS['goodreads'] is not None: LOOKUP_HITS['goodreads'] += 1
            if list_only:  # redirected to a book page: read it (it is loaded anyway), but this is what Goodreads blocks
                logging.info(f"        ↪️ GR search '{q}' redirected to a book page")
            if d := scrape_gr_details(r.url): 
                d['source'] = 'Text Search (Direct Hit)'
                logging.info(f"        ✅ Found via Text Search (Direct) (Count: {d.get('count')}, Rating: {round(safe_float(d.get('val')), 2)})")
                return d
        else:
            best_url, best_score, best_rating = None, 0.0, None
            results = parse_gr_next_results(r.text) or parse_gr_search_results(soup)  # new (Next.js) and classic layout
            if LOOKUP_HITS['goodreads'] is not None: LOOKUP_HITS['goodreads'] += len(results)
            if not results and 'No results' not in r.text and 'noResults' not in r.text: LOOKUP_HITS['goodreads'] = None  # unparseable, not empty
            if not results:
                logging.info(f"        🔍 GR search '{q}': no results parsed (title: '{page_title(soup)}', {len(r.text)} bytes)")
                if len(r.text) > 50000: save_gr_sample(r.text, q)
            ours = GR_CTX.get('series') or []
            for found_title, found_url, found_auth, found_rating in results:
                if ours and match_author(authors, found_auth):
                    for sname, splace, surl in GR_ROW_SERIES.get(found_url) or []:
                        if gr_series_match(sname, splace, ours):
                            if found_rating:
                                d = {'url': found_url, 'val': found_rating[0], 'count': found_rating[1], 'source': 'Text Search (series + number)'}
                                logging.info(f"        ✅ Found via Text Search (same series '{sname}' #{splace}, author ✓, no page load) (Count: {d['count']}, Rating: {round(safe_float(d['val']), 2)})")
                                return d
                        elif any(title_similarity(n, sname) >= 0.9 for n, _ in ours): series_pages.setdefault(surl, sname)
                norm_found = normalize_title_text(found_title)
                t_score = difflib.SequenceMatcher(None, norm_target, norm_found).ratio()
                if (len(norm_target) > 3 and norm_target in norm_found) or (len(norm_found) > 3 and norm_found in norm_target): t_score += 0.15
                f_nums, t_nums = extract_volume(found_title), extract_volume(title)
                if (f_nums and t_nums and not f_nums & t_nums): 
                    if t_score < 0.9: continue
                if not match_author(authors, found_auth): continue
                gid = (RE_GR_BOOK_ID.search(found_url) or [None, None])[1]
                if gid in known_ids and t_score > 0.6 and found_rating:  # the id Hardcover / an earlier run named, author matches
                    d = {'url': found_url, 'val': found_rating[0], 'count': found_rating[1], 'source': 'Text Search (known id)'}
                    logging.info(f"        ✅ Found via Text Search (known Goodreads id {gid}, no page load) (Count: {d['count']}, Rating: {round(safe_float(d['val']), 2)})")
                    return d
                if t_score > 0.75 and t_score > best_score:
                    best_score, best_url, best_rating = t_score, found_url, found_rating
            if best_url and best_rating:
                # Rating is already in the result list: no need to load the book page
                d = {'url': best_url, 'val': best_rating[0], 'count': best_rating[1], 'source': 'Text Search (List)'}
                logging.info(f"        ✅ Found via Text Search (List, no page load) (Count: {d['count']}, Rating: {round(safe_float(d['val']), 2)})")
                return d
            if best_url and not list_only:
                if d := scrape_gr_details(best_url): 
                    d['source'] = 'Text Search (List Match)'
                    logging.info(f"        ✅ Found via Text Search (List) (Count: {d.get('count')}, Rating: {round(safe_float(d.get('val')), 2)})")
                    return d
    for surl, sname in list(series_pages.items())[:1]:  # one series page at most
        num = next((n for nm, n in (GR_CTX.get('series') or []) if title_similarity(nm, sname) >= 0.9 and n), None)
        if num is None: break
        if h := gr_series_page(surl, num, authors):
            d = {'url': h[1], 'val': h[2][0], 'count': h[2][1], 'source': 'Series page'}
            logging.info(f"        ✅ Found via series page '{sname}' #{num}: '{h[0]}' (Count: {d['count']}, Rating: {round(safe_float(d['val']), 2)})")
            return d
        logging.info(f"        🔍 GR series page '{sname}': no single volume #{num} by this author")
    logging.info("        ❌ Not found via ID or Text.")
    return None

# ================= HARDCOVER =================
# GraphQL API (https://api.hardcover.app/v1/graphql). Only ratings are taken, never metadata.
# Lookup order: manual link -> cached book id -> ASIN/ISBN edition -> title+author search (strict match).
HC_URL = 'https://api.hardcover.app/v1/graphql'
HC_STATE = {'disabled': None, 'requests': 0, 'daily_left': None, 'cooldowns': 0}
HC_USAGE = {'day': '', 'requests': 0}

def hc_usage_today():
    """Live Hardcover requests already made today (UTC day) by this tool - the own budget, independent of the account plan."""
    day = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    if HC_USAGE.get('day') != day: HC_USAGE.update(day=day, requests=0)
    return HC_USAGE['requests']
HC_CACHE, HC_PREFETCH = {}, {}  # key -> {'id', 'slug'} (persisted) | book id -> book (bulk refresh of cached books)
HC_DAILY_RESERVE = 25  # stop before the daily limit is used up completely
HC_BOOK_FIELDS = "id title subtitle slug rating ratings_count compilation cached_featured_series contributions { author { name } } book_mappings(where:{platform_id:{_eq:1}}, limit:8){ external_id }"
if not (recorder.RECORD_DIR or recorder.REPLAY_DIR):  # live: also the role of every contributor (illustrator, translator, ...); recordings keep the old query
    HC_BOOK_FIELDS = HC_BOOK_FIELDS.replace("contributions { author { name } }", "contributions { contribution author { name } }")
# Hits that are not the book itself (unless the ABS title says so too)
RE_HC_SPECIAL = re.compile(r'(?i)\b(graphic novel|comic|manga|coloring|colouring|adventure module|sourcebook|summary of|study guide|'
                           r'sparknotes|cliffsnotes|box(?:ed)? set|boxset|omnibus|collection|collector\W?s edition|books? \d+\s*[-–&]\s*\d+|summary|workbook)\b')
# Hardcover main titles that name a whole series ('The High Druid of Shannara Trilogy')
RE_HC_SET_TITLE = re.compile(r'(?i)\b(trilogy|duology|quartet|quintet|series|saga|set|bundle|collection|books?\s*\d+\s*[-–]\s*\d+)\s*$')
RE_HC_SLUG = re.compile(r'hardcover\.app/books/([\w-]+)')

def hc_query(query, variables=None):
    """One GraphQL request. Returns the data dict or None. Raises SourceBlockedException on HTTP 429."""
    if HC_STATE['disabled']: return None
    if hc_usage_today() >= HC_DAILY_LIMIT - HC_DAILY_RESERVE:
        HC_STATE['disabled'] = f"own daily budget used ({HC_USAGE['requests']} of {HC_DAILY_LIMIT}, {'paid' if HARDCOVER_PAID else 'free'} plan)"
        logging.warning(f"        ⏭️ Hardcover {HC_STATE['disabled']}. Remaining books follow in the next runs.")
        return None
    throttle('hardcover')
    HC_STATE['requests'] += 1
    simulated = recorder.REPLAY_DIR or recorder.RECORD_DIR
    live_before = recorder._LAST_LIVE.get('api.hardcover.app') if simulated else None
    try:
        r = requests.post(HC_URL, json={'query': query, 'variables': variables or {}}, timeout=20,
                          headers={'authorization': f"Bearer {HARDCOVER_TOKEN}", 'content-type': 'application/json',
                                   'user-agent': 'abs-ratings (Audiobookshelf rating script)'})
    except requests.RequestException as e:
        logging.info(f"        ⚠️ Hardcover request failed: {e}")
        return None
    if not simulated or recorder._LAST_LIVE.get('api.hardcover.app') != live_before:  # only real (live) requests use the budget
        with IO_LOCK:
            hc_usage_today(); HC_USAGE['requests'] += 1
            if HC_USAGE['requests'] % 50 == 0: rw_json(HC_USAGE_FILE, HC_USAGE)
    burst = r.headers.get('X-Ratelimit-Remaining') or ''
    if burst.isdigit() and int(burst) <= 3: time.sleep(3)  # minute budget nearly used (e.g. another tool on the same token): let it refill
    left = r.headers.get('X-Ratelimit-Daily-Remaining') or ''
    if left.isdigit(): HC_STATE['daily_left'] = int(left)
    rst = next((v for k, v in r.headers.items() if 'reset' in k.lower() and 'ratelimit' in k.lower()), None)  # when the daily limit renews (if Hardcover tells)
    if rst: HC_STATE['reset'] = rst
    if r.status_code == 429: raise SourceBlockedException('HTTP 429')
    if r.status_code in (401, 403):
        HC_STATE['disabled'] = f"token rejected (HTTP {r.status_code})"
        logging.warning(f"        🛑 Hardcover {HC_STATE['disabled']}. Skipping Hardcover for the rest of this run.")
        return None
    if r.status_code != 200:
        logging.info(f"        ⚠️ Hardcover HTTP {r.status_code}: {r.text[:150]}")
        return None
    try: j = r.json()
    except ValueError: return None
    if j.get('errors'):
        msg = str(j['errors'])[:200]
        if re.search(r'(?i)jwt|token|unauthori', msg):
            HC_STATE['disabled'] = 'token rejected'
            logging.warning(f"        🛑 Hardcover rejects the token ({msg}). Skipping Hardcover for the rest of this run.")
        else: logging.info(f"        ⚠️ Hardcover error: {msg}")
        return None
    if HC_STATE['daily_left'] is not None and HC_STATE['daily_left'] < HC_DAILY_RESERVE:
        HC_STATE['disabled'] = 'daily request limit reached'
        logging.warning(f"        ⏭️ Hardcover daily request limit almost used up ({HC_STATE['daily_left']} left). Remaining books follow in the next runs.")
    return j.get('data') or {}

def hc_fetch_gr_ids(hc_id):
    """Search hits have no book_mappings: one small extra request."""
    try:
        data = hc_query("query($id:Int!){ books(where:{id:{_eq:$id}}, limit:1){ book_mappings(where:{platform_id:{_eq:1}}, limit:8){ external_id } } }", {'id': hc_id})
        return hc_goodreads_ids(((data or {}).get('books') or [{}])[0])
    except SourceBlockedException: raise
    except Exception: return []

def hc_audible_crosscheck(hc, aud):
    """Is the Hardcover book the same book as the Audible product? Returns (ok, why). Title similar AND author matching; both missing = mismatch."""
    t = max(title_similarity(clean_title(hc.get('title') or ''), clean_title(aud.get('title_raw') or '')),
            search_match_score(aud.get('title_raw') or '', hc.get('title') or '', ''), search_match_score(hc.get('title') or '', aud.get('title_raw') or '', aud.get('subtitle_raw')))
    a = match_author(aud.get('authors_raw') or [], ', '.join(hc.get('authors') or [])) if (aud.get('authors_raw') and hc.get('authors')) else None
    if a is False and (non_latin_script(', '.join(aud.get('authors_raw') or [])) or non_latin_script(', '.join(hc.get('authors') or []))): a = None  # '理不尽な孫の手' = Rifujin na Magonote
    why = f"title {t:.2f}, author {'?' if a is None else '✓' if a else '✗'}"
    hw, aw = set(normalize_title_text(hc.get('title') or '').split()), set(normalize_title_text(aud.get('title_raw') or '').split())
    contained = bool(hw and aw and (hw <= aw or aw <= hw) and min(len(hw), len(aw)) >= 2)  # 'Ascendant Insanity' in 'An Outcast in Another World: Ascendant Insanity'
    non_english = str((aud.get('meta_raw') or {}).get('language') or 'english').lower() not in ('english', 'en', 'eng')
    tied = hc.get('source') in ('ISBN', 'ASIN', 'manual link') and non_english  # identifier + non-English edition: a different title is a translation ('Shadow\'s Edge' = 'Am Rande der Schatten')
    # the Hardcover id was cached when an earlier run verified it (translations via edition titles); a German edition keeps its German title
    if hc.get('source') == 'cache' and non_english and a: tied = True
    alt_hit = any(title_similarity(clean_title(x), clean_title(aud.get('title_raw') or '')) >= 0.85 for x in hc.get('alts') or [])  # Hardcover lists the Audible title as an alternative title (translations)
    if alt_hit: why += ', Audible title is an alternative title on Hardcover'
    same_series = False
    for sn, sp in hc.get('series') or []:  # same series name AND same number on Hardcover and Audible ('Home Sweet Home' = 'Jake\'s Magical Market 3')
        for cs in (aud.get('meta_raw') or {}).get('series') or []:
            m_ = re.search(r'\d+(?:\.\d+)?', str(cs.get('part') or ''))
            if sn and sp is not None and m_ and float(m_.group(0)) == float(sp) and title_similarity(sn, cs.get('name') or '') >= 0.85: same_series = True
    if same_series: why += ', same series and number'
    # another volume (2026-11-09, library: 'Throne Hunters 4' cached as 'Throne Hunters: Throne Hunters, Book 1'): both titles carry a
    # volume number and they share none -> not the same book, whatever the title score says (containment counts 'Throne Hunters')
    va, vh = extract_volume(aud.get('title_raw') or ''), extract_volume(hc.get('title') or '')
    if va and vh and not (va & vh) and not same_series and not tied and not alt_hit:
        return False, why + f", other volume ({'/'.join(sorted(vh))} vs {'/'.join(sorted(va))})"
    if tied: why += ', tied by ' + hc['source']
    elif contained and t < 0.7: why += ', title words contained'
    return (a is not False and (t >= 0.7 or contained or tied or same_series or alt_hit)), why

def hc_goodreads_ids(book):
    """Goodreads book IDs Hardcover knows for this book (one per edition; the rating belongs to the work, any edition page shows it)."""
    return [m["external_id"] for m in (book or {}).get("book_mappings") or [] if str(m.get("external_id", "")).isdigit()]

def hc_target_titles(aud, search_title, title, abs_subtitle=None):
    """Titles a Hardcover hit is compared with. The first one is also the search query (unchanged).
    Added for comparison only: 'Title: Subtitle' (Hardcover search documents store the subtitle inside the title:
    'A Book Forged in Hell: Spinoza's Scandalous Treatise ...') and the book title after a numbered series prefix
    ('The Princess Diaries, Volume VII: Party Princess' -> 'Party Princess', how Hardcover lists it)."""
    aud = aud or {}
    out = [aud.get('title_raw'), search_title, title]
    if aud.get('title_raw') and aud.get('subtitle_raw'): out.append(f"{aud['title_raw']}: {aud['subtitle_raw']}")
    if title and abs_subtitle: out.append(f"{title}: {abs_subtitle}")
    for t in (aud.get('title_raw'), title):
        m = re.match(r'^(.+?):\s*(.+)$', t or '')
        if m and extract_volume(m.group(1)) and len(m.group(2).strip()) >= 6 and not extract_volume(m.group(2)):
            out.append(m.group(2).strip())
    return list(dict.fromkeys(t for t in out if t))

def hc_result(book, source):
    return {'gr_ids': hc_goodreads_ids(book), 'id': book.get('id'), 'slug': book.get('slug'), 'url': f"https://hardcover.app/books/{book.get('slug')}",
            'val': book.get('rating'), 'count': int(book.get('ratings_count') or 0), 'source': source, 'title': book.get('title'),
            'authors': [a for a in hc_authors(book) if a], 'contributions': book.get('contributions') or [],
            'alts': [t for t in (book.get('alternative_titles') or []) if t],
            'series': [(re.sub(r'\s*\([^)]*\)\s*$', '', ((book.get('featured_series') or book.get('cached_featured_series') or {}).get('series') or {}).get('name') or ''),
                        (book.get('featured_series') or book.get('cached_featured_series') or {}).get('position'))]}

def hc_authors(book):
    """Hardcover's AUTHORS of a book (contributors with another role - illustrator, translator, 'Contributor' - are left out)."""
    cs = [c for c in (book.get('contributions') or []) if (c or {}).get('contribution') in (None, '', 'Author')]
    return [((c or {}).get('author') or {}).get('name') for c in cs] or book.get('author_names') or []

def hc_learn_roles(book):
    """Names Hardcover lists with ANOTHER role than author on a verified book -> role names (REMOVE_ROLE_CREDITS removes them from the ABS
    author field when a real author remains; a name that is somewhere the first author is never removed)."""
    authors = {author_key_s(a) for a in hc_authors(book) if a}
    for c in book.get('contributions') or []:
        role, name = (c or {}).get('contribution'), ((c or {}).get('author') or {}).get('name')
        if name and role and role != 'Author' and author_key_s(name) not in authors:
            ROLE_NAMES.add(name)

def author_key_s(n):
    from abs_series import author_key
    return author_key(n)

def hc_prefetch(keys):
    """Refresh all cached books of this run in bulk (100 per request) instead of one request per book."""
    ids = sorted({HC_CACHE[k]['id'] for k in keys if HC_CACHE.get(k, {}).get('id')})
    for i in range(0, len(ids), 100):
        try: data = hc_query(f"query($ids:[Int!]){{ books(where:{{id:{{_in:$ids}}}}){{ {HC_BOOK_FIELDS} }} }}", {'ids': ids[i:i + 100]})
        except SourceBlockedException: break
        for b in (data or {}).get('books') or []: HC_PREFETCH[b['id']] = b
    if ids: logging.info(f"Hardcover: {len(HC_PREFETCH)}/{len(ids)} cached books refreshed in bulk.")

def hc_title_score(targets, book, extra_names=(), pure=False):
    """Best title similarity between our titles and the Hardcover title (with/without subtitle, alternative titles,
    edition title). pure=True: plain ratio without the containment bonus ('Dragon Heart' vs 'Dragon Heart: Final')."""
    names = [book.get('title') or '']
    if book.get('subtitle'): names.append(f"{book['title']}: {book['subtitle']}")
    names += [t for t in (book.get('alternative_titles') or []) if isinstance(t, str)][:15]
    names += [t for t in extra_names if t]
    best = 0.0
    for t in targets:
        for n in names:
            n = html_lib.unescape(n)
            if pure:
                a_, b_ = normalize_title_text(t), normalize_title_text(n)
                sc = difflib.SequenceMatcher(None, a_, b_).ratio() if a_ and b_ else 0.0
            else: sc = title_similarity(t, n)
            best = max(best, sc)
    return best

def hc_series_check(book, abs_series):
    """True/False if Hardcover's main series is one of the ABS series and the number matches/differs, else None."""
    fs = book.get('featured_series') or book.get('cached_featured_series') or {}
    pos, name = fs.get('position'), re.sub(r'\s*\([^)]*\)\s*$', '', ((fs.get('series') or {}).get('name') or ''))
    if pos is None or not name: return None
    verdict = None
    for s_name, seq in abs_series:  # a book can be in several orders ('[publication order]', '[chronological order]')
        if s_name and seq and title_similarity(s_name, name) >= 0.85:
            try:
                if float(seq) == float(pos): return True
                verdict = False
            except (TypeError, ValueError): pass
    return verdict

def hc_words_contained(targets, book):
    """All words of one of our titles appear in the Hardcover title ('Conquest' in 'Life Reset: Conquest')."""
    names = [f"{book.get('title') or ''} {book.get('subtitle') or ''}"]
    return any(set(normalize_title_text(t).split()) <= set(normalize_title_text(n).split()) for t in targets for n in names if normalize_title_text(t))

def hc_acceptable(targets, authors, book, min_score, abs_series=(), extra_names=(), pure=False):
    """Same strict rules as the other sources. Returns (ok, score, reason).
    pure=True is the title search: stricter, because the hit is not tied to our ASIN/ISBN."""
    # A Hardcover book without any author entry cannot contradict us; for ASIN/ISBN-tied hits (not a title search) the identifier decides.
    if authors and (hc_authors(book) or pure) and not match_author(authors, ', '.join(a for a in hc_authors(book) if a)):
        return False, 0, f"author {hc_authors(book)[:3]}"
    series_ok = hc_series_check(book, abs_series)
    fs = book.get('featured_series') or book.get('cached_featured_series') or {}
    score = hc_title_score(targets, book, extra_names, pure)
    # Series numbers often differ between ABS and Hardcover (reading order, part/volume schemes), so a different
    # number only rejects if the title is not clearly the same ('Dragon Heart' vs 'Dragon Heart: Final')
    if series_ok is False and score < 0.95:
        return False, score, f"series number #{fs.get('position')} vs ABS {[q for _, q in abs_series if q]}, title score {score:.2f}"

    # Same series and same number confirm the book even if ABS only has a series name as title.
    # For search hits, our title words must also appear in the Hardcover title.
    series_confirms = bool(series_ok) and (not pure or hc_words_contained(targets, book))
    if score < min_score and not series_confirms: return False, score, f"title score {score:.2f}"
    ours, theirs = set().union(*(extract_volume(t) for t in targets)), extract_volume(f"{book.get('title') or ''} {book.get('subtitle') or ''}")
    if ours and theirs and not ours & theirs and not series_ok: return False, score, f"volume {sorted(theirs)} vs {sorted(ours)}"
    # Library check 2026-11-05 (Phase C): number contradictions the title score did not see
    abs_nums = set()
    for _, q in abs_series:
        try: abs_nums.add(str(float(q)).rstrip('0').rstrip('.'))
        except (TypeError, ValueError): pass
    # only numbers below 1000 (years like 2026 are no volumes); Hardcover: the MAIN title only (subtitles carry series counts)
    nz = lambda xs: {str(float(x)).rstrip('0').rstrip('.') for x in xs if re.fullmatch(r'\d+(\.\d+)?', str(x)) and float(x) < 1000}
    h_main = nz(extract_volume(book.get('title') or ''))
    abs_t = HC_CTX.get('title') or ''
    abs_title_nums = nz(extract_volume(abs_t))  # the ABS (search) title, not Audible's
    if abs_title_nums and h_main and not abs_title_nums & h_main:  # ABS title 'Vol. 5' vs Hardcover 'Vol. 4' (even if a wrong ABS series number agrees)
        return False, score, f"volume in the title {sorted(h_main)} vs ABS title {sorted(abs_title_nums)}"
    if h_main and abs_nums and not nz(ours) and not h_main & abs_nums:  # 'Defiance of the Fall 3' for ABS series #1
        return False, score, f"Hardcover title volume {sorted(h_main)} vs ABS series {sorted(abs_nums)}"
    part = lambda t: set(re.findall(r'(?i)\bpart\s*(\d+)', t or ''))
    if part(abs_t) and part(book.get('title')) and not part(abs_t) & part(book.get('title')):  # 'Part 1 Volume 1' vs 'Part 5 Volume 1'
        return False, score, "part numbers differ"
    if series_ok is False:  # same series, another number:
        sname = ((fs.get('series') or {}).get('name') or '')
        if sname and not h_main and title_similarity(clean_title(book.get('title') or ''), sname) >= 0.9:  # the Hardcover title is only the series name (no number)
            return False, score, f"Hardcover title is the series name, number #{fs.get('position')} differs"
        if HC_CTX.get('lang') == 'English' and hc_title_score(targets, {**book, 'alternative_titles': []}, extra_names, pure) < min_score:
            # fits only via an alternative title (English book: no translation that would explain another title and numbering)
            return False, score, f"only an alternative title fits and #{fs.get('position')} is another volume of the series"
    # Our title has a number but the Hardcover title has none ('Reborn as the Villain's Lackey, Volume 2' vs the first book)
    if pure and all(extract_volume(t) for t in targets) and not theirs and not series_ok: return False, score, f"no volume number on Hardcover (ours: {sorted(ours)})"
    if (m := RE_HC_SET_TITLE.search(book.get('title') or '')) and not any(RE_HC_SET_TITLE.search(t) for t in targets):
        return False, score, f"whole series ('{m.group(0)}')"
    found_text = f"{book.get('title') or ''} {book.get('subtitle') or ''}"
    if (m := RE_HC_SPECIAL.search(found_text)) and not any(RE_HC_SPECIAL.search(t) for t in targets) and not (
            not RE_HC_SPECIAL.search(book.get('title') or '') and (book.get('ratings_count') or 0) >= 100
            and any(normalize_title_text(clean_title(t)) == normalize_title_text(book.get('title') or '') for t in targets)):
        # exception: the label is only in the SUBTITLE of the main book (Hardcover lists 'Harry Potter and the Prisoner of Azkaban'
        # with the subtitle 'Collector's Edition', 1000s of ratings) and the title is exactly ours -> the work itself, not an edition
        return False, score, f"other format ('{m.group(0)}')"
    if book.get('compilation') and not any(RE_HC_SPECIAL.search(t) for t in targets):
        # 'Midnight Tides' is flagged as compilation because Parts 1+2 exist separately, but it sits as ONE book (single position) in its series
        pos = fs.get('position')
        single = isinstance(pos, (int, float)) and float(pos).is_integer()
        if not (score >= 0.99 and single): return False, score, "compilation"
    return True, max(score, 1.0 if series_confirms else 0), ''

def get_hardcover_data(key, targets, authors, asin, isbn, manual_url=None, use_cache=True, abs_series=(), lang=None, abs_title=None):
    """Returns (data, status) with status in 'found', 'not_found', 'blocked', 'disabled'."""
    HC_CTX['lang'] = map_language(lang) if lang else None
    HC_CTX['title'] = abs_title
    if not HARDCOVER_ENABLED: return None, 'disabled'
    if HC_STATE['disabled']:
        logging.info(f"      -> ⏭️ Hardcover skipped ({HC_STATE['disabled']})")
        return None, 'blocked'
    logging.info("      -> Checking hardcover.app")
    while True:
        try:
            d = _hardcover_lookup(key, targets, authors, asin, isbn, manual_url, use_cache, abs_series)
            if d:
                logging.info(f"        ✅ Hardcover via {d['source']}: '{d.get('title')}' (Count: {d['count']}, Rating: {round(safe_float(d.get('val')), 2)}) {d['url']}")
                return d, 'found'
            return None, ('blocked' if HC_STATE['disabled'] else 'not_found')
        except SourceBlockedException as e:
            with IO_LOCK:  # parallel books: a 429 seen by several workers at once is ONE cool-down, not several
                fresh = time.monotonic() - HC_STATE.get('cool_ts', -1e9) > 60
                if fresh and HC_STATE['cooldowns'] < 2: HC_STATE['cooldowns'] += 1; HC_STATE['cool_ts'] = time.monotonic()
                go_on = not fresh or HC_STATE['cooldowns'] <= 2 and HC_STATE.get('cool_ts', 0) > time.monotonic() - 1
            if go_on and not HC_STATE['disabled']:
                wait = max(1, 60 - (time.monotonic() - HC_STATE['cool_ts']))
                logging.warning(f"        ⏸️ Hardcover per-minute limit ({e}). Waiting {wait:.0f}s ({HC_STATE['cooldowns']}/2)...")
                time.sleep(wait); continue
            left = HC_STATE.get('daily_left')
            HC_STATE['disabled'] = f"per-minute limit (HTTP 429){f', daily requests left: {left}' if left is not None else ''}"
            logging.warning(f"        🛑 Hardcover still answers HTTP 429 = the per-MINUTE limit (60/min, shared by everything using this token), "
                            f"not the daily limit{f' ({left} requests left today)' if left is not None else ''}. Skipping Hardcover for the rest of this run.")
            return None, 'blocked'

def _hardcover_lookup(key, targets, authors, asin, isbn, manual_url, use_cache, abs_series):
    LOOKUP_HITS['hardcover'] = 0
    if manual_url and (m := RE_HC_SLUG.search(manual_url)):
        data = hc_query(f"query($s:String!){{ books(where:{{slug:{{_eq:$s}}}}, limit:1){{ {HC_BOOK_FIELDS} }} }}", {'s': m.group(1)})
        if books := (data or {}).get('books'): return hc_result(books[0], 'manual link')
        logging.info(f"        ⚠️ Manual Hardcover link not found: {manual_url}")
    cached = HC_CACHE.get(key) if use_cache else None
    if cached and cached.get('id'):
        book = HC_PREFETCH.get(cached['id'])
        if not book:
            data = hc_query(f"query($id:Int!){{ books(where:{{id:{{_eq:$id}}}}, limit:1){{ {HC_BOOK_FIELDS} }} }}", {'id': cached['id']})
            book = ((data or {}).get('books') or [None])[0]
        if book: return hc_result(book, 'cache')
        logging.info("        ↪️ Cached Hardcover book not found anymore, searching again...")

    # An ID hit still has to fit (ISBNs in ABS are sometimes wrong).
    conds = []
    if asin and re.fullmatch(r'[A-Z0-9]{10}', asin): conds.append({'asin': {'_eq': asin}})
    if i := normalize_isbn(isbn): conds.append({'isbn_13' if len(i) == 13 else 'isbn_10': {'_eq': i}})
    prim = authors[0] if authors else ''
    queries = []
    # Title + author first; title alone finds books whose author spelling or extra contributors confuse the search
    for q in ([f"{targets[0]} {prim}", f"{clean_title(targets[0])} {prim}", targets[0]] if targets else []):
        q = q.strip()
        if q and q not in queries: queries.append(q)
    queries = queries[:3]
    eds = list(dict.fromkeys(clean_title(t) for t in targets[:3] if t and clean_title(t)))[:3]
    # One request per step, stopping at the first hit. (Measured 2026-11-05: Hardcover counts every search field of a request
    # against the minute AND daily limit, so combining steps in one request saves nothing and always pays for all steps.)
    def step(alias, q_single, vars_single, field): return hc_query(q_single, vars_single)
    if conds:
        data = step('ids', f"query($w:editions_bool_exp!){{ editions(where:$w, limit:5){{ title asin isbn_13 isbn_10 book{{ {HC_BOOK_FIELDS} }} }} }}", {'w': {'_or': conds}}, 'editions')
        best = None
        if data is None: LOOKUP_HITS['hardcover'] = None
        elif LOOKUP_HITS['hardcover'] is not None: LOOKUP_HITS['hardcover'] += len(data.get('editions') or [])
        for ed in (data or {}).get('editions') or []:
            book = ed.get('book') or {}
            ok, score, why = hc_acceptable(targets, authors, book, 0.6, abs_series, [ed.get('title')])
            via = 'ASIN' if asin and ed.get('asin') == asin else 'ISBN'
            if not ok: logging.info(f"        ✖ {via} hit '{book.get('title')}' rejected: {why}"); continue
            if not best or (book.get('ratings_count') or 0) > (best[0].get('ratings_count') or 0): best = (book, via)
        if best: return hc_result(best[0], best[1])

    for qi, q in enumerate(queries):
        data = step(f's{qi}', "query($q:String!){ search(query:$q, query_type:\"Book\", per_page:8, page:1){ results } }", {'q': q}, 'search')
        hits = (((data or {}).get('search') or {}).get('results') or {}).get('hits') or []
        if data is None: LOOKUP_HITS['hardcover'] = None
        elif LOOKUP_HITS['hardcover'] is not None: LOOKUP_HITS['hardcover'] += len(hits)
        cands = []
        for h in hits:
            doc = h.get('document') or {}
            try: doc['id'] = int(doc.get('id'))
            except (TypeError, ValueError): continue
            ok, score, why = hc_acceptable(targets, authors, doc, 0.85, abs_series, pure=True)
            if ok: cands.append((score, doc))
            else: logging.debug(f"        ✖ HC '{doc.get('title')}' rejected: {why}")
        if cands:
            # a hit whose MAIN title fits beats hits that fit only via an alternative title (Hardcover lists 'Queen Takes Rose' as an
            # alternative title of the other book 'Queen'); translations are unaffected (there is no main-title hit then)
            main_fit = [(s_, d) for s_, d in cands if hc_title_score(targets, {**d, 'alternative_titles': []}, (), True) >= 0.85]
            if main_fit and len(main_fit) < len(cands): cands = main_fit
            top = max(s for s, _ in cands)
            # Several near-equal titles (duplicates on Hardcover): take the one with the most ratings
            score, doc = max((c for c in cands if c[0] >= top - 0.05), key=lambda c: (c[1].get('ratings_count') or 0, c[0]))
            logging.info(f"        🔍 Hardcover search '{q}': '{doc.get('title')}' accepted (score {score:.2f}, {len(hits)} hits)")
            return hc_result(doc, 'search')
        logging.info(f"        🔍 Hardcover search '{q}': no matching hit ({len(hits)} hits)")
    if eds:
        data = step('ted', f"query($t:[String!]){{ editions(where:{{title:{{_in:$t}}}}, limit:10){{ title book{{ {HC_BOOK_FIELDS} }} }} }}", {'t': eds}, 'editions')
        if data is None: LOOKUP_HITS['hardcover'] = None
        elif LOOKUP_HITS['hardcover'] is not None: LOOKUP_HITS['hardcover'] += len(data.get('editions') or [])
        best = None
        for ed in (data or {}).get('editions') or []:
            book = ed.get('book') or {}
            if not book.get('id'): continue
            ok, score, why = hc_acceptable(targets, authors, book, 0.85, abs_series, [ed.get('title')], pure=True)
            if not ok: logging.debug(f"        ✖ HC edition '{ed.get('title')}' -> '{book.get('title')}' rejected: {why}"); continue
            if not best or (book.get('ratings_count') or 0) > (best[0].get('ratings_count') or 0): best = (book, ed.get('title'))
        if best:
            d = hc_result(best[0], 'edition title')
            d['alts'] = list(dict.fromkeys([*d['alts'], best[1]]))  # the edition title IS a title of this book (cross-check)
            logging.info(f"        🔍 Hardcover edition '{best[1]}' -> book '{best[0].get('title')}' accepted")
            return d
    return None

def hc_audio_asin(hc, abs_dur, authors, targets, narrators, lang):
    """Second-source path: Hardcover lists the Audible ASIN (+ audio length) of the audio edition. A candidate is taken only if the
    Audible product itself confirms author, runtime (+-10 min) and title, and the narrator does not contradict.
    Returns (asin, evidence, product) or None."""
    if not (hc and hc.get('id') and abs_dur): return None
    data = hc_query("query($id:Int!){ editions(where:{book_id:{_eq:$id}, asin:{_is_null:false}, audio_seconds:{_gt:0}}, limit:10){ asin title audio_seconds } }",
                    {'id': hc['id']})
    cands = [e for e in ((data or {}).get('editions') or []) if e.get('asin') and abs(abs_dur - (e.get('audio_seconds') or 0)) <= 600]
    for e in cands[:3]:
        asin = e['asin']
        if ASIN_OWNERS.get(asin): continue
        p = get_audible_data(asin, lang)
        if not p or not p.get('title_raw'): continue
        auth_ok = match_author(authors, ', '.join(p.get('authors_raw') or []))
        dur_ok = bool(p.get('runtime_sec') and abs(abs_dur - p['runtime_sec']) <= 600)
        t_abs = max([search_match_score(t, p['title_raw'], p.get('subtitle_raw')) for t in targets] or [0])
        t_hc = search_match_score(hc.get('title') or '', p['title_raw'], p.get('subtitle_raw'))
        hc_solid = hc.get('source') in ('ISBN', 'ASIN', 'manual link')
        norm_n = lambda n: re.sub(r'[^a-z]', '', fold_accents(n).lower())
        a_narr, c_narr = narr_keys(narrators), narr_keys(p.get('narrators_raw'))
        narr_contra = bool(a_narr and c_narr and not a_narr & c_narr)
        logging.info(f"        🔗 Hardcover edition ASIN {asin} ('{p['title_raw']}'): author {'✓' if auth_ok else '✗'}, runtime {'✓' if dur_ok else '✗'}, "
                     f"title vs ABS {t_abs:.2f}, vs Hardcover {t_hc:.2f}{' (ISBN-confirmed)' if hc_solid else ''}{', narrator differs' if narr_contra else ''}")
        if auth_ok and dur_ok and not narr_contra and (t_abs >= 0.85 or (hc_solid and t_hc >= 0.85)):
            return asin, ['hardcover-edition-asin', 'author', 'runtime', 'title' if t_abs >= 0.85 else 'title-via-hardcover-isbn'], p
    return None

def hc_replacement_isbn(hc, asin, abs_dur, abs_lang="English"):
    """ISBN-13 of a PRINT edition of the (verified) Hardcover book, in the SAME language as the book (user decision 2026-11-04: the ISBN
    field holds the normal book's ISBN; the audiobook is identified by its ASIN). Audio editions (with an ASIN or an audio length) are
    skipped; no Polish ISBN for an English audiobook. Returns (isbn, how) or None."""
    data = hc_query("query($id:Int!){ editions(where:{book_id:{_eq:$id}, isbn_13:{_is_null:false}}, limit:25){ isbn_13 asin audio_seconds language{ language } } }", {'id': hc['id']})
    eds = [(normalize_isbn(e.get('isbn_13')), e) for e in ((data or {}).get('editions') or [])]
    eds = [(i, e) for i, e in eds if i and isbn_checksum_ok(i) and not e.get('asin') and not e.get('audio_seconds')]
    same_lang = [(i, e) for i, e in eds if str(((e.get('language') or {}).get('language')) or '').lower() == str(abs_lang or '').lower()
                 and isbn_group_fits(i, abs_lang)]  # Hardcover's edition language is sometimes wrong (a Russian 978-5 'English' edition)
    return (same_lang[0][0], 'print edition of the same book (same language)') if same_lang else None

ISBN_GROUPS = {'english': ('9780', '9781', '9798'), 'german': ('9783',), 'french': ('9782',), 'spanish': ('97884',), 'italian': ('97888',),
               'dutch': ('97890', '97894'), 'japanese': ('9784',)}

def isbn_group_fits(isbn13, lang):
    """The ISBN registration group must belong to the book language (978-0/1 and 979-8 English, 978-3 German, ...); unknown language: no check."""
    groups = ISBN_GROUPS.get(str(lang or '').lower())
    return not groups or str(isbn13 or '').startswith(groups)

def fold_accents_s(t):
    return ''.join(c for c in unicodedata.normalize('NFKD', t or '') if not unicodedata.combining(c))

def _plain_text(html_text):
    t = re.sub(r'<[^>]+>', ' ', html_lib.unescape(html_text or ''))
    return re.sub(r'\s+', ' ', t).strip()

def _sum_norm(t):
    return re.sub(r'[^a-z0-9]+', ' ', re.sub(r'<[^>]+>', ' ', html_lib.unescape(t or '')).lower()).strip()

def description_of_other_book(own, asin, title):
    """The ABS description (without our block) is Audible's summary of ANOTHER book (round 3, 2026-11-09: 86 Vol. 4 had Vol. 5's text,
    Tom Sawyer had Iron Gold's): it differs from this book's own summary, matches another product's (>= 0.75, 6-word phrases shared)
    and does not name this book's title (a publisher template like 'The audiobook of The Light Fantastic is read by...' does).
    Returns that book's title or None."""
    n = _sum_norm(own)
    if len(n) < 120 or not asin: return None
    mine = (SUMMARY_INDEX.get(asin) or [None, ''])[1]
    if mine and difflib.SequenceMatcher(None, n[:400], mine[:400]).ratio() >= 0.3: return None
    nt = normalize_title_text(re.split(r'\s*[:(]', title or '')[0])
    sh = set(zip(*[n[:800].split()[i:] for i in range(6)]))
    best = None
    for a2, (t2, s2) in list(SUMMARY_INDEX.items()):
        if a2 == asin or not s2: continue
        if len(sh & set(zip(*[s2[:800].split()[i:] for i in range(6)]))) < 4: continue
        r = difflib.SequenceMatcher(None, n[:400], s2[:400]).ratio()
        own_named = len(nt) >= 6 and f' {nt} ' in f' {n} ' and f' {nt} ' not in f' {normalize_title_text(t2)} '  # 'The audiobook of The Light Fantastic...'
        if r >= 0.75 and not own_named and (not mine or difflib.SequenceMatcher(None, mine[:400], s2[:400]).ratio() < 0.5) and (best is None or r > best[0]): best = (r, t2)
    return best[1] if best else None

def description_replaceable(own, audible):
    """Is the ABS description (without our rating block) provably not the user's own text, so Audible's complete summary may
    replace it? (a) it is a truncated beginning of Audible's text, or (b) very short (< 30 % of Audible's and < 300 characters).
    Returns the reason or None. A long own text is never replaced."""
    a, b = _plain_text(own), _plain_text(audible)
    if not a or not b or len(b) <= len(a): return None
    core_a = re.sub(r'(\.\.\.|…|\[more\]|read more)$', '', a, flags=re.I).strip()
    if len(core_a) >= 40 and b.lower().startswith(core_a.lower()[:max(40, len(core_a) - 3)]): return f"ABS text is a truncated beginning ({len(a)} of {len(b)} characters)"
    if len(a) < 300 and len(a) < 0.3 * len(b): return f"ABS text very short ({len(a)} vs {len(b)} characters)"
    return None

def normalize_desc(d):
    """Compare descriptions independent of how ABS stores them ('<br />' vs '<br>', '&amp;' vs '&')."""
    d = html_lib.unescape(d or '')
    return re.sub(r'\s+', ' ', re.sub(r'<br\s*/?>', '<br>', d)).strip()

def strip_rating_block(desc):
    """Description text without our rating block (used to detect 'empty' descriptions)."""
    d = RE_RATING_BLOCK.sub('', desc or '')
    return re.sub(r'(?:\s|<br\s*/?>|<p>\s*</p>)+', ' ', d, flags=re.I).strip()

def parse_rating_block(desc):
    """Split our rating block into {source: 'Audible (n):<br>line<br>line'} (used to keep old values)."""
    block = (RE_RATING_BLOCK.search(desc or '') or [''])[0]
    out, cur = {}, None
    for line in re.split(r'<br\s*/?>', block):
        line = line.strip()
        if line.startswith(('⭐', '─')): cur = None; continue
        if m := RE_BLOCK_HEAD.match(line): cur = m.group(1); out[cur] = [line]
        elif cur and line: out[cur].append(line)
    return {k: '<br>'.join(v) for k, v in out.items()}

def source_lines(src, d):
    """Rating lines of one source, or None if it has no usable rating."""
    if not d: return None
    L = LABELS.get(RATING_LABELS, LABELS['en'])
    if RATING_MIN_COUNT and int(d.get('count', 0) or 0) < RATING_MIN_COUNT: return None
    if src == 'Audible':
        if int(d.get('count', 0) or 0) <= 0: return None
        lines = [f"Audible ({d.get('count')}):"]
        if v := d.get('overall'): lines.append(f"🏆 {moon_rating(v)} {round(safe_float(v), 1)} / 5 - {L['overall']}")
        if v := d.get('performance'): lines.append(f"🎙️ {moon_rating(v)} {round(safe_float(v), 1)} / 5 - {L['performance']}")
        if v := d.get('story'): lines.append(f"📖 {moon_rating(v)} {round(safe_float(v), 1)} / 5 - {L['story']}")
        return lines
    if src == 'Hardcover' and (not d.get('count') or not is_valid_rating(d.get('val'))): return None
    lines = [f"{src} ({d.get('count', 0)}):"]
    if (v := d.get('val')) and is_valid_rating(v): lines.append(f"🏆 {moon_rating(v)} {round(safe_float(v), 1)} / 5 - {L['rating']}")
    return lines

def build_description(current_desc, fresh, old):
    """fresh: {source: data from this run}, old: {source: text from the existing block} (kept if nothing new).
    Sources appear in RATING_ORDER."""
    lines = [f"{BLOCK_RULE} {LABELS.get(RATING_LABELS, LABELS['en'])['head']} {BLOCK_RULE}"]
    for src in RATING_ORDER:
        if new := source_lines(src, fresh.get(src)): lines += new
        elif old.get(src):
            stats['recycled'] += 1; lines.append(old[src])

    clean_d = RE_RATING_BLOCK.sub('', current_desc)
    clean_d = re.sub(r'(?s)\*\*Audible\*\*.*?---\s*\n*', '', clean_d)
    clean_d = re.sub(r'^(?:\s|<br\s*/?>)+', '', clean_d, flags=re.I).strip()
    if len(lines) == 1: return clean_d  # No ratings at all: don't write an empty rating block
    lines.append(BLOCK_END)
    if RATING_POSITION == 'bottom':
        clean_d = re.sub(r'(?:\s|<br\s*/?>)+$', '', clean_d, flags=re.I).strip()
        return (clean_d + "<br><br>" if clean_d else "") + "<br>".join(lines)
    return "<br>".join(lines) + "<br>" + clean_d

RESULTS, OVERRIDES = {}, {}  # loaded in main()

class ItemCapture(logging.Handler):
    """Collects the log lines of the book that is being processed (shown in the web UI problem view) - per thread."""
    def __init__(self): super().__init__()
    @property
    def lines(self):
        if not hasattr(_TL, 'item_lines'): _TL.item_lines = []
        return _TL.item_lines
    @lines.setter
    def lines(self, v): _TL.item_lines = v
    def emit(self, record):
        if len(self.lines) < 80: self.lines.append(record.getMessage().strip())
ITEM_LOG = ItemCapture()

def compare_view(title, authors, meta, abs_dur, aud, hc=None):
    """ABS vs Audible vs Hardcover side by side for the web UI (book page): [abs, audible, hardcover] per field (strings)."""
    aud, hc = aud or {}, hc or {}
    hser = '; '.join(f"{n} #{float(p):g}" if isinstance(p, (int, float)) else (f"{n} #{p}" if p else n) for n, p in (hc.get('series') or []) if n)
    mr = aud.get('meta_raw') or {}
    fmt_min = lambda s: f"{round(s / 60)} min" if s else ''
    ser = lambda xs: '; '.join(f"{x.get('name') or x.get('title')}{' #' + str(x.get('sequence') or x.get('part')) if (x.get('sequence') or x.get('part')) else ''}" for x in xs or [] if (x.get('name') or x.get('title')))
    return {
        'Titel': [title or '', aud.get('title_raw') or '', hc.get('title') or ''],
        'Autor': [', '.join(authors or []), ', '.join(aud.get('authors_raw') or []), ', '.join(hc.get('authors') or [])],
        'Sprecher': [', '.join(meta.get('narrators') or []), ', '.join(aud.get('narrators_raw') or []), ''],
        'Laufzeit': [fmt_min(abs_dur), fmt_min(aud.get('runtime_sec')), ''],
        'Reihe': [ser(meta.get('series')), ser(mr.get('series')), hser],
        'Verlag': [meta.get('publisher') or '', (mr.get('publisher') or {}).get('name') or '', ''],
        'Jahr': [str(meta.get('publishedYear') or ''), str(parse_release_year(mr.get('releaseDate')) or ''), ''],
        'Sprache': [meta.get('language') or '', mr.get('language') or '', ''],
        'ISBN': [meta.get('isbn') or '', '', ''],
    } if (aud.get('title_raw') or hc.get('title')) else {}


def set_result(key, lib_id, iid, title, authors, asin, status, reason, extra=None):
    RESULTS[key] = {'lib': lib_id, 'id': iid, 'title': title, 'author': ', '.join(authors or []), 'asin': asin,
                    'status': status, 'reason': reason, 'run': RUN_ID, 'dry_run': DRY_RUN,
                    'ts': datetime.now(timezone.utc).isoformat(timespec='seconds'), 'log': list(ITEM_LOG.lines), **(extra or {})}

def record_attempt(key, history, failed, recheck, complete=False, recheck_days=None):
    with IO_LOCK: return _record_attempt(key, history, failed, recheck, complete, recheck_days)

def _record_attempt(key, history, failed, recheck, complete=False, recheck_days=None):
    """Track the result of one item. Nothing is persisted in DRY_RUN mode.
    - complete: done, next run after REFRESH_DAYS
    - recheck_days: counts as processed (no strike), but re-check early after N days
    - otherwise: strike in 'failed'; after MAX_FAIL_ATTEMPTS it is parked until REFRESH_DAYS"""
    if DRY_RUN:
        logging.info("      -> 🧪 DRY_RUN: history/failed not updated.")
        return
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat(timespec='seconds')  # Always UTC
    recheck.pop(key, None)
    if complete or recheck_days:
        history[key] = now_iso; failed.pop(key, None)
        if recheck_days:
            recheck[key] = (now + timedelta(days=recheck_days)).isoformat(timespec='seconds')
            logging.info(f"      -> 🔁 Re-check scheduled in {recheck_days} day(s).")
    else:
        fails = failed.get(key, 0) + 1
        if fails >= MAX_FAIL_ATTEMPTS:
            logging.info("      -> 🛑 Max attempts reached."); history[key] = now_iso; failed.pop(key, None)
        else:
            failed[key] = fails; logging.warning(f"      -> ❌ Incomplete. Strike {fails}/{MAX_FAIL_ATTEMPTS}")
    rw_json(HISTORY_FILE, history)
    rw_json(FAILED_FILE, failed)
    rw_json(RECHECK_FILE, recheck)
    rw_json(GR_CACHE_FILE, GR_CACHE)
    rw_json(HC_CACHE_FILE, HC_CACHE)
    rw_json(SOURCE_STATE_FILE, SOURCE_STATE)

def item_tags(item):
    """Collect tags from all places ABS may put them (root, media, media.metadata)."""
    media = item.get('media') or {}
    raw = (item.get('tags') or []) + (media.get('tags') or []) + ((media.get('metadata') or {}).get('tags') or [])
    return {t.strip() for t in raw if isinstance(t, str)}

def process_library(lib_id, history, failed, recheck):
    """One library on its own (used by tests). Normal runs use run_libraries()."""
    queue = build_queue(lib_id, history, recheck)
    random.shuffle(queue)
    process_queue(queue[:MAX_BATCH_SIZE], history, failed, recheck)

def run_libraries(history, failed, recheck):
    """Collect the due books of ALL libraries into one pool, shuffle it and process BATCH_SIZE random books."""
    if ONLY_ITEMS:  # single-book mode (web UI fix / single book): exactly these books, due or not
        batch, listed = [], {}
        for lib_id in {k.partition('_')[0] for k in ONLY_ITEMS}:  # duplicate-ASIN guard needs the whole library, also in single-book mode
            try:
                load_series_names(lib_id)
                for it in abs_session.get(f"{ABS_URL}/api/libraries/{lib_id}/items").json()['results']:
                    listed[f"{lib_id}_{it['id']}"] = it  # author for the parallel lanes
                    note_listing_item(lib_id, it)
            except Exception as e: logging.warning(f"Could not load ASIN owners for {lib_id}: {e}")
        for k in ONLY_ITEMS:
            lib, _, iid = k.partition('_')
            batch.append(dict(listed.get(k) or {'id': iid, 'media': {}}, _lib=lib))
        logging.info(f"Single-book mode: {', '.join(ONLY_ITEMS)}")
        return process_queue(batch, history, failed, recheck)
    pool = []
    for lib in LIBRARY_IDS: pool += build_queue(lib, history, recheck)
    random.shuffle(pool)
    # run mode: build-up = the whole library over several runs (new books first); maintenance = new / changed books always + a small
    # random sample of the due ones. auto = build-up until every book was completed once.
    urgent = [it for it in pool if f"{it['_lib']}_{it['id']}" not in history or it.get('_manual_trigger') or it.get('_reverted')]
    rest = [it for it in pool if it not in urgent]
    never = sum(1 for it in pool if f"{it['_lib']}_{it['id']}" not in history and f"{it['_lib']}_{it['id']}" not in failed)
    mode = RUN_MODE if RUN_MODE in ('buildup', 'maintain') else ('buildup' if never or not history else 'maintain')
    if REFRESH_DAYS == 0 and mode == 'maintain':  # 'refresh 0 = one complete pass' (2026-11-08: maintenance capped it to 334 + 30 books)
        mode = 'buildup'
        logging.info("Refresh 0 days = one complete pass: every due book is checked (maintenance sample not used)")
    if mode == 'maintain':
        batch = urgent[:MAX_BATCH_SIZE] + rest[:max(0, min(MAINTAIN_SAMPLE, MAX_BATCH_SIZE - len(urgent)))]
    else:
        batch = (urgent + rest)[:MAX_BATCH_SIZE]
    logging.info(f"Run mode: {mode}{' (auto)' if RUN_MODE not in ('buildup', 'maintain') else ''} - {len(urgent)} new/changed, {len(rest)} other due books")
    per_lib = defaultdict(int)
    for it in batch: per_lib[it['_lib']] += 1
    logging.info(f"Pool: {len(pool)} due books in {len(LIBRARY_IDS)} libraries. This run: {len(batch)} random books "
                 f"({', '.join(f'{l[:8]}: {n}' for l, n in per_lib.items()) or '-'})"
                 + (f", {len(pool) - len(batch)} follow in the next runs." if len(pool) > len(batch) else "."))
    process_queue(batch, history, failed, recheck)

LIB_SERIES_NAMES = {}  # lib -> {lower-case series names} (ABS series list): splits an unnumbered name off the listing's seriesName

def load_series_names(lib_id):
    try:
        r = abs_session.get(f"{ABS_URL}/api/libraries/{lib_id}/series", params={'limit': 5000, 'minified': 1}, timeout=60)
        LIB_SERIES_NAMES[lib_id] = {(x.get('name') or '').strip().lower() for x in r.json().get('results') or [] if x.get('name')}
    except Exception as e: logging.debug(f"series names of {lib_id} not loaded: {e}")

def split_series_name(s, known=None):
    """ABS listing 'seriesName' -> [(name, number)]. The listing joins all series with ', ' - and names may contain commas themselves
    ('Memory, Sorrow & Thorn #3, Das Geheimnis der Großen Schwerter #3'): a part ends at ' #<number>'. An UNNUMBERED name in front
    ('Elfhame, The Folk of the Air #1') is split off when it is a known library series and the glued name is none (2026-11-08: the
    glued name hid that The Cruel Prince and The Stolen Heir both held Folk of the Air #1)."""
    out, cur = [], ''
    for tok in str(s or '').split(', '):
        if cur and known and cur.strip().lower() in known and not any(k.startswith(f"{cur}, {tok}".strip().lower()) for k in known):
            out.append((cur.strip(), '')); cur = ''
        cur = f"{cur}, {tok}" if cur else tok
        if m := re.search(r' #(\S+)$', tok):
            out.append((cur[:len(cur) - len(m.group(0))].strip(), m.group(1))); cur = ''
    if cur.strip(): out.append((cur.strip(), ''))  # unnumbered rest (several unnumbered series cannot be told apart)
    return out

NARR_SPELL = {}  # folded narrator key -> Counter of the spellings in the library (round 4: 'Ramón de Ocampo' / 'Ramon De Ocampo')

def narr_fold(n): return re.sub(r'[^a-z]', '', fold_accents(n or '').lower())

def narrator_canonical(n):
    """The library's most used spelling of this narrator (ties: the one with accents / initials with periods)."""
    c = NARR_SPELL.get(narr_fold(n))
    if not c or len(c) < 2: return n
    return max(c.items(), key=lambda kv: (kv[1], kv[0] != fold_accents(kv[0]), kv[0].count('.'), kv[0]))[0]

def note_listing_item(lib_id, it):
    """Library-wide facts from the listing: ASIN/ISBN owners (duplicate guards), runtimes, and which books share a series number."""
    key, md_ = f"{lib_id}_{it['id']}", (it.get('media') or {}).get('metadata') or {}
    for n_ in [x.strip() for x in str(md_.get('narratorName') or '').split(', ') if x.strip()]:
        with IO_LOCK:
            from collections import Counter as _C
            NARR_SPELL.setdefault(narr_fold(n_), _C())[n_] += 1
    if a := md_.get('asin'): ASIN_OWNERS.setdefault(a, set()).add(key)
    KEY_ASIN[key] = md_.get('asin'); KEY_LANG[key] = map_language(md_.get('language')) if md_.get('language') else None
    KEY_DUR[key] = (it.get('media') or {}).get('duration')
    if t_ := normalize_title_text(md_.get('title') or ''): TITLE_OWNERS.setdefault((lib_id, t_), set()).add((key, _first_author(md_.get('authorName'))))
    if i_ := normalize_isbn(md_.get('isbn')): ISBN_OWNERS.setdefault(i_, set()).add(key)
    for sn_, q_ in split_series_name(md_.get('seriesName'), LIB_SERIES_NAMES.get(lib_id)):
        if sn_:
            SERIES_BOOKS.setdefault((lib_id, sn_.lower()), []).append({'num': q_, 'title': md_.get('title'), 'asin': md_.get('asin'),
                                                                     'lang': map_language(md_.get('language')), 'author': _first_author(md_.get('authorName')), 'name': sn_,
                                                                     'author_name': str(md_.get('authorName') or '').split(',')[0].strip()})
            f_ = SERIES_FACTS.setdefault((lib_id, sn_.lower()), {'n': 0, 'authors': set(), 'folders': set(), 'name': sn_, 'langs': {}})
            f_['n'] += 1; f_['authors'] |= {_first_author(md_.get('authorName'))}
            if q_: f_['numbered'] = f_.get('numbered', 0) + 1
            if (l_ := map_language(md_.get('language'))) and str(md_.get('language')).lower() not in ('und', 'mul', 'zxx'):
                f_['langs'][l_] = f_['langs'].get(l_, 0) + 1
            pp_ = str(it.get('relPath') or '').split('/')
            if len(pp_) >= 3: f_['folders'].add(pp_[1].strip().lower())
        if sn_ and q_:
            # title + ASIN: two volumes with the SAME title ('Reborn to Master the Blade' 3 and 4, both #4 in ABS) are two books
            SEQ_TITLES.setdefault((lib_id, sn_.lower(), q_.strip()), set()).add(normalize_title_text(md_.get('title') or '') + ('|' + md_['asin'] if md_.get('asin') else ''))
            SEQ_WORKS.setdefault((lib_id, sn_.lower(), q_.strip(), map_language(md_.get('language')) or ''), set()).add(normalize_title_text(md_.get('title') or ''))

ITEM_PATHS_FILE = os.path.join(SCRIPT_DIR, 'item_paths.json')  # key -> [path, title, author] of the last run (moved-item guard)
_GUARD_STOP = {'the', 'and', 'der', 'die', 'das', 'und', 'von', 'book', 'band', 'teil', 'vol', 'volume', 'part', 'des', 'les'}

def _guard_toks(x):
    x = unicodedata.normalize('NFKD', str(x or '')).encode('ascii', 'ignore').decode().lower()
    return {t for t in re.sub(r'[^a-z0-9]+', ' ', re.sub(r"['`\u2019]", '', x)).split() if len(t) > 2 and t not in _GUARD_STOP}

def guard_moved_items(lib_id, items):
    """ABS took over the entry of ANOTHER book for a newly added folder (2026-11-10: 'The Ambassador's Mission' added -> ABS moved the
    'The Orb of Cairado' entry, with all its metadata, onto the new folder; the Orb folder had no entry any more). Cause in ABS 2.x: the
    folder watcher matches a NEW folder to an existing entry by inode only (LibraryScanner findLibraryItemByItemToItemInoMatch) without
    checking that the entry's own folder is gone, and Unraid's /mnt/user (shfs) inode numbers are not stable, so a stale stored inode
    can equal the new folder's. Guard: an entry whose path changed since the last run while NEITHER its title NOR its author appears in
    the new path is no move - it is a takeover. It is not processed (its metadata belongs to the other book), goes on the review list,
    and a library scan gives the other book's folder its entry back. Real moves (renamed folder, reorganized author folder) keep a word
    of the title or the author in the path and pass."""
    old = rw_json(ITEM_PATHS_FILE) or {}
    if not isinstance(old, dict): old = {}
    snap, keep, suspects = dict(old), [], []
    for it in items:
        key, path = f"{lib_id}_{it.get('id')}", it.get('path') or ''
        md = ((it.get('media') or {}).get('metadata') or {})
        title, author = md.get('title') or '', md.get('authorName') or ''
        prev = old.get(key)
        if prev and path and prev[0] != path and not prev[3:4] == [True]:
            ptoks = _guard_toks(path)
            ttoks, atoks = _guard_toks(prev[1] or title), _guard_toks(prev[2] or author)
            if (ttoks or atoks) and not (ttoks & ptoks) and not (atoks & ptoks):
                suspects.append((key, title, prev[0], path))
                snap[key] = [prev[0], prev[1], prev[2], True]  # stays flagged until the user decides / the entry fits its folder
                continue
        if prev and prev[3:4] == [True]:
            ptoks = _guard_toks(path)
            if not (_guard_toks(title) & ptoks) and not (_guard_toks(author) & ptoks) and not (OVERRIDES.get(key) or {}).get('identity') and \
                    not any(x.get('key') == key and x.get('kind') == 'moved' and x.get('status') not in (None, 'open') for x in (rw_json(REVIEW_FILE) or [])):
                continue  # still the other book's metadata: not processed until fixed
        snap[key] = [path, title, author]
        keep.append(it)
    for key, title, was, now in suspects:
        logging.warning(f"   ⛔ ABS moved the entry '{title}' from '{was}' to the NEW folder '{now}' (inode takeover): not processed, review list")
        add_review(key, title, 'moved', now, f"ABS gave the newly added folder '{now}' the entry (and all metadata) of '{title}' from "
                   f"'{was}'. The metadata belongs to the other book. Set the right ASIN for this folder ('another book') - the other "
                   f"folder gets a new entry from the library scan.", evidence=[was, now])
    if suspects and not DRY_RUN:
        try:
            abs_session.post(f"{ABS_URL}/api/libraries/{lib_id}/scan", timeout=30)
            logging.info("   🔄 Library scan started: the other book's folder gets its entry back")
        except Exception as e: logging.warning(f"   Library scan not started: {e}")
    if not DRY_RUN:
        live = {f"{lib_id}_{it.get('id')}" for it in items}
        snap = {k: v for k, v in snap.items() if not k.startswith(f"{lib_id}_") or k in live}
        rw_json(ITEM_PATHS_FILE, snap)
    return keep

def build_queue(lib_id, history, recheck):
    """Due books of one library (new, manually changed, due for refresh, re-checks). Each item gets '_lib'."""
    logging.info(f"--- Library: {lib_id} ---")
    try:
        r = abs_session.get(f"{ABS_URL}/api/libraries/{lib_id}/items")
        items = r.json()['results']
    except Exception as e: logging.error(f"Lib Error: {e}"); return []
    items = guard_moved_items(lib_id, items)

    # --- NEW QUEUE LOGIC (UTC ONLY) ---
    work_queue = []
    count_new, count_due, count_manual, count_recheck, count_locked, count_rating = 0, 0, 0, 0, 0, 0
    now = datetime.now()
    now_utc = datetime.now(timezone.utc)

    load_series_names(lib_id)
    for it in items: note_listing_item(lib_id, it)
    if items:  # books deleted in ABS leave the web UI's result list (they showed up as 'same ASIN twice', 2026-11-09)
        alive = {f"{lib_id}_{it['id']}" for it in items}
        with IO_LOCK:
            gone = [k for k in RESULTS if k.startswith(lib_id + '_') and k not in alive]
            for k in gone: RESULTS.pop(k, None)
        if gone: logging.info(f"Results of {len(gone)} book(s) no longer in ABS removed from the result list")

    for item in items:
        key = f"{lib_id}_{item['id']}"

        # lock_all items never enter the queue (no batch slot, no sleep)
        if 'lock_all' in item_tags(item):
            count_locked += 1
            r_ = RESULTS.setdefault(key, {'lib': lib_id, 'id': item['id'], 'title': ((item.get('media') or {}).get('metadata') or {}).get('title'),
                                          'status': 'ignored', 'reason': 'lock_all'})
            r_['locks'] = sorted(t for t in item_tags(item) if t.startswith('lock_'))  # lock report (status page)
            continue
        
        if key not in history:
            work_queue.append(item); count_new += 1
            continue
            
        last_run_str = history.get(key)
        try:
            if "T" in last_run_str: 
                 last_run_dt = datetime.fromisoformat(last_run_str)
            else:
                 # Legacy entries (old script) only store the date. Treat them as end of day,
                 # otherwise the script's own edits later that day look like manual updates.
                 last_run_dt = datetime.strptime(last_run_str, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=61)
            
            # Ensure last_run_dt is UTC-aware for comparison
            if last_run_dt.tzinfo is None:
                last_run_dt = last_run_dt.replace(tzinfo=timezone.utc)
        except: last_run_dt = datetime(2000, 1, 1, tzinfo=timezone.utc)

        abs_updated = item.get('updatedAt')
        is_manual_update = False
        
        if abs_updated:
            try:
                # Case A: Integer (Unix Timestamp in ms)
                if isinstance(abs_updated, int):
                    abs_dt = datetime.fromtimestamp(abs_updated / 1000, timezone.utc)
                # Case B: String (ISO 8601) - Usually ends in Z (UTC)
                else:
                    t_str = abs_updated.replace("Z", "+00:00")
                    abs_dt = datetime.fromisoformat(t_str)
                
                # Buffer: Ignore updates made by script itself (last run + 60s; also our later writes: apply, symbol rewrite, library task)
                journal_last(key, 'asin')
                try: own_dt = datetime.fromisoformat(_OWN_TS[key]) if key in _OWN_TS else None
                except ValueError: own_dt = None
                if own_dt and own_dt.tzinfo is None: own_dt = own_dt.replace(tzinfo=timezone.utc)
                if abs_dt > (max(last_run_dt, own_dt or last_run_dt) + timedelta(seconds=60)):
                    is_manual_update = True
            except: 
                pass

        recheck_due = False
        if key in recheck:
            try: recheck_due = now_utc >= datetime.fromisoformat(recheck[key])
            except ValueError: recheck_due = True

        watch = watch_changes(key, item)
        if watch != 'reverted' and block_lost(key, item):
            logging.info(f"  ↩️ '{(item.get('media') or {}).get('metadata', {}).get('title')}': rating block missing in the description (ABS rescan?) - checked again")
            watch = 'reverted'
        if watch == 'reverted':
            item['_recheck'] = True; item['_reverted'] = True
            work_queue.append(item); count_recheck += 1
            continue
        SST.seed(SOURCE_STATE, key, last_run_dt.isoformat(timespec='seconds'), audible_ok=(RESULTS.get(key) or {}).get('status') in ('ok', 'other_edition'),
                 gr_known=key in GR_CACHE, hc_known=key in HC_CACHE)  # first run with source_state.json: take over what is known
        if is_manual_update:
            item['_manual_trigger'] = True
            work_queue.append(item); count_manual += 1
        elif (now - last_run_dt.replace(tzinfo=None)).days >= REFRESH_DAYS:
            work_queue.append(item); count_due += 1
        elif recheck_due:
            item['_recheck'] = True
            work_queue.append(item); count_recheck += 1
        elif due := [s_ for s_ in SST.due_sources(SOURCE_STATE, key, enabled_sources(), REFRESH_DAYS) if s_ != 'audible']:
            item['_rating_only'] = due  # Audible is fresh: only the missing rating sources are asked
            work_queue.append(item); count_rating += 1

    count_fresh = len(items) - count_locked - len(work_queue)
    logging.info(f"Due: {count_new} New, {count_manual} Manual Updates, {count_due} Due, {count_recheck} Re-checks, {count_rating} only missing ratings ({count_locked} locked via lock_all). "
                 f"{count_fresh} books are up to date (refreshed within the last {REFRESH_DAYS} days).")
    stats['skipped'] += count_locked
    for it in work_queue: it['_lib'] = lib_id
    return work_queue

def gr_book_series(meta, aud):
    """[(series name, number)] of this book from ABS and from the Audible product (for the Goodreads series match)."""
    out = [(x.get('name'), x.get('sequence')) for x in (meta.get('series') or []) if x.get('name') and x.get('sequence')]
    for cs in ((aud or {}).get('meta_raw') or {}).get('series') or []:
        n = cs.get('name') or cs.get('title')
        m_ = re.search(r'\d+(?:\.\d+)?', str(cs.get('part') or cs.get('sequence') or ''))
        if n and m_: out.append((n, m_.group(0)))
    return list(dict.fromkeys(out))

def rewrite_block(block, swap):
    """One rating block with swapped symbols (swap: old symbol -> new) and the line frame instead of the old '⭐' frame."""
    # 5 symbols in a row ('🌕🌕🌕🌗🌑'): only those runs are swapped, never a symbol in the user's own text
    tok = '(?:' + '|'.join(re.escape(k) for k in sorted(swap, key=len, reverse=True)) + ')'  # emoji may be 2 code points (❤️)
    # the symbols of one rating line: after the line icon (🏆 🎙️ 📖), before the value - 1-5 of them (sets without an empty one)
    nb = re.sub(f'((?:🏆|🎙️|🎙|📖) )((?:{tok}){{1,5}})(?= \\d)', lambda x: x.group(1) + ''.join(swap[t] for t in re.findall(tok, x.group(2))), block)
    # the old '⭐' frame becomes the line frame (head and end line)
    nb = re.sub(r'^((?:<br\s*/?>\s*)*)⭐\s*((?:Ratings|Bewertungen)[^<]*?)\s*(?=<br)', lambda x: f"{x.group(1)}{BLOCK_RULE} {x.group(2)} {BLOCK_RULE}", nb)
    nb = re.sub(r'(<br\s*/?>\s*)⭐(?=\s*(?:<|$))', lambda x: x.group(1) + BLOCK_END, nb)
    return nb

def rewrite_symbols_pass():
    """'Change all rating symbols' (user 2026-11-09): every rating block gets the symbols of RATING_ICONS - the existing values are kept,
    only full / half / empty symbols are swapped (any known set, also the old 5-star one). Journaled; lock_description respected."""
    new = RATING_SYMBOL_SETS.get(RATING_ICONS, RATING_SYMBOL_SETS['moons'])
    swap = {'⚫': new[2]}  # the dark empty place of the short-lived '⭐✨⚫' set
    for st in RATING_SYMBOL_SETS.values():
        for i, ch in enumerate(st):
            if ch: swap.setdefault(ch, new[i])
    swap.update({ch: new[i] for st in RATING_SYMBOL_SETS.values() for i, ch in enumerate(st) if st == new and ch})
    # the library listing already carries description + tags: one request per library instead of one per book (user 2026-11-09: was ~7 min)
    items = []
    for lib in LIBRARY_IDS:
        try: items += [(lib, it) for it in abs_session.get(f"{ABS_URL}/api/libraries/{lib}/items").json().get('results') or []]
        except Exception as e: logging.error(f"Library {lib}: {e}")
    logging.info(f"Rating symbols -> {RATING_ICONS} ({''.join(new)}) for {len(items)} book(s){' - DRY RUN' if DRY_RUN else ''}")
    from abs_series import expand_locks
    todo, n_locked = [], 0
    for lib, it in items:
        meta = (it.get('media') or {}).get('metadata') or {}
        cur = meta.get('description') or ''
        m = RE_RATING_BLOCK.search(cur)
        if not m: continue
        block = m.group(0)
        nb = rewrite_block(block, swap)
        if nb == block: continue
        if 'lock_description' in expand_locks(item_tags(it)): n_locked += 1; continue
        todo.append((it['id'], f"{lib}_{it['id']}", meta, cur.replace(block, nb)))
    def write(t):
        iid, key, meta, desc = t
        return bool(abs_update(iid, key, meta.get('title'), {'description': desc}, meta, 'rating_symbols'))
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=4) as ex:  # ABS handles a few parallel writes well; each PATCH costs ~0.1-0.2 s on its side
        n_changed = sum(ex.map(write, todo))
    logging.info(f"Rating symbols changed in {n_changed} book(s){' (planned, DRY RUN)' if DRY_RUN else ''}; {n_locked} locked book(s) kept")

def remove_ratings_pass():
    """Remove the rating lines of REMOVE_RATINGS from the rating block of every book (ONLY_ITEMS or all libraries).
    Journaled like every write (undo restores the old description); lock_description respected. The source is paused for the
    book until REFRESH_DAYS, so the next scan does not add it right back (switch the source off to keep it away for good)."""
    # whole library: the listing carries description + tags (one request instead of one per book); single books: the item itself
    items = []
    if ONLY_ITEMS:
        for key in ONLY_ITEMS:
            try: items.append((key, abs_session.get(f"{ABS_URL}/api/items/{key.partition('_')[2]}", params={'expanded': 1}).json()))
            except Exception as e: logging.warning(f"  {key}: {e}")
    else:
        for lib in LIBRARY_IDS:
            try: items += [(f"{lib}_{it['id']}", it) for it in abs_session.get(f"{ABS_URL}/api/libraries/{lib}/items").json().get('results') or []]
            except Exception as e: logging.error(f"Library {lib}: {e}")
    logging.info(f"Remove ratings ({', '.join(sorted(REMOVE_RATINGS))}) from {len(items)} book(s){' - DRY RUN' if DRY_RUN else ''}")
    from abs_series import expand_locks
    todo, n_locked = [], 0
    for key, it in items:
        meta = (it.get('media') or {}).get('metadata') or {}
        cur = meta.get('description') or ''
        old = parse_rating_block(cur)
        gone = [src for src in old if src in REMOVE_RATINGS]
        if not gone: continue
        if 'lock_description' in expand_locks(item_tags(it)):
            n_locked += 1; logging.info(f"  🔒 {meta.get('title')}: description locked, ratings kept"); continue
        todo.append((key, meta, gone, build_description(cur, {}, {k: v for k, v in old.items() if k not in REMOVE_RATINGS})))
    def write(t):
        key, meta, gone, new = t
        if not abs_update(key.partition('_')[2], key, meta.get('title'), {'description': new}, meta, 'ratings_removed'): return False
        logging.info(f"  {'WOULD REMOVE' if DRY_RUN else 'removed'}: {meta.get('title')} - {', '.join(gone)}")
        if not DRY_RUN:
            with IO_LOCK:
                for src in gone: SST.record(SOURCE_STATE, key, src.lower(), 'absent', why='rating removed by you')
        return True
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=4) as ex: n_changed = sum(ex.map(write, todo))
    if not DRY_RUN: rw_json(SOURCE_STATE_FILE, SOURCE_STATE)
    logging.info(f"Ratings removed from {n_changed} book(s){' (planned, DRY RUN)' if DRY_RUN else ''}; {n_locked} locked book(s) kept")

REAPPLY_FIELDS = ('title', 'series', 'authors', 'narrators')
_JOURNAL_LAST = {}
_BLOCK_KEYS = {}  # key -> our last description write had a rating block (filled with _JOURNAL_LAST)
_OWN_TS = {}  # key -> time of our last real write (any task: scan, apply, symbol rewrite...) - not a 'manual update' in the next run
_UNDONE_VAL = {}  # (key, field) -> values of writes YOU undid: never written again (abs_update)


def _undone_ids():
    if '_ids' not in _UNDONE_VAL:
        ids = set()
        try:
            for l in open(os.path.join(SCRIPT_DIR, 'undone.jsonl'), encoding='utf-8'):
                try: ids.add(json.loads(l)['id'])
                except (ValueError, KeyError): pass
        except OSError: pass
        _UNDONE_VAL['_ids'] = ids
    return _UNDONE_VAL['_ids']

def block_lost(key, item):
    """Our rating block is gone from the description (an ABS rescan of the book folder reloads the old description - 86 Vol. 6,
    2026-11-09: block written, files touched, ABS rescanned the item; the description is not in last_written, so nothing noticed it
    until REFRESH_DAYS). Due again unless lock_description."""
    journal_last(key, 'asin')  # loads the journal once
    if not _BLOCK_KEYS.get(key): return False
    md = (item.get('media') or {}).get('metadata') or {}
    if 'description' not in md or RE_RATING_BLOCK.search(md.get('description') or ''): return False
    from abs_series import expand_locks
    return 'lock_description' not in expand_locks(item_tags(item))
def journal_last(key, field):
    """Our last real write of this field (changes.jsonl; undone writes don't count). Read once per run."""
    with IO_LOCK:
        if not _JOURNAL_LAST:
            _JOURNAL_LAST['_'] = True
            try:
                for line in open(CHANGES_FILE, encoding='utf-8'):
                    try: e = json.loads(line)
                    except ValueError: continue
                    if e.get('dry_run'): continue
                    if e.get('ts') and e.get('key') and e['ts'] > _OWN_TS.get(e['key'], ''): _OWN_TS[e['key']] = e['ts']
                    if e.get('field') not in ('description', 'cover', 'tags') and f"{e.get('run')}|{e.get('item')}|{e.get('field')}|{e.get('ts')}" in _undone_ids():
                        _UNDONE_VAL.setdefault((e.get('key'), e['field']), set()).add(listing_value(e['field'], e.get('new')))
                    if e.get('field') == 'description':  # did our last description write carry a rating block? (block_lost)
                        _BLOCK_KEYS[e.get('key')] = 'Infos' in str(e.get('new') or '') and 'undo' not in str(e.get('source') or '')
                    if e.get('field') not in REAPPLY_FIELDS + ('asin',): continue
                    if 'undo' in str(e.get('source') or ''): _JOURNAL_LAST.pop((e.get('key'), e['field']), None); continue
                    _JOURNAL_LAST[(e.get('key'), e['field'])] = e
            except OSError: pass
        return _JOURNAL_LAST.get((key, field))

def reapply_reverted(item, iid, key, title, meta, tags):
    """A field we set (group confirmation, your review decision, a cleaned title) shows exactly its value from BEFORE our change again
    (ABS rescan with an old metadata.json): write our value again - no lock_<field>, the ASIN is still the one we had, and the source
    is not one the normal rules redo anyway (series merge / author normalization run again at the end)."""
    done = {}
    for field in item.get('_reapply') or ():
        e = journal_last(key, field)
        src = str((e or {}).get('source') or '')
        if not e or f'lock_{field}' in tags or not (src == 'group_confirm' or src.startswith('applied:') or (field == 'title' and src == 'audible_meta')):
            continue
        a_e = journal_last(key, 'asin')
        if a_e and str(a_e.get('new') or '') != str(meta.get('asin') or ''): continue  # the ASIN changed since: the normal rules decide
        logging.info(f"      -> ↩️ {field} is back at its value from before our change: '{listing_value(field, e.get('old'))}' -> '{listing_value(field, e.get('new'))}' written again ({src})")
        if abs_update(iid, key, title, {field: e.get('new')}, {field: meta.get(field)}, f'reapply:{src}'):
            done[field] = e.get('new')
    return done

def watch_changes(key, item):
    """Change monitoring (1 listing request per run): a field we wrote now shows its OLD value again (ABS rescan restored a file
    name title, ...) -> 'reverted' (the book is checked again and our value written again); a NEW value that is neither old nor ours
    -> the user changed it: respected for good (manual_fields.json, optional lock_<field> tag). ABS does not record WHO changed a field,
    so doubt counts as the user's change."""
    fp = LAST_WRITTEN.get(key)
    if not fp: return None
    reverted = False
    for field, e in list(fp.items()):
        cur = listing_value(field, listing_field(item, field))
        if field in ('authors', 'narrators', 'series') and listing_field(item, field) is None: continue  # listing without this field
        # list fields: ABS sorts the listing ('A #1, B #2'), we may have written another order -> compare as sets
        lst = (lambda s: ', '.join(sorted(str(s or '').split(', ')))) if field in ('authors', 'narrators', 'series') else (lambda s: s)
        if lst(cur) == lst(e.get('ours')): continue
        # user decision 2026-11-08: old value back OR any other value -> checked again; only lock_<field> tags protect a field.
        # 'ours' becomes the current value, so a value the script then accepts does not trigger a re-check in every run.
        reverted = True
        how = 'is back at its old value' if (e.get('old') and lst(cur) == lst(e.get('old'))) else 'was changed outside the script'
        if how != 'is back at its old value':
            with IO_LOCK: e['ours'] = cur
        elif field in REAPPLY_FIELDS: item.setdefault('_reapply', []).append(field)
        logging.info(f"  ↩️ '{(item.get('media') or {}).get('metadata', {}).get('title')}': {field} {how} - checked again (lock_ tags protect a field)")
    return 'reverted' if reverted else None

def enabled_sources():
    return ['audible'] + (['hardcover'] if HARDCOVER_ENABLED else []) + (['goodreads'] if GOODREADS_ENABLED else [])

def sst_record(key, source, status, hits=None, aud_facts=None):
    """Remember what a source gave for this book (real runs only, like history). A long run of books without a result makes the
    breaker suspect the source: the check book decides whether it is broken (switched off, streak taken back) or fine."""
    outcome = 'found' if status == 'found' else SST.outcome_of(status, hits)
    with IO_LOCK:
        if source in SOURCE_DOWN: return
        suspect = bool(BREAKER and BREAKER.note(key, source, outcome, dict((SOURCE_STATE.get(key) or {}).get(source) or {}) or None))
    if suspect:  # canary outside the lock (network)
        bad = source_canaries(only=source) if source in ('audible', 'hardcover') else 'no check book for this source'
        with IO_LOCK:
            if source in SOURCE_DOWN: return
            if bad: source_down(source, BREAKER.trip(SOURCE_STATE if not DRY_RUN else {}, source, bad)); return
            BREAKER.clear(source)
        logging.info(f"        ℹ️ {source.capitalize()}: many books in a row without a result, but the check book is fine - carrying on")
    if DRY_RUN: return
    with IO_LOCK:
        e = SST.record(SOURCE_STATE, key, source, outcome)
        if aud_facts: SOURCE_STATE[key]['aud'] = SST.audible_facts(aud_facts)
    if e and e.get('st') == 'paused' and status != 'found':
        logging.info(f"        ⏸️ {source.capitalize()}: {e.get('why')} - paused for {REFRESH_DAYS} days")

SERIES_MEMBERS_FILE = os.path.join(SCRIPT_DIR, 'series_members.json')  # {host|series asin: {'ts', 'members'}}, 7 days

def series_members_full(host, sasin, cache):
    """Audible series list with number, title, release date and language of every volume (cached 7 days). None = unknown."""
    ck = f"v2|{host}|{sasin}"
    c = cache.get(ck)
    try:
        if c and datetime.now(timezone.utc) - datetime.fromisoformat(c['ts']) < timedelta(days=7): return c['members']
    except (KeyError, ValueError): pass
    hdr = {"User-Agent": FIXED_USER_AGENT, "Accept": "application/json"}
    throttle('audible_api', host)
    try:
        r = requests.get(f"https://{host}/1.0/catalog/products/{sasin}", params={"response_groups": "relationships"}, headers=hdr, timeout=15)
        if r.status_code == 404: cache[ck] = {'ts': datetime.now(timezone.utc).isoformat(timespec='seconds'), 'members': []}; return []
        rel = (r.json().get('product') or {}).get('relationships') if r.status_code == 200 else None
    except (requests.RequestException, ValueError): rel = None
    if rel is None: return None
    kids = [(x['asin'], str(x.get('sequence') or '').strip()) for x in rel if x.get('relationship_to_product') == 'child' and x.get('asin')]
    info = {}
    for i in range(0, len(kids), 50):
        throttle('audible_api', host)
        try:
            r = requests.get(f"https://{host}/1.0/catalog/products", params={"asins": ",".join(a for a, _ in kids[i:i + 50]), "response_groups": "product_desc,product_attrs"},
                             headers=hdr, timeout=20)
            prods = (r.json().get('products') or []) if r.status_code == 200 else None
        except (requests.RequestException, ValueError): prods = None
        if prods is None: return None
        for p in prods: info[p.get('asin')] = p
    members = [{'asin': a, 'num': q, 'title': (info.get(a) or {}).get('title') or '', 'release': (info.get(a) or {}).get('release_date') or '',
                'language': map_language((info.get(a) or {}).get('language'))} for a, q in kids]
    cache[ck] = {'ts': datetime.now(timezone.utc).isoformat(timespec='seconds'), 'members': members}
    return members

def missing_in_series_task():
    """User request 2026-11-09: missing volumes of the library's series (only real gaps, see missing_series.py) -> reports/missing_in_series.json."""
    from abs_series import series_key as _k, series_variant as _v
    cache = rw_json(SERIES_MEMBERS_FILE) or {}
    prev = rw_json(os.path.join(REPORT_DIR, 'missing_in_series.json')) or {}
    prev_rel = {m.get('asin') for x in prev.get('series') or [] for m in x.get('missing') or [] if not m.get('upcoming')}
    prev_up = {m.get('asin') for x in prev.get('series') or [] for m in x.get('missing') or [] if m.get('upcoming')}
    first_report = not prev.get('series')
    try: new_since = (datetime.strptime(prev.get('generated', '')[:10], '%Y-%m-%d') - timedelta(days=14)).strftime('%Y-%m-%d')
    except ValueError: new_since = None
    if not SERIES_BOOKS:  # series-only scan: the per-book part did not load the listing
        for lib in LIBRARY_IDS:
            try:
                load_series_names(lib)
                for it in abs_session.get(f"{ABS_URL}/api/libraries/{lib}/items").json()['results']: note_listing_item(lib, it)
            except Exception as e: logging.warning(f"Missing volumes: library {lib} not loaded ({e})")
    by_key = {}
    for t, asins in list(SERIES_ASINS.items()): by_key.setdefault(_k(t), set()).update(asins)
    hosts = {'German': ['api.audible.de'], 'English': ['api.audible.com', 'api.audible.co.uk']}
    out, asked, checked = [], 0, []
    for (lib, _), books in sorted(SERIES_BOOKS.items()):
        name = books[0]['name']
        langs = [b['lang'] for b in books if b.get('lang')]
        lang = max(set(langs), key=langs.count) if langs else None
        sasins = by_key.get(_k(name)) or set()
        if not sasins or lang not in hosts or (BOOK_LANGUAGES and lang not in BOOK_LANGUAGES): continue
        members = None
        for h in hosts[lang]:
            for sa in sorted(sasins):
                m = series_members_full(h, sa, cache); asked += 1
                if m: members, store = m, h.replace('api.', 'www.'); break
            if members: break
        if not members: continue
        checked.append(name)  # Audible knows this series: the series page can show 'complete'
        miss = MS.compare(books, members, _v(name), lang, MISSING_NOVELLAS)
        # a volume the library has OUTSIDE this series (other series name, no series, other ASIN) is no gap: ASIN or title anywhere
        miss = [m for m in miss if not ASIN_OWNERS.get(m.get('asin')) and not TITLE_OWNERS.get((lib, normalize_title_text(m.get('title') or '')))]
        for m in miss:
            # 'new' = NEWLY RELEASED since the last report: announced there, or released after it (14 days slack for late listings).
            # Only on the list for the first time (a series checked for the first time, a better match rule) is no news (user 2026-11-09).
            if m['upcoming'] or first_report or m.get('asin') in prev_rel: continue
            if m.get('asin') in prev_up or (new_since and (m.get('release') or '')[:10] >= new_since):
                m['new'] = True; NEW_VOLUMES.append(f"{name} #{m['num']}")
        if miss: out.append({'lib': lib, 'series': name, 'author': books[0].get('author_name') or books[0].get('author'), 'language': lang, 'have': len(books), 'store': store, 'missing': miss})
    rw_json(SERIES_MEMBERS_FILE, cache)
    rw_json(os.path.join(REPORT_DIR, 'missing_in_series.json'), {'generated': datetime.now().strftime('%Y-%m-%d %H:%M'), 'series': out, 'checked': sorted(set(checked))})
    logging.info(f"📚 Missing volumes: {sum(len(x['missing']) for x in out)} in {len(out)} series (series lists checked: {asked})")

def source_down(source, why):
    """Switch a source off for the rest of the run (failed check / tripped breaker): no misses, no writes from it, alert in the summary."""
    SOURCE_DOWN[source] = why
    logging.error(f"🚨 {source.capitalize()} switched off for this run: {why}")
    if source == 'hardcover': HC_STATE['disabled'] = f"source check: {why}"
    elif source == 'goodreads': GR_STATE['blocked'] = True

def source_canaries(only=None):
    """One known book per source must come back with every field the rules need (source_health.py). Before the per-book part
    (only=None: failed sources are switched off) and when the breaker suspects a source (only=<source>: returns the problem or '')."""
    if not SOURCE_CANARY or not USE_AUDIBLE_API: return '' if only else None
    hc_search = (lambda q: hc_query("query($q:String!){ search(query:$q, query_type:\"Book\", per_page:8, page:1){ results } }", {'q': q})) \
        if HARDCOVER_ENABLED and not HC_STATE['disabled'] else None
    down = SH.run_canaries(get_audible_api, hc_search, log=logging.warning, only=only)
    if only: return down.get(only, '')
    for s_, why in down.items(): source_down(s_, why)
    logging.info(f"Source check: {'all OK' if not down else 'PROBLEM - ' + '; '.join(f'{k}: {v}' for k, v in down.items())}")

def source_note(key, source):
    e = (SOURCE_STATE.get(key) or {}).get(source) or {}
    return f"{'found' if e.get('st') == 'found' else e.get('why') or e.get('st')} on {str(e.get('ts', ''))[:10]}, next check after {REFRESH_DAYS} days"

def rating_only_pass(item, key, idx, total):
    """Audible is fresh for this book, only some rating sources are due (user rule): no Audible request, no metadata change -
    just the missing ratings, added to the existing rating block. Returns True if a source was asked."""
    due = item['_rating_only']
    iid = item['id']
    item_data = abs_session.get(f"{ABS_URL}/api/items/{iid}", params={"expanded": 1}).json()
    from abs_series import expand_locks
    tags = expand_locks(item_tags(item_data))
    meta = item_data['media']['metadata']
    title = meta.get('title')
    if 'lock_all' in tags: stats['skipped'] += 1; return False
    authors = [a.get('name') if isinstance(a, dict) else a for a in meta.get('authors', [])]
    ov = OVERRIDES.get(key) or {}
    aud = (SOURCE_STATE.get(key) or {}).get('aud') or {}
    ITEM_LOG.lines = []
    logging.info("-" * 50)
    logging.info(f"({idx+1}/{total}) {title} [ASIN: {meta.get('asin')}] [Only missing ratings: {', '.join(due)}]")
    stats['processed'] += 1; stats['rating_only'] += 1
    hc_data = gr_data = None
    if 'hardcover' in due:
        targets = hc_target_titles(aud, title, title, meta.get('subtitle'))
        hc_data, hc_status = get_hardcover_data(key, targets, list(dict.fromkeys([*authors, *(aud.get('authors_raw') or [])])), meta.get('asin'), meta.get('isbn'),
                                                ov.get('hardcover'), use_cache=True, abs_series=[(x.get('name'), x.get('sequence')) for x in (meta.get('series') or [])], lang=meta.get('language'), abs_title=title)
        if hc_data and aud.get('title_raw'):  # the same cross-check as in a full pass, against the remembered Audible product
            cc_ok, cc_why = hc_audible_crosscheck(hc_data, aud)
            if not cc_ok:
                logging.info(f"        ⚠️ Hardcover '{hc_data.get('title')}' does not look like Audible '{aud.get('title_raw')}' ({cc_why}): Hardcover ratings are NOT used (review list)")
                add_review(key, title, 'hardcover', hc_data.get('url'), f"Hardcover book differs from the Audible product ({cc_why})")
                hc_data, LOOKUP_HITS['hardcover'] = None, None
            else:
                logging.info(f"        ✓ Hardcover cross-check against Audible: {cc_why}")
                hc_learn_roles(hc_data)
        elif hc_data:  # nothing to cross-check against: not used
            logging.info("        ⚠️ No remembered Audible product to cross-check the Hardcover hit: not used (next full check)")
            hc_data, LOOKUP_HITS['hardcover'] = None, None
        if HARDCOVER_ENABLED: stats['hc_found' if hc_data else 'hc_missing'] += 1
        sst_record(key, 'hardcover', 'found' if hc_data else hc_status, LOOKUP_HITS['hardcover'])
        if hc_data and hc_data.get('id') and not DRY_RUN: HC_CACHE[key] = {'id': hc_data['id'], 'slug': hc_data.get('slug')}
    if 'goodreads' in due:
        hc_gids = (hc_data or {}).get('gr_ids') or (hc_fetch_gr_ids(hc_data['id']) if hc_data and hc_data.get('id') and not GR_STATE['blocked'] else [])
        GR_CTX['series'] = gr_book_series(meta, aud)
        gr_data, gr_status = get_goodreads_data(meta.get('isbn'), aud.get('title_raw') or title, authors, authors[0] if authors else "",
                                                ov.get('goodreads') or GR_CACHE.get(key), hc_gids)
        if not gr_data and gr_status not in ('deferred', 'blocked', 'disabled'): stats['gr_missing'] += 1
        sst_record(key, 'goodreads', gr_status, LOOKUP_HITS['goodreads'])
        if gr_data and gr_data.get('url') and not DRY_RUN: GR_CACHE[key] = gr_data['url']
    if (hc_data or gr_data) and 'lock_description' not in tags:
        cur = meta.get('description') or ''
        final = build_description(cur, {'Goodreads': gr_data, 'Hardcover': hc_data}, parse_rating_block(cur))  # Audible line stays as it is
        if normalize_desc(final) != normalize_desc(cur) and abs_update(iid, key, title, {'description': final}, meta, 'ratings'):
            logging.info(f"      -> ✅ {'WOULD UPDATE (DRY_RUN)' if DRY_RUN else 'SUCCESS'}: ratings block (+{', '.join(n for n, d in (('Goodreads', gr_data), ('Hardcover', hc_data)) if d)})")
            stats['success'] += 1
        else: logging.info("      -> ✅ Description already up to date")
    elif not (hc_data or gr_data): logging.info("      -> ℹ️ No new rating found")
    if r := RESULTS.get(key):
        if gr_data: r['goodreads'] = gr_data.get('url')
        if hc_data: r['hardcover'] = hc_data.get('url')
    if not DRY_RUN:
        rw_json(SOURCE_STATE_FILE, SOURCE_STATE); rw_json(GR_CACHE_FILE, GR_CACHE); rw_json(HC_CACHE_FILE, HC_CACHE)
    return True

def process_queue(work_queue, history, failed, recheck):
    total = len(work_queue)
    if HARDCOVER_ENABLED: hc_prefetch([f"{it['_lib']}_{it['id']}" for it in work_queue])
    if USE_AUDIBLE_API and 'audible' not in SOURCE_DOWN: aud_prefetch([it for it in work_queue if not it.get('_rating_only')])
    start = datetime.now()
    consecutive_rl = 0

    def one(idx, item):
        nonlocal consecutive_rl
        if stats['aborted_ratelimit'] or 'audible' in SOURCE_DOWN: return
        lib_id = item['_lib']
        
        eta_str = eta_text(idx, total) if not LOG_ROUTER or WORKERS <= 1 else '…'  # parallel: the log router writes the real ETA when the book is finished
        
        search_penalty = False
        needs_sleep = False  # Only pause after items that actually hit Audible/Goodreads
        ITEM_LOG.lines = []
        LOOKUP_HITS['audible'] = 0  # products returned by the Audible searches of THIS book (negative cache)

        while True: 
            try:
                iid, key = item['id'], f"{lib_id}_{item['id']}"
                if item.get('_rating_only') and not ONLY_ITEMS:
                    needs_sleep = rating_only_pass(item, key, idx, total)
                    break
                r_item = abs_session.get(f"{ABS_URL}/api/items/{iid}", params={"expanded": 1})  # expanded: includes media.duration (also in single-book mode)
                if r_item.status_code == 404:  # deleted in ABS (single-book mode / a stale web UI entry): no error, it leaves the result list
                    logging.info(f"      -> 🗑️ Book is no longer in ABS - skipped")
                    with IO_LOCK: RESULTS.pop(key, None)
                    break
                item_data = r_item.json()
                from abs_series import expand_locks
                tags = expand_locks(item_tags(item_data))
                meta = item_data['media']['metadata']
                title = meta.get('title')

                if 'lock_all' in tags:
                    logging.info(f"({idx+1}/{total}) 🔒 Skipping '{title}' (lock_all tag found)")
                    stats['skipped'] += 1
                    break

                asin, lang = meta.get('asin'), meta.get('language')
                authors = [a.get('name') if isinstance(a, dict) else a for a in meta.get('authors', [])]
                ov = OVERRIDES.get(key) or {}
                if ov.get('asin') and ov['asin'] != asin:
                    # Manual ASIN from the web UI: written to ABS and protected like lock_asin
                    logging.info(f"      -> ✋ Manual ASIN {ov['asin']} (was {asin})")
                    abs_update(iid, key, title, {'asin': ov['asin']}, {'asin': asin}, 'manual')
                    asin = ov['asin']
                if ov.get('asin'): tags = tags | {'lock_asin'}
                
                logging.info(f"-"*50)
                needs_sleep = True

                reason_str = " [Reason: 🛠️ Manual Update]" if item.get('_manual_trigger') else (" [Reason: 🔁 Re-check]" if item.get('_recheck') else "")
                if item.get('_recheck'): stats['recheck'] += 1
                if item.get('_manual_trigger'): stats['manual_retrigger'] += 1

                logging.info(f"({idx+1}/{total}) [ETA: {eta_str}] {title} [ASIN: {asin}]{reason_str} (Try {failed.get(key,0)+1}/{MAX_FAIL_ATTEMPTS})")
                stats['processed'] += 1
                if item.get('_reapply') and (done_ := reapply_reverted(item, iid, key, title, meta, tags)):
                    meta.update(done_); title = meta.get('title')
                messy = parse_messy_title(title, [s_.get('name') for s_ in (meta.get('series') or [])])  # a one-word series prefix counts when it IS the ABS series ('Heist 01 Heist')
                search_title = messy['title'] if messy else title
                if messy: logging.info(f"      -> 🧹 Messy title detected ({messy['kind']}), searching as '{search_title}'")
                if not messy and authors and meta.get('series') and normalize_title_text(search_title) in {normalize_title_text(a_) for a_ in authors}:
                    # ABS title = author name (folder name): the real title is in the series field ('Jules Verne' / series '20.000 Meilen unter dem Meer')
                    logging.info(f"      -> 🧹 Title '{search_title}' is the author's name; searching with the series name '{meta['series'][0].get('name')}' instead")
                    search_title = meta['series'][0].get('name') or search_title
                asin_from_search = False
                folder_fix = None  # set by folder_volume_conflict() below
                narr_override = None  # narrators from the book folder when ABS narrator + ASIN belong to another recording
                skip_meta = False
                abs_dur = (item_data.get('media') or {}).get('duration') or (item.get('media') or {}).get('duration')
                src_due = set((SST.SOURCES if (item.get('_manual_trigger') or ONLY_ITEMS) else SST.due_sources(SOURCE_STATE, key, list(SST.SOURCES), REFRESH_DAYS)))

                asin_hint_note = None
                if not asin and 'lock_asin' not in tags and isbn_bad_reason(meta.get('isbn')) == 'value is an ASIN, not an ISBN':
                    hint = re.sub(r'[\s-]', '', str(meta.get('isbn'))).upper()
                    hd = get_audible_data(hint, lang) if not (ASIN_OWNERS.get(hint, set()) - {key}) else None
                    if hd and hd.get('title_raw'):
                        hv, hwhy = asin_plausibility(search_title, authors, hd['title_raw'], hd.get('subtitle_raw'), hd.get('authors_raw'),
                                                     meta.get('narrators') or [], hd.get('narrators_raw'), abs_dur, hd.get('runtime_sec'))
                        if hv in ('ok', 'asin_ok_metadata_differs') and not runtime_mismatch(abs_dur, hd.get('runtime_sec')):
                            logging.info(f"      -> 💡 ASIN found in the ISBN field: {hint} = '{hd['title_raw']}' ({', '.join(hwhy)}, runtime ok)")
                            if abs_update(iid, key, title, {'asin': hint}, {'asin': None}, 'isbn_field_asin'):
                                logging.info(f"        💾 ASIN {'would be set (DRY_RUN)' if DRY_RUN else 'set in ABS'}.")
                            ASIN_OWNERS.setdefault(hint, set()).add(key); asin = hint
                        else: logging.info(f"      -> ℹ️ ISBN field holds {hint} but it does not fit this book ({hv}, {', '.join(hwhy)}): ignored")
                    else: logging.info(f"      -> ℹ️ ISBN field holds {hint}, which is no Audible product: ignored")
                # User rule 2026-11-08: English books -> audible.com, German books -> audible.de FIRST (one request). An ASIN that is
                # not there makes the script search that store first; the other stores are asked only if that search finds nothing.
                pref_store = PREFERRED_STORE.get(map_language(lang)) if (lang and asin and USE_AUDIBLE_API) else None
                pref_miss, pref_res = False, None
                if pref_store:
                    pref_res = get_audible_api(asin, pref_store)
                    if isinstance(pref_res, dict):
                        logging.info(f"      -> Audible API: found on {pref_store}")
                    elif pref_res == 'missing' and 'lock_asin' not in tags:
                        pref_miss = True
                        logging.info(f"      -> Audible API: {asin} is not on {pref_store} (preferred store for {map_language(lang)} books): searching there first")
                aud_data = pref_res if isinstance(pref_res, dict) else (None if pref_miss else get_audible_data(asin, lang))

                if aud_data and aud_data.get('meta_raw'):
                    raw_lang = aud_data['meta_raw'].get('language')
                    if raw_lang: new_lang_detected = map_language(raw_lang)
                    else: new_lang_detected = None
                else: new_lang_detected = None
                
                should_search = False
                check_lang = new_lang_detected if new_lang_detected else lang

                # Language check: ABS and Audible disagree -> who is right?
                lang_conflict, lang_target, strict_search, search_only = False, None, False, None
                abs_lang = map_language(lang) if lang and str(lang).strip().lower() not in ('und', 'undetermined', 'mul', 'zxx', 'unknown') and book_lang_allowed(lang) else None
                if aud_data and new_lang_detected and not book_lang_allowed(new_lang_detected):
                    # the product behind the ASIN is in a language the library does not have (Malice: the French recording): another edition
                    lang_conflict_pre = True
                    logging.info(f"      -> ⚠️ Audible product is {new_lang_detected}, not one of your book languages ({', '.join(BOOK_LANGUAGES)}): "
                                 f"searching the edition in your languages, nothing is taken from it")
                    new_lang_detected = None
                else: lang_conflict_pre = False
                if lang_conflict_pre:
                    lang_conflict, lang_target = True, abs_lang
                if aud_data and new_lang_detected and abs_lang and abs_lang != new_lang_detected:
                    verdict = language_verdict(search_title, meta.get('narrators') or [], aud_data, 'lock_language' in tags,
                                               [x.get('name') for x in (meta.get('series') or [])])
                    if verdict == 'unclear':
                        skip_meta = True
                        logging.info(f"      -> ⚠️ Language mismatch: ABS {abs_lang} vs Audible {new_lang_detected}, and no narrator/series to decide which is right. "
                                     f"Nothing is changed except the ratings.")
                    elif verdict == 'asin_wrong':
                        lang_conflict, lang_target = True, abs_lang
                        check_lang = abs_lang
                        logging.info(f"      -> ⚠️ Language mismatch: ABS {abs_lang} vs Audible {new_lang_detected} for '{aud_data.get('title_raw')}'. "
                                     f"{'Language is locked' if 'lock_language' in tags else 'Title/narrator point to another edition'}: searching the {abs_lang} edition...")
                    else:
                        logging.info(f"      -> ℹ️ Language mismatch: ABS {abs_lang} vs Audible {new_lang_detected}. Title and narrator/series match Audible: ABS language is corrected.")

                # Deterministic plausibility check (no LLM): does the product behind the ASIN look like this book at all?
                plaus, plaus_why = "ok", []
                if asin and aud_data and aud_data.get("title_raw"):
                    plaus, plaus_why = asin_plausibility(search_title, authors, aud_data["title_raw"], aud_data.get("subtitle_raw"), aud_data.get("authors_raw"),
                                                         meta.get("narrators") or [], aud_data.get("narrators_raw"), abs_dur, aud_data.get("runtime_sec"))
                    if plaus == "asin_ok_metadata_differs":
                        logging.info(f"      -> ℹ️ ASIN is right (same narrator and runtime) but ABS title/authors differ from Audible '{aud_data.get('title_raw')}' ({', '.join(plaus_why)}): proposal in the review list")
                        add_review(key, title, "title", aud_data.get("title_raw"), "ASIN confirmed by runtime and narrator; ABS title/author differ", plaus_why,
                                   ctx={'iid': iid, 'cand_asin': asin, 'aud_title': aud_data.get('title_raw'), 'abs_series': [list(p) for p in _series_pairs(meta.get('series'))], 'tags': sorted(tags), 'lang': lang})
                folder_fix = folder_volume_conflict(item_data, key, title, meta) if 'lock_asin' not in tags else None
                if not folder_fix and not asin and 'lock_asin' not in tags and (tfix_ := tag_volume_conflict(item_data, title, meta)):
                    logging.info(f"      -> 🏷️ The audio file's own tags name '{tfix_['title']}', not '{title}': searching with the tag title (runtime must match to ±2 min)")
                    folder_fix = dict(tfix_, num=tfix_['num'] or next(iter(sorted(extract_volume(tfix_['title']))), None))
                if folder_fix and folder_fix.get('num') is None: folder_fix = None
                if folder_fix:
                    logging.info(f"      -> 📁 Title '{title}' is shared by several library books and the book folder names '{folder_fix['title']}' "
                                 f"(#{folder_fix['num']}): ABS holds another volume's data, searching with the folder title (runtime must match to ±2 min)")
                    search_title = folder_fix['title']
                    should_search, strict_search = True, True
                elif not asin:
                    logging.info("      -> ⚠️ No ASIN in ABS.")
                    should_search = True
                elif pref_miss:
                    should_search, strict_search, lang_target, search_only = True, True, map_language(lang), pref_store
                elif aud_data is None:
                    logging.info("      -> ⚠️ ASIN not found in any marketplace.")
                    should_search, strict_search = True, True
                elif lang_conflict:
                    should_search = True
                elif plaus == "wrong":
                    logging.info(f"      -> ⛔ ASIN {asin} belongs to another book: Audible '{aud_data.get('title_raw')}' ({', '.join(plaus_why)}). Searching the right one...")
                    should_search = True  # not strict: ABS narrators of such a book were filled from the wrong product and must not veto the right one
                elif aud_data.get('title_raw') and volume_conflict(title, aud_data.get('title_raw'), aud_data.get('subtitle_raw')) and not same_recording(abs_dur, meta.get('narrators'), aud_data):
                    logging.info(f"      -> ⚠️ Volume mismatch: ABS '{title}' vs Audible '{aud_data.get('title_raw')}'. ASIN probably belongs to another volume, searching...")
                    should_search = True
                elif runtime_mismatch(abs_dur, aud_data.get('runtime_sec')):
                    logging.info(f"      -> ⚠️ Runtime mismatch: ABS {format_time(abs_dur)} vs Audible {format_time(aud_data['runtime_sec'])}. ASIN probably points to another edition, searching...")
                    should_search = True
                elif (((fnarr_ := folder_narrators(item_data)) or meta.get('narrators')) and aud_data.get('narrators_raw') and abs_dur and aud_data.get('runtime_sec')
                      and abs(abs_dur - aud_data['runtime_sec']) > 600 and not ({re.sub(r'[^a-z]', '', fold_accents(n).lower()) for n in (fnarr_ or meta['narrators']) if n}
                                                                              & {re.sub(r'[^a-z]', '', fold_accents(n).lower()) for n in aud_data['narrators_raw'] if n})
                      and not any(same_person_name(n, a) for n in (fnarr_ or meta.get('narrators') or []) for a in authors)):
                    # the ASIN is ANOTHER RECORDING (Fool's Errand: James Langton 1518 min on .com, the file is Nick Taylor 1389 min = .co.uk):
                    # strict search for exactly this recording in every store; the current ASIN stays if there is none
                    logging.info(f"      -> ⚠️ ASIN is another recording (narrator {', '.join(aud_data['narrators_raw'][:2])}, runtime {abs(abs_dur - aud_data['runtime_sec']) // 60:.0f} min off): searching this recording...")
                    should_search, strict_search = True, True
                    if fnarr_: narr_override = fnarr_  # the folder names the recording's narrator (ABS narrator may have come with the wrong ASIN)
                elif int(aud_data.get('count', 0)) == 0 and aud_data.get('page_ok') and str((aud_data.get('meta_raw') or {}).get('releaseDate') or '')[:10] > datetime.now().strftime('%Y-%m-%d'):
                    # a re-release that is not out yet (Dune B0HQ4V4BGY, 2026-11-24, library 2026-11-09): the released edition has the ratings.
                    # Strict search: the same recording would replace the ASIN, else the other edition's ratings are used (nothing else changes)
                    logging.info(f"      -> 🆕 ASIN is not released yet ({aud_data['meta_raw']['releaseDate']}): looking for the released edition...")
                    should_search, strict_search = True, True
                elif int(aud_data.get('count', 0)) == 0 and aud_data.get('page_ok'):
                    logging.info(f"      -> 🆕 Product page exists on {aud_data.get('domain')}, but no ratings yet. No replacement search, re-check in {NO_RATINGS_RECHECK_DAYS} days.")
                elif int(aud_data.get('count', 0)) == 0:
                    logging.info("      -> ⚠️ Found 0 Ratings.")
                    should_search, strict_search = True, True
                elif (pref := PREFERRED_STORE.get(map_language(check_lang))) and aud_data.get('domain') != pref:
                    logging.info(f"      -> ℹ️ ASIN is from {aud_data.get('domain')}, preferred store for {map_language(check_lang)} books is {pref}. "
                                 f"Looking for the same edition there (the current ASIN is kept if there is no exact match)...")
                    should_search, strict_search, lang_target, search_only = True, True, map_language(check_lang), pref

                if should_search and asin and 'lock_asin' in tags:
                    logging.info(f"      -> 🔒 ASIN search skipped (lock_asin), keeping {asin}")
                    should_search = False  # the checks below still skip metadata if Audible doesn't fit

                if should_search:
                    search_penalty = True 
                    found = None
                    target_is_german = str(check_lang).lower() in GERMAN_LANG_CODES
                    
                    if target_is_german:
                        if aud_data and aud_data.get('variant_asin_de'):
                             found = aud_data['variant_asin_de']
                             logging.info(f"        🔗 Found ASIN via HTML Link (hreflang='de-de'): {found}")
                    else:
                        if aud_data and aud_data.get('variant_asin_us'):
                             found = aud_data['variant_asin_us']
                             logging.info(f"        🔗 Found ASIN via HTML Link (hreflang='en-us'): {found}")

                    LAST_MATCH.clear()
                    SEARCH_CTX.clear()
                    if not narr_override and (fn_ := folder_narrators(item_data)) and meta.get('narrators') and not (
                            {re.sub(r'[^a-z]', '', fold_accents(n).lower()) for n in fn_ if n} & {re.sub(r'[^a-z]', '', fold_accents(n).lower()) for n in meta['narrators'] if n}):
                        # folder and ABS name different narrators: search for the folder's recording, runtime to 2 min (Fool's Errand flip 2026-11-09)
                        narr_override = fn_; SEARCH_CTX_need_exact = True
                    else: SEARCH_CTX_need_exact = False
                    search_narr = narr_override or ([] if plaus == "wrong" else (meta.get("narrators") or []))
                    SEARCH_CTX['dup_claim'] = bool(asin and (ASIN_OWNERS.get(asin, set()) - {key}))  # another ABS book has the same ASIN
                    SEARCH_CTX['pub'], SEARCH_CTX['year'] = meta.get('publisher'), meta.get('publishedYear')
                    SEARCH_CTX['key'] = key
                    if folder_fix or SEARCH_CTX_need_exact: SEARCH_CTX['need_exact_rt'] = True
                    SEARCH_CTX['want_vol'] = extract_volume(search_title)  # the volume we are looking for (twin check)
                    if asin and aud_data and aud_data.get('title_raw'):
                        SEARCH_CTX.update({'old_title': aud_data['title_raw'], 'old_authors': ', '.join(aud_data.get('authors_raw') or [])})
                    srch_series = [dict(x, sequence=None) for x in (meta.get('series') or [])] if folder_fix else search_series(lib_id, meta.get("series"))  # ABS number belongs to another volume
                    if not found: found = find_missing_asin(search_title, authors, abs_dur, lang, raw_title=None if folder_fix else title,
                                                            required_lang=lang_target, strict=strict_search and bool(asin),
                                                            only_domain=search_only, narrators=search_narr, abs_series=srch_series)
                    if not found and pref_miss:
                        logging.info(f"        ℹ️ No exact match on {pref_store}: asking the other stores for {asin} (fallback)")
                        aud_data = get_audible_data(asin, lang, skip=(pref_store,))
                        wide, wide_strict = aud_data is None, True
                        if aud_data and aud_data.get('title_raw'):  # the same checks as for any found product
                            plaus, plaus_why = asin_plausibility(search_title, authors, aud_data['title_raw'], aud_data.get('subtitle_raw'), aud_data.get('authors_raw'),
                                                                 meta.get('narrators') or [], aud_data.get('narrators_raw'), abs_dur, aud_data.get('runtime_sec'))
                            if plaus == 'wrong':
                                logging.info(f"      -> ⛔ ASIN {asin} belongs to another book: Audible '{aud_data.get('title_raw')}' ({', '.join(plaus_why)}). Searching all stores...")
                                wide, wide_strict = True, False
                            elif runtime_mismatch(abs_dur, aud_data.get('runtime_sec')):
                                logging.info(f"      -> ⚠️ Runtime mismatch: ABS {format_time(abs_dur)} vs Audible {format_time(aud_data['runtime_sec'])}. Searching all stores...")
                                wide = True
                        if not wide:
                            search_only = aud_data.get('domain')  # the current ASIN stays (another store); no wider search
                            # the language check of the normal path, for the product found in the other store (Fractal Noise: ABS German,
                            # the file is Jennifer Hale's English recording = this ASIN on .com -> the ABS language is wrong, 2026-11-09)
                            fb_lang = map_language((aud_data.get('meta_raw') or {}).get('language'))
                            if fb_lang and abs_lang and fb_lang != abs_lang and book_lang_allowed(fb_lang) and 'lock_language' not in tags \
                                    and same_recording(abs_dur, meta.get('narrators'), aud_data):  # runtime 2 min + narrator: stronger than a (translated) title
                                new_lang_detected = fb_lang
                                logging.info(f"      -> ℹ️ Language mismatch: ABS {abs_lang} vs Audible {fb_lang}, same recording (runtime and narrator): ABS language is corrected.")
                        else:
                            if aud_data is None: logging.info("      -> ⚠️ ASIN not found in any marketplace: searching all stores")
                            search_only, lang_target = None, None
                            LAST_MATCH.clear()
                            found = find_missing_asin(search_title, authors, abs_dur, lang, raw_title=title, required_lang=None, strict=wide_strict,
                                                      only_domain=None, narrators=[] if plaus == 'wrong' else search_narr,
                                                      abs_series=search_series(lib_id, meta.get("series")))
                    weak_ = LAST_SEARCH.get('weak')
                    if not found and USE_AUDIBLE_API and meta.get('series') and abs_dur and not search_only and not folder_fix:
                        LAST_MATCH.clear()
                        found = find_missing_asin_via_series(search_title, authors, abs_dur, lang, raw_title=title, required_lang=lang_target,
                                                             strict=strict_search and bool(asin), narrators=search_narr,
                                                             abs_series=search_series(lib_id, meta.get('series')))
                    
                    if not found and weak_ and weak_.get('series_ok') and 'lock_asin' not in tags and not (ASIN_OWNERS.get(weak_['asin'], set()) - {key}):
                        found = weak_['asin']
                        logging.info(f"        ✅ Taken: '{weak_['title']}' [{found}] - same author, narrator, runtime (±2 min), series and number; only the title differs")
                        LAST_MATCH.clear(); LAST_MATCH.update({'asin': found, 'confirmed': True, 'evidence': ['author', 'narrator', 'runtime', 'series-number'], 'title': weak_['title'],
                                                               'domain': weak_.get('domain'), 'runtime': weak_.get('runtime_sec'), 'narrators': weak_.get('narrators') or [],
                                                               'authors': weak_.get('authors') or [], 'pub_year': False, 'count': 0, 'lang': weak_.get('lang')})
                    if not found and weak_ and 'lock_asin' not in tags:
                        add_review(key, title, 'asin', weak_['asin'], f"runtime (to the minute) and narrator match '{weak_['title']}'; title similarity {weak_.get('title_score')}, author {'matches' if weak_.get('author_ok') else 'differs'} - not enough for an automatic change",
                                   ctx={'iid': iid, 'cand_asin': weak_['asin'], 'aud_title': weak_.get('title'), 'abs_series': [list(p) for p in _series_pairs(meta.get('series'))], 'tags': sorted(tags), 'lang': lang,
                                        'cand': weak_.get('cand'), 'abs': {'title': title, 'authors': authors, 'narrators': meta.get('narrators') or [],
                                                                         'runtime_min': round((abs_dur or 0) / 60), 'series': [f"{x.get('name')} #{x.get('sequence')}" if x.get('sequence') else x.get('name') for x in (meta.get('series') or [])]}})
                    if found and found != asin and (ASIN_OWNERS.get(found, set()) - {key}):
                        logging.info(f"        ⛔ ASIN {found} already belongs to another book in the library/run: not assigned (needs a manual look)")
                        add_review(key, title, 'asin', found, 'ASIN already belongs to another book')
                        found = None
                    if found and found != asin and LAST_MATCH.get('asin') == found and not LAST_MATCH.get('confirmed'):
                        logging.info(f"        🧐 ASIN {found} ('{LAST_MATCH.get('title')}') has only weak evidence ({', '.join(LAST_MATCH.get('evidence') or [])}): "
                                     f"not written, added to the review list")
                        add_review(key, title, 'asin', found, 'weak evidence for an ASIN change', LAST_MATCH.get('evidence'))
                        stats['review'] = stats.get('review', 0) + 1
                        found = None
                    if found and found != asin and LAST_MATCH.get('asin') == found and 'lock_asin' not in tags:
                        tw = preferred_twin(lang_target or lang, key)  # same recording, better listing (preferred store / ABS publisher+year)
                        if tw:
                            logging.info(f"        🔁 Better listing of the same recording: {tw[0]} ({tw[1]})")
                            found = tw[0]
                    if found:
                        if found != asin:
                            logging.info(f"        ✨ NEW ASIN Found: {found}")
                            ASIN_OWNERS.setdefault(found, set()).add(key)
                            if abs_update(iid, key, title, {'asin': found}, {'asin': asin}, 'asin_search'):
                                logging.info(f"        💾 ASIN {'would be updated (DRY_RUN)' if DRY_RUN else 'updated in ABS'}.")
                            asin, stats['asin_found'] = found, stats['asin_found'] + 1
                            stats['asin_migrated'] += 1
                            asin_from_search = True
                            if lang_target: lang = lang_target
                            elif not target_is_german: lang = "English" 
                            aud_data = get_audible_data(asin, lang)
                            if aud_data and aud_data.get('meta_raw'):
                                raw_l = aud_data['meta_raw'].get('language')
                                if raw_l: new_lang_detected = map_language(raw_l)
                        else:
                            logging.info(f"        ℹ️ Search returned same ASIN. Keeping fallback data.")
                            if folder_fix and LAST_MATCH.get('asin') == found and LAST_MATCH.get('confirmed'):
                                folder_fix['same'] = True  # the folder title found THIS ASIN again (runtime ±2 min): title/number from it below
                    elif search_only: logging.info(f"        ℹ️ No exact match on {pref_store if pref_miss else search_only}. Keeping {asin} from {aud_data.get('domain') if aud_data else '?'}.")
                    else: logging.info(f"        ℹ️ No replacement found. Keeping fallback data.")
                    if plaus == "wrong" and not (found and found != asin):  # nothing taken from a product that is another book
                        add_review(key, title, "asin", asin, "ASIN belongs to another book and no safe replacement was found", plaus_why)
                        aud_data, skip_meta = None, True
                    rel = LAST_SEARCH.get('related')
                    if rel and not (max(title_similarity(search_title, rel.get('title_raw') or ''), title_similarity(title, rel.get('title_raw') or ''),
                                        search_match_score(title, rel.get('title_raw') or '', rel.get('subtitle_raw'))) >= 0.8
                                    and match_author(authors, ', '.join(rel.get('authors_raw') or []))):  # safety net: never the ratings of ANOTHER book
                        logging.warning(f"        ⛔ Other-edition candidate '{rel.get('title_raw')}' does not fit '{title}' (title/author): not used")
                        rel = None
                    if rel and not book_lang_allowed((rel.get('meta_raw') or {}).get('language')): rel = None
                    if not found and rel and not search_only and (not aud_data or int(aud_data.get('count', 0)) == 0 or lang_conflict_pre):
                        aud_data = dict(rel, related_edition=True)
                        skip_meta = True
                        logging.info(f"        📎 Using ratings of another edition: '{rel.get('title_raw')}' [{rel.get('asin')}] on {rel.get('domain')} "
                                     f"(runtime {format_time(rel.get('runtime_sec') or 0)} vs ABS {format_time(abs_dur or 0)}). ASIN and metadata are not changed.")
                        stats['other_edition'] += 1

                if lang_conflict_pre and aud_data and not aud_data.get('related_edition') and not book_lang_allowed((aud_data.get('meta_raw') or {}).get('language')):
                    aud_data, skip_meta = None, True  # neither metadata nor ratings of a product in another language
                    logging.info(f"      -> ⏭️ No edition in your book languages found: nothing is taken from the {(asin or '')} product (ASIN stays as it is)")
                elif lang_conflict and aud_data and map_language((aud_data.get('meta_raw') or {}).get('language')) not in (None, lang_target):
                    skip_meta = True
                    logging.info(f"      -> ⏭️ Metadata sync skipped: no {lang_target} edition found, ASIN stays as it is. Only ratings are used.")
                elif aud_data and aud_data.get('title_raw') and volume_conflict(title, aud_data.get('title_raw'), aud_data.get('subtitle_raw')) and not same_recording(abs_dur, meta.get('narrators'), aud_data) \
                        and not asin_from_search:
                    skip_meta = True
                    logging.info(f"      -> ⏭️ Metadata sync skipped: volume number still differs from Audible ('{aud_data.get('title_raw')}'). Only ratings are used.")
                elif aud_data and runtime_mismatch(abs_dur, aud_data.get('runtime_sec')):
                    skip_meta = True
                    logging.info(f"      -> ⏭️ Metadata sync skipped: runtime still differs (ABS {format_time(abs_dur)} vs Audible {format_time(aud_data['runtime_sec'])}). Only ratings are used.")

                if aud_data and aud_data.get('meta_raw') and not skip_meta:
                    md_raw = aud_data['meta_raw']
                    abs_updates = {}
                    log_updates = []

                    # Title fix: only for broken titles (messy pattern or ASIN found by search),
                    # and only if Audible's title confirms it is the same book.
                    aud_title, aud_sub = aud_data.get('title_raw'), aud_data.get('subtitle_raw')
                    # 'Camp Half-Blood Confidential (Percy Jackson and the Olympians)': the series belongs in the series field
                    if aud_title and (m := re.match(r'^(.+?)\s*\(([^()]+)\)\s*$', aud_title)):
                        aud_series_names = {(x.get('name') or '').strip().lower() for x in (md_raw.get('series') or [])}
                        if m.group(2).strip().lower() in aud_series_names: aud_title = m.group(1).strip()
                    if folder_fix and (asin_from_search or folder_fix.get('same')) and aud_title and abs_dur and aud_data.get('runtime_sec') and abs(abs_dur - aud_data['runtime_sec']) <= 120:
                        # the ABS title belongs to another volume (folder_volume_conflict) and the new ASIN is the same recording:
                        # Audible's title, or the folder title if Audible's is the same ambiguous one ('Mushoku Tensei')
                        new_t = aud_title if normalize_title_text(aud_title) != normalize_title_text(title) else folder_fix['title']
                        if 'lock_title' in tags: logging.info(f"        🔒 Title Update Skipped (Locked): '{new_t}'")
                        elif new_t != title:
                            abs_updates['title'] = new_t
                            log_updates.append(f"Title: '{title}' -> '{new_t}' (was another volume's title)")
                    elif aud_title and aud_title != title and same_recording(abs_dur, meta.get('narrators'), aud_data) and (
                            re.sub(r'(?i)\s*\((?:unabridged|ungek(?:ü|ue)rzt)\)\s*$', '', title) == aud_title
                            or re.sub(r'\b0+(\d)', r'\1', title) == re.sub(r'\b0+(\d)', r'\1', aud_title) and re.search(r'\b0\d', title) and not re.search(r'\b0\d', aud_title)):
                        # round 5 (2026-11-09): 'Witch King (Unabridged)', 'Ishura, Vol. 05: ...' - Audible's title of the same recording
                        if 'lock_title' in tags: logging.info(f"        🔒 Title Update Skipped (Locked): '{aud_title}'")
                        else:
                            abs_updates['title'] = aud_title
                            log_updates.append(f"Title: '{title}' -> '{aud_title}' (format label / zero-padded number)")
                    elif aud_title and same_recording(abs_dur, meta.get('narrators'), aud_data) and normalize_title_text(aud_title) != normalize_title_text(title) \
                            and any(re.fullmatch(r'(?i)\s*' + re.escape(str(s_.get('name') or '').strip()) + r'\s*#\s*\d+(?:\.\d+)?\s*', title)  # only the '#N' label: 'Beware of Chicken 2' IS the title (Audible adds marketing)
                                    or (normalize_title_text(title) == normalize_title_text(s_.get('name') or '') and normalize_title_text(aud_title).startswith(normalize_title_text(title) + ' ')
                                        and s_.get('sequence') and str(s_.get('sequence')) not in ('1', '1.0'))  # 'A Thousand Li' #12 = 'A Thousand Li: The Fourth Wall'
                                    for s_ in (meta.get('series') or []) if s_.get('name')):
                        # round 3 (2026-11-09): the title is only the series label ('The Dark Tower #1') - Audible's title of the same recording
                        if 'lock_title' in tags: logging.info(f"        🔒 Title Update Skipped (Locked): '{aud_title}'")
                        else:
                            abs_updates['title'] = aud_title
                            log_updates.append(f"Title: '{title}' -> '{aud_title}' (the title was only the series name and number)")
                    elif aud_title and same_recording(abs_dur, meta.get('narrators'), aud_data) and (head_ := series_tail_head(title, [s_.get('name') for s_ in (meta.get('series') or [])], aud_title)):
                        # round 1 (2026-11-09): the book's own series name appended to the title ('Hogfather: Discworld', 'Mark of the Crijik 4:
                        # The Burning Lake: Mark of the Crijik') - the title without it; Audible's own title when the rest is title + subtitle
                        new_t = aud_title if (aud_sub and glued_subtitle(head_, aud_sub, aud_title, aud_sub)) else head_
                        if 'lock_title' in tags: logging.info(f"        🔒 Title Update Skipped (Locked): '{new_t}'")
                        elif new_t != title:
                            abs_updates['title'] = new_t
                            log_updates.append(f"Title: '{title}' -> '{new_t}' (series name appended to the title)")
                            if aud_sub and not meta.get('subtitle') and 'lock_subtitle' not in tags and new_t == aud_title:
                                abs_updates['subtitle'] = aud_sub
                                log_updates.append(f"Subtitle: '' -> '{aud_sub}'")
                    elif aud_title and aud_sub and (glued := glued_subtitle(title, meta.get('subtitle'), aud_title, aud_sub, [s_.get('name') for s_ in (meta.get('series') or [])])):
                        # library 2026-11-09: the subtitle glued into the title ('Sojourn: Legend of Drizzt: Dark Elf Trilogy, Book 3' with that
                        # subtitle, 'Defiance of the Fall 4: A LitRPG Adventure: A LitRPG Adventure') - ABS title = Audible title + subtitle
                        if 'lock_title' in tags: logging.info(f"        🔒 Title Update Skipped (Locked): '{aud_title}'")
                        else:
                            abs_updates['title'] = aud_title
                            log_updates.append(f"Title: '{title}' -> '{aud_title}' (subtitle was glued into the title)")
                            if glued == 'subtitle' and 'lock_subtitle' not in tags:
                                abs_updates['subtitle'] = aud_sub
                                log_updates.append(f"Subtitle: '{meta.get('subtitle') or ''}' -> '{aud_sub}'")
                    elif aud_title and aud_title != title and messy:  # only broken titles are replaced
                        candidates = [search_title]
                        if messy and messy.get('subtitle'): candidates.append(f"{messy['title']}: {messy['subtitle']}")
                        confirmed = asin_from_search or any(title_similarity(c, aud_title) >= 0.8 for c in candidates)
                        if 'lock_title' in tags: logging.info(f"        🔒 Title Update Skipped (Locked): '{aud_title}'")
                        elif confirmed:
                            abs_updates['title'] = aud_title
                            log_updates.append(f"Title: '{title}' -> '{aud_title}'")
                            if aud_sub and not meta.get('subtitle'):
                                abs_updates['subtitle'] = aud_sub
                                log_updates.append(f"Subtitle: -> '{aud_sub}'")

                    from abs_series import foreign_for_book as _ffs
                    if meta.get('subtitle') and 'lock_title' not in tags and 'subtitle' not in abs_updates and _ffs(meta['subtitle'], meta.get('language') or lang) \
                            and not (aud_sub and normalize_title_text(aud_sub) == normalize_title_text(meta['subtitle'])):
                        # round 10: 'Le Livre des Terres Bannies 1' on the English Malice - a subtitle of a foreign edition
                        abs_updates['subtitle'] = aud_sub if (aud_sub and same_recording(abs_dur, meta.get('narrators'), aud_data)) else ''
                        log_updates.append(f"Subtitle: '{meta['subtitle']}' -> '{abs_updates['subtitle']}' (subtitle of a foreign edition)")
                    # round 9 (2026-11-10): subtitle = title ('Catching Fire' / 'Catching Fire') or an edition label ('Library Edition'):
                    # Audible's subtitle of the same recording, else none; ' - Library Edition' at the end of a title goes
                    if 'lock_title' not in tags and same_recording(abs_dur, meta.get('narrators'), aud_data):
                        cur_t, cur_s = abs_updates.get('title', title), abs_updates.get('subtitle', meta.get('subtitle')) or ''
                        new_t = re.sub(r'(?i)\s*[-–:]\s*library edition\s*$', '', cur_t).strip()
                        new_s = re.sub(r'(?i)\s*[-–:]?\s*library edition\s*$', '', cur_s).strip()
                        plain_ = lambda x: re.sub(r'[\W_]+', '', (x or '').lower())  # strict: 'X, Book 6' is NOT the title 'X 6'
                        if new_s and plain_(new_s) == plain_(new_t): new_s = ''
                        if not new_s and cur_s and aud_sub and plain_(aud_sub) != plain_(new_t): new_s = aud_sub
                        if new_t != cur_t and new_t:
                            abs_updates['title'] = new_t; log_updates.append(f"Title: '{cur_t}' -> '{new_t}' (edition label)")
                        if new_s != cur_s and cur_s:
                            abs_updates['subtitle'] = new_s; log_updates.append(f"Subtitle: '{cur_s}' -> '{new_s}' (was the title / an edition label)")

                    # recording details come only from the SAME recording: runtime within EDITION_FILL_MIN (unknown ABS runtime: as before)
                    same_rec_rt = not (abs_dur and aud_data.get('runtime_sec')) or abs(abs_dur - aud_data['runtime_sec']) <= EDITION_FILL_MIN * 60
                    if not same_rec_rt: logging.info(f"        ⏭️ Narrators/publisher/year/abridged not taken: runtime differs by {abs(abs_dur - aud_data['runtime_sec']) // 60:.0f} min (other recording)")
                    new_pub = (md_raw.get('publisher') or {}).get('name') if same_rec_rt else None
                    # Publisher and year are only filled when empty (Audible has the audiobook edition's data)
                    if 'lock_publisher' not in tags:
                        if new_pub and not meta.get('publisher'):
                            abs_updates['publisher'] = new_pub
                            log_updates.append(f"Publisher: '{meta.get('publisher')}' -> '{new_pub}'")
                    elif new_pub and not meta.get('publisher'): logging.info(f"        🔒 Publisher Update Skipped (Locked): '{new_pub}'")

                    if 'lock_year' not in tags:
                        new_year = parse_release_year(md_raw.get('releaseDate')) if same_rec_rt else None
                        cur_y = re.match(r'\s*(\d{4})', str(meta.get('publishedYear') or ''))
                        if new_year and not meta.get('publishedYear'):
                            abs_updates['publishedYear'] = new_year
                            log_updates.append(f"Year: '{meta.get('publishedYear')}' -> '{new_year}'")
                        elif new_year and not re.fullmatch(r'\s*\d{4}\s*', str(meta.get('publishedYear'))):
                            abs_updates['publishedYear'] = new_year  # 'April 27, 2018 (07-16-24)' from file tags is no year
                            log_updates.append(f"Year: '{meta.get('publishedYear')}' -> '{new_year}' (not a year)")
                        elif new_year and cur_y and int(cur_y.group(1)) > datetime.now().year and str(new_year) != cur_y.group(1) and int(str(new_year)[:4]) <= datetime.now().year:
                            # a year in the future cannot be right for a file you already have (Baptism of Fire 2027 = the year of an
                            # unreleased re-release whose ASIN the book had): the same recording's release year (round 1, 2026-11-09)
                            abs_updates['publishedYear'] = new_year
                            log_updates.append(f"Year: '{meta.get('publishedYear')}' -> '{new_year}' (year in the future)")
                    else:
                        new_year = parse_release_year(md_raw.get('releaseDate'))
                        if new_year and not meta.get('publishedYear'): logging.info(f"        🔒 Year Update Skipped (Locked): '{new_year}'")
                    
                    if not new_lang_detected and meta.get('language') and book_lang_allowed(meta['language']):
                        norm = map_language(meta['language'])  # e.g. 'eng' -> 'English' ('spa' in an English/German library stays a visible code)
                        if norm != meta['language']: new_lang_detected = norm
                    if 'lock_language' not in tags:
                        if new_lang_detected and new_lang_detected != meta.get('language'):
                            abs_updates['language'] = new_lang_detected
                            log_updates.append(f"Language: '{meta.get('language')}' -> '{new_lang_detected}'")
                    else:
                        if new_lang_detected and new_lang_detected != meta.get('language'): logging.info(f"        🔒 Language Update Skipped (Locked): '{new_lang_detected}'")
                    
                    fmt = str(md_raw.get('format') or '').lower()
                    if fmt and same_rec_rt:  # Only decide when Audible actually states a format (of the same recording)
                        new_abridged = 'abridged' in fmt and 'unabridged' not in fmt
                        if new_abridged != bool(meta.get('abridged')):
                            if 'lock_abridged' not in tags:
                                abs_updates['abridged'] = new_abridged
                                log_updates.append(f"Abridged: {meta.get('abridged')} -> {new_abridged}")
                            else: logging.info(f"        🔒 Abridged Update Skipped (Locked): {new_abridged}")

                    if 'lock_genres' not in tags:
                        current_genres = meta.get('genres') or []
                        new_genres_list = [g for g in dict.fromkeys(map_genre(c.get('name'), aud_data.get('domain')) for c in (md_raw.get('categories') or []) if c.get('name')) if g]
                        from abs_series import genre_key as _gk
                        have_g = {_gk(c) for c in current_genres}  # 'Science fiction' already there = 'Science Fiction' (no re-add after the genre cleanup)
                        added_genres = [g for g in new_genres_list if _gk(g) not in have_g and not have_g.add(_gk(g))]
                        if added_genres:
                            abs_updates['genres'] = current_genres + added_genres
                            log_updates.append(f"Genres: +{added_genres}")
                    else:
                        current_genres = meta.get('genres') or []
                        new_genres_list = [g for g in dict.fromkeys(map_genre(c.get('name'), aud_data.get('domain')) for c in (md_raw.get('categories') or []) if c.get('name')) if g]
                        from abs_series import genre_key as _gk
                        have_g = {_gk(c) for c in current_genres}  # 'Science fiction' already there = 'Science Fiction' (no re-add after the genre cleanup)
                        added_genres = [g for g in new_genres_list if _gk(g) not in have_g and not have_g.add(_gk(g))]
                        if added_genres: logging.info(f"        🔒 Genre Update Skipped (Locked): +{added_genres}")

                    if 'lock_series' not in tags:
                        # a non-Latin series name hangs on the BOOK, not on the recording: the same title is enough (Spice and Wolf, Solo
                        # Leveling: other recording of the same volume); the language may have been corrected just above ('spa' -> English)
                        nl_book = map_language(abs_updates.get('language') or lang) in ('English', 'German') and (same_rec_rt and bool(abs_dur and aud_data.get('runtime_sec'))
                                   or normalize_title_text(title) == normalize_title_text(aud_data.get('title_raw') or ''))
                        nl_cur = [x for x in (meta.get('series') or []) if x.get('name') and non_latin_script(x['name'])] if nl_book else []
                        if nl_book and not md_raw.get('series'):
                            for x in (meta.get('series') or []):
                                if x.get('name') and non_latin_name(x['name']) and not non_latin_script(x['name']):
                                    logging.info(f"        ⚠️ Series '{x['name']}' does not look English/German and Audible has no series for this book: review list")
                                    add_review(key, title, 'series', x['name'], 'series name does not look English/German; Audible has no series for this book - keep or rename it in ABS')
                        if nl_cur and not md_raw.get('series'):
                            # user rule 2026-11-08: series names are English/German; Audible lists no series for this book -> the foreign one goes
                            keep = [{"name": x.get('name'), "sequence": x.get('sequence')} for x in (meta.get('series') or []) if x.get('name') and x not in nl_cur]
                            logging.info(f"        🔁 Series {', '.join(repr(x['name']) for x in nl_cur)} removed (not English/German, Audible lists no series for this book)")
                            abs_updates['series'] = keep
                            log_updates.append("Series: " + (", ".join(f"'{x['name']}' #{x['sequence']}" for x in keep) or '(none)'))
                        if not md_raw.get('series'):  # Audible lists no series: single-book extras / another author's series name (Elolaivat, Song of Achilles)
                            same_b0 = (same_rec_rt and bool(abs_dur and aud_data.get('runtime_sec'))) or normalize_title_text(title) == normalize_title_text(aud_data.get('title_raw') or '')
                            cur0 = abs_updates.get('series') if 'series' in abs_updates else [{'name': x.get('name'), 'sequence': x.get('sequence')} for x in (meta.get('series') or []) if x.get('name')]
                            cl0, cl0_notes = series_cleanup(lib_id, item_data, cur0, [], authors, set(), same_b0, abs_updates.get('language') or lang)
                            from abs_series import foreign_for_book as _ffb0
                            if same_b0 and (not cl0 or all(non_latin_name(x['name']) or _ffb0(x['name'], abs_updates.get('language') or lang) for x in cl0)):
                                if hit_ := series_by_membership(lib_id, asin, AUDIBLE_API_HOSTS.get(aud_data.get('domain') or 'www.audible.com', 'api.audible.com'), authors, title):
                                    cl0 = [{'name': hit_[0], 'sequence': hit_[1] or None}]
                                    cl0_notes.append(f"-> '{hit_[0]}' #{hit_[1] or '-'} (Audible lists this book in that series' list)")
                            if same_b0 and cl0:  # the book has series: ADD the library series of the author whose Audible list holds it ('Tawny Man')
                                for n_, q_ in series_by_membership(lib_id, asin, AUDIBLE_API_HOSTS.get(aud_data.get('domain') or 'www.audible.com', 'api.audible.com'), authors, title, all_hits=True) or []:
                                    if not any(x['name'].strip().lower() == n_.strip().lower() for x in cl0):
                                        cl0 = cl0 + [{'name': n_, 'sequence': q_}]
                                        cl0_notes.append(f"'{n_}' #{q_} added (Audible lists this book in that series' list)")
                            if cl0 != cur0:
                                for n_ in cl0_notes: logging.info(f"        🧹 Series {n_}")
                                abs_updates['series'] = cl0
                                log_updates.append("Series: " + (", ".join(f"'{x['name']}'" + (f" #{x['sequence']}" if x.get('sequence') else '') for x in cl0) or '(none)'))
                        if (series_list := md_raw.get('series')) and any(not x.get('part') and x.get('asin') for x in series_list):
                            # the product lists a series WITHOUT a number, the series' own list has one for this ASIN (The Spine of the World =
                            # Legend of Drizzt #12, The Magicians' Guild = Black Magician Trilogy #1; audit 2026-11-09)
                            h_ = AUDIBLE_API_HOSTS.get(aud_data.get('domain') or 'www.audible.com')
                            fixed_ = []
                            for x in series_list:
                                if not x.get('part') and x.get('asin') and h_:
                                    q_ = next((q for a_, q, _t in (_series_members(h_, x['asin']) or []) if a_ == asin), '')
                                    if re.fullmatch(r'\d+(\.\d+)?', q_ or ''):
                                        x = dict(x, part=q_); logging.info(f"        🔢 Series '{x.get('name')}' #{q_}: number from Audible's series list")
                                fixed_.append(x)
                            series_list = fixed_
                        if series_list and ((same_rec_rt and bool(abs_dur and aud_data.get('runtime_sec'))) or normalize_title_text(title) == normalize_title_text(aud_data.get('title_raw') or '')):
                            from abs_series import series_key as _skx
                            aud_keys_ = {_skx(x.get('name') or '') for x in series_list}
                            need_ = any(_skx(x.get('name') or '') not in aud_keys_ for x in (meta.get('series') or []))  # ABS has a series Audible does not list
                            sib_ = sibling_series(asin, aud_data.get('domain'), series_list, aud_data.get('title_raw') or title, authors if need_ else None)
                            if sib_:
                                logging.info(f"        ➕ Other recording(s) of this book are also in: {', '.join(x['name'] + (' #' + str(x['part']) if x.get('part') else '') for x in sib_)}")
                                series_list = list(series_list) + sib_
                        if series_list:
                            # ABS series are curated: Audible's series are only taken when the book has none in ABS.
                            # Otherwise only missing numbers of the same series are filled (Audible often lists reading
                            # orders, universes and translations as extra series).
                            cur_list = [{"name": x.get('name'), "sequence": x.get('sequence')} for x in (meta.get('series') or []) if x.get('name')]
                            # User rules 2026-11-08 (same recording only): a series name in a non-Latin script (English/German books)
                            # or a series that is only a folder name of the book path ('Elolaivat', 'Die vergessenen Welten') is
                            # replaced by Audible's series. A folder series only when Audible's series already exists in the library.
                            en_de = map_language(abs_updates.get('language') or lang) in ('English', 'German')
                            ver = same_rec_rt and bool(abs_dur and aud_data.get('runtime_sec'))  # both runtimes known and equal
                            from abs_series import series_key as _skey, series_variant as _svar
                            same = lambda a, b: a == b or (_skey(a) == _skey(b) and _svar(a) == _svar(b))
                            aud_names = [n for s_ in series_list for n in ((s_.get('name') or '').strip(), canonical_series(lib_id, (s_.get('name') or '').strip(), authors)[0]) if n]
                            from abs_series import foreign_for_book as _ffb
                            bad_nl = [x for x in cur_list if nl_book and (non_latin_name(x['name']) or _ffb(x['name'], abs_updates.get('language') or lang))
                                      and (non_latin_script(x['name']) or not any(same(x['name'], n) for n in aud_names))]  # Audible uses this name too: it is the real one
                            bad_fd = [x for x in cur_list if ver and x not in bad_nl and folder_series_name(item_data, x['name'])
                                      and not any(same(x['name'], n) for n in aud_names)]  # a folder named like Audible's series is fine
                            base_list = [x for x in cur_list if x not in bad_nl and x not in bad_fd]
                            new_list = [dict(x) for x in base_list]
                            book_variant = _svar(f"{title} {aud_data.get('title_raw', '')} {aud_data.get('subtitle_raw', '')}")
                            ignored = []
                            for s_obj in series_list:
                                s_name = (s_obj.get('name') or '').strip()
                                if not s_name: continue
                                if _svar(s_name) - book_variant:  # 'The Witcher (Full-Cast Editions)' on a normal edition
                                    ignored.append(f"{s_name} (other edition)"); continue
                                s_seq = None
                                if part_txt := s_obj.get('part'):
                                    if m := re.search(r'(\d+(?:\.\d+)?)', str(part_txt)): s_seq = m.group(1)
                                rng_ = re.match(r'\s*(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)\s*$', str(part_txt or ''))
                                if s_seq is not None and rng_ and (is_boxset_title(title) or re.search(r"(?i)\b(?:pack|omnibus|collection|complete|bundle|books?\s+\d+\s*[-–&,]|box)\b", f"{title} {aud_data.get('title_raw') or ''} {aud_data.get('subtitle_raw') or ''}")):
                                    # user decision 2026-11-09: a pack / omnibus keeps Audible's own range as its number ('1-2' for Publisher's
                                    # Pack 1, '3-4' for Pack 2) - it says exactly which volumes the book holds
                                    s_seq = f"{rng_.group(1)}-{rng_.group(2)}"
                                elif s_seq is not None and (is_boxset_title(title) or is_boxset_title(aud_data.get('title_raw') or '')):  # 'Trilogie 1-3' is not book 5; Audible's TITLE only ('Black Magician Trilogy, Book 3' as subtitle is no box set)
                                    logging.info(f"        ℹ️ Series '{s_name}': no number for a box set / collection ('{title}')")
                                    s_seq = None
                                elif s_seq is None and not is_boxset_title(title):
                                    aud_t, aud_s = aud_data.get('title_raw', ''), aud_data.get('subtitle_raw', '')
                                    text = f"{aud_t} {aud_s}".strip()
                                    pattern = re.escape(s_name) + r'[\s:,-]+(?:Book\s+|Band\s+|Teil\s+|Vol\.?\s*)?(\d+(?:\.\d+)?)'
                                    if text and (m := re.search(pattern, text, re.IGNORECASE)): s_seq = m.group(1)
                                # Audible's own title says 'Vol. 8', the series lists #2 (Mushoku Tensei: Audible counts parts there): if the ABS
                                # title names the same single volume and the series name is part of the title, the title's volume wins
                                if s_seq and not is_boxset_title(title):
                                    # only an EXPLICIT 'Vol./Volume/Book N' (not 'Metro 2033'), N < 1000, and the FULL series name in the title
                                    # ('Threads of Destiny: Volume 1' is Destiny Cycle #4 - 'destiny' alone matched, 2026-11-08)
                                    _xv = lambda t: {strip_lead0(m) for m in re.findall(r'(?i)\b(?:vol(?:ume)?\.?|book|band)\s*(\d{1,3})\b', t or '')}
                                    tv_a, tv_b = _xv(aud_data.get('title_raw')), _xv(abs_updates.get('title') or title)
                                    _nn = lambda t: re.sub(r'[^a-z0-9]+', ' ', (t or '').lower()).strip()
                                    if len(tv_a) == 1 and tv_a == tv_b and s_seq.lstrip('0') not in tv_a and '.' not in s_seq \
                                            and len(_nn(s_name)) >= 8 and _nn(s_name) in _nn(aud_data.get('title_raw')):
                                        logging.info(f"        🔢 Series '{s_name}': Audible lists #{s_seq}, but its title and the ABS title say volume {next(iter(tv_a))}: #{next(iter(tv_a))}")
                                        s_seq = next(iter(tv_a))
                                if folder_fix and s_seq and s_seq != folder_fix['num'] and folder_fix['num'] in extract_volume(aud_data.get('title_raw') or folder_fix['title']):
                                    s_seq = folder_fix['num']  # Audible counts parts ('#2' for Vol. 8): folder and title agree on the volume
                                canon, why = canonical_series(lib_id, s_name, authors, book_variant)
                                existing = next((x for x in new_list if same(x['name'], canon)), None)
                                if existing:
                                    if why: logging.info(f"        🔗 Series '{s_name}' -> '{canon}' ({'alias list' if why == 'alias' else 'existing ABS series'})")
                                    if not existing.get('sequence') and s_seq: existing['sequence'] = s_seq
                                    elif s_seq and existing.get('sequence') and str(existing['sequence']) != str(s_seq) and re.fullmatch(r'0+\d+(?:\.\d+)?', str(existing['sequence']).strip()) \
                                            and str(existing['sequence']).strip().lstrip('0') == str(s_seq).strip():
                                        existing['sequence'] = s_seq  # '04' -> '4' (a zero-padded file-name number, same value)
                                    elif s_seq and existing.get('sequence') and str(existing['sequence']) != str(s_seq):
                                        proven_fix = ((SEARCH_CTX.get('seq_fix') or {}).get(asin) or {}) if asin_from_search else {}
                                        if ver and (seq_shared(lib_id, existing['name'], existing['sequence']) or (folder_fix and (asin_from_search or folder_fix.get('same')))):  # ABS number is no evidence
                                            new_q = s_seq
                                            if folder_fix and s_seq != folder_fix['num'] and folder_fix['num'] in extract_volume(aud_data.get('title_raw') or folder_fix['title']):
                                                new_q = folder_fix['num']  # Audible counts parts ('#2' for Vol. 8): the folder and the title agree on the volume
                                            logging.info(f"        🔢 Series '{canon}': ABS #{existing['sequence']} is shared by several books / belongs to another volume -> #{new_q}")
                                            existing['sequence'] = new_q
                                        elif rng_ and ver and re.fullmatch(r'\s*' + re.escape(rng_.group(1)) + r'(?:\.0)?\s*', str(existing['sequence'])):
                                            # the ABS number is the first volume of Audible's range (Publisher's Pack 4 = #7): the range says more ('7-8')
                                            logging.info(f"        🔢 Series '{canon}': #{existing['sequence']} -> #{s_seq} (the pack holds volumes {s_seq})")
                                            existing['sequence'] = s_seq
                                        elif ver and any(k == ck_ or difflib.SequenceMatcher(None, k, ck_).ratio() >= 0.9 for k in proven_fix for ck_, _ in _series_pairs([{'name': existing['name']}])):
                                            logging.info(f"        🔢 Series '{canon}': ABS #{existing['sequence']} was proven wrong by Audible's series list -> #{s_seq}")
                                            existing['sequence'] = s_seq
                                        elif ver and any(o is not existing and str(o.get('sequence') or '') == str(existing['sequence']) and not same(o['name'], existing['name'])
                                                         and any(same(o['name'], n_) for n_ in aud_names) for o in new_list):
                                            # 'Fitz and the Fool' #16 = the book's 'Realm of the Elderlings' number, Audible (same recording): Fitz #3, Realm #16
                                            logging.info(f"        🔢 Series '{canon}': ABS #{existing['sequence']} is the number of another series of this book -> Audible #{s_seq}")
                                            existing['sequence'] = s_seq
                                        else:
                                            logging.info(f"        ℹ️ Series '{canon}': ABS #{existing['sequence']} vs Audible #{s_seq} (ABS kept)")
                                elif not base_list and not non_latin_script(canon):
                                    if why: logging.info(f"        🔗 Series '{s_name}' -> '{canon}' ({'alias list' if why == 'alias' else 'existing ABS series'})")
                                    new_list.append({"name": canon, "sequence": s_seq})
                                elif (ver or normalize_title_text(title) == normalize_title_text(aud_data.get('title_raw') or '')
                                      or difflib.SequenceMatcher(None, normalize_title_text(title), normalize_title_text(aud_data.get('title_raw') or '')).ratio() >= 0.93
                                      or title_similarity(title, aud_data.get('title_raw') or '') >= 0.85) \
                                        and (s_seq or ((f_u := SERIES_FACTS.get((lib_id, canon.strip().lower())) or {}).get('n', 0) >= 2
                                                       and f_u.get('numbered', 0) <= 0.25 * f_u.get('n', 0))) \
                                        and not non_latin_script(canon) and series_in_library(lib_id, canon, authors, exact=True):
                                    # an UNNUMBERED Audible series too when the library holds it for 2+ books, mostly unnumbered there = an umbrella
                                    # ('The Cosmere' on The Final Empire); never into a numbered series (Witchwood Crown is no 'Memory, Sorrow & Thorn' #1)
                                    # user rule 2026-11-08: a series of THIS recording that already exists in the library for this author
                                    # is added although the book has other series (Way of Kings back into 'The Stormlight Archive')
                                    logging.info(f"        ➕ Series '{canon}' #{s_seq}: Audible lists it and it exists in the library for this author")
                                    new_list.append({"name": canon, "sequence": s_seq})
                                else:
                                    ignored.append(f"{canon}" + (f" #{s_seq}" if s_seq else ""))
                            added = [x for x in new_list if not any(same(x['name'], b['name']) for b in base_list)]
                            if bad_fd and not any(series_in_library(lib_id, x['name']) for x in added):
                                new_list = [x for x in new_list if x not in added] + [dict(x) for x in bad_fd]  # Audible's series is unknown here: folder name stays
                                added, bad_fd = [], []
                            bad = bad_nl + bad_fd
                            if bad and added:
                                for x in added:  # Audible gave no number: keep the one of the replaced series
                                    if not x.get('sequence'): x['sequence'] = next((b['sequence'] for b in bad if b.get('sequence')), None)
                                logging.info(f"        🔁 Series {', '.join(repr(b['name']) for b in bad)} -> {', '.join(repr(x['name']) for x in added)} "
                                             f"({'not English/German' if bad_nl else 'folder name, Audible series exists in the library'})")
                                from abs_series import foreign_for_book as _ffb2
                                keep_fd = [dict(b) for b in bad_fd if not (non_latin_name(b['name']) or _ffb2(b['name'], abs_updates.get('language') or lang))]
                                if keep_fd:  # an English folder series is a real (sub-)series ('Disciples of the Horned One' in 'Soul Force Saga'): it stays
                                    new_list += [b for b in keep_fd if b not in new_list]
                                    logging.info(f"        ℹ️ Series {', '.join(repr(b['name']) for b in keep_fd)} kept next to it (English/German folder series)")
                            elif bad_nl and any(same(b['name'], n) for b in base_list for n in aud_names):
                                logging.info(f"        🔁 Series {', '.join(repr(b['name']) for b in bad_nl)} removed (not English/German; Audible's series is already there)")
                            elif bad_nl and base_list:  # the book has another (English/German) series: the foreign extra one simply goes
                                logging.info(f"        🔁 Series {', '.join(repr(b['name']) for b in bad_nl)} removed (not English/German; the book is in {', '.join(repr(b['name']) for b in base_list)})")
                            elif bad_nl:
                                new_list = [x for x in new_list if x not in added] + [dict(x) for x in bad_nl]
                                for b in bad_nl:
                                    logging.info(f"        ⚠️ Series '{b['name']}' is not English/German and Audible has no series for this book: review list")
                                    add_review(key, title, 'series', b['name'], 'series name is not English/German; Audible has no series for this book - please rename it in ABS')
                            if ignored:
                                logging.info(f"        ℹ️ Audible series not added (ABS already has series, or other edition): {', '.join(ignored)}")
                            same_book = (same_rec_rt and bool(abs_dur and aud_data.get('runtime_sec'))) or normalize_title_text(title) == normalize_title_text(aud_data.get('title_raw') or '')
                            aud_ser = [{'name': (s_.get('name') or '').strip(), 'sequence': s_.get('part')} for s_ in series_list
                                       if (s_.get('name') or '').strip() and not (_svar(s_['name']) - book_variant)]
                            new_list, cl_notes = series_cleanup(lib_id, item_data, new_list, aud_ser, authors, book_variant, same_book, abs_updates.get('language') or lang)
                            for n_ in cl_notes: logging.info(f"        🧹 Series {n_}")
                            if same_book and aud_data.get('subtitle_raw'):
                                # round 5 (2026-11-09): Audible's subtitle names the book's series and number ('Legend of Drizzt: Companions Codex,
                                # Book 3') while ABS has another number nobody else holds there (#100): the subtitle's number
                                for x_ in new_list:
                                    m_ = re.search(r'(?i)(?:^|:\s*|\b)' + re.escape(str(x_.get('name') or '')) + r',?\s+(?:Book|Volume|Vol\.)\s+(\d+(?:\.\d+)?)\b', aud_data['subtitle_raw'])
                                    q_ = str(x_.get('sequence') or '').strip()
                                    others_q = [_seq_num(k_[2]) for k_ in SEQ_TITLES if k_[0] == lib_id and k_[1] == str(x_.get('name') or '').lower() and k_[2] != q_ and _seq_num(k_[2]) is not None]
                                    outlier_ = bool(others_q) and _seq_num(q_) is not None and _seq_num(q_) > max(others_q) + 10  # #100 next to #1 and #2 (not a reading-order shift like Ascend Online)
                                    if m_ and q_ and outlier_ and _seq_num(q_) != float(m_.group(1)) and '-' not in q_ \
                                            and not ((SEQ_TITLES.get((lib_id, str(x_['name']).lower(), m_.group(1))) or set()) - {normalize_title_text(title) + ('|' + asin if asin else '')}):
                                        logging.info(f"        🔢 Series '{x_['name']}': ABS #{q_} -> #{m_.group(1)} (Audible's subtitle '{aud_data['subtitle_raw']}')")
                                        x_['sequence'] = m_.group(1)
                            # a number that three or more DIFFERENT books share is rescan junk ('Mushoku Tensei Web Novel' #3 on 18 books):
                            # when Audible's series of the same recording was added, the junk series goes
                            junk = [x for x in new_list if x in base_list and x.get('sequence')
                                    and junk_seq(lib_id, x['name'], x['sequence'], lang)]
                            if ver and junk and any(x not in base_list for x in new_list):
                                logging.info(f"        🔁 Series {', '.join(repr(x['name']) + ' #' + str(x['sequence']) for x in junk)} removed (number shared by 3+ different books; Audible's series added)")
                                new_list = [x for x in new_list if x not in junk]
                            _sk = lambda L: sorted((x['name'], str(x.get('sequence') or '')) for x in L)
                            if _sk(new_list) != _sk(cur_list):  # only a real change, never a mere re-ordering
                                abs_updates['series'] = new_list
                                log_updates.append("Series: " + ", ".join(f"'{x['name']}' #{x['sequence']}" for x in new_list))
                        # a junk number Audible could not replace (the series is not Audible's, e.g. a reading-order series): the number goes,
                        # the book stays in the series - a wrong number is worse than none, and the next Audible match can fill it
                        cur_s = abs_updates.get('series') if 'series' in abs_updates else [{'name': x.get('name'), 'sequence': x.get('sequence')} for x in (meta.get('series') or []) if x.get('name')]
                        def _fix_junk(x):
                            if not junk_seq(lib_id, x['name'], x.get('sequence'), lang): return x
                            # the series IS the folder of the book ('Shannara (Chronological Order)/10 - A Knight of the Word'): the folder number is the order
                            fm = re.match(r'^(?:book\s*)?(\d+(?:\.\d+)?)\s+-\s+', str(item_data.get('relPath') or '').replace('\\', '/').split('/')[-1])
                            if fm and folder_series_name(item_data, x['name']) and (strip_lead0(fm.group(1))) != str(x.get('sequence')).strip(): return dict(x, sequence=strip_lead0(fm.group(1)))
                            return dict(x, sequence=None)
                        cleared = [_fix_junk(x) for x in cur_s]
                        if cleared != cur_s:
                            logging.info(f"        🔢 Series {', '.join(repr(a['name']) + ' #' + str(a['sequence']) for a, b in zip(cur_s, cleared) if a != b)}: number held by 3+ different books - replaced by the folder number or removed")
                            abs_updates['series'] = cleared
                            log_updates.append("Series: " + ", ".join(f"'{x['name']}'" + (f" #{x['sequence']}" if x.get('sequence') else '') for x in cleared))
                    else: logging.info("        🔒 Series Update Skipped (Locked)")

                    # Proven data errors in authors (same recording only, runtime to 2 min):
                    # 1) authors and narrators SWAPPED ('Overlord, Vol. 14': author Chris Guerrero, narrator Kugane Maruyama)
                    # 2) several Audible authors glued into ONE ABS author ('JF Brink Thefirstdefier' = 'JF Brink' + 'Thefirstdefier')
                    _nk = lambda xs: sorted(re.sub(r'[^a-z]', '', fold_accents(str(x)).lower()) for x in xs if x)
                    from abs_series import is_role_credit as _irc, author_key as _ak0
                    _roles0 = {_ak0(n) for n in tuple(ROLE_NAMES)}
                    a_auth = [n for i, n in enumerate(aud_data.get('authors_raw') or []) if not _irc(n) and not (i > 0 and _ak0(n) in _roles0)]  # only real authors
                    a_narr = aud_data.get('narrators_raw') or []
                    exact_rec = bool(abs_dur and aud_data.get('runtime_sec') and abs(abs_dur - aud_data['runtime_sec']) <= 120)
                    same_t = normalize_title_text(title) == normalize_title_text(aud_data.get('title_raw') or '')
                    full_x = bool(authors and meta.get('narrators') and set(_nk(authors)) <= set(_nk(a_narr)) and set(_nk(meta.get('narrators'))) <= set(_nk(a_auth)))
                    if (exact_rec or same_t or full_x) and a_auth and a_narr and authors and meta.get('narrators') and _nk(authors) != _nk(a_auth) \
                            and set(_nk(authors)) & set(_nk(a_narr)) and not set(_nk(authors)) & set(_nk(a_auth)) \
                            and set(_nk(meta.get('narrators'))) & set(_nk(a_auth)):  # ABS authors = Audible narrators (none of Audible's authors), ABS narrators hold the author
                        if 'lock_authors' in tags or 'lock_narrators' in tags: logging.info("        🔒 Authors/narrators swapped, but locked")
                        else:
                            abs_updates['authors'] = [{'name': n} for n in a_auth]; abs_updates['narrators'] = list(a_narr)
                            log_updates.append(f"Authors/narrators were swapped: authors -> {a_auth}, narrators -> {a_narr}")
                    elif exact_rec and len(authors) == 1 and len(a_auth) >= 2 and 'lock_authors' not in tags \
                            and ''.join(_nk(authors)) in (''.join(re.sub(r'[^a-z]', '', fold_accents(x).lower()) for x in a_auth),
                                                          ''.join(re.sub(r'[^a-z]', '', fold_accents(x).lower()) for x in reversed(a_auth))):
                        abs_updates['authors'] = [{'name': n} for n in a_auth]
                        log_updates.append(f"Author '{authors[0]}' is {len(a_auth)} authors glued together: -> {a_auth}")

                    # Narrators / authors: only filled when empty
                    aud_narr = aud_data.get('narrators_raw') or []
                    if aud_narr and not meta.get('narrators') and same_rec_rt:
                        if 'lock_narrators' in tags: logging.info(f"        🔒 Narrators Update Skipped (Locked): {aud_narr}")
                        else:
                            abs_updates['narrators'] = aud_narr
                            FILLED_NARR.add(key)  # copied from Audible: never evidence for Audible later (cover check)
                            log_updates.append(f"Narrators: -> {aud_narr}")
                    elif aud_narr and meta.get('narrators') and same_rec_rt and abs_dur and aud_data.get('runtime_sec') \
                            and abs(abs_dur - aud_data['runtime_sec']) <= 120:  # same recording (runtime to 2 min)
                        # hygiene: ABS lists the AUTHOR as narrator (file tags) while the same recording on Audible has other narrators
                        k_ = narr_keys  # joint credits per person too ('Bridget & Jake Bordeaux')
                        fn_ = folder_narrators(item_data) or []
                        ko_ = lambda xs: {''.join(sorted(re.findall(r'[a-z]+', fold_accents_s(x).lower()))) for x in xs if x}  # 'Isshiki Ichika' = 'Ichika Isshiki'
                        def typo_(a_, b_):  # 'Cliff Kurt' (ABS) vs 'Cliff Kirk' (Audible, same recording): same first name, last name one letter off
                            ta, tb = fold_accents_s(a_).lower().split(), fold_accents_s(b_).lower().split()
                            return len(ta) == len(tb) >= 2 and ta[:-1] == tb[:-1] and ta[-1] != tb[-1] and ta[-1][0] == tb[-1][0] and abs(len(ta[-1]) - len(tb[-1])) <= 1 \
                                and sum(x != y for x, y in zip(ta[-1], tb[-1])) + abs(len(ta[-1]) - len(tb[-1])) <= 2
                        if k_(meta['narrators']) and (k_(meta['narrators']) <= k_(authors) or ko_(meta['narrators']) <= ko_(authors)) and not (k_(aud_narr) & k_(authors)):
                            if 'lock_narrators' in tags: logging.info(f"        🔒 Narrators Update Skipped (Locked): {aud_narr}")
                            else:
                                abs_updates['narrators'] = aud_narr
                                FILLED_NARR.add(key)
                                log_updates.append(f"Narrators: {meta['narrators']} (= author) -> {aud_narr}")
                        elif fn_ and k_(fn_) & k_(aud_narr) and not (k_(meta['narrators']) & (k_(aud_narr) | k_(fn_))):
                            # ABS narrator belongs to ANOTHER recording (came with a wrong ASIN: Fool's Errand 'James Langton') while the book
                            # folder AND Audible's same recording (runtime 2 min) name the real one: it made the ASIN flip between rounds (2026-11-09)
                            if 'lock_narrators' in tags: logging.info(f"        🔒 Narrators Update Skipped (Locked): {aud_narr}")
                            else:
                                abs_updates['narrators'] = aud_narr
                                FILLED_NARR.add(key)
                                log_updates.append(f"Narrators: {meta['narrators']} -> {aud_narr} (book folder and Audible's same recording agree)")
                        elif len(meta['narrators']) == len(aud_narr) and not (k_(meta['narrators']) & k_(aud_narr)) \
                                and all(any(typo_(a_, b_) for b_ in aud_narr) for a_ in meta['narrators']):
                            if 'lock_narrators' in tags: logging.info(f"        🔒 Narrators Update Skipped (Locked): {aud_narr}")
                            else:
                                abs_updates['narrators'] = aud_narr
                                log_updates.append(f"Narrators: {meta['narrators']} -> {aud_narr} (spelling of the same recording's narrator)")
                    if 'lock_narrators' not in tags and not abs_updates.get('narrators') and meta.get('narrators'):
                        # round 4 (2026-11-09): one spelling per narrator - the library's most used one; the same person twice on a book goes
                        new_n = []
                        for n_ in meta['narrators']:
                            c_ = narrator_canonical(n_)
                            if not any(narr_fold(c_) == narr_fold(x_) for x_ in new_n): new_n.append(c_)
                        if new_n != list(meta['narrators']):
                            abs_updates['narrators'] = new_n
                            log_updates.append(f"Narrators: {meta['narrators']} -> {new_n} (one spelling per narrator)")
                    if not aud_data.get('related_edition'): note_author_links(authors, aud_data)
                    aud_auth = aud_data.get('authors_raw') or []
                    from abs_series import is_role_credit, author_key as _ak
                    _roles = {_ak(n) for n in tuple(ROLE_NAMES)}  # tuple(): atomic copy, another worker may add role names meanwhile
                    from abs_series import role_base as _rb
                    roles_here = {_ak(_rb(n)) for n in aud_auth if is_role_credit(n)}  # 'Ken Liu - translator' on THIS product
                    aud_auth = [n for i, n in enumerate(aud_auth) if not is_role_credit(n) and not (i > 0 and _ak(n) in _roles)]  # only real authors
                    if roles_here and authors and 'lock_authors' not in tags and not abs_updates.get('authors'):
                        keep_ = [a_ for a_ in authors if _ak(a_) not in roles_here]
                        if keep_ and len(keep_) < len(authors) and {_ak(a_) for a_ in keep_} & {_ak(n) for n in aud_auth}:
                            # Audible credits this person with a ROLE on exactly this product (The Three-Body Problem: ABS 'Ken Liu, Cixin Liu',
                            # Audible 'Cixin Liu, Ken Liu - translator'); a real author stays and agrees with Audible
                            abs_updates['authors'] = [{'name': a_} for a_ in keep_]
                            log_updates.append(f"Authors: {authors} -> {keep_} (Audible lists {', '.join(a_ for a_ in authors if a_ not in keep_)} with a role)")
                    if aud_auth and not authors:
                        if 'lock_authors' in tags: logging.info(f"        🔒 Authors Update Skipped (Locked): {aud_auth}")
                        else:
                            abs_updates['authors'] = [{'name': n} for n in aud_auth]
                            log_updates.append(f"Authors: -> {aud_auth}")
                    elif aud_auth and authors:
                        abs_norm = {re.sub(r'[^a-z]', '', fold_accents(a).lower()) for a in authors if a}
                        for an in aud_auth:
                            if re.sub(r'[^a-z]', '', fold_accents(an).lower()) not in abs_norm and match_author(authors, an):
                                AUTHOR_NOTES.append({'title': title, 'abs': authors, 'audible': an})
                                logging.info(f"        ℹ️ Author spelling differs: ABS {authors} vs Audible '{an}' (not changed, listed in report)")
                        if not match_author(authors, ', '.join(aud_auth)) and not any(re.search(r'[^\x00-ɏ]', a_) for a_ in aud_auth) \
                                and 'lock_authors' not in tags and same_recording(abs_dur, meta.get('narrators'), aud_data):
                            # same recording (runtime 2 min + narrator), but NO author in common ('War of the Spider Queen' books by Thomas M.
                            # Reid & co. listed under the series editor R.A. Salvatore): a person is never replaced automatically -> review list
                            add_review(key, title, 'authors', ', '.join(aud_auth), f"Audible names other authors for the same recording (ABS: {', '.join(authors)})")
                            logging.info(f"        ⚠️ Authors: ABS {authors} vs Audible {aud_auth} for the same recording: review list")
                    # Explicit: only ever set to True when Audible marks the product as adult
                    if aud_data.get('adult') and not meta.get('explicit'):
                        if 'lock_explicit' in tags: logging.info("        🔒 Explicit Update Skipped (Locked)")
                        else:
                            abs_updates['explicit'] = True
                            log_updates.append("Explicit: False -> True")

                    # The user confirmed (review list) an ASIN of ANOTHER book for this file (The New World -> 'Paladin of the New World 1',
                    # same runtime + narrator): the ABS metadata belongs to the other book, so the identity comes from Audible - title,
                    # authors, series (replaced, not added), narrators. Lock tags respected. (2026-11-09: author and old series had stayed)
                    id_switch = bool(ov.get('identity') and ov.get('asin') == asin and aud_data.get('title_raw') and abs_dur and aud_data.get('runtime_sec')
                                     and abs(abs_dur - aud_data['runtime_sec']) <= 120)
                    if id_switch:
                        id_aut = [n for n in (aud_data.get('authors_raw') or []) if not is_role_credit(n)]
                        id_ser = [{'name': canonical_series(lib_id, (x.get('name') or '').strip(), id_aut)[0], 'sequence': aud_part(x.get('part'))}
                                  for x in (md_raw.get('series') or []) if (x.get('name') or '').strip()]
                        for fld, val, lk in (('title', aud_data['title_raw'], 'lock_title'), ('subtitle', aud_data.get('subtitle_raw'), 'lock_title'),
                                             ('authors', [{'name': n} for n in id_aut], 'lock_authors'), ('series', id_ser, 'lock_series'),
                                             ('narrators', aud_data.get('narrators_raw') or [], 'lock_narrators')):
                            cur_v = meta.get(fld)
                            same_v = (sorted(str((x.get('name') if isinstance(x, dict) else x) or '').lower() for x in cur_v or []) ==
                                      sorted(str((x.get('name') if isinstance(x, dict) else x) or '').lower() for x in val or [])) if isinstance(val, list) else (cur_v or '') == (val or '')
                            if fld == 'series' and isinstance(val, list):
                                same_v = sorted((x.get('name'), str(x.get('sequence') or '')) for x in cur_v or []) == sorted((x['name'], str(x['sequence'] or '')) for x in val)
                            if not val or same_v: continue
                            if lk in tags: logging.info(f"        🔒 {fld.capitalize()} not replaced (locked)"); continue
                            abs_updates[fld] = val
                            log_updates.append(f"{fld.capitalize()}: -> {val if not isinstance(val, list) else ', '.join((x.get('name') + (' #' + str(x['sequence']) if x.get('sequence') else '')) if isinstance(x, dict) else x for x in val)} (ASIN confirmed by you: another book)")
                        if str(meta.get('isbn') or '').strip() and 'lock_isbn' not in tags and 'isbn' not in abs_updates:
                            # the old book's ISBN (Orb of Cairado's 9798331989484 on The Ambassador's Mission, 2026-11-10) or its ASIN in
                            # the ISBN field (The New World 'B0C71XXX3Q'): it belongs to the other book
                            abs_updates['isbn'] = ''
                            log_updates.append(f"ISBN: '{meta.get('isbn')}' -> '' (belongs to the other book)")
                        # publisher, year and genres of the other book stayed (Ambassador's Mission kept 2026, Tantor Media and the
                        # Orb's 'Novella', 'Steampunk' genres, 2026-11-10): replaced by the confirmed recording's values
                        id_pub = ((md_raw.get('publisher') or {}).get('name') or '').strip()
                        id_year = parse_release_year(md_raw.get('releaseDate'))
                        id_gen = [g for g in dict.fromkeys(map_genre(c.get('name'), aud_data.get('domain')) for c in (md_raw.get('categories') or []) if c.get('name')) if g]
                        for fld, val, lk in (('publisher', id_pub, 'lock_publisher'), ('publishedYear', str(id_year) if id_year else None, 'lock_year'),
                                             ('genres', id_gen, 'lock_genres')):
                            if not val or val == meta.get(fld): continue
                            if lk in tags: logging.info(f"        🔒 {fld} not replaced (locked)"); continue
                            abs_updates[fld] = val
                            log_updates.append(f"{fld}: {meta.get(fld)!r} -> {val!r} (ASIN confirmed by you: another book)")
                    if abs_updates:
                        logging.info(f"        🛠️ Meta Updates{' (DRY_RUN, not saved)' if DRY_RUN else ''}:")
                        for upd in log_updates: logging.info(f"          -> {upd}")
                        if abs_update(iid, key, title, abs_updates, meta, 'audible_meta'): stats['meta_updated'] += 1
                    else: logging.info("        ✅ No metadata updates necessary.")

                    # Cover: set when ABS has none; REPLACED when the book is verified as the same recording (runtime ±2 min + narrator,
                    # or an ASIN confirmed by this run's search + runtime ±2 min). cover_state.json avoids re-uploading the same cover every run.
                    has_cover = bool((item_data.get('media') or {}).get('coverPath'))
                    rt_exact = bool(abs_dur and aud_data.get('runtime_sec') and abs(abs_dur - aud_data['runtime_sec']) <= 120)
                    cover_verified = (same_recording(abs_dur, meta.get('narrators'), aud_data) and key not in FILLED_NARR) or (asin_from_search and rt_exact) \
                        or id_switch  # confirmed other book: the old cover shows the other book (Orb of Cairado's on Ambassador's Mission)
                    if aud_data.get('cover_url') and not lang_conflict and cover_verified and ((FILL_COVERS and not has_cover) or  # user rule: covers only for a VERIFIED ASIN
                                                                                               ((UPDATE_COVERS or id_switch) and has_cover and COVER_STATE.get(iid) != asin)):
                        if 'lock_cover' in tags: logging.info("        🔒 Cover Update Skipped (Locked)")
                        elif abs_set_cover(iid, key, title, aud_data['cover_url'], 'audible_cover' if not has_cover else 'audible_cover_verified', replace=has_cover):
                            stats['cover_added'] += 1
                            COVER_STATE[iid] = asin
                            logging.info(f"        🖼️ Cover {'would be ' if DRY_RUN else ''}{'replaced (verified book)' if has_cover else 'set'} from Audible: {aud_data['cover_url']}")

                    # Chapters (user 2026-11-10): Audible's chapters for poorly chaptered files of the SAME recording, only when they fit reliably
                    chs0_ = (item_data.get('media') or {}).get('chapters') or []
                    if MATCH_CHAPTERS and len(chs0_) <= 1 and 'lock_chapters' not in tags and not (cover_verified and not lang_conflict and asin and not aud_data.get('related_edition')):
                        # a book WITHOUT chapters whose recording is not verified: nothing can be matched safely -> review list (user 2026-11-10)
                        add_review(key, title, 'chapters', 'no chapters' if not chs0_ else 'a single chapter',
                                   "the book has no chapters and Audible's could not be matched (recording not verified) - add them in ABS (Edit > Chapters)")
                    if MATCH_CHAPTERS and not lang_conflict and asin and not aud_data.get('related_edition') and (cover_verified or len((item_data.get('media') or {}).get('chapters') or []) >= 8):
                        abs_chs = (item_data.get('media') or {}).get('chapters') or []
                        poor = chapters_poor(abs_chs, abs_dur)
                        if poor and 'lock_chapters' in tags: logging.info("        🔒 Chapters not replaced (locked)")
                        elif poor and abs_dur and abs_chs and max(float(c.get('end') or 0) for c in abs_chs) > abs_dur + 300 \
                                and aud_data.get('runtime_sec') and aud_data['runtime_sec'] > abs_dur + 300:  # user 2026-11-10: only > 5 min on the list
                            # the chapters AND Audible's runtime say the book is longer than the file: the file probably lacks its end
                            # (incomplete download) - nothing is cut, the user is told (round 'chapters', 2026-11-10: 'The Long Way' -78 min)
                            miss_ = round((aud_data['runtime_sec'] - abs_dur) / 60)
                            logging.info(f"        ⚠️ The file seems incomplete: chapters and Audible's runtime are {miss_} min longer than the file - review list")
                            add_review(key, title, 'incomplete', str(miss_), f"the audio file is {miss_} min shorter than its chapters and Audible's runtime - incomplete download or a shorter edition?")
                        elif poor and abs_dur and not (aud_ch := audible_chapters(asin, aud_data.get('domain'))) and poor in ('no chapters', 'a single chapter'):
                            logging.info("        ⚠️ The book has no chapters and Audible has none for it: review list")
                            add_review(key, title, 'chapters', poor, "the book has no chapters and Audible lists none for this product - add them in ABS (Edit > Chapters)")
                        elif poor and abs_dur and aud_ch:
                            new_chs, why_c = match_chapters(abs_chs, abs_dur, aud_ch)
                            if new_chs and not cover_verified and not ((an_ := anchor_align(abs_chs, aud_ch[0], abs_dur, 0.95)) and an_[2] >= 7):
                                new_chs, why_c = None, 'recording not verified and the chapter marks do not prove it'
                            if new_chs and poor == 'only numbered chapters without names' and all(RE_GENERIC_CH.match(c['title'] or '') for c in new_chs):
                                new_chs, why_c = None, "Audible's chapters have no names either"  # 'Chapter 1' for 'Chapter 1': nothing gained
                            if new_chs and abs_set_chapters(iid, key, title, new_chs, abs_chs, 'audible_chapters'):
                                stats['chapters'] += 1
                                logging.info(f"        📑 Chapters {'would be ' if DRY_RUN else ''}taken from Audible: {len(new_chs)} ({poor} before; {why_c})")
                            elif not new_chs:
                                logging.info(f"        ℹ️ Audible's chapters not taken: {why_c}")
                                if poor in ('no chapters', 'a single chapter'):  # user 2026-11-10: books WITHOUT chapters that cannot be matched go on the list
                                    add_review(key, title, 'chapters', poor, f"the book has no chapters and Audible's do not fit reliably ({why_c}) - add them in ABS (Edit > Chapters)")

                # Hardcover: ratings only. Audible's clean title first, the ABS title as second comparison.
                hc_targets = hc_target_titles(aud_data if not skip_meta else None, search_title, title, meta.get('subtitle'))
                if 'hardcover' in src_due or not HARDCOVER_ENABLED:
                    hc_data, hc_status = get_hardcover_data(key, hc_targets, list(dict.fromkeys([*authors, *((aud_data or {}).get('authors_raw') or [])])),  # ABS and Audible authors (ABS sometimes lists the translator)
                                                             asin, meta.get('isbn'), ov.get('hardcover'),
                                                            use_cache=not item.get('_manual_trigger'),
                                                            abs_series=[(x.get('name'), x.get('sequence')) for x in (meta.get('series') or [])], lang=lang, abs_title=search_title)
                else:
                    hc_data, hc_status = None, 'fresh'
                    logging.info(f"      -> ⏭️ Hardcover not asked: {source_note(key, 'hardcover')}")
                if hc_data and hc_data.get('id') and not DRY_RUN: HC_CACHE[key] = {'id': hc_data['id'], 'slug': hc_data.get('slug')}
                if hc_data and aud_data and aud_data.get('title_raw') and not aud_data.get('related_edition') and not skip_meta:  # every Hardcover hit is cross-checked against the Audible product
                    cc_ok, cc_why = hc_audible_crosscheck(hc_data, aud_data)
                    if not cc_ok and hc_data.get('source') == 'cache':  # the cached id came from earlier (wrong) data: forget it, search again
                        logging.info(f"        ♻️ Cached Hardcover '{hc_data.get('title')}' does not fit Audible '{aud_data.get('title_raw')}' ({cc_why}): cache dropped, searching again")
                        HC_CACHE.pop(key, None)
                        hc_data, hc_status = get_hardcover_data(key, hc_targets, list(dict.fromkeys([*authors, *((aud_data or {}).get('authors_raw') or [])])), asin, meta.get('isbn'),
                                                                ov.get('hardcover'), use_cache=False, abs_series=[(x.get('name'), x.get('sequence')) for x in (meta.get('series') or [])],
                                                                lang=lang, abs_title=search_title)
                        if hc_data and hc_data.get('id') and not DRY_RUN: HC_CACHE[key] = {'id': hc_data['id'], 'slug': hc_data.get('slug')}
                        cc_ok, cc_why = hc_audible_crosscheck(hc_data, aud_data) if hc_data else (True, '')
                    if hc_data and not cc_ok:
                        logging.info(f"        ⚠️ Hardcover '{hc_data.get('title')}' does not look like Audible '{aud_data.get('title_raw')}' ({cc_why}): Hardcover ratings are NOT used (review list)")
                        add_review(key, title, 'hardcover', hc_data.get('url'), f"Hardcover book differs from the Audible product ({cc_why})")
                        hc_data = None; LOOKUP_HITS['hardcover'] = None
                    elif hc_data:
                        logging.info(f"        ✓ Hardcover cross-check against Audible: {cc_why}")
                        hc_learn_roles(hc_data)
                if HARDCOVER_ENABLED and hc_status != 'fresh': stats['hc_found' if hc_data else 'hc_missing'] += 1
                sst_record(key, 'hardcover', 'found' if hc_data else hc_status, LOOKUP_HITS['hardcover'])
                if hc_data and (not aud_data or aud_data.get('related_edition')) and 'lock_asin' not in tags:
                    via = hc_audio_asin(hc_data, abs_dur, authors, hc_targets, meta.get('narrators') or [], lang)
                    if via and via[0] != asin:
                        logging.info(f"        ✨ NEW ASIN via Hardcover edition: {via[0]} (evidence: {', '.join(via[1])})")
                        ASIN_OWNERS.setdefault(via[0], set()).add(key)
                        if abs_update(iid, key, title, {'asin': via[0]}, {'asin': asin}, 'hardcover_asin'):
                            logging.info(f"        💾 ASIN {'would be updated (DRY_RUN)' if DRY_RUN else 'updated in ABS'}.")
                        asin, aud_data = via[0], via[2]
                        stats['asin_found'] += 1; stats['asin_migrated'] += 1

                isbn_written = False
                bad_isbn = isbn_bad_reason(meta.get('isbn'))
                if not bad_isbn and (i_c := normalize_isbn(meta.get('isbn'))):
                    # round 5 (2026-11-09): a German edition's ISBN on an English book (Dungeon Crawler Carl 978-3-7480...), a French one on a
                    # German book, or the same ISBN on several different books (Ishura 2 = Overlord 9): replaced only by a verified ISBN below
                    l_c = map_language(meta.get('language')) if meta.get('language') else None
                    foreign_ = ('9782', '9783', '9784', '97884', '97888', '97890', '97894')
                    if len(i_c) == 13 and ((l_c == 'English' and i_c.startswith(foreign_)) or (l_c == 'German' and i_c.startswith(tuple(x for x in foreign_ if x != '9783')))):
                        bad_isbn = f"registration group {i_c[:4]} does not fit a {l_c} book"
                    elif (others_ := [o for o in ISBN_OWNERS.get(i_c, set()) - {key} if KEY_ASIN.get(o) and KEY_ASIN.get(o) != asin]):
                        own_fits = lambda l: bool(l) and str(i_c).startswith(ISBN_GROUPS.get(str(l).lower(), ('-',)))
                        if not (own_fits(l_c) and any(not own_fits(KEY_LANG.get(o)) for o in others_)):  # Eragon (English, 978-0) keeps it, the German edition loses it
                            bad_isbn = 'the same ISBN is on another book of the library'
                empty_isbn = not str(meta.get('isbn') or '').strip()
                # ISBN = the PRINT edition (user decision 2026-11-04: the audiobook is identified by the ASIN). Wrong ISBNs are replaced and
                # empty ones filled, both only with the ISBN-13 of a print edition of the verified book (Hardcover), same language.
                if bad_isbn or (empty_isbn and hc_data and hc_data.get('id')):
                    verified = bool(hc_data and hc_data.get('id') and hc_data.get('source') in ('ASIN', 'search', 'cache', 'manual link') and asin and aud_data and not skip_meta)
                    fix = None
                    if 'lock_isbn' in tags:
                        if bad_isbn: logging.info(f"        🔒 ISBN '{meta.get('isbn')}' looks wrong ({bad_isbn}) but is locked")
                    elif verified:
                        try: fix = hc_replacement_isbn(hc_data, asin, abs_dur, map_language(meta.get('language')) if meta.get('language') else 'English')
                        except SourceBlockedException: fix = None
                        if fix and (ISBN_OWNERS.get(fix[0], set()) - {key}):
                            logging.info(f"        ⛔ ISBN {fix[0]} already belongs to another book: not assigned")
                            fix = None
                        elif fix: ISBN_OWNERS.setdefault(fix[0], set()).add(key)
                        if fix and abs_update(iid, key, title, {'isbn': fix[0]}, meta, 'isbn_fix' if bad_isbn else 'isbn_fill_print'):
                            if bad_isbn:
                                logging.info(f"        🔧 ISBN '{meta.get('isbn')}' is wrong ({bad_isbn}): replaced by {fix[0]} ({fix[1]}, Hardcover){' (DRY_RUN, not saved)' if DRY_RUN else ''}")
                                stats['isbn_repaired'] += 1
                            else:
                                logging.info(f"        🔧 ISBN added: {fix[0]} ({fix[1]}, Hardcover){' (DRY_RUN, not saved)' if DRY_RUN else ''}")
                                stats['isbn_added'] += 1
                    if not fix and bad_isbn and 'lock_isbn' not in tags and not skip_meta:  # last fallback: the audio edition ISBN from Audible
                        a_i = audio_isbn_fallback(asin, aud_data, abs_dur, meta.get('narrators'), asin_from_search, key, meta.get('isbn'))
                        if a_i and abs_update(iid, key, title, {'isbn': a_i}, meta, 'isbn_fix_audible'):
                            fix = (a_i, 'audio edition ISBN, Audible'); ISBN_OWNERS.setdefault(a_i, set()).add(key); stats['isbn_repaired'] += 1
                            logging.info(f"        🔧 ISBN '{meta.get('isbn')}' is wrong ({bad_isbn}): replaced by {a_i} (no print ISBN found; audio edition, Audible){' (DRY_RUN, not saved)' if DRY_RUN else ''}")
                    isbn_written = bool(fix)
                    if not fix and bad_isbn and 'lock_isbn' not in tags:
                        # nothing to decide (no candidate): not on the review list but in the report missing_isbn.json (status page)
                        logging.info(f"        ℹ️ ISBN '{meta.get('isbn')}' looks wrong ({bad_isbn}); no verified replacement, left unchanged (ISBN report)")
                        reports['isbn'][key] = {'key': key, 'title': title, 'author': ', '.join(authors or []), 'isbn': meta.get('isbn'), 'reason': bad_isbn,
                                                'hardcover_asked': bool(HARDCOVER_ENABLED and not HC_STATE.get('disabled')), 'ts': datetime.now().strftime('%Y-%m-%d')}
                if not bad_isbn or isbn_written: reports['isbn'].pop(key, None)
                gr_title = (aud_data or {}).get('title_raw') or search_title  # Audible's clean title searches best
                cached_url = ov.get('goodreads') or (None if item.get('_manual_trigger') else GR_CACHE.get(key))
                hc_gids = (hc_data or {}).get('gr_ids') or (hc_fetch_gr_ids(hc_data['id']) if hc_data and hc_data.get('id') and GOODREADS_ENABLED and not GR_STATE['blocked'] and 'goodreads' in src_due else [])
                GR_CTX['series'] = gr_book_series(meta, aud_data)
                if 'goodreads' in src_due or not GOODREADS_ENABLED:
                    gr_data, gr_status = get_goodreads_data(meta.get('isbn'), gr_title, authors, authors[0] if authors else "", cached_url, hc_gids,
                                                            force=bool(item.get('_manual_trigger') or ONLY_ITEMS))
                else:
                    gr_data, gr_status = None, 'fresh'
                    logging.info(f"      -> ⏭️ Goodreads not asked: {source_note(key, 'goodreads')}")
                sst_record(key, 'goodreads', gr_status, LOOKUP_HITS['goodreads'])
                if gr_data and gr_data.get('url') and not DRY_RUN: GR_CACHE[key] = gr_data['url']
                
                if gr_data and not meta.get('isbn') and not isbn_written:
                    # Only fill an empty ISBN, and only with a real ISBN (never an ASIN). Goodreads has the print
                    # edition's ISBN, so an existing ISBN is never overwritten.
                    new_id = normalize_isbn(gr_data.get('isbn'))
                    if new_id and 'lock_isbn' in tags: logging.info("        🔒 ISBN Update Skipped (Locked)")
                    elif new_id and abs_update(iid, key, title, {'isbn': new_id}, meta, 'goodreads_isbn'):
                        logging.info(f"        🔧 ISBN added{' (DRY_RUN, not saved)' if DRY_RUN else ''}: {new_id}")
                        stats['isbn_added'] += 1
                        isbn_written = True
                if empty_isbn and not isbn_written and 'lock_isbn' not in tags and not skip_meta:  # no print ISBN anywhere: audio edition ISBN as the last fallback
                    a_i = audio_isbn_fallback(asin, aud_data, abs_dur, meta.get('narrators'), asin_from_search, key, None)
                    if a_i and abs_update(iid, key, title, {'isbn': a_i}, meta, 'isbn_fill_audible'):
                        ISBN_OWNERS.setdefault(a_i, set()).add(key); stats['isbn_added'] += 1; isbn_written = True
                        logging.info(f"        🔧 ISBN added: {a_i} (no print ISBN found; audio edition, Audible){' (DRY_RUN, not saved)' if DRY_RUN else ''}")

                if 'lock_description' not in tags:
                    cur_desc = meta.get('description') or ''  # ABS sends null for empty descriptions
                    old_blocks = parse_rating_block(cur_desc)
                    # Empty description (apart from our rating block): use Audible's summary
                    summary = (aud_data or {}).get('summary') if not skip_meta else None
                    base_desc = cur_desc
                    if summary and ov.get('identity') and ov.get('asin') == asin and normalize_desc(strip_rating_block(cur_desc)) != normalize_desc(summary) \
                            and key not in IDENTITY_DESC:
                        base_desc = summary  # the old text describes the other book (identity confirmed by the user)
                        with IO_LOCK:
                            IDENTITY_DESC.add(key)  # once: a later own text stays
                            if not DRY_RUN: rw_json(IDENTITY_DESC_FILE, sorted(IDENTITY_DESC))
                        logging.info("        📝 Description replaced with Audible's summary (ASIN of another book confirmed by you)")
                    elif summary and not strip_rating_block(cur_desc):
                        base_desc = summary
                        stats['desc_filled'] += 1
                        logging.info("        📝 Empty description filled with Audible summary")
                    elif summary and aud_data and not aud_data.get('related_edition') and (same_recording(abs_dur, meta.get('narrators'), aud_data)
                                                                                    or title_similarity(title, aud_data.get('title_raw') or '') >= 0.7) \
                            and (other_d := description_of_other_book(strip_rating_block(cur_desc), asin, title)):
                        base_desc = summary  # provably not the user's own text: another book's summary
                        stats['desc_filled'] += 1
                        logging.info(f"        📝 Description replaced by Audible's summary (it was the description of '{other_d}')")
                    elif summary and (why_d := description_replaceable(strip_rating_block(cur_desc), summary)):
                        base_desc = summary  # user rule 2026-11-03: only a provably truncated / very short text, never an own long one
                        stats['desc_filled'] += 1
                        logging.info(f"        📝 Description replaced by Audible's complete summary ({why_d})")
                    if not summary and (other_d := description_of_other_book(strip_rating_block(cur_desc), asin, title)):
                        logging.info(f"        ⚠️ The description is the one of '{other_d}' and Audible has no summary for this book: review list")
                        add_review(key, title, 'description', other_d, f"description belongs to '{other_d}' - Audible has no text for this book, please replace it in ABS")
                    final_desc = build_description(base_desc, {'Audible': aud_data, 'Goodreads': gr_data, 'Hardcover': hc_data}, old_blocks)

                    has_aud = bool(aud_data and int(aud_data.get('count', 0)) > 0)
                    has_gr = bool(gr_data)
                    parts = [n for n, ok in (("Audible", has_aud), ("Goodreads", has_gr), ("Hardcover", bool(source_lines('Hardcover', hc_data)))) if ok]

                    if normalize_desc(final_desc) == normalize_desc(cur_desc):
                        logging.info(f"      -> ✅ Description already up to date ({', '.join(parts) or 'no ratings'})")
                        if has_aud or has_gr: stats['success'] += 1
                    elif abs_update(iid, key, title, {'description': final_desc}, meta, 'ratings'):
                        logging.info(f"      -> ✅ {'WOULD UPDATE (DRY_RUN)' if DRY_RUN else 'SUCCESS'}: ratings block ({', '.join(parts) or 'data cleaned'})")
                        if has_aud or has_gr: stats['success'] += 1
                    else: stats['failed'] += 1
                else:
                    logging.info("      -> 🔒 Description Update Skipped (Locked)")
                    has_aud = bool(aud_data and int(aud_data.get('count', 0)) > 0)
                    has_gr = bool(gr_data)
                    if has_aud or has_gr: stats['success'] += 1

                update_report("audible", key, title, authors[0] if authors else "", asin, "Not found", has_aud)
                update_report("goodreads", key, title, authors[0] if authors else "", meta.get('isbn'), "Not found", has_gr)
                
                no_ratings_yet = bool(aud_data and aud_data.get('page_ok') and not has_aud)
                # per-source state (source_state.json): Audible found -> the book is done for Audible; a missing Goodreads/Hardcover
                # rating is asked again on its own (next day, 5 misses -> pause), see the rating-only passes in build_queue
                if has_aud:
                    sst_record(key, 'audible', 'found', aud_facts=aud_data)
                    if GOODREADS_ENABLED and not has_gr and gr_status != 'fresh':
                        stats['gr_missing'] += 1; logging.info(f"      -> ℹ️ Audible OK, Goodreads {gr_status.replace('_', ' ')}.")
                    record_attempt(key, history, failed, recheck, complete=True)
                elif no_ratings_yet:  # on Audible, no ratings yet: asked again next day, after 5 tries paused until REFRESH_DAYS
                    stats['no_ratings_yet'] += 1
                    sst_record(key, 'audible', 'not_found', None)
                    record_attempt(key, history, failed, recheck)
                else:
                    if LOOKUP_HITS.get('audible') == 0 and not aud_data and not ov.get('asin'):
                        # negative cache (task): every search in every store answered with NO product at all -> surely not on Audible:
                        # no 5 daily attempts, the next check is after REFRESH_DAYS
                        logging.info(f"      -> 🗂️ Not on Audible (no search result in any store): next check in {REFRESH_DAYS} days")
                        record_attempt(key, history, failed, recheck, complete=True)
                    else:
                        logging.warning(f"      -> ⚠️ No Audible data (Audible: {has_aud}, GR: {has_gr})")
                        record_attempt(key, history, failed, recheck)

                # ISO code -> language name ('eng' -> 'English') also for books without a metadata sync (not found / other edition):
                # a pure spelling change, journaled, lock_language respected (audit 2026-11-09: 24 books kept 'eng'/'deu')
                lang_now = str(meta.get('language') or '').strip()
                if lang_now and lang_now.lower() in LANGUAGE_MAP and LANGUAGE_MAP[lang_now.lower()] != lang_now and 'lock_language' not in tags \
                        and book_lang_allowed(lang_now) \
                        and 'language' not in (locals().get('abs_updates') or {}) and not ov.get('ignore'):
                    if abs_update(iid, key, title, {'language': LANGUAGE_MAP[lang_now.lower()]}, meta, 'language_code'):
                        logging.info(f"        🛠️ Language: '{lang_now}' -> '{LANGUAGE_MAP[lang_now.lower()]}' (code spelled out)")

                # Result for the web UI dashboard / problem list
                if aud_data and aud_data.get('related_edition'):
                    status, reason = 'other_edition', f"Ratings of another edition ({aud_data.get('asin')}), ASIN not available anymore"
                elif has_aud: status, reason = 'ok', ''
                elif no_ratings_yet: status, reason = 'no_ratings_yet', 'On Audible, but no ratings yet'
                elif not asin:
                    status, reason = 'not_found', 'No ASIN and no match found on Audible'
                    if MATCH_CHAPTERS and abs_dur and len((item_data.get('media') or {}).get('chapters') or []) <= 1 and 'lock_chapters' not in tags:
                        add_review(key, title, 'chapters', 'no chapters', "the book has no chapters and no Audible product to take them from - add them in ABS (Edit > Chapters)")
                elif aud_data is None: status, reason = 'not_found', 'ASIN not available in any Audible store, no replacement found'
                else: status, reason = 'not_found', 'No ratings found on Audible'
                notes = []
                if skip_meta: notes.append('metadata skipped (Audible data does not fit this book)')
                if GOODREADS_ENABLED and not has_gr and gr_status not in ('deferred', 'fresh'): notes.append(f"Goodreads {gr_status.replace('_', ' ')}")
                if HARDCOVER_ENABLED and not hc_data and hc_status != 'fresh': notes.append(f"Hardcover {hc_status.replace('_', ' ')}")
                if ov.get('ignore'): status = 'ignored'
                review_sweep(key, {'asin', 'title', 'series'} | ({'hardcover'} if HARDCOVER_ENABLED and hc_status != 'fresh' else set()))
                cert, cert_parts = certainty_score(search_title, authors, meta.get('narrators') or [], abs_dur, meta.get('publisher'), meta.get('publishedYear'),
                                                   meta.get('series') or [], aud_data, hc_data, asin_from_search,
                                                   any(x.get('key') == key and x.get('status', 'open') == 'open' for x in (rw_json(REVIEW_FILE) or []) if isinstance(x, dict)))
                set_result(key, lib_id, iid, title, authors, asin, status, reason,
                           {'notes': notes, 'certainty': cert, 'certainty_parts': cert_parts, 'domain': (aud_data or {}).get('domain'), 'goodreads': (gr_data or {}).get('url') or (RESULTS.get(key) or {}).get('goodreads') if gr_status == 'fresh' else (gr_data or {}).get('url'),
                            'hardcover': ((hc_data or {}).get('url') or (RESULTS.get(key) or {}).get('hardcover')) if hc_status == 'fresh' else (hc_data or {}).get('url'), 'compare': compare_view(title, authors, meta, abs_dur, aud_data, hc_data),
                            'asin_source': 'manual' if ov.get('asin') else ('search' if asin_from_search else ('ABS (confirmed)' if asin else '')), 'hc_source': (hc_data or {}).get('source'),
                            'evidence': (LAST_MATCH.get('evidence') or []) if asin_from_search else [],
                            'locks': sorted(t for t in item_tags(item_data) if t.startswith('lock_'))})

                consecutive_rl = 0
                break 

            except RateLimitException as e:
                consecutive_rl += 1
                logging.warning(f"🛑 Rate Limit DETECTED: {e}")
                if e.is_hard or consecutive_rl >= MAX_CONSECUTIVE_RL: 
                    logging.error("🛑 ABORTING script due to Rate Limits."); stats['aborted_ratelimit'] = True; break
                time.sleep(RECOVERY_PAUSE * consecutive_rl)
            except Exception as e:
                logging.exception(f"Item Error: {e}"); stats['failed'] += 1
                # Count the crash as a strike so broken items don't block the batch forever
                record_attempt(f"{lib_id}_{item['id']}", history, failed, recheck)
                set_result(f"{lib_id}_{item['id']}", lib_id, item['id'], (item.get('media') or {}).get('metadata', {}).get('title') or item['id'],
                           [], None, 'error', f"{type(e).__name__}: {e}")
                break
        
        if stats['aborted_ratelimit']: return
        if not needs_sleep: return
        # Requests are throttled per source (MIN_INTERVAL); this pause is only an optional extra.
        sleep_dur = BASE_SLEEP + (random.uniform(0, min(1.0, BASE_SLEEP / 2)) if BASE_SLEEP else 0)
        if search_penalty: sleep_dur += SEARCH_PENALTY_SLEEP
        if sleep_dur > 0: time.sleep(sleep_dur)

    run_books(work_queue, one)

def _author_lane(item):
    """Books of the same primary author always go through the same worker, one after another: two copies of one book
    can never pass the duplicate-ASIN/ISBN guard at the same moment."""
    md = (item.get('media') or {}).get('metadata') or {}
    a = md.get('authorName') or ', '.join(x.get('name', '') if isinstance(x, dict) else str(x) for x in (md.get('authors') or []))
    a = re.sub(r'[^a-z]', '', unicodedata.normalize('NFKD', (a or '').split(',')[0]).lower())
    return a or f"_{item.get('id')}"

def run_books(work_queue, one):
    """Run one(idx, item) for every book: WORKERS threads; groups of the same author stay in one thread, in queue order."""
    if WORKERS <= 1 or len(work_queue) < 2:
        RUN_T0[0] = time.time(); DONE_TS.clear()
        for idx, item in enumerate(work_queue):
            one(idx, item)
            if stats['aborted_ratelimit']: break
        return
    groups = {}
    BOOKS_DONE[0] = 0; RUN_T0[0] = time.time(); DONE_TS.clear()
    for idx, item in enumerate(work_queue): groups.setdefault(_author_lane(item), []).append((idx, item))
    jobs = _queue_mod.Queue()
    for g in sorted(groups.values(), key=lambda g: g[0][0]): jobs.put(g)  # keep the (random) queue order as far as possible
    def worker():
        _TL.stats = {}
        try:
            while not stats['aborted_ratelimit']:
                try: g = jobs.get_nowait()
                except _queue_mod.Empty: break
                for idx, item in g:
                    _TL.logbuf = []
                    try: one(idx, item)
                    finally:
                        if LOG_ROUTER: LOG_ROUTER.flush_book()
                        _TL.logbuf = None
                    if stats['aborted_ratelimit']: break
        finally:
            stats.merge(_TL.stats); _TL.stats = None
    threads = [threading.Thread(target=worker, name=f"book-worker-{i+1}", daemon=True) for i in range(min(WORKERS, len(groups)))]
    logging.info(f"Parallel: {len(threads)} workers for {len(work_queue)} books ({len(groups)} author groups); max. 1 request at a time per host")
    for t in threads: t.start()
    for t in threads: t.join()

def log_run_header():
    logging.info("=" * 60)
    logging.info(f"ABS Ratings run {RUN_ID}{'  ***  DRY RUN: nothing is written to ABS  ***' if DRY_RUN else ''}")
    if os.getenv('CODE_VERSION'): logging.info(f"Code version: {os.getenv('CODE_VERSION')}")
    logging.info(f"Libraries: {', '.join(LIBRARY_IDS)} | Batch: {MAX_BATCH_SIZE} | Refresh: {REFRESH_DAYS}d | Sleep: {BASE_SLEEP}s")
    logging.info(f"Audible API: {USE_AUDIBLE_API} | Goodreads: {'on' if GOODREADS_ENABLED else 'OFF'} | Intervals: {MIN_INTERVAL} | Goodreads budget: {GOODREADS_MAX_PER_RUN or 'unlimited'}")
    logging.info(f"Hardcover: {'on' if HARDCOVER_ENABLED else ('OFF' if HARDCOVER_TOKEN else 'OFF (no token)')} | Rating block order: {', '.join(RATING_ORDER)}")
    logging.info(f"Runtime tolerance: {RUNTIME_TOLERANCE_PCT}% and {RUNTIME_TOLERANCE_MIN}min | Covers: {FILL_COVERS} | Genre language: {GENRE_LANGUAGE}")
    logging.info(f"Series merge: {SERIES_MERGE} (lock after merge: {SERIES_LOCK_AFTER_MERGE}) | Author normalize: {AUTHOR_NORMALIZE} | Library report: {LIBRARY_REPORT} | Series only: {SERIES_ONLY}")
    logging.info(f"Change journal: {PLANNED_FILE if DRY_RUN else CHANGES_FILE}")
    logging.info("=" * 60)

def log_run_summary(start_time, series_result):
    dur = format_time((datetime.now() - start_time).total_seconds())
    s = stats
    hc = HC_STATE
    hc_line = (f"Hardcover: found {s['hc_found']} | not found {s['hc_missing']} | requests {hc['requests']}"
               + (f" (daily limit left: {hc['daily_left']})" if hc['daily_left'] is not None else '') + (f" (limit reset: {hc['reset']})" if hc.get('reset') else '')
               + (f" | stopped: {hc['disabled']}" if hc['disabled'] else '')) if HARDCOVER_ENABLED else "Hardcover: off"
    lines = [
        f"Books processed: {s['processed']} (skipped/locked: {s['skipped']}, manual updates: {s['manual_retrigger']}, re-checks: {s['recheck']})",
        f"Ratings {'to write' if DRY_RUN else 'written'}: {s['success']} | Audible without ratings yet: {s['no_ratings_yet']} | Goodreads missing: {s['gr_missing']} (deferred: {s['gr_deferred']})",
        f"ASINs found: {s['asin_found']} | Ratings from another edition: {s['other_edition']} | Metadata updated: {s['meta_updated']} | Covers added: {s['cover_added']} | Chapters: {s.get('chapters', 0)} | Descriptions filled: {s['desc_filled']} | ISBNs added: {s['isbn_added']}",
        f"Series merges: {series_result.get('merges', 0)} ({series_result.get('books_changed', 0)} books {'would change' if DRY_RUN else 'changed'}) | "
        f"Author spellings unified: {series_result.get('author_fixes', 0)} ({series_result.get('authors_changed', 0)} books)",
        hc_line,
        f"Errors: {s['failed']}{' | Goodreads blocked' if GR_STATE['blocked'] else ''}{' | ABORTED (rate limit)' if s['aborted_ratelimit'] else ''}",
        *([f"SOURCE PROBLEM: {'; '.join(f'{k}: {v}' for k, v in SOURCE_DOWN.items())}"] if SOURCE_DOWN else []),
        f"Duration: {dur}",
    ]
    logging.info("=" * 60)
    logging.info(f"SUMMARY{' (DRY RUN - nothing was changed)' if DRY_RUN else ''}")
    for l in lines: logging.info("  " + l)
    if DRY_RUN: logging.info(f"  Planned changes: {PLANNED_FILE}")
    else: logging.info(f"  Undo this run: python3 abs_undo.py --run {RUN_ID} --apply")
    logging.info("=" * 60)
    logging.debug(f"Raw stats: {stats}")

def create_abs_backup():
    """Ask ABS to create a backup before a real run (ABS: Settings -> Backups). Returns True on success. A failed backup STOPS the
    real run (it was requested, so nothing is written without it); set ABS_BACKUP=false to run consciously without one."""
    try:
        r = abs_session.post(f"{ABS_URL}/api/backups", timeout=600)
        if r.status_code == 200: logging.info("💾 ABS backup created before this run."); return True
        logging.error(f"💾 ABS backup failed (HTTP {r.status_code}): {r.text[:150]}")
    except requests.RequestException as e:
        logging.error(f"💾 ABS backup failed: {e}")
    return False

def fail_early(log_file, start_time, msg):
    """Log a fatal start-up error and hand it to the Unraid notification."""
    logging.error(msg)
    stats['failed'] += 1
    write_env_file(log_file, start_time, error=msg)
    logging.info(f"Debug bundle (send this one for troubleshooting): {os.path.join(LOG_DIR, 'debug_latest.log')}")
    write_debug_bundle(log_file)

DEBUG_ENV_KEYS = ['ABS_URL', 'LIBRARY_IDS', 'GOODREADS_ENABLED', 'BATCH_SIZE', 'SLEEP_TIMER', 'REFRESH_DAYS', 'DRY_RUN', 'SERIES_ONLY', 'GOODREADS_MAX_PER_RUN',
                  'LOG_LEVEL', 'TZ', 'CODE_VERSION', 'USE_AUDIBLE_API', 'AUDIBLE_API_INTERVAL', 'AUDIBLE_WEB_INTERVAL', 'GOODREADS_INTERVAL',
                  'GR_BLOCK_COOLDOWN', 'GR_MAX_COOLDOWNS', 'RUNTIME_TOLERANCE_PCT', 'RUNTIME_TOLERANCE_MIN', 'FILL_COVERS', 'GENRE_LANGUAGE',
                  'SERIES_MERGE', 'SERIES_LOCK_AFTER_MERGE', 'AUTHOR_NORMALIZE', 'LIBRARY_REPORT', 'NO_RATINGS_RECHECK_DAYS',
                  'GR_MISSING_RECHECK_DAYS', 'SEARCH_PENALTY_SLEEP', 'ABS_TIMEOUT',
                  'HARDCOVER_ENABLED', 'HARDCOVER_INTERVAL', 'HARDCOVER_PAID', 'GR_BOOK_PAGES', 'GOODREADS_PAGES_PER_RUN', 'GR_IMPERSONATE', 'WORKERS', 'REMOVE_RATINGS', 'RATING_ICONS', 'RATING_LABELS', 'RATING_POSITION', 'RATING_MIN_COUNT', 'GENRE_RULES', 'RUN_MODE', 'MAINTAIN_SAMPLE', 'MANUAL_LOCK_TAG', 'RATING_ORDER']  # never API_TOKEN

def write_debug_bundle(log_file):
    """One file with everything needed for troubleshooting: settings (no token), the run log, all changes
    of this run, the library report summary and the state files. logs/debug_<run>.log + debug_latest.log."""
    def short(v, n=300):
        t = v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)
        t = (t or '').replace('\n', ' ')
        return t if len(t) <= n else t[:n] + f"... [{len(t)} chars]"
    out = [f"=== ABS RATINGS DEBUG BUNDLE ===", f"Run: {RUN_ID} | Created: {datetime.now().isoformat(timespec='seconds')} | DRY_RUN: {DRY_RUN}", ""]
    out.append("=== SETTINGS ===")
    out += [f"{k}={os.getenv(k)}" for k in DEBUG_ENV_KEYS if os.getenv(k) is not None]
    out.append(f"API_TOKEN set: {bool(API_TOKEN)} (length {len(API_TOKEN or '')}) | HARDCOVER_TOKEN set: {bool(HARDCOVER_TOKEN)}")
    out.append("")
    out.append("=== RUN LOG ===")
    try:
        for h in logging.getLogger().handlers: h.flush()
        with open(log_file, encoding='utf-8') as f: out.append(f.read().rstrip())
    except OSError as e: out.append(f"(log not readable: {e})")
    out.append("")
    jf = PLANNED_FILE if DRY_RUN else CHANGES_FILE
    out.append(f"=== CHANGES OF THIS RUN ({'planned, DRY_RUN' if DRY_RUN else 'applied'}) ===")
    try:
        entries = [e for e in (json.loads(l) for l in open(jf, encoding='utf-8') if l.strip()) if e.get('run') == RUN_ID] if os.path.exists(jf) else []
    except (OSError, ValueError) as e: entries = []; out.append(f"(journal not readable: {e})")
    counts = defaultdict(int)
    for e in entries: counts[f"{e['source']}/{e['field']}"] += 1
    out.append("Counts: " + (", ".join(f"{k}: {v}" for k, v in sorted(counts.items())) or "none"))
    for e in entries[:20000]:
        n = 200 if e['field'] == 'description' else 300
        out.append(f"[{e['source']}] {e['title']} | {e['field']}: {short(e['old'], n)} -> {short(e['new'], n)}")
    if len(entries) > 20000: out.append(f"... {len(entries) - 20000} more changes in {jf}")
    out.append("")
    out.append("=== LIBRARY REPORT (summary) ===")
    try:
        rep_ = json.load(open(os.path.join(REPORT_DIR, 'library_report.json'), encoding='utf-8'))
        rep_['missing'] = {k: f"{len(v)} books" + (f" (e.g. {', '.join(v[:5])})" if v else '') for k, v in rep_.get('missing', {}).items()}
        out.append(json.dumps(rep_, indent=1, ensure_ascii=False))
    except (OSError, ValueError): out.append("(no library report)")
    out.append("")
    out.append("=== STATE FILES ===")
    for name, path in [('history', HISTORY_FILE), ('failed', FAILED_FILE), ('recheck', RECHECK_FILE), ('goodreads cache', GR_CACHE_FILE), ('hardcover cache', HC_CACHE_FILE),
                       ('series aliases', SERIES_ALIASES_FILE), ('series index', SERIES_INDEX_FILE)]:
        d = rw_json(path)
        size = sum(len(v) for v in d.values()) if name.startswith('series') and isinstance(d, dict) else len(d)
        out.append(f"{name}: {size} entries")
    if SERIES_ALIASES: out.append("series aliases: " + json.dumps(SERIES_ALIASES, ensure_ascii=False))
    text = "\n".join(out) + "\n"
    for path in (os.path.join(LOG_DIR, f"debug_{RUN_ID}.log"), os.path.join(LOG_DIR, "debug_latest.log")):
        try:
            with open(path, 'w', encoding='utf-8') as f: f.write(text)
        except OSError as e: logging.error(f"Debug bundle not written: {e}")

def main():
    start_time = datetime.now()
    log_file = setup_logging()
    log_run_header()
    if _TOKEN_REMOVED or _URL_REMOVED:
        logging.warning(f"Removed {_TOKEN_REMOVED} invisible/invalid character(s) from API_TOKEN and {_URL_REMOVED} from ABS_URL (copy & paste artefacts).")
    if not ABS_URL or not API_TOKEN or not LIBRARY_IDS:
        return fail_early(log_file, start_time, "ABS_URL, API_TOKEN or LIBRARY_IDS missing in the userscript.")
    try:
        logging.info("Checking API connection...")
        r = abs_session.get(f"{ABS_URL}/api/libraries")
        if r.status_code == 401:
            return fail_early(log_file, start_time, "ABS rejected the API token (HTTP 401). Create a new token in ABS and paste it again.")
        if r.status_code != 200:
            return fail_early(log_file, start_time, f"Cannot connect to ABS API (HTTP {r.status_code}). Check ABS_URL.")
        logging.info("ABS connection OK.")
    except Exception as e:
        return fail_early(log_file, start_time, f"Connection to ABS failed: {e}")

    reports['audible'] = {x['key']: x for x in rw_json(os.path.join(REPORT_DIR, "missing_audible.json"))}
    reports['goodreads'] = {x['key']: x for x in rw_json(os.path.join(REPORT_DIR, "missing_goodreads.json"))}
    reports['isbn'] = {x['key']: x for x in rw_json(os.path.join(REPORT_DIR, "missing_isbn.json"))}  # wrong ISBN, no verified replacement
    
    logging.info("--- Start ---")
    history, failed, recheck = rw_json(HISTORY_FILE), rw_json(FAILED_FILE), rw_json(RECHECK_FILE)
    GR_CACHE.update(rw_json(GR_CACHE_FILE))
    HC_CACHE.update(rw_json(HC_CACHE_FILE))
    SERIES_ALIASES.update(rw_json(SERIES_ALIASES_FILE))
    SERIES_INDEX.update(rw_json(SERIES_INDEX_FILE))
    if SERIES_ALIASES or SERIES_INDEX:
        logging.info(f"Series aliases: {sum(len(v) for v in SERIES_ALIASES.values())} | Known series: {sum(len(v) for v in SERIES_INDEX.values())}")
    RESULTS.update(rw_json(RESULTS_FILE)); OVERRIDES.update(rw_json(OVERRIDES_FILE))
    COVER_STATE.update(rw_json(COVER_STATE_FILE))
    ROLE_NAMES.update(rw_json(ROLE_NAMES_FILE) or [])
    HC_USAGE.update(rw_json(HC_USAGE_FILE) or {})
    SOURCE_STATE.update(rw_json(SOURCE_STATE_FILE) or {})
    LAST_WRITTEN.update(rw_json(LAST_WRITTEN_FILE) or {}); MANUAL_FIELDS.update(rw_json(MANUAL_FIELDS_FILE) or {})
    FILLED_NARR.update(rw_json(FILLED_NARR_FILE) or [])
    IDENTITY_DESC.update(rw_json(IDENTITY_DESC_FILE) or [])
    SUMMARY_INDEX.update(rw_json(SUMMARY_INDEX_FILE) or {})
    try:  # also every narrator fill already in the journal (runs before this file existed)
        with open(CHANGES_FILE, encoding='utf-8') as f_:
            FILLED_NARR.update(json.loads(l)['key'] for l in f_ if '"field": "narrators"' in l and json.loads(l).get('source') == 'audible_meta')
    except (OSError, ValueError, KeyError): pass
    logging.getLogger().addHandler(ITEM_LOG)
    if ABS_BACKUP and not DRY_RUN and not create_abs_backup():
        return fail_early(log_file, start_time, "ABS backup failed - nothing was written. Check ABS: Settings -> Backups (backup path, disk space) or set ABS_BACKUP=false to run without a backup.")
    if REWRITE_RATING_SYMBOLS:
        rewrite_symbols_pass()
        log_run_summary(start_time, {}); write_env_file(log_file, start_time); write_debug_bundle(log_file)
        return
    if REMOVE_RATINGS:
        remove_ratings_pass()
        log_run_summary(start_time, {}); write_env_file(log_file, start_time); write_debug_bundle(log_file)
        return
    if not SERIES_ONLY:
        global BREAKER
        BREAKER = SH.Breaker(SOURCE_STATE, SST.SOURCES)
        source_canaries()
        if 'audible' in SOURCE_DOWN: logging.error("🚨 Per-book part skipped: the Audible check failed (see above). The library task still runs.")
        run_libraries(history, failed, recheck)
        group_confirm()
        if not DRY_RUN: rw_json(HISTORY_FILE, history); rw_json(FAILED_FILE, failed); rw_json(RECHECK_FILE, recheck); rw_json(HC_CACHE_FILE, HC_CACHE)
        save_reports()
        if AUTHOR_NOTES: rw_json(os.path.join(REPORT_DIR, "author_variants.json"), AUTHOR_NOTES)
        rw_json(RESULTS_FILE, RESULTS)  # also in DRY_RUN: it is the web UI's view of the last check
        if not DRY_RUN: rw_json(COVER_STATE_FILE, COVER_STATE)
        rw_json(ROLE_NAMES_FILE, sorted(ROLE_NAMES))  # knowledge, also from dry runs
        rw_json(HC_USAGE_FILE, HC_USAGE)  # real requests count in dry runs too
        if not DRY_RUN: rw_json(LAST_WRITTEN_FILE, LAST_WRITTEN); rw_json(MANUAL_FIELDS_FILE, MANUAL_FIELDS)
        if not DRY_RUN and len(SIBLING_CACHE) > 1: rw_json(SIBLING_FILE, {k: v for k, v in SIBLING_CACHE.items() if k != '_'})
        if not DRY_RUN: rw_json(FILLED_NARR_FILE, sorted(FILLED_NARR))

    series_result = {}
    if (SERIES_MERGE or LIBRARY_REPORT) and not ONLY_ITEMS:
        lib_written = set()
        def lib_update(iid, key, *a, **kw):  # library-task writes are OUR writes: not a 'manual update' in the next run
            ok = abs_update(iid, key, *a, **kw)
            if ok and not DRY_RUN: lib_written.add(key)
            return ok
        try:
            import abs_series
            series_result = abs_series.run({
                'session': abs_session, 'abs_url': ABS_URL, 'library_ids': LIBRARY_IDS, 'dry_run': DRY_RUN,
                'abs_update': lib_update, 'report_dir': REPORT_DIR, 'merge': SERIES_MERGE, 'report': LIBRARY_REPORT,
                'lock_after_merge': SERIES_LOCK_AFTER_MERGE, 'author_normalize': AUTHOR_NORMALIZE,
                'aliases': SERIES_ALIASES, 'aliases_file': SERIES_ALIASES_FILE, 'index_file': SERIES_INDEX_FILE,
                'decisions_file': os.path.join(SCRIPT_DIR, 'merge_decisions.json'), 'merges_file': os.path.join(SCRIPT_DIR, 'merges.json'),
                'review_file': os.path.join(SCRIPT_DIR, 'merge_review.json'),
                'author_links': {n: sorted(a) for n, a in AUTHOR_LINKS.items()},
                'remove_role_credits': REMOVE_ROLE_CREDITS, 'genre_cleanup': GENRE_CLEANUP, 'book_languages': list(BOOK_LANGUAGES), 'remove_empty_authors': REMOVE_EMPTY_AUTHORS, 'role_names': sorted(ROLE_NAMES), 'journal': journal_change,
                'audible_series_names': set(SERIES_ASINS),
                'extra': {'author_notes': AUTHOR_NOTES, 'unmapped_genres': UNMAPPED_GENRES},
            })
            stats['series_merged'] = series_result.get('merges', 0)
            if lib_written:  # 2026-11-09: 188 genre writes made 193 'manual updates' (full lookups, 1800 Hardcover requests) next run
                stamp = datetime.now(timezone.utc).isoformat(timespec='seconds')
                for k_ in lib_written:
                    if k_ in history: history[k_] = stamp
                rw_json(HISTORY_FILE, history); rw_json(LAST_WRITTEN_FILE, LAST_WRITTEN)  # watched like the per-book writes
        except ImportError:
            logging.error("abs_series.py not found next to abs_ratings.py - series task skipped.")
        except Exception as e:
            logging.exception(f"Series task failed: {e}")
    if MISSING_SERIES and not ONLY_ITEMS and USE_AUDIBLE_API and 'audible' not in SOURCE_DOWN:
        try: missing_in_series_task()
        except Exception as e: logging.exception(f"Missing-volumes report failed: {e}")
    if not ONLY_ITEMS:  # source traffic light for the web UI overview (user 2026-11-09)
        st_ = lambda s_, on: ({'status': 'off', 'why': SOURCE_DOWN[s_]} if s_ in SOURCE_DOWN else {'status': 'ok' if on else 'disabled'})
        sh_ = {'ts': datetime.now().strftime('%Y-%m-%d %H:%M'), 'audible': st_('audible', USE_AUDIBLE_API),
               'hardcover': dict(st_('hardcover', HARDCOVER_ENABLED), daily_left=HC_STATE.get('daily_left'),
                                 **({'status': 'off', 'why': HC_STATE['disabled']} if HARDCOVER_ENABLED and HC_STATE.get('disabled') and 'hardcover' not in SOURCE_DOWN else {})),
               'goodreads': {'status': 'blocked' if GR_STATE['blocked'] else ('ok' if GOODREADS_ENABLED else 'disabled')}}
        rw_json(os.path.join(SCRIPT_DIR, 'source_health.json'), sh_)
    write_env_file(log_file, start_time)
    log_run_summary(start_time, series_result)
    logging.info(f"Debug bundle (send this one for troubleshooting): {os.path.join(LOG_DIR, 'debug_latest.log')}")
    if SUMMARY_INDEX: rw_json(SUMMARY_INDEX_FILE, SUMMARY_INDEX)  # Audible summaries seen (description of another book)
    write_debug_bundle(log_file)

if __name__ == "__main__": main()
