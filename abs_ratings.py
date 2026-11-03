import os
import re
import time
import requests
from bs4 import BeautifulSoup
import re, json, random, difflib, logging, urllib.parse, unicodedata
import html as html_lib
import recorder; recorder.install()  # no-op unless RECORD_DIR / REPLAY_DIR is set (offline tests)
import atexit
from datetime import datetime, timezone, timedelta
from collections import defaultdict

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
    'audible_api': float(os.getenv('AUDIBLE_API_INTERVAL', 1)),
    'audible_web': float(os.getenv('AUDIBLE_WEB_INTERVAL', 3)),
    'goodreads': float(os.getenv('GOODREADS_INTERVAL', 10)),
    'hardcover': float(os.getenv('HARDCOVER_INTERVAL', 1.1)),  # API limit: 60 requests/min, 5000/day
}
USE_AUDIBLE_API = os.getenv('USE_AUDIBLE_API', 'true').lower() == 'true'
GOODREADS_ENABLED = os.getenv('GOODREADS_ENABLED', 'true').lower() == 'true'  # false = no Goodreads requests at all
HARDCOVER_TOKEN, _ = clean_env('HARDCOVER_TOKEN', r'A-Za-z0-9._\- ')
HARDCOVER_TOKEN = re.sub(r'(?i)^\s*bearer\s+', '', HARDCOVER_TOKEN).strip()
HARDCOVER_ENABLED = os.getenv('HARDCOVER_ENABLED', 'true').lower() == 'true' and bool(HARDCOVER_TOKEN)
HC_CACHE_FILE = os.path.join(SCRIPT_DIR, "hardcover_cache.json")  # key -> {'id': Hardcover book id, 'slug': ...}
# Order of the sources in the rating block (unknown names are ignored, missing ones are appended)
RATING_SOURCES = ['Audible', 'Goodreads', 'Hardcover']
RATING_ORDER = [s for s in (x.strip().capitalize() for x in os.getenv('RATING_ORDER', 'audible,goodreads,hardcover').split(',')) if s in RATING_SOURCES]
RATING_ORDER = list(dict.fromkeys(RATING_ORDER + RATING_SOURCES))
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
RUNTIME_TOLERANCE_PCT = float(os.getenv('RUNTIME_TOLERANCE_PCT', 15))
RUNTIME_TOLERANCE_MIN = float(os.getenv('RUNTIME_TOLERANCE_MIN', 20))
FILL_COVERS = os.getenv('FILL_COVERS', 'true').lower() == 'true'      # Set Audible cover when ABS has none
GENRE_LANGUAGE = os.getenv('GENRE_LANGUAGE', 'en').lower()            # 'en' = translate audible.de genres, 'keep' = as delivered
SERIES_MERGE = os.getenv('SERIES_MERGE', 'true').lower() == 'true'    # ABS-only series merge task at the end
LIBRARY_REPORT = os.getenv('LIBRARY_REPORT', 'true').lower() == 'true'
SERIES_ONLY = os.getenv('SERIES_ONLY', 'false').lower() == 'true'     # Only run the ABS-only series/report task
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
GOODREADS_MAX_PER_RUN = int(os.getenv('GOODREADS_MAX_PER_RUN', 300))

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

def map_genre(name, domain):
    """Translate audible.de genre names to English (GENRE_LANGUAGE=en)."""
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
RE_RATING_BLOCK = re.compile(r'(?s)⭐\s*Ratings.*?⭐(?:\s|<br\s*/?>)*')
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

stats = {k: 0 for k in ["processed", "success", "failed", "no_data", "skipped", "partial", "cooldown", "recycled", "asin_found", "isbn_added", "isbn_repaired", "asin_migrated", "meta_updated", "manual_retrigger", "no_ratings_yet", "gr_missing", "recheck", "gr_deferred", "cover_added", "desc_filled", "series_merged", "other_edition", "hc_found", "hc_missing"]}
stats['aborted_ratelimit'] = False
GR_STATE = {'blocked': False, 'cooldowns': 0, 'searches': 0}
GR_CACHE = {}  # Filled in main()  # Set once Goodreads blocks us; GR is then skipped for the rest of the run
reports = {"audible": {}, "goodreads": {}}
AUTHOR_NOTES = []  # Author spelling differences between ABS and Audible (report only)
SERIES_ALIASES, SERIES_INDEX = {}, {}  # Loaded in main()

def canonical_series(lib, name, book_authors):
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
    return name, None

def certainty_score(search_title, authors, narrators, abs_dur, publisher, year, abs_series, aud, hc=None, asin_from_search=False, review_open=False):
    """0-100: how sure are we that the product behind the ASIN is THIS book. Parts are the independent pieces of evidence (weights see ROADMAP.md):
    title 25, author 20, runtime 20 (±2 min) / 10 (inside tolerance), narrator 12, series/volume number 10, publisher+year 8, Hardcover ISBN/ASIN confirmation 5.
    Deductions: ASIN found by search -5, ratings of another edition -20, open review entry -15. Returns (score, {part: points})."""
    if not aud or not aud.get('title_raw'): return 0, {'no Audible product': 0}
    parts = {}
    t = max(search_match_score(search_title, aud['title_raw'], aud.get('subtitle_raw')), title_similarity(clean_title(search_title), clean_title(aud['title_raw'])))
    parts['title'] = round(25 * min(1.0, t))
    parts['author'] = 20 if match_author(authors, ', '.join(aud.get('authors_raw') or [])) else 0
    rt = aud.get('runtime_sec')
    parts['runtime'] = 0 if not (abs_dur and rt) else 20 if abs(abs_dur - rt) <= 120 else 10 if not runtime_mismatch(abs_dur, rt) else 0
    n = lambda xs: {re.sub(r'[^a-z]', '', x.lower()) for x in (xs or []) if x}
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
    norm = lambda n: re.sub(r'[^a-z]', '', n.lower())
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
    n = lambda xs: {re.sub(r'[^a-z]', '', x.lower()) for x in (xs or []) if x}
    return bool(n(abs_narrators) & n(aud.get('narrators_raw')))

BOXSET_RE = re.compile(r"\b\d+\s*[-–]\s*\d+\b|\b(trilog(y|ie)|box\s*set|boxed set|omnibus|sammelband|gesamtausgabe|publisher.?s pack|complete (series|collection)|collection)\b|\bbooks?\s+\d+\s*(?:-|–|&|and|,)\s*\d+", re.I)
def is_boxset_title(t): return bool(t and BOXSET_RE.search(t))

ASIN_OWNERS = {}  # asin -> set of item keys (ABS library + ASINs planned in this run); an ASIN is never given to a second book
ISBN_OWNERS = {}  # isbn -> set of item keys (an ISBN is never given to a second book)

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

def series_seq_conflict(abs_series, cand_series):
    """True if ABS and a candidate both state a number for the same series and the numbers differ (-> other volume)."""
    for ak, an in _series_pairs(abs_series):
        for ck, cn in _series_pairs(cand_series):
            if an is not None and cn is not None and (ak == ck or difflib.SequenceMatcher(None, ak, ck).ratio() >= 0.9) and an != cn:
                return True
    return False

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
def throttle(source):
    """Wait until MIN_INTERVAL[source] seconds have passed since the last request to that source."""
    if recorder.REPLAY_DIR or recorder.RECORD_DIR: return  # test mode: recorder.py waits only before LIVE requests
    wait = MIN_INTERVAL.get(source, 0) - (time.monotonic() - _last_request.get(source, 0))
    if wait > 0: time.sleep(wait + random.uniform(0, 0.5))
    _last_request[source] = time.monotonic()

def map_language(raw):
    if not raw: return None
    raw = str(raw).strip()
    return LANGUAGE_MAP.get(raw.lower(), raw[:1].upper() + raw[1:])

def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    f = os.path.join(LOG_DIR, f"run_{RUN_ID}.log" if os.getenv('RUN_ID') else f"run_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
    logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler(f, encoding='utf-8'), logging.StreamHandler()])
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    return f

# ================= CHANGE JOURNAL =================

def _plain(v):
    """Make ABS values journal-friendly (series/authors lose their internal ids)."""
    if isinstance(v, list):
        return [{k: x.get(k) for k in ('name', 'sequence') if k in x} if isinstance(x, dict) else x for x in v]
    return v

def journal_change(iid, key, title, field, old, new, source, extra=None):
    """Append one change to changes.jsonl (or the planned-changes file in DRY_RUN)."""
    entry = {'run': RUN_ID, 'ts': datetime.now(timezone.utc).isoformat(timespec='seconds'), 'item': iid, 'key': key,
             'title': title, 'field': field, 'old': _plain(old), 'new': _plain(new), 'source': source, 'dry_run': DRY_RUN}
    if extra: entry.update(extra)
    path = PLANNED_FILE if DRY_RUN else CHANGES_FILE
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'a', encoding='utf-8') as f: f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError as e: logging.error(f"Journal write failed: {e}")

def abs_update(iid, key, title, updates, old_meta, source, tags=None, old_tags=None, extra=None):
    """The only way this script writes metadata/tags to ABS. Journals every field (old -> new).
    In DRY_RUN nothing is sent, the change is only written to the planned-changes file."""
    if not updates and tags is None: return True
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
    if tags is not None: journal_change(iid, key, title, 'tags', old_tags, tags, source, extra)
    return True

def abs_set_cover(iid, key, title, url, source):
    """Set a cover from URL (only used when ABS has no cover). Undo removes the cover again."""
    if not DRY_RUN:
        r = abs_session.post(f"{ABS_URL}/api/items/{iid}/cover", json={"url": url})
        if r.status_code != 200:
            logging.error(f"        ❌ Cover upload failed for '{title}' (HTTP {r.status_code}): {r.text[:200]}")
            return False
    journal_change(iid, key, title, 'cover', None, url, source)
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
            tmp_path = path + ".tmp"
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
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
    elif stats['failed'] > 0: sub, icon, head = "ABS Ratings: Error ❌", "alert", "Error occurred!"
    elif any(stats[k] > 0 for k in ['success', 'recycled', 'asin_found', 'asin_migrated', 'isbn_added', 'meta_updated', 'series_merged']): sub, icon, head = "ABS Ratings: Success ✅", "normal", "Update complete"
    else: sub, icon, head = "ABS Ratings: Info ℹ️", "normal", "No changes"
    
    body = f"Proc: {stats['processed']} | New: {stats['success']} | ManUpd: {stats['manual_retrigger']} | MetaUpd: {stats['meta_updated']} | ASIN+: {stats['asin_found']} | Mig: {stats['asin_migrated']} | ISBN+: {stats['isbn_added']} | Fix: {stats['isbn_repaired']} | NoRatYet: {stats['no_ratings_yet']} | GR-: {stats['gr_missing']} | Cover+: {stats['cover_added']} | Series merged: {stats['series_merged']} | Err: {stats['failed']}"
    if GR_STATE['blocked']: body += " | ⚠️ Goodreads blocked"
    if stats['gr_deferred']: body += f" | GR deferred: {stats['gr_deferred']}"
    if stats['aborted_ratelimit']: body += " | ⚠️ ABORTED (Rate Limit)"
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
    v = safe_float(v)
    if v == 0: return "🌑" * 5
    full, decimal = int(v), v - int(v)
    if decimal >= 0.75:
        full += 1
        half = 0
    elif decimal >= 0.25:
        half = 1
    else:
        half = 0
    return ("🌕" * min(full, 5) + "🌗" * half).ljust(5, "🌑")[:5]

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
RE_SERIES_PREFIX = re.compile(r'^\s*([^:]{4,}?)\s+(0\d{1,2})\s+([^:].{2,})$')  # zero-padded like file names ('05')

RE_LEADING_NUM = re.compile(r'^\s*(0\d{1,2})\s*[-.:]?\s+(.{3,})$')  # '01 Nightside the Long Sun' (zero-padded only)

def parse_messy_title(t):
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
        if len(series.split()) >= 2 and (len(rest.split()) >= 2 or (len(rest) >= 5 and re.fullmatch(r"0\d", seq))):  # 'Prince of the Doomed City 01 Entranced'
            return {'title': rest, 'series': series, 'sequence': str(float(seq)).rstrip('0').rstrip('.'), 'kind': 'series_prefix'}
    if m := RE_LEADING_NUM.match(t):
        rest_ = m.group(2).strip()
        if len(rest_.split()) >= 2 or (re.fullmatch(r'0[1-9]', m.group(1)) and len(rest_) >= 5):  # '03 Blutmale' yes, '007 Skyfall' no
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
    return {x.lstrip('0') or '0' for x in raw}  # 'Vol. 02' == 'Vol. 2'

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
    for attempt in (1, 2):  # One retry for transient network errors
        throttle('goodreads' if source == 'goodreads' else 'audible_web')
        try:
            r = requests.get(url, headers=get_headers(domain), params=params, timeout=20)
            break
        except (requests.ConnectionError, requests.Timeout) as e:
            logging.warning(f"        🌐 Request failed (try {attempt}/2): {url} ({type(e).__name__})")
            if attempt == 1: time.sleep(3)
        except requests.RequestException as e:
            logging.warning(f"        🌐 Request failed: {url} ({type(e).__name__}: {e})")
            break
    if r is None: return None, None

    status, final_url = r.status_code, r.url
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

def get_audible_api(asin, domain):
    """Query the Audible catalog API for one marketplace.
    Returns a result dict in the same shape as the web scraper, 'missing' if the product
    is not available there, or None on errors (caller falls back to web scraping)."""
    host = AUDIBLE_API_HOSTS.get(domain)
    if not host: return None
    throttle('audible_api')
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

    res = {'domain': domain, 'source': 'API', 'page_ok': True, 'title_raw': p.get('title'), 'asin': p.get('asin') or asin,
           'authors_raw': [a.get('name') for a in (p.get('authors') or []) if a.get('name')],
           'authors_asin': [{'name': a.get('name'), 'asin': a.get('asin')} for a in (p.get('authors') or []) if a.get('name') and a.get('asin')],
           'runtime_sec': int(p.get('runtime_length_min') or 0) * 60}
    if p.get('subtitle'): res['subtitle_raw'] = p['subtitle']
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
    res['adult'] = bool(p.get('is_adult_product'))
    imgs = {k: v for k, v in (p.get('product_images') or {}).items() if str(k).isdigit() and v}
    if imgs: res['cover_url'] = imgs[max(imgs, key=int)]
    res['meta_raw'] = {
        'language': p.get('language'),
        'publisher': {'name': p.get('publisher_name')} if p.get('publisher_name') else None,
        'releaseDate': p.get('release_date') or p.get('issue_date'),
        'format': p.get('format_type'),
        'categories': [{'name': c} for c in categories],
        'series': [{'name': s.get('title'), 'part': s.get('sequence'), 'asin': s.get('asin')} for s in (p.get('series') or []) if s.get('title')],
    }
    note_series(p.get('series'))
    return res

def get_audible_data(asin, language):
    """Audible ratings + metadata. Uses the catalog API first and only falls back to
    scraping the product pages when the API can't answer for any marketplace."""
    if not asin: return None
    if not USE_AUDIBLE_API: return get_audible_data_web(asin, language)
    api_error, missing = False, []
    for domain in audible_domains(language):
        res = get_audible_api(asin, domain)
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
    logging.info(f"      -> Audible API: ASIN not available on {', '.join(missing) or '-'}{' (API errors)' if api_error else ''}, falling back to web scraping...")
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
            head, tail = abs_title.split(sep, 1)
            if normalize_title_text(head) == nf and set(normalize_title_text(tail).split()) & MARKETING_WORDS: score = max(score, 0.9)
    if extract_volume(found_title) and not extract_volume(abs_title):
        score = min(score, 0.75)  # 'Hell Mode' vs 'Hell Mode, Volume 3': could be any volume
    return score

LAST_SEARCH = {'related': None}  # best same-title/same-author candidate of the last search (other edition)

# ---------------- Audible series listing path (finds products the catalog search does not return) ----------------
SERIES_ASIN_FILE = os.path.join(SCRIPT_DIR, 'audible_series.json')  # {series title: [series asin]}, filled from every product we load
SERIES_ASINS = {}      # raw series title -> set(series asin)
_SERIES_CHILDREN = {}  # series asin -> [child asin]
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
        with open(SERIES_ASIN_FILE, 'w', encoding='utf-8') as f: json.dump({t: sorted(a) for t, a in SERIES_ASINS.items()}, f, ensure_ascii=False)
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
    if len(names) == 1 and len(aa) == 1 and aa[0].get('asin'): AUTHOR_LINKS.setdefault(names[0], set()).add(aa[0]['asin'])

def save_author_links():
    try:
        with open(AUTHOR_LINKS_FILE, 'w', encoding='utf-8') as f: json.dump({n: sorted(a) for n, a in AUTHOR_LINKS.items()}, f, ensure_ascii=False)
    except OSError: pass
atexit.register(save_author_links)

def series_candidates(abs_series, authors_list, duration, domain):
    """ASINs of books in the same Audible series as ABS's series. Audible's catalog search misses some (older) products, a series
    listing does not. Children are prefiltered with ONE bulk request: runtime within 10 min and matching author."""
    from abs_series import series_key
    keys = [series_key(x.get('name') or '') for x in (abs_series or []) if x.get('name')]
    wanted, ranked = set(), []
    words = lambda s: {w for w in s.split() if len(w) >= 4}
    for title, asins in SERIES_ASINS.items():
        sk = series_key(title)
        if any(k and (sk == k or sk.startswith(k + ' ')) for k in keys): wanted |= asins; continue
        # looser: ABS 'Dune Universe' vs Audible 'DUNE: Prelude to Dune' share a significant word (candidates are filtered hard afterwards)
        shared = max((len(words(sk) & words(k)) for k in keys if k), default=0)
        if shared: ranked.append((-shared, len(sk), sorted(asins)))
    for _, _, asins in sorted(ranked)[:10]: wanted |= set(asins)
    if not wanted or not duration: return []
    host, hdr = AUDIBLE_API_HOSTS[domain], {"User-Agent": FIXED_USER_AGENT, "Accept": "application/json"}
    children = []
    for sa in sorted(wanted)[:14]:
        if sa not in _SERIES_CHILDREN:
            throttle('audible_api')
            try:
                r = requests.get(f"https://{host}/1.0/catalog/products/{sa}", params={"response_groups": "relationships"}, headers=hdr, timeout=15)
                rel = (r.json().get('product') or {}).get('relationships') or [] if r.status_code == 200 else []
            except (requests.RequestException, ValueError): rel = []
            _SERIES_CHILDREN[sa] = [x['asin'] for x in rel if x.get('relationship_to_product') == 'child' and x.get('asin')]
        children += [c for c in _SERIES_CHILDREN[sa] if c not in children]
    out = []
    for i in range(0, len(children), 50):
        throttle('audible_api')
        try:
            r = requests.get(f"https://{host}/1.0/catalog/products", params={"asins": ",".join(children[i:i + 50]), "response_groups": "product_attrs,contributors,media"},
                             headers=hdr, timeout=20)
            prods = r.json().get('products') or [] if r.status_code == 200 else []
        except (requests.RequestException, ValueError): prods = []
        for p in prods:
            rt = int(p.get('runtime_length_min') or 0) * 60
            if rt and abs(rt - duration) <= max(600, 0.25 * duration) and match_author(authors_list, ', '.join(a.get('name') or '' for a in (p.get('authors') or []))):
                out.append((abs(rt - duration), p["asin"]))
    return [a for _, a in sorted(out)]  # closest runtime first


LAST_MATCH = {}  # evidence of the last accepted search hit: {asin, confirmed, evidence[]}
SEARCH_CTX = {}  # {'old_title','old_authors'}: what the current ASIN in ABS points to (set by the caller)
REVIEW_FILE = os.path.join(SCRIPT_DIR, 'review_queue.json')

def add_review(key, title, kind, candidate, reason, evidence=None):
    """Uncertain automatic decision: nothing is written, the user decides in the review list (review_queue.json)."""
    decided = rw_json(os.path.join(SCRIPT_DIR, 'review_decisions.json')) or {}
    if f"{kind}|{key}|{candidate}" in decided: return  # user already decided (accepted or rejected)
    q = rw_json(REVIEW_FILE) or []
    if not isinstance(q, list): q = []
    if any(x.get('key') == key and x.get('kind') == kind and x.get('candidate') == candidate for x in q): return
    q.append({'key': key, 'title': title, 'kind': kind, 'candidate': candidate, 'reason': reason, 'evidence': evidence or [],
              'run': RUN_ID, 'ts': datetime.now(timezone.utc).isoformat(timespec='seconds'), 'status': 'open'})
    rw_json(REVIEW_FILE, q)

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
    narrators = [n for n in (narrators or []) if not any(fold_accents(n).lower() == fold_accents(a).lower() for a in (authors_list or []))]  # an ABS "narrator" equal to the author is a data error, not evidence
    prim_auth = authors_list[0] if authors_list else ""
    short = re.split(r'\s*[:(]\s*|\s+-\s+', title)[0].strip()  # 'Dragon's Kin: Dragonriders of Pern, Book 16' -> 'Dragon's Kin'
    queries = [{"keywords": f"{title} {prim_auth}".strip()}]
    if prim_auth: queries.append({"title": title, "author": prim_auth})
    if short and short != title and len(short) >= 4:
        queries.append({"title": short, "author": prim_auth} if prim_auth else {"keywords": short})
    if raw_title and raw_title != title: queries.append({"keywords": raw_title})
    queries.append({"keywords": title})
    if narrators and narrators[0]: queries.append({"keywords": f"{title} {prim_auth} {narrators[0]}".strip()})  # generic titles ('The Prince'): the narrator surfaces the right recording
    nq = {"author": prim_auth, "narrator": narrators[0]} if (narrators and narrators[0] and prim_auth) else None  # last resort only (see end of function)
    nq2 = {"keywords": title, "narrator": narrators[0]} if (narrators and narrators[0]) else None
    if only_queries: queries = list(only_queries)
    LAST_SEARCH['related'] = None
    if not only_queries: LAST_SEARCH['weak'] = None
    api_ok = False
    near_pick = None  # accepted only via 'same narrator, runtime inside tolerance'; used if no candidate with exact runtime follows
    best_pick = None  # accepted with an INEXACT title: keep looking, a later candidate may match the title exactly ('The Wild Robot' vs '...: Booktrack Edition')
    if only_extra: queries = []  # series-listing path: no search queries, only the given candidates
    domains = [only_domain] if only_domain else search_domains(required_lang or lang)
    if only_extra: domains = domains[:1]
    for domain in domains:
        host, candidates = AUDIBLE_API_HOSTS[domain], list(extra_candidates or [])
        for q in queries:
            throttle('audible_api')
            try:
                r = requests.get(f"https://{host}/1.0/catalog/products", params={**q, "num_results": 5, "response_groups": "product_attrs"},
                                 headers={"User-Agent": FIXED_USER_AGENT, "Accept": "application/json"}, timeout=15)
                if r.status_code == 429: raise RateLimitException(f"HTTP 429 on {host} (API search)", True)
                if r.status_code != 200: continue
                api_ok = True
                logging.debug(f"        search {host} {q}: {len(r.json().get('products') or [])} results")
                for prod in r.json().get('products') or []:
                    if prod.get('asin') and prod['asin'] not in candidates: candidates.append(prod['asin'])
            except (requests.RequestException, ValueError) as e:
                logging.warning(f"        🌐 Audible API search failed ({host}): {type(e).__name__}")
            if len(candidates) >= 4: break
        if not candidates and not only_extra: logging.info(f"        ↪️ No search results on {domain} ({len(queries)} queries)")
        for cand in candidates[:max(6, len(extra_candidates or []))]:  # series-listing candidates are all checked (closest runtime first)
            p = get_audible_api(cand, domain)
            if not isinstance(p, dict): continue
            cand_lang = map_language((p.get('meta_raw') or {}).get('language'))
            if required_lang and cand_lang and cand_lang != required_lang:
                logging.info(f"        ↪️ Rejected '{p.get('title_raw')}' [{cand}] (language {cand_lang}, need {required_lang})")
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
                                     and {re.sub(r'[^a-z]', '', n.lower()) for n in (narrators or []) if n} & {re.sub(r'[^a-z]', '', n.lower()) for n in (p.get('narrators_raw') or []) if n})
            if f_nums and t_nums and not f_nums & t_nums and not self_inconsistent:  # different volume: never the same book (unless ABS contradicts itself and runtime + narrator agree)
                logging.info(f"        ↪️ Rejected '{found_title}' [{cand}] (volume {','.join(sorted(f_nums))} vs {','.join(sorted(t_nums))})")
                continue
            dup_resolved = False
            if series_seq_conflict(abs_series, (p.get('meta_raw') or {}).get('series')):  # ABS and Audible number the same series differently
                # Exception: two ABS books claim the same ASIN (one ABS number must be wrong) and this sibling product matches the file
                # length to the minute -> the runtime decides, not the (unreliable) ABS series number.
                exact_rt = bool(duration and p.get('runtime_sec') and abs(duration - p['runtime_sec']) <= 120)
                if not (SEARCH_CTX.get('dup_claim') and exact_rt):
                    logging.info(f"        ↪️ Rejected '{found_title}' [{cand}] (series number differs from ABS)")
                    continue
                dup_resolved = True
            auth_ok = match_author(authors_list, ", ".join(p.get('authors_raw') or []))
            dur_ok = bool(duration and p.get('runtime_sec') and abs(duration - p['runtime_sec']) < 600)
            dur_conflict = bool(duration and p.get('runtime_sec') and not dur_ok)
            norm_n = lambda n: re.sub(r'[^a-z]', '', n.lower())
            a_narr, c_narr = {norm_n(n) for n in (narrators or []) if n}, {norm_n(n) for n in (p.get('narrators_raw') or []) if n}
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
            norm_n = lambda n: re.sub(r'[^a-z]', '', n.lower())
            a_narr, c_narr = {norm_n(n) for n in (narrators or []) if n}, {norm_n(n) for n in (p.get('narrators_raw') or []) if n}
            # Author written in another script (ABS 'Rifujin na Magonote' vs Audible '理不尽な孫の手'): no contradiction if narrator and runtime (+-2 min) agree
            if not auth_ok and a_narr and c_narr and a_narr & c_narr and duration and p.get('runtime_sec') and abs(duration - p['runtime_sec']) <= 120:
                cand_auth = ", ".join(p.get('authors_raw') or [])
                if re.search(r'[^\x00-ɏ]', cand_auth) and not any(re.search(r'[^\x00-ɏ]', a) for a in authors_list):
                    auth_ok = True
                    logging.info(f"        ℹ️ Author '{cand_auth}' is written in another script; narrator and runtime agree, so it is not treated as a mismatch")
            if a_narr and c_narr and not a_narr & c_narr:  # other recording (e.g. UK vs US narrator): known and different narrators veto the hit, with or without an existing ASIN
                logging.info(f"        ↪️ Rejected '{found_title}' [{cand}] (narrator {', '.join(p.get('narrators_raw') or [])} differs)")
                continue
            # same narrator + runtime inside the library tolerance (15 % / 20 min): same recording, ABS file just differs a little
            dur_near_n = bool(duration and p.get("runtime_sec") and not runtime_mismatch(duration, p["runtime_sec"]) and a_narr and c_narr and a_narr & c_narr)
            na_w, nf_w = set(normalize_title_text(title).split()), set(normalize_title_text(found_title).split())
            exact_rt = bool(duration and p.get('runtime_sec') and abs(duration - p['runtime_sec']) <= 120)
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
                LAST_MATCH.clear(); LAST_MATCH.update({'asin': cand, 'confirmed': confirmed, 'evidence': evidence, 'title': found_title})
                msg = (f"        ✅ API match on {domain}: '{found_title}' [{cand}] (title {t_score:.2f}, author {'✓' if auth_ok else '✗'}, "
                       f"runtime {'✓' if dur_ok else '≈ (same narrator, inside tolerance)'}) evidence: {ev_txt}")
                if strict and not dur_ok and dur_near_n:  # keep looking: a candidate with exact runtime beats an approximate one
                    if near_pick is None: near_pick = (cand, dict(LAST_MATCH), msg)
                    continue
                if t_score < 0.99:
                    if best_pick is None or (round(t_score, 2), dur_ok) > best_pick[0]: best_pick = ((round(t_score, 2), dur_ok), cand, dict(LAST_MATCH), msg)
                    continue
                logging.info(msg)
                return cand
            if duration and p.get('runtime_sec') and abs(duration - p['runtime_sec']) <= 120 and a_narr and c_narr and a_narr & c_narr and not LAST_SEARCH.get('weak'):
                LAST_SEARCH['weak'] = {'asin': cand, 'title': found_title, 'title_score': round(t_score, 2), 'author_ok': bool(auth_ok)}  # runtime to the minute + narrator agree, title/author do not: a human decides
            logging.info(f"        ↪️ Rejected '{found_title}' [{cand}] (title {t_score:.2f}, author {'✓' if auth_ok else '✗'}, runtime {'✓' if dur_ok else '✗' if dur_conflict else '?'})")
        if best_pick: break  # do not search further stores when this store already has an acceptable hit
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
    return None if api_ok else False

def find_missing_asin(title, authors_list, duration, lang, raw_title=None, required_lang=None, strict=False,
                      only_domain=None, narrators=None, abs_series=None):
    logging.info(f"      -> 🔎 Searching Replacement ASIN for '{title}'{f' on {only_domain}' if only_domain else ''}"
                 f"{' (strict: title, author, runtime and narrator must match)' if strict else ''}...")
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
                logging.warning(f"        ⏸️ Goodreads blocks requests: {e}. Cool-down {GR_BLOCK_COOLDOWN}s, then retrying ({GR_STATE['cooldowns']}/{GR_MAX_COOLDOWNS})...")
                time.sleep(GR_BLOCK_COOLDOWN)
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

def _goodreads_lookup(isbn, title, authors, prim_auth, cached_url, hc_ids=None):
    # Option 1: Goodreads IDs from Hardcover -> the book page directly (1 request, no search)
    for gid in (hc_ids or [])[:GR_ID_TRIES]:
        d = scrape_gr_details(f"https://www.goodreads.com/book/show/{gid}", require_book_page=True)
        if not d: continue
        ok, t, a_ok = gr_page_matches(d, title, authors)
        logging.info(f"        🔗 Goodreads page via Hardcover ID {gid}: '{d.get('name') or d.get('page_title')}' title {t:.2f}, author {'?' if a_ok is None else '✓' if a_ok else '✗'}")
        if ok and d.get('count'):
            d['source'] = 'Hardcover-ID'
            logging.info(f"        ✅ Goodreads via Hardcover ID (Count: {d.get('count')}, Rating: {round(safe_float(d.get('val')), 2)})")
            return d, 'found'
    if cached_url:
        if d := scrape_gr_details(cached_url, require_book_page=True):
            d['source'] = 'Cache'
            logging.info(f"        ✅ Found via cached URL (Count: {d.get('count')}, Rating: {round(safe_float(d.get('val')), 2)})")
            return d, 'found'
        logging.info("        ↪️ Cached Goodreads URL failed, searching again...")
    if GOODREADS_MAX_PER_RUN and GR_STATE['searches'] >= GOODREADS_MAX_PER_RUN:
        if GR_STATE['searches'] == GOODREADS_MAX_PER_RUN:
            logging.info(f"        ⏭️ Goodreads search budget for this run reached ({GOODREADS_MAX_PER_RUN}). Remaining books follow in the next runs.")
            GR_STATE['searches'] += 1  # Log only once
        stats['gr_deferred'] += 1
        return None, 'deferred'
    GR_STATE['searches'] += 1
    d = _search_goodreads(isbn, title, authors, prim_auth)
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

def _search_goodreads(isbn, title, authors, prim_auth):
    # Audible ASINs practically never resolve on Goodreads, so only the ISBN is tried
    for q_id, src in [(normalize_isbn(isbn), 'ISBN Lookup')]:
        if q_id:
            # /book/isbn/<isbn> redirects straight to the book page (1 request instead of search + page)
            if d := scrape_gr_details(f"https://www.goodreads.com/book/isbn/{q_id}", require_book_page=True):
                d['source'] = src
                logging.info(f"        ✅ Found via {src} (Count: {d.get('count')}, Rating: {round(safe_float(d.get('val')), 2)})")
                return d
    base_title = clean_title(title)
    searches = []
    for q in [f"{title} {prim_auth}", f"{base_title} {prim_auth}", title, base_title]:
        q = q.strip()
        if q and q not in searches: searches.append(q)  # No duplicate requests
    norm_target = normalize_title_text(title)

    for q in searches:
        r, soup = fetch_url(f"https://www.goodreads.com/search", params={"q": q}, source='goodreads')
        if not soup: continue
        if "/book/show/" in r.url:
            if d := scrape_gr_details(r.url): 
                d['source'] = 'Text Search (Direct Hit)'
                logging.info(f"        ✅ Found via Text Search (Direct) (Count: {d.get('count')}, Rating: {round(safe_float(d.get('val')), 2)})")
                return d
        else:
            best_url, best_score, best_rating = None, 0.0, None
            results = parse_gr_search_results(soup)
            if not results:
                logging.info(f"        🔍 GR search '{q}': no results parsed (title: '{page_title(soup)}', {len(r.text)} bytes)")
                if len(r.text) > 50000: save_gr_sample(r.text, q)
            for found_title, found_url, found_auth, found_rating in results:
                norm_found = normalize_title_text(found_title)
                t_score = difflib.SequenceMatcher(None, norm_target, norm_found).ratio()
                if (len(norm_target) > 3 and norm_target in norm_found) or (len(norm_found) > 3 and norm_found in norm_target): t_score += 0.15
                f_nums, t_nums = extract_volume(found_title), extract_volume(title)
                if (f_nums and t_nums and not f_nums & t_nums): 
                    if t_score < 0.9: continue
                if not match_author(authors, found_auth): continue
                if t_score > 0.75 and t_score > best_score:
                    best_score, best_url, best_rating = t_score, found_url, found_rating
            if best_url and best_rating:
                # Rating is already in the result list: no need to load the book page
                d = {'url': best_url, 'val': best_rating[0], 'count': best_rating[1], 'source': 'Text Search (List)'}
                logging.info(f"        ✅ Found via Text Search (List, no page load) (Count: {d['count']}, Rating: {round(safe_float(d['val']), 2)})")
                return d
            if best_url:
                if d := scrape_gr_details(best_url): 
                    d['source'] = 'Text Search (List Match)'
                    logging.info(f"        ✅ Found via Text Search (List) (Count: {d.get('count')}, Rating: {round(safe_float(d.get('val')), 2)})")
                    return d
    logging.info("        ❌ Not found via ID or Text.")
    return None

# ================= HARDCOVER =================
# GraphQL API (https://api.hardcover.app/v1/graphql). Only ratings are taken, never metadata.
# Lookup order: manual link -> cached book id -> ASIN/ISBN edition -> title+author search (strict match).
HC_URL = 'https://api.hardcover.app/v1/graphql'
HC_STATE = {'disabled': None, 'requests': 0, 'daily_left': None, 'cooldowns': 0}
HC_CACHE, HC_PREFETCH = {}, {}  # key -> {'id', 'slug'} (persisted) | book id -> book (bulk refresh of cached books)
HC_DAILY_RESERVE = 25  # stop before the daily limit is used up completely
HC_BOOK_FIELDS = "id title subtitle slug rating ratings_count compilation cached_featured_series contributions { author { name } } book_mappings(where:{platform_id:{_eq:1}}, limit:8){ external_id }"
# Hits that are not the book itself (unless the ABS title says so too)
RE_HC_SPECIAL = re.compile(r'(?i)\b(graphic novel|comic|manga|coloring|colouring|adventure module|sourcebook|summary of|study guide|'
                           r'sparknotes|cliffsnotes|box(?:ed)? set|boxset|omnibus|collection|collector\W?s edition|books? \d+\s*[-–&]\s*\d+|summary|workbook)\b')
# Hardcover main titles that name a whole series ('The High Druid of Shannara Trilogy')
RE_HC_SET_TITLE = re.compile(r'(?i)\b(trilogy|duology|quartet|quintet|series|saga|set|bundle|collection|books?\s*\d+\s*[-–]\s*\d+)\s*$')
RE_HC_SLUG = re.compile(r'hardcover\.app/books/([\w-]+)')

def hc_query(query, variables=None):
    """One GraphQL request. Returns the data dict or None. Raises SourceBlockedException on HTTP 429."""
    if HC_STATE['disabled']: return None
    throttle('hardcover')
    HC_STATE['requests'] += 1
    try:
        r = requests.post(HC_URL, json={'query': query, 'variables': variables or {}}, timeout=20,
                          headers={'authorization': f"Bearer {HARDCOVER_TOKEN}", 'content-type': 'application/json',
                                   'user-agent': 'abs-ratings (Audiobookshelf rating script)'})
    except requests.RequestException as e:
        logging.info(f"        ⚠️ Hardcover request failed: {e}")
        return None
    left = r.headers.get('X-Ratelimit-Daily-Remaining') or ''
    if left.isdigit(): HC_STATE['daily_left'] = int(left)
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

def hc_goodreads_ids(book):
    """Goodreads book IDs Hardcover knows for this book (one per edition; the rating belongs to the work, any edition page shows it)."""
    return [m["external_id"] for m in (book or {}).get("book_mappings") or [] if str(m.get("external_id", "")).isdigit()]

def hc_result(book, source):
    return {'gr_ids': hc_goodreads_ids(book), 'id': book.get('id'), 'slug': book.get('slug'), 'url': f"https://hardcover.app/books/{book.get('slug')}",
            'val': book.get('rating'), 'count': int(book.get('ratings_count') or 0), 'source': source, 'title': book.get('title')}

def hc_authors(book):
    return [((c or {}).get('author') or {}).get('name') for c in (book.get('contributions') or [])] or book.get('author_names') or []

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
    # Our title has a number but the Hardcover title has none ('Reborn as the Villain's Lackey, Volume 2' vs the first book)
    if pure and all(extract_volume(t) for t in targets) and not theirs and not series_ok: return False, score, f"no volume number on Hardcover (ours: {sorted(ours)})"
    if (m := RE_HC_SET_TITLE.search(book.get('title') or '')) and not any(RE_HC_SET_TITLE.search(t) for t in targets):
        return False, score, f"whole series ('{m.group(0)}')"
    found_text = f"{book.get('title') or ''} {book.get('subtitle') or ''}"
    if (m := RE_HC_SPECIAL.search(found_text)) and not any(RE_HC_SPECIAL.search(t) for t in targets):
        return False, score, f"other format ('{m.group(0)}')"
    if book.get('compilation') and not any(RE_HC_SPECIAL.search(t) for t in targets):
        # 'Midnight Tides' is flagged as compilation because Parts 1+2 exist separately, but it sits as ONE book (single position) in its series
        pos = fs.get('position')
        single = isinstance(pos, (int, float)) and float(pos).is_integer()
        if not (score >= 0.99 and single): return False, score, "compilation"
    return True, max(score, 1.0 if series_confirms else 0), ''

def get_hardcover_data(key, targets, authors, asin, isbn, manual_url=None, use_cache=True, abs_series=()):
    """Returns (data, status) with status in 'found', 'not_found', 'blocked', 'disabled'."""
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
            if HC_STATE['cooldowns'] < 2:
                HC_STATE['cooldowns'] += 1
                logging.warning(f"        ⏸️ Hardcover rate limit ({e}). Waiting 60s ({HC_STATE['cooldowns']}/2)...")
                time.sleep(60); continue
            HC_STATE['disabled'] = 'rate limit'
            logging.warning("        🛑 Hardcover still rate limited. Skipping Hardcover for the rest of this run.")
            return None, 'blocked'

def _hardcover_lookup(key, targets, authors, asin, isbn, manual_url, use_cache, abs_series):
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

    # 1 request for all identifiers. An ID hit still has to fit (ISBNs in ABS are sometimes wrong).
    conds = []
    if asin and re.fullmatch(r'[A-Z0-9]{10}', asin): conds.append({'asin': {'_eq': asin}})
    if i := normalize_isbn(isbn): conds.append({'isbn_13' if len(i) == 13 else 'isbn_10': {'_eq': i}})
    if conds:
        data = hc_query(f"query($w:editions_bool_exp!){{ editions(where:$w, limit:5){{ title asin isbn_13 isbn_10 book{{ {HC_BOOK_FIELDS} }} }} }}", {'w': {'_or': conds}})
        best = None
        for ed in (data or {}).get('editions') or []:
            book = ed.get('book') or {}
            ok, score, why = hc_acceptable(targets, authors, book, 0.6, abs_series, [ed.get('title')])
            via = 'ASIN' if asin and ed.get('asin') == asin else 'ISBN'
            if not ok: logging.info(f"        ✖ {via} hit '{book.get('title')}' rejected: {why}"); continue
            if not best or (book.get('ratings_count') or 0) > (best[0].get('ratings_count') or 0): best = (book, via)
        if best: return hc_result(best[0], best[1])

    prim = authors[0] if authors else ''
    queries = []
    # Title + author first; title alone finds books whose author spelling or extra contributors confuse the search
    for q in ([f"{targets[0]} {prim}", f"{clean_title(targets[0])} {prim}", targets[0]] if targets else []):
        q = q.strip()
        if q and q not in queries: queries.append(q)
    for q in queries[:3]:
        data = hc_query("query($q:String!){ search(query:$q, query_type:\"Book\", per_page:8, page:1){ results } }", {'q': q})
        hits = (((data or {}).get('search') or {}).get('results') or {}).get('hits') or []
        cands = []
        for h in hits:
            doc = h.get('document') or {}
            try: doc['id'] = int(doc.get('id'))
            except (TypeError, ValueError): continue
            ok, score, why = hc_acceptable(targets, authors, doc, 0.85, abs_series, pure=True)
            if ok: cands.append((score, doc))
            else: logging.debug(f"        ✖ HC '{doc.get('title')}' rejected: {why}")
        if cands:
            top = max(s for s, _ in cands)
            # Several near-equal titles (duplicates on Hardcover): take the one with the most ratings
            score, doc = max((c for c in cands if c[0] >= top - 0.05), key=lambda c: (c[1].get('ratings_count') or 0, c[0]))
            logging.info(f"        🔍 Hardcover search '{q}': '{doc.get('title')}' accepted (score {score:.2f}, {len(hits)} hits)")
            return hc_result(doc, 'search')
        logging.info(f"        🔍 Hardcover search '{q}': no matching hit ({len(hits)} hits)")
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
        norm_n = lambda n: re.sub(r'[^a-z]', '', n.lower())
        a_narr, c_narr = {norm_n(n) for n in (narrators or []) if n}, {norm_n(n) for n in (p.get('narrators_raw') or []) if n}
        narr_contra = bool(a_narr and c_narr and not a_narr & c_narr)
        logging.info(f"        🔗 Hardcover edition ASIN {asin} ('{p['title_raw']}'): author {'✓' if auth_ok else '✗'}, runtime {'✓' if dur_ok else '✗'}, "
                     f"title vs ABS {t_abs:.2f}, vs Hardcover {t_hc:.2f}{' (ISBN-confirmed)' if hc_solid else ''}{', narrator differs' if narr_contra else ''}")
        if auth_ok and dur_ok and not narr_contra and (t_abs >= 0.85 or (hc_solid and t_hc >= 0.85)):
            return asin, ['hardcover-edition-asin', 'author', 'runtime', 'title' if t_abs >= 0.85 else 'title-via-hardcover-isbn'], p
    return None

def hc_replacement_isbn(hc, asin, abs_dur):
    """ISBN-13 of a Hardcover edition of the (verified) book: the edition carrying our ASIN or the audio length first, otherwise any print edition.
    Returns (isbn, how) or None."""
    data = hc_query("query($id:Int!){ editions(where:{book_id:{_eq:$id}, isbn_13:{_is_null:false}}, limit:25){ isbn_13 asin audio_seconds } }", {'id': hc['id']})
    eds = [(normalize_isbn(e.get('isbn_13')), e) for e in ((data or {}).get('editions') or [])]
    eds = [(i, e) for i, e in eds if i and isbn_checksum_ok(i)]
    for i, e in eds:
        if asin and e.get('asin') == asin: return i, 'edition with our ASIN'
    for i, e in eds:
        if abs_dur and e.get('audio_seconds') and abs(e['audio_seconds'] - abs_dur) <= 600: return i, 'audio edition with matching length'
    return (eds[0][0], 'other edition of the same book') if eds else None

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
        if line.startswith('⭐'): cur = None; continue
        if m := RE_BLOCK_HEAD.match(line): cur = m.group(1); out[cur] = [line]
        elif cur and line: out[cur].append(line)
    return {k: '<br>'.join(v) for k, v in out.items()}

def source_lines(src, d):
    """Rating lines of one source, or None if it has no usable rating."""
    if not d: return None
    if src == 'Audible':
        if int(d.get('count', 0) or 0) <= 0: return None
        lines = [f"Audible ({d.get('count')}):"]
        if v := d.get('overall'): lines.append(f"🏆 {moon_rating(v)} {round(safe_float(v), 1)} / 5 - Overall")
        if v := d.get('performance'): lines.append(f"🎙️ {moon_rating(v)} {round(safe_float(v), 1)} / 5 - Performance")
        if v := d.get('story'): lines.append(f"📖 {moon_rating(v)} {round(safe_float(v), 1)} / 5 - Story")
        return lines
    if src == 'Hardcover' and (not d.get('count') or not is_valid_rating(d.get('val'))): return None
    lines = [f"{src} ({d.get('count', 0)}):"]
    if (v := d.get('val')) and is_valid_rating(v): lines.append(f"🏆 {moon_rating(v)} {round(safe_float(v), 1)} / 5 - Rating")
    return lines

def build_description(current_desc, fresh, old):
    """fresh: {source: data from this run}, old: {source: text from the existing block} (kept if nothing new).
    Sources appear in RATING_ORDER."""
    lines = ["⭐ Ratings & Infos"]
    for src in RATING_ORDER:
        if new := source_lines(src, fresh.get(src)): lines += new
        elif old.get(src):
            stats['recycled'] += 1; lines.append(old[src])

    clean_d = RE_RATING_BLOCK.sub('', current_desc)
    clean_d = re.sub(r'(?s)\*\*Audible\*\*.*?---\s*\n*', '', clean_d)
    clean_d = re.sub(r'^(?:\s|<br\s*/?>)+', '', clean_d, flags=re.I).strip()
    if len(lines) == 1: return clean_d  # No ratings at all: don't write an empty rating block
    lines.append("⭐")
    return "<br>".join(lines) + "<br>" + clean_d

RESULTS, OVERRIDES = {}, {}  # loaded in main()

class ItemCapture(logging.Handler):
    """Collects the log lines of the book that is being processed (shown in the web UI problem view)."""
    def __init__(self): super().__init__(); self.lines = []
    def emit(self, record):
        if len(self.lines) < 80: self.lines.append(record.getMessage().strip())
ITEM_LOG = ItemCapture()

def set_result(key, lib_id, iid, title, authors, asin, status, reason, extra=None):
    RESULTS[key] = {'lib': lib_id, 'id': iid, 'title': title, 'author': ', '.join(authors or []), 'asin': asin,
                    'status': status, 'reason': reason, 'run': RUN_ID, 'dry_run': DRY_RUN,
                    'ts': datetime.now(timezone.utc).isoformat(timespec='seconds'), 'log': list(ITEM_LOG.lines), **(extra or {})}

def record_attempt(key, history, failed, recheck, complete=False, recheck_days=None):
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
        batch = []
        for lib_id in {k.partition('_')[0] for k in ONLY_ITEMS}:  # duplicate-ASIN guard needs the whole library, also in single-book mode
            try:
                for it in abs_session.get(f"{ABS_URL}/api/libraries/{lib_id}/items").json()['results']:
                    a = ((it.get('media') or {}).get('metadata') or {}).get('asin')
                    if a: ASIN_OWNERS.setdefault(a, set()).add(f"{lib_id}_{it['id']}")
                    i_ = normalize_isbn(((it.get('media') or {}).get('metadata') or {}).get('isbn'))
                    if i_: ISBN_OWNERS.setdefault(i_, set()).add(f"{lib_id}_{it['id']}")
            except Exception as e: logging.warning(f"Could not load ASIN owners for {lib_id}: {e}")
        for k in ONLY_ITEMS:
            lib, _, iid = k.partition('_')
            batch.append({'id': iid, '_lib': lib, 'media': {}})
        logging.info(f"Single-book mode: {', '.join(ONLY_ITEMS)}")
        return process_queue(batch, history, failed, recheck)
    pool = []
    for lib in LIBRARY_IDS: pool += build_queue(lib, history, recheck)
    random.shuffle(pool)
    batch = pool[:MAX_BATCH_SIZE]
    per_lib = defaultdict(int)
    for it in batch: per_lib[it['_lib']] += 1
    logging.info(f"Pool: {len(pool)} due books in {len(LIBRARY_IDS)} libraries. This run: {len(batch)} random books "
                 f"({', '.join(f'{l[:8]}: {n}' for l, n in per_lib.items()) or '-'})"
                 + (f", {len(pool) - len(batch)} follow in the next runs." if len(pool) > len(batch) else "."))
    process_queue(batch, history, failed, recheck)

def build_queue(lib_id, history, recheck):
    """Due books of one library (new, manually changed, due for refresh, re-checks). Each item gets '_lib'."""
    logging.info(f"--- Library: {lib_id} ---")
    try:
        r = abs_session.get(f"{ABS_URL}/api/libraries/{lib_id}/items")
        items = r.json()['results']
    except Exception as e: logging.error(f"Lib Error: {e}"); return []

    # --- NEW QUEUE LOGIC (UTC ONLY) ---
    work_queue = []
    count_new, count_due, count_manual, count_recheck, count_locked = 0, 0, 0, 0, 0
    now = datetime.now()
    now_utc = datetime.now(timezone.utc)

    for it in items:  # remember who owns which ASIN (duplicate-ASIN guard)
        a = ((it.get('media') or {}).get('metadata') or {}).get('asin')
        if a: ASIN_OWNERS.setdefault(a, set()).add(f"{lib_id}_{it['id']}")
        i_ = normalize_isbn(((it.get('media') or {}).get('metadata') or {}).get('isbn'))
        if i_: ISBN_OWNERS.setdefault(i_, set()).add(f"{lib_id}_{it['id']}")

    for item in items:
        key = f"{lib_id}_{item['id']}"

        # lock_all items never enter the queue (no batch slot, no sleep)
        if 'lock_all' in item_tags(item):
            count_locked += 1
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
                
                # Buffer: Ignore updates made by script itself (last run + 60s)
                if abs_dt > (last_run_dt + timedelta(seconds=60)):
                    is_manual_update = True
            except: 
                pass

        recheck_due = False
        if key in recheck:
            try: recheck_due = now_utc >= datetime.fromisoformat(recheck[key])
            except ValueError: recheck_due = True

        if is_manual_update:
            item['_manual_trigger'] = True
            work_queue.append(item); count_manual += 1
        elif (now - last_run_dt.replace(tzinfo=None)).days >= REFRESH_DAYS:
            work_queue.append(item); count_due += 1
        elif recheck_due:
            item['_recheck'] = True
            work_queue.append(item); count_recheck += 1

    count_fresh = len(items) - count_locked - len(work_queue)
    logging.info(f"Due: {count_new} New, {count_manual} Manual Updates, {count_due} Due, {count_recheck} Re-checks ({count_locked} locked via lock_all). "
                 f"{count_fresh} books are up to date (refreshed within the last {REFRESH_DAYS} days).")
    stats['skipped'] += count_locked
    for it in work_queue: it['_lib'] = lib_id
    return work_queue

def process_queue(work_queue, history, failed, recheck):
    total = len(work_queue)
    if HARDCOVER_ENABLED: hc_prefetch([f"{it['_lib']}_{it['id']}" for it in work_queue])
    start = datetime.now()
    consecutive_rl = 0

    for idx, item in enumerate(work_queue):
        if stats['aborted_ratelimit']: break
        lib_id = item['_lib']
        
        elapsed = (datetime.now() - start).total_seconds()
        items_done = idx + 1
        avg_time = elapsed / items_done
        remaining_items = total - items_done
        eta_seconds = avg_time * remaining_items
        eta_str = format_time(eta_seconds)
        
        search_penalty = False
        needs_sleep = False  # Only pause after items that actually hit Audible/Goodreads
        ITEM_LOG.lines = []

        while True: 
            try:
                iid, key = item['id'], f"{lib_id}_{item['id']}"
                item_data = abs_session.get(f"{ABS_URL}/api/items/{iid}", params={"expanded": 1}).json()  # expanded: includes media.duration (also in single-book mode)
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
                messy = parse_messy_title(title)
                search_title = messy['title'] if messy else title
                if messy: logging.info(f"      -> 🧹 Messy title detected ({messy['kind']}), searching as '{search_title}'")
                asin_from_search = False
                skip_meta = False
                abs_dur = (item_data.get('media') or {}).get('duration') or (item.get('media') or {}).get('duration')

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
                aud_data = get_audible_data(asin, lang)

                if aud_data and aud_data.get('meta_raw'):
                    raw_lang = aud_data['meta_raw'].get('language')
                    if raw_lang: new_lang_detected = map_language(raw_lang)
                    else: new_lang_detected = None
                else: new_lang_detected = None
                
                should_search = False
                check_lang = new_lang_detected if new_lang_detected else lang

                # Language check: ABS and Audible disagree -> who is right?
                lang_conflict, lang_target, strict_search, search_only = False, None, False, None
                abs_lang = map_language(lang) if lang else None
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
                        add_review(key, title, "title", aud_data.get("title_raw"), "ASIN confirmed by runtime and narrator; ABS title/author differ", plaus_why)
                if not asin:
                    logging.info("      -> ⚠️ No ASIN in ABS.")
                    should_search = True
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
                    search_narr = [] if plaus == "wrong" else (meta.get("narrators") or [])
                    SEARCH_CTX['dup_claim'] = bool(asin and (ASIN_OWNERS.get(asin, set()) - {key}))  # another ABS book has the same ASIN
                    SEARCH_CTX['pub'], SEARCH_CTX['year'] = meta.get('publisher'), meta.get('publishedYear')
                    if asin and aud_data and aud_data.get('title_raw'):
                        SEARCH_CTX.update({'old_title': aud_data['title_raw'], 'old_authors': ', '.join(aud_data.get('authors_raw') or [])})
                    if not found: found = find_missing_asin(search_title, authors, abs_dur, lang, raw_title=title,
                                                            required_lang=lang_target, strict=strict_search and bool(asin),
                                                            only_domain=search_only, narrators=search_narr, abs_series=meta.get("series") or [])
                    weak_ = LAST_SEARCH.get('weak')
                    if not found and USE_AUDIBLE_API and meta.get('series') and abs_dur and not search_only:
                        LAST_MATCH.clear()
                        found = find_missing_asin_via_series(search_title, authors, abs_dur, lang, raw_title=title, required_lang=lang_target,
                                                             strict=strict_search and bool(asin), narrators=search_narr,
                                                             abs_series=meta.get('series') or [])
                    
                    if not found and weak_ and 'lock_asin' not in tags:
                        add_review(key, title, 'asin', weak_['asin'], f"runtime (to the minute) and narrator match '{weak_['title']}'; title similarity {weak_.get('title_score')}, author {'matches' if weak_.get('author_ok') else 'differs'} - not enough for an automatic change")
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
                        else: logging.info(f"        ℹ️ Search returned same ASIN. Keeping fallback data.")
                    elif search_only: logging.info(f"        ℹ️ No exact match on {search_only}. Keeping {asin} from {aud_data.get('domain') if aud_data else '?'}.")
                    else: logging.info(f"        ℹ️ No replacement found. Keeping fallback data.")
                    if plaus == "wrong" and not (found and found != asin):  # nothing taken from a product that is another book
                        add_review(key, title, "asin", asin, "ASIN belongs to another book and no safe replacement was found", plaus_why)
                        aud_data, skip_meta = None, True
                    rel = LAST_SEARCH.get('related')
                    if not found and rel and not search_only and (not aud_data or int(aud_data.get('count', 0)) == 0):
                        aud_data = dict(rel, related_edition=True)
                        skip_meta = True
                        logging.info(f"        📎 Using ratings of another edition: '{rel.get('title_raw')}' [{rel.get('asin')}] on {rel.get('domain')} "
                                     f"(runtime {format_time(rel.get('runtime_sec') or 0)} vs ABS {format_time(abs_dur or 0)}). ASIN and metadata are not changed.")
                        stats['other_edition'] += 1

                if lang_conflict and aud_data and map_language((aud_data.get('meta_raw') or {}).get('language')) not in (None, lang_target):
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
                    if aud_title and aud_title != title and messy:  # only broken titles are replaced
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

                    new_pub = (md_raw.get('publisher') or {}).get('name')
                    # Publisher and year are only filled when empty (Audible has the audiobook edition's data)
                    if 'lock_publisher' not in tags:
                        if new_pub and not meta.get('publisher'):
                            abs_updates['publisher'] = new_pub
                            log_updates.append(f"Publisher: '{meta.get('publisher')}' -> '{new_pub}'")
                    elif new_pub and not meta.get('publisher'): logging.info(f"        🔒 Publisher Update Skipped (Locked): '{new_pub}'")

                    if 'lock_year' not in tags:
                        new_year = parse_release_year(md_raw.get('releaseDate'))
                        if new_year and not meta.get('publishedYear'):
                            abs_updates['publishedYear'] = new_year
                            log_updates.append(f"Year: '{meta.get('publishedYear')}' -> '{new_year}'")
                    else:
                        new_year = parse_release_year(md_raw.get('releaseDate'))
                        if new_year and not meta.get('publishedYear'): logging.info(f"        🔒 Year Update Skipped (Locked): '{new_year}'")
                    
                    if not new_lang_detected and meta.get('language'):
                        norm = map_language(meta['language'])  # e.g. 'eng' -> 'English'
                        if norm != meta['language']: new_lang_detected = norm
                    if 'lock_language' not in tags:
                        if new_lang_detected and new_lang_detected != meta.get('language'):
                            abs_updates['language'] = new_lang_detected
                            log_updates.append(f"Language: '{meta.get('language')}' -> '{new_lang_detected}'")
                    else:
                        if new_lang_detected and new_lang_detected != meta.get('language'): logging.info(f"        🔒 Language Update Skipped (Locked): '{new_lang_detected}'")
                    
                    fmt = str(md_raw.get('format') or '').lower()
                    if fmt:  # Only decide when Audible actually states a format
                        new_abridged = 'abridged' in fmt and 'unabridged' not in fmt
                        if new_abridged != bool(meta.get('abridged')):
                            if 'lock_abridged' not in tags:
                                abs_updates['abridged'] = new_abridged
                                log_updates.append(f"Abridged: {meta.get('abridged')} -> {new_abridged}")
                            else: logging.info(f"        🔒 Abridged Update Skipped (Locked): {new_abridged}")

                    if 'lock_genres' not in tags:
                        current_genres = meta.get('genres') or []
                        new_genres_list = list(dict.fromkeys(map_genre(c.get('name'), aud_data.get('domain')) for c in (md_raw.get('categories') or []) if c.get('name')))
                        added_genres = [g for g in new_genres_list if g not in current_genres]
                        if added_genres:
                            abs_updates['genres'] = current_genres + added_genres
                            log_updates.append(f"Genres: +{added_genres}")
                    else:
                        current_genres = meta.get('genres') or []
                        new_genres_list = list(dict.fromkeys(map_genre(c.get('name'), aud_data.get('domain')) for c in (md_raw.get('categories') or []) if c.get('name')))
                        added_genres = [g for g in new_genres_list if g not in current_genres]
                        if added_genres: logging.info(f"        🔒 Genre Update Skipped (Locked): +{added_genres}")

                    if 'lock_series' not in tags:
                        if series_list := md_raw.get('series'):
                            # ABS series are curated: Audible's series are only taken when the book has none in ABS.
                            # Otherwise only missing numbers of the same series are filled (Audible often lists reading
                            # orders, universes and translations as extra series).
                            cur_list = [{"name": x.get('name'), "sequence": x.get('sequence')} for x in (meta.get('series') or []) if x.get('name')]
                            new_list = [dict(x) for x in cur_list]
                            from abs_series import series_key as _skey, series_variant as _svar
                            same = lambda a, b: a == b or (_skey(a) == _skey(b) and _svar(a) == _svar(b))
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
                                if s_seq is not None and is_boxset_title(title):  # 'Trilogie 1-3' is not book 5
                                    logging.info(f"        ℹ️ Series '{s_name}': no number for a box set / collection ('{title}')")
                                    s_seq = None
                                elif s_seq is None and not is_boxset_title(title):
                                    aud_t, aud_s = aud_data.get('title_raw', ''), aud_data.get('subtitle_raw', '')
                                    text = f"{aud_t} {aud_s}".strip()
                                    pattern = re.escape(s_name) + r'[\s:,-]+(?:Book\s+|Band\s+|Teil\s+|Vol\.?\s*)?(\d+(?:\.\d+)?)'
                                    if text and (m := re.search(pattern, text, re.IGNORECASE)): s_seq = m.group(1)
                                canon, why = canonical_series(lib_id, s_name, authors)
                                existing = next((x for x in new_list if same(x['name'], canon)), None)
                                if existing:
                                    if why: logging.info(f"        🔗 Series '{s_name}' -> '{canon}' ({'alias list' if why == 'alias' else 'existing ABS series'})")
                                    if not existing.get('sequence') and s_seq: existing['sequence'] = s_seq
                                    elif s_seq and existing.get('sequence') and str(existing['sequence']) != str(s_seq):
                                        logging.info(f"        ℹ️ Series '{canon}': ABS #{existing['sequence']} vs Audible #{s_seq} (ABS kept)")
                                elif not cur_list:
                                    if why: logging.info(f"        🔗 Series '{s_name}' -> '{canon}' ({'alias list' if why == 'alias' else 'existing ABS series'})")
                                    new_list.append({"name": canon, "sequence": s_seq})
                                else:
                                    ignored.append(f"{canon}" + (f" #{s_seq}" if s_seq else ""))
                            if ignored:
                                logging.info(f"        ℹ️ Audible series not added (ABS already has series, or other edition): {', '.join(ignored)}")
                            if new_list != cur_list:
                                abs_updates['series'] = new_list
                                log_updates.append("Series: " + ", ".join(f"'{x['name']}' #{x['sequence']}" for x in new_list))
                    else: logging.info("        🔒 Series Update Skipped (Locked)")

                    # Narrators / authors: only filled when empty
                    aud_narr = aud_data.get('narrators_raw') or []
                    if aud_narr and not meta.get('narrators'):
                        if 'lock_narrators' in tags: logging.info(f"        🔒 Narrators Update Skipped (Locked): {aud_narr}")
                        else:
                            abs_updates['narrators'] = aud_narr
                            log_updates.append(f"Narrators: -> {aud_narr}")
                    if not aud_data.get('related_edition'): note_author_links(authors, aud_data)
                    aud_auth = aud_data.get('authors_raw') or []
                    if aud_auth and not authors:
                        if 'lock_authors' in tags: logging.info(f"        🔒 Authors Update Skipped (Locked): {aud_auth}")
                        else:
                            abs_updates['authors'] = [{'name': n} for n in aud_auth]
                            log_updates.append(f"Authors: -> {aud_auth}")
                    elif aud_auth and authors:
                        abs_norm = {re.sub(r'[^a-z]', '', a.lower()) for a in authors if a}
                        for an in aud_auth:
                            if re.sub(r'[^a-z]', '', an.lower()) not in abs_norm and match_author(authors, an):
                                AUTHOR_NOTES.append({'title': title, 'abs': authors, 'audible': an})
                                logging.info(f"        ℹ️ Author spelling differs: ABS {authors} vs Audible '{an}' (not changed, listed in report)")
                    # Explicit: only ever set to True when Audible marks the product as adult
                    if aud_data.get('adult') and not meta.get('explicit'):
                        if 'lock_explicit' in tags: logging.info("        🔒 Explicit Update Skipped (Locked)")
                        else:
                            abs_updates['explicit'] = True
                            log_updates.append("Explicit: False -> True")

                    if abs_updates:
                        logging.info(f"        🛠️ Meta Updates{' (DRY_RUN, not saved)' if DRY_RUN else ''}:")
                        for upd in log_updates: logging.info(f"          -> {upd}")
                        if abs_update(iid, key, title, abs_updates, meta, 'audible_meta'): stats['meta_updated'] += 1
                    else: logging.info("        ✅ No metadata updates necessary.")

                    # Cover: only when ABS has none
                    if FILL_COVERS and aud_data.get('cover_url') and not (item_data.get('media') or {}).get('coverPath'):
                        if 'lock_cover' in tags: logging.info("        🔒 Cover Update Skipped (Locked)")
                        elif abs_set_cover(iid, key, title, aud_data['cover_url'], 'audible_cover'):
                            stats['cover_added'] += 1
                            logging.info(f"        🖼️ Cover {'would be set (DRY_RUN)' if DRY_RUN else 'set'} from Audible: {aud_data['cover_url']}")

                # Hardcover: ratings only. Audible's clean title first, the ABS title as second comparison.
                hc_targets = list(dict.fromkeys(t for t in [(aud_data or {}).get('title_raw') if not skip_meta else None, search_title, title] if t))
                hc_data, hc_status = get_hardcover_data(key, hc_targets, authors, asin, meta.get('isbn'), ov.get('hardcover'),
                                                        use_cache=not item.get('_manual_trigger'),
                                                        abs_series=[(x.get('name'), x.get('sequence')) for x in (meta.get('series') or [])])
                if hc_data and hc_data.get('id') and not DRY_RUN: HC_CACHE[key] = {'id': hc_data['id'], 'slug': hc_data.get('slug')}
                if HARDCOVER_ENABLED: stats['hc_found' if hc_data else 'hc_missing'] += 1
                if hc_data and (not aud_data or aud_data.get('related_edition')) and 'lock_asin' not in tags:
                    via = hc_audio_asin(hc_data, abs_dur, authors, hc_targets, meta.get('narrators') or [], lang)
                    if via and via[0] != asin:
                        logging.info(f"        ✨ NEW ASIN via Hardcover edition: {via[0]} (evidence: {', '.join(via[1])})")
                        ASIN_OWNERS.setdefault(via[0], set()).add(key)
                        if abs_update(iid, key, title, {'asin': via[0]}, {'asin': asin}, 'hardcover_asin'):
                            logging.info(f"        💾 ASIN {'would be updated (DRY_RUN)' if DRY_RUN else 'updated in ABS'}.")
                        asin, aud_data = via[0], via[2]
                        stats['asin_found'] += 1; stats['asin_migrated'] += 1

                bad_isbn = isbn_bad_reason(meta.get('isbn'))
                if bad_isbn:
                    verified = bool(hc_data and hc_data.get('id') and hc_data.get('source') in ('ASIN', 'search', 'cache', 'manual link') and asin and aud_data and not skip_meta)
                    fix = None
                    if 'lock_isbn' in tags: logging.info(f"        🔒 ISBN '{meta.get('isbn')}' looks wrong ({bad_isbn}) but is locked")
                    elif verified:
                        try: fix = hc_replacement_isbn(hc_data, asin, abs_dur)
                        except SourceBlockedException: fix = None
                        if fix and (ISBN_OWNERS.get(fix[0], set()) - {key}):
                            logging.info(f"        ⛔ ISBN {fix[0]} already belongs to another book: not assigned")
                            fix = None
                        elif fix: ISBN_OWNERS.setdefault(fix[0], set()).add(key)
                        if fix and abs_update(iid, key, title, {'isbn': fix[0]}, meta, 'isbn_fix'):
                            logging.info(f"        🔧 ISBN '{meta.get('isbn')}' is wrong ({bad_isbn}): replaced by {fix[0]} ({fix[1]}, Hardcover){' (DRY_RUN, not saved)' if DRY_RUN else ''}")
                            stats['isbn_repaired'] += 1
                    if not fix and 'lock_isbn' not in tags:
                        logging.info(f"        ℹ️ ISBN '{meta.get('isbn')}' looks wrong ({bad_isbn}); no verified replacement, left unchanged (review list)")
                        add_review(key, title, 'isbn', meta.get('isbn'), f"ISBN looks wrong ({bad_isbn}); no verified replacement")
                gr_title = (aud_data or {}).get('title_raw') or search_title  # Audible's clean title searches best
                cached_url = ov.get('goodreads') or (None if item.get('_manual_trigger') else GR_CACHE.get(key))
                hc_gids = (hc_data or {}).get('gr_ids') or (hc_fetch_gr_ids(hc_data['id']) if hc_data and hc_data.get('id') and GOODREADS_ENABLED and not GR_STATE['blocked'] else [])
                gr_data, gr_status = get_goodreads_data(meta.get('isbn'), gr_title, authors, authors[0] if authors else "", cached_url, hc_gids,
                                                        force=bool(item.get('_manual_trigger') or ONLY_ITEMS))
                if gr_data and gr_data.get('url') and not DRY_RUN: GR_CACHE[key] = gr_data['url']
                
                if gr_data and not meta.get('isbn'):
                    # Only fill an empty ISBN, and only with a real ISBN (never an ASIN). Goodreads has the print
                    # edition's ISBN, so an existing ISBN is never overwritten.
                    new_id = normalize_isbn(gr_data.get('isbn'))
                    if new_id and 'lock_isbn' in tags: logging.info("        🔒 ISBN Update Skipped (Locked)")
                    elif new_id and abs_update(iid, key, title, {'isbn': new_id}, meta, 'goodreads_isbn'):
                        logging.info(f"        🔧 ISBN added{' (DRY_RUN, not saved)' if DRY_RUN else ''}: {new_id}")
                        stats['isbn_added'] += 1

                if 'lock_description' not in tags:
                    cur_desc = meta.get('description') or ''  # ABS sends null for empty descriptions
                    old_blocks = parse_rating_block(cur_desc)
                    # Empty description (apart from our rating block): use Audible's summary
                    summary = (aud_data or {}).get('summary') if not skip_meta else None
                    base_desc = cur_desc
                    if summary and not strip_rating_block(cur_desc):
                        base_desc = summary
                        stats['desc_filled'] += 1
                        logging.info("        📝 Empty description filled with Audible summary")
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
                if has_aud and has_gr:
                    record_attempt(key, history, failed, recheck, complete=True)
                elif has_aud:
                    # Audible worked. Missing Goodreads is no strike, just re-check earlier.
                    stats['gr_missing'] += 1
                    days = GR_BLOCKED_RECHECK_DAYS if gr_status in ('blocked', 'deferred', 'disabled') else GR_MISSING_RECHECK_DAYS
                    logging.info(f"      -> ℹ️ Audible OK, Goodreads {gr_status.replace('_', ' ')}.")
                    record_attempt(key, history, failed, recheck, recheck_days=days)
                elif no_ratings_yet:
                    stats['no_ratings_yet'] += 1
                    days = GR_BLOCKED_RECHECK_DAYS if gr_status in ('blocked', 'deferred', 'disabled') else NO_RATINGS_RECHECK_DAYS
                    record_attempt(key, history, failed, recheck, recheck_days=days)
                else:
                    logging.warning(f"      -> ⚠️ No Audible data (Audible: {has_aud}, GR: {has_gr})")
                    record_attempt(key, history, failed, recheck)

                # Result for the web UI dashboard / problem list
                if aud_data and aud_data.get('related_edition'):
                    status, reason = 'other_edition', f"Ratings of another edition ({aud_data.get('asin')}), ASIN not available anymore"
                elif has_aud: status, reason = 'ok', ''
                elif no_ratings_yet: status, reason = 'no_ratings_yet', 'On Audible, but no ratings yet'
                elif not asin: status, reason = 'not_found', 'No ASIN and no match found on Audible'
                elif aud_data is None: status, reason = 'not_found', 'ASIN not available in any Audible store, no replacement found'
                else: status, reason = 'not_found', 'No ratings found on Audible'
                notes = []
                if skip_meta: notes.append('metadata skipped (Audible data does not fit this book)')
                if GOODREADS_ENABLED and not has_gr and gr_status != 'deferred': notes.append(f"Goodreads {gr_status.replace('_', ' ')}")
                if HARDCOVER_ENABLED and not hc_data: notes.append(f"Hardcover {hc_status.replace('_', ' ')}")
                if ov.get('ignore'): status = 'ignored'
                cert, cert_parts = certainty_score(search_title, authors, meta.get('narrators') or [], abs_dur, meta.get('publisher'), meta.get('publishedYear'),
                                                   meta.get('series') or [], aud_data, hc_data, asin_from_search,
                                                   any(x.get('key') == key and x.get('status', 'open') == 'open' for x in (rw_json(REVIEW_FILE) or []) if isinstance(x, dict)))
                set_result(key, lib_id, iid, title, authors, asin, status, reason,
                           {'notes': notes, 'certainty': cert, 'certainty_parts': cert_parts, 'domain': (aud_data or {}).get('domain'), 'goodreads': (gr_data or {}).get('url'),
                            'hardcover': (hc_data or {}).get('url')})

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
        
        if stats['aborted_ratelimit']: break
        if not needs_sleep: continue
        # Requests are throttled per source (MIN_INTERVAL); this pause is only an optional extra.
        sleep_dur = BASE_SLEEP + (random.uniform(0, min(1.0, BASE_SLEEP / 2)) if BASE_SLEEP else 0)
        if search_penalty: sleep_dur += SEARCH_PENALTY_SLEEP
        if sleep_dur > 0: time.sleep(sleep_dur)

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
               + (f" (daily limit left: {hc['daily_left']})" if hc['daily_left'] is not None else '')
               + (f" | stopped: {hc['disabled']}" if hc['disabled'] else '')) if HARDCOVER_ENABLED else "Hardcover: off"
    lines = [
        f"Books processed: {s['processed']} (skipped/locked: {s['skipped']}, manual updates: {s['manual_retrigger']}, re-checks: {s['recheck']})",
        f"Ratings {'to write' if DRY_RUN else 'written'}: {s['success']} | Audible without ratings yet: {s['no_ratings_yet']} | Goodreads missing: {s['gr_missing']} (deferred: {s['gr_deferred']})",
        f"ASINs found: {s['asin_found']} | Ratings from another edition: {s['other_edition']} | Metadata updated: {s['meta_updated']} | Covers added: {s['cover_added']} | Descriptions filled: {s['desc_filled']} | ISBNs added: {s['isbn_added']}",
        f"Series merges: {series_result.get('merges', 0)} ({series_result.get('books_changed', 0)} books {'would change' if DRY_RUN else 'changed'}) | "
        f"Author spellings unified: {series_result.get('author_fixes', 0)} ({series_result.get('authors_changed', 0)} books)",
        hc_line,
        f"Errors: {s['failed']}{' | Goodreads blocked' if GR_STATE['blocked'] else ''}{' | ABORTED (rate limit)' if s['aborted_ratelimit'] else ''}",
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
    """Ask ABS to create a backup before a real run (ABS: Settings -> Backups). Failure is logged, the run continues."""
    try:
        r = abs_session.post(f"{ABS_URL}/api/backups", timeout=600)
        if r.status_code == 200: logging.info("💾 ABS backup created before this run.")
        else: logging.warning(f"💾 ABS backup failed (HTTP {r.status_code}): {r.text[:150]}")
    except requests.RequestException as e:
        logging.warning(f"💾 ABS backup failed: {e}")

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
                  'HARDCOVER_ENABLED', 'HARDCOVER_INTERVAL', 'RATING_ORDER']  # never API_TOKEN

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
    
    logging.info("--- Start ---")
    history, failed, recheck = rw_json(HISTORY_FILE), rw_json(FAILED_FILE), rw_json(RECHECK_FILE)
    GR_CACHE.update(rw_json(GR_CACHE_FILE))
    HC_CACHE.update(rw_json(HC_CACHE_FILE))
    SERIES_ALIASES.update(rw_json(SERIES_ALIASES_FILE))
    SERIES_INDEX.update(rw_json(SERIES_INDEX_FILE))
    if SERIES_ALIASES or SERIES_INDEX:
        logging.info(f"Series aliases: {sum(len(v) for v in SERIES_ALIASES.values())} | Known series: {sum(len(v) for v in SERIES_INDEX.values())}")
    RESULTS.update(rw_json(RESULTS_FILE)); OVERRIDES.update(rw_json(OVERRIDES_FILE))
    logging.getLogger().addHandler(ITEM_LOG)
    if ABS_BACKUP and not DRY_RUN: create_abs_backup()
    if not SERIES_ONLY:
        run_libraries(history, failed, recheck)
        if not DRY_RUN: rw_json(HISTORY_FILE, history); rw_json(FAILED_FILE, failed); rw_json(RECHECK_FILE, recheck); rw_json(HC_CACHE_FILE, HC_CACHE)
        save_reports()
        if AUTHOR_NOTES: rw_json(os.path.join(REPORT_DIR, "author_variants.json"), AUTHOR_NOTES)
        rw_json(RESULTS_FILE, RESULTS)  # also in DRY_RUN: it is the web UI's view of the last check

    series_result = {}
    if (SERIES_MERGE or LIBRARY_REPORT) and not ONLY_ITEMS:
        try:
            import abs_series
            series_result = abs_series.run({
                'session': abs_session, 'abs_url': ABS_URL, 'library_ids': LIBRARY_IDS, 'dry_run': DRY_RUN,
                'abs_update': abs_update, 'report_dir': REPORT_DIR, 'merge': SERIES_MERGE, 'report': LIBRARY_REPORT,
                'lock_after_merge': SERIES_LOCK_AFTER_MERGE, 'author_normalize': AUTHOR_NORMALIZE,
                'aliases': SERIES_ALIASES, 'aliases_file': SERIES_ALIASES_FILE, 'index_file': SERIES_INDEX_FILE,
                'decisions_file': os.path.join(SCRIPT_DIR, 'merge_decisions.json'), 'merges_file': os.path.join(SCRIPT_DIR, 'merges.json'),
                'review_file': os.path.join(SCRIPT_DIR, 'merge_review.json'),
                'author_links': {n: sorted(a) for n, a in AUTHOR_LINKS.items()},
                'audible_series_names': set(SERIES_ASINS),
                'extra': {'author_notes': AUTHOR_NOTES, 'unmapped_genres': UNMAPPED_GENRES},
            })
            stats['series_merged'] = series_result.get('merges', 0)
        except ImportError:
            logging.error("abs_series.py not found next to abs_ratings.py - series task skipped.")
        except Exception as e:
            logging.exception(f"Series task failed: {e}")
    write_env_file(log_file, start_time)
    log_run_summary(start_time, series_result)
    logging.info(f"Debug bundle (send this one for troubleshooting): {os.path.join(LOG_DIR, 'debug_latest.log')}")
    write_debug_bundle(log_file)

if __name__ == "__main__": main()
