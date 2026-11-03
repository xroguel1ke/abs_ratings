import os
import time
import requests
from bs4 import BeautifulSoup
import re, json, random, difflib, logging, urllib.parse
from datetime import datetime, timezone, timedelta

# ================= CONFIGURATION =================
ABS_URL = os.getenv('ABS_URL', '').rstrip('/')
API_TOKEN = os.getenv('API_TOKEN')
LIBRARY_IDS = [l.strip() for l in os.getenv('LIBRARY_IDS', '').split(',') if l.strip()]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")
REPORT_DIR = os.path.join(SCRIPT_DIR, "reports")
HISTORY_FILE = os.path.join(SCRIPT_DIR, "rating_history.json")
FAILED_FILE = os.path.join(SCRIPT_DIR, "failed_history.json")
RECHECK_FILE = os.path.join(SCRIPT_DIR, "recheck_history.json")  # key -> UTC time of an early re-check
GR_CACHE_FILE = os.path.join(SCRIPT_DIR, "goodreads_cache.json")  # key -> Goodreads book URL
ENV_OUTPUT_FILE = os.path.join(SCRIPT_DIR, "last_run.env")

REFRESH_DAYS = int(os.getenv('REFRESH_DAYS', 90))
MAX_BATCH_SIZE = int(os.getenv('BATCH_SIZE', 150))
MAX_FAIL_ATTEMPTS = 5
MAX_CONSECUTIVE_RL = 3
RECOVERY_PAUSE = 60
BASE_SLEEP = float(os.getenv('SLEEP_TIMER', 3))  # Pause between items (sources are additionally throttled below)
SEARCH_PENALTY_SLEEP = float(os.getenv('SEARCH_PENALTY_SLEEP', 0))  # Optional extra pause after ASIN searches
# Minimum seconds between two requests to the same source
MIN_INTERVAL = {
    'audible_api': float(os.getenv('AUDIBLE_API_INTERVAL', 1)),
    'audible_web': float(os.getenv('AUDIBLE_WEB_INTERVAL', 3)),
    'goodreads': float(os.getenv('GOODREADS_INTERVAL', 2)),
}
USE_AUDIBLE_API = os.getenv('USE_AUDIBLE_API', 'true').lower() == 'true'
AUDIBLE_API_HOSTS = {"www.audible.com": "api.audible.com", "www.audible.de": "api.audible.de"}
AUDIBLE_API_GROUPS = "rating,series,product_attrs,product_desc,category_ladders,contributors"
DRY_RUN = os.getenv('DRY_RUN', 'False').lower() == 'true'
ABS_TIMEOUT = int(os.getenv('ABS_TIMEOUT', 30))  # Seconds per ABS API request
NO_RATINGS_RECHECK_DAYS = int(os.getenv('NO_RATINGS_RECHECK_DAYS', 7))  # New releases without ratings yet
GR_MISSING_RECHECK_DAYS = int(os.getenv('GR_MISSING_RECHECK_DAYS', 30))  # Audible OK, Goodreads not found
GR_BLOCKED_RECHECK_DAYS = 1  # Goodreads blocked/unreachable during the run

# --- HEADERS & CONSTANTS ---
FIXED_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

GERMAN_LANG_CODES = ['de', 'deu', 'ger', 'german', 'deutsch']

LANGUAGE_MAP = {
    'englisch': 'English',
    'english': 'English',
    'deutsch': 'German',
    'german': 'German'
}

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
RE_AUDIBLE_BLOCK = re.compile(r'(?s)(Audible.*?)<br>\s*(?=Goodreads|⭐)')
RE_GR_BLOCK = re.compile(r'(?s)(Goodreads.*?)<br>\s*(?=⭐)')
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

stats = {k: 0 for k in ["processed", "success", "failed", "no_data", "skipped", "partial", "cooldown", "recycled", "asin_found", "isbn_added", "isbn_repaired", "asin_migrated", "meta_updated", "manual_retrigger", "no_ratings_yet", "gr_missing", "recheck"]}
stats['aborted_ratelimit'] = False
GR_STATE = {'blocked': False}
GR_CACHE = {}  # Filled in main()  # Set once Goodreads blocks us; GR is then skipped for the rest of the run
reports = {"audible": {}, "goodreads": {}}

class RateLimitException(Exception):
    def __init__(self, msg, is_hard=False): super().__init__(msg); self.is_hard = is_hard

class SourceBlockedException(Exception):
    """A non-critical source (Goodreads) blocks us. Skip the source instead of aborting the run."""

# ================= UTILS =================

_last_request = {}
def throttle(source):
    """Wait until MIN_INTERVAL[source] seconds have passed since the last request to that source."""
    wait = MIN_INTERVAL.get(source, 0) - (time.monotonic() - _last_request.get(source, 0))
    if wait > 0: time.sleep(wait + random.uniform(0, 0.5))
    _last_request[source] = time.monotonic()

def map_language(raw):
    if not raw: return None
    raw = str(raw).strip()
    return LANGUAGE_MAP.get(raw.lower(), raw[:1].upper() + raw[1:])

def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    f = os.path.join(LOG_DIR, f"run_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(f, encoding='utf-8'), logging.StreamHandler()])
    return f

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

def write_env_file(log_file, start_time):
    dur = f"{int((datetime.now() - start_time).total_seconds() // 60)}m {int((datetime.now() - start_time).total_seconds() % 60)}s"
    if stats['aborted_ratelimit']: sub, icon, head = "ABS Ratings: Aborted 🛑", "alert", "Rate Limit detected!"
    elif stats['failed'] > 0: sub, icon, head = "ABS Ratings: Error ❌", "alert", "Error occurred!"
    elif any(stats[k] > 0 for k in ['success', 'recycled', 'asin_found', 'asin_migrated', 'isbn_added', 'meta_updated']): sub, icon, head = "ABS Ratings: Success ✅", "normal", "Update complete"
    else: sub, icon, head = "ABS Ratings: Info ℹ️", "normal", "No changes"
    
    body = f"Proc: {stats['processed']} | New: {stats['success']} | ManUpd: {stats['manual_retrigger']} | MetaUpd: {stats['meta_updated']} | ASIN+: {stats['asin_found']} | Mig: {stats['asin_migrated']} | ISBN+: {stats['isbn_added']} | Fix: {stats['isbn_repaired']} | NoRatYet: {stats['no_ratings_yet']} | GR-: {stats['gr_missing']} | Err: {stats['failed']}"
    if GR_STATE['blocked']: body += " | ⚠️ Goodreads blocked"
    if stats['aborted_ratelimit']: body += " | ⚠️ ABORTED (Rate Limit)"
    
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
    t = re.sub(r'[:\-\(\)\[\]]', ' ', t)
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

RE_DUP_TITLE = re.compile(r'^\s*(.+?)\s*:\s*\1\s*(?::\s*(.+))?$', re.IGNORECASE)
RE_SERIES_PREFIX = re.compile(r'^\s*(.{4,}?)\s+(?:#|Book\s+|Vol\.?\s*|Band\s+|Teil\s+)?(\d{1,3}(?:\.\d)?)\s*[-:–.]?\s+(.{3,})$', re.IGNORECASE)

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
        # Guard against normal titles with numbers ("Catch 22 ..."): the series part needs at least 2 words
        if len(series.split()) >= 2 and len(rest.split()) >= 2:
            return {'title': rest, 'series': series, 'sequence': str(float(seq)).rstrip('0').rstrip('.'), 'kind': 'series_prefix'}
    return None

def title_similarity(a, b):
    na, nb = normalize_title_text(a), normalize_title_text(b)
    if not na or not nb: return 0.0
    score = difflib.SequenceMatcher(None, na, nb).ratio()
    if (len(na) > 3 and na in nb) or (len(nb) > 3 and nb in na): score += 0.15
    return score

def extract_volume(text): return set(RE_VOL.findall(text)) | ({m.group(1)} if (m := re.search(r'\b(\d+)$', text.strip())) else set())

def format_time(seconds):
    if seconds < 60: 
        return f"{int(seconds)}s"
    if seconds >= 3600:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"
    return f"{int(seconds // 60)}m {int(seconds % 60)}s"

def match_author(abs_authors, web_author):
    if not abs_authors or not web_author: return False
    web_clean = [w.strip().lower() for w in web_author.split(',')]
    for abs_auth in abs_authors:
        abs_clean = abs_auth.lower()
        for wa in web_clean:
            if abs_clean in wa or wa in abs_clean: return True
            a_tok = set(re.split(r'[^a-z0-9]+', abs_clean))
            wa_tok = set(re.split(r'[^a-z0-9]+', wa))
            if len(a_tok.intersection(wa_tok)) >= 2: return True
            if len(a_tok.intersection(wa_tok)) == 1 and len(a_tok) == 1: return True
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
    if language and str(language).strip().lower() in GERMAN_LANG_CODES:
        return ["www.audible.de", "www.audible.com"]
    return ["www.audible.com", "www.audible.de"]

def get_audible_api(asin, domain):
    """Query the Audible catalog API for one marketplace.
    Returns a result dict in the same shape as the web scraper, 'missing' if the product
    is not available there, or None on errors (caller falls back to web scraping)."""
    host = AUDIBLE_API_HOSTS.get(domain)
    if not host: return None
    throttle('audible_api')
    url = f"https://{host}/1.0/catalog/products/{asin}"
    try:
        r = requests.get(url, params={"response_groups": AUDIBLE_API_GROUPS},
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
    for ladder in p.get('category_ladders') or []:
        for c in ladder.get('ladder') or []:
            if c.get('name') and c['name'] not in categories: categories.append(c['name'])
    res['meta_raw'] = {
        'language': p.get('language'),
        'publisher': {'name': p.get('publisher_name')} if p.get('publisher_name') else None,
        'releaseDate': p.get('release_date') or p.get('issue_date'),
        'format': p.get('format_type'),
        'categories': [{'name': c} for c in categories],
        'series': [{'name': s.get('title'), 'part': s.get('sequence')} for s in (p.get('series') or []) if s.get('title')],
    }
    return res

def get_audible_data(asin, language):
    """Audible ratings + metadata. Uses the catalog API first and only falls back to
    scraping the product pages when the API can't answer for any marketplace."""
    if not asin: return None
    if not USE_AUDIBLE_API: return get_audible_data_web(asin, language)
    api_error = False
    for domain in audible_domains(language):
        logging.info(f"      -> Checking {domain} (API)...")
        res = get_audible_api(asin, domain)
        if res is None: api_error = True; continue
        if res == 'missing':
            logging.info(f"        ⚠️ Not available on {domain}")
            continue
        if res['count'] > 0 and res.get('overall'):
            logging.info(f"        ✅ SUCCESS on {domain} (Count: {res['count']}, Rating: {round(safe_float(res['overall']), 2)})")
        else:
            logging.info(f"        ℹ️ Product exists on {domain}, but has no ratings yet.")
        return res
    logging.info(f"        ↪️ {'API error' if api_error else 'Not found via API'}, falling back to web scraping...")
    return get_audible_data_web(asin, language)

def get_audible_data_web(asin, language):
    if not asin: return None
    domains = audible_domains(language)

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

def search_audible_api(title, authors_list, duration, lang, raw_title=None):
    """Find an ASIN via the Audible catalog search API. Every candidate is verified by loading
    the product (title, author, runtime). Returns the ASIN, None (no match) or False (API error)."""
    prim_auth = authors_list[0] if authors_list else ""
    queries = [{"keywords": f"{title} {prim_auth}".strip()}]
    if prim_auth: queries.append({"title": title, "author": prim_auth})
    if raw_title and raw_title != title: queries.append({"keywords": raw_title})
    api_ok = False
    for domain in audible_domains(lang):
        host, candidates = AUDIBLE_API_HOSTS[domain], []
        for q in queries:
            throttle('audible_api')
            try:
                r = requests.get(f"https://{host}/1.0/catalog/products", params={**q, "num_results": 5, "response_groups": "product_attrs"},
                                 headers={"User-Agent": FIXED_USER_AGENT, "Accept": "application/json"}, timeout=15)
                if r.status_code == 429: raise RateLimitException(f"HTTP 429 on {host} (API search)", True)
                if r.status_code != 200: continue
                api_ok = True
                for prod in r.json().get('products') or []:
                    if prod.get('asin') and prod['asin'] not in candidates: candidates.append(prod['asin'])
            except (requests.RequestException, ValueError) as e:
                logging.warning(f"        🌐 Audible API search failed ({host}): {type(e).__name__}")
            if len(candidates) >= 3: break
        for cand in candidates[:5]:
            p = get_audible_api(cand, domain)
            if not isinstance(p, dict): continue
            found_title = p.get('title_raw') or ''
            t_score = max(title_similarity(title, found_title), title_similarity(title, f"{found_title} {p.get('subtitle_raw') or ''}"))
            f_nums, t_nums = extract_volume(found_title + ' ' + (p.get('subtitle_raw') or '')), extract_volume(title)
            if f_nums and t_nums and not f_nums & t_nums and t_score < 0.9: continue
            auth_ok = match_author(authors_list, ", ".join(p.get('authors_raw') or []))
            dur_ok = bool(duration and p.get('runtime_sec') and abs(duration - p['runtime_sec']) < 600)
            dur_conflict = bool(duration and p.get('runtime_sec') and not dur_ok)
            if t_score >= 0.8 and auth_ok and not dur_conflict or t_score >= 0.85 and dur_ok:
                logging.info(f"        ✅ API match on {domain}: '{found_title}' [{cand}] (title {t_score:.2f}, author {'✓' if auth_ok else '✗'}, runtime {'✓' if dur_ok else '?'})")
                return cand
            logging.info(f"        ↪️ Rejected '{found_title}' [{cand}] (title {t_score:.2f}, author {'✓' if auth_ok else '✗'}, runtime {'✗' if dur_conflict else '?'})")
    return None if api_ok else False

def find_missing_asin(title, authors_list, duration, lang, raw_title=None):
    logging.info(f"      -> 🔎 Searching Replacement ASIN for '{title}'...")
    if USE_AUDIBLE_API:
        res = search_audible_api(title, authors_list, duration, lang, raw_title)
        if res is not False: return res
        logging.info("        ↪️ API search unavailable, falling back to web search...")
    return find_missing_asin_web(title, authors_list, duration, lang)

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

def get_goodreads_data(isbn, title, authors, prim_auth, cached_url=None):
    """Returns (data, status) with status in 'found', 'not_found', 'blocked'.
    cached_url: Goodreads book page found in an earlier run (1 request instead of a search)."""
    if GR_STATE['blocked']:
        logging.info("      -> ⏭️ Goodreads skipped (blocked earlier in this run)")
        return None, 'blocked'
    logging.info("      -> Checking www.goodreads.com")
    try:
        if cached_url:
            if d := scrape_gr_details(cached_url, require_book_page=True):
                d['source'] = 'Cache'
                logging.info(f"        ✅ Found via cached URL (Count: {d.get('count')}, Rating: {round(safe_float(d.get('val')), 2)})")
                return d, 'found'
            logging.info("        ↪️ Cached Goodreads URL failed, searching again...")
        d = _search_goodreads(isbn, title, authors, prim_auth)
        return d, ('found' if d else 'not_found')
    except SourceBlockedException as e:
        GR_STATE['blocked'] = True
        logging.warning(f"        🛑 Goodreads blocks requests: {e}. Skipping Goodreads for the rest of this run.")
        return None, 'blocked'

def parse_gr_search_results(soup):
    """Return [(title, absolute_url, author)] from a Goodreads search page.
    Tries the classic table layout first, then a layout-independent fallback that pairs every
    /book/show/ link with the nearest /author/show/ link."""
    results, seen = [], set()
    def add(link, author):
        href = urllib.parse.urljoin("https://www.goodreads.com", link.get('href', '')).split('?')[0]
        t = link.get_text(" ", strip=True)
        if not t or href in seen: return
        seen.add(href); results.append((t, href, author))

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
            if d := scrape_gr_details(f"https://www.goodreads.com/search?q={urllib.parse.quote(str(q_id))}", require_book_page=True):
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
            best_url, best_score = None, 0.0
            results = parse_gr_search_results(soup)
            if not results:
                logging.info(f"        🔍 GR search '{q}': no results parsed (title: '{page_title(soup)}', {len(r.text)} bytes)")
                if len(r.text) > 50000: save_gr_sample(r.text, q)
            for found_title, found_url, found_auth in results:
                norm_found = normalize_title_text(found_title)
                t_score = difflib.SequenceMatcher(None, norm_target, norm_found).ratio()
                if (len(norm_target) > 3 and norm_target in norm_found) or (len(norm_found) > 3 and norm_found in norm_target): t_score += 0.15
                f_nums, t_nums = extract_volume(found_title), extract_volume(title)
                if (f_nums and t_nums and not f_nums & t_nums): 
                    if t_score < 0.9: continue
                if not match_author(authors, found_auth): continue
                if t_score > 0.75 and t_score > best_score:
                    best_score, best_url = t_score, found_url
            if best_url:
                if d := scrape_gr_details(best_url): 
                    d['source'] = 'Text Search (List Match)'
                    logging.info(f"        ✅ Found via Text Search (List) (Count: {d.get('count')}, Rating: {round(safe_float(d.get('val')), 2)})")
                    return d
    logging.info("        ❌ Not found via ID or Text.")
    return None

def build_description(current_desc, aud, gr, old_aud, old_gr):
    lines = ["⭐ Ratings & Infos"]
    if aud and int(aud.get('count',0)) > 0:
        lines.append(f"Audible ({aud.get('count')}):")
        if v := aud.get('overall'): lines.append(f"🏆 {moon_rating(v)} {round(safe_float(v), 1)} / 5 - Overall")
        if v := aud.get('performance'): lines.append(f"🎙️ {moon_rating(v)} {round(safe_float(v), 1)} / 5 - Performance")
        if v := aud.get('story'): lines.append(f"📖 {moon_rating(v)} {round(safe_float(v), 1)} / 5 - Story")
    elif old_aud:
        stats['recycled'] += 1; lines.append(old_aud)
    
    if gr:
        lines.append(f"Goodreads ({gr.get('count', 0)}):")
        if v := gr.get('val'): lines.append(f"🏆 {moon_rating(v)} {round(safe_float(v), 1)} / 5 - Rating")
    elif old_gr:
        stats['recycled'] += 1; lines.append(old_gr)
        
    lines.append("⭐")
    clean_d = RE_RATING_BLOCK.sub('', current_desc)
    clean_d = re.sub(r'(?s)\*\*Audible\*\*.*?---\s*\n*', '', clean_d)
    return "<br>".join(lines) + "<br>" + re.sub(r'^(?:\s|<br\s*/?>)+', '', clean_d, flags=re.I).strip()

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

def item_tags(item):
    """Collect tags from all places ABS may put them (root, media, media.metadata)."""
    media = item.get('media') or {}
    raw = (item.get('tags') or []) + (media.get('tags') or []) + ((media.get('metadata') or {}).get('tags') or [])
    return {t.strip() for t in raw if isinstance(t, str)}

def process_library(lib_id, history, failed, recheck):
    logging.info(f"--- Processing Library: {lib_id} ---")
    try:
        r = abs_session.get(f"{ABS_URL}/api/libraries/{lib_id}/items")
        items = r.json()['results']
    except Exception as e: logging.error(f"Lib Error: {e}"); return

    # --- NEW QUEUE LOGIC (UTC ONLY) ---
    work_queue = []
    count_new, count_due, count_manual, count_recheck, count_locked = 0, 0, 0, 0, 0
    now = datetime.now()
    now_utc = datetime.now(timezone.utc)

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

    random.shuffle(work_queue)
    total = min(len(work_queue), MAX_BATCH_SIZE)
    logging.info(f"Queue: {count_new} New, {count_manual} Manual Updates, {count_due} Due, {count_recheck} Re-checks ({count_locked} locked via lock_all). Total: {total}")
    stats['skipped'] += count_locked
    
    start = datetime.now()
    consecutive_rl = 0

    for idx, item in enumerate(work_queue[:MAX_BATCH_SIZE]):
        if stats['aborted_ratelimit']: break
        
        elapsed = (datetime.now() - start).total_seconds()
        items_done = idx + 1
        avg_time = elapsed / items_done
        remaining_items = total - items_done
        eta_seconds = avg_time * remaining_items
        eta_str = format_time(eta_seconds)
        
        search_penalty = False
        needs_sleep = False  # Only pause after items that actually hit Audible/Goodreads

        while True: 
            try:
                iid, key = item['id'], f"{lib_id}_{item['id']}"
                item_data = abs_session.get(f"{ABS_URL}/api/items/{iid}").json()
                tags = item_tags(item_data)
                meta = item_data['media']['metadata']
                title = meta.get('title')

                if 'lock_all' in tags:
                    logging.info(f"({idx+1}/{total}) 🔒 Skipping '{title}' (lock_all tag found)")
                    stats['skipped'] += 1
                    break

                asin, lang = meta.get('asin'), meta.get('language')
                authors = [a.get('name') if isinstance(a, dict) else a for a in meta.get('authors', [])]
                
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

                aud_data = get_audible_data(asin, lang)

                if aud_data and aud_data.get('meta_raw'):
                    raw_lang = aud_data['meta_raw'].get('language')
                    if raw_lang: new_lang_detected = map_language(raw_lang)
                    else: new_lang_detected = None
                else: new_lang_detected = None
                
                should_search = False
                check_lang = new_lang_detected if new_lang_detected else lang

                if not asin:
                    logging.info("      -> ⚠️ No ASIN in ABS.")
                    should_search = True
                elif aud_data is None:
                    logging.info("      -> ⚠️ ASIN not found (All domains).")
                    should_search = True
                elif int(aud_data.get('count', 0)) == 0 and aud_data.get('page_ok'):
                    logging.info(f"      -> 🆕 Product page exists on {aud_data.get('domain')}, but no ratings yet. No replacement search, re-check in {NO_RATINGS_RECHECK_DAYS} days.")
                elif int(aud_data.get('count', 0)) == 0:
                    logging.info("      -> ⚠️ Found 0 Ratings.")
                    should_search = True
                elif (str(check_lang).lower() not in GERMAN_LANG_CODES) and aud_data.get('domain') == 'www.audible.de':
                    logging.info("      -> ⚠️ Non-German Book only found on .de (Possible broken .com ASIN). Attempting Fix...")
                    should_search = True
                elif (str(check_lang).lower() in GERMAN_LANG_CODES) and aud_data.get('domain') == 'www.audible.com':
                    logging.info("      -> ⚠️ German Book only found on .com (Possible broken .de ASIN). Attempting Fix...")
                    should_search = True

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

                    if not found: found = find_missing_asin(search_title, authors, item['media'].get('duration'), lang, raw_title=title)
                    
                    if found:
                        if found != asin:
                            logging.info(f"        ✨ NEW ASIN Found: {found}")
                            if not DRY_RUN: 
                                abs_session.patch(f"{ABS_URL}/api/items/{iid}/media", json={"metadata": {"asin": found}})
                                logging.info(f"        💾 ASIN updated in ABS.")
                            asin, stats['asin_found'] = found, stats['asin_found'] + 1
                            stats['asin_migrated'] += 1
                            asin_from_search = True
                            if not target_is_german: lang = "English" 
                            aud_data = get_audible_data(asin, lang)
                            if aud_data and aud_data.get('meta_raw'):
                                raw_l = aud_data['meta_raw'].get('language')
                                if raw_l: new_lang_detected = map_language(raw_l)
                        else: logging.info(f"        ℹ️ Search returned same ASIN. Keeping fallback data.")
                    else: logging.info(f"        ℹ️ No replacement found. Keeping fallback data.")


                if aud_data and aud_data.get('meta_raw'):
                    md_raw = aud_data['meta_raw']
                    abs_updates = {}
                    log_updates = []

                    # Title fix: only for broken titles (messy pattern or ASIN found by search),
                    # and only if Audible's title confirms it is the same book.
                    aud_title, aud_sub = aud_data.get('title_raw'), aud_data.get('subtitle_raw')
                    if aud_title and aud_title != title and (messy or asin_from_search):
                        confirmed = asin_from_search or title_similarity(search_title, aud_title) >= 0.8
                        if 'lock_title' in tags: logging.info(f"        🔒 Title Update Skipped (Locked): '{aud_title}'")
                        elif confirmed:
                            abs_updates['title'] = aud_title
                            log_updates.append(f"Title: '{title}' -> '{aud_title}'")
                            if aud_sub and not meta.get('subtitle'):
                                abs_updates['subtitle'] = aud_sub
                                log_updates.append(f"Subtitle: -> '{aud_sub}'")

                    new_pub = (md_raw.get('publisher') or {}).get('name')
                    if 'lock_publisher' not in tags:
                        if new_pub and new_pub != meta.get('publisher'):
                            abs_updates['publisher'] = new_pub
                            log_updates.append(f"Publisher: '{meta.get('publisher')}' -> '{new_pub}'")
                    elif new_pub and new_pub != meta.get('publisher'): logging.info(f"        🔒 Publisher Update Skipped (Locked): '{new_pub}'")

                    if 'lock_year' not in tags:
                        new_year = parse_release_year(md_raw.get('releaseDate'))
                        if new_year and new_year != str(meta.get('publishedYear') or ''):
                            abs_updates['publishedYear'] = new_year
                            log_updates.append(f"Year: '{meta.get('publishedYear')}' -> '{new_year}'")
                    else:
                        new_year = parse_release_year(md_raw.get('releaseDate'))
                        if new_year and new_year != str(meta.get('publishedYear') or ''): logging.info(f"        🔒 Year Update Skipped (Locked): '{new_year}'")
                    
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
                        new_genres_list = [c.get('name') for c in (md_raw.get('categories') or []) if c.get('name')]
                        added_genres = [g for g in new_genres_list if g not in current_genres]
                        if added_genres:
                            abs_updates['genres'] = current_genres + added_genres
                            log_updates.append(f"Genres: +{added_genres}")
                    else:
                        current_genres = meta.get('genres') or []
                        new_genres_list = [c.get('name') for c in (md_raw.get('categories') or []) if c.get('name')]
                        added_genres = [g for g in new_genres_list if g not in current_genres]
                        if added_genres: logging.info(f"        🔒 Genre Update Skipped (Locked): +{added_genres}")

                    if 'lock_series' not in tags:
                        if series_list := md_raw.get('series'):
                            new_series_list = []
                            for s_obj in series_list:
                                s_name = s_obj.get('name')
                                s_seq = None
                                if part_txt := s_obj.get('part'):
                                    if m := re.search(r'(\d+(?:\.\d+)?)', part_txt): s_seq = m.group(1)
                                if s_seq is None and s_name:
                                    search_texts = []
                                    if aud_data:
                                        aud_t = aud_data.get('title_raw', '')
                                        aud_s = aud_data.get('subtitle_raw', '')
                                        if aud_t or aud_s: search_texts.append(f"{aud_t} {aud_s}".strip())
                                    search_texts.append(title)
                                    for search_text in search_texts:
                                        pattern = re.escape(s_name) + r'[\s:,-]+(\d+(?:\.\d+)?)'
                                        if m := re.search(pattern, search_text, re.IGNORECASE): s_seq = m.group(1); break
                                        if m := re.search(r'(?:Teil|Band|Book|Vol\.?)\s*(\d+(?:\.\d+)?)', search_text, re.IGNORECASE): s_seq = m.group(1); break
                                if s_name: new_series_list.append({"name": s_name, "sequence": s_seq})
                            
                            curr_series_list = meta.get('series') or []
                            curr_series_norm = []
                            for s in curr_series_list: curr_series_norm.append({"name": s.get('name'), "sequence": s.get('sequence')})
                            if new_series_list and new_series_list != curr_series_norm:
                                abs_updates['series'] = new_series_list
                                s_log_str = ", ".join([f"'{x['name']}' #{x['sequence']}" for x in new_series_list])
                                log_updates.append(f"Series Updated: {s_log_str}")
                    else: logging.info("        🔒 Series Update Skipped (Locked)")

                    if abs_updates:
                        logging.info(f"        🛠️ Meta Updates{' (DRY_RUN, not saved)' if DRY_RUN else ''}:")
                        for upd in log_updates: logging.info(f"          -> {upd}")
                        if not DRY_RUN: abs_session.patch(f"{ABS_URL}/api/items/{iid}/media", json={"metadata": abs_updates})
                        stats['meta_updated'] += 1
                    else: logging.info("        ✅ No metadata updates necessary.")

                gr_title = (aud_data or {}).get('title_raw') or search_title  # Audible's clean title searches best
                cached_url = None if item.get('_manual_trigger') else GR_CACHE.get(key)
                gr_data, gr_status = get_goodreads_data(meta.get('isbn'), gr_title, authors, authors[0] if authors else "", cached_url)
                if gr_data and gr_data.get('url') and not DRY_RUN: GR_CACHE[key] = gr_data['url']
                
                if gr_data and not DRY_RUN:
                    if 'lock_isbn' not in tags:
                          # Only accept real ISBNs (never write an ASIN into the ISBN field)
                          new_id = normalize_isbn(gr_data.get('isbn'))
                          if new_id and normalize_isbn(meta.get('isbn')) != new_id:
                            logging.info(f"        🔧 ISBN Fixed/Added: {new_id}")
                            abs_session.patch(f"{ABS_URL}/api/items/{iid}/media", json={"metadata": {"isbn": new_id}})
                            stats['isbn_added' if not meta.get('isbn') else 'isbn_repaired'] += 1
                    else: logging.info("        🔒 ISBN Update Skipped (Locked)")

                if 'lock_description' not in tags:
                    cur_desc = meta.get('description') or ''  # ABS sends null for empty descriptions
                    old_aud = (RE_AUDIBLE_BLOCK.search(cur_desc) or [None, None])[1]
                    old_gr = (RE_GR_BLOCK.search(cur_desc) or [None, None])[1]
                    final_desc = build_description(cur_desc, aud_data, gr_data, old_aud and old_aud.strip(), old_gr and old_gr.strip())
                    
                    has_aud = bool(aud_data and int(aud_data.get('count', 0)) > 0)
                    has_gr = bool(gr_data)
                    
                    if not DRY_RUN:
                        if abs_session.patch(f"{ABS_URL}/api/items/{iid}/media", json={"metadata": {"description": final_desc}}).status_code == 200:
                            success_parts = []
                            if has_aud: success_parts.append("Audible")
                            if has_gr: success_parts.append("Goodreads")
                            success_str = f"({', '.join(success_parts)})" if success_parts else "Data Cleaned"
                            logging.info(f"      -> ✅ SUCCESS: {success_str}")
                            if has_aud or has_gr: stats['success'] += 1
                        else: stats['failed'] += 1
                    else:
                        if has_aud or has_gr: stats['success'] += 1
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
                    days = GR_BLOCKED_RECHECK_DAYS if gr_status == 'blocked' else GR_MISSING_RECHECK_DAYS
                    logging.info(f"      -> ℹ️ Audible OK, Goodreads {gr_status.replace('_', ' ')}.")
                    record_attempt(key, history, failed, recheck, recheck_days=days)
                elif no_ratings_yet:
                    stats['no_ratings_yet'] += 1
                    days = NO_RATINGS_RECHECK_DAYS if gr_status != 'blocked' else GR_BLOCKED_RECHECK_DAYS
                    record_attempt(key, history, failed, recheck, recheck_days=days)
                else:
                    logging.warning(f"      -> ⚠️ No Audible data (Audible: {has_aud}, GR: {has_gr})")
                    record_attempt(key, history, failed, recheck)
                
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
                break
        
        if stats['aborted_ratelimit']: break
        if not needs_sleep: continue
        sleep_dur = BASE_SLEEP + random.uniform(1, 3)
        if search_penalty: sleep_dur += SEARCH_PENALTY_SLEEP
        time.sleep(sleep_dur)

def main():
    if not ABS_URL or not API_TOKEN: return print("Error: Envs missing.")
    log_file = setup_logging()
    try:
        logging.info("Checking API connection...")
        if abs_session.get(f"{ABS_URL}/api/libraries").status_code != 200:
            return print("Error: Cannot connect to ABS API (Check URL/Token).")
    except Exception as e:
        return print(f"Error: Connection failed: {e}")

    reports['audible'] = {x['key']: x for x in rw_json(os.path.join(REPORT_DIR, "missing_audible.json"))}
    reports['goodreads'] = {x['key']: x for x in rw_json(os.path.join(REPORT_DIR, "missing_goodreads.json"))}
    
    logging.info("--- Start ---")
    start_time = datetime.now()
    history, failed, recheck = rw_json(HISTORY_FILE), rw_json(FAILED_FILE), rw_json(RECHECK_FILE)
    GR_CACHE.update(rw_json(GR_CACHE_FILE))
    for lib in LIBRARY_IDS: process_library(lib, history, failed, recheck)
    if not DRY_RUN: rw_json(HISTORY_FILE, history); rw_json(FAILED_FILE, failed); rw_json(RECHECK_FILE, recheck)
    save_reports()
    write_env_file(log_file, start_time)
    logging.info(f"--- Done. Stats: {stats} ---")

if __name__ == "__main__": main()
