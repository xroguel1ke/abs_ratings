import os
import time

# --- TIMEZONE FIX ---
os.environ['TZ'] = 'Europe/Berlin'
try:
    time.tzset()
except: pass

import requests
from bs4 import BeautifulSoup
import re, json, random, difflib, logging, urllib.parse
from datetime import datetime

# ================= CONFIGURATION =================
ABS_URL = os.getenv('ABS_URL', '').rstrip('/')
API_TOKEN = os.getenv('API_TOKEN')
LIBRARY_IDS = [l.strip() for l in os.getenv('LIBRARY_IDS', '').split(',') if l.strip()]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")
REPORT_DIR = os.path.join(SCRIPT_DIR, "reports")
HISTORY_FILE = os.path.join(SCRIPT_DIR, "rating_history.json")
FAILED_FILE = os.path.join(SCRIPT_DIR, "failed_history.json")
ENV_OUTPUT_FILE = os.path.join(SCRIPT_DIR, "last_run.env")

REFRESH_DAYS = int(os.getenv('REFRESH_DAYS', 90))
MAX_BATCH_SIZE = int(os.getenv('BATCH_SIZE', 150))
MAX_FAIL_ATTEMPTS = 5
MAX_CONSECUTIVE_RL = 3
RECOVERY_PAUSE = 60
BASE_SLEEP = int(os.getenv('SLEEP_TIMER', 6))
SEARCH_PENALTY_SLEEP = 10  # Extra sleep after expensive search operations
DRY_RUN = os.getenv('DRY_RUN', 'False').lower() == 'true'

# --- HEADERS & CONSTANTS ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/122.0.0.0"
]

GERMAN_LANG_CODES = ['de', 'deu', 'ger', 'german', 'deutsch']

# Base Headers
HEADERS_BASE = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Referer": "https://www.google.com/",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-User": "?1"
}

HEADERS_ABS = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
abs_session = requests.Session()
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

stats = {k: 0 for k in ["processed", "success", "failed", "no_data", "skipped", "partial", "cooldown", "recycled", "asin_found", "isbn_added", "isbn_repaired", "asin_migrated", "meta_updated"]}
stats['aborted_ratelimit'] = False
reports = {"audible": {}, "goodreads": {}}

class RateLimitException(Exception):
    def __init__(self, msg, is_hard=False): super().__init__(msg); self.is_hard = is_hard

# ================= UTILS =================

def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    f = os.path.join(LOG_DIR, f"run_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(f, encoding='utf-8'), logging.StreamHandler()])
    return f

# UPDATED: Atomic Writes for Safety
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
    
    body = f"Proc: {stats['processed']} | New: {stats['success']} | MetaUpd: {stats['meta_updated']} | ASIN+: {stats['asin_found']} | Mig: {stats['asin_migrated']} | ISBN+: {stats['isbn_added']} | Fix: {stats['isbn_repaired']} | Err: {stats['failed']}"
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
    return ("🌕" * min(full, 5) + "🌗" * (1 if 0.25 <= decimal < 0.75 else 0)).ljust(5, "🌑")[:5]

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
    h["User-Agent"] = random.choice(USER_AGENTS)
    if domain and "audible.de" in domain:
        h["Accept-Language"] = "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7"
    else:
        h["Accept-Language"] = "en-US,en;q=0.9"
    return h

def fetch_url(url, params=None, domain=None):
    try:
        headers = get_headers(domain)
        cookies = {} 
        r = requests.get(url, headers=headers, params=params, cookies=cookies, timeout=20)
        
        if r.status_code == 429: raise RateLimitException("HTTP 429", True)
        if r.status_code in [503, 403]: raise RateLimitException(f"HTTP {r.status_code}")
        
        soup = BeautifulSoup(r.text, 'lxml')
        if soup.title and "captcha" in (soup.title.text).lower(): raise RateLimitException("Captcha detected")
        return r, soup
    except RateLimitException: raise
    except Exception as e: return None, None

def scrape_search_result_fallback(domain, asin):
    try:
        h = get_headers(domain) 
        logging.info(f"        🔎 Attempting Search Fallback on {domain}...")
        r = requests.get(f"https://{domain}/search", params={"keywords": asin, "ipRedirectOverride": "true"}, headers=h, timeout=15)
        soup = BeautifulSoup(r.text, 'lxml')
        
        item = soup.find('li', attrs={'data-asin': asin}) or (soup.find('div', attrs={'data-asin': asin}).find_parent('li') if soup.find('div', attrs={'data-asin': asin}) else None)
        if item:
            ratings = {}
            if rate_txt := item.find('span', class_=re.compile(r'ratingLabel|ratingText')):
                if m := re.search(r'(\d+[.,]?\d*)', rate_txt.get_text()):
                     if is_valid_rating(m.group(1).replace(',', '.')):
                           ratings['overall'] = m.group(1).replace(',', '.')
            if count_txt := item.find('span', class_=re.compile(r'ratingsLabel|ratingCount')):
                if m := re.search(r'([\d,.]+)', count_txt.get_text()): ratings['count'] = int(re.sub(r'[^\d]', '', m.group(1)))
            if ratings.get('overall') and ratings.get('count'): return ratings
    except: pass
    return None

# ================= CORE LOGIC =================

def get_audible_data(asin, language):
    if not asin: return None
    
    domains = ["www.audible.com", "www.audible.de"]
    if language and str(language).strip().lower() in ['de', 'deu', 'german', 'deutsch']:
        domains = ["www.audible.de", "www.audible.com"]

    best_result = None

    for domain in domains:
        logging.info(f"      -> Checking {domain}...")
        
        url = f"https://{domain}/pd/{asin}?ipRedirectOverride=true"
        cookies = {}
        if "audible.de" in domain: cookies["audible_site_preference"] = "de"
        elif "audible.com" in domain: cookies["audible_site_preference"] = "us"
        
        headers = get_headers(domain)

        try:
            r = requests.get(url, headers=headers, cookies=cookies, timeout=15)
            
            # --- SOFT FAIL & "NO RESULTS" DETECTION ---
            txt_lower = r.text.lower()
            soup = BeautifulSoup(r.text, 'lxml')
            title_lower = (soup.title.text if soup.title else "").lower()
            
            soft_404_markers = [
                "looks like this title is no longer available",
                "titel ist leider nicht verfügbar",
                "no results for",
                "keine ergebnisse für"
            ]

            if any(marker in txt_lower for marker in soft_404_markers) or "search" in title_lower:
                logging.info(f"        ⚠️ Soft-404 (Not Available/No Results/Search Page) on {domain}")
                if domain in ["www.audible.com", "www.audible.de"]:
                    if fb := scrape_search_result_fallback(domain, asin):
                        logging.info(f"        ✅ Found via Search Fallback (Soft-404) (Count: {fb['count']})")
                        fb['domain'] = domain
                        return fb
                continue

            # HARD FAIL
            if r.status_code == 404 or "/pderror" in r.url:
                logging.info(f"        ❌ 404/Error on {domain}")
                if domain in ["www.audible.com", "www.audible.de"]:
                    if fb := scrape_search_result_fallback(domain, asin):
                        logging.info(f"        ✅ Found via Search Fallback (404) (Count: {fb['count']})")
                        fb['domain'] = domain
                        return fb
                continue

            # --- EXTRACTION ---
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
            
            # UPDATED: Parse Extra Metadata JSON
            try:
                if json_script := soup.find('script', type='application/json'):
                    md = json.loads(json_script.string)
                    if isinstance(md, list): md = md[0]
                    if isinstance(md, dict):
                        ratings['meta_raw'] = md
            except: pass

            # 1. TAGS (Priority 1)
            if sum_tag := soup.find('adbl-rating-summary'):
                if is_valid_rating(sum_tag.get('performance-value')): ratings['performance'] = sum_tag.get('performance-value')
                if is_valid_rating(sum_tag.get('story-value')): ratings['story'] = sum_tag.get('story-value')
                
                if st := sum_tag.find('adbl-star-rating'): 
                    if is_valid_rating(st.get('value')): ratings['overall'] = st.get('value')
                    ratings['count'] = st.get('count')

            # 2. JSON (Priority 2)
            if not ratings.get('count') or not ratings.get('overall'):
                for s in soup.find_all('script', type='application/ld+json'):
                    try:
                        d = json.loads(s.string)
                        for i in (d if isinstance(d, list) else [d]):
                            if 'aggregateRating' in i: 
                                val = i['aggregateRating'].get('ratingValue')
                                if is_valid_rating(val): ratings['overall'] = val
                    except: pass

            # 3. REGEX (Priority 3 - Last Resort)
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
                logging.info(f"        ⚠️ Page OK (200), but 0 Ratings/Invalid Rating found.")
                if best_result is None: best_result = {'count': 0, 'source': 'Empty', 'domain': domain}

        except Exception as e:
            logging.error(f"        ⚠️ Request failed: {e}")
            continue

    if best_result: return best_result
    return None

def find_missing_asin(title, authors_list, duration, lang, force_domain=None):
    logging.info(f"      -> 🔎 Searching Replacement ASIN for '{title}'...")
    doms = ["www.audible.com", "www.audible.de"]
    if lang and str(lang).strip().lower() in GERMAN_LANG_CODES: doms = ["www.audible.de", "www.audible.com"]
    
    prim_auth = authors_list[0] if authors_list else ""
    
    strategies = [
        {"params": {"title": title, "author_author": prim_auth, "ipRedirectOverride": "true"}, "mode": "Strict"},
        {"params": {"title": title, "ipRedirectOverride": "true"}, "mode": "TitleOnly"}
    ]

    for d in doms:
        for strat in strategies:
            
            r, soup = fetch_url(f"https://{d}/search", params=strat["params"], domain=d)
            
            if not soup: continue
            
            for item in soup.find_all('li', class_=re.compile(r'productListItem')):
                asin = item.get('data-asin') or (item.find('div', attrs={'data-asin': True}) or {}).get('data-asin')
                if not asin: continue
                
                ft = item.find('h3', class_=re.compile(r'bc-heading'))
                if not ft: continue
                found_title = ft.get_text(strip=True)
                
                t_score = difflib.SequenceMatcher(None, title.lower(), found_title.lower()).ratio()
                if t_score < 0.7: 
                    continue

                dur_match = False
                found_dur_sec = 0
                if rt := item.find('li', class_=re.compile(r'runtimeLabel')):
                    h = re.search(r'(\d+)\s*(?:Std|hr|h)', rt.text)
                    m = re.search(r'(\d+)\s*(?:Min|m)', rt.text)
                    found_dur_sec = (int(h.group(1))*3600 if h else 0) + (int(m.group(1))*60 if m else 0)
                    if found_dur_sec > 0:
                        if duration and abs(duration - found_dur_sec) < 300: dur_match = True
                    else:
                        dur_match = True 

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

def scrape_gr_details(url):
    r, soup = fetch_url(url)
    if not soup: return None
    res = {'url': url, 'source': 'GR'}
    for s in soup.find_all('script', type='application/ld+json'):
        try:
            d = json.loads(s.string)
            if 'aggregateRating' in d:
                res['val'] = d['aggregateRating'].get('ratingValue')
                res['count'] = d['aggregateRating'].get('reviewCount') or d['aggregateRating'].get('ratingCount')
            if 'isbn' in d: res['isbn'] = d['isbn']
        except: pass

    if 'val' not in res:
        if m := re.search(r'(\d+[.,]\d+)\s+avg rating', soup.get_text()): res['val'] = m.group(1).replace(',', '.')
    if 'count' not in res:
        if m := re.search(r'([\d,.]+)\s+ratings', soup.get_text()): res['count'] = int(re.sub(r'[^\d]', '', m.group(1)))
    if 'isbn' not in res: res['isbn'] = (soup.find('meta', property="books:isbn") or {}).get('content')
    if 'isbn' not in res and (m := RE_ISBN_JSON.search(r.text)): res['isbn'] = m.group(1)
    if 'asin' not in res:
        if m := RE_ASIN_JSON.search(r.text) or RE_URL_ASIN.search(r.text): res['asin'] = m.group(1)
        if not res.get('asin'):
            if m := re.search(r'ASIN[:\s]*(B0\w+)', soup.get_text()): res['asin'] = m.group(1)
    return res if 'val' in res else None

def get_goodreads_data(isbn, asin, title, authors, prim_auth):
    logging.info("      -> Checking www.goodreads.com")
    # 1. ID Search
    for q_id, src in [(isbn, 'ISBN Lookup'), (asin, 'ASIN Lookup')]:
        if q_id:
            if d := scrape_gr_details(f"https://www.goodreads.com/search?q={q_id}"):
                d['source'] = src
                logging.info(f"        ✅ Found via {src} (Count: {d.get('count')}, Rating: {round(safe_float(d.get('val')), 2)})")
                return d
    
    # 2. Text Search
    searches = [f"{t} {prim_auth}" for t in [title, clean_title(title)] if t] + [title]
    base_title = clean_title(title)
    if base_title and base_title != title: searches.append(base_title)
    norm_target = normalize_title_text(title)

    for q in searches:
        r, soup = fetch_url(f"https://www.goodreads.com/search", params={"q": q})
        if not soup: continue
        
        if "/book/show/" in r.url:
            if d := scrape_gr_details(r.url): 
                d['source'] = 'Text Search (Direct Hit)'
                logging.info(f"        ✅ Found via Text Search (Direct) (Count: {d.get('count')}, Rating: {round(safe_float(d.get('val')), 2)})")
                return d
        else:
            best_url, best_score = None, 0.0
            for row in soup.find_all('tr', itemtype="http://schema.org/Book"):
                link = row.find('a', class_='bookTitle')
                if not link: continue
                found_title = link.get_text(strip=True)
                
                norm_found = normalize_title_text(found_title)
                t_score = difflib.SequenceMatcher(None, norm_target, norm_found).ratio()
                
                if (len(norm_target) > 3 and norm_target in norm_found) or \
                   (len(norm_found) > 3 and norm_found in norm_target): t_score += 0.15
                
                f_nums, t_nums = extract_volume(found_title), extract_volume(title)
                if (f_nums and t_nums and not f_nums & t_nums): 
                    if t_score < 0.9: continue
                
                found_auth = row.find('a', class_='authorName').text if row.find('a', class_='authorName') else ""
                if not match_author(authors, found_auth): continue
                
                if t_score > 0.75 and t_score > best_score:
                    best_score, best_url = t_score, "https://www.goodreads.com" + link['href']
            
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

def process_library(lib_id, history, failed):
    logging.info(f"--- Processing Library: {lib_id} ---")
    try:
        r = abs_session.get(f"{ABS_URL}/api/libraries/{lib_id}/items")
        items = r.json()['results']
    except Exception as e: logging.error(f"Lib Error: {e}"); return

    queue = [i for i in items if f"{lib_id}_{i['id']}" not in history]
    due = [i for i in items if i not in queue and (datetime.now() - datetime.strptime(history.get(f"{lib_id}_{i['id']}", "2000-01-01"), "%Y-%m-%d")).days >= REFRESH_DAYS]
    work_queue = queue + due
    random.shuffle(work_queue)
    total = min(len(work_queue), MAX_BATCH_SIZE)
    logging.info(f"Queue: {len(queue)} New, {len(due)} Due. Total: {total}")
    
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
        
        search_penalty = False # Flag for extra sleep

        while True: # Retry Loop
            try:
                iid, key = item['id'], f"{lib_id}_{item['id']}"
                meta = abs_session.get(f"{ABS_URL}/api/items/{iid}").json()['media']['metadata']
                title, asin, lang = meta.get('title'), meta.get('asin'), meta.get('language')
                authors = [a.get('name') if isinstance(a, dict) else a for a in meta.get('authors', [])]
                
                logging.info(f"-"*50)
                logging.info(f"({idx+1}/{total}) [ETA: {eta_str}] {title} [ASIN: {asin}] (Try {failed.get(key,0)+1}/{MAX_FAIL_ATTEMPTS})")
                stats['processed'] += 1 

                # 1. AUDIBLE
                aud_data = get_audible_data(asin, lang)
                
                # REPLACEMENT LOGIC
                should_search = False
                if not asin:
                    logging.info("      -> ⚠️ No ASIN in ABS.")
                    should_search = True
                elif aud_data is None:
                    logging.info("      -> ⚠️ ASIN not found (All domains).")
                    should_search = True
                elif int(aud_data.get('count', 0)) == 0:
                    logging.info("      -> ⚠️ Found 0 Ratings.")
                    should_search = True
                elif (str(lang).lower() not in GERMAN_LANG_CODES) and aud_data.get('domain') == 'www.audible.de':
                    logging.info("      -> ⚠️ Non-German Book only found on .de (Possible broken .com ASIN). Attempting Fix...")
                    should_search = True
                elif (str(lang).lower() in GERMAN_LANG_CODES) and aud_data.get('domain') == 'www.audible.com':
                    logging.info("      -> ⚠️ German Book only found on .com (Possible broken .de ASIN). Attempting Fix...")
                    should_search = True

                if should_search:
                    search_penalty = True # Mark as expensive operation
                    found = None
                    
                    if str(lang).lower() in GERMAN_LANG_CODES:
                        if aud_data and aud_data.get('variant_asin_de'):
                             found = aud_data['variant_asin_de']
                             logging.info(f"        🔗 Found ASIN via HTML Link (hreflang='de-de'): {found}")
                    
                    else:
                        if aud_data and aud_data.get('variant_asin_us'):
                             found = aud_data['variant_asin_us']
                             logging.info(f"        🔗 Found ASIN via HTML Link (hreflang='en-us'): {found}")

                    if not found:
                          found = find_missing_asin(title, authors, item['media'].get('duration'), lang)
                    
                    if found:
                        if found != asin:
                            logging.info(f"        ✨ NEW ASIN Found: {found}")
                            if not DRY_RUN: 
                                abs_session.patch(f"{ABS_URL}/api/items/{iid}/media", json={"metadata": {"asin": found}})
                                logging.info(f"        💾 ASIN updated in ABS.")
                            asin, stats['asin_found'] = found, stats['asin_found'] + 1
                            stats['asin_migrated'] += 1
                            aud_data = get_audible_data(asin, lang)
                        else:
                             logging.info(f"        ℹ️ Search returned same ASIN. Keeping fallback data.")
                    else:
                        logging.info(f"        ℹ️ No replacement found. Keeping fallback data.")

                time.sleep(1)
                
                # UPDATED: Extended Metadata Sync
                if aud_data and aud_data.get('meta_raw') and not DRY_RUN:
                    md_raw = aud_data['meta_raw']
                    abs_updates = {}
                    log_updates = []

                    # 1. Publisher
                    new_pub = (md_raw.get('publisher') or {}).get('name')
                    if new_pub and new_pub != meta.get('publisher'):
                        abs_updates['publisher'] = new_pub
                        log_updates.append(f"Publisher: '{meta.get('publisher')}' -> '{new_pub}'")

                    # 2. Publish Year (from releaseDate "MM-DD-YY")
                    if rel_date := md_raw.get('releaseDate'):
                        try:
                            # Audible sometimes uses MM-DD-YY
                            new_year = "20" + rel_date.split('-')[-1] if len(rel_date.split('-')[-1]) == 2 else rel_date.split('-')[-1]
                            if new_year.isdigit() and new_year != meta.get('publishedYear'):
                                abs_updates['publishedYear'] = new_year
                                log_updates.append(f"Year: '{meta.get('publishedYear')}' -> '{new_year}'")
                        except: pass

                    # 3. Language
                    new_lang = md_raw.get('language')
                    if new_lang and new_lang != meta.get('language'):
                        abs_updates['language'] = new_lang
                        log_updates.append(f"Language: '{meta.get('language')}' -> '{new_lang}'")
                    
                    # 4. Abridged (Checkbox)
                    fmt = md_raw.get('format', '').lower()
                    new_abridged = True if 'abridged' in fmt and 'unabridged' not in fmt else False
                    if new_abridged != meta.get('abridged'):
                        abs_updates['abridged'] = new_abridged
                        log_updates.append(f"Abridged: {meta.get('abridged')} -> {new_abridged}")

                    # 5. Genres (Append only)
                    current_genres = meta.get('genres') or []
                    new_genres_list = [c.get('name') for c in md_raw.get('categories', []) if c.get('name')]
                    added_genres = [g for g in new_genres_list if g not in current_genres]
                    if added_genres:
                        abs_updates['genres'] = current_genres + added_genres
                        log_updates.append(f"Genres: +{added_genres}")

                    # 6. Series
                    if series_list := md_raw.get('series'):
                        s_obj = series_list[0] # Take first series
                        new_series = s_obj.get('name')
                        new_seq = None
                        if part_txt := s_obj.get('part'):
                            if m := re.search(r'(\d+)', part_txt): new_seq = m.group(1)
                        
                        # Only update if series name is different OR if sequence is different (and exists)
                        # Audible is master. Overwrite ABS if diff.
                        update_series = False
                        curr_series_list = meta.get('series') or []
                        
                        # Note: ABS supports multiple series, but usually 1. 
                        # If ABS has no series, or the first series name differs -> Update
                        curr_series_name = curr_series_list[0].get('name') if curr_series_list else None
                        curr_seq = curr_series_list[0].get('sequence') if curr_series_list else None

                        if new_series and (new_series != curr_series_name or (new_seq and new_seq != curr_seq)):
                            # ABS format: "series": [{"name": "X", "sequence": "1"}]
                            abs_updates['series'] = [{"name": new_series, "sequence": new_seq}]
                            log_updates.append(f"Series: '{curr_series_name}' #{curr_seq} -> '{new_series}' #{new_seq}")

                    if abs_updates:
                        logging.info(f"        🛠️ Meta Updates:")
                        for upd in log_updates:
                             logging.info(f"          -> {upd}")
                        abs_session.patch(f"{ABS_URL}/api/items/{iid}/media", json={"metadata": abs_updates})
                        stats['meta_updated'] += 1
                    else:
                        logging.info("        ✅ No metadata updates necessary.")

                # 2. GOODREADS
                gr_data = get_goodreads_data(meta.get('isbn'), asin, title, authors, authors[0] if authors else "")
                
                # ISBN REPAIR
                if gr_data and not DRY_RUN:
                      new_id = gr_data.get('isbn') or gr_data.get('asin')
                      if new_id and str(meta.get('isbn') or "").replace('-','') != str(new_id).replace('-',''):
                        logging.info(f"        🔧 ISBN Fixed/Added: {new_id}")
                        abs_session.patch(f"{ABS_URL}/api/items/{iid}/media", json={"metadata": {"isbn": new_id}})
                        stats['isbn_added' if not meta.get('isbn') else 'isbn_repaired'] += 1

                # 3. UPDATE
                old_aud = (RE_AUDIBLE_BLOCK.search(meta.get('description', '')) or [None, None])[1]
                old_gr = (RE_GR_BLOCK.search(meta.get('description', '')) or [None, None])[1]
                final_desc = build_description(meta.get('description', ''), aud_data, gr_data, old_aud and old_aud.strip(), old_gr and old_gr.strip())
                
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

                # 4. HISTORY
                update_report("audible", key, title, authors[0] if authors else "", asin, "Not found", has_aud)
                update_report("goodreads", key, title, authors[0] if authors else "", meta.get('isbn'), "Not found", has_gr)
                
                fails = failed.get(key, 0) + 1
                
                if has_aud and has_gr:
                    history[key] = datetime.now().strftime("%Y-%m-%d"); failed.pop(key, None)
                elif fails >= MAX_FAIL_ATTEMPTS:
                    logging.info("      -> 🛑 Max attempts reached."); history[key] = datetime.now().strftime("%Y-%m-%d"); failed.pop(key, None)
                else:
                    failed[key] = fails; logging.warning(f"      -> ❌ Partial/No data (Audible: {has_aud}, GR: {has_gr}). Strike {fails}/{MAX_FAIL_ATTEMPTS}")
                
                # SAVE IMMEDIATELY (Atomic)
                rw_json(HISTORY_FILE, history)
                rw_json(FAILED_FILE, failed)
                
                consecutive_rl = 0
                break # Success!

            except RateLimitException as e:
                consecutive_rl += 1
                logging.warning(f"🛑 Rate Limit DETECTED: {e}")
                if e.is_hard or consecutive_rl >= MAX_CONSECUTIVE_RL: 
                    logging.error("🛑 ABORTING script due to Rate Limits."); stats['aborted_ratelimit'] = True; break
                time.sleep(RECOVERY_PAUSE * consecutive_rl)
            except Exception as e:
                logging.error(f"Item Error: {e}"); stats['failed'] += 1; break
        
        if stats['aborted_ratelimit']: break
        
        # UPDATED: Sleep Logic (Search Penalty)
        sleep_dur = BASE_SLEEP + random.uniform(1, 3)
        if search_penalty:
            sleep_dur += SEARCH_PENALTY_SLEEP
        
        time.sleep(sleep_dur)

def main():
    if not ABS_URL or not API_TOKEN: return print("Error: Envs missing.")
    
    # SETUP LOGGING FIRST
    log_file = setup_logging()
    
    # Connection Check
    try:
        logging.info("Checking API connection...")
        if abs_session.get(f"{ABS_URL}/api/libraries").status_code != 200:
            return print("Error: Cannot connect to ABS API (Check URL/Token).")
    except Exception as e:
        return print(f"Error: Connection failed: {e}")

    # Reports Init
    reports['audible'] = {x['key']: x for x in rw_json(os.path.join(REPORT_DIR, "missing_audible.json"))}
    reports['goodreads'] = {x['key']: x for x in rw_json(os.path.join(REPORT_DIR, "missing_goodreads.json"))}
    
    logging.info("--- Start ---")
    start_time = datetime.now()
    history, failed = rw_json(HISTORY_FILE), rw_json(FAILED_FILE)
    
    for lib in LIBRARY_IDS: process_library(lib, history, failed)
    
    rw_json(HISTORY_FILE, history); rw_json(FAILED_FILE, failed); save_reports()
    write_env_file(log_file, start_time)
    logging.info(f"--- Done. Stats: {stats} ---")

if __name__ == "__main__": main()
