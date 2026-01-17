import requests
from bs4 import BeautifulSoup
import time
import re
import json
import os
import random
import urllib.parse
import difflib
import logging
from datetime import datetime

# ================= CONFIGURATION =================
# Audiobookshelf URL and Token
ABS_URL = os.getenv('ABS_URL', '').rstrip('/')
API_TOKEN = os.getenv('API_TOKEN')
# List of Library IDs to process
LIBRARY_IDS = [l.strip() for l in os.getenv('LIBRARY_IDS', '').split(',') if l.strip()]

# Paths (Mapped from Host)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")
REPORT_DIR = os.path.join(SCRIPT_DIR, "reports")
HISTORY_FILE = os.path.join(SCRIPT_DIR, "rating_history.json")
FAILED_FILE = os.path.join(SCRIPT_DIR, "failed_history.json")
ENV_OUTPUT_FILE = os.path.join(SCRIPT_DIR, "last_run.env")

# Settings
REFRESH_DAYS = int(os.getenv('REFRESH_DAYS', 90))
MAX_BATCH_SIZE = int(os.getenv('BATCH_SIZE', 150))
MAX_FAIL_ATTEMPTS = 5
MAX_CONSECUTIVE_RATE_LIMITS = 3  # Abort after 3 consecutive errors
RECOVERY_PAUSE = 60             # Wait 60s to recover from a potential block
DRY_RUN = os.getenv('DRY_RUN', 'False').lower() == 'true'
BASE_SLEEP = int(os.getenv('SLEEP_TIMER', 6)) 
# =================================================

# Headers for ABS API calls
HEADERS_ABS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

# Headers for scraping
HEADERS_SCRAPE = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9"
}

# Statistics tracker
stats = {
    "processed": 0, "success": 0, "failed": 0, "no_data": 0, 
    "skipped": 0, "partial": 0, "cooldown": 0, "recycled": 0, 
    "asin_found": 0, "isbn_added": 0, "isbn_repaired": 0,
    "aborted_ratelimit": False
}

# In-Memory Report Storage
report_audible = {}
report_goodreads = {}

class RateLimitException(Exception):
    """Custom exception raised when a rate limit or captcha is detected."""
    def __init__(self, message, is_hard_limit=False):
        super().__init__(message)
        self.is_hard_limit = is_hard_limit

def setup_logging():
    """Sets up logging to file and console."""
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(LOG_DIR, f"run_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return log_file

def write_env_file(log_filename, start_time):
    """Writes a simple env file for the bash script to source."""
    duration = datetime.now() - start_time
    minutes, seconds = divmod(duration.total_seconds(), 60)
    duration_str = f"{int(minutes)}m {int(seconds)}s"

    if stats['aborted_ratelimit']:
        status_subject = "ABS Ratings: Abbruch 🛑"
        status_icon = "alert"
        status_header = "Rate Limit erkannt!"
    elif stats['failed'] > 0:
        status_subject = "ABS Ratings: Fehler ❌"
        status_icon = "alert"
        status_header = "Fehler aufgetreten!"
    elif stats['success'] > 0 or stats['recycled'] > 0 or stats['asin_found'] > 0:
        status_subject = "ABS Ratings: Erfolg ✅"
        status_icon = "normal"
        status_header = "Update abgeschlossen"
    else:
        status_subject = "ABS Ratings: Info ℹ️"
        status_icon = "normal"
        status_header = "Keine Änderungen"

    report_body = (f"Proc: {stats['processed']} | New: {stats['success']} | "
                   f"ASIN+: {stats['asin_found']} | ISBN+: {stats['isbn_added']} | "
                   f"ISBN Fix: {stats['isbn_repaired']} | Err: {stats['failed']}")
    
    if stats['aborted_ratelimit']:
        report_body += " | ⚠️ ABORTED (Rate Limit)"

    try:
        with open(ENV_OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write(f"ABS_SUBJECT='{status_subject}'\n")
            f.write(f"ABS_ICON='{status_icon}'\n")
            f.write(f"ABS_HEADER='{status_header}'\n")
            f.write(f"ABS_DURATION='{duration_str}'\n")
            f.write(f"ABS_REPORT_BODY='{report_body}'\n")
            f.write(f"ABS_LOG_FILE='{os.path.basename(log_filename)}'\n")
    except Exception as e:
        logging.error(f"Could not write env file: {e}")

def load_json(path):
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f: return json.load(f)
        except: return {}
    return {}

def save_json(path, data):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f: json.dump(data, f, indent=4, ensure_ascii=False)
    except: pass

# === REPORTING FUNCTIONS ===

def load_reports():
    global report_audible, report_goodreads
    if not os.path.exists(REPORT_DIR):
        os.makedirs(REPORT_DIR)
    
    f_aud = os.path.join(REPORT_DIR, "missing_audible.json")
    f_gr = os.path.join(REPORT_DIR, "missing_goodreads.json")
    
    try:
        raw_aud = load_json(f_aud)
        if isinstance(raw_aud, list):
            report_audible = {item['key']: item for item in raw_aud if 'key' in item}
        else:
            report_audible = {}
            
        raw_gr = load_json(f_gr)
        if isinstance(raw_gr, list):
            report_goodreads = {item['key']: item for item in raw_gr if 'key' in item}
        else:
            report_goodreads = {}
    except:
        report_audible = {}
        report_goodreads = {}

def update_report(source, unique_key, title, author, identifier, reason, success):
    target_dict = report_audible if source == "audible" else report_goodreads
    
    if success:
        if unique_key in target_dict:
            del target_dict[unique_key]
    else:
        target_dict[unique_key] = {
            "key": unique_key,
            "title": title,
            "author": author,
            "identifier": identifier,
            "reason": reason,
            "last_check": datetime.now().strftime("%Y-%m-%d")
        }

def save_reports():
    f_aud = os.path.join(REPORT_DIR, "missing_audible.json")
    f_gr = os.path.join(REPORT_DIR, "missing_goodreads.json")
    
    list_aud = sorted(report_audible.values(), key=lambda x: x['title'])
    list_gr = sorted(report_goodreads.values(), key=lambda x: x['title'])
    
    save_json(f_aud, list_aud)
    save_json(f_gr, list_gr)

# === UTILS ===

def is_due_for_update(unique_key, history):
    if unique_key not in history: return True
    try:
        last_run = datetime.strptime(history[unique_key], "%Y-%m-%d")
        if (datetime.now() - last_run).days >= REFRESH_DAYS: return True
    except: return True
    return False

def remove_old_rating_block(description):
    if not description: return ""
    pattern = r'(?s)⭐\s*Ratings.*?⭐(?:\s|<br\s*/?>)*'
    description = re.sub(pattern, '', description)
    description = re.sub(r'(?s)\*\*Audible\*\*.*?---\s*\n*', '', description)
    description = re.sub(r'^(?:\s|<br\s*/?>)+', '', description, flags=re.IGNORECASE)
    return description.strip()

def safe_float(val):
    try:
        if not val: return 0.0
        return float(str(val).replace(',', '.'))
    except: return 0.0

def generate_moon_rating(val):
    try:
        v = safe_float(val)
        if v == 0: return "🌑🌑🌑🌑🌑"
        full = int(v)
        decimal = v - full
        half = 1 if 0.25 <= decimal < 0.75 else 0
        if decimal >= 0.75: full += 1
        full = min(full, 5)
        if full == 5: half = 0
        return "🌕" * full + "🌗" * half + "🌑" * (5 - full - half)
    except: return "🌑🌑🌑🌑🌑"

def check_potential_rate_limit(response, soup=None):
    """Checks for HTTP codes or Captcha content."""
    if response.status_code == 429:
        raise RateLimitException("HTTP 429 (Too Many Requests)", is_hard_limit=True)
    if response.status_code == 503:
        raise RateLimitException("HTTP 503 (Service Unavailable)")
    if response.status_code == 403:
        raise RateLimitException("HTTP 403 (Forbidden)")

    if soup:
        text_lower = soup.get_text().lower()
        title_lower = soup.title.string.lower() if soup.title else ""
        captcha_indicators = ["enter the characters you see", "robot check", "security check"]
        if "captcha" in title_lower or "robot check" in title_lower:
             raise RateLimitException("Captcha detected in Title")
        for indicator in captcha_indicators:
            if indicator in text_lower:
                if len(text_lower) < 5000: 
                    raise RateLimitException(f"Captcha detected: '{indicator}'")

# === HELPER FUNCTIONS ===

def clean_title_for_search(title):
    if not title: return ""
    title = re.sub(r'(?i)\b(unabridged|abridged|audiobook|graphic audio|dramatized adaptation)\b', '', title)
    title = re.sub(r'[\(\[].*?[\)\]]', '', title)
    if ':' in title:
        title = title.split(':')[0]
    elif ' - ' in title:
        title = title.split(' - ')[0]
    return title.strip()

def extract_volume_number(text):
    nums = set()
    if not text: return nums
    matches = re.findall(r'(?i)(?:\b(?:book|vol\.?|volume|part|no\.?)|#)\s*(\d+)', text)
    nums.update(matches)
    end_match = re.search(r'\b(\d+)$', text.strip())
    if end_match: nums.add(end_match.group(1))
    return nums

def check_numbers_match(abs_title, gr_title):
    abs_nums = extract_volume_number(abs_title)
    if not abs_nums: return True 
    gr_nums = extract_volume_number(gr_title)
    common = abs_nums.intersection(gr_nums)
    return bool(common)

def check_author_match_list(abs_authors_list, gr_author_str):
    if not abs_authors_list or not gr_author_str: return False
    
    def tokenize(s):
        s = re.sub(r'[\(\[].*?[\)\]]', '', s)
        tokens = re.split(r'[^a-zA-Z0-9]+', s.lower())
        return set([t for t in tokens if len(t) > 1])

    gr_tokens = tokenize(gr_author_str)
    
    for abs_author in abs_authors_list:
        if difflib.SequenceMatcher(None, abs_author.lower(), gr_author_str.lower()).ratio() > 0.6:
            return True
        abs_tokens = tokenize(abs_author)
        common = abs_tokens.intersection(gr_tokens)
        if len(common) >= 2 or (len(common) >= 1 and len(abs_tokens) == 1): return True
    return False

def fuzzy_match(s1, s2, threshold=0.8):
    if not s1 or not s2: return False
    return difflib.SequenceMatcher(None, s1.lower(), s2.lower()).ratio() >= threshold

def parse_audible_duration(duration_str):
    if not duration_str: return 0
    hours = 0
    mins = 0
    m_h = re.search(r'(\d+)\s*(?:Std|hr|h)', duration_str, re.IGNORECASE)
    if m_h: hours = int(m_h.group(1))
    m_m = re.search(r'(\d+)\s*(?:Min|m)', duration_str, re.IGNORECASE)
    if m_m: mins = int(m_m.group(1))
    return (hours * 3600) + (mins * 60)

# =====================================================
# AUDIBLE SEARCH & SCRAPING
# =====================================================

def find_missing_asin(title, author, abs_duration_sec, language):
    if not title: return None
    search_domains = ["www.audible.com", "www.audible.de"]
    german_codes = ['de', 'deu', 'ger', 'german', 'deutsch']
    if language and language.lower() in german_codes:
        search_domains = ["www.audible.de", "www.audible.com"]
    query_title = urllib.parse.quote_plus(title)
    query_author = urllib.parse.quote_plus(author) if author else ""
    for domain in search_domains:
        url = f"https://{domain}/search?title={query_title}&author_author={query_author}&ipRedirectOverride=true"
        try:
            r = requests.get(url, headers=HEADERS_SCRAPE, timeout=20)
            
            # Check for Rate Limit before parsing
            check_potential_rate_limit(r)
            
            if r.status_code != 200: continue
            soup = BeautifulSoup(r.text, 'lxml')
            
            # Additional Content Check if no items found
            items = soup.find_all('li', class_=re.compile(r'productListItem'))
            if not items:
                check_potential_rate_limit(r, soup)
                
            for item in items:
                asin_attr = item.get('data-asin')
                if not asin_attr: 
                    div = item.find('div', attrs={'data-asin': True})
                    if div: asin_attr = div.get('data-asin')
                if not asin_attr: continue
                title_tag = item.find('h3', class_=re.compile(r'bc-heading'))
                if not title_tag: continue
                found_title = title_tag.get_text(strip=True)
                if not fuzzy_match(title, found_title, 0.7): continue
                if abs_duration_sec and abs_duration_sec > 0:
                    runtime_tag = item.find('li', class_=re.compile(r'runtimeLabel'))
                    if runtime_tag:
                        runtime_text = runtime_tag.get_text(strip=True)
                        audible_sec = parse_audible_duration(runtime_text)
                        if audible_sec > 0:
                            if abs(abs_duration_sec - audible_sec) > 900: continue
                return asin_attr
        except RateLimitException:
            raise # Re-raise to be caught in main loop
        except Exception as e:
            logging.warning(f"Error searching ASIN on {domain}: {e}")
    return None

def get_audible_data(asin, language=None):
    if not asin: return None
    domains = [
        "www.audible.com", "www.audible.de", "www.audible.co.uk",
        "www.audible.fr", "www.audible.ca", "www.audible.com.au",
        "www.audible.it", "www.audible.es"
    ]
    
    # Priorisiere DE wenn Sprache gesetzt ist
    german_codes = ['de', 'deu', 'ger', 'german', 'deutsch']
    if language and language.lower() in german_codes:
        if "www.audible.de" in domains:
            domains.remove("www.audible.de")
            domains.insert(0, "www.audible.de")

    found_domains = [] 
    for domain in domains:
        url = f"https://{domain}/pd/{asin}?ipRedirectOverride=true"
        try:
            r = requests.get(url, headers=HEADERS_SCRAPE, timeout=20)
            
            # Check Status Code first
            check_potential_rate_limit(r)
            
            if r.status_code == 404: continue 
            if r.status_code == 200:
                if "/pderror" in r.url.lower(): 
                    continue
                
                soup = BeautifulSoup(r.text, 'lxml')
                
                # --- FLEXIBLE TITLE CHECK ---
                title_tag = soup.find(['h1', 'h2', 'h3'], class_=re.compile(r'bc-heading'))
                if not title_tag:
                    title_tag = soup.find(['h1', 'h2', 'h3'], attrs={'data-uia': 'product-title'})

                redirected_to_home = ("audible." in r.url and len(r.url) < 35)

                found_domains.append(domain)
                ratings = {}

                # 1. Summary Tag
                summary_tag = soup.find('adbl-rating-summary')
                if summary_tag:
                    if summary_tag.has_attr('performance-value'): ratings['performance'] = summary_tag['performance-value']
                    if summary_tag.has_attr('story-value'): ratings['story'] = summary_tag['story-value']
                    star_tag = summary_tag.find('adbl-star-rating')
                    if star_tag:
                         if star_tag.has_attr('value'): ratings['overall'] = star_tag['value']
                         if star_tag.has_attr('count'): ratings['count'] = star_tag['count']

                # 2. JSON-LD
                if not ratings.get('overall'):
                    scripts = soup.find_all('script', type='application/ld+json')
                    for script in scripts:
                        try:
                            data = json.loads(script.string)
                            if isinstance(data, list):
                                for item in data:
                                    if 'aggregateRating' in item: ratings['overall'] = item['aggregateRating'].get('ratingValue')
                            elif isinstance(data, dict) and 'aggregateRating' in data:
                                ratings['overall'] = data['aggregateRating'].get('ratingValue')
                        except: pass
                
                # 3. Next.js Data
                if not ratings.get('overall'):
                    next_data = soup.find('script', id='__NEXT_DATA__')
                    if next_data:
                        try:
                            jd = json.loads(next_data.string)
                            def find_rating_in_json(obj):
                                if isinstance(obj, dict):
                                    if 'rating' in obj and isinstance(obj['rating'], dict):
                                        r = obj['rating']
                                        if 'value' in r and 'count' in r: return r
                                    for k, v in obj.items():
                                        res = find_rating_in_json(v)
                                        if res: return res
                                elif isinstance(obj, list):
                                    for item in obj:
                                        res = find_rating_in_json(item)
                                        if res: return res
                                return None
                            found_r = find_rating_in_json(jd)
                            if found_r:
                                ratings['overall'] = found_r.get('value')
                                ratings['count'] = found_r.get('count')
                        except: pass

                # 4. Classic Metadata
                if not ratings.get('overall'):
                    meta_tag = soup.find('adbl-product-metadata')
                    if meta_tag:
                        script = meta_tag.find('script', type='application/json')
                        if script:
                            try:
                                d = json.loads(script.string)
                                if 'rating' in d:
                                    r_data = d['rating']
                                    if 'count' in r_data: ratings['count'] = r_data['count']
                                    if 'value' in r_data: ratings['overall'] = r_data['value']
                            except: pass

                count = ratings.get('count')
                
                if count and int(count) > 0:
                    logging.info(f"    -> Audible: ✅ Found on {domain} (Count: {count})")
                    return ratings
                
                if (title_tag or redirected_to_home) and not count:
                      # Suspicious: We found a page but no content. Check for Captcha.
                      check_potential_rate_limit(r, soup)
                      logging.info(f"    -> Audible: ⚠️ Page found on {domain}, but NO ratings (Count: 0)")
                      return {'count': 0, 'overall': None}

        except RateLimitException:
            raise # Bubble up to main loop
        except: pass
        
    if "www.audible.com" in found_domains:
        logging.info("    -> Audible: ⚠️ Page found on .com, but NO ratings (Count: 0)")
        return {'count': 0, 'overall': None}
    
    logging.info("    -> Audible: ❌ Not found (Page error or Redirect)")
    return None 

# =====================================================
# GOODREADS SEARCH & SCRAPING
# =====================================================

def scrape_goodreads_book_details(url):
    try:
        r = requests.get(url, headers=HEADERS_SCRAPE, timeout=20)
        
        # Check Status Code first
        check_potential_rate_limit(r)
        
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.text, 'lxml')
        result = {'url': url}
        
        scripts = soup.find_all('script', type='application/ld+json')
        for script in scripts:
            try:
                data = json.loads(script.string)
                if 'aggregateRating' in data: 
                    result['val'] = data['aggregateRating'].get('ratingValue')
                    if 'reviewCount' in data['aggregateRating']:
                        result['count'] = data['aggregateRating'].get('reviewCount')
                    elif 'ratingCount' in data['aggregateRating']:
                         result['count'] = data['aggregateRating'].get('ratingCount')
                    if 'isbn' in data: result['isbn'] = data['isbn']
            except: pass
            
        if 'val' not in result:
            rating_candidates = soup.find_all(string=re.compile(r"avg rating"))
            for text_node in rating_candidates:
                match = re.search(r'(\d+[.,]\d+)\s+avg rating', text_node)
                if match: 
                    result['val'] = match.group(1).replace(',', '.')
                    break
        if 'count' not in result:
            count_candidates = soup.find_all(string=re.compile(r"ratings"))
            for text_node in count_candidates:
                match = re.search(r'([\d,.]+)\s+ratings', text_node)
                if match:
                    clean_count = re.sub(r'[^\d]', '', match.group(1))
                    if clean_count: result['count'] = int(clean_count)
                    break
        
        if 'isbn' not in result:
            meta_isbn = soup.find('meta', property="books:isbn")
            if meta_isbn: result['isbn'] = meta_isbn.get('content')
            
        # [NEW] Fallback for embedded JSON/Apollo state (React hidden data)
        if 'isbn' not in result and 'asin' not in result:
             # Pattern 1: Exact JSON key-value pair "asin":"B0..."
             json_asin_match = re.search(r'"asin"\s*:\s*"([A-Z0-9]{10})"', r.text)
             if json_asin_match:
                 result['asin'] = json_asin_match.group(1)
             
             # Pattern 2: URL Parameters like creativeASIN=B0...
             if 'asin' not in result:
                 url_asin_match = re.search(r'(?:creativeASIN|asin)=([A-Z0-9]{10})', r.text)
                 if url_asin_match:
                     result['asin'] = url_asin_match.group(1)

             # JSON ISBN Fallback
             json_isbn_match = re.search(r'"isbn"\s*:\s*"([0-9]{10,13})"', r.text)
             if json_isbn_match:
                 result['isbn'] = json_isbn_match.group(1)

        # [OLD] Fallback for text content scan (Generic text scan)
        if 'isbn' not in result and 'asin' not in result:
            text_content = soup.get_text()
            asin_match = re.search(r'ASIN[:\s]*(B0\w+)', text_content)
            if asin_match:
                result['asin'] = asin_match.group(1)

        # If we still have no result, check if it was a Captcha page that returned 200 OK
        if 'val' not in result:
             check_potential_rate_limit(r, soup)

        return result if 'val' in result else None
    except RateLimitException:
        raise
    except: return None

def find_best_goodreads_match_in_list(html, target_title, target_abs_authors):
    soup = BeautifulSoup(html, 'lxml')
    if soup.find('div', id='metacol') or soup.find('h1', id='bookTitle'):
        return "DIRECT_HIT" 
    rows = soup.find_all('tr', itemtype="http://schema.org/Book")
    if not rows: return None
    best_match_url = None
    best_score = 0.0
    clean_target = clean_title_for_search(target_title)
    
    for row in rows:
        try:
            title_tag = row.find('a', class_='bookTitle')
            if not title_tag: continue
            found_title = title_tag.get_text(strip=True)
            clean_found = clean_title_for_search(found_title)
            url = title_tag['href']
            author_tag = row.find('a', class_='authorName')
            found_author = author_tag.get_text(strip=True) if author_tag else ""
            
            raw_t_score = difflib.SequenceMatcher(None, target_title.lower(), found_title.lower()).ratio()
            clean_t_score = difflib.SequenceMatcher(None, clean_target.lower(), clean_found.lower()).ratio()
            t_score = max(raw_t_score, clean_t_score)
            
            containment_bonus = 0.0
            if (len(clean_target) > 3 and clean_target.lower() in clean_found.lower()) or \
               (len(clean_found) > 3 and clean_found.lower() in clean_target.lower()):
                containment_bonus = 0.2
            
            total_score = t_score + containment_bonus
            
            if not check_numbers_match(target_title, found_title): 
                continue

            if not check_author_match_list(target_abs_authors, found_author):
                if total_score > 0.8:
                    logging.info(f"    (Debug) Match Rejected: Title '{found_title}' score {round(total_score,2)} ok, but GR Author '{found_author}' not in ABS list {target_abs_authors}")
                continue
            
            if total_score > 0.70 and total_score > best_score:
                best_score = total_score
                best_match_url = "https://www.goodreads.com" + url
        except: pass
    return best_match_url

def get_goodreads_data(isbn, asin, title, abs_authors_list, primary_author):
    if isbn:
        url = f"https://www.goodreads.com/search?q={isbn}"
        try:
            r = requests.get(url, headers=HEADERS_SCRAPE, timeout=20)
            check_potential_rate_limit(r)
            if r.status_code == 200:
                data = scrape_goodreads_book_details(r.url)
                if data:
                    data['source'] = 'ISBN Lookup'
                    return data
        except RateLimitException: raise
        except: pass

    if asin:
        url = f"https://www.goodreads.com/search?q={asin}"
        try:
            r = requests.get(url, headers=HEADERS_SCRAPE, timeout=20)
            check_potential_rate_limit(r)
            if r.status_code == 200:
                data = scrape_goodreads_book_details(r.url)
                if data:
                    data['source'] = 'ASIN Lookup'
                    return data
        except RateLimitException: raise
        except: pass
    
    search_titles = [title]
    clean = clean_title_for_search(title)
    if clean and clean != title: search_titles.append(clean)
    
    if primary_author:
        for search_t in search_titles:
            if not search_t: continue
            query = f"{search_t} {primary_author}"
            url = f"https://www.goodreads.com/search?q={urllib.parse.quote_plus(query)}"
            try:
                r = requests.get(url, headers=HEADERS_SCRAPE, timeout=20)
                check_potential_rate_limit(r)
                if r.status_code == 200:
                    if "/book/show/" in r.url:
                        data = scrape_goodreads_book_details(r.url)
                        if data:
                            data['source'] = 'Text Search (Direct Hit)'
                            return data
                    else:
                        match_url = find_best_goodreads_match_in_list(r.text, title, abs_authors_list)
                        if match_url == "DIRECT_HIT":
                             data = scrape_goodreads_book_details(r.url)
                             if data: 
                                 data['source'] = 'Text Search (Direct Hit)'
                                 return data
                        elif match_url:
                            data = scrape_goodreads_book_details(match_url)
                            if data:
                                data['source'] = 'Text Search (Title+Author List)'
                                return data
            except RateLimitException: raise
            except: pass

    logging.info("    -> Standard search failed. Trying Title-only fallback...")
    for search_t in search_titles:
        if not search_t: continue
        query = search_t
        url = f"https://www.goodreads.com/search?q={urllib.parse.quote_plus(query)}"
        try:
            r = requests.get(url, headers=HEADERS_SCRAPE, timeout=20)
            check_potential_rate_limit(r)
            if r.status_code == 200:
                if "/book/show/" in r.url:
                    data = scrape_goodreads_book_details(r.url)
                    if data:
                        data['source'] = 'Text Search (Title Only)'
                        return data
                else:
                    match_url = find_best_goodreads_match_in_list(r.text, title, abs_authors_list)
                    if match_url and match_url != "DIRECT_HIT":
                        data = scrape_goodreads_book_details(match_url)
                        if data:
                            data['source'] = 'Text Search (Title Only)'
                            return data
        except RateLimitException: raise
        except: pass

    return None

# =====================================================
# MAIN LOGIC
# =====================================================

def process_library(lib_id, history, failed_history):
    logging.info(f"--- Processing Library: {lib_id} ---")
    try:
        r = requests.get(f"{ABS_URL}/api/libraries/{lib_id}/items", headers=HEADERS_ABS)
        if r.status_code != 200:
            logging.error(f"Failed to fetch library items. Status: {r.status_code}")
            stats['failed'] += 1
            return
        items = r.json()['results']
    except Exception as e:
        logging.error(f"Connection error ABS: {e}")
        stats['failed'] += 1
        return

    queue_new = []
    queue_due = []
    
    for item in items:
        item_id = item['id']
        unique_key = f"{lib_id}_{item_id}"
        if unique_key not in history: queue_new.append(item)
        elif is_due_for_update(unique_key, history): queue_due.append(item)
        else: stats['skipped'] += 1

    random.shuffle(queue_new)
    random.shuffle(queue_due)
    work_queue = queue_new + queue_due
    
    logging.info(f"Queue: {len(queue_new)} New, {len(queue_due)} Due. (Total skipped: {stats['skipped']})")
    
    count_processed = 0
    total_in_batch = min(len(work_queue), MAX_BATCH_SIZE)
    consecutive_rate_limits = 0
    
    for count_processed, item in enumerate(work_queue[:total_in_batch]):
        if stats['aborted_ratelimit']: break
        
        try:
            item_id = item['id']
            unique_key = f"{lib_id}_{item['id']}"
            metadata = item.get('media', {})['metadata']
            title = metadata.get('title')
            
            try:
                item_res = requests.get(f"{ABS_URL}/api/items/{item_id}", headers=HEADERS_ABS)
                if item_res.status_code == 200:
                    item_data = item_res.json()
                    metadata = item_data['media']['metadata']
                    current_desc = metadata.get('description', '')
                    abs_duration = item_data['media'].get('duration')
                    title = metadata.get('title')
                    language = metadata.get('language')
                else:
                    current_desc = metadata.get('description', '')
                    abs_duration = item.get('media', {}).get('duration')
                    language = metadata.get('language')
            except Exception as e:
                stats['failed'] += 1
                continue

            old_audible = None
            old_gr = None
            m_aud = re.search(r'(?s)(Audible.*?)<br>\s*(?=Goodreads|⭐)', current_desc)
            if m_aud: old_audible = m_aud.group(1).strip()
            m_gr = re.search(r'(?s)(Goodreads.*?)<br>\s*(?=⭐)', current_desc)
            if m_gr: old_gr = m_gr.group(1).strip()

            asin = metadata.get('asin')
            isbn = metadata.get('isbn')
            
            author_data_raw = metadata.get('authors', [])
            abs_authors_list = []
            primary_search_author = ""
            
            if isinstance(author_data_raw, list):
                for a in author_data_raw:
                    if isinstance(a, dict):
                        name = a.get('name')
                        role = a.get('role', '').lower()
                        if name:
                            abs_authors_list.append(name)
                            if "auth" in role or "writ" in role:
                                primary_search_author = name
                    elif isinstance(a, str):
                        abs_authors_list.append(a)
            
            if not primary_search_author and abs_authors_list:
                primary_search_author = abs_authors_list[0]

            if not title: continue 
            
            # --- Visual Separator for Logging ---
            logging.info("-" * 50)
            
            logging.info(f"({count_processed + 1}/{total_in_batch}) Processing: {title} (ASIN: {asin if asin else 'None'})")
            stats['processed'] += 1
            
            audible_data = None
            if asin: audible_data = get_audible_data(asin, language=language)
            
            should_search_asin = (not asin) or (asin and audible_data is None)
            
            if should_search_asin and not DRY_RUN:
                if not asin: logging.info("    -> No ASIN present. Searching...")
                else: logging.info("    -> ASIN seems invalid. Searching replacement...")
                
                found_asin = find_missing_asin(title, primary_search_author, abs_duration, language)
                
                if found_asin:
                    if found_asin == asin:
                        logging.info(f"    -> Found same ASIN {found_asin}. Stopping search to avoid loop.")
                    else:
                        logging.info(f"    -> ✨ Found NEW ASIN: {found_asin}")
                        try:
                            patch_asin_url = f"{ABS_URL}/api/items/{item_id}/media"
                            r_asin = requests.patch(patch_asin_url, json={"metadata": {"asin": found_asin}}, headers=HEADERS_ABS)
                            if r_asin.status_code == 200:
                                asin = found_asin
                                stats['asin_found'] += 1
                                audible_data = get_audible_data(asin, language=language)
                        except: pass
            
            found_audible = bool(audible_data) or bool(old_audible)
            if not found_audible:
                # Audible logging handled in function
                pass
            
            time.sleep(1)
            gr = get_goodreads_data(isbn, asin, title, abs_authors_list, primary_search_author)
            
            # Reset consecutive errors if we made it here without exception
            consecutive_rate_limits = 0
            
            if gr:
                source = gr.get('source', 'Unknown')
                val = gr.get('val', 'N/A')
                logging.info(f"    -> Goodreads: ✅ Found via {source} (Rating: {val})")
            else:
                logging.info(f"    -> Goodreads: ❌ Not found (Tried: ISBN, ASIN, Text w/ Author validation)")

            # --- REVISED ISBN/ASIN FALLBACK LOGIC ---
            if gr and not DRY_RUN:
                new_id = gr.get('isbn')
                used_fallback = False

                # Fallback to ASIN if ISBN is missing
                if not new_id:
                    new_id = gr.get('asin')
                    if new_id:
                        used_fallback = True
                
                # Check how we found the book
                source = gr.get('source', '')
                is_lookup_by_id = source in ['ISBN Lookup', 'ASIN Lookup']

                # Case 1: Goodreads returned NEITHER ISBN NOR ASIN
                if not new_id:
                      # Only warn if we scraped via Text Search. 
                      # If we found it via ID lookup, the existing ID is obviously fine.
                      if not is_lookup_by_id:
                          logging.info(f"    -> ISBN: ⚠️ Goodreads data returned no ISBN or ASIN.")

                # Case 2: We have an ID (either ISBN or ASIN-fallback)
                else:
                    # Clean comparison to avoid re-patching identical IDs (e.g. 978- vs 978)
                    clean_abs_isbn = str(isbn).replace('-','').strip() if isbn else ""
                    clean_new_id = str(new_id).replace('-','').strip()

                    if not isbn:
                        type_label = "GR-ASIN" if used_fallback else "ISBN"
                        logging.info(f"    -> ISBN: ✨ Missing locally. Adding ({type_label}): {new_id}")
                        try:
                            patch_isbn_url = f"{ABS_URL}/api/items/{item_id}/media"
                            requests.patch(patch_isbn_url, json={"metadata": {"isbn": new_id}}, headers=HEADERS_ABS)
                            stats['isbn_added'] += 1
                        except: pass

                    elif clean_abs_isbn == clean_new_id:
                         # It matches, just verify in logs
                         type_label = "GR-ASIN" if used_fallback else "ISBN"
                         logging.info(f"    -> ISBN: ✅ Verified Match ({type_label}: {new_id})")

                    else:
                        # Update existing
                        type_label = "GR-ASIN" if used_fallback else "ISBN"
                        logging.info(f"    -> ISBN: 🔧 Updating ({type_label} Fallback) Old: {isbn} -> New: {new_id}" if used_fallback else f"    -> ISBN: 🔧 Updating (Old: {isbn} -> New: {new_id})")
                        try:
                            patch_isbn_url = f"{ABS_URL}/api/items/{item_id}/media"
                            requests.patch(patch_isbn_url, json={"metadata": {"isbn": new_id}}, headers=HEADERS_ABS)
                            stats['isbn_repaired'] += 1
                        except: pass

            found_gr = bool(gr) or bool(old_gr)
            
            has_asin = bool(asin)
            is_complete = False
            if has_asin:
                if found_audible and found_gr: is_complete = True
            else:
                if found_gr: is_complete = True
            
            should_save_history = False
            
            if is_complete:
                should_save_history = True
            else:
                fails = failed_history.get(unique_key, 0) + 1
                failed_history[unique_key] = fails
                
                if not found_audible and not found_gr:
                    stats['no_data'] += 1
                    logging.warning(f"    -> ❌ No data found (Attempt {fails}/{MAX_FAIL_ATTEMPTS}).")
                else:
                    stats['partial'] += 1
                    logging.info(f"    -> ⚠️ Incomplete data (Audible: {found_audible}, GR: {found_gr}). Attempt {fails}/{MAX_FAIL_ATTEMPTS}.")

                if fails >= MAX_FAIL_ATTEMPTS:
                    should_save_history = True
                    stats['cooldown'] += 1
                    logging.info("    -> 🛑 Max attempts reached. Cooldown started.")
                    del failed_history[unique_key]
                else:
                    save_json(FAILED_FILE, failed_history)
            
            if should_save_history:
                history[unique_key] = datetime.now().strftime("%Y-%m-%d")
                save_json(HISTORY_FILE, history)
                if unique_key in failed_history:
                    del failed_history[unique_key]
                    save_json(FAILED_FILE, failed_history)
            
            # --- LIVE REPORT SAVING ---
            update_report("audible", unique_key, title, primary_search_author, asin, "Not found", found_audible)
            update_report("goodreads", unique_key, title, primary_search_author, isbn, "Not found", found_gr)
            if not DRY_RUN:
                save_reports()
            
            BR = "<br>" 
            block = f"⭐ Ratings & Infos{BR}"
            
            if audible_data:
                cnt = audible_data.get('count')
                header_text = f"Audible ({cnt}):" if cnt else "Audible:"
                block += f"{header_text}{BR}"
                ov = audible_data.get('overall')
                pf = audible_data.get('performance')
                st = audible_data.get('story')
                if ov: block += f"🏆 {generate_moon_rating(ov)} {round(safe_float(ov), 1)} / 5 - Overall{BR}"
                if pf: block += f"🎙️ {generate_moon_rating(pf)} {round(safe_float(pf), 1)} / 5 - Performance{BR}"
                if st: block += f"📖 {generate_moon_rating(st)} {round(safe_float(st), 1)} / 5 - Story{BR}"
            elif old_audible:
                logging.info("    -> ♻️ Recycling old Audible rating.")
                stats['recycled'] += 1
                block += f"{old_audible}{BR}"

            if gr:
                cnt = gr.get('count')
                header_text = f"Goodreads ({cnt}):" if cnt else "Goodreads:"
                block += f"{header_text}{BR}"
                val = gr.get('val')
                if val: block += f"🏆 {generate_moon_rating(val)} {round(safe_float(val), 1)} / 5 - Rating{BR}"
            elif old_gr:
                logging.info("    -> ♻️ Recycling old Goodreads rating.")
                if not old_audible: stats['recycled'] += 1
                block += f"{old_gr}{BR}"
            
            block += f"⭐{BR}" 
            
            clean_desc = remove_old_rating_block(current_desc)
            final_desc = block + clean_desc

            if not DRY_RUN:
                try:
                    patch_url = f"{ABS_URL}/api/items/{item_id}/media"
                    res = requests.patch(patch_url, json={"metadata": {"description": final_desc}}, headers=HEADERS_ABS)
                    if res.status_code == 200:
                        updates = []
                        
                        if (audible_data and int(audible_data.get('count', 0)) > 0) or old_audible:
                            updates.append("Audible")
                        
                        if gr or old_gr:
                            updates.append("Goodreads")
                        
                        update_str = " & ".join(updates) if updates else "Description Cleaned"
                        
                        logging.info(f"    -> ✅ UPDATE SUCCESS (Content: {update_str})")
                        if is_complete: stats['success'] += 1
                    else:
                        logging.error(f"    -> ❌ API ERROR: {res.status_code}")
                        stats['failed'] += 1
                except Exception as e:
                    logging.error(f"    -> ❌ API Exception: {e}")
                    stats['failed'] += 1
            else:
                logging.info(f"    -> [DRY RUN] Would save (Complete: {is_complete}).")
                if is_complete: stats['success'] += 1
        
        except RateLimitException as rle:
            consecutive_rate_limits += 1
            logging.warning(f"🛑 Rate Limit DETECTED: {rle}")
            
            if rle.is_hard_limit:
                 logging.error(f"🛑 HARD STOP initiated due to HTTP 429. Aborting script.")
                 stats['aborted_ratelimit'] = True
                 break
            elif consecutive_rate_limits >= MAX_CONSECUTIVE_RATE_LIMITS:
                 logging.error(f"🛑 Aborting script: {consecutive_rate_limits} consecutive rate limit errors.")
                 stats['aborted_ratelimit'] = True
                 break
            else:
                 logging.info(f"    -> Pausing for {RECOVERY_PAUSE}s before next item (Attempt {consecutive_rate_limits}/{MAX_CONSECUTIVE_RATE_LIMITS})...")
                 time.sleep(RECOVERY_PAUSE)
                 # No strike added, loop continues to next item
        
        except Exception as e:
            logging.error(f"CRASH on item {item.get('id')}: {e}")
            stats['failed'] += 1
            continue
        
        time.sleep(BASE_SLEEP + random.uniform(1, 3))
    
    if stats['aborted_ratelimit']:
        logging.info("-" * 50)
        logging.info(f"🛑 ABBRUCH wg. Rate Limit! (Erfolgreich verarbeitet: {stats['success']} / {total_in_batch} geplant)")
        logging.info("-" * 50)

def main():
    if not ABS_URL or not API_TOKEN:
        print("Error: Missing ABS_URL or API_TOKEN env vars.")
        return

    log_file = setup_logging()
    load_reports() 
    
    start_time = datetime.now()
    logging.info("--- Starting ABS Ratings Update ---")
    
    history = load_json(HISTORY_FILE)
    failed_history = load_json(FAILED_FILE)
    
    for lib_id in LIBRARY_IDS:
        process_library(lib_id, history, failed_history)
        if stats['aborted_ratelimit']: break
        
    save_reports() 
    
    logging.info(f"--- Finished ---")
    logging.info(f"Stats: {stats}")
    
    write_env_file(log_file, start_time)

if __name__ == "__main__":
    main()
