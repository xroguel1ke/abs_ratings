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
MAX_FAIL_ATTEMPTS = 5 # Back to 5 Strikes!
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
    "asin_found": 0, "isbn_added": 0, "isbn_repaired": 0
}

# In-Memory Report Storage
report_audible = {}
report_goodreads = {}

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

    if stats['failed'] > 0:
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
    if common: return True
    return False

def check_author_match(abs_author_str, gr_author_str):
    if not abs_author_str or not gr_author_str: return False
    
    # 1. Fuzzy Fallback (e.g. for Typos)
    if difflib.SequenceMatcher(None, abs_author_str.lower(), gr_author_str.lower()).ratio() > 0.6:
        return True

    # 2. Smart Token Logic
    def tokenize(s):
        s = re.sub(r'[\(\[].*?[\)\]]', '', s)
        tokens = re.split(r'[^a-zA-Z0-9]+', s.lower())
        return set([t for t in tokens if len(t) > 1])

    abs_tokens = tokenize(abs_author_str)
    gr_tokens = tokenize(gr_author_str)
    
    common = abs_tokens.intersection(gr_tokens)
    
    if len(common) >= 2: return True
    if len(common) >= 1 and len(abs_tokens) == 1: return True
        
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
            if r.status_code != 200: continue
            soup = BeautifulSoup(r.text, 'lxml')
            items = soup.find_all('li', class_=re.compile(r'productListItem'))
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
        except Exception as e:
            logging.warning(f"Error searching ASIN on {domain}: {e}")
    return None

def get_audible_data(asin):
    if not asin: return None
    domains = [
        "www.audible.com", "www.audible.de", "www.audible.co.uk",
        "www.audible.fr", "www.audible.ca", "www.audible.com.au",
        "www.audible.it", "www.audible.es"
    ]
    found_domains = [] 
    for domain in domains:
        url = f"https://{domain}/pd/{asin}?ipRedirectOverride=true"
        try:
            r = requests.get(url, headers=HEADERS_SCRAPE, timeout=20)
            if r.status_code == 404: continue 
            if r.status_code == 200:
                if "/pderror" in r.url.lower(): continue
                soup = BeautifulSoup(r.text, 'lxml')
                title_tag = soup.find('h1', class_=re.compile(r'bc-heading'))
                if not title_tag: continue
                found_domains.append(domain)
                ratings = {}
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
                summary_tag = soup.find('adbl-rating-summary')
                if summary_tag:
                    if summary_tag.has_attr('performance-value'): ratings['performance'] = summary_tag['performance-value']
                    if summary_tag.has_attr('story-value'): ratings['story'] = summary_tag['story-value']
                count = ratings.get('count')
                if count and int(count) > 0:
                    logging.info(f"  -> Found ratings on {domain} (Count: {count})")
                    return ratings
        except: pass
    if "www.audible.com" in found_domains:
        return {'count': 0, 'overall': None}
    if found_domains:
        return None 
    return None 

# =====================================================
# GOODREADS SEARCH & SCRAPING
# =====================================================

def scrape_goodreads_book_details(url):
    try:
        r = requests.get(url, headers=HEADERS_SCRAPE, timeout=20)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.text, 'lxml')
        result = {'url': url}
        
        # JSON-LD Parsing
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
            
        # Fallback Text Parsing
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
        
        # ISBN & ASIN Extraction
        if 'isbn' not in result:
            meta_isbn = soup.find('meta', property="books:isbn")
            if meta_isbn: result['isbn'] = meta_isbn.get('content')
        
        # Try to find ASIN (Commonly labeled as ASIN in text)
        if 'isbn' not in result:
            text_content = soup.get_text()
            asin_match = re.search(r'ASIN[:\s]+(B0\w+)', text_content)
            if asin_match:
                result['asin'] = asin_match.group(1)

        return result if 'val' in result else None
    except: return None

def find_best_goodreads_match_in_list(html, target_title, target_author):
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
            
            # Author Check
            if not check_author_match(target_author, found_author): continue
            
            # Number Check
            if not check_numbers_match(target_title, found_title): continue
            
            # Score Calculation
            raw_t_score = difflib.SequenceMatcher(None, target_title.lower(), found_title.lower()).ratio()
            clean_t_score = difflib.SequenceMatcher(None, clean_target.lower(), clean_found.lower()).ratio()
            t_score = max(raw_t_score, clean_t_score)
            
            # Boost if one contains the other (e.g. "Dune" in "Dune: Special Edition")
            containment_bonus = 0.0
            if (len(clean_target) > 3 and clean_target.lower() in clean_found.lower()) or \
               (len(clean_found) > 3 and clean_found.lower() in clean_target.lower()):
                containment_bonus = 0.2
            
            total_score = t_score + containment_bonus
            
            # Lower threshold to 0.70 to accept more "loose" title matches
            if total_score > 0.70 and total_score > best_score:
                best_score = total_score
                best_match_url = "https://www.goodreads.com" + url
        except: pass
    return best_match_url

def get_goodreads_data(isbn, asin, title, author):
    # 1. Search by ISBN
    if isbn:
        url = f"https://www.goodreads.com/search?q={isbn}"
        try:
            r = requests.get(url, headers=HEADERS_SCRAPE, timeout=20)
            if r.status_code == 200:
                data = scrape_goodreads_book_details(r.url)
                if data:
                    data['source'] = 'isbn_lookup'
                    return data
        except: pass

    # 2. Search by ASIN
    if asin:
        url = f"https://www.goodreads.com/search?q={asin}"
        try:
            r = requests.get(url, headers=HEADERS_SCRAPE, timeout=20)
            if r.status_code == 200:
                data = scrape_goodreads_book_details(r.url)
                if data:
                    data['source'] = 'asin_lookup'
                    return data
        except: pass
    
    # 3. Text Search (Fallback)
    search_titles = [title]
    clean = clean_title_for_search(title)
    if clean and clean != title: search_titles.append(clean)
    primary_author = author.split(',')[0].split('&')[0].strip()
    
    # 3a. Search with "Title + Author"
    for search_t in search_titles:
        if not search_t: continue
        query = f"{search_t} {primary_author}"
        url = f"https://www.goodreads.com/search?q={urllib.parse.quote_plus(query)}"
        try:
            r = requests.get(url, headers=HEADERS_SCRAPE, timeout=20)
            if r.status_code == 200:
                if "/book/show/" in r.url:
                    data = scrape_goodreads_book_details(r.url)
                    if data:
                        data['source'] = 'text_lookup'
                        return data
                else:
                    match_url = find_best_goodreads_match_in_list(r.text, title, author)
                    if match_url == "DIRECT_HIT":
                         data = scrape_goodreads_book_details(r.url)
                         if data: 
                             data['source'] = 'text_lookup'
                             return data
                    elif match_url:
                        logging.info(f"  -> Goodreads found (Title+Author): {match_url}")
                        data = scrape_goodreads_book_details(match_url)
                        if data:
                            data['source'] = 'text_lookup'
                            return data
        except: pass

    # 3b. Fallback: Search with "Title ONLY"
    logging.info("  -> Standard search failed. Trying Title-only fallback...")
    for search_t in search_titles:
        if not search_t: continue
        query = search_t
        url = f"https://www.goodreads.com/search?q={urllib.parse.quote_plus(query)}"
        try:
            r = requests.get(url, headers=HEADERS_SCRAPE, timeout=20)
            if r.status_code == 200:
                if "/book/show/" in r.url:
                    data = scrape_goodreads_book_details(r.url)
                    if data:
                        data['source'] = 'text_lookup'
                        return data
                else:
                    match_url = find_best_goodreads_match_in_list(r.text, title, author)
                    if match_url and match_url != "DIRECT_HIT":
                        logging.info(f"  -> Goodreads found (Title-Only): {match_url}")
                        data = scrape_goodreads_book_details(match_url)
                        if data:
                            data['source'] = 'text_lookup'
                            return data
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
    
    for item in work_queue:
        if count_processed >= MAX_BATCH_SIZE:
            logging.info(f"Batch limit of {MAX_BATCH_SIZE} reached.")
            break
        
        # === ITEM PROCESSING WRAPPER ===
        try:
            item_id = item['id']
            unique_key = f"{lib_id}_{item_id}"
            metadata = item.get('media', {})['metadata']
            title = metadata.get('title')
            language = metadata.get('language')
            
            try:
                item_res = requests.get(f"{ABS_URL}/api/items/{item_id}", headers=HEADERS_ABS)
                if item_res.status_code == 200:
                    item_data = item_res.json()
                    current_desc = item_data['media']['metadata'].get('description', '')
                    abs_duration = item_data['media'].get('duration')
                else:
                    current_desc = metadata.get('description', '')
                    abs_duration = item.get('media', {}).get('duration')
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
            author_data = metadata.get('authors', [])
            author = ""
            if isinstance(author_data, list) and len(author_data) > 0:
                first = author_data[0]
                if isinstance(first, str): author = first
                elif isinstance(first, dict) and 'name' in first: author = first['name']

            if not title: continue 
            
            logging.info(f"Processing: {title} (ASIN: {asin if asin else 'None'})")
            stats['processed'] += 1
            
            # --- AUDIBLE ---
            audible_data = None
            if asin: audible_data = get_audible_data(asin)
            
            should_search_asin = (not asin) or (asin and audible_data is None)
            
            if should_search_asin and not DRY_RUN:
                if not asin: logging.info("  -> No ASIN present. Searching...")
                else: logging.info("  -> ASIN seems invalid. Searching replacement...")
                found_asin = find_missing_asin(title, author, abs_duration, language)
                if found_asin:
                    logging.info(f"  -> ✨ Found ASIN: {found_asin}")
                    try:
                        patch_asin_url = f"{ABS_URL}/api/items/{item_id}/media"
                        r_asin = requests.patch(patch_asin_url, json={"metadata": {"asin": found_asin}}, headers=HEADERS_ABS)
                        if r_asin.status_code == 200:
                            asin = found_asin
                            stats['asin_found'] += 1
                            audible_data = get_audible_data(asin)
                    except: pass
            
            # Update Audible Report
            found_audible = bool(audible_data) or bool(old_audible)
            if not found_audible:
                update_report("audible", unique_key, title, author, asin, "No Ratings found & Search failed", False)
            else:
                update_report("audible", unique_key, title, author, asin, "Success", True)

            # --- GOODREADS ---
            time.sleep(1)
            gr = get_goodreads_data(isbn, asin, title, author)
            
            # UPDATE LOGIC: Patch ISBN/ASIN if missing OR if old ISBN failed
            if gr and not DRY_RUN:
                new_id = gr.get('isbn')
                if not new_id: new_id = gr.get('asin')
                
                if new_id:
                    should_patch_isbn = False
                    if not isbn:
                        should_patch_isbn = True
                        logging.info(f"  -> ✨ ISBN/ID missing. Adding: {new_id}")
                        stats['isbn_added'] += 1
                    elif gr.get('source') != 'isbn_lookup':
                        should_patch_isbn = True
                        logging.info(f"  -> 🔧 Existing ISBN failed. Replacing with: {new_id}")
                        stats['isbn_repaired'] += 1
                    
                    if should_patch_isbn:
                        try:
                            patch_isbn_url = f"{ABS_URL}/api/items/{item_id}/media"
                            requests.patch(patch_isbn_url, json={"metadata": {"isbn": new_id}}, headers=HEADERS_ABS)
                        except: pass

            found_gr = bool(gr) or bool(old_gr)
            # Update Goodreads Report
            if not found_gr:
                search_id = isbn if isbn else "No ISBN"
                update_report("goodreads", unique_key, title, author, search_id, "No Match found (ID/Title/Fallback)", False)
            else:
                update_report("goodreads", unique_key, title, author, isbn, "Success", True)
            
            # --- HISTORY & STATUS ---
            has_asin = bool(asin)
            is_complete = False
            if has_asin:
                if found_audible and found_gr: is_complete = True
            else:
                if found_gr: is_complete = True
            
            # Revert to old logic: Only save history if COMPLETE or MAX FAILS reached
            should_save_history = False
            
            if is_complete:
                should_save_history = True
            else:
                # If incomplete, we do NOT save history unless fails exceed max
                fails = failed_history.get(unique_key, 0) + 1
                failed_history[unique_key] = fails
                
                if not found_audible and not found_gr:
                    stats['no_data'] += 1
                    logging.warning(f"  -> ❌ No data found (Attempt {fails}/{MAX_FAIL_ATTEMPTS}).")
                else:
                    stats['partial'] += 1
                    logging.info(f"  -> ⚠️ Incomplete data (Audible: {found_audible}, GR: {found_gr}). Attempt {fails}/{MAX_FAIL_ATTEMPTS}.")

                if fails >= MAX_FAIL_ATTEMPTS:
                    should_save_history = True
                    stats['cooldown'] += 1
                    logging.info("  -> 🛑 Max attempts reached. Cooldown started.")
                    del failed_history[unique_key]
                else:
                    save_json(FAILED_FILE, failed_history)
            
            if should_save_history:
                history[unique_key] = datetime.now().strftime("%Y-%m-%d")
                save_json(HISTORY_FILE, history)
                if unique_key in failed_history:
                    del failed_history[unique_key]
                    save_json(FAILED_FILE, failed_history)
            
            # --- BUILD DESC ---
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
                logging.info("  -> ♻️ Recycling old Audible rating.")
                stats['recycled'] += 1
                block += f"{old_audible}{BR}"

            if gr:
                cnt = gr.get('count')
                header_text = f"Goodreads ({cnt}):" if cnt else "Goodreads:"
                block += f"{header_text}{BR}"
                val = gr.get('val')
                if val: block += f"🏆 {generate_moon_rating(val)} {round(safe_float(val), 1)} / 5 - Rating{BR}"
            elif old_gr:
                logging.info("  -> ♻️ Recycling old Goodreads rating.")
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
                        logging.info(f"  -> ✅ UPDATE OK.")
                        if is_complete: stats['success'] += 1
                    else:
                        logging.error(f"  -> ❌ API ERROR: {res.status_code}")
                        stats['failed'] += 1
                except Exception as e:
                    logging.error(f"  -> ❌ API Exception: {e}")
                    stats['failed'] += 1
            else:
                logging.info(f"  -> [DRY RUN] Would save (Complete: {is_complete}).")
                if is_complete: stats['success'] += 1
        
        except Exception as e:
            logging.error(f"CRASH on item {item.get('id')}: {e}")
            stats['failed'] += 1
            continue
        
        count_processed += 1
        time.sleep(BASE_SLEEP + random.uniform(1, 3))

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
        
    save_reports() 
    
    logging.info(f"--- Finished ---")
    logging.info(f"Stats: {stats}")
    
    write_env_file(log_file, start_time)

if __name__ == "__main__":
    main()
