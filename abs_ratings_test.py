import requests
from bs4 import BeautifulSoup
import time, re, json, os, random, urllib.parse, difflib, logging
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
DRY_RUN = os.getenv('DRY_RUN', 'False').lower() == 'true'
BASE_SLEEP = int(os.getenv('SLEEP_TIMER', 6))

# --- OPTIMIZATION: User Agents for Stealth ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/122.0.0.0"
]

# --- OPTIMIZATION: Persistent Session for ABS (Local Speed) ---
HEADERS_ABS = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
abs_session = requests.Session()
abs_session.headers.update(HEADERS_ABS)

HEADERS_SCRAPE_BASE = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,de;q=0.8"
}

# Pre-compiled Regex for Performance
RE_ASIN = re.compile(r'ASIN[:\s]*(B0\w+)')
RE_ISBN_JSON = re.compile(r'"isbn"\s*:\s*"([0-9]{10,13})"')
RE_ASIN_JSON = re.compile(r'"asin"\s*:\s*"([A-Z0-9]{10})"')
RE_URL_ASIN = re.compile(r'(?:creativeASIN|asin)=([A-Z0-9]{10})')
RE_AUDIBLE_BLOCK = re.compile(r'(?s)(Audible.*?)<br>\s*(?=Goodreads|⭐)')
RE_GR_BLOCK = re.compile(r'(?s)(Goodreads.*?)<br>\s*(?=⭐)')
RE_RATING_BLOCK = re.compile(r'(?s)⭐\s*Ratings.*?⭐(?:\s|<br\s*/?>)*')
RE_CLEAN_TITLE = re.compile(r'(?i)\b(unabridged|abridged|audiobook|graphic audio|dramatized adaptation)\b|[\(\[].*?[\)\]]')
RE_VOL = re.compile(r'(?i)(?:\b(?:book|vol\.?|volume|part|no\.?)|#)\s*(\d+)')

stats = {k: 0 for k in ["processed", "success", "failed", "no_data", "skipped", "partial", "cooldown", "recycled", "asin_found", "isbn_added", "isbn_repaired"]}
stats['aborted_ratelimit'] = False
reports = {"audible": {}, "goodreads": {}}

class RateLimitException(Exception):
    def __init__(self, msg, is_hard=False): super().__init__(msg); self.is_hard = is_hard

# ================= UTILS & IO =================

def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    f = os.path.join(LOG_DIR, f"run_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(f, encoding='utf-8'), logging.StreamHandler()])
    return f

def rw_json(path, data=None):
    """Centralized Read/Write JSON"""
    try:
        if data is None: # Read
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f: return json.load(f)
            return {}
        else: # Write
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f: json.dump(data, f, indent=4, ensure_ascii=False)
    except: return {} if data is None else None

def update_report(src, key, title, author, ident, reason, success):
    if success: reports[src].pop(key, None)
    else: reports[src][key] = {"key": key, "title": title, "author": author, "identifier": ident, "reason": reason, "last_check": datetime.now().strftime("%Y-%m-%d")}

def save_reports():
    for k, v in reports.items(): rw_json(os.path.join(REPORT_DIR, f"missing_{k}.json"), sorted(v.values(), key=lambda x: x['title']))

def write_env_file(log_file, start_time):
    dur = f"{int((datetime.now() - start_time).total_seconds() // 60)}m {int((datetime.now() - start_time).total_seconds() % 60)}s"
    if stats['aborted_ratelimit']: sub, icon, head = "ABS Ratings: Abbruch 🛑", "alert", "Rate Limit erkannt!"
    elif stats['failed'] > 0: sub, icon, head = "ABS Ratings: Fehler ❌", "alert", "Fehler aufgetreten!"
    elif any(stats[k] > 0 for k in ['success', 'recycled', 'asin_found']): sub, icon, head = "ABS Ratings: Erfolg ✅", "normal", "Update abgeschlossen"
    else: sub, icon, head = "ABS Ratings: Info ℹ️", "normal", "Keine Änderungen"
    
    body = f"Proc: {stats['processed']} | New: {stats['success']} | ASIN+: {stats['asin_found']} | ISBN+: {stats['isbn_added']} | Fix: {stats['isbn_repaired']} | Err: {stats['failed']}"
    if stats['aborted_ratelimit']: body += " | ⚠️ ABORTED (Rate Limit)"
    
    try:
        with open(ENV_OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write(f"ABS_SUBJECT='{sub}'\nABS_ICON='{icon}'\nABS_HEADER='{head}'\nABS_DURATION='{dur}'\nABS_REPORT_BODY='{body}'\nABS_LOG_FILE='{os.path.basename(log_file)}'\n")
    except: pass

def fetch_url(url, params=None):
    """Centralized HTTP Request with Rate Limit Handling & Rotating Agents"""
    try:
        # Rotate User Agent for every request to avoid blocking
        headers = HEADERS_SCRAPE_BASE.copy()
        headers["User-Agent"] = random.choice(USER_AGENTS)
        
        r = requests.get(url, headers=headers, params=params, timeout=20)
        
        if r.status_code == 429: raise RateLimitException("HTTP 429 (Too Many Requests)", True)
        if r.status_code in [503, 403]: raise RateLimitException(f"HTTP {r.status_code}")
        
        soup = BeautifulSoup(r.text, 'lxml')
        txt = soup.get_text().lower()
        title = (soup.title.string or "").lower()
        
        if "captcha" in title or "robot check" in title or any(x in txt and len(txt) < 5000 for x in ["enter the characters", "robot check"]):
            raise RateLimitException("Captcha detected")
        return r, soup
    except RateLimitException: raise
    except Exception as e: return None, None

# ================= LOGIC HELPER =================

def safe_float(v): return float(str(v).replace(',', '.')) if v else 0.0
def clean_title(t): return RE_CLEAN_TITLE.sub('', t).split(':')[0].split(' - ')[0].strip() if t else ""
def moon_rating(v):
    v = safe_float(v)
    if v == 0: return "🌑" * 5
    full, decimal = int(v), v - int(v)
    half = 1 if 0.25 <= decimal < 0.75 else 0
    full = full + 1 if decimal >= 0.75 else full
    return ("🌕" * min(full, 5) + "🌗" * half).ljust(5, "🌑")[:5]

def extract_volume(text): return set(RE_VOL.findall(text)) | ({m.group(1)} if (m := re.search(r'\b(\d+)$', text.strip())) else set())

def match_author(abs_authors, gr_author):
    if not abs_authors or not gr_author: return False
    gr_tok = set(re.split(r'[^a-zA-Z0-9]+', gr_author.lower()))
    for a in abs_authors:
        if difflib.SequenceMatcher(None, a.lower(), gr_author.lower()).ratio() > 0.6: return True
        common = set(re.split(r'[^a-zA-Z0-9]+', a.lower())).intersection(gr_tok)
        if len(common) >= 2 or (len(common) >= 1 and len(re.split(r'[^a-zA-Z0-9]+', a.lower())) == 1): return True
    return False

def find_rating_recursive(obj):
    """Helper for Next.js recursion"""
    if isinstance(obj, dict):
        if 'rating' in obj and isinstance(obj['rating'], dict) and 'value' in obj['rating']: return obj['rating']
        for k, v in obj.items():
            if res := find_rating_recursive(v): return res
    elif isinstance(obj, list):
        for item in obj:
            if res := find_rating_recursive(item): return res
    return None

# ================= AUDIBLE LOGIC =================

def get_audible_data(asin, language):
    if not asin: return None
    domains = [
        "www.audible.com", "www.audible.de", "www.audible.co.uk", "www.audible.fr",
        "www.audible.ca", "www.audible.com.au", "www.audible.it", "www.audible.es"
    ]
    if language and language.lower() in ['de', 'deu', 'ger', 'german', 'deutsch']:
        if "www.audible.de" in domains:
            domains.remove("www.audible.de"); domains.insert(0, "www.audible.de")

    found_domains = []
    for domain in domains:
        r, soup = fetch_url(f"https://{domain}/pd/{asin}?ipRedirectOverride=true")
        if not r or r.status_code == 404 or "/pderror" in r.url: continue
        
        if "audible." in r.url and len(r.url) < 35: 
             logging.info(f"    -> Audible: ⚠️ Page found on {domain}, but redirected to Home.")
             continue

        found_domains.append(domain)
        ratings = {}
        
        # 1. Summary Tag
        if sum_tag := soup.find('adbl-rating-summary'):
            ratings = {k: sum_tag.get(f'{k}-value') for k in ['performance', 'story']}
            if st := sum_tag.find('adbl-star-rating'):
                ratings.update({'overall': st.get('value'), 'count': st.get('count')})
        
        # 2. JSON-LD
        if not ratings.get('overall'):
            for s in soup.find_all('script', type='application/ld+json'):
                try:
                    d = json.loads(s.string)
                    for i in (d if isinstance(d, list) else [d]):
                        if 'aggregateRating' in i: ratings['overall'] = i['aggregateRating'].get('ratingValue')
                except: pass
        
        # 3. Next.js Data
        if not ratings.get('overall'):
            if nxt := soup.find('script', id='__NEXT_DATA__'):
                try:
                    if r_found := find_rating_recursive(json.loads(nxt.string)):
                        ratings['overall'], ratings['count'] = r_found.get('value'), r_found.get('count')
                except: pass
                
        # 4. Metadata Script
        if not ratings.get('overall'):
            if meta_tag := soup.find('adbl-product-metadata'):
                if sc := meta_tag.find('script', type='application/json'):
                    try:
                        d = json.loads(sc.string)
                        if 'rating' in d: ratings.update({'overall': d['rating'].get('value'), 'count': d['rating'].get('count')})
                    except: pass
        
        count = ratings.get('count')
        if count and int(count) > 0:
            logging.info(f"    -> Audible: ✅ Found on {domain} (Count: {count})")
            return ratings
        
        if soup.find(['h1', 'h2', 'h3'], class_=re.compile(r'bc-heading|product-title')):
             logging.info(f"    -> Audible: ⚠️ Page found on {domain}, but NO ratings (Count: 0)")
             return {'count': 0}

    logging.info("    -> Audible: ❌ Not found (Page error or Redirect)")
    return None

def find_missing_asin(title, author, duration, lang):
    doms = ["www.audible.com", "www.audible.de"]
    if lang and lang.lower() in ['de', 'deu', 'ger', 'german', 'deutsch']:
        doms = ["www.audible.de", "www.audible.com"]
    
    for d in doms:
        r, soup = fetch_url(f"https://{d}/search", params={"title": title, "author_author": author or "", "ipRedirectOverride": "true"})
        if not soup: continue
        
        for item in soup.find_all('li', class_=re.compile(r'productListItem')):
            asin = item.get('data-asin') or (item.find('div', attrs={'data-asin': True}) or {}).get('data-asin')
            if not asin: continue
            
            ft = item.find('h3', class_=re.compile(r'bc-heading'))
            if not ft: continue
            found_title = ft.get_text(strip=True)
            
            if difflib.SequenceMatcher(None, title.lower(), found_title.lower()).ratio() > 0.7:
                if duration and (rt := item.find('li', class_=re.compile(r'runtimeLabel'))):
                    h = re.search(r'(\d+)\s*(?:Std|hr|h)', rt.text)
                    m = re.search(r'(\d+)\s*(?:Min|m)', rt.text)
                    sec = (int(h.group(1))*3600 if h else 0) + (int(m.group(1))*60 if m else 0)
                    if sec > 0 and abs(duration - sec) > 900: continue
                return asin
    return None

# ================= GOODREADS LOGIC =================

def scrape_gr_details(url):
    r, soup = fetch_url(url)
    if not soup: return None
    res = {'url': url}
    
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
    # 1. ID Search
    for q_id, src in [(isbn, 'ISBN Lookup'), (asin, 'ASIN Lookup')]:
        if q_id:
            if d := scrape_gr_details(f"https://www.goodreads.com/search?q={q_id}"):
                d['source'] = src; return d
    
    # 2. Text Search
    searches = [f"{t} {prim_auth}" for t in [title, clean_title(title)] if t] + [title]
    clean_target = clean_title(title)

    for q in searches:
        r, soup = fetch_url(f"https://www.goodreads.com/search", params={"q": q})
        if not soup: continue
        
        if "/book/show/" in r.url:
            if d := scrape_gr_details(r.url): d['source'] = 'Text Search (Direct Hit)'; return d
        else:
            best_url, best_score = None, 0.0
            for row in soup.find_all('tr', itemtype="http://schema.org/Book"):
                link = row.find('a', class_='bookTitle')
                if not link: continue
                found_title = link.get_text(strip=True)
                clean_found = clean_title(found_title)
                
                t_score = max(difflib.SequenceMatcher(None, title.lower(), found_title.lower()).ratio(),
                              difflib.SequenceMatcher(None, clean_target.lower(), clean_found.lower()).ratio())
                
                if (len(clean_target) > 3 and clean_target.lower() in clean_found.lower()) or \
                   (len(clean_found) > 3 and clean_found.lower() in clean_target.lower()):
                    t_score += 0.2
                
                f_nums, t_nums = extract_volume(found_title), extract_volume(title)
                if (f_nums and t_nums and not f_nums & t_nums): continue
                
                found_auth = row.find('a', class_='authorName').text if row.find('a', class_='authorName') else ""
                if not match_author(authors, found_auth):
                     if t_score > 0.8: logging.info(f"    (Debug) Match Rejected: '{found_title}' score {t_score}, but Author mismatch.")
                     continue
                
                if t_score > 0.7 and t_score > best_score:
                    best_score, best_url = t_score, "https://www.goodreads.com" + link['href']
            
            if best_url:
                if d := scrape_gr_details(best_url): d['source'] = 'Text Search (List Match)'; return d
    return None

def build_description(current_desc, aud, gr, old_aud, old_gr):
    lines = ["⭐ Ratings & Infos"]
    # Audible Block
    if aud and int(aud.get('count',0)) > 0:
        lines.append(f"Audible ({aud.get('count')}):")
        if v := aud.get('overall'): lines.append(f"🏆 {moon_rating(v)} {round(safe_float(v), 1)} / 5 - Overall")
        if v := aud.get('performance'): lines.append(f"🎙️ {moon_rating(v)} {round(safe_float(v), 1)} / 5 - Performance")
        if v := aud.get('story'): lines.append(f"📖 {moon_rating(v)} {round(safe_float(v), 1)} / 5 - Story")
    elif old_aud:
        logging.info("    -> ♻️ Recycling old Audible rating.")
        stats['recycled'] += 1; lines.append(old_aud)
    
    # Goodreads Block
    if gr:
        lines.append(f"Goodreads ({gr.get('count', 0)}):")
        if v := gr.get('val'): lines.append(f"🏆 {moon_rating(v)} {round(safe_float(v), 1)} / 5 - Rating")
    elif old_gr:
        logging.info("    -> ♻️ Recycling old Goodreads rating.")
        if not old_aud: stats['recycled'] += 1
        lines.append(old_gr)
        
    lines.append("⭐")
    
    clean_d = RE_RATING_BLOCK.sub('', current_desc)
    clean_d = re.sub(r'(?s)\*\*Audible\*\*.*?---\s*\n*', '', clean_d)
    return "<br>".join(lines) + "<br>" + re.sub(r'^(?:\s|<br\s*/?>)+', '', clean_d, flags=re.I).strip()

# ================= MAIN =================

def process_library(lib_id, history, failed):
    logging.info(f"--- Processing Library: {lib_id} ---")
    try:
        # Use abs_session for local API speed
        r = abs_session.get(f"{ABS_URL}/api/libraries/{lib_id}/items")
        if r.status_code != 200: raise Exception(f"Status {r.status_code}")
        items = r.json()['results']
    except Exception as e:
        logging.error(f"Failed to fetch library items: {e}"); stats['failed'] += 1; return

    queue = [i for i in items if f"{lib_id}_{i['id']}" not in history]
    due = [i for i in items if i not in queue and (datetime.now() - datetime.strptime(history.get(f"{lib_id}_{i['id']}", "2000-01-01"), "%Y-%m-%d")).days >= REFRESH_DAYS]
    else_skipped = len(items) - len(queue) - len(due)
    
    work_queue = queue + due
    random.shuffle(work_queue)
    
    logging.info(f"Queue: {len(queue)} New, {len(due)} Due. (Total skipped: {else_skipped})")
    consecutive_rl = 0

    for idx, item in enumerate(work_queue[:MAX_BATCH_SIZE]):
        if stats['aborted_ratelimit']: break
        
        try:
            iid, key = item['id'], f"{lib_id}_{item['id']}"
            
            # Fresh details fetch via Session
            ir = abs_session.get(f"{ABS_URL}/api/items/{iid}")
            if ir.status_code != 200: continue
            meta = ir.json()['media']['metadata']
            
            title, asin, isbn, lang = meta.get('title'), meta.get('asin'), meta.get('isbn'), meta.get('language')
            authors = [a.get('name') if isinstance(a, dict) else a for a in meta.get('authors', [])]
            prim_auth = next((a for a in authors), "")
            
            logging.info(f"-"*50)
            logging.info(f"({idx+1}) Processing: {title} (ASIN: {asin if asin else 'None'})")
            stats['processed'] += 1

            # --- AUDIBLE ---
            aud_data = get_audible_data(asin, lang)
            should_search = (not asin) or (asin and aud_data is None)
            
            if should_search and not DRY_RUN:
                if not asin: logging.info("    -> No ASIN present. Searching...")
                else: logging.info("    -> ASIN seems invalid. Searching replacement...")
                
                if found := find_missing_asin(title, prim_auth, item['media'].get('duration'), lang):
                    if found != asin:
                        logging.info(f"    -> ✨ Found NEW ASIN: {found}")
                        abs_session.patch(f"{ABS_URL}/api/items/{iid}/media", json={"metadata": {"asin": found}})
                        asin, stats['asin_found'] = found, stats['asin_found'] + 1
                        aud_data = get_audible_data(asin, lang)

            time.sleep(1)
            
            # --- GOODREADS ---
            gr_data = get_goodreads_data(isbn, asin, title, authors, prim_auth)
            if gr_data: logging.info(f"    -> Goodreads: ✅ Found via {gr_data['source']} (Rating: {gr_data.get('val')})")
            else: logging.info(f"    -> Goodreads: ❌ Not found (Tried: ISBN, ASIN, Text w/ Author validation)")
            
            # Patch ISBN
            if gr_data and not DRY_RUN:
                new_id = gr_data.get('isbn') or gr_data.get('asin')
                is_fallback = not gr_data.get('isbn') and gr_data.get('asin')
                
                if not new_id:
                     if gr_data['source'] not in ['ISBN Lookup', 'ASIN Lookup']: logging.info(f"    -> ISBN: ⚠️ Goodreads data returned no ISBN or ASIN.")
                else:
                    if str(isbn or "").replace('-','') != str(new_id).replace('-',''):
                        type_lbl = "GR-ASIN" if is_fallback else "ISBN"
                        if not isbn: logging.info(f"    -> ISBN: ✨ Missing locally. Adding ({type_lbl}): {new_id}")
                        else: logging.info(f"    -> ISBN: 🔧 Updating (Old: {isbn} -> New: {new_id})")
                        
                        abs_session.patch(f"{ABS_URL}/api/items/{iid}/media", json={"metadata": {"isbn": new_id}})
                        stats['isbn_added' if not isbn else 'isbn_repaired'] += 1
                    else:
                        logging.info(f"    -> ISBN: ✅ Verified Match")

            # --- DESCRIPTION ---
            old_aud = (RE_AUDIBLE_BLOCK.search(meta.get('description', '')) or [None, None])[1]
            old_gr = (RE_GR_BLOCK.search(meta.get('description', '')) or [None, None])[1]
            
            final_desc = build_description(meta.get('description', ''), aud_data, gr_data, old_aud and old_aud.strip(), old_gr and old_gr.strip())
            
            has_aud, has_gr = bool(aud_data or old_aud), bool(gr_data or old_gr)
            is_complete = (asin and has_aud and has_gr) or (not asin and has_gr)

            if not DRY_RUN:
                res = abs_session.patch(f"{ABS_URL}/api/items/{iid}/media", json={"metadata": {"description": final_desc}})
                if res.status_code == 200:
                    logging.info("    -> ✅ UPDATE SUCCESS")
                    if is_complete: stats['success'] += 1
                else: 
                    logging.error(f"    -> ❌ API ERROR: {res.status_code}")
                    stats['failed'] += 1
            else:
                 logging.info(f"    -> [DRY RUN] Would save (Complete: {is_complete}).")
                 if is_complete: stats['success'] += 1

            # --- REPORTING ---
            update_report("audible", key, title, prim_auth, asin, "Not found", has_aud)
            update_report("goodreads", key, title, prim_auth, isbn, "Not found", has_gr)
            
            fails = failed.get(key, 0) + 1
            if is_complete:
                history[key] = datetime.now().strftime("%Y-%m-%d")
                failed.pop(key, None)
            elif fails >= MAX_FAIL_ATTEMPTS:
                logging.info("    -> 🛑 Max attempts reached. Cooldown started.")
                history[key] = datetime.now().strftime("%Y-%m-%d") # Mark as done to trigger cooldown
                failed.pop(key, None)
                stats['cooldown'] += 1
            else:
                failed[key] = fails
                if not has_aud and not has_gr: stats['no_data'] += 1; logging.warning(f"    -> ❌ No data found (Attempt {fails}/{MAX_FAIL_ATTEMPTS}).")
                else: stats['partial'] += 1; logging.info(f"    -> ⚠️ Incomplete data (Attempt {fails}).")
            
            consecutive_rl = 0
            
        except RateLimitException as e:
            consecutive_rl += 1
            logging.warning(f"🛑 Rate Limit DETECTED: {e}")
            if e.is_hard or consecutive_rl >= MAX_CONSECUTIVE_RL: 
                logging.error("🛑 ABORTING script due to Rate Limits."); stats['aborted_ratelimit'] = True; break
            logging.info(f"    -> Pausing for {RECOVERY_PAUSE}s...")
            time.sleep(RECOVERY_PAUSE)
        except Exception as e:
            logging.error(f"CRASH on item {item.get('id')}: {e}"); stats['failed'] += 1
        
        time.sleep(BASE_SLEEP + random.uniform(1, 3))

def main():
    if not ABS_URL or not API_TOKEN: return print("Error: Missing ABS_URL or API_TOKEN env vars.")
    log_file = setup_logging()
    
    # Init Reports
    rw_json(REPORT_DIR + "/dummy", None)
    reports['audible'] = {x['key']: x for x in rw_json(os.path.join(REPORT_DIR, "missing_audible.json")) or []}
    reports['goodreads'] = {x['key']: x for x in rw_json(os.path.join(REPORT_DIR, "missing_goodreads.json")) or []}
    
    start_time = datetime.now()
    logging.info("--- Starting ABS Ratings Update ---")
    history, failed_history = rw_json(HISTORY_FILE), rw_json(FAILED_FILE)
    
    for lib_id in LIBRARY_IDS:
        process_library(lib_id, history, failed_history)
        if stats['aborted_ratelimit']: break
    
    rw_json(HISTORY_FILE, history); rw_json(FAILED_FILE, failed_history); save_reports()
    logging.info(f"--- Finished ---\nStats: {stats}")
    write_env_file(log_file, start_time)

if __name__ == "__main__": main()
