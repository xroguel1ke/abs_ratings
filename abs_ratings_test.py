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

HEADERS_ABS = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
HEADERS_SCRAPE = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9"
}

# Pre-compiled Regex
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
    
    with open(ENV_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(f"ABS_SUBJECT='{sub}'\nABS_ICON='{icon}'\nABS_HEADER='{head}'\nABS_DURATION='{dur}'\nABS_REPORT_BODY='{body}'\nABS_LOG_FILE='{os.path.basename(log_file)}'\n")

def fetch_url(url, params=None):
    try:
        r = requests.get(url, headers=HEADERS_SCRAPE, params=params, timeout=20)
        if r.status_code == 429: raise RateLimitException("HTTP 429", True)
        if r.status_code in [503, 403]: raise RateLimitException(f"HTTP {r.status_code}")
        
        soup = BeautifulSoup(r.text, 'lxml')
        txt = soup.get_text().lower()
        if "captcha" in (soup.title.string or "").lower() or any(x in txt and len(txt) < 5000 for x in ["enter the characters", "robot check"]):
            raise RateLimitException("Captcha detected")
        return r, soup
    except RateLimitException: raise
    except: return None, None

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

def match_author(abs_authors, gr_author):
    if not abs_authors or not gr_author: return False
    gr_tok = set(re.split(r'[^a-zA-Z0-9]+', gr_author.lower()))
    for a in abs_authors:
        if difflib.SequenceMatcher(None, a.lower(), gr_author.lower()).ratio() > 0.6: return True
        if len(set(re.split(r'[^a-zA-Z0-9]+', a.lower())).intersection(gr_tok)) >= 2: return True
    return False

def get_audible_data(asin, language):
    if not asin: return None
    # FIX: Alle 8 Domains + Smart Sorting für Deutsch
    domains = [
        "www.audible.com", "www.audible.de", "www.audible.co.uk", "www.audible.fr",
        "www.audible.ca", "www.audible.com.au", "www.audible.it", "www.audible.es"
    ]
    if language and language.lower() in ['de', 'deu', 'ger', 'german', 'deutsch']:
        if "www.audible.de" in domains:
            domains.remove("www.audible.de"); domains.insert(0, "www.audible.de")

    for domain in domains:
        r, soup = fetch_url(f"https://{domain}/pd/{asin}?ipRedirectOverride=true")
        if not r or r.status_code == 404 or "/pderror" in r.url: continue
        
        ratings = {}
        if sum_tag := soup.find('adbl-rating-summary'):
            ratings = {k: sum_tag.get(f'{k}-value') for k in ['performance', 'story']}
            if st := sum_tag.find('adbl-star-rating'):
                ratings.update({'overall': st.get('value'), 'count': st.get('count')})
        
        if not ratings.get('overall'):
            for s in soup.find_all('script', type='application/ld+json'):
                try:
                    d = json.loads(s.string)
                    for i in (d if isinstance(d, list) else [d]):
                        if 'aggregateRating' in i: ratings['overall'] = i['aggregateRating'].get('ratingValue')
                except: pass
        
        if ratings.get('count') and int(ratings['count']) > 0:
            logging.info(f"   -> Audible: ✅ Found on {domain} (Count: {ratings['count']})")
            return ratings
        elif soup.find(class_=re.compile(r'bc-heading')):
             return {'count': 0} 

    return None

def find_missing_asin(title, author, duration, lang):
    # FIX: Auch hier Smart Sorting
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
            if ft and difflib.SequenceMatcher(None, title.lower(), ft.get_text(strip=True).lower()).ratio() > 0.7:
                if duration and (rt := item.find('li', class_=re.compile(r'runtimeLabel'))):
                    h = re.search(r'(\d+)\s*(?:Std|hr|h)', rt.text)
                    m = re.search(r'(\d+)\s*(?:Min|m)', rt.text)
                    sec = (int(h.group(1))*3600 if h else 0) + (int(m.group(1))*60 if m else 0)
                    if abs(duration - sec) > 900: continue
                return asin
    return None

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

    return res if 'val' in res else None

def get_goodreads_data(isbn, asin, title, authors, prim_auth):
    for q_id, src in [(isbn, 'ISBN Lookup'), (asin, 'ASIN Lookup')]:
        if q_id:
            if d := scrape_gr_details(f"https://www.goodreads.com/search?q={q_id}"):
                d['source'] = src; return d
    
    searches = [f"{t} {prim_auth}" for t in [title, clean_title(title)] if t] + [title]
    for q in searches:
        r, soup = fetch_url(f"https://www.goodreads.com/search", params={"q": q})
        if not soup: continue
        
        if "/book/show/" in r.url:
            if d := scrape_gr_details(r.url): d['source'] = 'Direct Hit'; return d
        else:
            best_url, best_score = None, 0.0
            for row in soup.find_all('tr', itemtype="http://schema.org/Book"):
                link = row.find('a', class_='bookTitle')
                if not link: continue
                score = difflib.SequenceMatcher(None, title.lower(), link.get_text(strip=True).lower()).ratio()
                f_nums, t_nums = set(RE_VOL.findall(link.text)), set(RE_VOL.findall(title))
                if (f_nums and t_nums and not f_nums & t_nums) or not match_author(authors, row.find('a', class_='authorName').text):
                     continue
                if score > 0.7 and score > best_score:
                    best_score, best_url = score, "https://www.goodreads.com" + link['href']
            
            if best_url:
                if d := scrape_gr_details(best_url): d['source'] = 'List Match'; return d
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
        items = requests.get(f"{ABS_URL}/api/libraries/{lib_id}/items", headers=HEADERS_ABS).json()['results']
    except: stats['failed'] += 1; return

    queue = [i for i in items if f"{lib_id}_{i['id']}" not in history]
    due = [i for i in items if i not in queue and (datetime.now() - datetime.strptime(history.get(f"{lib_id}_{i['id']}", "2000-01-01"), "%Y-%m-%d")).days >= REFRESH_DAYS]
    work_queue = queue + due
    random.shuffle(work_queue)
    
    logging.info(f"Queue: {len(queue)} New, {len(due)} Due. Skipped: {len(items)-len(work_queue)}")
    consecutive_rl = 0

    for idx, item in enumerate(work_queue[:MAX_BATCH_SIZE]):
        if stats['aborted_ratelimit']: break
        
        try:
            iid, key = item['id'], f"{lib_id}_{item['id']}"
            # FIX: Frische Daten abrufen (war schon drin, aber jetzt expliziter)
            meta = requests.get(f"{ABS_URL}/api/items/{iid}", headers=HEADERS_ABS).json()['media']['metadata']
            title, asin, isbn, lang = meta.get('title'), meta.get('asin'), meta.get('isbn'), meta.get('language')
            authors = [a.get('name') if isinstance(a, dict) else a for a in meta.get('authors', [])]
            prim_auth = next((a for a in authors), "")
            
            logging.info(f"-"*50 + f"\n({idx+1}) Processing: {title} (ASIN: {asin}, Lang: {lang})")
            stats['processed'] += 1

            # Audible (jetzt mit Language Parameter)
            aud_data = get_audible_data(asin, lang)
            if (not asin or not aud_data) and not DRY_RUN:
                if found := find_missing_asin(title, prim_auth, item['media'].get('duration'), lang):
                    if found != asin:
                        logging.info(f"   -> ✨ New ASIN: {found}")
                        requests.patch(f"{ABS_URL}/api/items/{iid}/media", json={"metadata": {"asin": found}}, headers=HEADERS_ABS)
                        asin, stats['asin_found'] = found, stats['asin_found'] + 1
                        aud_data = get_audible_data(asin, lang)
            
            time.sleep(1)
            gr_data = get_goodreads_data(isbn, asin, title, authors, prim_auth)
            logging.info(f"   -> Goodreads: {'✅ ' + gr_data['source'] if gr_data else '❌ Not found'}")
            
            if gr_data and not DRY_RUN:
                new_id = gr_data.get('isbn') or gr_data.get('asin')
                if new_id and str(isbn or "").replace('-','') != str(new_id).replace('-',''):
                    logging.info(f"   -> ISBN/ID Update: {new_id}")
                    requests.patch(f"{ABS_URL}/api/items/{iid}/media", json={"metadata": {"isbn": new_id}}, headers=HEADERS_ABS)
                    stats['isbn_added' if not isbn else 'isbn_repaired'] += 1

            old_aud = (RE_AUDIBLE_BLOCK.search(meta.get('description', '')) or [None, None])[1]
            old_gr = (RE_GR_BLOCK.search(meta.get('description', '')) or [None, None])[1]
            final_desc = build_description(meta.get('description', ''), aud_data, gr_data, old_aud and old_aud.strip(), old_gr and old_gr.strip())
            
            has_aud, has_gr = bool(aud_data or old_aud), bool(gr_data or old_gr)
            is_complete = (asin and has_aud and has_gr) or (not asin and has_gr)

            if not DRY_RUN:
                if requests.patch(f"{ABS_URL}/api/items/{iid}/media", json={"metadata": {"description": final_desc}}, headers=HEADERS_ABS).status_code == 200:
                    logging.info("   -> ✅ Description updated")
                    if is_complete: stats['success'] += 1
                else: stats['failed'] += 1
            else:
                 if is_complete: stats['success'] += 1

            update_report("audible", key, title, prim_auth, asin, "Not found", has_aud)
            update_report("goodreads", key, title, prim_auth, isbn, "Not found", has_gr)
            
            fails = failed.get(key, 0) + (1 if not is_complete else 0)
            if is_complete or fails >= MAX_FAIL_ATTEMPTS:
                history[key] = datetime.now().strftime("%Y-%m-%d")
                failed.pop(key, None)
                if not is_complete: stats['cooldown'] += 1
            else:
                failed[key] = fails
                stats['partial' if has_aud or has_gr else 'no_data'] += 1
            
            consecutive_rl = 0
            
        except RateLimitException as e:
            consecutive_rl += 1
            logging.warning(f"🛑 Rate Limit: {e}")
            if e.is_hard or consecutive_rl >= MAX_CONSECUTIVE_RL: stats['aborted_ratelimit'] = True; break
            time.sleep(RECOVERY_PAUSE)
        except Exception as e:
            logging.error(f"Error: {e}"); stats['failed'] += 1
        
        time.sleep(BASE_SLEEP + random.uniform(1, 3))

def main():
    if not ABS_URL or not API_TOKEN: return print("Missing Config")
    log = setup_logging()
    rw_json(REPORT_DIR + "/dummy", None)
    reports['audible'] = {x['key']: x for x in rw_json(os.path.join(REPORT_DIR, "missing_audible.json")) or []}
    reports['goodreads'] = {x['key']: x for x in rw_json(os.path.join(REPORT_DIR, "missing_goodreads.json")) or []}
    
    start = datetime.now()
    hist, fail = rw_json(HISTORY_FILE), rw_json(FAILED_FILE)
    
    for lid in LIBRARY_IDS:
        process_library(lid, hist, fail)
        if stats['aborted_ratelimit']: break
    
    rw_json(HISTORY_FILE, hist); rw_json(FAILED_FILE, fail); save_reports()
    logging.info(f"Finished. Stats: {stats}")
    write_env_file(log, start)

if __name__ == "__main__": main()
