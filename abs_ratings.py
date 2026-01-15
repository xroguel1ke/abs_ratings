import requests
from bs4 import BeautifulSoup
import time
import re
import json
import os
import random
import urllib.parse
import subprocess
import shutil
import difflib  # Used for fuzzy string matching (title comparison)
from datetime import datetime

# ================= CONFIGURATION =================
# Audiobookshelf URL and Token
ABS_URL = os.getenv('ABS_URL', '').rstrip('/')
API_TOKEN = os.getenv('API_TOKEN')
# List of Library IDs to process
LIBRARY_IDS = [l.strip() for l in os.getenv('LIBRARY_IDS', '').split(',') if l.strip()]

# File paths for storing history and failure counts
HISTORY_FILE = "/mnt/user/appdata/audiobookshelf/abs_scripts/rating_history.json"
FAILED_FILE = "/mnt/user/appdata/audiobookshelf/abs_scripts/failed_history.json"

# Settings
REFRESH_DAYS = int(os.getenv('REFRESH_DAYS', 90))  # How often to update existing ratings
MAX_BATCH_SIZE = int(os.getenv('BATCH_SIZE', 150)) # Max items per run
MAX_FAIL_ATTEMPTS = 3  # Number of failures before triggering a cooldown

# Dry Run: If True, no changes are written to ABS
DRY_RUN = os.getenv('DRY_RUN', 'False').lower() == 'true'
BASE_SLEEP = int(os.getenv('SLEEP_TIMER', 6)) 
# =================================================

# Headers for ABS API calls
HEADERS_ABS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

# Headers for scraping (mimics a real browser to avoid blocking)
HEADERS_SCRAPE = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9"
}

# Statistics tracker
stats = {
    "processed": 0, "success": 0, "failed": 0, "no_data": 0, "skipped": 0, "partial": 0, "cooldown": 0, "recycled": 0, "asin_found": 0
}

def load_json(path):
    """Loads a JSON file safely."""
    if os.path.exists(path):
        try:
            with open(path, 'r') as f: return json.load(f)
        except: return {}
    return {}

def save_json(path, data):
    """Saves data to a JSON file."""
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f: json.dump(data, f, indent=4)
    except: pass

def send_unraid_notify():
    """Sends a notification to the Unraid WebGUI if available."""
    subject = "ABS Ratings Update"
    msg = (f"Durchlauf beendet (Batch: {MAX_BATCH_SIZE}).\n"
           f"Bearbeitet: {stats['processed']}\n"
           f"Erfolg (Neu): {stats['success']}\n"
           f"ASINs gefunden: {stats['asin_found']}\n"
           f"Erhalten (Recycled): {stats['recycled']}\n"
           f"In Cooldown: {stats['cooldown']}\n"
           f"Keine Daten: {stats['no_data']}")
    severity = 'normal' if stats['failed'] == 0 else 'warning'
    
    notify_cmd = shutil.which("notify")
    if not notify_cmd:
        # Check common Unraid paths
        possible_paths = ["/usr/local/emhttp/webGui/scripts/notify", "/usr/sbin/notify", "/usr/bin/notify"]
        for p in possible_paths:
            if os.path.exists(p):
                notify_cmd = p
                break
    
    if notify_cmd:
        subprocess.run([notify_cmd, '-e', 'ABS Scripts', '-s', subject, '-d', msg, '-i', severity], check=False)

def is_due_for_update(unique_key, history):
    """Checks if an item hasn't been updated for REFRESH_DAYS."""
    if unique_key not in history: return True
    try:
        last_run = datetime.strptime(history[unique_key], "%Y-%m-%d")
        if (datetime.now() - last_run).days >= REFRESH_DAYS: return True
    except: return True
    return False

def remove_old_rating_block(description):
    """
    Removes the old rating block and aggressively cleans up leading whitespace/breaks.
    Ensures the text starts cleanly after the new block is inserted.
    """
    if not description: return ""
    
    # 1. Remove the entire block from start-star to end-star
    # (?s) allows dot (.) to match newlines
    pattern = r'(?s)⭐\s*Ratings.*?⭐(?:\s|<br\s*/?>)*'
    description = re.sub(pattern, '', description)

    # 2. Legacy Support: Remove old text-only blocks
    description = re.sub(r'(?s)\*\*Audible\*\*.*?---\s*\n*', '', description)

    # 3. Gap Killer: Removes ALL leading whitespace/newlines/breaks
    description = re.sub(r'^(?:\s|<br\s*/?>)+', '', description, flags=re.IGNORECASE)
    
    return description.strip()

def generate_moon_rating(val):
    """Converts a numeric value (e.g., 4.5) into a moon emoji string."""
    try:
        if not val: return "🌑🌑🌑🌑🌑"
        v = round(float(str(val).replace(',', '.')), 1)
        full = int(v)
        decimal = v - full
        half = 1 if 0.25 <= decimal < 0.75 else 0
        if decimal >= 0.75: full += 1
        full = min(full, 5)
        if full == 5: half = 0
        return "🌕" * full + "🌗" * half + "🌑" * (5 - full - half)
    except: return "🌑🌑🌑🌑🌑"

# === HELPER FUNCTIONS FOR ASIN SEARCH ===

def fuzzy_match(s1, s2, threshold=0.8):
    """Compares two strings using difflib. Returns True if similarity >= threshold."""
    if not s1 or not s2: return False
    return difflib.SequenceMatcher(None, s1.lower(), s2.lower()).ratio() >= threshold

def parse_audible_duration(duration_str):
    """
    Parses duration strings like '12 Std. 36 Min.' or '12 hrs 5 mins' into seconds.
    Used to verify if the found book matches the file length.
    """
    if not duration_str: return 0
    hours = 0
    mins = 0
    
    # Extract hours
    m_h = re.search(r'(\d+)\s*(?:Std|hr|h)', duration_str, re.IGNORECASE)
    if m_h: hours = int(m_h.group(1))
    
    # Extract minutes
    m_m = re.search(r'(\d+)\s*(?:Min|m)', duration_str, re.IGNORECASE)
    if m_m: mins = int(m_m.group(1))
    
    return (hours * 3600) + (mins * 60)

def find_missing_asin(title, author, abs_duration_sec):
    """
    Searches for a missing ASIN on Audible.de using Title and Author.
    Verifies the result by matching Title (fuzzy) and Duration (tolerance check).
    """
    if not title: return None
    
    # Build advanced search URL
    query_title = urllib.parse.quote_plus(title)
    query_author = urllib.parse.quote_plus(author) if author else ""
    url = f"https://www.audible.de/search?title={query_title}&author_author={query_author}&ipRedirectOverride=true"
    
    try:
        r = requests.get(url, headers=HEADERS_SCRAPE, timeout=20)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.text, 'lxml')
        
        # Iterate through search results
        items = soup.find_all('li', class_=re.compile(r'productListItem'))
        
        for item in items:
            # 1. Extract ASIN
            asin_attr = item.get('data-asin')
            if not asin_attr: 
                # Fallback: Check inner divs
                div = item.find('div', attrs={'data-asin': True})
                if div: asin_attr = div.get('data-asin')
            
            if not asin_attr: continue
            
            # 2. Verify Title (Fuzzy Match)
            title_tag = item.find('h3', class_=re.compile(r'bc-heading'))
            if not title_tag: continue
            found_title = title_tag.get_text(strip=True)
            
            if not fuzzy_match(title, found_title, 0.7): # Allow slight variations
                continue
                
            # 3. Verify Duration (Critical Check)
            if abs_duration_sec and abs_duration_sec > 0:
                runtime_tag = item.find('li', class_=re.compile(r'runtimeLabel'))
                if runtime_tag:
                    runtime_text = runtime_tag.get_text(strip=True)
                    audible_sec = parse_audible_duration(runtime_text)
                    
                    if audible_sec > 0:
                        diff = abs(abs_duration_sec - audible_sec)
                        # Tolerance: +/- 15 Minutes (900 seconds)
                        # Prevents matching abridged vs. unabridged versions
                        if diff > 900:
                            continue
            
            # If we reach here, Title and Duration match -> Success
            return asin_attr
            
    except: pass
    return None

# ============================================

def get_audible_data(asin):
    """
    Scrapes Audible for ratings. Checks multiple regions (.com, .uk, .de, etc.).
    Returns early if data is found in a region.
    """
    if not asin: return None
    domains = [
        "www.audible.com", "www.audible.co.uk", "www.audible.de",
        "www.audible.fr", "www.audible.ca", "www.audible.com.au",
        "www.audible.it", "www.audible.es"
    ]
    for domain in domains:
        url = f"https://{domain}/pd/{asin}?ipRedirectOverride=true"
        try:
            r = requests.get(url, headers=HEADERS_SCRAPE, timeout=20)
            if r.status_code != 200: continue 
            soup = BeautifulSoup(r.text, 'lxml')
            ratings = {}
            
            # Try getting detailed JSON data (Rating Count + Value)
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

            # Fallback to LD+JSON
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
            
            # Get Performance and Story ratings
            summary_tag = soup.find('adbl-rating-summary')
            if summary_tag:
                if summary_tag.has_attr('performance-value'): ratings['performance'] = summary_tag['performance-value']
                if summary_tag.has_attr('story-value'): ratings['story'] = summary_tag['story-value']
            
            # If we found at least an overall rating, return it
            if ratings.get('overall'): return ratings
        except: pass
    return None

def scrape_goodreads_page(url):
    """Scrapes Goodreads for rating value and count."""
    try:
        r = requests.get(url, headers=HEADERS_SCRAPE, timeout=30)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.text, 'lxml')
        result = {}
        
        # Try JSON-LD first
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
            except: pass
            
        # Fallback: Regex search in HTML for Rating
        if 'val' not in result:
            rating_candidates = soup.find_all(string=re.compile(r"avg rating"))
            for text_node in rating_candidates:
                match = re.search(r'(\d+[.,]\d+)\s+avg rating', text_node)
                if match: 
                    result['val'] = match.group(1).replace(',', '.')
                    break
        
        # Fallback: Regex search for Count
        if 'count' not in result:
            count_candidates = soup.find_all(string=re.compile(r"ratings"))
            for text_node in count_candidates:
                match = re.search(r'([\d,.]+)\s+ratings', text_node)
                if match:
                    clean_count = re.sub(r'[^\d]', '', match.group(1))
                    if clean_count: result['count'] = int(clean_count)
                    break

        return result if 'val' in result else None
    except: pass
    return None

def get_goodreads_rating(isbn, asin, title, author):
    """Tries to find Goodreads rating via ISBN, ASIN, or Title search."""
    def fetch(query): return scrape_goodreads_page(f"https://www.goodreads.com/search?q={query}")
    if isbn:
        res = fetch(isbn)
        if res: return res
    if asin:
        res = fetch(asin)
        if res: return res
    if title:
        search_query = title
        if author: search_query += f" {author}"
        encoded_query = urllib.parse.quote_plus(search_query)
        res = fetch(encoded_query)
        if res: return res
    return None

def process_library(lib_id, history, failed_history):
    print(f"\n--- Library: {lib_id} ---", flush=True)
    try:
        r = requests.get(f"{ABS_URL}/api/libraries/{lib_id}/items", headers=HEADERS_ABS)
        items = r.json()['results']
    except Exception as e:
        print(f"Verbindungsfehler ABS: {e}", flush=True)
        return

    # === PRIORITIZATION ===
    # queue_new: Items never processed
    # queue_due: Items processed > REFRESH_DAYS ago
    queue_new = []
    queue_due = []
    
    for item in items:
        item_id = item['id']
        unique_key = f"{lib_id}_{item_id}"
        
        if unique_key not in history:
            queue_new.append(item)
        elif is_due_for_update(unique_key, history):
            queue_due.append(item)
        else:
            stats['skipped'] += 1

    random.shuffle(queue_new)
    random.shuffle(queue_due)
    work_queue = queue_new + queue_due
    
    print(f"  -> {len(queue_new)} Neue, {len(queue_due)} Fällige.", flush=True)
    
    count_processed = 0
    
    for item in work_queue:
        if count_processed >= MAX_BATCH_SIZE:
            print(f"\n🛑 Batch-Limit von {MAX_BATCH_SIZE} erreicht.", flush=True)
            break
        
        item_id = item['id']
        unique_key = f"{lib_id}_{item_id}"
        metadata = item.get('media', {})['metadata']
        
        # 1. Fetch CURRENT description (for Backup/Recycling)
        item_res = requests.get(f"{ABS_URL}/api/items/{item_id}", headers=HEADERS_ABS)
        if item_res.status_code == 200:
            item_data = item_res.json()
            current_desc = item_data['media']['metadata'].get('description', '')
            # Get duration for ASIN verification
            abs_duration = item_data['media'].get('duration')
        else:
            current_desc = metadata.get('description', '')
            abs_duration = item.get('media', {}).get('duration')

        # 2. Extract OLD ratings (as Fallback)
        old_audible = None
        old_gr = None
        m_aud = re.search(r'(?s)(Audible.*?)<br>\s*(?=Goodreads|⭐)', current_desc)
        if m_aud: old_audible = m_aud.group(1).strip()
        m_gr = re.search(r'(?s)(Goodreads.*?)<br>\s*(?=⭐)', current_desc)
        if m_gr: old_gr = m_gr.group(1).strip()

        # 3. Metadata Setup
        title = metadata.get('title')
        asin = metadata.get('asin')
        isbn = metadata.get('isbn')
        
        author_data = metadata.get('authors', [])
        author = ""
        if isinstance(author_data, list) and len(author_data) > 0:
            first = author_data[0]
            if isinstance(first, str): author = first
            elif isinstance(first, dict) and 'name' in first: author = first['name']

        if not title: continue 
        
        print(f"Bearbeite: {title}...", flush=True)
        stats['processed'] += 1
        
        # === NEW: AUTOMATIC ASIN SEARCH ===
        if not asin and not DRY_RUN:
            print("  -> 🔍 Keine ASIN. Suche auf Audible...", flush=True)
            found_asin = find_missing_asin(title, author, abs_duration)
            if found_asin:
                print(f"  -> ✨ ASIN gefunden: {found_asin} (Speichere in ABS...)", flush=True)
                # Patch ASIN directly to ABS
                patch_asin_url = f"{ABS_URL}/api/items/{item_id}/media"
                requests.patch(patch_asin_url, json={"metadata": {"asin": found_asin}}, headers=HEADERS_ABS)
                # Use found ASIN for current run
                asin = found_asin
                stats['asin_found'] += 1
            else:
                print("  -> ❌ Keine passende ASIN gefunden.", flush=True)
        # ====================================

        # Fetch Ratings
        audible = get_audible_data(asin)
        time.sleep(1)
        gr = get_goodreads_rating(isbn, asin, title, author)

        # Check if we have data (New or Recycled)
        audible_found = bool(audible) or bool(old_audible)
        gr_found = bool(gr) or bool(old_gr)
        
        has_asin = bool(asin)
        is_complete = False
        if has_asin:
            if audible_found and gr_found: is_complete = True
        else:
            if gr_found: is_complete = True
        
        # === 3-STRIKES LOGIC (COOLDOWN) ===
        if not is_complete:
            if not audible_found and not gr_found:
                # Total failure -> Increment counter
                fails = failed_history.get(unique_key, 0) + 1
                failed_history[unique_key] = fails
                stats['no_data'] += 1
                print(f"  -> ❌ Keine Daten (Versuch {fails}/{MAX_FAIL_ATTEMPTS}).", flush=True)
                
                # If max attempts reached, set to cooldown
                if fails >= MAX_FAIL_ATTEMPTS:
                    print(f"  -> 💤 Cooldown aktiviert.", flush=True)
                    history[unique_key] = datetime.now().strftime("%Y-%m-%d")
                    save_json(HISTORY_FILE, history)
                    del failed_history[unique_key]
                    save_json(FAILED_FILE, failed_history)
                    stats['cooldown'] += 1
                else:
                    save_json(FAILED_FILE, failed_history)
            else:
                stats['partial'] += 1
                print(f"  -> ⚠️ Unvollständig (Audible: {audible_found}, GR: {gr_found}).", flush=True)
        else:
            # Success -> Clear fail counter
            if unique_key in failed_history:
                del failed_history[unique_key]
                save_json(FAILED_FILE, failed_history)
        
        # === BUILD LAYOUT ===
        BR = "<br>" 
        block = f"⭐ Ratings & Infos{BR}"
        
        # -- Audible Block --
        if audible:
            # New data found
            cnt = audible.get('count')
            header_text = f"Audible ({cnt}):" if cnt else "Audible:"
            block += f"{header_text}{BR}"
            ov = audible.get('overall')
            pf = audible.get('performance')
            st = audible.get('story')
            if ov: block += f"🏆 {generate_moon_rating(ov)} {round(float(ov), 1)} / 5 - Overall{BR}"
            if pf: block += f"🎙️ {generate_moon_rating(pf)} {round(float(pf), 1)} / 5 - Performance{BR}"
            if st: block += f"📖 {generate_moon_rating(st)} {round(float(st), 1)} / 5 - Story{BR}"
        elif old_audible:
            # No new data, but old data exists -> RECYCLE
            print("  -> ♻️ Behalte altes Audible Rating.", flush=True)
            stats['recycled'] += 1
            block += f"{old_audible}{BR}"

        # -- Goodreads Block --
        if gr:
            # New data found
            cnt = gr.get('count')
            header_text = f"Goodreads ({cnt}):" if cnt else "Goodreads:"
            block += f"{header_text}{BR}"
            val = gr.get('val')
            if val: block += f"🏆 {generate_moon_rating(val)} {round(float(val), 1)} / 5 - Rating{BR}"
        elif old_gr:
            # Recycle old data
            print("  -> ♻️ Behalte altes Goodreads Rating.", flush=True)
            if not old_audible: stats['recycled'] += 1
            block += f"{old_gr}{BR}"
        
        block += f"⭐{BR}" 
        # ====================
        
        # Clean up existing description and merge
        clean_desc = remove_old_rating_block(current_desc)
        final_desc = block + clean_desc

        if not DRY_RUN:
            patch_url = f"{ABS_URL}/api/items/{item_id}/media"
            res = requests.patch(patch_url, json={"metadata": {"description": final_desc}}, headers=HEADERS_ABS)
            if res.status_code == 200:
                print(f"  -> ✅ UPDATE OK.", flush=True)
                if is_complete:
                    history[unique_key] = datetime.now().strftime("%Y-%m-%d")
                    save_json(HISTORY_FILE, history)
                    stats['success'] += 1
            else:
                print(f"  -> ❌ API FEHLER: {res.status_code}", flush=True)
                stats['failed'] += 1
        else:
            print(f"  -> [DRY RUN] Would save (Complete: {is_complete}).", flush=True)
            if is_complete: stats['success'] += 1
        
        count_processed += 1
        time.sleep(BASE_SLEEP + random.uniform(1, 3))

    print(f"--- Fertig ({count_processed} Items bearbeitet) ---", flush=True)

def main():
    if not ABS_URL or not API_TOKEN: return
    history = load_json(HISTORY_FILE)
    failed_history = load_json(FAILED_FILE)
    
    for lib_id in LIBRARY_IDS:
        process_library(lib_id, history, failed_history)
    if not DRY_RUN: send_unraid_notify()

if __name__ == "__main__":
    main()
