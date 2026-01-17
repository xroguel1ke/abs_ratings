import os
import sys
import requests
from bs4 import BeautifulSoup
import re
import json
import random
from datetime import datetime

# --- CONFIG ---
ASIN = os.getenv('TEST_ASIN')
BASE_LOG_DIR = os.getenv('LOG_DIR', os.path.dirname(os.path.abspath(__file__)))

# --- SETUP RUN DIRECTORY & LOGGING ---
# Erstellt einen Ordner: /debug_logs/YYYY-MM-DD_HH-MM-SS_ASIN/
timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
RUN_DIR = os.path.join(BASE_LOG_DIR, f"{timestamp}_{ASIN}")
os.makedirs(RUN_DIR, exist_ok=True)

# Klasse um print() gleichzeitig in Konsole und Datei zu schreiben
class DualLogger(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open(os.path.join(RUN_DIR, "debug_output.log"), "a", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush() # Sofort speichern

    def flush(self):
        self.terminal.flush()
        self.log.flush()

# Ab jetzt wird jeder print() Aufruf umgeleitet
sys.stdout = DualLogger()
sys.stderr = sys.stdout # Auch Fehler ins Log

# --- REGEX PATTERNS ---
RE_RAW_STORY = re.compile(r'story-value="([0-9.]+)"')
RE_RAW_PERFORMANCE = re.compile(r'performance-value="([0-9.]+)"')
RE_RAW_OVERALL = re.compile(r'value="([0-9.]+)"') 
RE_RAW_COUNT = re.compile(r'count="(\d+)"')
RE_ASIN_URL = re.compile(r'/pd/.*?/([A-Z0-9]{10})')

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15"
]

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-User": "?1"
}

def get_headers():
    h = HEADERS.copy()
    h["User-Agent"] = random.choice(USER_AGENTS)
    return h

def find_rating_recursive(obj):
    if isinstance(obj, dict):
        if 'rating' in obj and isinstance(obj['rating'], dict) and 'value' in obj['rating']: return obj['rating']
        for k, v in obj.items():
            if res := find_rating_recursive(v): return res
    elif isinstance(obj, list):
        for item in obj:
            if res := find_rating_recursive(item): return res
    return None

def analyze_domain(domain):
    print(f"\n{'='*20} {domain.upper()} {'='*20}")
    url = f"https://{domain}/pd/{ASIN}?ipRedirectOverride=true"
    
    cookies = {}
    if "audible.de" in domain: cookies["audible_site_preference"] = "de"
    elif "audible.com" in domain: cookies["audible_site_preference"] = "us"

    try:
        # 1. REQUEST
        print(f"📡 Requesting: {url}")
        r = requests.get(url, headers=get_headers(), cookies=cookies, timeout=15)
        
        print(f"   -> Status: {r.status_code}")
        print(f"   -> Final URL: {r.url}")
        
        url_asin_match = RE_ASIN_URL.search(r.url)
        found_asin_url = url_asin_match.group(1) if url_asin_match else "UNKNOWN"
        if found_asin_url != ASIN and found_asin_url != "UNKNOWN":
            print(f"   ⚠️ REDIRECT DETECTED: ASIN changed from {ASIN} to {found_asin_url}")
        
        if r.status_code != 200:
            print("   ❌ Page failed to load. Skipping extraction checks.")
            check_search_fallback(domain)
            return

        # Dump HTML in den neuen Unterordner
        dump_filename = f"html_{domain}.html"
        dump_path = os.path.join(RUN_DIR, dump_filename)
        with open(dump_path, "w", encoding="utf-8") as f: f.write(r.text)
        print(f"   💾 HTML Dump: {dump_filename}")

        soup = BeautifulSoup(r.text, 'lxml')
        raw_text = r.text

        # 2. CHECK BLOCKERS
        txt_lower = raw_text.lower()
        is_adult = "age restricted" in txt_lower or "submit birth date" in txt_lower
        is_geo = "not available in your region" in txt_lower
        
        if is_adult: print("   ⚠️  WARNING: 'Age Restricted' text found.")
        if is_geo: print("   ⚠️  WARNING: 'Not available in your region' text found.")

        # 3. EXTRACTION METHODS
        print("\n   --- Extraction Analysis ---")
        
        # Method A: Tags
        res_tags = {}
        if sum_tag := soup.find('adbl-rating-summary'):
            res_tags = {'story': sum_tag.get('story-value'), 'perf': sum_tag.get('performance-value')}
            if st := sum_tag.find('adbl-star-rating'):
                res_tags['overall'] = st.get('value')
                res_tags['count'] = st.get('count')
        print(f"   [A] HTML Tags:   {'✅ ' + str(res_tags) if res_tags.get('count') else '❌ Not found'}")

        # Method B: Regex
        res_regex = {}
        if m := RE_RAW_STORY.search(raw_text): res_regex['story'] = m.group(1)
        if m := RE_RAW_PERFORMANCE.search(raw_text): res_regex['perf'] = m.group(1)
        if m := RE_RAW_COUNT.search(raw_text): res_regex['count'] = m.group(1)
        if m := RE_RAW_OVERALL.search(raw_text): res_regex['overall'] = m.group(1)
        print(f"   [B] Raw Regex:   {'✅ ' + str(res_regex) if res_regex.get('count') else '❌ Not found'}")

        # Method C: JSON-LD
        res_json = {}
        for s in soup.find_all('script', type='application/ld+json'):
            try:
                d = json.loads(s.string)
                items = d if isinstance(d, list) else [d]
                for i in items:
                    if 'aggregateRating' in i:
                        res_json['overall'] = i['aggregateRating'].get('ratingValue')
                        res_json['count'] = i['aggregateRating'].get('ratingCount') or i['aggregateRating'].get('reviewCount')
            except: pass
        print(f"   [C] JSON-LD:     {'✅ ' + str(res_json) if res_json.get('count') else '❌ Not found'}")

        # Method D: Next.js
        res_next = {}
        if nxt := soup.find('script', id='__NEXT_DATA__'):
            try:
                if r_found := find_rating_recursive(json.loads(nxt.string)):
                    res_next['overall'], res_next['count'] = r_found.get('value'), r_found.get('count')
            except: pass
        print(f"   [D] Next.js:     {'✅ ' + str(res_next) if res_next.get('count') else '❌ Not found'}")

        # 4. VERDICT
        success = (res_tags.get('count') or res_regex.get('count') or res_json.get('count') or res_next.get('count'))
        
        if success:
            print(f"\n   🎉 VERDICT: SUCCESS on {domain}!")
            if is_adult: print("      (Data found despite Adult Warning -> Script logic handles this correctly)")
        else:
            print(f"\n   🛑 VERDICT: FAILED on {domain} (Detail Page).")
            check_search_fallback(domain)

    except Exception as e:
        print(f"   🔥 CRASH: {e}")

def check_search_fallback(domain):
    print(f"\n   --- Checking Search Fallback ({domain}) ---")
    url = f"https://{domain}/search"
    try:
        r = requests.get(url, params={"keywords": ASIN, "ipRedirectOverride": "true"}, headers=get_headers(), timeout=15)
        soup = BeautifulSoup(r.text, 'lxml')
        
        item = soup.find('li', attrs={'data-asin': ASIN})
        if not item:
             div = soup.find('div', attrs={'data-asin': ASIN})
             if div: item = div.find_parent('li')
        
        if item:
            rating_span = item.find('span', class_=re.compile(r'ratingLabel|ratingText'))
            count_span = item.find('span', class_=re.compile(r'ratingsLabel|ratingCount'))
            
            rating = rating_span.get_text().strip() if rating_span else "None"
            count = count_span.get_text().strip() if count_span else "None"
            
            print(f"   🔎 Search Result: Found item in list!")
            print(f"      Rating Text: '{rating}'")
            print(f"      Count Text:  '{count}'")
            
            if "None" not in rating and "None" not in count:
                print("   ✅ FALLBACK WOULD WORK: Script can extract data from search.")
            else:
                print("   ❌ FALLBACK FAILED: Item found, but no rating text visible.")
        else:
            print("   ❌ FALLBACK FAILED: ASIN not found in search results.")
            
    except Exception as e:
        print(f"   Error checking fallback: {e}")

if __name__ == "__main__":
    if not ASIN:
        print("Error: No TEST_ASIN environment variable found.")
    else:
        print(f"--- LOG START: {datetime.now()} ---")
        analyze_domain("www.audible.com")
        analyze_domain("www.audible.de")
        print(f"\n--- LOG END ---")
