"""
Core of the web UI: settings, run manager (starts abs_ratings.py as a subprocess), scheduler,
notifications (Apprise) and readers for the files the script writes into the data folder.
The script itself stays a normal command line program; the UI only passes environment variables.
"""
import glob, json, os, re, secrets, signal, subprocess, sys, threading, time, zipfile, io
from datetime import datetime, timedelta

import requests
from werkzeug.security import generate_password_hash, check_password_hash

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))          # repo root: abs_ratings.py lives here
CONFIG_DIR = os.getenv('CONFIG_DIR', '/config')                                # data folder (history, logs, reports ...)
LOG_DIR = os.path.join(CONFIG_DIR, 'logs')
REPORT_DIR = os.path.join(CONFIG_DIR, 'reports')
SETTINGS_FILE = os.path.join(CONFIG_DIR, 'settings.json')
RUNS_FILE = os.path.join(CONFIG_DIR, 'webui_runs.json')

# ---------------------------------------------------------------- settings
# (key, label, type, default, help, group). 'secret' fields are never sent back to the browser.
FIELDS = [
    # (key, label, type, default, help, group) - English source text (German in webui/i18n_de.py); groups are cards, SECTIONS the left menu
    ('ABS_URL', 'Audiobookshelf URL', 'str', '', 'e.g. http://192.168.50.16:13378', 'Connection'),
    ('API_TOKEN', 'ABS API token', 'secret', '', 'ABS: Settings → Users → API token', 'Connection'),
    ('LIBRARY_IDS', 'Libraries', 'libraries', '', 'Which ABS libraries are processed', 'Connection'),
    ('ABS_TIMEOUT', 'ABS timeout (s)', 'int', 30, '', 'Connection'),

    ('DRY_RUN', 'Test run (DRY_RUN) for scheduled scans', 'bool', True, 'On = nothing is written to ABS, changes are only planned and logged', 'Run'),
    ('BATCH_SIZE', 'Books per scan', 'int', 300, 'Picked at random from all due books of all libraries', 'Run'),
    ('REFRESH_DAYS', 'Refresh after (days)', 'int', 90, '0 = every book is due (one complete pass, also in maintenance mode)', 'Run'),
    ('RUN_MODE', 'Run mode', 'choice:auto|buildup|maintain', 'auto', 'auto = build-up until every book was checked once, then maintenance. Maintenance: new and changed books always, plus a few random due books', 'Run'),
    ('MAINTAIN_SAMPLE', 'Random books per maintenance scan', 'int', 30, 'Besides new and changed books', 'Run'),
    ('QUICK_MODE_MINUTES', 'Quick mode: look for new books every (minutes)', 'int', 0, '0 = off. New books in ABS are checked right away instead of waiting for the next scheduled scan', 'Run'),
    ('ABS_BACKUP', 'ABS backup before real scans', 'bool', True, 'Creates an ABS backup before every scan without DRY_RUN; if it fails nothing is written', 'Run'),
    ('SLEEP_TIMER', 'Extra pause between books (s)', 'float', 0, 'Every source has its own minimum interval, so 0 is safe and fastest', 'Run'),
    ('NO_RATINGS_RECHECK_DAYS', 'Re-check books without ratings (days)', 'int', 7, '', 'Run'),
    ('LOG_LEVEL', 'Log level', 'choice:INFO|DEBUG', 'INFO', 'DEBUG only for troubleshooting (much bigger logs)', 'Logs'),
    ('LOG_RETENTION_DAYS', 'Keep logs (days)', 'int', 30, 'Older logs and planning files are deleted', 'Logs'),

    ('AUDIBLE_MARKETPLACES', 'Audible stores', 'str', 'com,co.uk,ca,com.au,de,fr,it,es', 'An ASIN is searched in these stores', 'Audible'),
    ('USE_AUDIBLE_API', 'Use the Audible API', 'bool', True, 'Off = read web pages only (not recommended)', 'Audible'),
    ('AUDIBLE_API_INTERVAL', 'Interval Audible API (s)', 'float', 0.5, '', 'Audible'),
    ('AUDIBLE_WEB_INTERVAL', 'Interval Audible web page (s)', 'float', 3, '', 'Audible'),
    ('BOOK_LANGUAGES', 'Languages of your books', 'str', '', 'Comma list, e.g. English, German. Audible editions in other languages are never taken; empty = any language', 'Audible'),
    ('RUNTIME_TOLERANCE_PCT', 'Same-book runtime tolerance (%)', 'float', 15, 'Only decides whether the ASIN still names the same BOOK (another recording can be 12-16 % longer). Lower values lose correct matches. Narrator, publisher, year and cover need the runtime within 10 min anyway', 'Audible'),
    ('RUNTIME_TOLERANCE_MIN', 'Same-book runtime tolerance (min)', 'float', 20, 'Both limits must be exceeded', 'Audible'),

    ('HARDCOVER_ENABLED', 'Use Hardcover', 'bool', True, 'Ratings and the print ISBN. The scan stops shortly before the daily limit', 'Hardcover'),
    ('HARDCOVER_TOKEN', 'Hardcover API token', 'secret', '', 'hardcover.app → Settings → API', 'Hardcover'),
    ('HARDCOVER_PAID', 'Paid Hardcover plan', 'bool', False, 'Off = free plan: at most 5000 requests per day. On = 50000 per day. Both 60 per minute', 'Hardcover'),
    ('HARDCOVER_INTERVAL', 'Interval Hardcover (s)', 'float', 1.1, 'Hardcover allows 60 requests per minute', 'Hardcover'),

    ('GOODREADS_ENABLED', 'Use Goodreads', 'bool', False, 'Experimental: Goodreads blocks bots quickly. At the first block Goodreads is stopped for the scan', 'Goodreads'),
    ('GOODREADS_MAX_PER_RUN', 'Goodreads searches per scan', 'int', 25, 'Fallback when no book page helps. Remembered books do not count. Safe so far: about 25 per night', 'Goodreads'),
    ('GOODREADS_PAGES_PER_RUN', 'Goodreads book pages per scan', 'int', 3, 'Book page first (allowed by Goodreads robots.txt), but a bot check comes after 3-7 pages; then only the search is used for the rest of the scan', 'Goodreads'),
    ('GR_IMPERSONATE', 'Goodreads: imitate a browser signature', 'bool', False, 'Sends the network signature of Chrome (curl_cffi, no browser). Your choice and risk: Goodreads does not want bots. Budgets and the stop at a block stay', 'Goodreads'),
    ('GOODREADS_INTERVAL', 'Interval Goodreads (s)', 'float', 10, '', 'Goodreads'),
    ('GR_BLOCK_COOLDOWN', 'Pause after a Goodreads block (s)', 'int', 900, '', 'Goodreads'),
    ('GR_MAX_COOLDOWNS', 'Pauses before Goodreads is skipped', 'int', 0, '0 = stop at the first block (recommended)', 'Goodreads'),
    ('GR_BOOK_PAGES', 'Goodreads book page first', 'bool', True, 'Off = read the rating from the search list only', 'Goodreads'),
    ('GR_MISSING_RECHECK_DAYS', 'Re-check books without Goodreads (days)', 'int', 30, '', 'Goodreads'),

    ('RATING_ORDER', 'Sources in the rating block', 'order:audible|goodreads|hardcover', 'audible,goodreads,hardcover', 'Ticked sources are shown, top to bottom (all enabled sources are still asked)', 'Metadata'),
    ('RATING_ICONS', 'Rating symbols', 'choice:moons|stars|circles|squares|diamonds|yellow_stars|hearts|traffic|green_squares|orange_circles', 'moons', 'New and refreshed ratings use these; "Change all rating symbols" rewrites the existing ones', 'Metadata'),
    ('RATING_LABELS', 'Rating block language', 'choice:en|de', 'en', '', 'Metadata'),
    ('RATING_POSITION', 'Rating block position', 'choice:top|bottom', 'top', '', 'Metadata'),
    ('RATING_MIN_COUNT', 'Minimum number of ratings', 'int', 0, 'A source with fewer ratings is not shown (0 = all)', 'Metadata'),
    ('FILL_COVERS', 'Set missing covers', 'bool', True, 'Only for a verified ASIN', 'Metadata'),
    ('MATCH_CHAPTERS', 'Take Audible chapters', 'bool', True, 'For files without proper chapters (none, one, only numbers) and only when they fit reliably: same length to 5 s, or the file\'s own chapter marks / Audible\'s intro-outro explain a difference up to 2 min. lock_chapters protects a book', 'Metadata'),
    ('UPDATE_COVERS', 'Replace existing covers', 'bool', True, 'Only for a verified ASIN (same recording); the old cover is backed up and can be restored', 'Metadata'),
    ('GENRE_LANGUAGE', 'Genre language', 'choice:en|keep', 'en', 'Audible.de genres are translated to English', 'Metadata'),
    ('GENRE_RULES', 'Genre rules', 'text', '', 'One per line: "Old => New" renames, "Old => -" never adds that genre (only genres coming from Audible)', 'Metadata'),

    ('SERIES_MERGE', 'Merge series automatically', 'bool', True, 'Only safe cases; uncertain ones go to the review list', 'Series & authors'),
    ('AUTHOR_NORMALIZE', 'Unify author spellings', 'bool', True, '', 'Series & authors'),
    ('REMOVE_ROLE_CREDITS', 'Keep only real authors', 'bool', True, 'Removes illustrators, translators, editors, forewords … from the author field (only when a real author remains); never taken from Audible', 'Series & authors'),
    ('MISSING_SERIES', 'List missing volumes of your series', 'bool', True, 'Audible series lists are checked once a week (a few hundred requests)', 'Series & authors'),
    ('MISSING_NOVELLAS', 'Missing volumes: include novellas (0.5, 2.5 ...)', 'bool', False, '', 'Series & authors'),
    ('GENRE_CLEANUP', 'Clean up genres', 'bool', True, 'One spelling per genre (the most used one); number-only and foreign-script genres are removed from English/German books', 'Series & authors'),
    ('REMOVE_EMPTY_AUTHORS', 'Remove empty authors', 'bool', True, 'Deletes only ABS authors without a single book (left-overs after merges); books are never deleted', 'Series & authors'),
    ('LIBRARY_REPORT', 'Create the library report', 'bool', True, '', 'Series & authors'),
    ('SERIES_LOCK_AFTER_MERGE', 'Set lock_series after a merge', 'bool', False, 'Old variant, not recommended', 'Series & authors'),

    ('UI_LANGUAGE', 'Language', 'choice:en|de', 'en', '', 'Interface'),
]
CHOICE_LABELS = {'en': 'English', 'de': 'Deutsch', 'moons': '🌕🌕🌕🌗🌑', 'stars': '★★★⯪☆', 'circles': '●●●◐○', 'squares': '■■■◧□', 'diamonds': '◆◆◆⬖◇',
                 'yellow_stars': '⭐⭐⭐✨', 'hearts': '❤️❤️❤️💗🤍', 'traffic': '🟢🟢🟢🟡⚪', 'green_squares': '🟩🟩🟩🟨⬜', 'orange_circles': '🟠🟠🟠🟡⚪', 'top': 'Above the description', 'bottom': 'Below the description',
                 'keep': 'Keep as delivered', 'auto': 'Automatic', 'buildup': 'Build-up (whole library)', 'maintain': 'Maintenance (new and changed books)',
                 'INFO': 'Normal', 'DEBUG': 'Debug (detailed)'}
# rarely changed: shown folded ("Advanced") inside their card
ADVANCED = {'ABS_TIMEOUT', 'SLEEP_TIMER', 'NO_RATINGS_RECHECK_DAYS', 'USE_AUDIBLE_API', 'AUDIBLE_API_INTERVAL',
            'AUDIBLE_WEB_INTERVAL', 'RUNTIME_TOLERANCE_PCT', 'RUNTIME_TOLERANCE_MIN', 'HARDCOVER_INTERVAL', 'GOODREADS_INTERVAL', 'GR_BLOCK_COOLDOWN',
            'GR_MAX_COOLDOWNS', 'GR_MISSING_RECHECK_DAYS', 'GR_BOOK_PAGES', 'GOODREADS_PAGES_PER_RUN', 'SERIES_LOCK_AFTER_MERGE'}
# settings sections (left menu, one form + save button each) -> the field groups (cards) they contain
SECTIONS = {'Connection': ['Connection'], 'Run': ['Run'], 'Sources': ['Audible', 'Hardcover', 'Goodreads'], 'Metadata': ['Metadata'],
            'Series & authors': ['Series & authors'], 'Logs': ['Logs'], 'Schedule': [], 'Notifications': [], 'Interface': ['Interface']}
def section_slug(name): return re.sub(r'[^a-z0-9]+', '-', name.lower().replace('&', 'and')).strip('-')
# cards that belong to a data source: colour dot
PROVIDERS = {'Audible': ('', '#c8833a', 'Main source: ASIN, ratings, metadata, cover'),
             'Hardcover': ('', '#6f7f5a', 'Ratings and print ISBN'),
             'Goodreads': ('', '#8a6f52', 'Ratings – experimental')}
SCRIPT_KEYS = [f[0] for f in FIELDS if f[0] not in ('LOG_RETENTION_DAYS', 'UI_LANGUAGE')]
DEFAULTS = {f[0]: f[3] for f in FIELDS}
DEFAULTS.update({
    'schedule_enabled': False, 'schedule_mode': 'simple', 'schedule_days': [0, 1, 2, 3, 4, 5, 6],
    'schedule_time': '02:30', 'schedule_cron': '30 2 * * *',
    'apprise_url': '', 'notify_on': 'errors',
    'password_hash': generate_password_hash('admin'), 'must_change_password': True,
    'secret_key': '',
})

_lock = threading.RLock()


def load_settings():
    with _lock:
        try:
            with open(SETTINGS_FILE, encoding='utf-8') as f: data = json.load(f)
        except (OSError, ValueError): data = {}
        s = dict(DEFAULTS); s.update(data)
        if not s.get('secret_key'):
            s['secret_key'] = secrets.token_hex(32); save_settings(s)
        return s


def save_settings(s):
    with _lock:
        os.makedirs(CONFIG_DIR, exist_ok=True)
        tmp = SETTINGS_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f: json.dump(s, f, indent=2, ensure_ascii=False)
        os.replace(tmp, SETTINGS_FILE)


SCAN_FORM_BOOL = ('task_meta', 'task_authors', 'task_series', 'task_roles', 'task_empty', 'dry', 'backup', 'HARDCOVER_ENABLED', 'GOODREADS_ENABLED')
SCAN_FORM_INT = ('BATCH_SIZE', 'REFRESH_DAYS')


def scan_form(s):
    """Last choices on the scan page; defaults from the saved settings (a choice there never changes a setting)."""
    f = {'task_meta': True, 'task_authors': bool(s.get('AUTHOR_NORMALIZE')), 'task_series': bool(s.get('SERIES_MERGE')),
         'task_roles': bool(s.get('REMOVE_ROLE_CREDITS')), 'task_empty': bool(s.get('REMOVE_EMPTY_AUTHORS')), 'dry': True, 'backup': bool(s.get('ABS_BACKUP')),
         'BATCH_SIZE': s.get('BATCH_SIZE'), 'REFRESH_DAYS': s.get('REFRESH_DAYS'), 'HARDCOVER_ENABLED': bool(s.get('HARDCOVER_ENABLED')),
         'GOODREADS_ENABLED': bool(s.get('GOODREADS_ENABLED')), 'selection': 'all', 'list': '', 'adv_open': False}
    f.update({k: v for k, v in (s.get('scan_form') or {}).items() if k in f})
    return f


def save_scan_form(form):
    """form: request.form of the scan page (checkbox missing = off)."""
    s = load_settings()
    f = {k: form.get(k) == 'on' for k in SCAN_FORM_BOOL}
    for k in SCAN_FORM_INT:
        try: f[k] = max(0, int(form.get(k)))
        except (TypeError, ValueError): f[k] = s.get(k)
    f['selection'] = form.get('selection') if form.get('selection') in ('all', 'problems', 'new', 'list') else 'all'
    f['list'] = (form.get('list') or '')[:20000]
    f['adv_open'] = form.get('adv_open') == '1'
    s['scan_form'] = f
    save_settings(s)
    return f


APP_VERSION = os.getenv('APP_VERSION', 'dev')
_UPDATE = {'ts': 0, 'newer': False}


def update_available():
    """'branch@sha' image: is there a newer commit on that branch? Asked at most every 6 h (GitHub API, no token, nothing sent)."""
    if '@' not in APP_VERSION: return False
    if time.time() - _UPDATE['ts'] < 6 * 3600: return _UPDATE['newer']
    _UPDATE['ts'] = time.time()
    branch, sha = APP_VERSION.split('@', 1)
    try:
        r = requests.get(f"https://api.github.com/repos/xroguel1ke/abs_ratings/commits/{branch}", timeout=10,
                         headers={'Accept': 'application/vnd.github+json', 'User-Agent': 'abs-ratings'})
        if r.status_code == 200: _UPDATE['newer'] = not str(r.json().get('sha', '')).startswith(sha[:7])
    except (requests.RequestException, ValueError): pass
    return _UPDATE['newer']


EXPORT_SKIP = {'password_hash', 'secret_key', 'must_change_password', 'apprise_url'} | {f[0] for f in FIELDS if f[2] == 'secret'}


def settings_export():
    return {k: v for k, v in load_settings().items() if k not in EXPORT_SKIP}


def settings_import(data):
    """Known keys only; tokens, password and notification URL are never imported. Returns the number of values taken."""
    known = {f[0] for f in FIELDS} | {'schedule_enabled', 'schedule_mode', 'schedule_days', 'schedule_time', 'schedule_cron', 'notify_on', 'scan_form'}
    s = load_settings(); n = 0
    for k, v in (data or {}).items():
        if k in known and k not in EXPORT_SKIP: s[k] = v; n += 1
    save_settings(s)
    return n


def lock_report():
    """Lock tags per book (from the last check): {tag: [(key, title)]} - the locks are respected everywhere, this only shows them."""
    out = {}
    for k, r in results().items():
        for t in r.get('locks') or []: out.setdefault(t, []).append((k, r.get('title')))
    return dict(sorted(out.items()))


def same_asin_groups(s=None):
    """Books that share ONE ASIN (last results), checked against ABS (user question 2026-11-09: 'do I have these twice?'): books that
    are gone from ABS are left out, every book gets its folder, runtime and size, and the group a verdict:
    'same_folder' (ABS lists one folder twice), 'copy' (two folders, same runtime: the audiobook is stored twice),
    'editions' (runtimes differ: two recordings share one ASIN, one ASIN is wrong). Show only."""
    by = {}
    for k, r in results().items():
        if r.get('asin') and r.get('status') != 'ignored': by.setdefault(r['asin'], []).append({'key': k, 'title': r.get('title'), 'author': r.get('author')})
    out = []
    for a, v in by.items():
        if len(v) < 2: continue
        if s is not None:
            for b in v:
                try:
                    resp = abs_get(s, f"/api/items/{b['key'].split('_', 1)[1]}?expanded=1", timeout=10)
                    if resp.status_code == 404: b['gone'] = True; continue
                    it = resp.json(); m = it.get('media') or {}
                    b.update(path=it.get('relPath') or it.get('path') or '', minutes=round((m.get('duration') or 0) / 60), size_mb=round((it.get('size') or m.get('size') or 0) / 1e6))
                except Exception: pass
            v = [b for b in v if not b.get('gone')]
            if len(v) < 2: continue
        paths, mins = {b.get('path') for b in v if b.get('path')}, [b['minutes'] for b in v if b.get('minutes')]
        verdict = None
        if len(paths) == 1 and len(mins) == len(v): verdict = 'same_folder'
        elif mins and len(mins) == len(v): verdict = 'copy' if max(mins) - min(mins) <= 2 else 'editions'
        out.append([a, v, verdict])
    return sorted(out, key=lambda x: x[1][0]['title'] or '')


def isbn_valid(v):
    """ISBN-10 / ISBN-13 checksum (an ASIN is never an ISBN)."""
    v = re.sub(r'[\s-]', '', str(v or '')).upper()
    if re.fullmatch(r'\d{13}', v): return sum(int(c) * (3 if i % 2 else 1) for i, c in enumerate(v)) % 10 == 0
    if re.fullmatch(r'\d{9}[\dX]', v): return sum((10 - i) * (10 if c == 'X' else int(c)) for i, c in enumerate(v)) % 11 == 0
    return False


def manual_fields():
    """Fields the user changed in ABS after our last write (change monitoring): never overwritten again. Newest first."""
    d = _load_json(os.path.join(CONFIG_DIR, 'manual_fields.json'), {})
    out = [{'key': k, 'field': f, **v} for k, fs in d.items() for f, v in (fs or {}).items()]
    return sorted(out, key=lambda x: x.get('ts') or '', reverse=True)


def check_password(s, pw): return check_password_hash(s['password_hash'], pw or '')


def set_password(s, pw):
    s['password_hash'] = generate_password_hash(pw); s['must_change_password'] = False; save_settings(s)


def coerce(ftype, value):
    if ftype == 'bool': return value in (True, 'true', 'on', '1', 'yes')
    if ftype == 'int': return int(float(value or 0))
    if ftype == 'float': return float(value or 0)
    return (value or '').strip() if isinstance(value, str) else value


def script_env(s, extra=None):
    env = dict(os.environ)
    for k in SCRIPT_KEYS:
        v = s.get(k, DEFAULTS.get(k))
        env[k] = ('true' if v else 'false') if isinstance(v, bool) else str(v)
    env['DATA_DIR'] = CONFIG_DIR
    env['PYTHONUNBUFFERED'] = '1'
    env.update({k: str(v) for k, v in (extra or {}).items()})
    return env


# ---------------------------------------------------------------- ABS helpers
def abs_get(s, path, timeout=10):
    return requests.get(f"{s['ABS_URL'].rstrip('/')}{path}", headers={'Authorization': f"Bearer {s['API_TOKEN']}"}, timeout=timeout)


COVER_DIR = os.path.join(CONFIG_DIR, 'cache', 'covers')


_SERIES_CACHE = {'ts': 0, 'data': None}
MISSING_HIDDEN_FILE = os.path.join(CONFIG_DIR, 'missing_hidden.json')


def missing_hidden(): return _load_json(MISSING_HIDDEN_FILE, {'series': [], 'volumes': []})


def set_missing_hidden(kind, ident, hide=True):
    h = missing_hidden(); lst = set(h.get(kind) or [])
    (lst.add if hide else lst.discard)(ident); h[kind] = sorted(lst); _save_json(MISSING_HIDDEN_FILE, h)
    _SERIES_CACHE['data'] = None


def library_series(force=False):
    """All series of the configured libraries with their books (ABS series endpoint, cached 5 min) plus the missing-volume state:
    [{'name', 'lib', 'author', 'books': [{'id','key','title','num','lang'}], 'langs', 'dups', 'missing_rel', 'missing_up', 'known'}]."""
    import time
    if not force and _SERIES_CACHE['data'] is not None and time.time() - _SERIES_CACHE['ts'] < 300: return _SERIES_CACHE['data']
    s = load_settings()
    rep = _load_json(os.path.join(REPORT_DIR, 'missing_in_series.json'), {})
    hid = missing_hidden(); hid_s, hid_v = set(hid.get('series') or []), set(hid.get('volumes') or [])
    miss = {(m.get('lib'), m.get('series')): dict(m, missing=[x for x in m.get('missing') or [] if x.get('asin') not in hid_v])
            for m in rep.get('series') or [] if f"{m.get('lib')}|{m.get('series')}" not in hid_s}
    known = set(rep.get('checked') or [])
    out = []
    for lib in [x.strip() for x in str(s.get('LIBRARY_IDS') or '').split(',') if x.strip()]:
        try: res = abs_get(s, f'/api/libraries/{lib}/series?limit=5000&minified=1', timeout=60).json().get('results') or []
        except Exception: continue
        for se in res:
            name = se.get('name') or ''
            books = []
            for b in se.get('books') or []:
                md = (b.get('media') or {}).get('metadata') or {}
                m_ = re.search(re.escape(name) + r' #(\S+?)(?:,|$)', md.get('seriesName') or '')
                books.append({'id': b.get('id'), 'key': f"{lib}_{b.get('id')}", 'title': md.get('title') or '?', 'num': m_.group(1) if m_ else '',
                              'lang': md.get('language') or '', 'author': (md.get('authorName') or '').split(',')[0].strip()})
            def sk(b):
                try: return (0, float(b['num']))
                except ValueError: return (1, 0)
            books.sort(key=sk)
            nums = {}
            for b in books:
                if b['num']: nums.setdefault((b['num'], b['lang']), set()).add(b['title'].lower())
            mm = miss.get((lib, name)) or {}
            out.append({'name': name, 'lib': lib, 'author': books[0]['author'] if books else '', 'books': books,
                        'langs': sorted({b['lang'] for b in books if b['lang']}), 'mixed': len({b['lang'] for b in books if b['lang']}) > 1, 'dups': sorted(n for (n, _l), t in nums.items() if len(t) > 1),
                        'missing_rel': sum(1 for x in mm.get('missing') or [] if not x.get('upcoming')),
                        'missing_up': sum(1 for x in mm.get('missing') or [] if x.get('upcoming')), 'known': name in known or bool(mm),
                        'store': mm.get('store') or 'www.audible.com',
                        # books and missing volumes in one list, in number order: the series page shows the gaps where they are
                        'entries': sorted([dict(b, kind='book') for b in books] + [dict(x, kind='missing') for x in mm.get('missing') or [] if x.get('asin') or x.get('title')],
                                          key=lambda e: sk({'num': str(e.get('num') or '')}))})
    out.sort(key=lambda x: x['name'].lower())
    _SERIES_CACHE.update(ts=time.time(), data=out)
    return out


LANG_NAMES = {'english': 'English', 'german': 'German', 'french': 'French', 'spanish': 'Spanish', 'italian': 'Italian', 'japanese': 'Japanese'}
DETAIL_FIELDS = ['title', 'subtitle', 'authors', 'narrators', 'series', 'language', 'publisher', 'publishedYear', 'genres', 'isbn', 'asin', 'description']


def book_detail(key, domain=None):
    """Book page: every ABS field next to Audible's value for the ASIN (Audible answer cached 1 day). -> (rows, tags, item) or (None, ...)."""
    import time
    s = load_settings(); iid = key.split('_', 1)[-1]
    if not s.get('ABS_URL') or not s.get('API_TOKEN'): return None, [], {}
    try: it = abs_get(s, f'/api/items/{iid}?expanded=1', timeout=15).json()
    except Exception: return None, [], {}
    if not isinstance(it, dict) or not it.get('media'): return None, [], {}
    md = (it.get('media') or {}).get('metadata') or {}
    tags = sorted(t for t in (it.get('media') or {}).get('tags') or [] if t.startswith('lock_'))
    aud = {}
    if md.get('asin'):
        cache = os.path.join(CONFIG_DIR, 'cache', f"aud_{md['asin']}.json")
        if os.path.exists(cache) and time.time() - os.path.getmtime(cache) < 86400: aud = _load_json(cache, {})
        else:
            for dom in ([domain] if domain else []) + ['www.audible.com', 'www.audible.de', 'www.audible.co.uk']:
                try:
                    r = requests.get(f"https://{dom.replace('www.', 'api.')}/1.0/catalog/products/{md['asin']}",
                                     params={'response_groups': 'product_desc,product_attrs,contributors,series,category_ladders,product_extended_attrs'},
                                     headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}, timeout=15)
                    p = r.json().get('product') if r.status_code == 200 else None
                except Exception: p = None
                if p and p.get('title'):
                    aud = {'title': p.get('title'), 'subtitle': p.get('subtitle'),
                           'authors': [a['name'] for a in p.get('authors') or [] if not re.search(r' - (translator|illustrator|editor|introduction|foreword|afterword|Übersetzer)', a.get('name') or '', re.I)],
                           'narrators': [n['name'] for n in p.get('narrators') or []],
                           'series': [{'name': x.get('title'), 'sequence': x.get('sequence') or None} for x in p.get('series') or [] if x.get('title')],
                           'language': LANG_NAMES.get(str(p.get('language') or '').lower(), p.get('language')), 'publisher': p.get('publisher_name'),
                           'publishedYear': (p.get('release_date') or '')[:4] or None, 'asin': md['asin'], 'description': p.get('publisher_summary'), 'store': dom}
                    os.makedirs(os.path.dirname(cache), exist_ok=True); json.dump(aud, open(cache, 'w', encoding='utf-8'), ensure_ascii=False); break
    cur = {'title': md.get('title'), 'subtitle': md.get('subtitle'), 'authors': [a.get('name') for a in md.get('authors') or []], 'narrators': md.get('narrators') or [],
           'series': [{'name': x.get('name'), 'sequence': x.get('sequence')} for x in md.get('series') or []], 'language': md.get('language'), 'publisher': md.get('publisher'),
           'publishedYear': md.get('publishedYear'), 'genres': md.get('genres') or [], 'isbn': md.get('isbn'), 'asin': md.get('asin'), 'description': md.get('description')}
    norm = lambda v: json.dumps(v, sort_keys=True, ensure_ascii=False).lower() if v not in (None, '', []) else ''
    rows = []
    for f in DETAIL_FIELDS:
        a = aud.get(f) if f in aud else None
        lock = 'lock_title' if f == 'subtitle' else ('lock_year' if f == 'publishedYear' else f'lock_{f}')
        rows.append({'field': f, 'abs': cur.get(f), 'aud': a, 'same': norm(cur.get(f)) == norm(a) or a in (None, '', []),
                     'can_apply': f in aud and a not in (None, '', []) and f not in ('asin',), 'lock': lock, 'locked': lock in tags or 'lock_all' in tags or 'lock_metadata' in tags})
    return rows, tags, it


def set_lock(key, lock, on):
    """Add/remove a lock_<field> tag on the book (ABS media tags)."""
    s = load_settings(); iid = key.split('_', 1)[-1]
    it = abs_get(s, f'/api/items/{iid}', timeout=15).json()
    tags = list((it.get('media') or {}).get('tags') or [])
    tags = sorted(set(tags) | {lock}) if on else [t for t in tags if t != lock]
    r = requests.patch(f"{s['ABS_URL'].rstrip('/')}/api/items/{iid}/media", json={'tags': tags}, headers={'Authorization': f"Bearer {s['API_TOKEN']}"}, timeout=15)
    return r.status_code == 200


def cover_file(kind, ident, max_age, full=False):
    """Cover image for the web UI, cached on disk (the ABS token never reaches the browser).
    kind 'abs': the book's current ABS cover (ident = item id, 1 h); 'audible': the cover of an Audible product (ident = ASIN, 7 days).
    Returns (path, mimetype) or (None, None)."""
    import time
    if not re.fullmatch(r'[A-Za-z0-9_-]{6,64}', ident or ''): return None, None
    os.makedirs(COVER_DIR, exist_ok=True)
    p = os.path.join(COVER_DIR, f"{kind}_{ident}{'_full' if full else ''}.img")
    if os.path.exists(p) and time.time() - os.path.getmtime(p) < max_age and os.path.getsize(p) > 0: return p, 'image/jpeg'
    try:
        if kind == 'abs':
            r = abs_get(load_settings(), f'/api/items/{ident}/cover' + ('' if full else '?width=400'), timeout=20)
        else:
            size = '1215' if full else '500'
            api = requests.get(f'https://api.audible.com/1.0/catalog/products/{ident}', params={'response_groups': 'media', 'image_sizes': size},
                               headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}, timeout=15)
            url = ((api.json().get('product') or {}).get('product_images') or {}).get(size) if api.status_code == 200 else None
            if not url:
                for dom in ('de', 'co.uk'):  # other stores (German books)
                    api = requests.get(f'https://api.audible.{dom}/1.0/catalog/products/{ident}', params={'response_groups': 'media', 'image_sizes': size},
                                       headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}, timeout=15)
                    url = ((api.json().get('product') or {}).get('product_images') or {}).get(size) if api.status_code == 200 else None
                    if url: break
            r = requests.get(url, timeout=15) if url else None
        if r is None or r.status_code != 200 or not r.content: return (p, 'image/jpeg') if os.path.exists(p) else (None, None)
        with open(p + '.tmp', 'wb') as f: f.write(r.content)
        os.replace(p + '.tmp', p)
        return p, r.headers.get('Content-Type') or 'image/jpeg'
    except Exception:
        return (p, 'image/jpeg') if os.path.exists(p) else (None, None)


def _t(text, **kw): return text % kw if kw else text  # default (English) for the message callbacks below


def abs_test(s, t=_t):
    try:
        r = abs_get(s, '/api/libraries')
        if r.status_code == 200:
            return True, t('Connected. %(n)s libraries found.', n=len(r.json().get('libraries', [])))
        return False, t('ABS answers with HTTP %(c)s', c=r.status_code) + (' (' + t('wrong token?') + ')' if r.status_code == 401 else '')
    except requests.RequestException as e:
        return False, t('No connection: %(e)s', e=e)


def abs_libraries(s):
    try:
        r = abs_get(s, '/api/libraries')
        return [{'id': l['id'], 'name': l.get('name', l['id']), 'type': l.get('mediaType')} for l in r.json().get('libraries', [])]
    except (requests.RequestException, ValueError, KeyError):
        return []


def abs_healthy(s):
    if not s.get('ABS_URL'): return False
    try: return requests.get(f"{s['ABS_URL'].rstrip('/')}/ping", timeout=5).status_code == 200
    except requests.RequestException: return False


def hardcover_test(token, t=_t):
    try:
        r = requests.post('https://api.hardcover.app/v1/graphql', json={'query': '{ me { username } }'},
                          headers={'Authorization': token if token.lower().startswith('bearer') else f'Bearer {token}'}, timeout=15)
        if r.status_code == 200 and 'errors' not in r.json():
            me = r.json().get('data', {}).get('me')
            name = (me[0] if isinstance(me, list) and me else me or {}).get('username', '?')
            return True, t('Connected as %(n)s.', n=name)
        return False, t('Hardcover answers with HTTP %(c)s', c=r.status_code) + ': ' + r.text[:150]
    except (requests.RequestException, ValueError) as e:
        return False, t('No connection: %(e)s', e=e)


# ---------------------------------------------------------------- notifications
NOTIFY_EVENTS = ['scan_done', 'errors', 'source_off', 'review_new', 'volumes_new', 'weekly']


def notify_events(s):
    """Chosen notification events; the old 'notify_on' (errors / always) maps onto them."""
    ev = s.get('notify_events')
    if isinstance(ev, list): return set(ev)
    return {'errors', 'source_off'} | ({'scan_done'} if s.get('notify_on') == 'always' else set())


def notify(s, title, body, is_error=None, force=False):
    """Send to every Apprise URL (several targets: comma / space / new line separated)."""
    urls = [u for u in re.split(r'[\s,]+', (s.get('apprise_url') or '').strip()) if u]
    # a plain ntfy web address ('https://ntfy.example.com/topic') is what people copy from the browser: Apprise wants ntfys://
    urls = [re.sub(r'^http(s?)://', lambda m: 'ntfys://' if m.group(1) else 'ntfy://', u) if re.match(r'https?://[^/]*ntfy[^/]*/[^/]+', u) else u for u in urls]
    if not urls: return False
    if not force and is_error is not None and not (is_error and 'errors' in notify_events(s)) and 'scan_done' not in notify_events(s): return False
    try:
        import apprise
        a = apprise.Apprise()
        for u in urls: a.add(u)
        return bool(a.notify(title=title, body=body))
    except Exception:
        return False


def ntfy_check(url, title):
    """After a test: did the ntfy server really store the message? Apprise reports success for an http:// address that is only
    redirected to https (nothing stored) - so the topic is read back. Returns None (no ntfy / not readable), True or a reason."""
    m = re.match(r'(ntfys?)://(?:([^@/]+)@)?([^/]+)/([^?\s,]+)', url or '')
    if not m: return None
    scheme, cred, host, topic = ('https' if m.group(1) == 'ntfys' else 'http'), m.group(2) or '', m.group(3), m.group(4)
    auth = {'headers': {'Authorization': f'Bearer {cred}'}} if cred.startswith('tk_') else {'auth': tuple(cred.split(':', 1))} if ':' in cred else {}
    try:
        if scheme == 'http':  # a GET only reads - it shows the redirect without publishing anything
            h = requests.get(f"http://{host}/{topic}/json", params={'poll': 1, 'since': '1s'}, timeout=10, allow_redirects=False, **auth)
            if h.status_code in (301, 302, 307, 308): return 'redirect'
        g = requests.get(f"{scheme}://{host}/{topic}/json", params={'poll': 1, 'since': '2m'}, timeout=10, **auth)
        if g.status_code in (401, 403): return 'not_stored'  # no read permission -> most likely no write permission either
        if g.status_code != 200: return None
        return True if any(json.loads(l).get('title') == title for l in g.text.splitlines() if l.strip()) else 'not_stored'
    except Exception:
        return None


def notify_run(s, run):
    """After a full / library scan: one message with the lines of every chosen event that happened."""
    ev, summ = notify_events(s), run.get('summary') or ''
    lines = []
    if 'errors' in ev and run['status'] != 'ok': lines.append(f"❌ {run.get('subject') or 'Error'}")
    if 'source_off' in ev and 'switched off' in summ or ('source_off' in ev and ' off: ' in summ):
        lines += [p.strip() for p in summ.split('|') if ' off: ' in p]
    if 'volumes_new' in ev and 'New volumes' in summ: lines += [p.strip() for p in summ.split('|') if 'New volumes' in p]
    if 'review_new' in ev:
        n = sum(1 for x in review_queue() if x.get('created_run', x.get('run')) == run.get('id') and x.get('status', 'open') == 'open')  # 'run' is refreshed for old ones
        if n: lines.append(f"📝 {n} new entr{'y' if n == 1 else 'ies'} on the review list")
    if 'scan_done' in ev: lines.append(summ or run['status'])
    if lines: notify(s, run.get('subject') or f"ABS Ratings: {run['label']}", '\n'.join(dict.fromkeys(lines)), force=True)


def weekly_summary(s):
    """Monday morning: changes of the last 7 days, open review entries, missing volumes (once per week)."""
    if 'weekly' not in notify_events(s): return
    now = datetime.now(); wk = now.strftime('%G-W%V')
    if now.weekday() != 0 or now.hour < 8 or s.get('weekly_sent') == wk: return
    since = (now - timedelta(days=7)).strftime('%Y-%m-%d')
    ch, books = 0, set()
    try:
        with open(os.path.join(CONFIG_DIR, 'changes.jsonl'), encoding='utf-8') as f:
            for l in f:
                if '"dry_run": true' in l: continue
                try: e = json.loads(l)
                except ValueError: continue
                if str(e.get('ts') or '')[:10] >= since and e.get('field') != 'description': ch += 1; books.add(e.get('key'))
    except OSError: pass
    rep = _load_json(os.path.join(REPORT_DIR, 'missing_in_series.json'), {})
    rel = sum(1 for x in rep.get('series') or [] for m in x.get('missing') or [] if not m.get('upcoming'))
    body = f"Last 7 days: {ch} changes in {len(books)} books\nReview list: {len(open_queue())} open\nMissing volumes (released): {rel}"
    if notify(s, 'ABS Ratings: weekly summary', body, force=True):
        s2 = load_settings(); s2['weekly_sent'] = wk; save_settings(s2)


# ---------------------------------------------------------------- runs
def _load_json(path, default):
    try:
        with open(path, encoding='utf-8') as f: return json.load(f)
    except (OSError, ValueError): return default


def _save_json(path, data):
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f: json.dump(data, f, indent=1, ensure_ascii=False)
    os.replace(tmp, path)


def parse_env_summary():
    """last_run.env written by the script: ABS_SUBJECT='..' etc."""
    out = {}
    try:
        with open(os.path.join(CONFIG_DIR, 'last_run.env'), encoding='utf-8') as f:
            for line in f:
                if m := re.match(r"(\w+)='(.*)'$", line.strip()): out[m.group(1)] = m.group(2)
    except OSError: pass
    return out


RUN_OVERRIDABLE = {'BATCH_SIZE', 'REFRESH_DAYS', 'HARDCOVER_ENABLED', 'GOODREADS_ENABLED', 'SERIES_MERGE', 'AUTHOR_NORMALIZE', 'UPDATE_COVERS', 'REMOVE_ROLE_CREDITS', 'REMOVE_EMPTY_AUTHORS', 'ABS_BACKUP', 'REMOVE_RATINGS'}  # start dialog
KIND_LABELS = {'full': 'Scan', 'series': 'Series & report only', 'item': 'Single book', 'undo': 'Undo', 'apply': 'Apply test run'}


class RunManager:
    """Starts at most one script process at a time and keeps a small run history (webui_runs.json)."""

    def __init__(self):
        self.proc, self.current = None, None
        self.lock = threading.Lock()

    def runs(self): return _load_json(RUNS_FILE, [])

    def get(self, run_id): return next((r for r in self.runs() if r['id'] == run_id), None)

    def running(self): return self.proc is not None and self.proc.poll() is None

    def _save_run(self, run):
        with _lock:
            runs = [r for r in self.runs() if r['id'] != run['id']]
            runs.insert(0, run)
            _save_json(RUNS_FILE, runs[:300])

    def start(self, kind='full', dry_run=None, only_items=None, trigger='manual', label=None, undo_args=None, run_settings=None):
        """run_settings: values for THIS run only (start dialog), applied on top of the saved settings, never saved."""
        with self.lock:
            if self.running(): return None, 'A scan is already running.'
            s = load_settings()
            if run_settings: s = {**s, **{k: v for k, v in run_settings.items() if k in RUN_OVERRIDABLE}}
            if not s.get('ABS_URL') or not s.get('API_TOKEN'): return None, 'ABS URL and token are missing (settings).'
            run_id = datetime.now().strftime('%Y%m%d-%H%M%S')
            known = {r.get('id') for r in self.runs()[:20]}
            while run_id in known:  # two quick runs in the same second (review list clicks): never share logs / the run entry
                time.sleep(1); run_id = datetime.now().strftime('%Y%m%d-%H%M%S')
            dry = s['DRY_RUN'] if dry_run is None else dry_run
            if kind == 'apply': dry = False
            extra = {'RUN_ID': run_id, 'DRY_RUN': 'true' if dry else 'false'}
            if kind == 'series': extra['SERIES_ONLY'] = 'true'
            if kind == 'item' or (kind == 'full' and only_items): extra['ONLY_ITEMS'] = ','.join(only_items or [])
            env = script_env(s, extra)
            if (run_settings or {}).get('REMOVE_RATINGS'): env['REMOVE_RATINGS'] = run_settings['REMOVE_RATINGS']  # remove-ratings task
            if (run_settings or {}).get('REWRITE_RATING_SYMBOLS'): env['REWRITE_RATING_SYMBOLS'] = 'true'  # rewrite all rating symbols
            os.makedirs(LOG_DIR, exist_ok=True)
            if kind == 'undo':
                cmd = [sys.executable, '-u', os.path.join(APP_DIR, 'abs_undo.py')] + list(undo_args or [])
                log_path = os.path.join(LOG_DIR, f"undo_{run_id}.log")
            elif kind == 'apply':  # apply a dry run (abs_apply.py checks every value against ABS before writing)
                env['DRY_RUN'] = 'false'
                cmd = [sys.executable, '-u', os.path.join(APP_DIR, 'abs_apply.py')] + list(undo_args or [])
                log_path = os.path.join(LOG_DIR, f"run_{run_id}.log")
            else:
                cmd = [sys.executable, '-u', os.path.join(APP_DIR, 'abs_ratings.py')]
                log_path = os.path.join(LOG_DIR, f"run_{run_id}.log")
            out = open(os.path.join(LOG_DIR, f"console_{run_id}.log"), 'w', encoding='utf-8')
            self.proc = subprocess.Popen(cmd, cwd=APP_DIR, env=env, stdout=out, stderr=subprocess.STDOUT,
                                         start_new_session=True)
            run = {'id': run_id, 'kind': kind, 'label': label or KIND_LABELS.get(kind, kind), 'dry_run': dry,
                   'trigger': trigger, 'items': (only_items or [])[:50], 'n_items': len(only_items or []), 'run_settings': run_settings or {}, 'start': datetime.now().isoformat(timespec='seconds'),
                   'end': None, 'status': 'running', 'log': os.path.basename(log_path),
                   'console': f"console_{run_id}.log", 'summary': ''}
            self.current = run
            self._save_run(run)
            threading.Thread(target=self._wait, args=(run, out, s), daemon=True).start()
            return run_id, None

    def _wait(self, run, out, s):
        rc = self.proc.wait()
        out.close()
        # last_run.env is written by abs_ratings.py only: an apply / undo run, or a scan that died before writing it, must not show
        # (and be marked 'error' by) the summary of an EARLIER scan
        envp = os.path.join(CONFIG_DIR, 'last_run.env')
        try: fresh = os.path.getmtime(envp) >= datetime.fromisoformat(run['start']).timestamp() - 1
        except (OSError, ValueError, KeyError): fresh = False
        summary = parse_env_summary() if run['kind'] in ('full', 'series', 'item') and fresh else {}
        if not os.path.exists(os.path.join(LOG_DIR, run['log'])): run['log'] = run['console']
        run.update(end=datetime.now().isoformat(timespec='seconds'), returncode=rc,
                   status='stopped' if rc in (-15, -9) else ('ok' if rc == 0 and summary.get('ABS_ICON') != 'alert' else 'error'),
                   summary=summary.get('ABS_REPORT_BODY', ''), subject=summary.get('ABS_SUBJECT', ''))
        if run['kind'] == 'undo' and rc == 0: run['status'] = 'ok'
        self._save_run(run)
        self.current = None
        if run['kind'] in ('full', 'series'):
            notify_run(s, run)

    def stop(self):
        if self.running():
            try: os.killpg(self.proc.pid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError): self.proc.terminate()
            return True
        return False


RUNNER = RunManager()


# ---------------------------------------------------------------- scheduler
def next_run_time(s, now=None):
    now = now or datetime.now()
    if not s.get('schedule_enabled'): return None
    if s.get('schedule_mode') == 'cron':
        try:
            from croniter import croniter
            return croniter(s.get('schedule_cron') or '30 2 * * *', now).get_next(datetime)
        except Exception:
            return None
    days = [int(d) for d in (s.get('schedule_days') or [])]
    try: hh, mm = [int(x) for x in (s.get('schedule_time') or '02:30').split(':')]
    except ValueError: return None
    for add in range(0, 8):
        d = now + timedelta(days=add)
        cand = d.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if cand > now and cand.weekday() in days: return cand
    return None


def scheduler_loop():
    planned = None
    while True:
        try:
            s = load_settings()
            nxt = next_run_time(s)
            if planned and datetime.now() >= planned:
                # switched off just before the slot: no run; previous run still busy: this slot is skipped
                if s.get('schedule_enabled') and not RUNNER.running(): RUNNER.start('full', trigger='schedule', label='Scheduled scan')
                planned = None
            quick_poll(s)
            weekly_summary(s)
            if planned is None or planned > datetime.now():  # a changed time / day / cron moves the planned slot (later too, not only earlier)
                planned = nxt
            cleanup_logs(s)
        except Exception:
            pass
        time.sleep(20)


_quick = {'t': 0}
def quick_poll(s):
    """Quick mode: every QUICK_MODE_MINUTES look for books that were never checked and process them at once (1 ABS listing request)."""
    every = int(s.get('QUICK_MODE_MINUTES') or 0)
    if every <= 0 or time.time() - _quick['t'] < every * 60 or RUNNER.running(): return
    _quick['t'] = time.time()
    hist = _load_json(os.path.join(CONFIG_DIR, 'rating_history.json'), {})
    failed = _load_json(os.path.join(CONFIG_DIR, 'failed_history.json'), {})
    new = []
    for lib in [l.strip() for l in (s.get('LIBRARY_IDS') or '').replace(';', ',').split(',') if l.strip()]:
        try:
            for it in abs_get(s, f'/api/libraries/{lib}/items', timeout=60).json().get('results') or []:
                k = f"{lib}_{it['id']}"
                if k not in hist and k not in failed: new.append(k)
        except (requests.RequestException, ValueError): return
    if new: RUNNER.start('full', trigger='quick', only_items=new[:int(s.get('BATCH_SIZE') or 300)], label=f'Quick mode: {len(new)} new book(s)')


_last_cleanup = {'t': 0}
def cleanup_logs(s):
    if time.time() - _last_cleanup['t'] < 3600: return
    _last_cleanup['t'] = time.time()
    limit = time.time() - int(s.get('LOG_RETENTION_DAYS') or 30) * 86400
    # logs, planned changes of old test runs and the helper files of web UI actions (selections, undo lists). The journal
    # (changes.jsonl) and the cover backups stay: undo needs them.
    for d, pat in ((LOG_DIR, r'.*\.log$'), (REPORT_DIR, r'(planned_changes_.*\.jsonl|apply_select_.*\.json|undo_ids_.*\.json)$')):
        try:
            for name in os.listdir(d):
                p = os.path.join(d, name)
                if re.match(pat, name) and name != 'debug_latest.log' and os.path.getmtime(p) < limit: os.remove(p)
        except OSError: pass
    import shutil  # old per-run debug folders of the first script version (debug_logs/<date>_<asin>/)
    dd = os.path.join(CONFIG_DIR, 'debug_logs')
    try:
        for name in os.listdir(dd):
            p = os.path.join(dd, name)
            if os.path.isdir(p) and os.path.getmtime(p) < limit: shutil.rmtree(p, ignore_errors=True)
    except OSError: pass


def start_scheduler():
    threading.Thread(target=scheduler_loop, daemon=True).start()


# ---------------------------------------------------------------- data readers
def results(): return _load_json(os.path.join(CONFIG_DIR, 'results.json'), {})
def overrides(): return _load_json(os.path.join(CONFIG_DIR, 'overrides.json'), {})
def merges(): return _load_json(os.path.join(CONFIG_DIR, 'merges.json'), {})
def review(): return _load_json(os.path.join(CONFIG_DIR, 'merge_review.json'), {'series': [], 'authors': []})
def decisions(): return _load_json(os.path.join(CONFIG_DIR, 'merge_decisions.json'), {})
FIELD_LABELS = {'asin': 'ASIN', 'cover': 'Cover', 'series': 'Series', 'narrators': 'Narrators', 'description': 'Description', 'authors': 'Author',
                'genres': 'Genres', 'publisher': 'Publisher', 'year': 'Year', 'isbn': 'ISBN', 'language': 'Language', 'ratings': 'Ratings'}

def coverage():
    """Share of books that HAVE each field, from library_report.json (written by the series/report task). [(label, have, total, percent, missing titles)]"""
    rep = _load_json(os.path.join(REPORT_DIR, 'library_report.json'), {})
    total = rep.get('books')
    if isinstance(total, (list, dict)): total = len(total)
    total = int(total or 0); rows = []
    by_title = {}
    for k_, r_ in results().items(): by_title.setdefault((r_.get('title') or '').strip(), k_)
    linked = lambda titles: [(t, by_title.get((t or '').strip())) for t in sorted(titles)]  # (title, book key) - the status page links them
    for k, titles in (rep.get('missing') or {}).items():
        have = max(total - len(titles), 0)
        rows.append((FIELD_LABELS.get(k, k), have, total, round(100 * have / total, 1) if total else 0, linked(titles)[:300]))
    # every metadata field the tool works on; a field the last report does not know yet is shown as 'after the next scan'
    have_keys = set((rep.get('missing') or {}).keys())
    for k in ('asin', 'title', 'subtitle', 'authors', 'narrators', 'series', 'genres', 'publisher', 'year', 'isbn', 'language', 'description', 'cover'):
        if k not in have_keys and k not in ('title', 'subtitle'): rows.append((FIELD_LABELS.get(k, k), None, total, None, []))
    # ratings per source from the last check of every book (results.json)
    res = results(); n = len(res)
    if n:
        for label, pred in (('Audible ratings', lambda r: r.get('status') == 'ok'), ('Goodreads ratings', lambda r: bool(r.get('goodreads'))),
                            ('Hardcover', lambda r: bool(r.get('hardcover')))):
            miss = sorted(((r.get('title') or '?'), k_) for k_, r in res.items() if not pred(r))
            rows.append((label, n - len(miss), n, round(100 * (n - len(miss)) / n, 1), miss[:300]))
    rows.sort(key=lambda r: (r[3] is None, r[3] or 0))
    _coverage_trend(rows, rep.get('generated', '') or datetime.now().strftime('%Y-%m-%d'))
    return rows, rep.get('generated', '')


def _coverage_trend(rows, stamp):
    """Keep one coverage snapshot per report/day in coverage_history.json (for the trend on the status page)."""
    path = os.path.join(CONFIG_DIR, 'coverage_history.json')
    hist = _load_json(path, [])
    snap = {'ts': stamp, 'pct': {r[0]: r[3] for r in rows if r[3] is not None}}
    if not hist or hist[-1].get('ts') != stamp:
        hist.append(snap); _save_json(path, hist[-120:])
    elif hist[-1].get('pct') != snap['pct']:
        hist[-1] = snap; _save_json(path, hist[-120:])


HEALTH_LABELS = {'no_asin': 'Without ASIN', 'shared_asin': 'ASIN on several books', 'wrong_store': 'ASIN not from the preferred store',
                 'non_latin_series': 'Series name not English/German', 'junk_number': 'Series number on 3+ different books',
                 'folder_conflict': 'Book folder names another volume', 'case_variants': 'Series spelled in several ways'}

def health():
    """Library checks of the last report (abs_series.library_health): [(key, label, entries)], worst first."""
    h = (_load_json(os.path.join(REPORT_DIR, 'library_report.json'), {}) or {}).get('health')
    if h is None: return None
    return [(k, HEALTH_LABELS[k], h.get(k) or []) for k in HEALTH_LABELS]

def coverage_trend():
    return _load_json(os.path.join(CONFIG_DIR, 'coverage_history.json'), [])

def api_status():
    """Hardcover daily limit / Goodreads state from the newest run logs, cache sizes, open review items."""
    st = {'hc_left': None, 'hc_when': '', 'gr': 'unknown', 'caches': {}, 'queue': len(open_queue())}
    try:
        logs = sorted((f for f in os.listdir(LOG_DIR) if f.startswith('run_') and f.endswith('.log')), reverse=True)
    except OSError: logs = []
    for f in logs[:8]:
        try: txt = open(os.path.join(LOG_DIR, f), encoding='utf-8', errors='replace').read()
        except OSError: continue
        if st['hc_left'] is None and (m := re.findall(r'daily limit left: (\d+)', txt)):
            st['hc_left'], st['hc_when'] = int(m[-1]), f[4:23].replace('_', ' ')
            st['hc_reset'] = (re.findall(r'limit reset: ([^)]+)\)', txt) or [''])[-1]
        if st['gr'] == 'unknown' and 'Goodreads' in txt:
            st['gr'] = 'blocked (last scan)' if re.search(r'Bot challenge|still blocks|blocks requests', txt) else 'ok (last scan)'
        if st['hc_left'] is not None and st['gr'] != 'unknown': break
    s = load_settings(); st['hc_limit'] = 50000 if s.get('HARDCOVER_PAID') else 5000
    u = _load_json(os.path.join(CONFIG_DIR, 'hardcover_usage.json'), {})
    st['hc_used'] = u.get('requests', 0) if u.get('day') == datetime.utcnow().strftime('%Y-%m-%d') else 0  # own budget, UTC day
    for name, label in (('hardcover_cache.json', 'Hardcover IDs'), ('goodreads_cache.json', 'Goodreads pages'), ('audible_series.json', 'Audible series'),
                        ('audible_authors.json', 'Audible authors'), ('rating_history.json', 'Rating history')):
        d = _load_json(os.path.join(CONFIG_DIR, name), {})
        st['caches'][label] = len(d)
    return st

def review_queue():
    q = _load_json(os.path.join(CONFIG_DIR, 'review_queue.json'), [])
    return q if isinstance(q, list) else []
def queue_decisions(): return _load_json(os.path.join(CONFIG_DIR, 'review_decisions.json'), {})
def queue_id(x): return f"{x.get('kind')}|{x.get('key')}|{x.get('candidate')}"
def open_queue():
    """Uncertain automatic decisions (ASIN / ISBN / title proposals) that wait for the user; decided ones are hidden."""
    d = queue_decisions()
    # ISBN entries WITHOUT a replacement are no decision (older runs put them here; they live in reports/missing_isbn.json now)
    return [dict(x, id=queue_id(x)) for x in review_queue() if queue_id(x) not in d and x.get('status', 'open') == 'open'
            and not (x.get('kind') == 'isbn' and 'no verified replacement' in (x.get('reason') or ''))]


def isbn_report(): return _load_json(os.path.join(REPORT_DIR, 'missing_isbn.json'), [])
def set_queue_decision(qid, action):
    with _lock:
        d = queue_decisions(); d[qid] = {'action': action, 'ts': datetime.now().isoformat(timespec='seconds')}
        _save_json(os.path.join(CONFIG_DIR, 'review_decisions.json'), d)
def history_count(): return len(_load_json(os.path.join(CONFIG_DIR, 'rating_history.json'), {}))


def save_override(key, data):
    with _lock:
        o = overrides(); cur = o.get(key, {})
        cur.update({k: v for k, v in data.items() if v is not None})
        for k in [k for k, v in cur.items() if v in ('', False)]: cur.pop(k)
        if cur: o[key] = cur
        else: o.pop(key, None)
        _save_json(os.path.join(CONFIG_DIR, 'overrides.json'), o)


def set_decision(mid, action, **extra):
    with _lock:
        d = decisions(); d[mid] = {'action': action, 'ts': datetime.now().isoformat(timespec='seconds'), **extra}
        _save_json(os.path.join(CONFIG_DIR, 'merge_decisions.json'), d)
        if action in ('keep', 'undone', 'merge', 'removed'):  # drop from the review list right away
            rv = review()
            for k in ('series', 'authors', 'other'): rv[k] = [x for x in rv.get(k, []) if x.get('id') != mid]
            _save_json(os.path.join(CONFIG_DIR, 'merge_review.json'), rv)


def remove_alias(lib, member):
    path = os.path.join(CONFIG_DIR, 'series_aliases.json')
    with _lock:
        a = _load_json(path, {})
        if a.get(lib, {}).pop(member, None) is not None: _save_json(path, a)


def _undone_ids():
    try:
        with open(os.path.join(CONFIG_DIR, 'undone.jsonl'), encoding='utf-8') as f:
            return {json.loads(l).get('id') for l in f if l.strip()}
    except (OSError, ValueError): return set()


def entry_id(e): return f"{e.get('run')}|{e.get('item')}|{e.get('field')}|{e.get('ts')}"  # same id as abs_undo.py


def history():
    """Applied changes (changes.jsonl) grouped by run, newest first: [{run, start, end, changes, books, undone, fields{field: n}}]."""
    undone, runs = _undone_ids(), {}
    try:
        with open(os.path.join(CONFIG_DIR, 'changes.jsonl'), encoding='utf-8') as f:
            for l in f:
                try: e = json.loads(l)
                except ValueError: continue
                r = runs.setdefault(e.get('run'), {'run': e.get('run'), 'start': e.get('ts'), 'end': e.get('ts'), 'changes': 0, 'books': set(), 'undone': 0, 'fields': {}})
                r['changes'] += 1; r['books'].add(e.get('item')); r['undone'] += entry_id(e) in undone
                r['fields'][e.get('field')] = r['fields'].get(e.get('field'), 0) + 1
                r['start'], r['end'] = min(r['start'] or '', e.get('ts') or ''), max(r['end'] or '', e.get('ts') or '')
    except OSError: pass
    out = sorted(runs.values(), key=lambda r: r['start'] or '', reverse=True)
    for r in out: r['books'] = len(r['books'])
    return out


def history_entries(run_id, q='', field=''):
    """Applied changes of one run with undo state, optionally filtered by book title and field."""
    undone, out = _undone_ids(), []
    try:
        with open(os.path.join(CONFIG_DIR, 'changes.jsonl'), encoding='utf-8') as f:
            for l in f:
                if f'"run": "{run_id}"' not in l: continue
                try: e = json.loads(l)
                except ValueError: continue
                if q and q.lower() not in (e.get('title') or '').lower(): continue
                if field and e.get('field') != field: continue
                e['undone'] = entry_id(e) in undone
                out.append(e)
    except OSError: pass
    return out


def history_search(q='', field='', source='', since='', until='', limit=500):
    """Applied changes across all scans (newest first): filter by book title, field, source (script, review list, manual ...) and date."""
    undone, out = _undone_ids(), []
    try:
        with open(os.path.join(CONFIG_DIR, 'changes.jsonl'), encoding='utf-8') as f: lines = f.readlines()
    except OSError: return [], 0
    total = 0
    for l in reversed(lines):
        try: e = json.loads(l)
        except ValueError: continue
        if e.get('dry_run'): continue
        if q and q.lower() not in (e.get('title') or '').lower(): continue
        if field and e.get('field') != field: continue
        src = str(e.get('source') or '')
        if source == 'script' and (src.startswith(('applied:', 'review', 'manual', 'series_page')) or src == 'review_list'): continue
        if source == 'you' and not (src.startswith(('applied:', 'manual', 'series_page')) or src == 'review_list'): continue
        d = str(e.get('ts') or '')[:10]
        if since and d < since: continue
        if until and d > until: continue
        total += 1
        if len(out) < limit: e['undone'] = entry_id(e) in undone; out.append(e)
    return out, total


def book_changes(key, limit=200):
    """All journal entries of one book: applied changes (changes.jsonl, with undo state) + the newest planned-changes file."""
    out, undone = [], set()
    try:
        with open(os.path.join(CONFIG_DIR, 'undone.jsonl'), encoding='utf-8') as f:
            undone = {json.loads(l).get('id') for l in f if l.strip()}
    except (OSError, ValueError): pass
    planned = sorted(glob.glob(os.path.join(REPORT_DIR, 'planned_changes_*.jsonl')))[-1:]
    for path in [os.path.join(CONFIG_DIR, 'changes.jsonl')] + planned:
        try:
            with open(path, encoding='utf-8') as f:
                for l in f:
                    if key not in l: continue
                    try: e = json.loads(l)
                    except ValueError: continue
                    if e.get('key') != key: continue
                    e['undone'] = f"{e.get('run')}|{e.get('item')}|{e.get('field')}|{e.get('ts')}" in undone
                    out.append(e)
        except OSError: pass
    return sorted(out, key=lambda e: e.get('ts') or '', reverse=True)[:limit]


def run_changes(run_id):
    """All journal entries of one run (applied or planned)."""
    out = []
    for path in (os.path.join(CONFIG_DIR, 'changes.jsonl'), os.path.join(REPORT_DIR, f'planned_changes_{run_id}.jsonl')):
        try:
            with open(path, encoding='utf-8') as f:
                for l in f:
                    if f'"run": "{run_id}"' in l:
                        try: out.append(json.loads(l))
                        except ValueError: pass
        except OSError: pass
    return out


def log_files():
    files = []
    for d, kind in ((LOG_DIR, 'log'), (REPORT_DIR, 'report')):
        try:
            for name in os.listdir(d):
                if kind == 'report' and not (name.startswith('planned_changes_') or name.startswith('library_report')): continue
                p = os.path.join(d, name)
                st = os.stat(p)
                files.append({'name': name, 'dir': kind, 'size': st.st_size, 'mtime': datetime.fromtimestamp(st.st_mtime)})
        except OSError: pass
    return sorted(files, key=lambda x: x['mtime'], reverse=True)


def safe_path(kind, name):
    base = LOG_DIR if kind == 'log' else REPORT_DIR
    p = os.path.realpath(os.path.join(base, name))
    return p if p.startswith(os.path.realpath(base) + os.sep) and os.path.isfile(p) else None


RE_PROG = re.compile(r'\((\d+)/(\d+)\) \[ETA: ([^\]]*)\] (.+?) \[ASIN: ([^\]]*)\]')
_prog_cache = {}

def run_progress(log_path):
    """Live numbers parsed from the run log: position, ETA, current book and counters."""
    try: st = os.stat(log_path)
    except OSError: return {}
    hit = _prog_cache.get(log_path)
    if hit and hit[0] == (st.st_size, st.st_mtime): return hit[1]
    try: txt = open(log_path, encoding='utf-8', errors='replace').read()
    except OSError: return {}
    pos = RE_PROG.findall(txt)
    n, total, eta, title, asin = pos[-1] if pos else (0, 0, '', '', '')
    n, total = int(n), int(total)
    cnt = lambda pat: len(re.findall(pat, txt))
    d = {'n': n, 'total': total, 'pct': round(100 * n / total, 1) if total else 0, 'eta': eta, 'title': title,
         'audible_found': cnt(r'SUCCESS on'), 'no_audible': cnt(r'No Audible data'), 'hardcover': cnt(r'✅ Hardcover via'),
         'goodreads': cnt(r'✅ Goodreads via|Found via cached URL'), 'new_asin': cnt(r'NEW ASIN'), 'reviews': cnt(r'added to the review list|review list\)'),
         'errors': cnt(r' - (?:ERROR|WARNING) - .*(?:Error|Traceback)'), 'goodreads_blocked': bool(re.search(r'Goodreads still blocks|Bot challenge', txt)),
         'titles': cnt(r'-> Title: '), 'series': cnt(r'-> Series: '), 'covers': cnt(r'Cover (?:replaced|added|set)'), 'merges': cnt(r'🔗 MERGE')}
    # speed and expected end: from the timestamps of the progress lines (books FINISHED per minute over the last ~150 books)
    ts = re.findall(r'^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d+ - INFO - \((\d+)/\d+\) \[ETA', txt, re.M)
    if len(ts) >= 5:
        try:
            t = [(datetime.strptime(x, '%Y-%m-%d %H:%M:%S'), int(k)) for x, k in ts[-150:]]
            mins = (t[-1][0] - t[0][0]).total_seconds() / 60
            if mins > 0:
                d['speed'] = round((t[-1][1] - t[0][1]) / mins, 1)
                left = total - n
                if d['speed'] > 0 and left > 0:
                    d['finish'] = (datetime.now() + timedelta(minutes=left / d['speed'])).strftime('%H:%M')
        except ValueError: pass
    _prog_cache[log_path] = ((st.st_size, st.st_mtime), d)
    return d

def read_tail(path, offset=0, max_bytes=400_000):
    try:
        size = os.path.getsize(path)
        if offset == 0 and size > max_bytes: offset = size - max_bytes
        with open(path, 'rb') as f:
            f.seek(offset); data = f.read()
        return data.decode('utf-8', 'replace'), offset + len(data)
    except OSError:
        return '', offset


def zip_logs():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as z:
        for f in log_files():
            p = safe_path(f['dir'], f['name'])
            if p: z.write(p, f"{f['dir']}s/{f['name']}")
    buf.seek(0)
    return buf


# ---------------------------------------------------------------- manual fix input
RE_ASIN = re.compile(r'^[A-Z0-9]{10}$')

def parse_fix_input(text):
    """Detects what was pasted: ASIN, Audible link (any store), Goodreads link or Hardcover link."""
    t = (text or '').strip()
    if not t: return {}
    if RE_ASIN.match(t.upper()) and not t.startswith('http'): return {'asin': t.upper()}
    if 'audible.' in t:
        m = re.search(r'/(?:pd|ac)/(?:[^/?#]*/)?([A-Z0-9]{10})(?:[/?#]|$)', t) or re.search(r'\b(B0[A-Z0-9]{8}|\d{9}[\dX])\b', t)
        store = re.search(r'audible\.([a-z.]+?)(?:/|$)', t)
        if m: return {'asin': m.group(1), 'store': store.group(1) if store else None}
    if 'goodreads.com' in t:
        m = re.search(r'goodreads\.com/book/show/(\d+)', t)
        if m: return {'goodreads': f"https://www.goodreads.com/book/show/{m.group(1)}"}
    if 'hardcover.app' in t:
        m = re.search(r'hardcover\.app/books/([\w-]+)', t)
        if m: return {'hardcover': f"https://hardcover.app/books/{m.group(1)}"}
    return {'error': 'Not recognised. Expected: ASIN, Audible, Goodreads or Hardcover link.'}


PROBLEM_STATUSES = ('not_found', 'other_edition', 'no_ratings_yet', 'error')


def select_items(s, mode, text='', limit=300):
    """Book keys for the start dialog: 'problems' (last result not ok), 'new' (never checked), 'list' (keys, item ids or titles,
    one per line). Returns (keys, note). 'all' = None (the script picks the due books itself)."""
    res = results()
    if mode == 'problems':
        keys = [k for k, r in res.items() if r.get('status') in PROBLEM_STATUSES]
        return keys[:limit], f"{len(keys)} problem books" + (f", the first {limit}" if len(keys) > limit else '')
    libs = [l for l in (s.get('LIBRARY_IDS') or '').replace(';', ',').split(',') if l.strip()]
    if mode == 'new':
        keys = []
        for lib in libs:
            try:
                for it in abs_get(s, f'/api/libraries/{lib.strip()}/items', timeout=60).json().get('results') or []:
                    k = f"{lib.strip()}_{it['id']}"
                    if k not in res: keys.append(k)
            except (requests.RequestException, ValueError): pass
        return keys[:limit], f"{len(keys)} never checked books" + (f", the first {limit}" if len(keys) > limit else '')
    if mode == 'list':
        by_title = {}
        for k, r in res.items(): by_title.setdefault((r.get('title') or '').strip().lower(), []).append(k)
        keys, unknown = [], []
        for line in (text or '').splitlines():
            t = line.strip()
            if not t: continue
            if '_' in t and t.split('_', 1)[0] in libs: keys.append(t)
            elif re.fullmatch(r'[0-9a-f-]{36}', t) and libs: keys.append(f"{libs[0].strip()}_{t}")
            elif t.lower() in by_title: keys += by_title[t.lower()]
            else: unknown.append(t)
        keys = list(dict.fromkeys(keys))
        return keys[:limit], f"{len(keys)} books from the list" + (f"; not found: {', '.join(unknown[:5])}" if unknown else '')
    return None, 'all due books'
