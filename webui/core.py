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
    ('REFRESH_DAYS', 'Refresh after (days)', 'int', 90, '0 = every book is due (one complete pass)', 'Run'),
    ('RUN_MODE', 'Run mode', 'choice:auto|buildup|maintain', 'auto', 'auto = build-up until every book was checked once, then maintenance. Maintenance: new and changed books always, plus a few random due books', 'Run'),
    ('MAINTAIN_SAMPLE', 'Random books per maintenance scan', 'int', 30, 'Besides new and changed books', 'Run'),
    ('QUICK_MODE_MINUTES', 'Quick mode: look for new books every (minutes)', 'int', 0, '0 = off. New books in ABS are checked right away instead of waiting for the next scheduled scan', 'Run'),
    ('MANUAL_LOCK_TAG', 'Lock tag for fields you changed in ABS', 'bool', False, 'Fields you change yourself in ABS are never overwritten anyway; this also sets the lock_<field> tag in ABS', 'Run'),
    ('ABS_BACKUP', 'ABS backup before real scans', 'bool', True, 'Creates an ABS backup before every scan without DRY_RUN; if it fails nothing is written', 'Run'),
    ('SLEEP_TIMER', 'Extra pause between books (s)', 'float', 0, 'Every source has its own minimum interval, so 0 is safe and fastest', 'Run'),
    ('NO_RATINGS_RECHECK_DAYS', 'Re-check books without ratings (days)', 'int', 7, '', 'Run'),
    ('LOG_LEVEL', 'Log level', 'choice:INFO|DEBUG', 'INFO', 'DEBUG only for troubleshooting (much bigger logs)', 'Logs'),
    ('LOG_RETENTION_DAYS', 'Keep logs (days)', 'int', 30, 'Older logs and planning files are deleted', 'Logs'),

    ('AUDIBLE_MARKETPLACES', 'Audible stores', 'str', 'com,co.uk,ca,com.au,de,fr,it,es', 'An ASIN is searched in these stores', 'Audible'),
    ('USE_AUDIBLE_API', 'Use the Audible API', 'bool', True, 'Off = read web pages only (not recommended)', 'Audible'),
    ('AUDIBLE_API_INTERVAL', 'Interval Audible API (s)', 'float', 1, '', 'Audible'),
    ('AUDIBLE_WEB_INTERVAL', 'Interval Audible web page (s)', 'float', 3, '', 'Audible'),
    ('RUNTIME_TOLERANCE_PCT', 'Same-book runtime tolerance (%)', 'float', 15, 'Only decides whether the ASIN still names the same BOOK (another recording can be 12-16 % longer). Lower values lose correct matches. Narrator, publisher, year and cover need the runtime within 10 min anyway', 'Audible'),
    ('RUNTIME_TOLERANCE_MIN', 'Same-book runtime tolerance (min)', 'float', 20, 'Both limits must be exceeded', 'Audible'),

    ('HARDCOVER_ENABLED', 'Use Hardcover', 'bool', True, 'Ratings and the print ISBN. The scan stops shortly before the daily limit', 'Hardcover'),
    ('HARDCOVER_TOKEN', 'Hardcover API token', 'secret', '', 'hardcover.app → Settings → API', 'Hardcover'),
    ('HARDCOVER_PAID', 'Paid Hardcover plan', 'bool', False, 'Off = free plan: at most 5000 requests per day. On = 50000 per day. Both 60 per minute', 'Hardcover'),
    ('HARDCOVER_INTERVAL', 'Interval Hardcover (s)', 'float', 1.1, 'Hardcover allows 60 requests per minute', 'Hardcover'),

    ('GOODREADS_ENABLED', 'Use Goodreads', 'bool', False, 'Experimental: Goodreads blocks bots quickly. At the first block Goodreads is stopped for the scan', 'Goodreads'),
    ('GOODREADS_MAX_PER_RUN', 'Goodreads requests per scan', 'int', 25, 'Remembered books do not count. Safe so far: about 25 per night', 'Goodreads'),
    ('GOODREADS_INTERVAL', 'Interval Goodreads (s)', 'float', 10, '', 'Goodreads'),
    ('GR_BLOCK_COOLDOWN', 'Pause after a Goodreads block (s)', 'int', 900, '', 'Goodreads'),
    ('GR_MAX_COOLDOWNS', 'Pauses before Goodreads is skipped', 'int', 0, '0 = stop at the first block (recommended)', 'Goodreads'),
    ('GR_BOOK_PAGES', 'Also load Goodreads book pages', 'bool', False, 'Off = read the rating from the search list only (book pages trigger the bot block much sooner)', 'Goodreads'),
    ('GR_MISSING_RECHECK_DAYS', 'Re-check books without Goodreads (days)', 'int', 30, '', 'Goodreads'),

    ('RATING_ORDER', 'Sources in the rating block', 'order:audible|goodreads|hardcover', 'audible,goodreads,hardcover', 'Ticked sources are shown, top to bottom (all enabled sources are still asked)', 'Metadata'),
    ('RATING_ICONS', 'Rating symbols', 'choice:moons|stars', 'moons', '🌕🌕🌕🌕🌗 or ★★★★★', 'Metadata'),
    ('RATING_LABELS', 'Rating block language', 'choice:en|de', 'en', 'Overall / Performance / Story or Gesamt / Sprecher / Geschichte', 'Metadata'),
    ('RATING_POSITION', 'Rating block position', 'choice:top|bottom', 'top', 'Above or below the book description', 'Metadata'),
    ('RATING_MIN_COUNT', 'Minimum number of ratings', 'int', 0, 'A source with fewer ratings is not shown (0 = all)', 'Metadata'),
    ('FILL_COVERS', 'Set missing covers', 'bool', True, 'Only for a verified ASIN', 'Metadata'),
    ('UPDATE_COVERS', 'Replace existing covers', 'bool', True, 'Only for a verified ASIN (same recording); the old cover is backed up and can be restored', 'Metadata'),
    ('GENRE_LANGUAGE', 'Genre language', 'choice:en|keep', 'en', 'en = translate German Audible genres, keep = unchanged', 'Metadata'),
    ('GENRE_RULES', 'Genre rules', 'text', '', 'One per line: "Old => New" renames, "Old => -" never adds that genre (only genres coming from Audible)', 'Metadata'),

    ('SERIES_MERGE', 'Merge series automatically', 'bool', True, 'Only safe cases; uncertain ones go to the review list', 'Series & authors'),
    ('AUTHOR_NORMALIZE', 'Unify author spellings', 'bool', True, '', 'Series & authors'),
    ('REMOVE_ROLE_CREDITS', 'Keep only real authors', 'bool', True, 'Removes illustrators, translators, editors, forewords … from the author field (only when a real author remains); never taken from Audible', 'Series & authors'),
    ('REMOVE_EMPTY_AUTHORS', 'Remove empty authors', 'bool', True, 'Deletes only ABS authors without a single book (left-overs after merges); books are never deleted', 'Series & authors'),
    ('LIBRARY_REPORT', 'Create the library report', 'bool', True, '', 'Series & authors'),
    ('SERIES_LOCK_AFTER_MERGE', 'Set lock_series after a merge', 'bool', False, 'Old variant, not recommended', 'Series & authors'),

    ('UI_LANGUAGE', 'Language', 'choice:en|de', 'en', 'English (default) or Deutsch', 'Interface'),
]
# rarely changed: shown folded ("Advanced") inside their card
ADVANCED = {'ABS_TIMEOUT', 'SLEEP_TIMER', 'NO_RATINGS_RECHECK_DAYS', 'USE_AUDIBLE_API', 'AUDIBLE_API_INTERVAL',
            'AUDIBLE_WEB_INTERVAL', 'RUNTIME_TOLERANCE_PCT', 'RUNTIME_TOLERANCE_MIN', 'HARDCOVER_INTERVAL', 'GOODREADS_INTERVAL', 'GR_BLOCK_COOLDOWN',
            'GR_MAX_COOLDOWNS', 'GR_MISSING_RECHECK_DAYS', 'GR_BOOK_PAGES', 'SERIES_LOCK_AFTER_MERGE'}
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


def same_asin_groups():
    """Books that share ONE ASIN (last results): usually the same book twice in the library. Show only."""
    by = {}
    for k, r in results().items():
        if r.get('asin') and r.get('status') != 'ignored': by.setdefault(r['asin'], []).append({'key': k, 'title': r.get('title'), 'author': r.get('author')})
    return sorted(([a, v] for a, v in by.items() if len(v) > 1), key=lambda x: x[1][0]['title'] or '')


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
def notify(s, title, body, is_error):
    url = (s.get('apprise_url') or '').strip()
    if not url or (s.get('notify_on') == 'errors' and not is_error): return False
    try:
        import apprise
        a = apprise.Apprise(); a.add(url)
        return bool(a.notify(title=title, body=body))
    except Exception:
        return False


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
            dry = s['DRY_RUN'] if dry_run is None else dry_run
            if kind == 'apply': dry = False
            extra = {'RUN_ID': run_id, 'DRY_RUN': 'true' if dry else 'false'}
            if kind == 'series': extra['SERIES_ONLY'] = 'true'
            if kind == 'item' or (kind == 'full' and only_items): extra['ONLY_ITEMS'] = ','.join(only_items or [])
            env = script_env(s, extra)
            if (run_settings or {}).get('REMOVE_RATINGS'): env['REMOVE_RATINGS'] = run_settings['REMOVE_RATINGS']  # remove-ratings task
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
        summary = parse_env_summary() if run['kind'] != 'undo' else {}
        if not os.path.exists(os.path.join(LOG_DIR, run['log'])): run['log'] = run['console']
        run.update(end=datetime.now().isoformat(timespec='seconds'), returncode=rc,
                   status='stopped' if rc in (-15, -9) else ('ok' if rc == 0 and summary.get('ABS_ICON') != 'alert' else 'error'),
                   summary=summary.get('ABS_REPORT_BODY', ''), subject=summary.get('ABS_SUBJECT', ''))
        if run['kind'] == 'undo' and rc == 0: run['status'] = 'ok'
        self._save_run(run)
        self.current = None
        if run['kind'] in ('full', 'series'):
            notify(s, run.get('subject') or f"ABS Ratings: {run['label']}", run['summary'] or run['status'], run['status'] != 'ok')

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
                if RUNNER.running(): pass  # previous run still busy: skip this slot
                else: RUNNER.start('full', trigger='schedule', label='Scheduled scan')
                planned = None
            quick_poll(s)
            if planned is None or (nxt and nxt < planned) or not s.get('schedule_enabled'):
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
    for d, pat in ((LOG_DIR, r'.*\.log$'), (REPORT_DIR, r'planned_changes_.*\.jsonl$')):
        try:
            for name in os.listdir(d):
                p = os.path.join(d, name)
                if re.match(pat, name) and name != 'debug_latest.log' and os.path.getmtime(p) < limit: os.remove(p)
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
         'errors': cnt(r' - (?:ERROR|WARNING) - .*(?:Error|Traceback)'), 'goodreads_blocked': bool(re.search(r'Goodreads still blocks|Bot challenge', txt))}
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
