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
    ('ABS_URL', 'Audiobookshelf URL', 'str', '', 'z.B. http://192.168.50.16:13378', 'Verbindung'),
    ('API_TOKEN', 'ABS API-Token', 'secret', '', 'ABS: Einstellungen → Benutzer → API-Token', 'Verbindung'),
    ('LIBRARY_IDS', 'Bibliotheken', 'libraries', '', 'Welche ABS-Bibliotheken bearbeitet werden', 'Verbindung'),
    ('HARDCOVER_TOKEN', 'Hardcover API-Token', 'secret', '', 'hardcover.app → Settings → API', 'Verbindung'),

    ('DRY_RUN', 'Testlauf (DRY_RUN) für geplante Läufe', 'bool', True, 'An = nichts wird in ABS geschrieben, nur geplant und protokolliert', 'Lauf'),
    ('BATCH_SIZE', 'Bücher pro Lauf', 'int', 300, 'Zufällig aus allen fälligen Büchern aller Bibliotheken', 'Lauf'),
    ('REFRESH_DAYS', 'Auffrischen nach (Tage)', 'int', 90, '0 = alle Bücher sind fällig (einmaliger Komplett-Durchlauf)', 'Lauf'),
    ('SLEEP_TIMER', 'Zusätzliche Pause zwischen Büchern (s)', 'float', 0, 'Jede Quelle hat ihren eigenen Mindestabstand (unten), 0 ist daher sicher und am schnellsten', 'Lauf'),
    ('ABS_BACKUP', 'ABS-Backup vor echten Läufen', 'bool', True, 'Legt vor jedem Lauf ohne DRY_RUN ein ABS-Backup an', 'Lauf'),
    ('ABS_TIMEOUT', 'ABS Timeout (s)', 'int', 30, '', 'Lauf'),

    ('USE_AUDIBLE_API', 'Audible-API nutzen', 'bool', True, 'Aus = nur Webseiten auslesen (nicht empfohlen)', 'Quellen'),
    ('AUDIBLE_MARKETPLACES', 'Audible-Shops', 'str', 'com,co.uk,ca,com.au,de,fr,it,es', 'In diesen Shops wird eine ASIN gesucht', 'Quellen'),
    ('AUDIBLE_API_INTERVAL', 'Abstand Audible-API (s)', 'float', 1, '', 'Quellen'),
    ('AUDIBLE_WEB_INTERVAL', 'Abstand Audible-Webseite (s)', 'float', 3, '', 'Quellen'),
    ('GOODREADS_ENABLED', 'Goodreads', 'bool', True, 'Aus = keine Goodreads-Anfragen, vorhandene Bewertungen bleiben', 'Quellen'),
    ('GOODREADS_MAX_PER_RUN', 'Goodreads-Suchen pro Lauf', 'int', 300, 'Gemerkte Bücher zählen nicht. 0 = unbegrenzt', 'Quellen'),
    ('GOODREADS_INTERVAL', 'Abstand Goodreads (s)', 'float', 10, '', 'Quellen'),
    ('GR_BLOCK_COOLDOWN', 'Pause nach Goodreads-Sperre (s)', 'int', 900, '', 'Quellen'),
    ('GR_MAX_COOLDOWNS', 'Pausen bis Goodreads übersprungen wird', 'int', 2, '', 'Quellen'),
    ('HARDCOVER_ENABLED', 'Hardcover', 'bool', True, 'Braucht den Hardcover-Token (Verbindung). Nur Bewertungen, keine Metadaten. Limit: 5000 Anfragen/Tag', 'Quellen'),
    ('HARDCOVER_INTERVAL', 'Abstand Hardcover (s)', 'float', 1.1, 'Hardcover erlaubt 60 Anfragen pro Minute', 'Quellen'),
    ('RATING_ORDER', 'Reihenfolge im Bewertungsblock', 'str', 'audible,goodreads,hardcover', 'Komma-getrennt, z.B. audible,hardcover,goodreads', 'Quellen'),

    ('FILL_COVERS', 'Fehlende Cover von Audible setzen', 'bool', True, '', 'Metadaten'),
    ('GENRE_LANGUAGE', 'Genre-Sprache', 'choice:en|keep', 'en', 'en = deutsche Audible-Genres übersetzen, keep = unverändert', 'Metadaten'),
    ('RUNTIME_TOLERANCE_PCT', 'Laufzeit-Toleranz (%)', 'float', 15, 'Größere Abweichung = andere Ausgabe', 'Metadaten'),
    ('RUNTIME_TOLERANCE_MIN', 'Laufzeit-Toleranz (min)', 'float', 20, 'Beide Grenzen müssen überschritten sein', 'Metadaten'),
    ('NO_RATINGS_RECHECK_DAYS', 'Neu prüfen ohne Bewertungen (Tage)', 'int', 7, '', 'Metadaten'),
    ('GR_MISSING_RECHECK_DAYS', 'Neu prüfen ohne Goodreads (Tage)', 'int', 30, '', 'Metadaten'),

    ('SERIES_MERGE', 'Reihen automatisch zusammenführen', 'bool', True, 'Nur sichere Fälle, unsichere landen im Merge-Review', 'Reihen & Autoren'),
    ('AUTHOR_NORMALIZE', 'Autoren-Schreibweisen vereinheitlichen', 'bool', True, '', 'Reihen & Autoren'),
    ('SERIES_LOCK_AFTER_MERGE', 'Nach Zusammenführung lock_series setzen', 'bool', False, 'Alte Variante, nicht empfohlen', 'Reihen & Autoren'),
    ('LIBRARY_REPORT', 'Bibliotheks-Report erstellen', 'bool', True, '', 'Reihen & Autoren'),

    ('LOG_LEVEL', 'Log-Level', 'choice:INFO|DEBUG', 'INFO', '', 'Logs'),
    ('LOG_RETENTION_DAYS', 'Logs aufbewahren (Tage)', 'int', 30, '', 'Logs'),
]
SCRIPT_KEYS = [f[0] for f in FIELDS if f[0] != 'LOG_RETENTION_DAYS']
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


def abs_test(s):
    try:
        r = abs_get(s, '/api/libraries')
        if r.status_code == 200:
            return True, f"Verbunden. {len(r.json().get('libraries', []))} Bibliotheken gefunden."
        return False, f"ABS antwortet mit HTTP {r.status_code}" + (' (Token falsch?)' if r.status_code == 401 else '')
    except requests.RequestException as e:
        return False, f"Keine Verbindung: {e}"


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


def hardcover_test(token):
    try:
        r = requests.post('https://api.hardcover.app/v1/graphql', json={'query': '{ me { username } }'},
                          headers={'Authorization': token if token.lower().startswith('bearer') else f'Bearer {token}'}, timeout=15)
        if r.status_code == 200 and 'errors' not in r.json():
            me = r.json().get('data', {}).get('me')
            name = (me[0] if isinstance(me, list) and me else me or {}).get('username', '?')
            return True, f"Verbunden als {name}."
        return False, f"Hardcover antwortet mit HTTP {r.status_code}: {r.text[:150]}"
    except (requests.RequestException, ValueError) as e:
        return False, f"Keine Verbindung: {e}"


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


KIND_LABELS = {'full': 'Lauf', 'series': 'Nur Reihen & Report', 'item': 'Einzelbuch', 'undo': 'Rückgängig', 'apply': 'Testlauf übernehmen'}


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

    def start(self, kind='full', dry_run=None, only_items=None, trigger='manual', label=None, undo_args=None):
        with self.lock:
            if self.running(): return None, 'Es läuft bereits ein Lauf.'
            s = load_settings()
            if not s.get('ABS_URL') or not s.get('API_TOKEN'): return None, 'ABS-URL und Token fehlen (Einstellungen).'
            run_id = datetime.now().strftime('%Y%m%d-%H%M%S')
            dry = s['DRY_RUN'] if dry_run is None else dry_run
            if kind == 'apply': dry = False
            extra = {'RUN_ID': run_id, 'DRY_RUN': 'true' if dry else 'false'}
            if kind == 'series': extra['SERIES_ONLY'] = 'true'
            if kind == 'item': extra['ONLY_ITEMS'] = ','.join(only_items or [])
            env = script_env(s, extra)
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
                   'trigger': trigger, 'items': only_items or [], 'start': datetime.now().isoformat(timespec='seconds'),
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
                else: RUNNER.start('full', trigger='schedule', label='Geplanter Lauf')
                planned = None
            if planned is None or (nxt and nxt < planned) or not s.get('schedule_enabled'):
                planned = nxt
            cleanup_logs(s)
        except Exception:
            pass
        time.sleep(20)


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
FIELD_LABELS = {'asin': 'ASIN', 'cover': 'Cover', 'series': 'Reihe', 'narrators': 'Sprecher', 'description': 'Beschreibung', 'authors': 'Autor',
                'genres': 'Genres', 'publisher': 'Verlag', 'year': 'Jahr', 'isbn': 'ISBN', 'language': 'Sprache', 'ratings': 'Bewertungen'}

def coverage():
    """Share of books that HAVE each field, from library_report.json (written by the series/report task). [(label, have, total, percent, missing titles)]"""
    rep = _load_json(os.path.join(REPORT_DIR, 'library_report.json'), {})
    total = rep.get('books')
    if isinstance(total, (list, dict)): total = len(total)
    total = int(total or 0); rows = []
    for k, titles in (rep.get('missing') or {}).items():
        have = max(total - len(titles), 0)
        rows.append((FIELD_LABELS.get(k, k), have, total, round(100 * have / total, 1) if total else 0, sorted(titles)[:40]))
    rows.sort(key=lambda r: r[3])
    return rows, rep.get('generated', '')

def api_status():
    """Hardcover daily limit / Goodreads state from the newest run logs, cache sizes, open review items."""
    st = {'hc_left': None, 'hc_when': '', 'gr': 'unbekannt', 'caches': {}, 'queue': len(open_queue())}
    try:
        logs = sorted((f for f in os.listdir(LOG_DIR) if f.startswith('run_') and f.endswith('.log')), reverse=True)
    except OSError: logs = []
    for f in logs[:8]:
        try: txt = open(os.path.join(LOG_DIR, f), encoding='utf-8', errors='replace').read()
        except OSError: continue
        if st['hc_left'] is None and (m := re.findall(r'daily limit left: (\d+)', txt)):
            st['hc_left'], st['hc_when'] = int(m[-1]), f[4:23].replace('_', ' ')
        if st['gr'] == 'unbekannt' and 'Goodreads' in txt:
            st['gr'] = 'gesperrt (im letzten Lauf)' if re.search(r'Bot challenge|still blocks|blocks requests', txt) else 'ok (im letzten Lauf)'
        if st['hc_left'] is not None and st['gr'] != 'unbekannt': break
    for name, label in (('hardcover_cache.json', 'Hardcover-IDs'), ('goodreads_cache.json', 'Goodreads-Seiten'), ('audible_series.json', 'Audible-Reihen'),
                        ('audible_authors.json', 'Audible-Autoren'), ('rating_history.json', 'Bewertungs-Verlauf')):
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
    return [dict(x, id=queue_id(x)) for x in review_queue() if queue_id(x) not in d and x.get('status', 'open') == 'open']
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
        if action in ('keep', 'undone', 'merge'):  # drop from the review list right away
            rv = review()
            for k in ('series', 'authors'): rv[k] = [x for x in rv.get(k, []) if x.get('id') != mid]
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
    return {'error': 'Nicht erkannt. Erwartet: ASIN, Audible-, Goodreads- oder Hardcover-Link.'}
