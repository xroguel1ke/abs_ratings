"""UI language: English by default, German selectable. Run: python tests/test_webui_i18n.py"""
import glob, json, os, re, sys, tempfile
cfg = tempfile.mkdtemp()
os.environ['CONFIG_DIR'] = cfg
ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.insert(0, ROOT)
json.dump({'lib_1': {'title': 'Test Book One', 'status': 'not_found', 'author': 'A', 'reason': 'No ASIN and no match found on Audible',
                     'certainty': 40, 'certainty_parts': {'title': 25}, 'compare': {'Titel': ['A', 'A', ''], 'Laufzeit': ['100 min', '101 min', '']}}},
          open(os.path.join(cfg, 'results.json'), 'w'))
json.dump({'must_change_password': False}, open(os.path.join(cfg, 'settings.json'), 'w'))
from webui import app as W, core, i18n

PAGES = ['/', '/books', '/review', '/status', '/runs', '/history', '/settings', '/logs', '/problem/lib_1', '/password']
# words that must never show up on an English page (book data in the fixtures is English)
GERMAN = re.compile(r'Bücher|Einstellungen|Prüfliste|Übernehmen|rückgängig|Testlauf|Übersicht|Verlauf|speichern|Zeitplan|Sicherheit|Begründung|Abbrechen|nicht gefunden')


def client(lang=None):
    s = json.load(open(os.path.join(cfg, 'settings.json')))
    if lang: s['UI_LANGUAGE'] = lang
    else: s.pop('UI_LANGUAGE', None)
    json.dump(s, open(os.path.join(cfg, 'settings.json'), 'w'))
    c = W.app.test_client()
    with c.session_transaction() as ss: ss['auth'] = True
    return c


def used_strings():
    out = set()
    for f in glob.glob(os.path.join(ROOT, 'webui/templates/*.html')):
        t = open(f, encoding='utf-8').read()
        out |= {m.group(2).replace("\\'", "'") for m in re.finditer(r"\b_\((['\"])(.*?)(?<!\\)\1", t)}
        out |= {m.group(2) for m in re.finditer(r"\bT\((['\"])(.*?)(?<!\\)\1", t)}
    for f in ('webui/app.py', 'webui/static/app.js'):
        t = open(os.path.join(ROOT, f), encoding='utf-8').read()
        out |= {m.group(2) for m in re.finditer(r"\bT\((['\"])(.*?)(?<!\\)\1", t)}
    for key, label, ftype, default, help_, group in core.FIELDS: out |= {label, group} | ({help_} if help_ else set())
    out |= set(core.SECTIONS) | {p[2] for p in core.PROVIDERS.values()} | set(W.STATUS_LABEL.values()) | set(core.FIELD_LABELS.values())
    return {s for s in out if s and s not in ('ASIN', 'ISBN', 'OK', 'Logs', 'Hardcover', 'Audible', 'Goodreads', 'Genres', 'Cover', 'Status', 'Scan', 'Scans')}


def test_every_ui_string_has_a_german_translation():
    missing = sorted(s for s in used_strings() if s not in i18n.DE)
    assert not missing, f"{len(missing)} strings without German: {missing[:15]}"


def test_placeholders_match():
    bad = [k for k, v in i18n.DE.items() if sorted(re.findall(r'%\((\w+)\)s', k)) != sorted(re.findall(r'%\((\w+)\)s', v))]
    assert not bad, bad


def test_all_templates_compile():
    for f in glob.glob(os.path.join(ROOT, 'webui/templates/*.html')): W.app.jinja_env.get_template(os.path.basename(f))


def test_default_is_english():
    c = client()
    for p in PAGES:
        r = c.get(p); h = r.get_data(as_text=True)
        assert r.status_code == 200, (p, r.status_code)
        m = GERMAN.search(re.sub(r'<script>window.I18N = .*?</script>', '', h))
        assert not m, (p, m.group(0), h[max(0, m.start() - 120):m.end() + 60])
        assert '<html lang="en">' in h
    assert 'window.I18N = {}' in c.get('/').get_data(as_text=True)


def test_german_selectable():
    c = client('de')
    want = {'/': 'Übersicht', '/books': 'Sicherheit', '/review': 'Prüfliste', '/status': 'Abdeckung der Bibliothek', '/runs': 'Scan starten',
            '/history': 'Verlauf', '/settings': 'Verbindung speichern', '/logs': 'Noch keine Logs', '/problem/lib_1': 'Keine ASIN und kein Treffer bei Audible',
            '/password': 'Passwort ändern'}
    for p, w in want.items():
        h = c.get(p).get_data(as_text=True)
        assert w in h, (p, w)
        assert '<html lang="de">' in h
    assert 'Laufzeit' in c.get('/problem/lib_1').get_data(as_text=True)


def test_language_saved_in_interface_section():
    c = client()
    r = c.post('/settings', data={'section': 'Interface', 'UI_LANGUAGE': 'de'}); assert r.status_code in (302, 303)
    assert core.load_settings()['UI_LANGUAGE'] == 'de'
    assert 'Einstellungen' in c.get('/settings').get_data(as_text=True)
    c.post('/settings', data={'section': 'Interface', 'UI_LANGUAGE': 'en'})
    assert core.load_settings()['UI_LANGUAGE'] == 'en'


def test_scan_page_remembers_choices_without_changing_settings():
    c = client()
    before = {k: core.load_settings().get(k) for k in ('AUTHOR_NORMALIZE', 'SERIES_MERGE', 'BATCH_SIZE')}
    r = c.post('/api/scan_form', data={'task_meta': 'on', 'task_series': 'on', 'BATCH_SIZE': '42', 'REFRESH_DAYS': '0',
                                         'selection': 'list', 'list': 'Book A\nBook B', 'adv_open': '1'})  # dry + authors unticked
    assert r.status_code == 200
    h = c.get('/runs').get_data(as_text=True)  # e.g. after a visit to the settings
    assert 'name="task_authors" >' in h and 'name="task_series" checked' in h, 'task choices kept'
    assert 'name="dry" id="dry"  onchange' in h, 'test-run switch kept (off)'
    assert 'name="BATCH_SIZE" value="42"' in h and 'Book A\nBook B</textarea>' in h and 'id="advopts" open' in h
    assert 'value="list" checked' in h
    assert {k: core.load_settings().get(k) for k in before} == before, 'saved settings untouched'


if __name__ == '__main__':
    for n, f in sorted(globals().items()):
        if n.startswith('test_'): f(); print('ok', n)
    print('webui i18n tests passed')
