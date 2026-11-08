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

PAGES = ['/', '/books', '/review', '/runs', '/history', '/settings', '/logs', '/problem/lib_1', '/password']
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
    want = {'/': 'Übersicht', '/books': 'Sicherheit', '/review': 'Prüfliste', '/runs': 'Scan starten',
            '/history': 'Geschriebene Änderungen', '/settings': 'Verbindung speichern', '/logs': 'Noch keine Logs', '/problem/lib_1': 'Keine ASIN und kein Treffer bei Audible',
            '/password': 'Passwort ändern'}
    for p, w in want.items():
        h = c.get(p).get_data(as_text=True)
        assert w in h, (p, w)
        assert '<html lang="de">' in h
    assert 'Laufzeit' in c.get('/problem/lib_1').get_data(as_text=True)
    assert 'Abdeckung' in c.get('/').get_data(as_text=True)  # the status page lives in the overview now


def test_language_saved_in_interface_section():
    c = client()
    r = c.post('/settings', data={'section': 'Interface', 'UI_LANGUAGE': 'de'}); assert r.status_code in (302, 303)
    assert core.load_settings()['UI_LANGUAGE'] == 'de'
    assert 'Einstellungen' in c.get('/settings').get_data(as_text=True)
    c.post('/settings', data={'section': 'Interface', 'UI_LANGUAGE': 'en'})
    assert core.load_settings()['UI_LANGUAGE'] == 'en'


def test_values_are_human_readable():
    hv = W.human_value
    assert hv([{'name': 'The Legend of Drizzt', 'sequence': '1'}], 'series') == 'The Legend of Drizzt #1'
    assert hv([{'name': 'A', 'sequence': '1'}, {'name': 'B', 'sequence': None}], 'series') == 'A #1; B'
    assert hv([{'name': 'R. A. Salvatore'}, {'name': 'X'}], 'authors') == 'R. A. Salvatore, X'
    assert hv(['Fantasy', 'Epic'], 'genres') == 'Fantasy, Epic' and hv(None) == '–' and hv([]) == '–' and hv(True) == 'yes'
    with open(os.path.join(cfg, 'changes.jsonl'), 'w') as f:
        f.write(json.dumps({'run': 'r1', 'ts': '2026-11-04T10:00:00+00:00', 'item': 'i1', 'key': 'lib_1', 'title': 'Homeland', 'field': 'series',
                            'old': [{'name': 'The Legend of Drizzt', 'sequence': '1'}], 'new': [{'name': 'Legend of Drizzt', 'sequence': '1'}],
                            'source': 'series_merge', 'dry_run': False}) + '\n')
    d = W.diff_value
    assert str(d(['A', 'B'], ['B', 'C'], 'authors')) == '<span class="del">A</span>, B' and str(d(['B', 'C'], ['A', 'B'], 'authors', True)) == 'B, <span class="ins">C</span>'
    assert str(d('The Big Book Two', 'The Big Book', 'title', True)) == 'The Big Book <span class="ins">Two</span>' and str(d('The Big Book', 'The Big Book Two', 'title')) == 'The Big Book'
    assert str(d('B0AAA', 'B0BBB', 'asin')) == '<span class="del">B0AAA</span>' and str(d(None, 'x', 'isbn')) == '–'
    assert str(d('<b>', None, 'title', True)) == '<span class="ins">&lt;b&gt;</span>', 'escaped'
    h = client().get('/history/r1').get_data(as_text=True)
    assert '<span class="del">The Legend of Drizzt #1</span>' in h and '<span class="ins">Legend of Drizzt #1</span>' in h
    assert 'The Legend of Drizzt #1' in h and 'Legend of Drizzt #1' in h and "[{&#39;name" not in h and "{'name'" not in h, h[-1500:]


def test_settings_export_import_without_secrets():
    s = core.load_settings(); s["API_TOKEN"] = "secret-token"; s["apprise_url"] = "ntfy://x"; s["BATCH_SIZE"] = 123; core.save_settings(s)
    c = client(); r = c.get("/settings/export"); d = json.loads(r.get_data(as_text=True))
    assert "API_TOKEN" not in d and "apprise_url" not in d and "password_hash" not in d and d["BATCH_SIZE"] == 123
    import io
    d["BATCH_SIZE"] = 77; d["API_TOKEN"] = "evil"
    c.post("/settings/import", data={"file": (io.BytesIO(json.dumps(d).encode()), "s.json")}, content_type="multipart/form-data")
    s = core.load_settings(); assert s["BATCH_SIZE"] == 77 and s["API_TOKEN"] == "secret-token"


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
