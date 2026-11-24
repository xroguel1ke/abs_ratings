"""Settings in 6 sections (user 2026-11-24): one form saves all its cards, old section names still work. Run: python -m pytest tests/test_settings_sections.py"""
import json, os, sys, tempfile
cfg = tempfile.mkdtemp()
os.environ['CONFIG_DIR'] = cfg
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
json.dump({'must_change_password': False}, open(os.path.join(cfg, 'settings.json'), 'w'))
from webui import app as W, core


def client():
    c = W.app.test_client()
    with c.session_transaction() as s: s['auth'] = True
    return c


def test_six_sections_and_every_field_has_one():
    assert list(core.SECTIONS) == ['Connection', 'Scans', 'Sources', 'Metadata', 'Notifications', 'System']
    groups = {g for gs in core.SECTIONS.values() for g in gs}
    assert {f[5] for f in core.FIELDS} <= groups, 'a field group without a section is never shown'
    assert all(k in {f[0] for f in core.FIELDS} for k in list(core.SUBHEADS) + list(core.DEPENDS) + list(core.DEPENDS.values()))


def test_scans_form_saves_run_and_schedule():
    c = client()
    r = c.post('/settings', data={'section': 'Scans', 'BATCH_SIZE': '77', 'REFRESH_DAYS': '90', 'NEW_RELEASE_DAYS': '90', 'NEW_RELEASE_REFRESH_DAYS': '10',
                                  'RUN_MODE': 'auto', 'MAINTAIN_SAMPLE': '30', 'QUICK_MODE_MINUTES': '0', 'schedule_enabled': 'on', 'schedule_time': '03:15',
                                  'schedule_days': ['0', '2']})
    assert r.status_code in (302, 303) and r.headers['Location'].endswith('#scans')
    s = core.load_settings()
    assert s['BATCH_SIZE'] == 77 and s['schedule_enabled'] and s['schedule_time'] == '03:15' and s['schedule_days'] == [0, 2]


def test_metadata_form_saves_series_switches_and_old_names_still_work():
    c = client()
    c.post('/settings', data={'section': 'Metadata', 'RATING_ORDER': 'audible', 'RATING_ICONS': 'moons', 'RATING_LABELS': 'en', 'RATING_POSITION': 'top',
                              'RATING_MIN_COUNT': '0', 'GENRE_LANGUAGE': 'en', 'GENRE_MAX': '6', 'SERIES_MERGE': 'on'})
    s = core.load_settings(); assert s['SERIES_MERGE'] is True and s['AUTHOR_NORMALIZE'] is False  # unticked switch of the same form = off
    c.post('/settings', data={'section': 'Notifications', 'apprise_url': 'ntfy://x', 'ev_errors': 'on'})
    assert core.load_settings()['apprise_url'] == 'ntfy://x'
    c.post('/settings', data={'section': 'Schedule', 'schedule_time': '04:00', 'schedule_days': ['1']})  # an old form
    assert core.load_settings()['schedule_time'] == '04:00'


def test_settings_page_renders_every_section():
    html = client().get('/settings').get_data(as_text=True)
    for slug in ('connection', 'scans', 'sources', 'metadata', 'notifications', 'system'): assert f'data-sec="{slug}"' in html
    assert 'class="sw"' in html and 'data-dep="NEW_RELEASE_REFRESH"' in html
