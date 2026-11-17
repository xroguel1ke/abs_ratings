"""Release calendar (webui/gcal.py) against a fake Google Calendar API - no network.
Run: python -m pytest tests/test_gcal.py"""
import datetime as dt, os, re, sys, tempfile

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
os.environ.setdefault('CONFIG_DIR', tempfile.mkdtemp())
sys.path.insert(0, ROOT)
from webui import gcal  # noqa: E402

TODAY = dt.date(2026, 11, 18)
REPORT = {'series': [
    {'lib': 'L', 'series': '86--EIGHTY-SIX', 'author': 'Asato Asato', 'language': 'English', 'store': 'www.audible.com', 'missing': [
        {'num': '14', 'title': '86--EIGHTY-SIX, Vol. 14', 'asin': 'B0HD1GHDWQ', 'release': '2027-01-05', 'upcoming': True},
        {'num': '15', 'title': 'Vol. 15', 'asin': 'B0NODATE01', 'release': '', 'upcoming': True},           # no date: no event
        {'num': '3', 'title': 'Old one', 'asin': 'B0RELEASED', 'release': '2025-01-01', 'upcoming': False}]},  # released: no event
    {'lib': 'L', 'series': 'Running Wild', 'author': 'K. Author', 'language': 'German', 'store': 'www.audible.de', 'missing': [
        {'num': '5', 'title': 'Wildes Ende', 'asin': 'B0HDBX2JGY', 'release': '2027-06-22', 'upcoming': True}]},
    {'lib': 'L', 'series': 'Hidden Saga', 'author': 'X', 'store': 'www.audible.com', 'missing': [
        {'num': '2', 'title': 'Hidden 2', 'asin': 'B0HIDDEN02', 'release': '2027-02-01', 'upcoming': True}]}]}
HIDDEN = {'series': ['L|Hidden Saga'], 'volumes': []}


class R:
    def __init__(self, code, j=None): self.status_code, self._j, self.text = code, j or {}, ''
    def json(self): return self._j


class FakeCal:
    """Events by id; list honours privateExtendedProperty and timeMin like Google does."""
    def __init__(self, events=None, deny=False): self.ev, self.deny, self.calls = dict(events or {}), deny, []
    def _id(self, url): m = re.search(r'/events/([^/?]+)$', url); return m and m.group(1)
    def get(self, url, params=None, timeout=None):
        self.calls.append(('GET', url))
        if self.deny: return R(403)
        eid = self._id(url)
        if eid: e = self.ev.get(eid); return R(200, e) if e and e.get('status') != 'cancelled' else R(404)
        prop = (params or {}).get('privateExtendedProperty')
        tmin = ((params or {}).get('timeMin') or '0000')[:10]
        items = [e for e in self.ev.values() if e.get('status') != 'cancelled' and e['start']['date'] >= tmin
                 and (not prop or (e.get('extendedProperties') or {}).get('private', {}).get(prop.split('=')[0]) == prop.split('=')[1])]
        return R(200, {'items': items})
    def post(self, url, json=None, timeout=None):
        self.calls.append(('POST', json['id']))
        if json['id'] in self.ev: return R(409)
        self.ev[json['id']] = dict(json); return R(200, json)
    def put(self, url, json=None, timeout=None):
        eid = self._id(url); self.calls.append(('PUT', eid)); self.ev[eid] = dict(json, id=eid); return R(200, json)
    def delete(self, url, timeout=None):
        eid = self._id(url); self.calls.append(('DELETE', eid)); self.ev[eid]['status'] = 'cancelled'; return R(204)


def test_event_id_is_valid_for_google():
    eid = gcal.event_id('B0HD1GHDWQ')
    assert re.fullmatch(r'[a-v0-9]{5,1024}', eid) and eid == gcal.event_id('B0HD1GHDWQ')


def test_wanted_only_dated_upcoming_not_hidden():
    w = gcal.wanted_events(REPORT, HIDDEN, today=TODAY)
    assert sorted(e['extendedProperties']['private']['asin'] for e in w.values()) == ['B0HD1GHDWQ', 'B0HDBX2JGY']
    e = w[gcal.event_id('B0HD1GHDWQ')]
    assert e['summary'] == '86--EIGHTY-SIX, Vol. 14' and e['start'] == {'date': '2027-01-05'} and e['end'] == {'date': '2027-01-06'}
    assert 'https://www.audible.com/pd/B0HD1GHDWQ' in e['description'] and e['transparency'] == 'transparent'
    assert w[gcal.event_id('B0HDBX2JGY')]['summary'] == 'Wildes Ende (Running Wild #5)'
    assert 'www.audible.de/pd/B0HDBX2JGY' in w[gcal.event_id('B0HDBX2JGY')]['description']


def test_sync_create_then_nothing_then_update():
    cal = FakeCal()
    w = gcal.wanted_events(REPORT, HIDDEN, today=TODAY)
    assert gcal.sync(cal, 'c', w, today=TODAY) == {'created': 2, 'updated': 0, 'deleted': 0, 'unchanged': 0}
    assert gcal.sync(cal, 'c', w, today=TODAY) == {'created': 0, 'updated': 0, 'deleted': 0, 'unchanged': 2}
    rep = {'series': [dict(REPORT['series'][0], missing=[dict(REPORT['series'][0]['missing'][0], release='2027-02-02')]), REPORT['series'][1]]}
    n = gcal.sync(cal, 'c', gcal.wanted_events(rep, HIDDEN, today=TODAY), today=TODAY)  # date moved
    assert n['updated'] == 1 and cal.ev[gcal.event_id('B0HD1GHDWQ')]['start'] == {'date': '2027-02-02'}


def test_removed_future_deleted_past_kept_foreign_untouched():
    w = gcal.wanted_events(REPORT, HIDDEN, today=TODAY)
    old = gcal.event_body({'series': 'S'}, {'asin': 'B0PAST0001', 'title': 'Past'}, dt.date(2026, 10, 1))
    gone = gcal.event_body({'series': 'S'}, {'asin': 'B0GONE0001', 'title': 'Gone'}, dt.date(2027, 3, 1))
    mine = {gcal.event_id('B0PAST0001'): dict(old, id=gcal.event_id('B0PAST0001')), gcal.event_id('B0GONE0001'): dict(gone, id=gcal.event_id('B0GONE0001'))}
    foreign = {'x1': {'id': 'x1', 'summary': 'Dentist', 'start': {'date': '2027-03-01'}, 'end': {'date': '2027-03-02'}, 'status': 'confirmed'}}
    cal = FakeCal({**mine, **foreign})
    n = gcal.sync(cal, 'c', w, today=TODAY)
    assert n['deleted'] == 1 and cal.ev[gcal.event_id('B0GONE0001')]['status'] == 'cancelled'
    assert cal.ev[gcal.event_id('B0PAST0001')].get('status') == 'confirmed'  # history stays
    assert cal.ev['x1']['status'] == 'confirmed' and not any(c[1] == 'x1' for c in cal.calls if c[0] != 'GET')


def test_deleted_by_hand_comes_back():
    w = gcal.wanted_events(REPORT, HIDDEN, today=TODAY)
    cal = FakeCal(); gcal.sync(cal, 'c', w, today=TODAY)
    eid = gcal.event_id('B0HD1GHDWQ'); cal.ev[eid]['status'] = 'cancelled'
    n = gcal.sync(cal, 'c', w, today=TODAY)
    assert n['created'] == 1 and cal.ev[eid]['status'] == 'confirmed' and ('PUT', eid) in cal.calls


def test_dry_run_writes_nothing():
    cal = FakeCal(); n = gcal.sync(cal, 'c', gcal.wanted_events(REPORT, HIDDEN, today=TODAY), today=TODAY, dry=True)
    assert n['created'] == 2 and not cal.ev and all(c[0] == 'GET' for c in cal.calls)


def test_no_access_is_a_readable_error():
    try: gcal.sync(FakeCal(deny=True), 'c', {}, today=TODAY); assert False
    except gcal.CalendarError as e: assert 'shared with the service account' in str(e)


def test_bad_key_and_core_never_raises():
    for bad in ('not json', '{"type": "authorized_user"}'):
        try: gcal.session(bad); assert False
        except gcal.CalendarError: pass
    from webui import core
    ok, n, err = core.calendar_sync({'GCAL_KEY': 'not json', 'GCAL_CALENDAR_ID': 'c'})
    assert not ok and n is None and 'JSON' in err and core.calendar_state()['ok'] is False
    ok, n, err = core.calendar_sync({})
    assert not ok and 'needed' in err
