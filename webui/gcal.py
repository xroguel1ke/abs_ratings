"""Release calendar: announced volumes of the library's series (reports/missing_in_series.json) as all-day events in ONE
Google calendar. The server only sends (outgoing HTTPS to Google); nothing has to reach the server from outside.

Access: a Google service account (JSON key in the settings) that the user shared exactly this one calendar with
("Make changes to events"). Every event the tool writes carries extendedProperties.private abs_ratings=1 and its ASIN;
only those events are ever changed or deleted - other entries in the calendar are never touched.

Sync rules:
- wanted = announced (upcoming) volumes WITH a release date, not hidden on the series page (volume or whole series)
- event id = sha1('abs_ratings:' + ASIN) -> the same volume is always the same event (no duplicates, date changes move it)
- an unchanged event is not written again; a changed one is updated
- a FUTURE event of the tool whose volume is no longer wanted (in the library now, hidden, cancelled) is deleted;
  past events stay as history
"""
import datetime as dt
import hashlib
import json

SCOPES = ['https://www.googleapis.com/auth/calendar.events']
API = 'https://www.googleapis.com/calendar/v3/calendars/{cal}/events'
MARK = 'abs_ratings'


class CalendarError(Exception):
    pass


def event_id(asin):
    """Google event ids allow the characters a-v and 0-9: a hex digest fits."""
    return hashlib.sha1(f'abs_ratings:{asin}'.encode('utf-8')).hexdigest()


def wanted_events(report, hidden=None, today=None):
    """{event id: event body} for every announced volume with a release date that is not hidden."""
    today = today or dt.date.today()
    hidden = hidden or {}
    hid_s, hid_v = set(hidden.get('series') or []), set(hidden.get('volumes') or [])
    out = {}
    for s in (report or {}).get('series') or []:
        name = s.get('series') or ''
        if name in hid_s or f"{s.get('lib')}|{name}" in hid_s: continue
        for m in s.get('missing') or []:
            asin, rel = m.get('asin'), (m.get('release') or '')[:10]
            if not m.get('upcoming') or not asin or asin in hid_v: continue
            try: day = dt.date.fromisoformat(rel)
            except ValueError: continue  # announced without a date: no event
            if day < today - dt.timedelta(days=1): continue  # an old 'upcoming' entry is not created in the past
            out[event_id(asin)] = event_body(s, m, day)
    return out


def event_body(series, vol, day):
    name, title, num = series.get('series') or '', vol.get('title') or '', vol.get('num') or ''
    summary = title if (name and name.lower() in title.lower()) or not name else f"{title} ({name}{' #' + num if num else ''})"
    store = series.get('store') or 'www.audible.com'
    lines = [x for x in (series.get('author'), f"{name} #{num}" if name and num else name, series.get('language'),
                         f"https://{store}/pd/{vol['asin']}") if x]
    return {'summary': summary, 'description': '\n'.join(lines),
            'start': {'date': day.isoformat()}, 'end': {'date': (day + dt.timedelta(days=1)).isoformat()},
            'transparency': 'transparent', 'status': 'confirmed',
            'extendedProperties': {'private': {MARK: '1', 'asin': vol['asin']}}}


def _same(have, want):
    """Only the fields the tool writes decide whether an event has to be written again."""
    return (have.get('status') == 'confirmed' and have.get('summary') == want['summary']
            and (have.get('description') or '') == want['description'] and have.get('start') == want['start']
            and have.get('end') == want['end'] and have.get('transparency', 'opaque') == want['transparency']
            and ((have.get('extendedProperties') or {}).get('private') or {}).get(MARK) == '1')


def session(key_json):
    """Authorised HTTP session for the service account (google-auth refreshes the token itself)."""
    try: info = json.loads(key_json) if isinstance(key_json, str) else key_json
    except ValueError as e: raise CalendarError(f'the service account key is no valid JSON ({e})')
    if not isinstance(info, dict) or info.get('type') != 'service_account' or not info.get('private_key'):
        raise CalendarError('the key is no service account key (JSON with "type": "service_account")')
    from google.oauth2 import service_account
    from google.auth.transport.requests import AuthorizedSession
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return AuthorizedSession(creds), info.get('client_email', '')


def _check(r, what):
    if r.status_code in (401, 403):
        raise CalendarError(f'{what}: no access ({r.status_code}) - is the calendar shared with the service account '
                            f'("Make changes to events")?')
    if r.status_code == 404: raise CalendarError(f'{what}: calendar not found - check the calendar ID')
    if r.status_code >= 400: raise CalendarError(f'{what}: HTTP {r.status_code} {r.text[:200]}')


def own_events(sess, cal, time_min):
    """The tool's events from time_min on (other entries of the calendar are not listed)."""
    url, out, token = API.format(cal=cal), {}, None
    while True:
        params = {'privateExtendedProperty': f'{MARK}=1', 'timeMin': time_min, 'maxResults': 250, 'singleEvents': 'true',
                  'showDeleted': 'false'}
        if token: params['pageToken'] = token
        r = sess.get(url, params=params, timeout=30); _check(r, 'list events')
        j = r.json()
        for e in j.get('items') or []: out[e['id']] = e
        token = j.get('nextPageToken')
        if not token: return out


def sync(sess, cal, wanted, today=None, dry=False):
    """Brings the calendar in line with wanted. Returns counts {'created', 'updated', 'deleted', 'unchanged'}."""
    today = today or dt.date.today()
    url = API.format(cal=cal)
    have = own_events(sess, cal, f"{(today - dt.timedelta(days=1)).isoformat()}T00:00:00Z")
    n = {'created': 0, 'updated': 0, 'deleted': 0, 'unchanged': 0}
    for eid, body in wanted.items():
        cur = have.get(eid)
        if cur is None:  # not listed from yesterday on: maybe it exists with an older date or was deleted by hand
            r = sess.get(f'{url}/{eid}', timeout=30)
            if r.status_code == 200: cur = r.json()
            elif r.status_code not in (404, 410): _check(r, 'read event')
        if cur is not None and _same(cur, body): n['unchanged'] += 1; continue
        if dry: n['updated' if cur is not None else 'created'] += 1; continue
        if cur is not None:
            r = sess.put(f'{url}/{eid}', json=body, timeout=30); _check(r, f"update '{body['summary']}'"); n['updated'] += 1
        else:
            r = sess.post(url, json=dict(body, id=eid), timeout=30)
            if r.status_code == 409:  # the id exists as a deleted event: bring it back
                r = sess.put(f'{url}/{eid}', json=body, timeout=30); _check(r, f"restore '{body['summary']}'")
            else: _check(r, f"create '{body['summary']}'")
            n['created'] += 1
    for eid, e in have.items():
        if eid in wanted: continue
        start = (e.get('start') or {}).get('date') or ''
        if start < today.isoformat(): continue  # past events stay as history
        if not dry:
            r = sess.delete(f'{url}/{eid}', timeout=30)
            if r.status_code not in (200, 204, 404, 410): _check(r, f"delete '{e.get('summary')}'")
        n['deleted'] += 1
    return n


def check_access(sess, cal):
    """Reads the calendar's events once: raises CalendarError with a readable reason when access is missing."""
    r = sess.get(API.format(cal=cal), params={'maxResults': 1}, timeout=30); _check(r, 'calendar')
    return True
