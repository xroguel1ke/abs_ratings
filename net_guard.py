"""Network outages (user 2026-11-24: 'Syl: Nucleus' got 'not found' + a strike while the network dropped Audible and Hardcover for
20+ minutes - 'No route to host' / 'Network is unreachable'). A connection that cannot even be OPENED is no answer of the source:

- the request waits until the host can be reached again (TCP probe every NET_PROBE_SECONDS, at most NET_WAIT_MINUTES) and is sent again;
- if the host stays away, GAVE_UP is set: the run stops after the current book, and that book gets no strike / no 'not found'
  (book_failed() tells abs_ratings);
- only hosts the script needs (Audible, Hardcover, ABS) or that answered before in this run wait - an unknown, dead host
  (DNS name that does not exist) fails at once as before.

Installed below requests' Session.request (HTTPAdapter.send), so recorder.py replays never reach it."""
import os, socket, threading, time, logging, urllib.parse
import requests
from requests.adapters import HTTPAdapter

WAIT_MINUTES = float(os.getenv('NET_WAIT_MINUTES', '15') or 15)
PROBE_SECONDS = float(os.getenv('NET_PROBE_SECONDS', '15') or 15)
MAX_RETRIES = 3
KNOWN = ('audible.', 'hardcover.app', 'goodreads.com')  # sources the script always needs
GAVE_UP = {}             # host -> (reason, when): unreachable for the whole wait; expires after another WAIT_MINUTES (long-lived Goodreads mode)
_SEEN_OK = set()         # hosts that answered in this run
_LOCKS = {}
_LOCKS_LOCK = threading.Lock()
_TL = threading.local()  # per book thread: did a request give up?
_installed = [False]
_sleep = time.sleep      # tests replace these
_probe_connect = socket.create_connection


def down(host):
    """The reason while the host counts as given up (another WAIT_MINUTES after giving up), else None."""
    e = GAVE_UP.get(host)
    if e and time.monotonic() - e[1] < WAIT_MINUTES * 60: return e[0]
    GAVE_UP.pop(host, None); return None


def any_down(): return any(down(h) for h in list(GAVE_UP))


def book_start(): _TL.failed = None
def book_failed(): return getattr(_TL, 'failed', None)


def _is_outage(exc):
    """A connection that could not be opened or broke off - not an HTTP answer, not a TLS problem, not an unknown name."""
    if isinstance(exc, (requests.exceptions.SSLError, requests.exceptions.ProxyError, requests.exceptions.InvalidURL)): return False
    if not isinstance(exc, (requests.exceptions.ConnectionError,)): return False
    text = repr(exc)
    if 'Name or service not known' in text or 'nodename nor servname' in text or 'NXDOMAIN' in text: return False  # the name does not exist
    return True


def _wanted(host, abs_host):
    return bool(host) and (host == abs_host or host in _SEEN_OK or any(k in host for k in KNOWN))


def _reachable(host, port):
    try:
        s = _probe_connect((host, port), 5); s.close(); return True
    except OSError:
        return False


def wait_for(host, port, why):
    """Block until host:port accepts connections again (True) or WAIT_MINUTES passed (False). One thread probes, the others wait."""
    if down(host): return False
    with _LOCKS_LOCK: lock = _LOCKS.setdefault(host, threading.Lock())
    with lock:
        if down(host): return False
        if _reachable(host, port): return True  # a short blip, or another thread waited it out already
        t0 = time.monotonic()
        logging.warning(f"🌐 Network: {host} cannot be reached ({why}) - waiting up to {WAIT_MINUTES:g} min, nothing is counted as 'not found'")
        while time.monotonic() - t0 < WAIT_MINUTES * 60:
            _sleep(PROBE_SECONDS)
            if _reachable(host, port):
                logging.info(f"🌐 Network: {host} reachable again after {round(time.monotonic() - t0)} s - carrying on")
                return True
        GAVE_UP[host] = (why, time.monotonic())
        logging.error(f"🌐 Network: {host} still unreachable after {WAIT_MINUTES:g} min - the run stops after this book (no strikes)")
        return False


_SESS = threading.local()


def _session():
    s = getattr(_SESS, 's', None)
    if s is None: s = _SESS.s = requests.Session()
    return s


def pooled_request(method, url, **kw):
    """requests.get()/post() opened a NEW connection for every call (a new Session each time). Behind the provider's DS-Lite NAT
    (session limit per line) 6 parallel test loops held ~2,800 sessions and the AFTR refused new ones for everybody (2026-11-24,
    ICMP 'administratively prohibited' from 192.0.0.1). One kept-alive session per thread reuses the connections; its cookies are
    cleared after every call, so each call behaves like the fresh session before (cookies only live within one redirect chain)."""
    s = _session()
    try: return s.request(method=method, url=url, **kw)
    finally: s.cookies.clear()


def install(abs_url=''):
    if _installed[0]: return
    _installed[0] = True
    abs_host = urllib.parse.urlparse(abs_url or '').hostname
    requests.api.request = pooled_request  # requests.get/post/... call this module function
    orig = HTTPAdapter.send

    def send(self, request, **kw):
        u = urllib.parse.urlparse(request.url)
        host, port = u.hostname, u.port or (443 if u.scheme == 'https' else 80)
        for attempt in range(MAX_RETRIES + 1):
            if (why_ := down(host)):
                _TL.failed = why_
                raise requests.exceptions.ConnectionError(f"{host} unreachable (network outage: {why_})", request=request)
            try:
                r = orig(self, request, **kw)
                _SEEN_OK.add(host)
                return r
            except requests.exceptions.ConnectionError as e:
                if not _is_outage(e) or not _wanted(host, abs_host) or attempt == MAX_RETRIES: raise
                why = str(e.__context__ or e).split('Caused by ')[-1][:120]
                if not wait_for(host, port, why):
                    _TL.failed = why
                    raise
    HTTPAdapter.send = send
