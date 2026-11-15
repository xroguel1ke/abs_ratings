"""Pauses are logged at once (not with the buffered book block); selection runs never wait for Goodreads. Run: python -m pytest tests/test_pause_log.py"""
import os, sys, logging
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/pause_log_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import abs_ratings as A


class ListH(logging.Handler):
    def __init__(self): super().__init__(); self.msgs = []
    def emit(self, r): self.msgs.append(r.getMessage())


def test_pause_bypasses_book_buffer():
    h = ListH(); router = A._BookLogRouter([h])
    lg = logging.getLogger('pause_test'); lg.handlers = [router]; lg.propagate = False; lg.setLevel(logging.INFO)
    A._TL.logbuf = []
    lg.info('book line')
    lg.warning('PAUSE line', extra={'now': True})
    assert h.msgs == ['PAUSE line'] and len(A._TL.logbuf) == 1
    A._TL.logbuf = None


def test_selection_run_skips_goodreads_cooldown(monkeypatch):
    calls = []
    def blocked(*a, **kw): raise A.SourceBlockedException('Bot challenge')
    monkeypatch.setattr(A, '_goodreads_lookup', blocked)
    monkeypatch.setattr(A, 'GOODREADS_ENABLED', True)
    monkeypatch.setattr(A, 'GR_MAX_COOLDOWNS', 2)
    monkeypatch.setattr(A, 'ONLY_ITEMS', ['lib_x'])
    monkeypatch.setattr(A.time, 'sleep', lambda s: calls.append(s))
    monkeypatch.setattr(A, 'GR_STATE', dict(A.GR_STATE, blocked=False, cooldowns=0))
    assert A.get_goodreads_data(None, 'T', ['A'], 'A', force=True) == (None, 'blocked')
    assert calls == []  # no 15-minute wait
