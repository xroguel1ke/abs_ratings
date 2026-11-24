import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import abs_ratings as A


def test_audible_check_retried_before_switch_off(monkeypatch):
    # log check 2026-11-17: one connection error at 18:05 switched Audible off for the whole run (0 books)
    calls, downed = [], []
    def fake(get_audible, hc_search=None, log=print, only=None):
        calls.append(only)
        return {'audible': 'check book X: no product (None)'} if len(calls) == 1 else {}
    monkeypatch.setattr(A.SH, 'run_canaries', fake)
    monkeypatch.setattr(A.time, 'sleep', lambda s: None)
    monkeypatch.setattr(A, 'source_down', lambda s, why: downed.append(s))
    monkeypatch.setattr(A, 'SOURCE_CANARY', True)
    monkeypatch.setattr(A, 'USE_AUDIBLE_API', True)
    monkeypatch.setattr(A, 'CANARY_REUSE_MIN', 0)  # every call checks (a passed check is otherwise reused for an hour)
    A.source_canaries()
    assert downed == [] and calls == [None, 'audible']


def test_audible_check_switches_off_when_it_stays_down(monkeypatch):
    downed = []
    monkeypatch.setattr(A.SH, 'run_canaries', lambda *a, **k: {'audible': 'down'})
    monkeypatch.setattr(A.time, 'sleep', lambda s: None)
    monkeypatch.setattr(A, 'source_down', lambda s, why: downed.append(s))
    monkeypatch.setattr(A, 'SOURCE_CANARY', True)
    monkeypatch.setattr(A, 'USE_AUDIBLE_API', True)
    monkeypatch.setattr(A, 'CANARY_REUSE_MIN', 0)  # every call checks (a passed check is otherwise reused for an hour)
    A.source_canaries()
    assert downed == ['audible']
