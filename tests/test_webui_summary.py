"""Scan result line in the web UI: zeros left out, short keys as words, warnings on their own line without emoji.
Run: python -m pytest tests/test_webui_summary.py"""
import os, sys, tempfile

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
os.environ['CONFIG_DIR'] = tempfile.mkdtemp()
sys.path.insert(0, ROOT)
from webui import app as W  # noqa: E402

BODY = ("Proc: 25 | New: 3 | ManUpd: 0 | MetaUpd: 0 | ASIN+: 0 | Mig: 0 | ISBN+: 0 | Fix: 0 | NoRatYet: 0 | GR-: 0 | Cover+: 1 | "
        "Series merged: 0 | Err: 2 | ⚠️ Goodreads blocked | GR deferred: 5 | 📚 New volumes of your series: Ironbound #3 | DRY RUN")


def render(text):
    with W.app.test_request_context():
        return str(W.run_summary(text))


def test_counts_and_notes():
    out = render(BODY)
    assert '25 checked · 3 rated · 1 covers · <span class="err">2 errors</span> · 5 Goodreads lookups deferred' in out
    assert 'ManUpd' not in out and '0 ' not in out and 'DRY RUN' not in out
    assert '<span class="sumnote">Goodreads blocked</span>' in out
    assert '<span class="sumnote">New volumes of your series: Ironbound #3</span>' in out
    assert '⚠' not in out and '📚' not in out


def test_nothing_changed():
    assert render("Proc: 0 | New: 0 | Err: 0") == '0 checked · nothing changed'


def test_other_text_unchanged_and_escaped():
    assert render("ABS_URL missing <b>") == 'ABS_URL missing &lt;b&gt;'
    assert render('') == ''


def test_german():
    with W.app.test_request_context():
        W.g.lang = 'de'
        assert str(W.run_summary("Proc: 4 | New: 1")) == '4 geprüft · 1 bewertet'
