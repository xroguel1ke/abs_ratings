"""ABS Ratings web UI (Flask). Start: python -m webui.app (port 47823, data folder /config)."""
import json, os, re, functools, difflib
from markupsafe import Markup
from collections import Counter
from datetime import datetime

from flask import Flask, render_template, request, redirect, url_for, session, jsonify, send_file, abort, flash, g, has_request_context

from webui import core, i18n

app = Flask(__name__)
app.secret_key = core.load_settings()['secret_key']
app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE='Lax')

PROBLEM_STATUS = {'not_found': 'Not found', 'error': 'Error', 'other_edition': 'Other edition', 'no_ratings_yet': 'No ratings yet'}
STATUS_LABEL = dict(PROBLEM_STATUS, ok='OK', ignored='Ignored')


def lang():
    """UI language of this request (settings: UI_LANGUAGE, default English)."""
    if not has_request_context():  # outside a request (tests, background): the saved setting
        try: return core.load_settings().get('UI_LANGUAGE') or 'en'
        except Exception: return 'en'
    if 'lang' not in g:
        try: g.lang = core.load_settings().get('UI_LANGUAGE') or 'en'
        except Exception: g.lang = 'en'
    return g.lang


def T(text, **kw): return i18n.t(text, lang(), **kw)


app.jinja_env.globals['_'] = T
from webui.icons import icon, moon
app.jinja_env.globals['moon'] = moon
app.jinja_env.globals['icon'] = icon
REASON_DE = {
    'No ASIN and no match found on Audible': 'Keine ASIN und kein Treffer bei Audible',
    'ASIN not available in any Audible store, no replacement found': 'ASIN in keinem Audible-Shop verfügbar, kein Ersatz gefunden',
    'No ratings found on Audible': 'Keine Bewertungen bei Audible gefunden',
    'On Audible, but no ratings yet': 'Bei Audible vorhanden, aber noch keine Bewertungen',
    'metadata skipped (Audible data does not fit this book)': 'Metadaten nicht übernommen (Audible-Daten passen nicht zu diesem Buch)',
    'Goodreads not found': 'Goodreads nicht gefunden', 'Goodreads blocked': 'Goodreads gesperrt', 'Goodreads disabled': 'Goodreads aus',
    'Hardcover not found': 'Hardcover nicht gefunden', 'Hardcover blocked': 'Hardcover übersprungen (Limit/Token)',
}
def de(text):
    """Script reasons are English: translated only for the German UI."""
    if not text or lang() != 'de': return text
    if text.startswith('Ratings of another edition'): return text.replace('Ratings of another edition', 'Bewertungen einer anderen Ausgabe').replace('ASIN not available anymore', 'ASIN nicht mehr verfügbar')
    return REASON_DE.get(text, text)
app.jinja_env.filters['de'] = de


FIELD_NAMES = {'asin': 'ASIN', 'title': 'Title', 'subtitle': 'Subtitle', 'description': 'Description', 'genres': 'Genres', 'cover': 'Cover',
               'language': 'Language', 'publisher': 'Publisher', 'publishedYear': 'Year', 'narrators': 'Narrators', 'authors': 'Authors',
               'series': 'Series', 'isbn': 'ISBN', 'explicit': 'Explicit', 'abridged': 'Abridged', 'tags': 'Tags', 'author': 'Empty author'}


def field_label(f): return T(FIELD_NAMES.get(f, f))


def human_value(v, field=''):
    """A journal / planned value as people read it: series 'Name #1; Other #2', lists comma-separated, yes/no, '–' for empty.
    (The journal keeps the raw ABS values for undo; this is only how they are shown.)"""
    if v is None or v == '' or v == [] or v == {}: return '–'
    if isinstance(v, bool): return T('yes') if v else T('no')
    if isinstance(v, dict):
        if v.get('name'): return human_value([v], field)
        return ', '.join(f"{k}: {human_value(x)}" for k, x in v.items())
    if isinstance(v, (list, tuple)):
        out = []
        for x in v:
            if isinstance(x, dict):
                n = x.get('name') or x.get('title') or ''
                seq = x.get('sequence') if x.get('sequence') not in (None, '') else x.get('part')
                out.append(f"{n} #{seq}" if seq not in (None, '') else n)
            else: out.append(str(x))
        return ('; ' if field == 'series' else ', ').join(o for o in out if o) or '–'
    return str(v)


app.jinja_env.filters['hv'] = human_value

LIST_FIELDS = {'authors', 'narrators', 'genres', 'tags', 'series'}


def _items(v, field):
    if v is None or v == '' or v == []: return []
    if isinstance(v, (list, tuple)): return [human_value([x], field) for x in v]
    return [human_value(v, field)]


def diff_value(v, other, field='', new=False):
    """The value with its differences marked: removed parts red (old side), added parts green (new side).
    Lists: per entry. Short texts: per word, unless nearly everything changed (then the whole value).
    Single values (ASIN, ISBN, year): the whole value."""
    esc = lambda x: str(Markup.escape(x))
    cls = 'ins' if new else 'del'
    mark = lambda x: f'<span class="{cls}">{esc(x)}</span>'
    if v is None or v == '' or v == []: return Markup('–')
    if other is None or other == '' or other == []:  # all new / all removed
        return Markup(mark(human_value(v, field)))
    if field in LIST_FIELDS or isinstance(v, (list, tuple)):
        mine, theirs = _items(v, field), set(_items(other, field))
        sep = '; ' if field == 'series' else ', '
        return Markup(sep.join(esc(x) if x in theirs else mark(x) for x in mine))
    a, b = human_value(v, field), human_value(other, field)
    if a == b: return Markup(esc(a))
    wa, wb = a.split(), b.split()
    if len(wa) <= 1 or len(wb) <= 1: return Markup(mark(a))
    # opcodes over (other -> this) for the new side, (this -> other) for the old side; we print only THIS side's words
    sm = difflib.SequenceMatcher(None, wb, wa) if new else difflib.SequenceMatcher(None, wa, wb)
    if sm.ratio() < 0.4: return Markup(mark(a))  # nearly everything differs: one mark, no patchwork
    out = []
    for op, i1, i2, j1, j2 in sm.get_opcodes():
        words = wa[j1:j2] if new else wa[i1:i2]
        if not words: continue
        out.append(' '.join(esc(w) for w in words) if op == 'equal' else mark(' '.join(words)))
    return Markup(' '.join(out))


app.jinja_env.filters['hvd'] = diff_value
app.jinja_env.globals['field_label'] = field_label


def login_required(view):
    @functools.wraps(view)
    def wrapped(*a, **kw):
        if not session.get('auth'): return redirect(url_for('login', next=request.path))
        if core.load_settings().get('must_change_password') and request.endpoint not in ('password', 'logout'):
            return redirect(url_for('password'))
        return view(*a, **kw)
    return wrapped


@app.context_processor
def inject():
    s = core.load_settings()
    nxt = core.next_run_time(s)
    return {'running': core.RUNNER.running(), 'current_run': core.RUNNER.current, 'next_run': nxt,
            'status_label': {k: T(v) for k, v in STATUS_LABEL.items()}, 'app_version': core.APP_VERSION, 'update_available': core.update_available(), 'abs_base': (s.get('ABS_URL') or '').rstrip('/'), 'lang': lang(), 'js_i18n': i18n.DE if lang() == 'de' else {}}


# ---------------------------------------------------------------- auth
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if core.check_password(core.load_settings(), request.form.get('password')):
            session.clear(); session['auth'] = True; session.permanent = True
            return redirect(request.args.get('next') or url_for('dashboard'))
        flash(T('Wrong password.'), 'error')
    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear(); return redirect(url_for('login'))


@app.route('/password', methods=['GET', 'POST'])
def password():
    if not session.get('auth'): return redirect(url_for('login'))
    s = core.load_settings()
    if request.method == 'POST':
        pw, pw2 = request.form.get('new', ''), request.form.get('new2', '')
        if not s.get('must_change_password') and not core.check_password(s, request.form.get('old')):
            flash(T('Old password is wrong.'), 'error')
        elif len(pw) < 6 or pw == 'admin': flash(T('At least 6 characters and not "admin".'), 'error')
        elif pw != pw2: flash(T('The passwords do not match.'), 'error')
        else:
            core.set_password(s, pw); flash(T('Password changed.'), 'ok'); return redirect(url_for('dashboard'))
    return render_template('password.html', forced=s.get('must_change_password'))


# ---------------------------------------------------------------- dashboard
@app.route('/')
@login_required
def dashboard():
    tab = request.args.get('tab', 'problems')
    if tab in ('queue', 'merged', 'review'):  # these moved to the review list page
        return redirect(url_for('review_page') + {'queue': '#buecher', 'merged': '#reihen', 'review': '#reihen'}[tab])
    res = core.results()
    counts = Counter(r.get('status') for r in res.values())
    status_filter = request.args.get('status', '')
    q = (request.args.get('q') or '').lower()
    problems = [dict(r, key=k) for k, r in res.items()
                if r.get('status') in PROBLEM_STATUS and (not status_filter or r['status'] == status_filter)
                and (not q or q in (r.get('title') or '').lower() or q in (r.get('author') or '').lower())]
    problems.sort(key=lambda r: (r.get('status') != 'not_found', r.get('title') or ''))
    dec = core.decisions()
    merged = [m for m in core.merges().values() if dec.get(m['id'], {}).get('action') not in ('confirmed', 'undone')]
    merged.sort(key=lambda m: m.get('last_seen', ''), reverse=True)
    rv = core.review()
    queue = core.open_queue()
    res_all = core.results()
    for x in queue: x['res'] = res_all.get(x['key']) or {}  # links to Audible / Goodreads / Hardcover / ABS for comparing
    runs = core.RUNNER.runs()
    last = next((r for r in runs if r['kind'] in ('full', 'series') and r['status'] != 'running'), None)
    gr_note = ''
    if last and 'Goodreads blocked' in (last.get('summary') or ''): gr_note = T('blocked (last scan)')
    tiles = {'books': len(res), 'ok': counts.get('ok', 0), 'problems': sum(counts.get(k, 0) for k in PROBLEM_STATUS),
             'other_edition': counts.get('other_edition', 0), 'not_found': counts.get('not_found', 0),
             'no_ratings_yet': counts.get('no_ratings_yet', 0), 'ignored': counts.get('ignored', 0),
             'merged': len(merged), 'review': len(rv.get('series', [])) + len(rv.get('authors', [])),
             'queue': len(queue),
             'history': core.history_count()}
    return render_template('dashboard.html', tab=tab, tiles=tiles, problems=problems, merged=merged, review=rv, queue=queue,
                           last=last, gr_note=gr_note, status_filter=status_filter, q=q, counts=counts,
                           settings=core.load_settings(), **status_context())


@app.route('/problem/<key>', methods=['GET', 'POST'])
@login_required
def problem(key):
    res = core.results().get(key)
    if not res: abort(404)
    ov = core.overrides().get(key, {})
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'ignore':
            core.save_override(key, {'ignore': True}); res['status'] = 'ignored'
            flash(T('Book is ignored.'), 'ok'); return redirect(url_for('dashboard'))
        if action == 'unignore':
            core.save_override(key, {'ignore': False}); flash(T('No longer ignored.'), 'ok')
            return redirect(url_for('problem', key=key))
        if action == 'remove_ratings':
            rm = [src for src in ('audible', 'goodreads', 'hardcover') if request.form.get(f'rm_{src}') == 'on']
            if not rm: flash(T('Please choose at least one source.'), 'error'); return redirect(url_for('problem', key=key))
            run_id, err = core.RUNNER.start('item', dry_run=False, only_items=[key], label=f"Remove ratings ({', '.join(rm)}): {res.get('title')}",
                                            run_settings={'REMOVE_RATINGS': ','.join(rm), 'ABS_BACKUP': False})
            if err: flash(T(err), 'error'); return redirect(url_for('problem', key=key))
            return redirect(url_for('run_view', run_id=run_id, back=key))
        data = {}
        for field in ('smart', 'asin', 'goodreads', 'hardcover'):
            val = (request.form.get(field) or '').strip()
            if not val: continue
            parsed = core.parse_fix_input(val)
            if parsed.get('error'): flash(f"{val}: {T(parsed['error'])}", 'error'); return redirect(url_for('problem', key=key))
            data.update({k: v for k, v in parsed.items() if k in ('asin', 'goodreads', 'hardcover')})
        if not data and action != 'retry':
            flash(T('Please enter an ASIN or a link.'), 'error'); return redirect(url_for('problem', key=key))
        if data: core.save_override(key, data)
        run_id, err = core.RUNNER.start('item', dry_run=(action == 'test'), only_items=[key],
                                        label=f"Single book: {res.get('title')}")
        if err: flash(T(err), 'error'); return redirect(url_for('problem', key=key))
        return redirect(url_for('run_view', run_id=run_id, back=key))
    return render_template('problem.html', key=key, r=res, ov=ov, changes=core.book_changes(key))


@app.route('/run/<run_id>/apply', methods=['POST'])
@login_required
def run_apply(run_id):
    """Apply the planned changes of a dry run: 'check' only compares with ABS, 'apply' writes (abs_apply.py)."""
    run = core.RUNNER.get(run_id) or abort(404)
    if not run.get('dry_run') or run.get('kind') not in ('full', 'item') or not re.fullmatch(r'[\w-]+', run_id): abort(400)
    really = request.form.get('mode') == 'apply'
    args = ['--run', run_id] + (['--apply'] if really else [])
    ids = request.form.getlist('sel')
    if request.form.get('selection') == '1':  # from the review page: only the ticked changes
        nums = sorted({int(x) for x in ids if x.isdigit()})
        if not nums: flash(T('Nothing selected.'), 'error'); return redirect(url_for('run_review', run_id=run_id))
        sel_path = os.path.join(core.REPORT_DIR, f'apply_select_{run_id}.json')
        with open(sel_path, 'w', encoding='utf-8') as f: json.dump(nums, f)
        args += ['--select', sel_path]
    n = f" ({len(ids)} selected)" if request.form.get('selection') == '1' else ''
    new_id, err = core.RUNNER.start('apply', undo_args=args, label=f"{'Apply test run' if really else 'Check test run'}{n}: {run.get('label')} ({run_id})")
    if err: flash(T(err), 'error'); return redirect(url_for('run_view', run_id=run_id))
    return redirect(url_for('run_view', run_id=new_id))


RISKY = {'asin': 'ASIN', 'title': 'Titel', 'language': 'Sprache', 'series': 'Reihe', 'authors': 'Autoren', 'isbn': 'ISBN'}
LANG_CODES = {'eng': 'english', 'en': 'english', 'ger': 'german', 'deu': 'german', 'de': 'german', 'fre': 'french', 'fra': 'french', 'fr': 'french',
              'spa': 'spanish', 'es': 'spanish', 'ita': 'italian', 'it': 'italian', 'jpn': 'japanese', 'ja': 'japanese'}


def _lang(v): v = str(v or '').strip().lower(); return LANG_CODES.get(v, v)


@app.route('/run/<run_id>/review')
@login_required
def run_review(run_id):
    """Review page of a dry run: planned changes grouped by field, risky ones first, checkboxes for applying a selection."""
    run = core.RUNNER.get(run_id) or abort(404)
    if not run.get('dry_run') or not re.fullmatch(r'[\w-]+', run_id): abort(400)
    q, only = request.args.get('q', '').lower(), request.args.get('field', '')
    plan = []
    try:
        with open(os.path.join(core.REPORT_DIR, f'planned_changes_{run_id}.jsonl'), encoding='utf-8') as f:
            for n, l in enumerate(f):
                if not l.strip(): continue
                e = json.loads(l); e['n'] = n
                # risky = can change WHICH book the entry points to or what it is called (CLAUDE.md "riskant")
                e['risky'] = e['field'] in RISKY and not (e['field'] == 'isbn' and not e.get('old')) and not (e['field'] == 'series' and e.get('old'))
                if e['field'] == 'language': e['risky'] = bool(e.get('old')) and _lang(e.get('old')) != _lang(e.get('new'))  # 'eng' -> 'English' is no change
                if (q and q not in (e.get('title') or '').lower()) or (only and e['field'] != only): continue
                plan.append(e)
    except OSError: abort(404)
    groups = {}
    for e in plan: groups.setdefault(e['field'], []).append(e)
    order = sorted(groups, key=lambda f: (f not in RISKY, -len(groups[f])))
    return render_template('run_review.html', run=run, groups=[(f, groups[f]) for f in order], q=q, field=only,
                           risky=[e for e in plan if e['risky']], total=len(plan), risky_labels=RISKY)


@app.route('/history')
@login_required
def history():
    return render_template('history.html', runs=core.history())


@app.route('/history/<run_id>')
@login_required
def history_run(run_id):
    q, field = request.args.get('q', ''), request.args.get('field', '')
    entries = core.history_entries(run_id, q, field)
    fields = sorted({e.get('field') for e in core.history_entries(run_id)})
    return render_template('history_run.html', run_id=run_id, entries=entries, q=q, field=field, fields=fields)


@app.route('/history/<run_id>/undo', methods=['POST'])
@login_required
def history_undo(run_id):
    """Undo a whole run, one book of it, or one field of one book - always through abs_undo.py (journaled in undone.jsonl)."""
    item, field = request.form.get('item', ''), request.form.get('field', '')
    if not re.fullmatch(r'[\w-]+', run_id) or (item and not re.fullmatch(r'[\w-]+', item)) or (field and not re.fullmatch(r'\w+', field)):
        abort(400)
    args = ['--run', run_id] + (['--item', item] if item else []) + (['--field', field] if field else []) + ['--apply']
    what = request.form.get('label') or ('whole scan ' + run_id)
    run_id_u, err = core.RUNNER.start('undo', undo_args=args, label=f"Undo: {what}")
    if err: flash(T(err), 'error'); return redirect(url_for('history_run', run_id=run_id))
    return redirect(url_for('run_view', run_id=run_id_u))




@app.route('/review')
@login_required
def review_page():
    """The ONE review list (left menu): books (ASIN/title/Hardcover proposals), series, authors, other (duplicates)."""
    dec = core.decisions()
    merged = [m for m in core.merges().values() if dec.get(m['id'], {}).get('action') not in ('confirmed', 'undone')]
    merged.sort(key=lambda m: m.get('last_seen', ''), reverse=True)
    rv = core.review()
    queue = core.open_queue()
    res_all = core.results()
    for x in queue: x['res'] = res_all.get(x['key']) or {}
    rep = core._load_json(os.path.join(core.REPORT_DIR, 'library_report.json'), {})
    secs = {
        'buecher': {'label': T('Books'), 'entries': queue},
        'reihen': {'label': T('Series'), 'review': rv.get('series') or [], 'merged': [m for m in merged if m['kind'] == 'series']},
        'autoren': {'label': T('Authors'), 'review': rv.get('authors') or [], 'merged': [m for m in merged if m['kind'] == 'author']},
        'sonstiges': {'label': T('Other'), 'dups': rep.get('duplicate_recordings') or [], 'manual': core.manual_fields(), 'same_asin': core.same_asin_groups(), 'foreign': rv.get('other') or []},
    }
    counts = {'buecher': len(queue), 'reihen': len(secs['reihen']['review']) + len(secs['reihen']['merged']),
              'autoren': len(secs['autoren']['review']) + len(secs['autoren']['merged']), 'sonstiges': len(secs['sonstiges']['foreign']) + len(secs['sonstiges']['dups']) + len(secs['sonstiges']['same_asin'])}
    return render_template('review.html', secs=secs, counts=counts, generated=rv.get('generated'))


@app.route('/merged/<path:mid>/<action>', methods=['POST'])
@login_required
def merged_action(mid, action):
    m = core.merges().get(mid)
    if not m: abort(404)
    if action == 'confirm':
        core.set_decision(mid, 'confirmed'); flash(T("'%(a)s' → '%(b)s' confirmed.", a=m['member'], b=m['target']), 'ok')
        return redirect(url_for('review_page') + ('#autoren' if m['kind'] == 'author' else '#reihen'))
    if action == 'undo':
        core.set_decision(mid, 'undone')
        if m['kind'] == 'series': core.remove_alias(m['lib'], m['member'])
        if m.get('status') == 'applied':
            run_id, err = core.RUNNER.start('undo', undo_args=['--merge-id', mid, '--apply'],
                                            label=f"{T('Undo')}: {m['member']} → {m['target']}")
            if err: flash(T(err), 'error'); return redirect(url_for('review_page') + ('#autoren' if m['kind'] == 'author' else '#reihen'))
            return redirect(url_for('run_view', run_id=run_id))
        flash(T('Merge discarded (it was only planned).'), 'ok')
    return redirect(url_for('review_page') + ('#autoren' if m['kind'] == 'author' else '#reihen'))


@app.route('/queue/<path:qid>/<action>', methods=['POST'])
@login_required
def queue_action(qid, action):
    """Prüfliste: accept = take the proposal, reject = keep the book as it is (the script does not propose it again)."""
    if action not in ('accept', 'reject'): abort(400)
    item = next((x for x in core.open_queue() if x['id'] == qid), None)
    if not item: abort(404)
    if action == 'accept' and item.get('kind') == 'asin' and item.get('candidate'):
        core.save_override(item['key'], {'asin': item['candidate']})
        flash(T('ASIN %(a)s saved: it is written for this book with the next scan (and locked). Use "Test run first" on the book page to check it before.', a=item['candidate']), 'ok')
    elif action == 'accept' and item.get('kind') == 'hardcover' and item.get('candidate'):
        core.save_override(item['key'], {'hardcover': item['candidate']})
        flash(T('Hardcover link saved: it is used for this book from the next scan on.'), 'ok')
    elif action == 'accept' and item.get('kind') in ('title', 'isbn') and item.get('candidate'):
        # write through abs_apply.py (value check against ABS, journal, undo) - no new Audible request
        field = item['kind']
        rid = 'review-' + datetime.now().strftime('%Y%m%d-%H%M%S')
        iid = item['key'].split('_', 1)[1]
        if field == 'isbn':
            if not core.isbn_valid(item['candidate']):
                flash(T('%(c)s is no valid ISBN - nothing written.', c=item['candidate']), 'error'); return redirect(url_for('review_page') + '#buecher')
            try: old = ((core.abs_get(core.load_settings(), f'/api/items/{iid}').json().get('media') or {}).get('metadata') or {}).get('isbn')
            except Exception: flash(T('ABS not reachable.'), 'error'); return redirect(url_for('review_page') + '#buecher')
        else: old = item.get('title')
        os.makedirs(core.REPORT_DIR, exist_ok=True)
        with open(os.path.join(core.REPORT_DIR, f'planned_changes_{rid}.jsonl'), 'w', encoding='utf-8') as f:
            f.write(json.dumps({'run': rid, 'ts': datetime.now().isoformat(timespec='seconds'), 'item': iid, 'key': item['key'], 'title': item.get('title'),
                                'field': field, 'old': old, 'new': item['candidate'], 'source': 'review_list', 'dry_run': True}, ensure_ascii=False) + '\n')
        core.set_queue_decision(qid, action)
        new_id, err = core.RUNNER.start('apply', undo_args=['--run', rid, '--apply'], label=f"{field.capitalize()} from the review list: {item.get('title')}",
                                             run_settings={'ABS_BACKUP': False})  # the user checked it himself: no 3-minute ABS backup (undo stays possible)
        if err: flash(T(err), 'error'); return redirect(url_for('review_page') + '#buecher')
        return redirect(url_for('run_view', run_id=new_id))
    elif action == 'accept':
        flash(T('Proposal confirmed: %(c)s. This kind is not written automatically yet - please change it in ABS.', c=item.get('candidate')), 'ok')
    else:
        flash(T('Proposal rejected: the book stays unchanged.'), 'ok')
    core.set_queue_decision(qid, action)
    return redirect(url_for('review_page') + '#buecher')


@app.route('/review/<path:mid>/<action>', methods=['POST'])
@login_required
def review_action(mid, action):
    if action == 'remove_series':  # review list Other: take this series off the book (abs_apply: value check, journal, undo)
        x = next((r for r in core.review().get('other', []) if r.get('id') == mid), None)
        if not x: abort(404)
        try: cur = ((core.abs_get(core.load_settings(), f"/api/items/{x['item']}").json().get('media') or {}).get('metadata') or {}).get('series') or []
        except Exception: flash(T('ABS not reachable.'), 'error'); return redirect(url_for('review_page') + '#sonstiges')
        new = [{'name': s.get('name'), 'sequence': s.get('sequence')} for s in cur if s.get('name') != x['series']]
        old = [{'name': s.get('name'), 'sequence': s.get('sequence')} for s in cur]
        rid = 'review-' + datetime.now().strftime('%Y%m%d-%H%M%S')
        os.makedirs(core.REPORT_DIR, exist_ok=True)
        with open(os.path.join(core.REPORT_DIR, f'planned_changes_{rid}.jsonl'), 'w', encoding='utf-8') as f:
            f.write(json.dumps({'run': rid, 'ts': datetime.now().isoformat(timespec='seconds'), 'item': x['item'], 'key': x['key'], 'title': x['title'],
                                'field': 'series', 'old': old, 'new': new, 'source': 'review_list', 'dry_run': True}, ensure_ascii=False) + '\n')
        core.set_decision(mid, 'removed')
        new_id, err = core.RUNNER.start('apply', undo_args=['--run', rid, '--apply'], label=f"Series removed from the review list: {x['title']}", run_settings={'ABS_BACKUP': False})
        if err: flash(T(err), 'error'); return redirect(url_for('review_page') + '#sonstiges')
        return redirect(url_for('run_view', run_id=new_id))
    if action not in ('merge', 'keep', 'merge_as'): abort(400)
    if action == 'merge_as':  # merge, but the candidate's name becomes the target ('choose the target name')
        x = next((r for k in ('series', 'authors') for r in core.review().get(k, []) if r.get('id') == mid), None)
        if not x: abort(404)
        core.set_decision(mid, 'merge', target=x.get('series'))
    else: core.set_decision(mid, action)
    flash(T('Decision saved. It is applied with the next scan.'), 'ok')
    return redirect(url_for('review_page') + ('#autoren' if mid.startswith('author|') else '#sonstiges' if mid.startswith('other|') else '#reihen'))


# ---------------------------------------------------------------- runs
@app.route('/runs', methods=['GET', 'POST'])
@login_required
def runs():
    if request.method == 'POST':
        a = request.form.get('action')
        if a == 'stop':
            core.RUNNER.stop(); flash(T('The scan is being stopped.'), 'ok'); return redirect(url_for('runs'))
        rs, items = None, None
        if a == 'start':  # scan page: tasks as checkboxes (metadata & ratings / authors / series) + test-run switch
            core.save_scan_form(request.form)  # remembered for the next visit (never changes the settings)
            meta, authors, series, roles, empty = (request.form.get(k) == 'on' for k in ('task_meta', 'task_authors', 'task_series', 'task_roles', 'task_empty'))
            rm = [src for src in ('audible', 'goodreads', 'hardcover') if request.form.get(f'rm_{src}') == 'on']
            if rm:  # remove-ratings task: runs alone (no searches, no metadata)
                dry = request.form.get('dry') == 'on'
                s_ = core.load_settings()
                items, note = (None, T('all books'))
                if request.form.get('custom') == '1' and request.form.get('selection', 'all') != 'all':
                    items, note = core.select_items(s_, request.form.get('selection'), request.form.get('list', ''), limit=5000)
                    if not items: flash(T('No books for this selection (%(n)s).', n=note), 'error'); return redirect(url_for('runs'))
                run_id, err = core.RUNNER.start('full', dry_run=dry, only_items=items, label=f"{'Test run' if dry else 'Scan (real)'}: remove ratings ({', '.join(rm)}) – {note}",
                                                run_settings={'REMOVE_RATINGS': ','.join(rm), 'ABS_BACKUP': request.form.get('backup') == 'on'})
                if err: flash(T(err), 'error'); return redirect(url_for('runs'))
                return redirect(url_for('run_view', run_id=run_id))
            if not (meta or authors or series or roles or empty): flash(T('Please choose at least one task.'), 'error'); return redirect(url_for('runs'))
            dry = request.form.get('dry') == 'on'
            kind = 'full' if meta else 'series'  # without metadata only the series/author task runs (whole library)
            tasks = [t for t, on in (('metadata', meta), ('authors', authors), ('series', series), ('only real authors', roles), ('empty authors', empty)) if on]
            label = f"{'Test run' if dry else 'Scan (real)'}: {', '.join(tasks)}"
            rs = {'SERIES_MERGE': series, 'AUTHOR_NORMALIZE': authors, 'REMOVE_ROLE_CREDITS': roles, 'REMOVE_EMPTY_AUTHORS': empty,
                  'ABS_BACKUP': request.form.get('backup') == 'on'}
        else:  # older buttons (kept for links / scripts)
            kind = {'run': 'full', 'dry': 'full', 'series': 'series'}.get(a)
            if not kind: abort(400)
            dry = True if a == 'dry' else (False if a == 'run' else None)
            label = {'run': 'Scan (real)', 'dry': 'Test run', 'series': 'Series & report only'}[a]
        if request.form.get('custom') == '1' and kind == 'full':  # options for THIS run only
            s = core.load_settings()
            def num(k):
                try: return max(0, int(request.form.get(k, s.get(k))))
                except (TypeError, ValueError): return s.get(k)
            rs = dict(rs or {}, BATCH_SIZE=num('BATCH_SIZE'), REFRESH_DAYS=num('REFRESH_DAYS'))
            for k in ('HARDCOVER_ENABLED', 'GOODREADS_ENABLED'): rs[k] = request.form.get(k) == 'on'
            if a != 'start': rs['SERIES_MERGE'] = request.form.get('SERIES_MERGE') == 'on'
            mode = request.form.get('selection', 'all')
            items, note = core.select_items(s, mode, request.form.get('list', ''), limit=min(rs['BATCH_SIZE'] or 300, 1500))
            if mode != 'all' and not items: flash(T('No books for this selection (%(n)s).', n=note), 'error'); return redirect(url_for('runs'))
            if items is None: note = f"up to {rs['BATCH_SIZE']} of all due books"  # 'all' picks from all due books, the batch size still limits it
            label += f" – {note}"
        run_id, err = core.RUNNER.start(kind, dry_run=dry, label=label, only_items=items, run_settings=rs)
        if err: flash(T(err), 'error'); return redirect(url_for('runs'))
        return redirect(url_for('run_view', run_id=run_id))
    s = core.load_settings()
    return render_template('runs.html', runs=core.RUNNER.runs()[:100], s=s, f=core.scan_form(s))


@app.route('/api/scan_form', methods=['POST'])
@login_required
def api_scan_form():
    """Scan page: every change is saved at once, so leaving the page (e.g. to the settings) keeps the choices."""
    core.save_scan_form(request.form)
    return jsonify(ok=True)


@app.route('/run/<run_id>')
@login_required
def run_view(run_id):
    run = core.RUNNER.get(run_id) or abort(404)
    changes = core.run_changes(run_id) if run['status'] != 'running' else []
    return render_template('run.html', run=run, changes=changes, back=request.args.get('back'))


@app.route('/api/run/<run_id>/progress')
@login_required
def api_run_progress(run_id):
    run = core.RUNNER.get(run_id) or abort(404)
    p = core.safe_path('log', run['log']) if run.get('log') else None
    return jsonify(status=run['status'], **(core.run_progress(p) if p else {}))


@app.route('/api/run/<run_id>/log')
@login_required
def api_run_log(run_id):
    run = core.RUNNER.get(run_id) or abort(404)
    name = run['log'] if os.path.exists(os.path.join(core.LOG_DIR, run['log'])) else run['console']
    text, off = core.read_tail(os.path.join(core.LOG_DIR, name), int(request.args.get('offset', 0)))
    return jsonify(text=text, offset=off, status=run['status'])


@app.route('/api/status')
@login_required
def api_status():
    nxt = core.next_run_time(core.load_settings())
    return jsonify(running=core.RUNNER.running(), current=core.RUNNER.current, next_run=nxt.isoformat() if nxt else None)


# ---------------------------------------------------------------- logs
@app.route('/logs')
@login_required
def logs():
    return render_template('logs.html', files=core.log_files())


@app.route('/logs/view/<kind>/<path:name>')
@login_required
def log_view(kind, name):
    p = core.safe_path(kind, name) or abort(404)
    return render_template('log_view.html', kind=kind, name=name, size=os.path.getsize(p))


@app.route('/api/log/<kind>/<path:name>')
@login_required
def api_log(kind, name):
    p = core.safe_path(kind, name) or abort(404)
    text, off = core.read_tail(p, int(request.args.get('offset', 0)))
    return jsonify(text=text, offset=off)


@app.route('/logs/download/<kind>/<path:name>')
@login_required
def log_download(kind, name):
    p = core.safe_path(kind, name) or abort(404)
    return send_file(p, as_attachment=True, download_name=name)


@app.route('/logs/zip')
@login_required
def logs_zip():
    return send_file(core.zip_logs(), as_attachment=True, download_name=f"abs_ratings_logs_{datetime.now():%Y%m%d-%H%M}.zip",
                     mimetype='application/zip')


@app.route('/books')
@login_required
def books():
    """All processed books with their certainty score (sortable, filterable)."""
    sort, direction, q = request.args.get('sort', 'certainty'), request.args.get('dir', 'asc'), (request.args.get('q') or '').lower()
    mx = request.args.get('max', type=int)
    rows = [dict(r, key=k) for k, r in core.results().items()
            if (not q or q in (r.get('title') or '').lower() or q in (r.get('author') or '').lower()) and (mx is None or (r.get('certainty') is not None and r['certainty'] <= mx))]
    keyf = {'certainty': lambda r: (r.get('certainty') is None, r.get('certainty') or 0), 'title': lambda r: (r.get('title') or '').lower(),
            'status': lambda r: r.get('status') or '', 'author': lambda r: (r.get('author') or '').lower()}.get(sort, lambda r: 0)
    rows.sort(key=keyf, reverse=(direction == 'desc'))
    return render_template('books.html', rows=rows[:1000], total=len(rows), sort=sort, direction=direction, q=q, mx=mx)


@app.route('/status')
@login_required
def status():
    return redirect(url_for('dashboard') + '#coverage')


def status_context():
    rows, generated = core.coverage()
    trend = core.coverage_trend()
    first = {k: v for k, v in (trend[0]['pct'] if len(trend) > 1 else {}).items()}
    return dict(rows=rows, generated=generated, api=core.api_status(), isbn_bad=core.isbn_report(), locks=core.lock_report(), first=first,
                first_ts=(trend[0]['ts'] if len(trend) > 1 else ''), points=len(trend), health=core.health())


@app.route('/report')
@login_required
def report():
    p = core.safe_path('report', 'library_report.html') or abort(404)
    return send_file(p)


# ---------------------------------------------------------------- settings
@app.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():
    s = core.load_settings()
    if request.method == 'POST':
        # one section per form: only the fields of THIS section are read (an unticked box elsewhere must not become "off")
        sec = request.form.get('section', '')
        groups_in = set(core.SECTIONS.get(sec, ()))
        for key, label, ftype, default, help_, group in core.FIELDS:
            if group not in groups_in: continue
            if ftype == 'secret':
                v = request.form.get(key, '')
                if v.strip(): s[key] = v.strip()
                elif request.form.get(f'clear_{key}'): s[key] = ''
            elif ftype == 'libraries':
                s[key] = ','.join(request.form.getlist(key)) or request.form.get(f'{key}_manual', '').strip()
            elif ftype == 'bool':
                s[key] = request.form.get(key) == 'on'
            else:
                try: s[key] = core.coerce(ftype, request.form.get(key, default))
                except ValueError: flash(T('%(l)s: invalid value', l=T(label)), 'error')
        if sec == 'Schedule':
            s['schedule_enabled'] = request.form.get('schedule_enabled') == 'on'
            s['schedule_mode'] = 'cron' if request.form.get('schedule_mode') == 'on' else 'simple'
            s['schedule_days'] = [int(d) for d in request.form.getlist('schedule_days')]
            s['schedule_time'] = request.form.get('schedule_time') or '02:30'
            s['schedule_cron'] = (request.form.get('schedule_cron') or '').strip() or '30 2 * * *'
        if sec == 'Notifications':
            s['apprise_url'] = (request.form.get('apprise_url') or '').strip()
            s['notify_on'] = request.form.get('notify_on', 'errors')
        if sec not in core.SECTIONS: flash(T('Unknown section.'), 'error'); return redirect(url_for('settings'))
        core.save_settings(s)
        flash(T('%(s)s: saved.', s=T(sec)), 'ok')
        return redirect(url_for('settings') + '#' + core.section_slug(sec))
    libs = core.abs_libraries(s) if s.get('ABS_URL') and s.get('API_TOKEN') else []
    groups = {}
    for f in core.FIELDS: groups.setdefault(f[5], []).append(f)
    return render_template('settings.html', s=s, groups=groups, sections=core.SECTIONS, slug=core.section_slug, adv=core.ADVANCED, providers=core.PROVIDERS,
                           libs=libs, selected=set((s.get('LIBRARY_IDS') or '').split(',')), next_run=core.next_run_time(s))


@app.route('/export.csv')
@login_required
def export_csv():
    """All checked books with status, ASIN, sources and certainty (from the last check) as CSV."""
    import csv, io
    from flask import Response
    buf = io.StringIO(); w = csv.writer(buf)
    w.writerow(['title', 'author', 'status', 'asin', 'audible_store', 'goodreads', 'hardcover', 'certainty', 'checked', 'reason'])
    for k, r in sorted(core.results().items(), key=lambda x: (x[1].get('title') or '').lower()):
        w.writerow([r.get('title'), r.get('author'), r.get('status'), r.get('asin'), r.get('domain'), r.get('goodreads'), r.get('hardcover'),
                    r.get('certainty'), r.get('ts'), r.get('reason')])
    return Response(buf.getvalue(), mimetype='text/csv', headers={'Content-Disposition': 'attachment; filename=abs_ratings_books.csv'})


@app.route('/settings/export')
@login_required
def settings_export():
    from flask import Response
    return Response(json.dumps(core.settings_export(), indent=2, ensure_ascii=False), mimetype='application/json',
                    headers={'Content-Disposition': 'attachment; filename=abs_ratings_settings.json'})


@app.route('/settings/import', methods=['POST'])
@login_required
def settings_import():
    f = request.files.get('file')
    try: n = core.settings_import(json.load(f))
    except Exception: flash(T('This is no settings file.'), 'error'); return redirect(url_for('settings') + '#interface')
    flash(T('%(n)s settings imported (tokens and password are never part of an export).', n=n), 'ok')
    return redirect(url_for('settings') + '#interface')


@app.route('/api/test/<what>', methods=['POST'])
@login_required
def api_test(what):
    s = core.load_settings()
    if what == 'abs':
        if request.form.get('url'): s['ABS_URL'] = request.form['url']
        if request.form.get('token'): s['API_TOKEN'] = request.form['token']
        ok, msg = core.abs_test(s, T)
    elif what == 'hardcover':
        ok, msg = core.hardcover_test(request.form.get('token') or s.get('HARDCOVER_TOKEN', ''), T)
    elif what == 'notify':
        url = request.form.get('url') or s.get('apprise_url')
        ok = core.notify(dict(s, apprise_url=url, notify_on='always'), 'ABS Ratings: Test', T('Notifications work.'), False)
        msg = T('Test message sent.') if ok else T('Sending failed (check the URL).')
    elif what == 'cron':
        tmp = dict(s, schedule_enabled=True, schedule_mode='cron', schedule_cron=request.form.get('cron', ''))
        nxt = core.next_run_time(tmp)
        ok, msg = bool(nxt), (T('Next run: %(t)s', t=f"{nxt:%a %d.%m.%Y %H:%M}") if nxt else T('Invalid cron expression'))
    else: abort(404)
    return jsonify(ok=ok, msg=msg)


@app.route('/health')
def health():
    ok = core.abs_healthy(core.load_settings())
    return (jsonify(status='ok' if ok else 'abs unreachable'), 200 if ok else 503)


@app.template_filter('dt')
def fmt_dt(v):
    if not v: return '–'
    if isinstance(v, str):
        try: v = datetime.fromisoformat(v)
        except ValueError: return v
    return v.strftime('%d.%m.%Y %H:%M')


@app.template_filter('size')
def fmt_size(n):
    for unit in ('B', 'KB', 'MB', 'GB'):
        if n < 1024: return f"{n:.0f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def main():
    os.makedirs(core.CONFIG_DIR, exist_ok=True)
    core.start_scheduler()
    from waitress import serve
    serve(app, host='0.0.0.0', port=int(os.getenv('PORT', 47823)), threads=8)


if __name__ == '__main__':
    main()
