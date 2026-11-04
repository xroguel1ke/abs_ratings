"""ABS Ratings web UI (Flask). Start: python -m webui.app (port 47823, data folder /config)."""
import json, os, re, functools
from collections import Counter
from datetime import datetime

from flask import Flask, render_template, request, redirect, url_for, session, jsonify, send_file, abort, flash

from webui import core

app = Flask(__name__)
app.secret_key = core.load_settings()['secret_key']
app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE='Lax')

PROBLEM_STATUS = {'not_found': 'Nicht gefunden', 'error': 'Fehler', 'other_edition': 'Andere Ausgabe',
                  'no_ratings_yet': 'Noch keine Bewertungen'}
STATUS_LABEL = dict(PROBLEM_STATUS, ok='OK', ignored='Ignoriert')
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
    if not text: return text
    if text.startswith('Ratings of another edition'): return text.replace('Ratings of another edition', 'Bewertungen einer anderen Ausgabe').replace('ASIN not available anymore', 'ASIN nicht mehr verfügbar')
    return REASON_DE.get(text, text)
app.jinja_env.filters['de'] = de


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
            'status_label': STATUS_LABEL, 'abs_base': (s.get('ABS_URL') or '').rstrip('/')}


# ---------------------------------------------------------------- auth
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if core.check_password(core.load_settings(), request.form.get('password')):
            session.clear(); session['auth'] = True; session.permanent = True
            return redirect(request.args.get('next') or url_for('dashboard'))
        flash('Falsches Passwort.', 'error')
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
            flash('Altes Passwort falsch.', 'error')
        elif len(pw) < 6 or pw == 'admin': flash('Mindestens 6 Zeichen und nicht "admin".', 'error')
        elif pw != pw2: flash('Die Passwörter stimmen nicht überein.', 'error')
        else:
            core.set_password(s, pw); flash('Passwort geändert.', 'ok'); return redirect(url_for('dashboard'))
    return render_template('password.html', forced=s.get('must_change_password'))


# ---------------------------------------------------------------- dashboard
@app.route('/')
@login_required
def dashboard():
    tab = request.args.get('tab', 'problems')
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
    runs = core.RUNNER.runs()
    last = next((r for r in runs if r['kind'] in ('full', 'series') and r['status'] != 'running'), None)
    gr_note = ''
    if last and 'Goodreads blocked' in (last.get('summary') or ''): gr_note = 'gesperrt (letzter Lauf)'
    tiles = {'books': len(res), 'ok': counts.get('ok', 0), 'problems': sum(counts.get(k, 0) for k in PROBLEM_STATUS),
             'other_edition': counts.get('other_edition', 0), 'not_found': counts.get('not_found', 0),
             'no_ratings_yet': counts.get('no_ratings_yet', 0), 'ignored': counts.get('ignored', 0),
             'merged': len(merged), 'review': len(rv.get('series', [])) + len(rv.get('authors', [])),
             'queue': len(queue),
             'history': core.history_count()}
    return render_template('dashboard.html', tab=tab, tiles=tiles, problems=problems, merged=merged, review=rv, queue=queue,
                           last=last, gr_note=gr_note, status_filter=status_filter, q=q, counts=counts,
                           settings=core.load_settings())


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
            flash('Buch wird ignoriert.', 'ok'); return redirect(url_for('dashboard'))
        if action == 'unignore':
            core.save_override(key, {'ignore': False}); flash('Ignorieren aufgehoben.', 'ok')
            return redirect(url_for('problem', key=key))
        data = {}
        for field in ('smart', 'asin', 'goodreads', 'hardcover'):
            val = (request.form.get(field) or '').strip()
            if not val: continue
            parsed = core.parse_fix_input(val)
            if parsed.get('error'): flash(f"{val}: {parsed['error']}", 'error'); return redirect(url_for('problem', key=key))
            data.update({k: v for k, v in parsed.items() if k in ('asin', 'goodreads', 'hardcover')})
        if not data and action != 'retry':
            flash('Bitte eine ASIN oder einen Link eingeben.', 'error'); return redirect(url_for('problem', key=key))
        if data: core.save_override(key, data)
        run_id, err = core.RUNNER.start('item', dry_run=(action == 'test'), only_items=[key],
                                        label=f"Einzelbuch: {res.get('title')}")
        if err: flash(err, 'error'); return redirect(url_for('problem', key=key))
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
        if not nums: flash('Nichts ausgewählt.', 'error'); return redirect(url_for('run_review', run_id=run_id))
        sel_path = os.path.join(core.REPORT_DIR, f'apply_select_{run_id}.json')
        with open(sel_path, 'w', encoding='utf-8') as f: json.dump(nums, f)
        args += ['--select', sel_path]
    n = f" ({len(ids)} ausgewählt)" if request.form.get('selection') == '1' else ''
    new_id, err = core.RUNNER.start('apply', undo_args=args, label=f"{'Testlauf übernehmen' if really else 'Testlauf prüfen'}{n}: {run.get('label')} ({run_id})")
    if err: flash(err, 'error'); return redirect(url_for('run_view', run_id=run_id))
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
    what = request.form.get('label') or ('ganzer Lauf ' + run_id)
    run_id_u, err = core.RUNNER.start('undo', undo_args=args, label=f"Rückgängig: {what}")
    if err: flash(err, 'error'); return redirect(url_for('history_run', run_id=run_id))
    return redirect(url_for('run_view', run_id=run_id_u))


@app.route('/merged/<path:mid>/<action>', methods=['POST'])
@login_required
def merged_action(mid, action):
    m = core.merges().get(mid)
    if not m: abort(404)
    if action == 'confirm':
        core.set_decision(mid, 'confirmed'); flash(f"'{m['member']}' → '{m['target']}' bestätigt.", 'ok')
        return redirect(url_for('dashboard', tab='merged'))
    if action == 'undo':
        core.set_decision(mid, 'undone')
        if m['kind'] == 'series': core.remove_alias(m['lib'], m['member'])
        if m.get('status') == 'applied':
            run_id, err = core.RUNNER.start('undo', undo_args=['--merge-id', mid, '--apply'],
                                            label=f"Rückgängig: {m['member']} → {m['target']}")
            if err: flash(err, 'error'); return redirect(url_for('dashboard', tab='merged'))
            return redirect(url_for('run_view', run_id=run_id))
        flash('Zusammenführung verworfen (war nur geplant).', 'ok')
    return redirect(url_for('dashboard', tab='merged'))


@app.route('/queue/<path:qid>/<action>', methods=['POST'])
@login_required
def queue_action(qid, action):
    """Prüfliste: accept = take the proposal, reject = keep the book as it is (the script does not propose it again)."""
    if action not in ('accept', 'reject'): abort(400)
    item = next((x for x in core.open_queue() if x['id'] == qid), None)
    if not item: abort(404)
    if action == 'accept' and item.get('kind') == 'asin' and item.get('candidate'):
        core.save_override(item['key'], {'asin': item['candidate']})
        flash(f"ASIN {item['candidate']} gemerkt: wird beim nächsten Lauf für dieses Buch geschrieben (und gesperrt). Mit \"Erst als Testlauf\" auf der Buchseite kannst du es vorher prüfen.", 'ok')
    elif action == 'accept':
        flash(f"Vorschlag bestätigt: {item.get('candidate')}. Für diese Art (ISBN/Titel) wird er noch nicht automatisch geschrieben — bitte in ABS übernehmen.", 'ok')
    else:
        flash('Vorschlag verworfen: das Buch bleibt unverändert.', 'ok')
    core.set_queue_decision(qid, action)
    return redirect(url_for('dashboard', tab='queue'))


@app.route('/review/<path:mid>/<action>', methods=['POST'])
@login_required
def review_action(mid, action):
    if action not in ('merge', 'keep'): abort(400)
    core.set_decision(mid, action)
    flash('Entscheidung gespeichert. Sie wird beim nächsten Lauf (oder "Nur Reihen & Report") angewendet.', 'ok')
    return redirect(url_for('dashboard', tab='review'))


# ---------------------------------------------------------------- runs
@app.route('/runs', methods=['GET', 'POST'])
@login_required
def runs():
    if request.method == 'POST':
        a = request.form.get('action')
        if a == 'stop':
            core.RUNNER.stop(); flash('Lauf wird abgebrochen.', 'ok'); return redirect(url_for('runs'))
        kind = {'run': 'full', 'dry': 'full', 'series': 'series'}.get(a)
        if not kind: abort(400)
        dry = True if a == 'dry' else (False if a == 'run' else None)
        label = {'run': 'Lauf (echt)', 'dry': 'Testlauf', 'series': 'Nur Reihen & Report'}[a]
        rs, items = None, None
        if request.form.get('custom') == '1' and kind == 'full':  # start dialog: settings for THIS run only
            s = core.load_settings()
            def num(k):
                try: return max(0, int(request.form.get(k, s.get(k))))
                except (TypeError, ValueError): return s.get(k)
            rs = {'BATCH_SIZE': num('BATCH_SIZE'), 'REFRESH_DAYS': num('REFRESH_DAYS')}
            for k in ('HARDCOVER_ENABLED', 'GOODREADS_ENABLED', 'SERIES_MERGE'): rs[k] = request.form.get(k) == 'on'
            mode = request.form.get('selection', 'all')
            items, note = core.select_items(s, mode, request.form.get('list', ''), limit=min(rs['BATCH_SIZE'] or 300, 1500))
            if mode != 'all' and not items: flash(f'Keine Bücher für diese Auswahl ({note}).', 'error'); return redirect(url_for('runs'))
            label += f" – {note}"
        run_id, err = core.RUNNER.start(kind, dry_run=dry, label=label, only_items=items, run_settings=rs)
        if err: flash(err, 'error'); return redirect(url_for('runs'))
        return redirect(url_for('run_view', run_id=run_id))
    return render_template('runs.html', runs=core.RUNNER.runs()[:100], s=core.load_settings())


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
    rows, generated = core.coverage()
    trend = core.coverage_trend()
    first = {k: v for k, v in (trend[0]['pct'] if len(trend) > 1 else {}).items()}
    return render_template('status.html', rows=rows, generated=generated, api=core.api_status(), isbn_bad=core.isbn_report(), first=first, first_ts=(trend[0]['ts'] if len(trend) > 1 else ''), points=len(trend))


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
                except ValueError: flash(f"{label}: ungültiger Wert", 'error')
        if sec == 'Zeitplan':
            s['schedule_enabled'] = request.form.get('schedule_enabled') == 'on'
            s['schedule_mode'] = 'cron' if request.form.get('schedule_mode') == 'on' else 'simple'
            s['schedule_days'] = [int(d) for d in request.form.getlist('schedule_days')]
            s['schedule_time'] = request.form.get('schedule_time') or '02:30'
            s['schedule_cron'] = (request.form.get('schedule_cron') or '').strip() or '30 2 * * *'
        if sec == 'Benachrichtigungen':
            s['apprise_url'] = (request.form.get('apprise_url') or '').strip()
            s['notify_on'] = request.form.get('notify_on', 'errors')
        if sec not in core.SECTIONS: flash('Unbekannter Bereich.', 'error'); return redirect(url_for('settings'))
        core.save_settings(s)
        flash(f'{sec}: gespeichert.', 'ok')
        return redirect(url_for('settings') + '#' + core.section_slug(sec))
    libs = core.abs_libraries(s) if s.get('ABS_URL') and s.get('API_TOKEN') else []
    groups = {}
    for f in core.FIELDS: groups.setdefault(f[5], []).append(f)
    return render_template('settings.html', s=s, groups=groups, sections=core.SECTIONS, slug=core.section_slug, adv=core.ADVANCED, providers=core.PROVIDERS,
                           libs=libs, selected=set((s.get('LIBRARY_IDS') or '').split(',')), next_run=core.next_run_time(s))


@app.route('/api/test/<what>', methods=['POST'])
@login_required
def api_test(what):
    s = core.load_settings()
    if what == 'abs':
        if request.form.get('url'): s['ABS_URL'] = request.form['url']
        if request.form.get('token'): s['API_TOKEN'] = request.form['token']
        ok, msg = core.abs_test(s)
    elif what == 'hardcover':
        ok, msg = core.hardcover_test(request.form.get('token') or s.get('HARDCOVER_TOKEN', ''))
    elif what == 'notify':
        url = request.form.get('url') or s.get('apprise_url')
        ok = core.notify(dict(s, apprise_url=url, notify_on='always'), 'ABS Ratings: Test', 'Benachrichtigungen funktionieren.', False)
        msg = 'Testnachricht gesendet.' if ok else 'Senden fehlgeschlagen (URL prüfen).'
    elif what == 'cron':
        tmp = dict(s, schedule_enabled=True, schedule_mode='cron', schedule_cron=request.form.get('cron', ''))
        nxt = core.next_run_time(tmp)
        ok, msg = bool(nxt), (f"Nächste Ausführung: {nxt:%a %d.%m.%Y %H:%M}" if nxt else 'Ungültiger Cron-Ausdruck')
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
