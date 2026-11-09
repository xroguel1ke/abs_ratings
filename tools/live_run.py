"""Real scan with the CURRENT dev code against the real ABS (no container update needed). Started by Claude for the user's
fix loop (user request 2026-11-08). Same settings as the web UI (webui.core.script_env), the run appears in the web UI scan list.

  docker run --rm --name abs_ratings_test_live --network host -v <config>:/config -v <dev repo>:/src:ro -w /src <image> \
      python -I tools/live_run.py [--only KEY,KEY] [--dry]

Refuses to start while another scan is running (web UI run list). Full pass: REFRESH_DAYS=0, BATCH_SIZE 2500, ABS backup first."""
import argparse, json, os, subprocess, sys
from datetime import datetime

os.environ.setdefault('CONFIG_DIR', '/config')
sys.path.insert(0, '/src')
from webui import core  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument('--only', default='', help='comma separated item keys (single books)')
ap.add_argument('--dry', action='store_true', help='test run (nothing is written)')
ap.add_argument('--env', action='append', default=[], help='KEY=VALUE override for this run (tests)')
ap.add_argument('--series-only', action='store_true', help='only the ABS library task (series, authors, genres, report)')
ap.add_argument('--clear-stale', action='store_true', help="mark this tool's runs that still say 'running' as stopped (killed run)")
a = ap.parse_args()

runs = core.RUNNER.runs()
if a.clear_stale:
    for r in runs[:10]:
        if r.get('status') == 'running' and r.get('trigger') == 'claude':
            r.update(status='stopped', end=datetime.now().isoformat(timespec='seconds')); core.RUNNER._save_run(r); print('stale run marked stopped:', r['id'])
    sys.exit(0)
if any(r.get('status') == 'running' for r in runs[:5]):
    print('another scan is running - not started'); sys.exit(2)
s = core.load_settings()
s.update({'REFRESH_DAYS': 0, 'BATCH_SIZE': 2500, 'ABS_BACKUP': True, 'HARDCOVER_ENABLED': True})
run_id = datetime.now().strftime('%Y%m%d-%H%M%S')
extra = {'RUN_ID': run_id, 'DRY_RUN': 'true' if a.dry else 'false'}
if a.only: extra['ONLY_ITEMS'] = a.only
if a.series_only: extra['SERIES_ONLY'] = 'true'
extra.update(dict(x.split('=', 1) for x in a.env))
env = core.script_env(s, extra)
env['PYTHONFAULTHANDLER'] = '1'  # a crash (SIGSEGV seen 2026-11-08) prints the Python stack into the console log
log = f"run_{run_id}.log"
run = {'id': run_id, 'kind': 'full', 'label': 'Scan (Claude fix loop)' + (' - test run' if a.dry else ''), 'dry_run': a.dry, 'trigger': 'claude',
       'items': a.only.split(',')[:50] if a.only else [], 'n_items': len(a.only.split(',')) if a.only else 0, 'run_settings': {},
       'start': datetime.now().isoformat(timespec='seconds'), 'end': None, 'status': 'running', 'log': log, 'console': f"console_{run_id}.log", 'summary': ''}
core.RUNNER._save_run(run)
print('run', run_id, flush=True)
with open(os.path.join(core.LOG_DIR, run['console']), 'w', encoding='utf-8') as out:
    rc = subprocess.call([sys.executable, '-u', '/src/abs_ratings.py'], cwd='/src', env=env, stdout=out, stderr=subprocess.STDOUT)
summary = core.parse_env_summary()
run.update(end=datetime.now().isoformat(timespec='seconds'), returncode=rc,
           status='ok' if rc == 0 and summary.get('ABS_ICON') != 'alert' else 'error', summary=summary.get('ABS_REPORT_BODY', ''), subject=summary.get('ABS_SUBJECT', ''))
core.RUNNER._save_run(run)
print('done', run_id, run['status'], flush=True)
