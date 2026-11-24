"""A run started from another process (docker exec) must block the scheduler's quick run and must not stay 'running' for ever
(2026-11-24: a quick run ran at the same time as a review-list run; two entries stayed 'running'). Run: python -m pytest tests/test_runner_other_process.py"""
import json, os, subprocess, sys, tempfile, time
cfg = tempfile.mkdtemp()
os.environ['CONFIG_DIR'] = cfg
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
import webui.core as C


def test_script_process_of_another_process_counts_as_running(tmp_path):
    script = tmp_path / 'abs_ratings.py'; script.write_text('import time; time.sleep(30)\n')
    p = subprocess.Popen([sys.executable, '-u', str(script)])
    try:
        for _ in range(50):
            if p.pid in C.other_script_pids(): break
            time.sleep(0.05)
        assert p.pid in C.other_script_pids()
        r = C.RunManager(); assert r.running()
        assert r.start('item', only_items=['x'])[1] == 'A scan is already running.'
        env = dict(os.environ, GR_BACKGROUND='1')
        g = subprocess.Popen([sys.executable, '-u', str(script)], env=env)
        try:
            time.sleep(0.3); assert g.pid not in C.other_script_pids()  # the Goodreads background mode is stopped by start() itself
        finally: g.kill()
    finally: p.kill(); p.wait()
    assert p.pid not in C.other_script_pids()


def test_stale_running_entry_is_finished_from_its_log(monkeypatch):
    os.makedirs(C.LOG_DIR, exist_ok=True)
    open(os.path.join(C.LOG_DIR, 'run_1.log'), 'w').write('...\n  Errors: 0\n  Duration: 26s\n')
    open(os.path.join(C.LOG_DIR, 'run_2.log'), 'w').write('... died\n')
    C._save_json(C.RUNS_FILE, [{'id': '1', 'status': 'running', 'end': None, 'log': 'run_1.log'},
                               {'id': '2', 'status': 'running', 'end': None, 'log': 'run_2.log'},
                               {'id': '0', 'status': 'ok', 'end': 'x', 'log': 'run_0.log'}])
    monkeypatch.setattr(C, 'other_script_pids', lambda own=None: [])
    runs = C.RunManager().runs()
    assert [(r['id'], r['status']) for r in runs] == [('1', 'ok'), ('2', 'stopped'), ('0', 'ok')] and runs[0]['end']
    assert json.load(open(C.RUNS_FILE))[0]['status'] == 'ok'
    monkeypatch.setattr(C, 'other_script_pids', lambda own=None: [123])  # a script still runs: entries are left alone
    C._save_json(C.RUNS_FILE, [{'id': '3', 'status': 'running', 'end': None, 'log': 'run_1.log'}])
    assert C.RunManager().runs()[0]['status'] == 'running'
