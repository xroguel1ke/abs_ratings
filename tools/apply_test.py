"""End-to-end test of abs_apply.py against the fake ABS (recordings only): dry run -> manual edit (conflict) -> check -> apply -> undo.
Run inside the image: docker run --rm --network host -e LIB=<library id> -v $PWD:/src -v <empty dir>:/data -w /src <image> python -I tools/apply_test.py (keys in /data/keys.txt)"""
import json, os, subprocess, sys, time, requests, glob
P = 18976; B = f'http://127.0.0.1:{P}'
fake = subprocess.Popen([sys.executable, '-I', 'tools/fake_abs.py', '--snapshot', 'tests/private/abs_snapshot.json', '--port', str(P), '--patch-log', '/data/patches.jsonl'], cwd='/src', stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(4)
keys = open('/data/keys.txt').read().split()
base = {**os.environ, 'ABS_URL': B, 'API_TOKEN': 'fake', 'DATA_DIR': '/data', 'LIBRARY_IDS': os.environ['LIB'], 'HARDCOVER_ENABLED': 'false', 'GOODREADS_ENABLED': 'false',
        'RECORD_DIR': '/src/tests/private/recordings', 'RECORD_REUSE': '1', 'RECORD_NO_LIVE': 'audible,api.hardcover.app,www.goodreads.com,media-amazon', 'ABS_BACKUP': 'true'}
def run(cmd, **env): return subprocess.run(cmd, cwd='/src', env={**base, **env}, capture_output=True, text=True)
r = run([sys.executable, '-u', 'abs_ratings.py'], DRY_RUN='true', ONLY_ITEMS=','.join(keys), RUN_ID='DRY1')
plan = [json.loads(l) for l in open('/data/reports/planned_changes_DRY1.jsonl')]
print('dry run planned', len(plan), 'changes for', len({e['item'] for e in plan}), 'books')
victim = next(e for e in plan if e['field'] == 'title' or e['field'] == 'genres')
requests.patch(f"{B}/api/items/{victim['item']}/media", json={'metadata': {victim['field']: 'EDITED BY HAND' if victim['field'] == 'title' else ['Hand']}})
print('conflict created on', victim['title'][:40], victim['field'])
open('/data/patches.jsonl', 'w').close()
c = run([sys.executable, '-u', 'abs_apply.py', '--run', 'DRY1'], RUN_ID='CHK1')
print('CHECK:', [l for l in c.stdout.splitlines() + c.stderr.splitlines() if 'SUMMARY' in l][-1].split('SUMMARY')[-1], '| writes:', sum(1 for _ in open('/data/patches.jsonl')))
a = run([sys.executable, '-u', 'abs_apply.py', '--run', 'DRY1', '--apply'], RUN_ID='APP1')
out = a.stdout + a.stderr
print('APPLY:', [l for l in out.splitlines() if 'SUMMARY' in l][-1].split('SUMMARY')[-1]); print([l.split(' - ')[-1] for l in out.splitlines() if 'conflict' in l][:2])
ch = [json.loads(l) for l in open('/data/changes.jsonl') if '"run": "APP1"' in l]
print('journal APP1', len(ch), '| fake ABS got', dict(__import__('collections').Counter(json.loads(l)['kind'] for l in open('/data/patches.jsonl'))))
u = run([sys.executable, '-u', 'abs_undo.py', '--run', 'APP1', '--apply'])
print('UNDO:', u.stdout.strip().splitlines()[-1])
a2 = run([sys.executable, '-u', 'abs_apply.py', '--run', 'DRY1'], RUN_ID='CHK2')
print('CHECK after undo:', [l for l in (a2.stdout + a2.stderr).splitlines() if 'SUMMARY' in l][-1].split('SUMMARY')[-1])
fake.terminate()
