"""Series merge / author normalisation WRITE + UNDO against the FAKE ABS (never the real one), whole library from the snapshot.
docker run --rm --network host -e LIB=<library id> -v $PWD:/src -v <empty dir>:/data -w /src <image> python -I tools/series_write_test.py
Checks: what is written (journal = fake ABS PATCHes), every write carries a merge_id, undo of ONE merge, undo of the whole run,
and that afterwards the fake ABS metadata of every touched book equals the snapshot again."""
import json, os, subprocess, sys, time, requests, collections
P = 18974; B = f'http://127.0.0.1:{P}'
fake = subprocess.Popen([sys.executable, '-I', 'tools/fake_abs.py', '--snapshot', 'tests/private/abs_snapshot.json', '--port', str(P), '--patch-log', '/data/patches.jsonl'],
                        cwd='/src', stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(5)
env = {**os.environ, 'ABS_URL': B, 'API_TOKEN': 'fake', 'DATA_DIR': '/data', 'LIBRARY_IDS': os.environ['LIB'], 'SERIES_ONLY': 'true', 'SERIES_MERGE': 'true',
       'AUTHOR_NORMALIZE': 'true', 'LIBRARY_REPORT': 'true', 'ABS_BACKUP': 'true', 'HARDCOVER_ENABLED': 'false', 'GOODREADS_ENABLED': 'false'}
snap = json.load(open('/src/tests/private/abs_snapshot.json'))['items']
r = subprocess.run([sys.executable, '-u', 'abs_ratings.py'], cwd='/src', env={**env, 'DRY_RUN': 'false', 'RUN_ID': 'SER1'}, capture_output=True, text=True)
print('run exit', r.returncode, '|', [l.split(' - ')[-1] for l in (r.stdout + r.stderr).splitlines() if 'Traceback' in l or 'ERROR' in l][:3])
ch = [json.loads(l) for l in open('/data/changes.jsonl') if l.strip()] if os.path.exists('/data/changes.jsonl') else []
print('journal:', len(ch), 'changes', dict(collections.Counter((e['field'], e['source']) for e in ch)))
print('without merge_id:', sum(1 for e in ch if not e.get('merge_id')), '| merges:', len({e.get('merge_id') for e in ch}))
for mid, n in collections.Counter(e.get('merge_id') for e in ch).most_common(40): print('  ', n, mid)
m1 = next(e['merge_id'] for e in ch if e.get('merge_id'))
u1 = subprocess.run([sys.executable, '-u', 'abs_undo.py', '--merge-id', m1, '--apply'], cwd='/src', env=env, capture_output=True, text=True)
print('undo one merge:', u1.stdout.strip().splitlines()[-1])
u2 = subprocess.run([sys.executable, '-u', 'abs_undo.py', '--run', 'SER1', '--apply'], cwd='/src', env=env, capture_output=True, text=True)
print('undo run:', u2.stdout.strip().splitlines()[-1])
bad = 0
for iid in {e['item'] for e in ch}:
    now = requests.get(f'{B}/api/items/{iid}', params={'expanded': 1}).json()['media']['metadata']; old = snap[iid]['media']['metadata']
    norm = lambda xs, k: sorted(json.dumps({kk: x.get(kk) for kk in k}, sort_keys=True) for x in (xs or []))
    if norm(now.get('series'), ('name', 'sequence')) != norm(old.get('series'), ('name', 'sequence')) or norm(now.get('authors'), ('name',)) != norm(old.get('authors'), ('name',)):
        bad += 1; print('  NOT restored:', old.get('title'), now.get('series'), old.get('series'))
print('books touched', len({e['item'] for e in ch}), '| not restored after undo:', bad)
fake.terminate()
