"""Watchdog: notifies ONLY when the loop looks stalled (no file activity for STALE_HOURS) -> user must act.
Activity = newest mtime in the dev repo (excl. .git/__pycache__) and in the test data folder.
Read-only mounts: /src (dev repo), /testdata (abs_scripts_test), /config/settings.json (Apprise URL, never printed).
Env: STALE_HOURS (default 2), CHECK_MIN (default 5), REPEAT_HOURS (default 6), DRY=1 (print instead of sending, run once)."""
import json, os, sys, time

STALE = float(os.environ.get('STALE_HOURS', 2)) * 3600
CHECK = float(os.environ.get('CHECK_MIN', 5)) * 60
REPEAT = float(os.environ.get('REPEAT_HOURS', 6)) * 3600
DRY = os.environ.get('DRY') == '1'
ROOTS = ['/src', '/testdata']
SKIP = {'.git', '__pycache__'}

MSG = ("Die ABS-Schleife steht seit {h:.1f} h (Nutzungslimit oder Absturz). So gehts weiter: "
       "Server-Terminal öffnen, 'tmux attach -t abs' eingeben, dort 'weiter' tippen + Enter. "
       "Falls 'no sessions' (Server neu gestartet): 'tmux new -s abs', dann "
       "'cd /mnt/user/appdata/abs_ratings_dev && claude --continue', dort 'weiter'. "
       "Bei Limit: nach dem Reset 'weiter' senden. Stand: GitHub claude/matching-loop, docs/LOOP_LOG.md.")


def newest():
    best = 0.0
    for root in ROOTS:
        for d, dirs, files in os.walk(root):
            dirs[:] = [x for x in dirs if x not in SKIP]
            for f in files:
                try: best = max(best, os.path.getmtime(os.path.join(d, f)))
                except OSError: pass
    return best


def send(body):
    if DRY:
        print('WOULD SEND:', body); return True
    try:
        url = (json.load(open('/config/settings.json', encoding='utf-8')).get('apprise_url') or '').strip()
        import apprise
        a = apprise.Apprise(); a.add(url)
        return bool(a.notify(title='ABS Loop steht - bitte eingreifen', body=body))
    except Exception as e:
        print('send failed:', type(e).__name__, flush=True); return False


last_sent = 0.0
while True:
    age = time.time() - newest()
    if age >= STALE:
        if time.time() - last_sent >= REPEAT and send(MSG.format(h=age / 3600)): last_sent = time.time()
    else:
        last_sent = 0.0  # activity is back: re-arm
    print(f'{time.strftime("%H:%M")} idle {age/60:.0f} min', flush=True)
    if DRY: break
    time.sleep(CHECK)
