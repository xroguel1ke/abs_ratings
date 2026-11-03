"""
Undo changes made by abs_ratings.py, using changes.jsonl.

  python3 abs_undo.py --list                       # show runs with their number of changes
  python3 abs_undo.py --run 20261103-031927        # show what would be undone (nothing is changed)
  python3 abs_undo.py --run 20261103-031927 --apply
  python3 abs_undo.py --item <ABS item id> --apply # undo all changes of one book
  python3 abs_undo.py --run <id> --field series --apply   # only one field (e.g. series, title, description)

Needs ABS_URL and API_TOKEN as environment variables (same as abs_ratings.py).
Changes are undone newest first, so a field ends up with the value it had before the selected changes.
Undone entries are recorded in undone.jsonl and skipped next time.
"""
import os, sys, json, argparse
from collections import Counter
import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CHANGES_FILE = os.path.join(SCRIPT_DIR, "changes.jsonl")
UNDONE_FILE = os.path.join(SCRIPT_DIR, "undone.jsonl")


def load(path):
    if not os.path.exists(path): return []
    with open(path, encoding='utf-8') as f:
        return [json.loads(l) for l in f if l.strip()]


def entry_id(e): return f"{e['run']}|{e['item']}|{e['field']}|{e['ts']}"


def main():
    ap = argparse.ArgumentParser(description="Undo abs_ratings.py changes")
    ap.add_argument('--list', action='store_true', help="list runs")
    ap.add_argument('--run', help="run id to undo")
    ap.add_argument('--item', help="ABS item id to undo")
    ap.add_argument('--field', help="only undo this field")
    ap.add_argument('--apply', action='store_true', help="really write to ABS (default: only show)")
    a = ap.parse_args()

    changes = load(CHANGES_FILE)
    done = {x['id'] for x in load(UNDONE_FILE)}
    if a.list or not (a.run or a.item):
        runs = Counter(e['run'] for e in changes if entry_id(e) not in done)
        if not runs: print("No changes recorded (or everything was undone already)."); return
        print("Run id            Changes")
        for run, n in sorted(runs.items()): print(f"{run}   {n}")
        return

    sel = [e for e in changes if entry_id(e) not in done
           and (not a.run or e['run'] == a.run) and (not a.item or e['item'] == a.item)
           and (not a.field or e['field'] == a.field)]
    if not sel: print("Nothing to undo for this selection."); return

    url, token = os.getenv('ABS_URL', '').rstrip('/'), os.getenv('API_TOKEN')
    if a.apply and (not url or not token): sys.exit("ABS_URL and API_TOKEN must be set.")
    sess = requests.Session()
    sess.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})

    print(f"{'UNDO' if a.apply else 'Would undo (add --apply to execute)'}: {len(sel)} changes")
    ok = fail = 0
    for e in sorted(sel, key=lambda x: x['ts'], reverse=True):  # newest first
        old_show = e['old'] if e['field'] != 'description' else f"<{len(e['old'] or '')} chars>"
        print(f"  [{e['run']}] {e['title']}: {e['field']} -> {json.dumps(old_show, ensure_ascii=False)[:120]}")
        if not a.apply: continue
        try:
            if e['field'] == 'cover':
                r = sess.delete(f"{url}/api/items/{e['item']}/cover", timeout=30)
            elif e['field'] == 'tags':
                r = sess.patch(f"{url}/api/items/{e['item']}/media", json={"tags": e['old'] or []}, timeout=30)
            else:
                r = sess.patch(f"{url}/api/items/{e['item']}/media", json={"metadata": {e['field']: e['old']}}, timeout=30)
            if r.status_code == 200:
                ok += 1
                with open(UNDONE_FILE, 'a', encoding='utf-8') as f:
                    f.write(json.dumps({'id': entry_id(e)}) + "\n")
            else:
                fail += 1; print(f"     ❌ HTTP {r.status_code}: {r.text[:150]}")
        except requests.RequestException as ex:
            fail += 1; print(f"     ❌ {ex}")
    if a.apply: print(f"Done: {ok} undone, {fail} failed.")


if __name__ == "__main__":
    main()
