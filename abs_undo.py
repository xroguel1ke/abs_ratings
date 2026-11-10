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
import os, re, sys, json, argparse
from collections import Counter
import requests

SCRIPT_DIR = os.getenv('DATA_DIR') or os.path.dirname(os.path.abspath(__file__))
CHANGES_FILE = os.path.join(SCRIPT_DIR, "changes.jsonl")
UNDONE_FILE = os.path.join(SCRIPT_DIR, "undone.jsonl")
LAST_WRITTEN_FILE = os.path.join(SCRIPT_DIR, "last_written.json")


def forget_written(undone):
    """An undone field is no longer 'ours': drop it from the change monitoring (last_written.json), else the next scan sees the old
    value as a change outside / a reverted value and writes the undone change again."""
    try: lw = json.load(open(LAST_WRITTEN_FILE, encoding='utf-8'))
    except (OSError, ValueError): return
    for key, field in undone:
        if field in (lw.get(key) or {}): lw[key].pop(field, None)
    tmp = LAST_WRITTEN_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f: json.dump(lw, f, ensure_ascii=False, indent=4)
    os.replace(tmp, LAST_WRITTEN_FILE)


def norm(field, v):
    """Comparable form of an ABS value / journal value (ABS stores '<br />' and '&amp;', sorts lists, adds ids)."""
    import html
    if field == 'description': return re.sub(r'\s+', ' ', re.sub(r'<br\s*/?>', '<br>', html.unescape(v or ''))).strip()
    if field in ('authors', 'narrators', 'genres', 'tags'):
        return sorted(str((x.get('name') if isinstance(x, dict) else x) or '').strip().lower() for x in (v or []))
    if field == 'chapters':
        return [(round(float(c.get('start') or 0), 1), str(c.get('title') or '')) for c in (v or [])]
    if field == 'series':
        return sorted((str(x.get('name') or '').strip().lower(), str(x.get('sequence') or '')) for x in (v or []))
    if v in (None, '', [], False) and field not in ('explicit', 'abridged'): return None
    return str(v).strip()


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
    ap.add_argument('--merge-id', help="undo one series/author merge (web UI)")
    ap.add_argument('--ids-file', help="JSON list of journal entry ids to undo (web UI history search)")
    ap.add_argument('--apply', action='store_true', help="really write to ABS (default: only show)")
    ap.add_argument('--force', action='store_true', help="undo even if the field was changed again since (newer change / by hand)")
    a = ap.parse_args()

    changes = load(CHANGES_FILE)
    done = {x['id'] for x in load(UNDONE_FILE)}
    ids = set(json.load(open(a.ids_file, encoding='utf-8'))) if a.ids_file else None
    if a.list or not (a.run or a.item or a.merge_id or ids):
        runs = Counter(e['run'] for e in changes if entry_id(e) not in done)
        if not runs: print("No changes recorded (or everything was undone already)."); return
        print("Run id            Changes")
        for run, n in sorted(runs.items()): print(f"{run}   {n}")
        return

    sel = [e for e in changes if entry_id(e) not in done
           and (not a.run or e['run'] == a.run) and (not a.item or e['item'] == a.item)
           and (not a.field or e['field'] == a.field) and (not a.merge_id or e.get('merge_id') == a.merge_id) and (ids is None or entry_id(e) in ids)]
    if not sel: print("Nothing to undo for this selection."); return

    url = re.sub(r'[^A-Za-z0-9:/._\-\[\]%]', '', os.getenv('ABS_URL', '')).rstrip('/')
    token = re.sub(r'[^A-Za-z0-9._\-]', '', os.getenv('API_TOKEN', ''))  # strip copy & paste artefacts
    if a.apply and (not url or not token): sys.exit("ABS_URL and API_TOKEN must be set.")
    sess = requests.Session()
    sess.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})

    print(f"{'UNDO' if a.apply else 'Would undo (add --apply to execute)'}: {len(sel)} changes")
    ok = fail = skipped = 0; undone_fields = []
    current = {}  # (item, field) -> value in ABS now (read once per book, then follows our own undo steps)
    def now_value(iid, field):
        if (iid, field) not in current:
            try:
                r = sess.get(f"{url}/api/items/{iid}", timeout=30)
                media = r.json().get('media') or {} if r.status_code == 200 else None
            except (requests.RequestException, ValueError): media = None
            if media is None: return '__unknown__'
            for f_, v_ in (media.get('metadata') or {}).items(): current.setdefault((iid, f_), v_)
            current.setdefault((iid, 'tags'), media.get('tags') or [])
            current.setdefault((iid, 'chapters'), media.get('chapters') or [])
            current.setdefault((iid, field), None)
        return current[(iid, field)]
    for e in sorted(sel, key=lambda x: x['ts'], reverse=True):  # newest first
        old_show = e['old'] if e['field'] != 'description' else f"<{len(e['old'] or '')} chars>"
        print(f"  [{e['run']}] {e['title']}: {e['field']} -> {json.dumps(old_show, ensure_ascii=False)[:120]}")
        if not a.apply: continue
        if e['field'] == 'author':  # an EMPTY author object was deleted (no book had it) - nothing to restore
            print('     (empty author removed - nothing to undo)'); continue
        if e['field'] != 'cover' and not a.force:
            cur = now_value(e['item'], e['field'])
            if cur != '__unknown__' and norm(e['field'], cur) != norm(e['field'], e.get('new')):
                # a NEWER change (another run, you, an ABS rescan) is in the field: undoing this old one would overwrite it
                print(f"     ⏭️ changed again since this change - skipped (newer value kept; undo the newer change first or use --force)"); skipped += 1; continue
        try:
            if e['field'] == 'cover' and e.get('old') and os.path.isfile(str(e['old'])):  # replaced cover: upload the backup again
                data = open(e['old'], 'rb').read()
                ext = 'png' if data[:8] == b'\x89PNG\r\n\x1a\n' else 'webp' if data[:4] == b'RIFF' and data[8:12] == b'WEBP' else 'jpg'
                # ABS accepts only png/jpg/jpeg/webp file names (older backups were saved as .img)
                r = requests.post(f"{url}/api/items/{e['item']}/cover", headers={"Authorization": f"Bearer {token}"},
                                  files={'cover': (f"cover.{ext}", data, 'image/png' if ext == 'png' else f'image/{ext}' if ext == 'webp' else 'image/jpeg')}, timeout=60)
            elif e['field'] == 'cover' and e.get('old'):
                print("     ❌ backup of the old cover is missing - skipped"); fail += 1; continue
            elif e['field'] == 'cover':
                r = sess.delete(f"{url}/api/items/{e['item']}/cover", timeout=30)
            elif e['field'] == 'chapters':
                old_c = [dict(c, id=i) for i, c in enumerate(e['old'] or [])]
                r = sess.post(f"{url}/api/items/{e['item']}/chapters", json={'chapters': old_c}, timeout=30)
            elif e['field'] == 'tags':
                r = sess.patch(f"{url}/api/items/{e['item']}/media", json={"tags": e['old'] or []}, timeout=30)
            else:
                r = sess.patch(f"{url}/api/items/{e['item']}/media", json={"metadata": {e['field']: e['old']}}, timeout=30)
            if r.status_code == 200:
                ok += 1; undone_fields.append((e.get('key'), e.get('field'))); current[(e['item'], e['field'])] = e.get('old')
                with open(UNDONE_FILE, 'a', encoding='utf-8') as f:
                    f.write(json.dumps({'id': entry_id(e)}) + "\n")
            else:
                fail += 1; print(f"     ❌ HTTP {r.status_code}: {r.text[:150]}")
        except requests.RequestException as ex:
            fail += 1; print(f"     ❌ {ex}")
    if a.apply and undone_fields: forget_written(undone_fields)
    if a.apply: print(f"Done: {ok} undone, {fail} failed{f', {skipped} skipped (changed again since)' if skipped else ''}.")


if __name__ == "__main__":
    main()
