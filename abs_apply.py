"""Apply the planned changes of a DRY RUN to ABS (web UI: "Testlauf übernehmen").

  python abs_apply.py --run <dry-run id> [--keys k1,k2] [--apply]

Safety:
- a change is written only if the ABS value is STILL the value the dry run saw ('old'); otherwise it is skipped as a conflict
  (the book was edited in the meantime - by you or by another run). Lock tags set since the dry run are respected.
- an ASIN is never written when another book of the library owns it by now.
- every write goes through abs_ratings.abs_update / abs_set_cover: journaled in changes.jsonl (new run id), covers backed up,
  so the history page / abs_undo.py can undo it.
- with ABS_BACKUP=true an ABS backup is created first; if it fails nothing is written.
Without --apply it only reports what it would do (nothing is written)."""
import argparse, json, logging, os, sys
from collections import Counter

os.environ.setdefault('DRY_RUN', 'false')
import abs_ratings as A
from abs_series import expand_locks

LOCK = {'chapters': 'lock_chapters', 'asin': 'lock_asin', 'title': 'lock_title', 'subtitle': 'lock_title', 'series': 'lock_series', 'language': 'lock_language',
        'genres': 'lock_genres', 'publisher': 'lock_publisher', 'publishedYear': 'lock_year', 'narrators': 'lock_narrators',
        'authors': 'lock_authors', 'cover': 'lock_cover', 'explicit': 'lock_explicit', 'abridged': 'lock_abridged',
        'isbn': 'lock_isbn', 'description': 'lock_description'}


def norm(field, v):
    """Comparable form of an ABS value / journal value."""
    if field == 'description': return A.normalize_desc(v or '')
    if field in ('authors', 'narrators', 'genres'):
        return sorted(str((x.get('name') if isinstance(x, dict) else x) or '').strip().lower() for x in (v or []))
    if field == 'chapters':
        return [(round(float(c.get('start') or 0), 1), str(c.get('title') or '')) for c in (v or [])]
    if field == 'series':
        return sorted((str(x.get('name') or '').strip().lower(), str(x.get('sequence') or '')) for x in (v or []))
    if v in (None, '', [], False) and field not in ('explicit', 'abridged'): return None
    return str(v).strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--run', required=True, help='id of the dry run')
    ap.add_argument('--keys', default='', help='only these item keys (comma separated)')
    ap.add_argument('--select', default='', help='JSON file with the line numbers (0-based) of the planned changes to apply (web UI checkboxes)')
    ap.add_argument('--apply', action='store_true', help='really write (default: report only)')
    a = ap.parse_args()
    A.DRY_RUN = not a.apply
    A.setup_logging()
    # change monitoring: what WE wrote last per field (abs_update keeps it up to date; without loading + saving it, the next scan took a
    # value written here for 'changed outside' and re-applied the older journal entry - Malice 2026-11-09)
    A.LAST_WRITTEN.update(A.rw_json(A.LAST_WRITTEN_FILE) or {})
    path = os.path.join(A.REPORT_DIR, f"planned_changes_{a.run}.jsonl")
    if not os.path.exists(path): logging.error(f"No planned changes for run {a.run} ({path})"); return 2
    plan = [dict(json.loads(l), _n=n) for n, l in enumerate(open(path, encoding='utf-8')) if l.strip()]
    if a.select:
        sel = set(json.load(open(a.select, encoding='utf-8')))
        plan = [e for e in plan if e['_n'] in sel]
    only = {k for k in a.keys.split(',') if k}
    if only: plan = [e for e in plan if e.get('key') in only]
    logging.info(f"{'APPLY' if a.apply else 'CHECK (nothing written)'} dry run {a.run}: {len(plan)} planned changes, {len({e['item'] for e in plan})} books")
    if a.apply and A.ABS_BACKUP and not A.create_abs_backup():
        logging.error("ABS backup failed - nothing was written."); return 3
    # who owns which ASIN NOW (an ASIN is never given to a second book)
    owners = {}
    for lib in {e['key'].partition('_')[0] for e in plan}:
        for it in A.abs_session.get(f"{A.ABS_URL}/api/libraries/{lib}/items").json().get('results') or []:
            x = ((it.get('media') or {}).get('metadata') or {}).get('asin')
            if x: owners.setdefault(x, set()).add(f"{lib}_{it['id']}")
    by_item, res = {}, Counter()
    for e in plan: by_item.setdefault(e['item'], []).append(e)
    for iid, changes in by_item.items():
        r = A.abs_session.get(f"{A.ABS_URL}/api/items/{iid}", params={'expanded': 1})
        if r.status_code != 200: logging.warning(f"  ⚠️ {changes[0]['title']}: not in ABS any more (HTTP {r.status_code}) - skipped"); res['missing'] += len(changes); continue
        item = r.json(); meta = (item.get('media') or {}).get('metadata') or {}
        tags = expand_locks(A.item_tags(item)); key, title = changes[0]['key'], changes[0]['title']
        updates, old_meta = {}, {}
        for e in changes:
            f = e['field']
            if LOCK.get(f) in tags or 'lock_all' in tags or ('lock_metadata' in tags and f != 'cover'):
                logging.info(f"  🔒 {title}: {f} locked since the dry run - skipped"); res['locked'] += 1; continue
            if f == 'cover':
                has_cover = bool((item.get('media') or {}).get('coverPath'))
                if e.get('old') is None and has_cover:
                    logging.info(f"  ⏭️ {title}: cover - ABS has a cover by now - skipped (conflict)"); res['conflict'] += 1; continue
                if a.apply and A.abs_set_cover(iid, key, title, e['new'], 'applied:' + a.run, replace=has_cover): res['applied'] += 1
                elif not a.apply: res['would'] += 1
                continue
            if f == 'chapters':  # own endpoint (abs_set_chapters): compared and written on their own
                cur_c = (item.get('media') or {}).get('chapters') or []
                if norm(f, cur_c) != norm(f, e.get('old')):
                    logging.info(f"  ⏭️ {title}: chapters changed since the dry run - skipped (conflict)"); res['conflict'] += 1; continue
                if a.apply and A.abs_set_chapters(iid, key, title, [dict(c, id=i) for i, c in enumerate(e['new'])], cur_c, 'applied:' + a.run): res['applied'] += 1
                elif not a.apply: res['would'] += 1
                continue
            cur = meta.get(f)
            if f == 'tags': cur = (item.get('media') or {}).get('tags')
            if f in updates: cur = updates[f]  # the same field twice in one run (book scan, then the library task): the 2nd starts from the 1st
            if norm(f, cur) != norm(f, e.get('old')):
                logging.info(f"  ⏭️ {title}: {f} changed since the dry run ({str(cur)[:60]!r} != {str(e.get('old'))[:60]!r}) - skipped (conflict)"); res['conflict'] += 1; continue
            if f == 'asin' and e.get('new') and (owners.get(e['new'], set()) - {key}):
                logging.info(f"  ⛔ {title}: ASIN {e['new']} belongs to another book by now - skipped"); res['conflict'] += 1; continue
            updates[f] = e['new']; old_meta.setdefault(f, cur)  # journal 'old' = the value ABS had before this apply
        if updates:
            if a.apply:
                if A.abs_update(iid, key, title, updates, old_meta, 'applied:' + a.run):
                    res['applied'] += len(updates); logging.info(f"  💾 {title}: {', '.join(updates)}")
                else: res['failed'] += len(updates)
            else:
                res['would'] += len(updates); logging.info(f"  ✓ {title}: would write {', '.join(updates)}")
    if a.apply and res['applied']: A.rw_json(A.LAST_WRITTEN_FILE, A.LAST_WRITTEN)
    logging.info(f"SUMMARY: {dict(res)}" + (f" | undo: python3 abs_undo.py --run {A.RUN_ID} --apply" if a.apply else ''))
    return 0 if not res['failed'] else 1


if __name__ == '__main__':
    sys.exit(main())
