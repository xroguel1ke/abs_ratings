"""Reorganize the audiobook folder tree from a plan file (JSON). Default: dry run (prints what it would do); --apply executes.

  python tools/library_reorg.py --root /books --plan plan.json [--apply] [--log out.log]

Plan: {"moves": [[src, dst], ...],            # folders, relative to root; dst must not exist, its parent is created
       "author_merges": [[a, b], ...],        # author folders that are the same person (moves between them are allowed)
       "remove_no_audio": [dir, ...],         # leftover folders WITHOUT any audio file (e.g. only a cover) - refused if audio inside
       "delete_metadata_json": true,          # every metadata.json under root is archived to <root>/.abs_ratings_backup/ first
       "prune_empty": true}                   # remove folders that are empty afterwards (bottom-up)

Safety: nothing outside --root is touched; a move never leaves its author folder (first path part) unless the two authors are
listed in author_merges; no audio file is ever deleted: the audio inventory (name + size) before and after must match exactly,
otherwise the run reports it loudly. Moves are plain renames on the same file system (no copies)."""
import argparse, json, os, sys, tarfile, time

AUDIO = {'.m4b', '.mp3', '.m4a', '.opus', '.flac', '.ogg', '.aac', '.wma', '.wav'}


def audio_inventory(root):
    inv = {}
    for dp, dns, fns in os.walk(root):
        dns[:] = [d for d in dns if d != '.abs_ratings_backup']
        for f in fns:
            p = os.path.join(dp, f)
            if os.path.splitext(f)[1].lower() in AUDIO and not os.path.islink(p):  # links point elsewhere: never followed
                inv[os.path.relpath(p, root)] = os.path.getsize(p)
    return inv


def inside(root, rel):
    p = os.path.realpath(os.path.join(root, rel))
    return p == root or p.startswith(root + os.sep)


def has_audio(path):
    return any(os.path.splitext(f)[1].lower() in AUDIO or os.path.islink(os.path.join(dp, f)) for dp, _, fns in os.walk(path) for f in fns)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', required=True); ap.add_argument('--plan', required=True)
    ap.add_argument('--apply', action='store_true'); ap.add_argument('--log')
    a = ap.parse_args()
    root = os.path.realpath(a.root)
    plan = json.load(open(a.plan, encoding='utf-8'))
    out = open(a.log, 'w', encoding='utf-8') if a.log else None
    def log(s):
        print(s, flush=True)
        if out: out.write(s + '\n'); out.flush()
    mode = 'APPLY' if a.apply else 'DRY RUN'
    log(f"== library_reorg {mode} {time.strftime('%Y-%m-%d %H:%M:%S')} root={root}")
    merges = {frozenset(x) for x in plan.get('author_merges', [])}
    before = audio_inventory(root)
    log(f"audio files before: {len(before)} ({sum(before.values()) / 1e9:.1f} GB)")

    # 1) validate every move first (all or nothing)
    errors, moves, planned_dst = [], [], set()
    for src, dst in plan.get('moves', []):
        why = None
        if not (inside(root, src) and inside(root, dst)): why = 'outside root'
        elif not os.path.isdir(os.path.join(root, src)): why = 'source missing'
        elif os.path.exists(os.path.join(root, dst)) or dst in planned_dst: why = 'target exists'
        elif src.split('/')[0] != dst.split('/')[0] and frozenset((src.split('/')[0], dst.split('/')[0])) not in merges: why = 'would leave the author folder'
        if why: errors.append(f"  ✗ {src}  ->  {dst}: {why}")
        else: moves.append((src, dst)); planned_dst.add(dst)
    for e in errors: log(e)
    if errors:
        log(f"{len(errors)} invalid move(s): nothing is changed. Fix the plan first."); sys.exit(2)
    for d in plan.get('remove_no_audio', []):
        if not inside(root, d) or not os.path.isdir(os.path.join(root, d)): log(f"  ✗ remove {d}: missing/outside"); sys.exit(2)
        if has_audio(os.path.join(root, d)): log(f"  ✗ remove {d}: contains audio - refused"); sys.exit(2)

    # 2) metadata.json archive + delete
    mj = []
    if plan.get('delete_metadata_json'):
        for dp, dns, fns in os.walk(root):
            dns[:] = [d for d in dns if d != '.abs_ratings_backup']
            if 'metadata.json' in fns: mj.append(os.path.relpath(os.path.join(dp, 'metadata.json'), root))
        log(f"metadata.json files: {len(mj)} (archived to .abs_ratings_backup/, then deleted)")
        if a.apply and mj:
            os.makedirs(os.path.join(root, '.abs_ratings_backup'), exist_ok=True)
            arc = os.path.join(root, '.abs_ratings_backup', f"metadata_json_{time.strftime('%Y%m%d-%H%M%S')}.tar.gz")
            with tarfile.open(arc, 'w:gz') as t:
                for r in mj: t.add(os.path.join(root, r), arcname=r)
            for r in mj: os.remove(os.path.join(root, r))
            log(f"  archived to {os.path.relpath(arc, root)} and deleted")

    # 3) leftover folders without audio (before the moves: their paths are the current ones)
    for d in plan.get('remove_no_audio', []):
        log(f"  remove (no audio): {d}  [{', '.join(sorted(os.listdir(os.path.join(root, d))))}]")
        if a.apply:
            for dp, dns, fns in os.walk(os.path.join(root, d), topdown=False):
                for f in fns: os.remove(os.path.join(dp, f))
                for x in dns: os.rmdir(os.path.join(dp, x))
            os.rmdir(os.path.join(root, d))

    # 4) moves
    for src, dst in moves:
        log(f"  move  {src}\n     -> {dst}")
        if a.apply:
            os.makedirs(os.path.dirname(os.path.join(root, dst)), exist_ok=True)
            os.rename(os.path.join(root, src), os.path.join(root, dst))

    # 5) empty folders (bottom-up)
    pruned = 0
    if plan.get('prune_empty'):
        for dp, dns, fns in os.walk(root, topdown=False):
            if dp == root or '.abs_ratings_backup' in dp: continue
            left = [x for x in os.listdir(dp)] if a.apply else [x for x in os.listdir(dp)
                                                               if not (x == 'metadata.json' and plan.get('delete_metadata_json'))]
            if not left and not any(os.path.join(root, s) == dp for s, _ in moves):
                pruned += 1
                if a.apply: os.rmdir(dp)
        log(f"empty folders {'removed' if a.apply else 'to remove (after the moves; dry-run count is a lower bound)'}: {pruned}")

    # 6) audio check
    if a.apply:
        after = audio_inventory(root)
        mapped = {}
        for rel, size in before.items():
            new = rel
            for src, dst in moves:
                if new == src or new.startswith(src + '/'): new = dst + new[len(src):]
            mapped[new] = size
        missing = [r for r in mapped if after.get(r) != mapped[r]]
        log(f"audio files after: {len(after)} ({sum(after.values()) / 1e9:.1f} GB) | missing or changed: {len(missing)}")
        for r in missing[:20]: log(f"  !! {r}")
        if missing or len(after) != len(before): log("!!! AUDIO CHECK FAILED - see above"); sys.exit(3)
        log("audio check: every audio file is still there (same size)")
    log(f"== done ({mode}): {len(moves)} moves, {len(plan.get('remove_no_audio', []))} leftover folders, {len(mj)} metadata.json")


if __name__ == '__main__':
    main()
