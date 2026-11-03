"""Offline: apply abs_ratings.asin_plausibility to every book of the snapshot using the fetched products (tests/private/asin_products.json).
Prints the 'wrong' and 'suspicious' books for hand review. Env is set so that importing abs_ratings has no side effects."""
import json, os, sys, collections
os.environ.setdefault('DATA_DIR', '/tmp/audit_eval'); os.environ.setdefault('ABS_URL', 'http://127.0.0.1:1'); os.environ.setdefault('API_TOKEN', 'x')
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
import abs_ratings as A
root = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'tests', 'private')
snap = json.load(open(os.path.join(root, 'abs_snapshot.json'), encoding='utf-8'))
prods = json.load(open(os.path.join(root, 'asin_products.json'), encoding='utf-8'))
cnt, rows = collections.Counter(), []
for iid, d in snap['items'].items():
    md = d['media']['metadata']; a = md.get('asin')
    if not a: cnt['no asin'] += 1; continue
    p = prods.get(a)
    if p is None: cnt['asin not found anywhere' if a in prods else 'not fetched'] += 1; rows.append(('missing', md['title'], a, '', [])); continue
    messy = A.parse_messy_title(md['title']); t = messy['title'] if messy else md['title']
    v, why = A.asin_plausibility(t, [x['name'] for x in md.get('authors') or []], p['title'], p.get('subtitle'), p['authors'], md.get('narrators') or [], p['narrators'], d['media'].get('duration'), (p.get('runtime_min') or 0) * 60)
    cnt[v] += 1
    if v != 'ok': rows.append((v, md['title'], a, p['title'] + (' | ' + (p.get('subtitle') or '')) + ' | ' + ', '.join(p['authors']) + f" | ABS authors: {', '.join(x['name'] for x in md.get('authors') or [])}", why))
print(dict(cnt))
for v, t, a, pt, why in sorted(rows, key=lambda r: r[0]):
    print(f'[{v}] {t[:55]!r} {a} -> {pt[:150]}  ({"; ".join(why)})')

# --- second check over the 'ok' books: volume number conflicts and runtime differences (other recording / other edition)
vol, rt = [], []
for iid, d in snap['items'].items():
    md = d['media']['metadata']; a = md.get('asin'); p = prods.get(a) if a else None
    if not p: continue
    if A.volume_conflict(md['title'], p['title'], p.get('subtitle')): vol.append((md['title'], p['title'], a))
    ad, pd = d['media'].get('duration'), (p.get('runtime_min') or 0) * 60
    if ad and pd and A.runtime_mismatch(ad, pd): rt.append((md['title'], a, round(ad / 60), p['runtime_min']))
print(f'\nvolume conflicts: {len(vol)} | runtime mismatches (>15 % and >20 min): {len(rt)}')
for t, pt, a in vol[:25]: print(f'  [volume] {t[:50]!r} {a} -> {pt[:60]!r}')
for t, a, x, y in sorted(rt, key=lambda r: -abs(r[2] - r[3]))[:25]: print(f'  [runtime] {t[:50]!r} {a} ABS {x} min vs Audible {y} min')
