"""Guard for the thread-safety rule (CLAUDE.md, "Parallel books"): with WORKERS=3 every per-book value must be thread-local.
A shared module-level dict produced wrong ASINs / ratings twice (2026-11-04/05). This test lists every module-level
dict / set / list of abs_ratings.py and fails when a NEW one appears that is not on the reviewed allowlist below.

A new name here means: make it thread-local (_TLDict / _TL) if it holds per-book values, or - when it is really run-wide
(caches, owner maps, file state guarded by IO_LOCK) - add it to KNOWN with the reason. Run: python -m pytest tests/test_global_state.py
"""
import ast, os

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'abs_ratings.py')
CONTAINER_CALLS = {'dict', 'set', 'list', 'defaultdict', 'OrderedDict', 'Counter', 'deque'}

# State of 2026-11-17, taken over as it was (run-wide caches, owner maps, file state, report lists, constants); per-book values are _TLDict.
KNOWN = {
    'ALL_MARKETS', 'ASIN_OWNERS', 'AUDIBLE_API_HOSTS', 'AUDIBLE_MARKETS', 'AUD_GENRES', 'AUD_PREFETCH',
    'AUTHOR_LINKS', 'AUTHOR_NOTES', 'BOOKS_DONE', 'BOOK_LANGUAGES', 'CHAPTER_REGION', 'COVER_STATE', 'COVERS_SET',  # COVERS_SET: items with a URL cover this run (IO_LOCK, 2026-11-19)
    'DEBUG_ENV_KEYS', 'FILLED_NARR', 'GENRE_DROPS', 'GENRE_HC', 'GENRE_LADDERS', 'GENRE_MAP_DE',
    'GENRE_SERIES_DROPS', 'GERMAN_LANG_CODES', 'GR_CACHE', 'GR_STATE', 'HC_CACHE', 'HC_PREFETCH',
    'HC_STATE', 'HC_USAGE', 'HEADERS_ABS', 'HEADERS_BASE', 'IDENTITY_DESC', 'IDENTITY_META',
    'ISBN_GROUPS', 'ISBN_OWNERS', 'KEY_ASIN', 'KEY_DUR', 'KEY_LANG', 'LABELS',
    'LANGUAGE_MAP', 'LAST_WRITTEN', 'LIBRARY_IDS', 'LIB_SERIES_NAMES', 'MANUAL_FIELDS', 'MARKETING_WORDS',
    'MARKET_BY_LANG', 'MIN_INTERVAL', 'NARR_SPELL', 'NEW_PENDING', 'NEW_VOLUMES', 'NUMBER_MAP',
    'ONLY_ITEMS', 'OUR_WRITTEN', 'OVERRIDES', 'PREFERRED_STORE', 'PROMO_SENTENCES', 'RATING_ORDER',
    'RATING_SOURCES', 'RATING_SYMBOL_SETS', 'REMOVE_RATINGS', 'RESULTS', 'ROLE_NAMES', 'RUN_T0',
    'SECOND_PASS', 'SEQ_TITLES', 'SEQ_WORKS', 'SERIES_ALIASES', 'SERIES_ASINS', 'SERIES_BOOKS',
    'SERIES_FACTS', 'SERIES_AUTHORS', 'FOLDER_SERIES', 'SERIES_INDEX', 'SHARED_DESC', 'SIBLING_CACHE', 'SOLO_AUTHORS', 'SOURCE_DOWN',
    'SOURCE_STATE', 'SUMMARY_INDEX', 'TITLE_OWNERS', 'UNMAPPED_GENRES', '_ASIN_TITLE', '_AUD_ISBN',
    '_BLOCK_KEYS', '_CORRUPT_KEPT', '_GUARD_STOP', '_HOST_LOCKS', '_JOURNAL_LAST', '_LOADED', '_LANG_WORDS',
    '_MEMBERS_CACHE', '_OWN_TS', '_SCAN_GATE', '_SERIES_CHILDREN', '_SERIES_SEQ', '_THROTTLE_LOCKS',
    '_UNDONE_VAL', '_gr_sample_saved', '_last_request', 'reports',
}


def module_containers():
    tree = ast.parse(open(SRC, encoding='utf-8').read())
    found = {}
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)): continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        for tg in targets:
            names = [tg] if isinstance(tg, ast.Name) else list(tg.elts) if isinstance(tg, ast.Tuple) else []
            values = [node.value] if isinstance(tg, ast.Name) else list(node.value.elts) if isinstance(node.value, ast.Tuple) else []
            for nm, val in zip(names, values):
                if not isinstance(nm, ast.Name): continue
                mutable = isinstance(val, (ast.Dict, ast.Set, ast.List, ast.DictComp, ast.SetComp, ast.ListComp)) or (
                    isinstance(val, ast.Call) and isinstance(val.func, ast.Name) and val.func.id in CONTAINER_CALLS)
                if mutable: found[nm.id] = node.lineno
    return found


def test_no_new_module_level_containers():
    new = {n: l for n, l in module_containers().items() if n not in KNOWN}
    assert not new, ("New module-level dict/set/list in abs_ratings.py: "
                     + ", ".join(f"{n} (line {l})" for n, l in sorted(new.items(), key=lambda x: x[1]))
                     + ". Per-book values must be thread-local (_TLDict / _TL); run-wide state goes on KNOWN in this test.")


def test_allowlist_has_no_stale_names():
    gone = KNOWN - set(module_containers())
    assert not gone, f"Remove from KNOWN (no longer in abs_ratings.py): {sorted(gone)}"


if __name__ == '__main__':
    test_no_new_module_level_containers(); test_allowlist_has_no_stale_names(); print('global state: ok')
