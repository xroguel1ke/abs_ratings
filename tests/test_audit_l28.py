"""Rules from the library audit 2026-11-28. Run: python -m pytest tests/test_audit_l28.py"""
import os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/audit_l28_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import abs_ratings as A, abs_series as S

SRC = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'abs_ratings.py'), encoding='utf-8').read()


def test_role_credit_with_author_prefix():
    assert S.is_role_credit('Ellen Datlow - author/editor') and S.role_base('Ellen Datlow - author/editor') == 'Ellen Datlow'
    assert S.is_role_credit('Jane Doe - author & editor') and S.is_role_credit('Ken Liu - translator')
    assert not S.is_role_credit('Ellen Datlow') and not S.is_role_credit('Author Name')
    assert S.EDITOR_ROLE_RE.search('Ellen Datlow - author/editor') and S.EDITOR_ROLE_RE.search('John Joseph Adams - editor')
    assert not S.EDITOR_ROLE_RE.search('Ken Liu - translator') and not S.EDITOR_ROLE_RE.search('Kurone Mishima - illustrator')


def _ctx(calls, lib=False):
    return {'abs_update': lambda iid, key, title, upd, old, src: calls.append((title, upd)) or True, 'dry_run': True,
            'new_rules_library': lib, 'new_book_days': 30}


def test_sole_editor_keeps_the_name_without_role():
    now_ms = time.time() * 1000
    rec = lambda t, au, added: {'id': t, 'lib': 'L', 'title': t, 'authors': au, 'tags': set(), 'added': added}
    calls = []
    S.remove_role_credits(_ctx(calls), [rec('Best Horror 4', ['Ellen Datlow - author/editor'], now_ms),
                                        rec('Old Anthology', ['Ellen Datlow - author/editor'], now_ms - 90 * 86400000),
                                        rec('Illustrated', ['Kurone Mishima - illustrator'], now_ms),
                                        rec('Three-Body', ['Cixin Liu', 'Ken Liu - translator'], now_ms - 90 * 86400000)])
    got = {t: [a['name'] for a in u['authors']] for t, u in calls}
    assert got['Best Horror 4'] == ['Ellen Datlow']
    assert 'Old Anthology' not in got  # library book: only with NEW_RULES_LIBRARY
    assert 'Illustrated' not in got  # an illustrator alone is no author: left for a person to decide
    assert got['Three-Body'] == ['Cixin Liu']  # the old rule (a real author stays) is unchanged for every book
    calls = []
    S.remove_role_credits(_ctx(calls, lib=True), [rec('Old Anthology', ['Ellen Datlow - author/editor'], now_ms - 90 * 86400000)])
    assert [a['name'] for a in calls[0][1]['authors']] == ['Ellen Datlow']


def test_narrator_role_suffix_and_library_gate():
    assert "aud_narr = list(dict.fromkeys(_rb_n(n_) if _irc_n(n_) else n_ for n_ in (aud_data.get('narrators_raw') or [])))" in SRC
    assert "c_ = narrator_canonical(_rb_n(n_) if lib_ok_ and _irc_n(n_) else n_)" in SRC
    assert "lib_ok_ = recent_ or NEW_RULES_LIBRARY" in SRC and A.NEW_RULES_LIBRARY is False


def test_too_long_also_when_the_metadata_sync_is_skipped():
    i = SRC.index("Metadata sync skipped: runtime still differs")
    blk = SRC[i:i + 1500]
    assert "abs_dur > 1.5 * rt_s_ and abs_dur - rt_s_ > 3600" in blk and "add_review(key, title, 'too_long'" in blk


def test_praise_first_then_plot_is_no_review_only_text():
    # 'Death's End': Audible's own summary opens with praise and then tells the plot
    praise = 'PRAISE FOR THE THREE-BODY PROBLEM SERIES: “A mind-bending epic.” — The New York Times “Wildly imaginative.” — Barack Obama '
    plot = ('Half a century after the Doomsday Battle, the uneasy balance of Dark Forest Deterrence keeps the Trisolaran invaders at bay. '
            'Earth enjoys unprecedented prosperity due to the infusion of Trisolaran knowledge. With human science advancing daily and the '
            'Trisolarans adopting Earth culture, it seems that the two civilizations will soon be able to co-exist peacefully as equals. ')
    assert not A.description_is_reviews(praise + plot)
    assert A.description_is_reviews(praise)
    assert A.description_junk(praise + plot) is None


def test_review_entries_whose_proposal_abs_has_go(tmp_path, monkeypatch):
    import json
    f = tmp_path / 'rq.json'
    q = [{'key': 'L_1', 'kind': 'authors', 'candidate': 'Thomas M. Reid', 'status': 'open'},
         {'key': 'L_2', 'kind': 'authors', 'candidate': 'Stephen King, Richard Chizmar', 'status': 'open'},
         {'key': 'L_3', 'kind': 'title', 'candidate': 'The Saints of Salvation', 'status': 'open'},
         {'key': 'L_4', 'kind': 'chapters', 'candidate': 'no chapters', 'status': 'open'},
         {'key': 'L_5', 'kind': 'asin', 'candidate': 'B0ABCDEF12', 'status': 'open'}]
    f.write_text(json.dumps(q))
    monkeypatch.setattr(A, 'REVIEW_FILE', str(f)); monkeypatch.setattr(A, 'DRY_RUN', False)
    items = [{'id': '1', 'media': {'metadata': {'authorName': 'Thomas M. Reid'}}},
             {'id': '2', 'media': {'metadata': {'authorName': 'Stephen King'}}},
             {'id': '3', 'media': {'metadata': {'title': 'The Saints of Salvation'}}},
             {'id': '4', 'media': {'metadata': {'title': 'X'}}},
             {'id': '5', 'media': {'metadata': {'asin': 'B0OTHER123'}}}]
    assert A.review_drop_applied('L', items) == 2
    st = {x['key']: x['status'] for x in json.loads(f.read_text())}
    assert st == {'L_1': 'obsolete', 'L_2': 'open', 'L_3': 'obsolete', 'L_4': 'open', 'L_5': 'open'}
    monkeypatch.setattr(A, 'DRY_RUN', True)
    assert A.review_drop_applied('L', items) == 0  # a test run never changes the list


def test_hardcover_part_of_our_book_is_rejected():
    # 'The Green Mile': part 5 'Night Journey' lists 'The Green Mile, Part 5 : Night Journey' as alternative title
    nj = {'title': 'Night Journey', 'alternative_titles': ['The Green Mile, Part 5 : Night Journey', 'The Green Mile, Teil 5: Reise in die Nacht']}
    assert A.hc_part_of_ours(['The Green Mile'], nj)
    assert not A.hc_part_of_ours(['The Green Mile'], {'title': 'The Green Mile', 'alternative_titles': ['The Green Mile, Part 5']})  # main title fits
    assert not A.hc_part_of_ours(['The Green Mile, Part 5'], nj)  # we ARE a part
    assert not A.hc_part_of_ours(['Dragonwatch'], {'title': 'Smocza Straż', 'alternative_titles': ['Dragonwatch', 'Smocza Straż']})  # a translation


def test_compilation_with_exactly_our_title_is_our_book():
    i = SRC.index('if book.get(\'compilation\') and not any(RE_HC_SPECIAL.search(t) for t in targets):')
    blk = SRC[i:i + 1200]
    assert "exact_ = (book.get('ratings_count') or 0) >= 100" in blk and "(single or exact_)" in blk


def test_year_later_than_the_recordings_release():
    i = SRC.index("(later than this recording's release)")
    blk = SRC[SRC.rindex('elif new_year and cur_y and lib_ok_', 0, i):i]
    assert "int(cur_y.group(1)) > int(str(new_year)[:4])" in blk and "abs(abs_dur - aud_data['runtime_sec']) <= 120" in blk
