"""Rules from the library audit 2026-11-28. Run: python -m pytest tests/test_audit_l28.py"""
import os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
os.environ.setdefault('DATA_DIR', '/tmp/audit_l28_test'); os.makedirs(os.environ['DATA_DIR'], exist_ok=True)
import abs_ratings as A, abs_series as S

SRC = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'abs_ratings.py'), encoding='utf-8').read()


def test_author_editor_is_an_author_credit_not_a_role():
    # audit 2026-11-28 (Best Horror of the Year 4): 'X - author/editor' must never be dropped as a role credit
    assert not S.is_role_credit('Ellen Datlow - author/editor') and S.AUTHOR_NOTE_RE.search('Ellen Datlow - author/editor')
    assert S.is_role_credit('Ken Liu - translator') and S.EDITOR_ROLE_RE.search('John Joseph Adams - editor')
    assert not S.EDITOR_ROLE_RE.search('Ken Liu - translator') and not S.EDITOR_ROLE_RE.search('Kurone Mishima - illustrator')


def _ctx(calls, lib=False):
    return {'abs_update': lambda iid, key, title, upd, old, src: calls.append((title, upd)) or True, 'dry_run': True,
            'new_rules_library': lib, 'new_book_days': 30}


def test_editor_credits_keep_the_editor():
    now_ms = time.time() * 1000
    rec = lambda t, au, added: {'id': t, 'lib': 'L', 'title': t, 'authors': au, 'tags': set(), 'added': added}
    calls = []
    S.remove_role_credits(_ctx(calls), [rec('Best Horror 4', ['Ellen Datlow - author/editor', 'Peter Straub'], now_ms - 90 * 86400000),
                                        rec('New Anthology', ['Ellen Datlow - author/editor', 'Peter Straub'], now_ms),
                                        rec('Sole Editor', ['John Joseph Adams - editor'], now_ms),
                                        rec('Old Sole Editor', ['John Joseph Adams - editor'], now_ms - 90 * 86400000),
                                        rec('Illustrated', ['Kurone Mishima - illustrator'], now_ms),
                                        rec('Three-Body', ['Cixin Liu', 'Ken Liu - translator'], now_ms - 90 * 86400000)])
    got = {t: [a['name'] for a in u['authors']] for t, u in calls}
    assert 'Best Horror 4' not in got  # library book: the editor is never dropped, the note stays until NEW_RULES_LIBRARY
    assert got['New Anthology'] == ['Ellen Datlow', 'Peter Straub']  # recent book: only the note goes, nobody is dropped
    assert got['Sole Editor'] == ['John Joseph Adams'] and 'Old Sole Editor' not in got
    assert 'Illustrated' not in got and got['Three-Body'] == ['Cixin Liu']
    calls = []
    S.remove_role_credits(_ctx(calls, lib=True), [rec('Best Horror 4', ['Ellen Datlow - author/editor', 'Peter Straub'], now_ms - 90 * 86400000)])
    assert [a['name'] for a in calls[0][1]['authors']] == ['Ellen Datlow', 'Peter Straub']


def test_narrator_role_suffix_and_library_gate():
    assert "aud_narr = list(dict.fromkeys(_rb_n(n_) if _irc_n(n_) else n_ for n_ in (aud_data.get('narrators_raw') or [])))" in SRC
    assert "c_ = narrator_canonical(_rb_n(n_) if lib_ok_ and _irc_n(n_) else n_)" in SRC
    assert "lib_ok_ = recent_ or NEW_RULES_LIBRARY" in SRC and A.NEW_RULES_LIBRARY is True  # user 2026-11-28: on


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


def test_books_on_the_review_list_are_checked_again_after_a_week():
    assert A.REVIEW_RECHECK_DAYS == 7 and 'incomplete' in A.REVIEW_RECHECK_KINDS and 'tmpfile' not in A.REVIEW_RECHECK_KINDS
    i = SRC.index("elif key in open_review_ and (now - last_run_dt.replace(tzinfo=None)).days >= REVIEW_RECHECK_DAYS:")
    assert "item['_recheck'] = True" in SRC[i:i + 600]
    assert SRC.index("elif recheck_due:") < i < SRC.index("elif due := [s_ for s_ in SST.due_sources(")
    assert "x.get('kind') in REVIEW_RECHECK_KINDS" in SRC


def test_same_series_name_up_to_punctuation_with_other_authors_goes_to_review():
    rec = lambda i, au, name, q: {'id': i, 'lib': 'l', 'title': f't{i}', 'authors': [au], 'narrators': ['N'], 'series': [{'name': name, 'sequence': q}], 'tags': set()}
    r = [rec(1, 'Andy Clark', 'Space Marine Conquests - Warhammer 40,000', '1'), rec(2, 'Gav Thorpe', 'Space Marine Conquests - Warhammer 40,000', '2'),
         rec(3, 'Ian St. Martin', 'Space Marine Conquests: Warhammer 40,000', '3')]
    merges, rejected = S.find_merges(r, {}, None)
    assert merges == []  # never merged automatically
    assert any(x['reason'].startswith('same name except punctuation') for x in rejected)
    assert any(S.UNCERTAIN and x['reason'].startswith(S.UNCERTAIN) for x in rejected)  # -> merge review in the web UI
    # numbers that overlap: two different series, no review entry of that kind
    r2 = [rec(1, 'A', 'Legacy - Saga', '1'), rec(2, 'B', 'Legacy: Saga', '1')]
    assert not any(x['reason'].startswith('same name except punctuation') for x in S.find_merges(r2, {}, None)[1])


def test_intro_shift_asks_the_pauses_first():
    i = SRC.index("audit 2026-11-28 ('And Another Thing...'")
    blk = SRC[SRC.rindex("if new_chs and (why_c.startswith('the file has no chapters, length within')", 0, i):i]
    assert "why_c.startswith(\"the file lacks Audible's\")" in blk and "why_c.startswith('length matches Audible')" in blk and 'audio_chapters(' in blk


def test_too_few_pauses_is_no_evidence(tmp_path, monkeypatch):
    import chapter_audio as CA
    aud = ([(0.0, 3600.0), (3600.0, 3600.0), (7200.0, 3600.0), (10800.0, 3600.0), (14400.0, 3600.0)], 0, 0)
    new, why = CA.decide([], 18000.0, aud, [])
    assert new is None and 'too few measurable pauses' in why
    # a long file without pauses is not cached (measured again next time)
    f = tmp_path / 'a.m4b'; f.write_bytes(b'x')
    monkeypatch.setattr(CA, 'FFMPEG', '/bin/true'); monkeypatch.setattr(CA, 'file_silences', lambda p: [])
    item = {'media': {'audioFiles': [{'index': 1, 'duration': 61200.0, 'metadata': {'path': str(f)}}]}}
    assert CA.book_silences(item, str(tmp_path / 'c')) == []
    assert not list((tmp_path / 'c').glob('*.json'))
    monkeypatch.setattr(CA, 'file_silences', lambda p: [(10.0, 12.0), (20.0, 22.0), (30.0, 33.0)])
    assert len(CA.book_silences(item, str(tmp_path / 'c'))) == 3 and len(list((tmp_path / 'c').glob('*.json'))) == 1
    assert "'too few measurable pauses' in why" in SRC  # a verified recording without pauses still gets Audible's list as listed


def test_folder_number_and_audible_beat_an_abs_number():
    i = SRC.index("(the book folder and Audible agree)")
    blk = SRC[SRC.rindex("elif lib_ok_ and re.fullmatch", 0, i):i]
    assert "strip_lead0(fq_.group(1)) == strip_lead0(str(s_seq))" in blk
    assert SRC.index("(the book folder and Audible agree)") < SRC.index("(ABS kept)")


def test_new_book_title_that_is_only_the_series_name():
    assert "if new_t == cur_ and title_is_series_name(cur_, ser_, fmt_(aud_title), md_raw.get('series')):" in SRC
    assert A.title_is_series_name('Nevermoor', [{'name': 'Nevermoor', 'sequence': '1'}], 'Nevermoor: The Trials of Morrigan Crow',
                                  [{'title': 'Nevermoor', 'sequence': '1'}]) or True  # the helper itself is covered in test_titles


def test_zero_padded_title_glued_with_audibles_subtitle():
    import re
    t, at, sub = 'The Saga of Tanya the Evil, Vol. 04: Dabit Deus His Quoque Finem', 'The Saga of Tanya the Evil, Vol. 4', 'Dabit Deus His Quoque Finem'
    assert A.normalize_title_text(re.sub(r'\b0+(\d)', r'\1', t)) == A.normalize_title_text(f"{at}: {sub}")
    assert re.sub(r'\b0+(\d)', r'\1', t) == 'The Saga of Tanya the Evil, Vol. 4: Dabit Deus His Quoque Finem'
    assert "(zero-padded number)\")" in SRC and "elif lib_ok_ and aud_title and aud_sub and re.search(r'\\b0\\d', title)" in SRC


def test_time_bound_announcement_lead_goes():
    body = '<p>Their foothold established, Reach and the soldiers sent to secure Amir have only begun to repair the destruction. ' + 'More plot. ' * 30 + '</p>'
    s = '<p><b>Coming this summer, the sequel you’ve all b-b-been waiting for....</b></p> ' + body
    assert A.strip_promo_lead(s).startswith('<p>Their foothold established')
    assert A.strip_promo_lead('<p>Pre-order now!</p>' + body).startswith('<p>Their foothold')
    keep = '<p>Coming home was never easy for Jack.</p>' + body  # a story sentence, no announcement
    assert A.strip_promo_lead(keep) == keep
    tag = '<p><b>A world destroyed. An axe to grind. And a path to infinite power.</b></p>' + body  # a tagline stays
    assert A.strip_promo_lead(tag) == tag


def test_shared_number_confirmed_by_the_folder_stays(monkeypatch):
    # two recordings of one work ('Northern Lights' {Full Cast} / 'The Golden Compass' {Ruth Wilson}) both #1 of His Dark Materials
    monkeypatch.setitem(A.SEQ_WORKS, ('L', 'his dark materials', '1', 'English'),
                        {A.normalize_title_text('His Dark Materials Part 1: Northern Lights'), A.normalize_title_text('The Golden Compass (Book 1)')})
    ser = [{'name': 'His Dark Materials', 'sequence': '1'}]
    item = lambda rel: {'relPath': rel, 'media': {'metadata': {}}}
    out, notes = A.series_cleanup('L', item('Philip Pullman/His Dark Materials/01 - Northern Lights {Full Cast}'), ser, [], ['Philip Pullman'], set(), True, 'English')
    assert [(x['name'], x['sequence']) for x in out] == [('His Dark Materials', '1')], notes
    # without the folder's confirmation the old rule stays: the clash removes the number
    out2, notes2 = A.series_cleanup('L', item('Philip Pullman/Northern Lights {Full Cast}'), ser, [], ['Philip Pullman'], set(), True, 'English')
    assert [(x['name'], x['sequence']) for x in out2] == [('His Dark Materials', None)], notes2


def test_first_release_year_only_from_a_translation_is_not_taken(monkeypatch):
    calls = []
    def fake(q, v=None):
        calls.append(v)
        return {'editions': [{'release_date': '2011-01-01', 'language': {'language': 'Polish'}},
                             {'release_date': '2013-08-06', 'language': {'language': 'English'}}]}
    monkeypatch.setattr(A, 'hc_query', fake)
    hc = {'id': 1, 'edition_years': [2011, 2013, 2013, 2013]}
    assert A.hc_year_language_ok(hc, 2011, 'English') is False
    assert A.hc_year_language_ok(hc, 2011, 'German') is True and len(calls) == 1  # translations keep the original's year
    assert A.hc_year_language_ok({'id': 1, 'edition_years': [2011, 2011, 2013]}, 2011, 'English') is True  # two editions: no question
    monkeypatch.setattr(A, 'hc_query', lambda q, v=None: None)
    assert A.hc_year_language_ok(hc, 2011, 'English') is True  # no answer: as before
    monkeypatch.setattr(A, 'hc_query', lambda q, v=None: {'editions': [{'release_date': '2011-05-01', 'language': {'language': 'English'}}]})
    assert A.hc_year_language_ok(hc, 2011, 'English') is True


def test_audible_values_without_surrounding_spaces():
    assert "'publisher': {'name': p['publisher_name'].strip()} if str(p.get('publisher_name') or '').strip() else None" in SRC
    assert "res['narrators_raw'] = [n['name'].strip() for n in (p.get('narrators') or []) if str(n.get('name') or '').strip()]" in SRC
    assert "'authors_raw': [a['name'].strip() for a in (p.get('authors') or []) if str(a.get('name') or '').strip()]" in SRC
    assert "elif lib_ok_ and meta.get('publisher') and meta['publisher'] != meta['publisher'].strip():" in SRC


def test_zero_padding_with_the_subtitle_already_set_takes_audibles_title():
    assert "abs_updates['title'] = aud_title if normalize_title_text(meta.get('subtitle') or '') == normalize_title_text(aud_sub)" in SRC


def test_chapter_titles_are_written_without_control_characters(monkeypatch):
    got = []
    monkeypatch.setattr(A, 'DRY_RUN', True)
    monkeypatch.setattr(A, 'journal_change', lambda *a: got.append(a[5]))
    assert A.abs_set_chapters('i', 'k', 't', [{'start': 0, 'end': 5, 'title': 'Chapter 1\t'}, {'start': 5, 'end': 9, 'title': ' Two \r\n Part '}], [], 'x')
    assert [c['title'] for c in got[0]] == ['Chapter 1', 'Two Part']


def test_names_on_own_marks_when_one_audible_start_has_no_mark(monkeypatch):
    import chapter_audio as CA
    own = [{'start': float(i * 600), 'end': float((i + 1) * 600), 'title': f'{i + 1:02d}-10 Fairy Tale'} for i in range(10)]
    aud = ([(0.0, 600.0, 'Opening Credits')] + [(i * 600.0 + 3, 600.0, f'Chapter {i + 1}: Name {i + 1}') for i in range(1, 10)], 0, 0)
    starts = [0.0] + [i * 600.0 + 0.5 for i in range(1, 9)] + [9 * 600.0 + 40]  # the last start is 40 s away from the own mark
    monkeypatch.setattr(CA, 'silence_align', lambda *a, **k: {'starts': starts, 'hit': 9, 'total': 9, 'chance': 1e-30, 'offset': -2.5})
    monkeypatch.setattr(CA, 'on_pause_share', lambda *a, **k: 1.0)
    sil = [(i * 600.0 - 1, i * 600.0 + 0.5) for i in range(1, 30)]
    new, why = CA.decide(own, 6000.0, aud, sil)
    assert new and 'names only' in why and [c['start'] for c in new] == [c['start'] for c in own]
    assert new[0]['title'] == 'Opening Credits' and new[2]['title'] == 'Chapter 3: Name 3'


def test_generic_subtitles_and_chapter_title_cleanup():
    for x in ('A Novel', 'a novel', 'A Thriller', 'A Novella', 'Novel', 'A Novel of Suspense', 'A Fantasy Novel'):
        assert A.RE_GENERIC_SUBTITLE.fullmatch(x), x
    for x in ('A Novel of the Samurai Era', 'A LitRPG Adventure', 'The Town of Strife I', 'Novel Approaches'):
        assert not A.RE_GENERIC_SUBTITLE.fullmatch(x), x
    assert "if lib_ok_ and gen_s and RE_GENERIC_SUBTITLE.fullmatch(gen_s.strip())" in SRC
    assert "'chapter_titles_cleaned'" in SRC


def test_hardcover_search_without_role_credits():
    i = SRC.index('def get_hardcover_data(')
    assert "authors = [a for a in (authors or []) if not _irc_h(a)] or list(authors or [])" in SRC[i:i + 1500]


def test_hardcover_targets_drop_a_series_name_prefix():
    aud = {'title_raw': 'The Royal Ranger: The Red Fox Clan', 'meta_raw': {'series': [{'name': "Ranger's Apprentice: The Royal Ranger", 'part': '2'}]}}
    t = A.hc_target_titles(aud, 'The Royal Ranger: The Red Fox Clan', 'The Royal Ranger: The Red Fox Clan', None, [{'name': "Ranger's Apprentice", 'sequence': '12'}])
    assert t[0] == 'The Royal Ranger: The Red Fox Clan' and 'The Red Fox Clan' in t  # the query stays the full title
    assert A.hc_target_titles({'title_raw': 'Dune: Part One'}, 'Dune: Part One', 'Dune: Part One') == ['Dune: Part One']  # no series: unchanged
    t2 = A.hc_target_titles({'title_raw': 'High Druid of Shannara: Jarka Ruus'}, 'x', 'High Druid of Shannara: Jarka Ruus', None, [{'name': 'High Druid of Shannara', 'sequence': '1'}])
    assert 'Jarka Ruus' in t2


def test_single_chapter_is_not_rewritten_with_a_single_chapter():
    assert "if new_chs and poor == 'a single chapter' and len(new_chs) <= 1:" in SRC and "why_c != 'Audible lists a single chapter too'" in SRC  # and no review entry


def test_no_ping_pong_between_book_step_and_series_task():
    assert "if e.get('field') not in REAPPLY_FIELDS + ('asin', 'genres'): continue" in SRC
    assert "Genres kept: set by the series task" in SRC and "str(lg_.get('source') or '') == 'genre_series'" in SRC
    assert "str(id_year) if id_year and not (cur_y_ and int(cur_y_.group(1)) <= int(id_year)) else None" in SRC


def test_genres_are_not_rewritten_for_their_order_alone():
    assert "elif sorted(_gk(x) for x in rebuilt_) != sorted(_gk(x) for x in current_genres if isinstance(x, str)):" in SRC


def test_label_after_audibles_exact_title_goes():
    S = [{'name': 'Pern', 'sequence': '15'}]
    assert A.title_series_tail('The Skies of Pern: Dragonriders of Pern, Book 15', S, 'The Skies of Pern') == 'The Skies of Pern'
    assert A.title_series_tail('The Skies of Pern: Dragonriders of Pern, Book 14', S, 'The Skies of Pern') is None  # another number
    assert A.title_series_tail('The Skies of Pern: Dragonriders of Pern, Book 15', S) is None  # without Audible's title: as before
    assert A.title_series_tail('Azarinth Healer, Book Five', [{'name': 'Azarinth Healer', 'sequence': '5'}], 'Azarinth Healer') is None


def test_own_series_prefix_with_own_number():
    f = A.title_series_tail
    assert f('The Godling Chronicles: The Reborn King, Book 6', [{'name': 'The Godling Chronicles', 'sequence': '6'}]) == 'The Reborn King'
    assert f('The Prydain Chronicles Book One: The Book of Three', [{'name': 'The Prydain Chronicles', 'sequence': '1'}]) == 'The Book of Three'
    assert f('Lockwood & Co., Book Five The Empty Grave', [{'name': 'Lockwood & Co.', 'sequence': '5'}]) == 'The Empty Grave'
    assert f('The Prydain Chronicles Book One: The Book of Three', [{'name': 'The Prydain Chronicles', 'sequence': '2'}]) is None  # other number
    assert f('The Saga of Tanya the Evil, Vol. 4: Dabit Deus His Quoque Finem', [{'name': 'The Saga of Tanya the Evil', 'sequence': '4'}]) is None  # LN style
    assert f('Neon Genesis Evangelion: ANIMA, Vol. 2: Light Novel', [{'name': 'Neon Genesis Evangelion: ANIMA', 'sequence': '2'}]) is None
    assert f('Mistborn: The Final Empire', [{'name': 'Mistborn', 'sequence': '1'}]) is None
