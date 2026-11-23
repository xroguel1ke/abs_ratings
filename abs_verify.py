#!/usr/bin/env python3
"""abs_verify: is this downloaded audiobook really the requested book? (2026-11-22)

Stand-alone helper for the Shelfmark auto-import. It only CALLS the existing, tested identification
logic of abs_ratings (Audible search with runtime/author/narrator/volume checks, Audnexus chapters,
chapter alignment) - it changes nothing in abs_ratings and never talks to Audiobookshelf.

Input (stdin, JSON):
  {"expect": {"title", "author", "series", "num", "language"},
   "probe":  {"duration": seconds, "chapters": [{"start", "end", "title"}], "narrators": [...], "tag_title": str},
   "library": [{"author", "rel", "dur"}],          # current ABS audiobooks (for duplicate / ambiguity checks)
   "exclude_rel": "..."}                            # tests only: ignore this library entry (the book itself)
Output (stdout, JSON):
  {"verdict": "ok" | "wrong" | "unsure", "reasons": [...], "asin", "domain", "title", "series", "narrators",
   "runtime", "diff", "chapter_proof"}
Only "ok" may be imported automatically. Everything uncertain is "unsure" (-> manual folder).
Environment: DATA_DIR must point to a SEPARATE scratch folder (never the abs_ratings data folder).
"""
import json
import logging
import os
import re
import sys

if not os.getenv("DATA_DIR"):
    sys.exit("DATA_DIR must be set to a separate scratch folder")
os.environ.setdefault("DRY_RUN", "true")          # belt and braces: abs_ratings never writes anything from here
os.environ.setdefault("GOODREADS_ENABLED", "false")
os.environ.setdefault("HARDCOVER_ENABLED", "false")
logging.basicConfig(stream=sys.stderr, level=os.getenv("VERIFY_LOG", "WARNING"), format="%(message)s")

import abs_ratings as A  # noqa: E402

A.BOOK_LANGUAGES[:] = ["English", "German"]

RUNTIME_OK_S = 120          # same recording: Audible runtime (whole minutes) within 2 min
DUP_S = 0.05                # identical file: duration equal to the millisecond
AMBIG_S = 120               # a library book this close in length could be the same recording


def nk(names):
    return {re.sub(r"[^a-z]", "", A.fold_accents(str(n)).lower()) for n in (names or []) if n}


def num_eq(a, b):
    try:
        return abs(float(str(a).strip()) - float(str(b).strip())) < 1e-6
    except (TypeError, ValueError):
        return str(a).strip().lstrip("0") == str(b).strip().lstrip("0") and str(a).strip() != ""


def verify(req):
    exp, probe = req.get("expect") or {}, req.get("probe") or {}
    title, author = (exp.get("title") or "").strip(), (exp.get("author") or "").strip()
    series, num = (exp.get("series") or "").strip(), str(exp.get("num") or "").strip()
    lang = A.map_language(exp.get("language") or "English") or "English"
    dur = float(probe.get("duration") or 0)
    chs = probe.get("chapters") or []
    tag_narr = [x.strip() for n in (probe.get("narrators") or []) for x in re.split(r"\s*(?:;|,|/|&| and )\s*", str(n or "")) if x.strip()]
    tag_narr = [n for n in dict.fromkeys(tag_narr) if not A.same_person_name(n, author)]
    out = {"verdict": "unsure", "reasons": []}
    why = out["reasons"]

    if not title or not author or dur < 600:
        why.append("missing title/author or no usable duration")
        return out

    # 1) identical to a book that is already in the library (mislabeled duplicate)
    lib = [b for b in (req.get("library") or []) if b.get("rel") != req.get("exclude_rel")]
    same_author = [b for b in lib if b.get("author") and A.match_author([author], b["author"])]
    for b in same_author:
        if b.get("dur") and abs(float(b["dur"]) - dur) <= DUP_S:
            out["verdict"] = "wrong"
            why.append(f"identical length to library book '{b['rel']}' (duplicate, wrong label)")
            return out

    # 2) the Audible product of the requested book.
    #    a) the request names its ASIN (ReadMeABook): exactly that product, the file must be that recording
    #    b) otherwise abs_ratings' search (title, author, runtime, narrator, volume)
    abs_series = [{"name": series, "sequence": num}] if series else None
    try:
        if exp.get("asin"):
            asin = str(exp["asin"]).strip()
        else:
            asin = A.search_audible_api(title, [author], dur, lang, required_lang=lang, narrators=tag_narr or None, abs_series=abs_series)
            if not asin and series:
                asin = A.find_missing_asin_via_series(title, [author], dur, lang, required_lang=lang, narrators=tag_narr or None, abs_series=abs_series)
    except Exception as exc:  # rate limit / network: never guess
        why.append(f"Audible search failed: {type(exc).__name__}")
        return out
    if asin is False:
        why.append("Audible API unavailable")
        return out
    if not asin:
        why.append("no Audible edition of this book matches the file (runtime/author/volume)")
        return out
    try:
        p = A.get_audible_data(asin, lang)
    except Exception as exc:
        why.append(f"Audible product {asin} not loadable ({type(exc).__name__})")
        return out
    if not isinstance(p, dict):
        why.append(f"Audible product {asin} not loadable")
        return out
    meta = p.get("meta_raw") or {}
    if exp.get("asin"):
        # the request gives no series data: take the product's own (its first series = the main one)
        if not series and meta.get("series"):
            series = meta["series"][0].get("name") or ""
            num = str(meta["series"][0].get("part") or "")
        if not exp.get("language") and A.map_language(meta.get("language")):
            lang = A.map_language(meta.get("language"))
    out["authors"] = p.get("authors_raw") or []
    out["ladders"] = meta.get("ladders") or []
    out["publisher"] = (meta.get("publisher") or {}).get("name")
    out["language"] = A.map_language(meta.get("language"))
    out["expect_series"], out["expect_num"] = series, num
    aud_series = [{"name": s.get("name"), "part": s.get("part")} for s in meta.get("series") or []]
    out.update(asin=asin, domain=p.get("domain"), title=p.get("title_raw"), series=aud_series,
               narrators=p.get("narrators_raw") or [], runtime=p.get("runtime_sec"))

    # 3) the product must be the REQUESTED book
    if not A.match_author([author], ", ".join(p.get("authors_raw") or [])):
        out["verdict"] = "wrong"; why.append(f"author on Audible: {p.get('authors_raw')}"); return out
    p_lang = A.map_language(meta.get("language"))
    if p_lang and p_lang != lang:
        out["verdict"] = "wrong"; why.append(f"language {p_lang}, expected {lang}"); return out
    # Audible formats: 'unabridged', 'original_recording' (Audible Originals / full-cast productions = complete) and
    # 'abridged'. Only a shortened edition is a reason for manual (2026-11-23: Exoplanet was 'original_recording').
    fmt_ = (meta.get("format") or "unabridged").lower()
    if "abridged" in fmt_ and "unabridged" not in fmt_:
        why.append(f"format {meta.get('format')}"); return out
    t_score = max(A.search_match_score(title, p.get("title_raw") or "", p.get("subtitle_raw")),
                  A.title_similarity(A.clean_title(title), A.clean_title(p.get("title_raw") or "")))
    from abs_series import series_key
    in_series = [s for s in aud_series if series and s.get("name") and series_key(s["name"]) == series_key(series)]
    if num:
        own = [s.get("part") for s in in_series if s.get("part") not in (None, "")]
        other = [s.get("part") for s in aud_series if s.get("part") not in (None, "")]
        if own and not any(num_eq(x, num) for x in own):
            if t_score >= 0.9 and not A.volume_conflict(title, p.get("title_raw"), p.get("subtitle_raw")):
                # same title, other number: a different numbering scheme ('First Law World' 4 vs 1) - not provable either way
                why.append(f"Audible numbers it {own} of '{series}', expected {num} (numbering differs, same title)"); return out
            # the SAME series has another number and the title differs / names another volume: provably another volume
            out["verdict"] = "wrong"; why.append(f"Audible volume {own} of '{series}', expected {num}"); return out
        if not own:
            if other and any(num_eq(x, num) for x in other):
                pass  # series named differently on Audible, but the number matches
            elif other or A.volume_conflict(title, p.get("title_raw"), p.get("subtitle_raw")) or t_score < 0.9:
                why.append(f"volume {num} not provable (Audible series {aud_series}, title {t_score:.2f})"); return out
        why.append(f"volume {num} ✓")
    if t_score < 0.85 and not (num and in_series):
        why.append(f"title only {t_score:.2f}"); return out

    # 4) same RECORDING: runtime, chapter structure, narrator
    rt = p.get("runtime_sec") or 0
    diff = dur - rt
    out["diff"] = round(diff, 1)
    if not rt:
        why.append("Audible has no runtime"); return out
    # chapter proof = the file's OWN chapter marks sit at Audible's chapter starts (abs_ratings.anchor_align).
    # A matching total length ("length matches Audible", "lacks Audible's intro") is NO proof of the volume.
    proof = None
    marks = [c for c in chs if c.get("end") is not None]
    aud_ch = A.audible_chapters(asin, p.get("domain")) if len(marks) >= 3 else None
    if aud_ch:
        an = A.anchor_align(marks, aud_ch[0], dur)
        if an:
            proof = f"file chapter marks at Audible's chapter starts ({an[1]}/{an[2]}, offset {an[0]:+.1f} s)"
        else:
            al = [float(c["end"]) - float(c["start"]) for c in marks]
            if len(al) == len(aud_ch[0]) >= 3 and all(abs(a - b[1]) <= 3 for a, b in list(zip(al, aud_ch[0]))[1:-1]):
                proof = "file chapter lengths equal Audible's"
    out["chapter_proof"] = proof
    if abs(diff) > RUNTIME_OK_S:
        if dur > 1.6 * rt:
            out["verdict"] = "wrong"; why.append(f"{dur/3600:.1f} h vs {rt/3600:.1f} h on Audible (bundle)"); return out
        if dur < 0.5 * rt:
            out["verdict"] = "wrong"; why.append(f"{dur/3600:.1f} h vs {rt/3600:.1f} h on Audible (incomplete)"); return out
        if not proof:
            why.append(f"runtime {diff:+.0f} s from Audible"); return out
    why.append(f"runtime {diff:+.0f} s ✓" + (f", {proof}" if proof else ""))
    if not proof:
        if aud_ch:
            # the file HAS chapter marks and Audible has chapters, but they do not line up: another volume/edition?
            why.append(f"{len(marks)} chapter marks do not line up with Audible's {len(aud_ch[0])} chapters"); return out
        # no chapters to compare: the runtime is the only link to THIS volume - another product of the same
        # series with (nearly) the same length makes it ambiguous (Slow Life 2 vs 4: 0.8 s apart)
        try:
            sib = A.series_candidates([{"name": s_["name"]} for s_ in aud_series if s_.get("name")] or abs_series, [author], dur, p.get("domain"))
        except Exception:
            sib = None
        if sib is None:
            why.append("series listing not available (no chapter proof)"); return out
        for c in sib:
            if c == asin:
                continue
            q = A.get_audible_data(c, lang)
            if isinstance(q, dict) and q.get("runtime_sec") and abs(q["runtime_sec"] - dur) <= RUNTIME_OK_S:
                qn = [x.get("part") for x in ((q.get("meta_raw") or {}).get("series") or []) if x.get("part")]
                if not (num and any(num_eq(x, num) for x in qn)):
                    why.append(f"another product of the series has the same length: {q.get('title_raw')} {qn} [{c}] (no chapter proof)"); return out
    if tag_narr and not (nk(tag_narr) & nk(p.get("narrators_raw"))):
        why.append(f"file narrator {tag_narr} ≠ Audible {p.get('narrators_raw')} (other edition?)"); return out
    # 5) ambiguity: a book of the same author in the library with (nearly) the same length - without chapter proof
    #    the file could be that book (Slow Life 2 vs 4 differ by 0.8 s)
    if not proof:
        close = [b["rel"] for b in same_author if b.get("dur") and abs(float(b["dur"]) - dur) <= AMBIG_S]
        if close:
            why.append(f"same length as library book {close[0]!r} and no chapter proof"); return out
    out["verdict"] = "ok"
    return out


def main():
    req = json.load(sys.stdin)
    try:
        res = verify(req)
    except Exception as exc:  # never crash the caller: uncertain = manual
        res = {"verdict": "unsure", "reasons": [f"verifier error: {type(exc).__name__}: {exc}"]}
    json.dump(res, sys.stdout, ensure_ascii=False)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
