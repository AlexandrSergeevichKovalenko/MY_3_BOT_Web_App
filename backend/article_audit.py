"""
Artikel Sprint article-correctness audit.

Cross-checks every stored der/die/das against an authoritative reference and
reports mismatches. Reference precedence per word:
  1. Two-gender / person-adjective words → 'ambiguous' (no single article; flagged).
  2. Wiktionary genus (direct page, else compound-head decomposition) — the эталон.
  3. Deterministic gender guard (strong_gender) — corroborates and covers the
     compounds Wiktionary misses.
When Wiktionary and the guard disagree → 'conflict' (surfaced, never auto-fixed).

audit_all() only READS + reports. apply_fixes() writes the confirmed corrections.
Both are theme-scopable. Genus lookups are cached in the DB, so apply runs off the
same data the report was built from without new network calls.
"""
from __future__ import annotations

import logging

_CHUNK = 150  # words per reference batch (for progress + bounded memory)


def _iter_rows(theme_keys: list[str] | None) -> list[dict]:
    from backend.database import list_all_article_sprint_rows
    rows = list_all_article_sprint_rows()
    if theme_keys:
        keep = set(theme_keys)
        rows = [r for r in rows if r["theme_key"] in keep]
    return rows


def _rec(r: dict, **extra) -> dict:
    base = {"id": r["id"], "theme": r["theme_key"], "word": r["word"],
            "stored": str(r["article"]).lower(), "meaning_ru": r.get("meaning_ru", "")}
    base.update(extra)
    return base


def _classify(rows: list[dict], progress_cb=None) -> dict:
    from backend.article_sprint_generator import (
        strong_gender, is_ambiguous_noun, is_nominalized_person_adjective,
    )
    from backend.article_wiktionary_ref import reference_articles

    report = {
        "checked": len(rows), "ok": 0,
        "mismatch": [], "review": [], "ambiguous": [], "unknown": [],
    }
    done = 0
    for i in range(0, len(rows), _CHUNK):
        chunk = rows[i:i + _CHUNK]
        refs = reference_articles([r["word"] for r in chunk])
        for r in chunk:
            w, stored = r["word"], str(r["article"]).lower()
            ref = refs.get(w, {})
            wik = ref.get("articles") or set()  # documented genders

            # 0) curated two-gender rows (der See/die See) are authoritative by design
            # and one sense may decline adjectivally (das Junge) so Wiktionary's noun
            # overview under-documents it — never second-guess these.
            if r.get("two_gender"):
                report["ambiguous"].append(_rec(r, why="two-gender"))
                continue

            # 1) curated meaning-dependent two-gender / person-adjective nouns — the
            # article depends on the intended sense. Keep them; flag only so the game
            # shows the Russian meaning. NOT derived from Wiktionary's noisy multi-genus.
            if is_ambiguous_noun(w):
                why = "person-adj" if is_nominalized_person_adjective(w) else "meaning-dependent"
                report["ambiguous"].append(_rec(r, why=why))
                continue

            # 2) Wiktionary is authoritative when it documents gender(s).
            if wik:
                if stored in wik:
                    report["ok"] += 1              # stored article is a documented gender
                elif len(wik) == 1:
                    ref_art = next(iter(wik))       # single documented gender, stored differs → fix
                    report["mismatch"].append(_rec(r, ref=ref_art, basis="wiktionary"))
                else:
                    # several documented genders, none matches stored → can't auto-pick.
                    report["review"].append(_rec(r, options=sorted(wik)))
                continue

            # 3) no Wiktionary genus → deterministic guard, else unknown.
            guard = strong_gender(w)
            if guard:
                if guard == stored:
                    report["ok"] += 1
                else:
                    report["mismatch"].append(_rec(r, ref=guard, basis="guard"))
            else:
                report["unknown"].append(_rec(r))
        done += len(chunk)
        if progress_cb:
            try:
                progress_cb(done, len(rows))
            except Exception:
                pass
    _excuse_plural_die(report)
    return report


def _excuse_plural_die(report: dict) -> None:
    """Снять обвинение с «die», когда написание — форма множественного числа.

    Артикль принадлежит ФОРМЕ, а у множественного он всегда die. Справочник же
    судит лемму: у «Geschwister» это das Geschwister (единственное), у «Pumps» —
    der Pumps (одна туфля). Аудит из-за этого предлагал «исправить» правильные
    die Geschwister и die Pumps, то есть внести ошибку в то единственное, чему
    тренажёр учит.

    Проверяем узко: только для строк с «die», только если страница САМА называет это
    же написание своей формой множественного числа. Сеть дёргается один раз и лишь по
    подозреваемым (их единицы), а не по всему банку.
    """
    suspects = [m for m in report["mismatch"] if m["stored"] == "die"]
    if not suspects:
        return
    from backend.article_wiktionary_ref import form_facts_for_titles
    try:
        facts = form_facts_for_titles(sorted({m["word"] for m in suspects}))
    except Exception:
        logging.warning("audit: разбор форм не удался, оставляем как есть", exc_info=True)
        return
    excused = []
    for m in suspects:
        f = facts.get(m["word"]) or {}
        surface = str(f.get("plural_surface") or "").strip().lower()
        if surface and surface == m["word"].strip().lower():
            excused.append(m)
    if not excused:
        return
    for m in excused:
        report["mismatch"].remove(m)
        # в «ambiguous», а не в «ok»: строка правильная, но помечена — игра показывает
        # для таких значение. Счётчики остаются сходящимися: checked = ok + mismatch +
        # review + ambiguous + unknown.
        report["ambiguous"].append({**m, "why": "plural-die"})


def audit_all(theme_keys: list[str] | None = None, progress_cb=None) -> dict:
    """Read-only audit. Returns a report dict with mismatch/conflict/ambiguous/unknown
    buckets and counts. Changes nothing."""
    rows = _iter_rows(theme_keys)
    report = _classify(rows, progress_cb=progress_cb)
    report["counts"] = {
        "checked": report["checked"], "ok": report["ok"],
        "mismatch": len(report["mismatch"]), "review": len(report["review"]),
        "ambiguous": len(report["ambiguous"]), "unknown": len(report["unknown"]),
    }
    return report


def apply_fixes(theme_keys: list[str] | None = None, progress_cb=None) -> dict:
    """Apply the confirmed corrections (mismatch bucket only) to the article bank.
    Review / ambiguous / unknown rows are left untouched for manual decision.
    Re-derives from the (now cached) reference, so it fixes exactly what a fresh
    audit reports."""
    report = audit_all(theme_keys, progress_cb=progress_cb)
    from backend.database import update_article_sprint_article
    fixed = 0
    examples: list[str] = []
    for m in report["mismatch"]:
        try:
            update_article_sprint_article(m["id"], m["ref"])
            fixed += 1
            if len(examples) < 25:
                examples.append(f"{m['stored']} → {m['ref']} {m['word']} ({m['basis']})")
        except Exception:
            logging.warning("apply_fixes: update failed id=%s word=%s", m["id"], m["word"], exc_info=True)
    return {"fixed": fixed, "attempted": len(report["mismatch"]),
            "review": len(report["review"]), "ambiguous": len(report["ambiguous"]),
            "unknown": len(report["unknown"]), "examples": examples}
