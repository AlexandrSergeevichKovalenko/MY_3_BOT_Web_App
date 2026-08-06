"""Ночная проверка грамматики фраз общего словаря.

Зачем. Артикль у слова сверяется со справочником бесплатно, а у фразы справочника нет:
«der Titanic rammen ein Eisberg und beginnen zu sinken» ни в одном словаре не лежит.
Грамматику предложения может рассудить только язык, а это деньги — значит партиями,
с потолком и с отчётом.

Два судьи, и это главное правило. Один и тот же вопрос задаётся ДВАЖДЫ, независимо.
Молча правим ТОЛЬКО когда оба назвали одну категорию и выдали дословно один и тот же
исправленный текст. Разошлись хоть в букве — фраза уходит владельцу на решение, а не
в базу. Проверено на выборке 06.08.2026: расхождение — верный признак того, что модель
не уверена и придумывает («предлог не тот» при том, что в правке дописана только точка).

Молча правим только ошибки, которые не зависят от контекста: опечатка, согласование,
падеж, предлог. ПОРЯДОК СЛОВ — никогда: кусок, вырванный из предложения, законно
выглядит переставленным, и мы не знаем, откуда человек его взял. Такие всегда идут
владельцу. Решение владельца 06.08.2026; если их окажется слишком много для ручного
разбора, правило пересмотрим по числам, а не на ощупь.
"""
from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor

from backend.database import (
    count_open_phrase_reviews,
    count_phrases_left_for_grammar_check,
    mark_phrase_checked,
    pick_phrases_for_grammar_check,
    queue_phrase_for_review,
)

# Ошибки, которые верны или неверны сами по себе, независимо от контекста.
SILENT_CATEGORIES = {"rechtschreibung", "kongruenz", "kasus", "praeposition"}
NIGHT_CAP = int(os.getenv("PHRASE_NIGHT_CHECK_CAP", "500") or "500")
WORKERS = int(os.getenv("PHRASE_NIGHT_CHECK_WORKERS", "6") or "6")


def _judge_twice(text: str, kind: str) -> list[dict]:
    from backend.openai_manager import run_phrase_grammar_verdict

    out = []
    for _ in range(2):
        try:
            out.append(run_phrase_grammar_verdict(text=text, kind=kind) or {})
        except Exception as exc:
            logging.debug("судья фраз не ответил: %s", exc)
            out.append({})
    return out


def _both_agree(judges: list[dict]) -> tuple[bool, str, str]:
    """Согласны ли судьи ДОСЛОВНО. Возвращает (согласны, категория, исправленный текст)."""
    if len(judges) != 2:
        return False, "", ""
    a, b = judges
    if a.get("verdict") != "error" or b.get("verdict") != "error":
        return False, "", ""
    if (a.get("category") or "") != (b.get("category") or ""):
        return False, "", ""
    fix_a = str(a.get("corrected") or "").strip()
    fix_b = str(b.get("corrected") or "").strip()
    if not fix_a or fix_a != fix_b:
        return False, "", ""
    return True, str(a.get("category") or ""), fix_a


def _apply_silent_fix(unit_id: int, corrected: str) -> bool:
    """Записать исправленную фразу в слой слов. Ключ поиска пересобираем, старое
    написание оставляем рядом: по нему уже могли сохраниться карточки."""
    from backend.database import get_db_connection_context
    from backend.lex_units import normalize_query

    key = normalize_query(corrected)
    if not key:
        return False
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM bt_3_lex_units WHERE lang='de' AND lemma_key=%s AND id<>%s LIMIT 1;",
                    (key, int(unit_id)),
                )
                if cur.fetchone():
                    return False        # такое слово уже есть — сливать надо осознанно
                cur.execute(
                    "UPDATE bt_3_lex_units SET display=%s, lemma=%s, lemma_key=%s WHERE id=%s;",
                    (corrected, corrected, key, int(unit_id)),
                )
                cur.execute(
                    """INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                       VALUES ('de', %s, %s, 'exact') ON CONFLICT DO NOTHING;""",
                    (key, int(unit_id)),
                )
            conn.commit()
        return True
    except Exception as exc:
        logging.warning("правка фразы %s не записалась: %s", unit_id, exc)
        return False


def run_phrase_night_check(*, limit: int | None = None, dry_run: bool = False) -> dict:
    """Одна ночная партия. Возвращает отчёт для утреннего сообщения владельцу."""
    cap = int(limit if limit is not None else NIGHT_CAP)
    report = {"cap": cap, "picked": 0, "checked": 0, "fixed": 0, "doubt": 0, "errors": 0,
              "by_category": {}, "left": 0, "open_reviews": 0, "dry_run": bool(dry_run)}
    rows = pick_phrases_for_grammar_check(cap)
    report["picked"] = len(rows)
    if not rows:
        report["left"] = count_phrases_left_for_grammar_check()
        report["open_reviews"] = count_open_phrase_reviews()
        return report

    def work(row):
        return row, _judge_twice(row["text"], row["kind"])

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for row, judges in pool.map(work, rows):
            report["checked"] += 1
            if not any(j for j in judges):
                report["errors"] += 1
                continue
            agreed, category, corrected = _both_agree(judges)
            if agreed and category in SILENT_CATEGORIES:
                if dry_run or _apply_silent_fix(row["unit_id"], corrected):
                    report["fixed"] += 1
                    report["by_category"][category] = report["by_category"].get(category, 0) + 1
                    if not dry_run:
                        mark_phrase_checked(row["unit_id"], corrected, "fixed")
                    continue
            # Хоть один судья увидел ошибку, но согласия нет или это порядок слов —
            # решает владелец. Сюда же попадает всё «зависит от контекста».
            if any(str(j.get("verdict") or "") == "error" for j in judges):
                report["doubt"] += 1
                if not dry_run:
                    queue_phrase_for_review(
                        unit_id=row["unit_id"], text=row["text"],
                        translation=row["translation"], judges=judges,
                    )
                    mark_phrase_checked(row["unit_id"], row["text"], "doubt")
                continue
            if not dry_run:
                mark_phrase_checked(row["unit_id"], row["text"], "ok")

    report["left"] = count_phrases_left_for_grammar_check()
    report["open_reviews"] = count_open_phrase_reviews()
    return report
