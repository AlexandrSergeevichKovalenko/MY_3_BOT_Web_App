# -*- coding: utf-8 -*-
"""НОЧНОЙ ПОВТОР: карточки, где примеры не удалось переписать с первого раза.

ЗАЧЕМ. 24.08.2026 панель нашла 655 карточек с кривыми примерами, 544 переписаны сразу.
Осталось 111: у 30 второй голос забраковал предложенную замену, у 72 модель не дала
пригодных примеров, у 7 проверяющий не ответил. Владелец спросил ровно то, что и надо
было спросить: «а как число будет уменьшаться? есть ли механизм под капотом?»

Механизма не было. Число легло бы в отчёт и стояло там вечно — это и есть «список,
который никто не выполняет», запрещённый правилом «мы всё автоматизируем».

ЧТО ДЕЛАЕТ ЭТОТ МОДУЛЬ. Каждую ночь берёт маленькую порцию таких карточек и пробует
переписать примеры заново: пишет OpenAI, проверяет второй голос от Google, в базу идёт
только одобренное. Порция маленькая нарочно — это уборка, а не гонка.

ПОЧЕМУ ПОВТОР ВООБЩЕ ИМЕЕТ СМЫСЛ, а не крутит одно и то же:
  • у модели ненулевая температура: второй заход даёт другой вариант, а не копию;
  • проверяющий видит НОВЫЙ текст, а не тот, который уже забраковал;
  • справочники и корпус за ночь пополняются, и слово может стать понятнее.

⛔ БЕСКОНЕЧНОГО КРУГА НЕ БУДЕТ. Три неудачные попытки — и карточка уходит владельцу в ту
же очередь разбора спорных фраз, где он решает кнопками. Машина честно признаёт, что
исчерпала себя, вместо того чтобы жечь деньги каждую ночь на одном и том же слове.

⛔ ПОТОЛОК РАСХОДА обязателен: ночная работа без потолка однажды уже показала, что
счётчик может врать, и владелец увидел это на счёте, а не в логе.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

FIELD = "phrase_panel"
DEFECT, CLEAN, DISPUTED = "дефект", "подтверждено", "спорное"
# ┌─ ЗАВЕДЕНО 28.08.2026. ЧЕТВЁРТЫЙ ВЕРДИКТ: «ДОПОЛНИТЬ». ───────────────────────┐
# │ Человек на экране проверки почистил примеры сам и попросил добрать недостающие.│
# │ От «дефекта» это отличается принципиально: там примеры переписываются ЦЕЛИКОМ, │
# │ а здесь его собственные трогать нельзя — мы только дописываем недостающие.     │
# │ Владелец 28.08.2026: «твои останутся как есть».                                │
# └──────────────────────────────────────────────────────────────────────────────┘
TOP_UP = "дополнить"
ПРИМЕРОВ_В_КАРТОЧКЕ = 3          # столько их держит карточка; ниже — добираем
MAX_ATTEMPTS = 3
WRITER_MODEL = "gpt-4.1-2025-04-14"
PRICE_IN, PRICE_OUT = 2.0 / 1e6, 8.0 / 1e6         # bt_3_billing_price_snapshots
CHECK_COST = 0.0015                                 # замер второго голоса, 24.08.2026
SOURCE = "примеры переписаны, проверено"

SYSTEM = """You write usage examples for one entry of a German↔Russian learner's dictionary.

You get the entry (a phrase a learner saved), its saved Russian meaning, the current
examples that were judged wrong, and what a previous attempt got wrong.

Write TWO new examples. Rules:
  • each example must genuinely use the entry — German inflects, so the verb may be
    conjugated, the noun may take a case, the word order may change; that is expected;
  • the German must be natural, grammatical, everyday German a native would say;
  • the Russian must translate ITS OWN German sentence, not the entry;
  • if the entry has a saved meaning, the examples must show THAT meaning;
  • do not repeat the previous attempt: it was rejected.

Answer STRICT JSON: {"examples":[{"source":"<German>","target":"<Russian>"},{...}]}"""

TOP_UP_SYSTEM = """You add missing usage examples to one entry of a German↔Russian
learner's dictionary. A human already curated this card and kept the examples they
consider correct.

You get the entry, its saved Russian meaning, the examples the human KEPT, and how many
new ones are still missing.

Write EXACTLY the requested number of new examples. Rules:
  • never rewrite, translate differently, or comment on the kept examples — they stay
    exactly as they are and are shown to you only so you do not repeat them;
  • each new example must genuinely use the entry — German inflects, so the verb may be
    conjugated, the noun may take a case, the word order may change; that is expected;
  • the German must be natural, grammatical, everyday German a native would say;
  • the Russian must translate ITS OWN German sentence, not the entry;
  • the new examples must show the SAME meaning as the kept ones.

Answer STRICT JSON: {"examples":[{"source":"<German>","target":"<Russian>"},{...}]}"""


def ensure_attempts_column() -> None:
    """Счётчик попыток. Без него повтор не отличит первую неудачу от третьей."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE bt_3_field_checks "
                        "ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0;")
        conn.commit()


def count_open_defects() -> int:
    """Сколько карточек ждут переписывания. Это число видит владелец в утреннем отчёте."""
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM bt_3_field_checks "
                            "WHERE field = %s AND verdict = %s;", (FIELD, DEFECT))
                return int((cur.fetchone() or (0,))[0])
    except Exception:
        logging.warning("счётчик непереписанных примеров не прочитан", exc_info=True)
        return -1                                   # -1 говорит «не знаю», а не «ноль»


def _escalate(cur, unit_id: int, display: str, translation: str, why: str) -> None:
    """Машина исчерпала себя — вопрос уходит человеку, в уже существующую очередь."""
    judges = [{"verdict": "doubt", "category": "примеры не удалось переписать",
               "corrected": "", "why": f"{MAX_ATTEMPTS} попытки подряд: {why}"[:400]}]
    cur.execute("""INSERT INTO bt_3_phrase_review (unit_id, text, translation, judges, status)
                   VALUES (%s,%s,%s,%s::jsonb,'open') ON CONFLICT DO NOTHING;""",
                (unit_id, str(display or "")[:500], str(translation or "")[:500],
                 json.dumps(judges, ensure_ascii=False)))
    cur.execute("""UPDATE bt_3_field_checks SET verdict=%s, source=%s, checked_at=NOW()
                   WHERE unit_id=%s AND field=%s;""",
                (DISPUTED, "исчерпаны попытки, отдано владельцу", unit_id, FIELD))


def retry_batch(*, limit: int = 20, budget_usd: float = 0.30) -> dict[str, Any]:
    """Одна ночная порция. Возвращает отчёт числами — его печатает утренний доклад."""
    from backend.database import get_db_connection_context
    from backend import lex_units
    from backend.second_voice_check import review_new_card

    report = {"взято": 0, "переписано": 0, "снова не вышло": 0,
              "отдано владельцу": 0, "потрачено": 0.0, "осталось": 0}
    ensure_attempts_column()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.unit_id, u.display, u.kind, u.card, c.reference, c.attempts,
                       c.verdict
                FROM bt_3_field_checks c
                JOIN bt_3_lex_units u ON u.id = c.unit_id
                WHERE c.field = %s AND c.verdict = ANY(%s) AND u.card IS NOT NULL
                ORDER BY c.attempts, c.unit_id
                LIMIT %s;""", (FIELD, [DEFECT, TOP_UP], int(limit)))
            rows = cur.fetchall()
    report["взято"] = len(rows)
    if not rows:
        report["осталось"] = count_open_defects()
        return report

    from openai import OpenAI
    client = OpenAI(timeout=60.0, max_retries=1)
    spent = 0.0

    for unit_id, display, kind, card, why, attempts, verdict in rows:
        if spent >= budget_usd:
            logging.info("ночной повтор примеров: потолок $%.2f, остановились", budget_usd)
            break
        # ⚠ ДВА РАЗНЫХ ЗАДАНИЯ, И ПУТАТЬ ИХ НЕЛЬЗЯ.
        #   «дефект»    — примеры кривые, переписываем ЦЕЛИКОМ.
        #   «дополнить» — человек почистил их сам и попросил добрать недостающие.
        #                 Его примеры — не «broken», их трогать запрещено; модель
        #                 видит их только чтобы не повторить и попасть в тот же смысл.
        добор = str(verdict or "") == TOP_UP
        свои = [e for e in ((card or {}).get("usage_examples") or []) if isinstance(e, dict)]
        не_хватает = max(0, ПРИМЕРОВ_В_КАРТОЧКЕ - len(свои))
        if добор and not не_хватает:
            # Пока задание ждало ночи, примеров стало достаточно — добирать нечего.
            with get_db_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute("""UPDATE bt_3_field_checks SET verdict=%s, checked_at=NOW(),
                                          source='добирать было нечего'
                                    WHERE unit_id=%s AND field=%s;""", (CLEAN, unit_id, FIELD))
                conn.commit()
            continue
        ask = {"entry": display, "kind": kind,
               "saved_meaning": (card or {}).get("translation_ru")}
        if добор:
            ask["keep_these_examples_untouched"] = свои
            ask["write_only_this_many_new"] = не_хватает
        else:
            ask["broken_examples"] = (card or {}).get("usage_examples")
            ask["previous_attempt_was_rejected_because"] = str(why or "")[:300]
        try:
            answer = client.chat.completions.create(
                model=WRITER_MODEL, temperature=0.6,     # выше нуля: нужен ДРУГОЙ вариант
                response_format={"type": "json_object"},
                messages=[{"role": "system", "content": TOP_UP_SYSTEM if добор else SYSTEM},
                          {"role": "user", "content": json.dumps(ask, ensure_ascii=False)}])
            spent += (answer.usage.prompt_tokens * PRICE_IN
                      + answer.usage.completion_tokens * PRICE_OUT)
            fresh = [e for e in (json.loads(answer.choices[0].message.content).get("examples") or [])
                     if isinstance(e, dict) and str(e.get("source") or "").strip()
                     and str(e.get("target") or "").strip()][:ПРИМЕРОВ_В_КАРТОЧКЕ]
            if добор:
                # Примеры человека идут ПЕРВЫМИ и остаются нетронутыми — дописанное
                # встаёт за ними. Порядок здесь и есть обещание «твои останутся».
                fresh = (свои + fresh)[:ПРИМЕРОВ_В_КАРТОЧКЕ]
        except Exception as exc:                        # noqa: BLE001
            logging.warning("ночной повтор примеров: модель молчит на %s: %s", unit_id, exc)
            fresh = []

        ok, reason = False, "модель не дала примеров"
        if fresh:
            candidate = dict(card or {})
            candidate["usage_examples"] = fresh
            review = review_new_card(headword=str(display or ""), card=candidate,
                                     kind=str(kind or "word"))
            spent += CHECK_COST
            if review.get("checked") and review.get("ok"):
                ok = True
            else:
                reason = str(review.get("why") or "второй голос не ответил")[:200]

        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                if ok and lex_units.save_unit_card(int(unit_id), candidate, source=SOURCE):
                    cur.execute("""UPDATE bt_3_field_checks
                                   SET verdict=%s, source=%s, checked_at=NOW()
                                   WHERE unit_id=%s AND field=%s;""",
                                (CLEAN, "примеры добраны к правкам человека" if добор
                                 else "примеры переписаны ночным повтором",
                                 unit_id, FIELD))
                    report["переписано"] += 1
                elif int(attempts or 0) + 1 >= MAX_ATTEMPTS:
                    _escalate(cur, unit_id, display,
                              str((card or {}).get("translation_ru") or ""), reason)
                    report["отдано владельцу"] += 1
                else:
                    cur.execute("""UPDATE bt_3_field_checks
                                   SET attempts = attempts + 1, reference = %s, checked_at = NOW()
                                   WHERE unit_id=%s AND field=%s;""",
                                (reason[:400], unit_id, FIELD))
                    report["снова не вышло"] += 1
                conn.commit()

    report["потрачено"] = round(spent, 4)
    report["осталось"] = count_open_defects()
    logging.info("ночной повтор примеров: %s", report)
    return report
