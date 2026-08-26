# -*- coding: utf-8 -*-
"""Накопленные КРУГИ в очереди спорных фраз: разобрать то, что уже лежит.

ЗАЧЕМ. Правило «ничего нового — не спрашиваем» встало на входе 26.08.2026
(`database.queue_phrase_for_review`), но в очереди владельца уже лежали вопросы,
заданные по второму и третьему разу. Замер того же дня: 17 фраз имеют больше одной
записи, 9 из них открыты СНОВА после того, как владелец по ним уже решал. Живой пример
— unit 5146: 13.08 он выбрал «auf die Vernehmung», 20.08 вернул «auf der», 26.08 у него
спросили в третий раз. Три решения — ноль движения.

Починить систему и оставить накопленное — это половина работы, а половина не
принимается (CLAUDE.md).

ЧТО ДЕЛАЕТ. По каждому открытому вопросу, который владелец уже решал:

  1. Спрашивает решающий голос — нет ли ВЕРНОГО текста, которого владелец не видел.
     Есть, и он прошёл проверку — вопрос остаётся открытым, но уже с ответом: это не
     круг, а новый вопрос. Так расчистился «Der Bus fährt 100 Personen mit» — фраза,
     которую владелец оставил «как есть» только потому, что верного варианта на экране
     не было вообще.
  2. Нечего ответить — вопрос закрывается в пользу ПОСЛЕДНЕГО решения владельца, а
     фраза помечается проверенной, чтобы не вернулась ночью. Его решение и есть ответ:
     новый вопрос был ошибкой судьи, а не находкой.

Ничего не удаляет и ничего не переписывает в словаре: трогает только очередь разбора.

    python3 scripts/phrase_review_close_the_circle.py            # показать, не менять
    python3 scripts/phrase_review_close_the_circle.py --apply    # разобрать
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

from backend.database import (  # noqa: E402
    _phrase_text_key, get_db_connection_context, list_open_phrase_reviews,
    phrase_check_text_hash, phrase_question_is_a_repeat, phrase_review_is_panel,
    phrase_review_settled_texts,
)
from backend.phrase_night_check import (  # noqa: E402
    _judge_proposals, answer_beyond_what_the_owner_saw,
)


def _repeats() -> list[dict]:
    """Открытые вопросы, по которым владелец уже высказывался."""
    out = []
    for it in list_open_phrase_reviews(1000):
        judges = it.get("judges") or []
        if phrase_review_is_panel(judges):
            continue                      # у карточек словаря своя очередь и свой вопрос
        settled = {_phrase_text_key(h.get("decided_text") or h.get("text") or "")
                   for h in (it.get("history") or [])}
        settled |= {_phrase_text_key(h.get("text") or "") for h in (it.get("history") or [])}
        settled.discard("")
        if not settled:
            continue
        if phrase_question_is_a_repeat(it["text"], _judge_proposals(judges), settled):
            out.append(it)
    return out


def _close_as_settled(review_id: int, unit_id: int, text: str) -> None:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE bt_3_phrase_review SET status = 'kept', decided_at = NOW(), "
                "decided_text = %s WHERE id = %s AND status = 'open';", (text, int(review_id)))
            cursor.execute(
                """INSERT INTO bt_3_phrase_check (unit_id, text_hash, verdict, checked_at)
                   VALUES (%s, %s, 'ok', NOW())
                   ON CONFLICT (unit_id) DO UPDATE
                     SET text_hash = EXCLUDED.text_hash, verdict = 'ok', checked_at = NOW();""",
                (int(unit_id), phrase_check_text_hash(text)))
        conn.commit()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    repeats = _repeats()
    print(f"открытых вопросов-повторов: {len(repeats)}\n")
    answered, closed = 0, 0
    for it in repeats:
        seen = sorted(phrase_review_settled_texts(it["unit_id"]))
        print(f"#{it['id']} unit {it['unit_id']}: {it['text']}")
        print(f"    владелец уже решал: {seen}")
        if not args.apply:
            continue
        if answer_beyond_what_the_owner_saw(
                unit_id=it["unit_id"], text=it["text"],
                translation=it.get("translation") or "", judges=it.get("judges") or []):
            answered += 1
            print("    → решающий голос дал текст, которого владелец не видел: вопрос остаётся")
            continue
        _close_as_settled(it["id"], it["unit_id"], it["text"])
        closed += 1
        print("    → отвечать нечем: это круг, закрыт в пользу решения владельца")

    if args.apply:
        print(f"\nостались с новым ответом: {answered}\nзакрыто как круг: {closed}")
    else:
        print("\nэто показ. Чтобы разобрать: --apply")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
