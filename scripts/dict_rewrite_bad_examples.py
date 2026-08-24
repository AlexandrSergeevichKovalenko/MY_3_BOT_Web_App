# -*- coding: utf-8 -*-
"""ХВОСТ ПЕРВЫЙ: переписать примеры, которые панель признала кривыми.

ЧТО ЧИНИМ. Проход по 5 073 фразам дал 655 карточек, где дефект в ПРИМЕРАХ — то есть в
том, что сочинили мы, а не человек. Заголовок его, примеры наши; переписываем только наше.

ПОЧЕМУ ОНИ САМИ НЕ ПОЧИНЯТСЯ. Ночной добор берёт `kind='word' AND card IS NULL` — только
одиночные слова без разбора. Фразы он не трогает никогда, и карточка с кривым примером
осталась бы такой навсегда. Второй голос, поставленный в дверь записи, защищает будущее,
но прошлое надо переписать руками — вот этим скриптом.

КАК. Пишет OpenAI, проверяет Google — тот же порядок, что и в двери:
  1. просим два новых примера к заголовку и сохранённому смыслу;
  2. отдаём их второму голосу (backend/second_voice_check);
  3. пишем ТОЛЬКО одобренное. Забраковано или не спросили — карточку не трогаем,
     отметка «дефект» остаётся, и слово попадёт в следующий заход.

⚠ ПОЧЕМУ ЗАПИСЬ ИДЁТ С ОСОБЫМ ИСТОЧНИКОМ. `save_unit_card` сам зовёт второй голос для
источников, где текст сочинила модель. Здесь проверка УЖЕ прошла в этом скрипте, и
второй платный запрос был бы просто тратой денег на тот же ответ. Поэтому источник
называется «примеры переписаны, проверено» и в список сочиняющих не входит — но дверь
всё равно применяет к записи свои остальные правила: разворот к языку слова и стража
размноженного текста.

⛔ ПОТОЛОК РАСХОДА обязателен: владелец поднял предел на €3 именно под этот заход.
Достигли — останавливаемся, непереписанные остаются с отметкой «дефект».

    python3 scripts/dict_rewrite_bad_examples.py --limit 5
    python3 scripts/dict_rewrite_bad_examples.py --apply --budget 3.2
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

from backend.database import get_db_connection_context      # noqa: E402
from backend import lex_units                               # noqa: E402
from backend.second_voice_check import review_new_card      # noqa: E402

WRITER_MODEL = "gpt-4.1-2025-04-14"
PRICE_IN, PRICE_OUT = 2.0 / 1e6, 8.0 / 1e6      # из bt_3_billing_price_snapshots
SOURCE = "примеры переписаны, проверено"

SYSTEM = """You write usage examples for one entry of a German↔Russian learner's dictionary.

You get the entry (a phrase or sentence a learner saved), its saved Russian meaning, and
the current examples, which were judged wrong.

Write TWO new examples. Rules:
  • each example must genuinely use the entry — German inflects, so the verb may be
    conjugated, the noun may take a case, the word order may change; that is expected;
  • the German must be natural, grammatical, everyday German a native would say;
  • the Russian must translate ITS OWN German sentence, not the entry;
  • if the entry has a saved meaning, the examples must show THAT meaning — an idiom must
    be shown as an idiom, not by its literal words;
  • no placeholders, no quotation of the entry as a bare fragment, no repeating the old
    broken examples.

Answer STRICT JSON: {"examples":[{"source":"<German>","target":"<Russian>"},{...}]}"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--budget", type=float, default=3.2, help="потолок расхода, USD")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.unit_id, u.display, u.kind, u.card, c.reference
                FROM bt_3_field_checks c
                JOIN bt_3_lex_units u ON u.id = c.unit_id
                WHERE c.field = 'phrase_panel' AND c.verdict = 'дефект'
                  AND u.card IS NOT NULL
                ORDER BY c.unit_id;""")
            rows = cur.fetchall()
    if args.limit:
        rows = rows[:args.limit]
    print(f"карточек с кривыми примерами: {len(rows)}\n")

    os.environ.setdefault("OPENAI_API_KEY", lex_units.os.getenv("OPENAI_API_KEY") or "")
    if not os.getenv("OPENAI_API_KEY"):
        import subprocess
        os.environ["OPENAI_API_KEY"] = json.loads(subprocess.run(
            ["railway", "variables", "--service", "BACKEND_WEB(backend:server.py)", "--json"],
            capture_output=True, text=True).stdout)["OPENAI_API_KEY"]
    from openai import OpenAI
    client = OpenAI(timeout=60.0, max_retries=1)

    spent = 0.0
    tally = {"переписано": 0, "забраковано": 0, "не спросили": 0, "модель молчит": 0}
    started = time.time()

    for index, (unit_id, display, kind, card, why) in enumerate(rows, 1):
        if spent >= args.budget:
            print(f"\n⛔ ПОТОЛОК ${args.budget:.2f} ДОСТИГНУТ на {index-1}-й карточке.")
            print(f"   непереписанных осталось {len(rows) - index + 1}, отметка «дефект» на них"
                  " сохранена — попадут в следующий заход.")
            break

        meaning = str((card or {}).get("translation_ru") or "")
        ask = {"entry": display, "kind": kind, "saved_meaning": meaning,
               "broken_examples": (card or {}).get("usage_examples"),
               "what_was_wrong": str(why or "")}
        try:
            answer = client.chat.completions.create(
                model=WRITER_MODEL, temperature=0.3,
                response_format={"type": "json_object"},
                messages=[{"role": "system", "content": SYSTEM},
                          {"role": "user", "content": json.dumps(ask, ensure_ascii=False)}])
            spent += (answer.usage.prompt_tokens * PRICE_IN
                      + answer.usage.completion_tokens * PRICE_OUT)
            fresh = json.loads(answer.choices[0].message.content).get("examples") or []
        except Exception as exc:                       # noqa: BLE001
            tally["модель молчит"] += 1
            print(f"   {str(display)[:40]:42} модель молчит: {type(exc).__name__}")
            continue
        fresh = [e for e in fresh
                 if isinstance(e, dict) and str(e.get("source") or "").strip()
                 and str(e.get("target") or "").strip()][:3]
        if not fresh:
            tally["модель молчит"] += 1
            continue

        candidate = dict(card or {})
        candidate["usage_examples"] = fresh
        review = review_new_card(headword=str(display or ""), card=candidate,
                                 kind=str(kind or "word"))
        # Цену проверки берём по её же токенам нельзя — модуль их не возвращает; она
        # известна из замера: ~$0.0015 за карточку. Считаем по замеру, чтобы потолок
        # не оказался фикцией, как это уже было с «размышлениями» Gemini.
        spent += 0.0015
        if not review.get("checked"):
            tally["не спросили"] += 1
            print(f"   {str(display)[:40]:42} второй голос не ответил")
            continue
        if not review.get("ok"):
            tally["забраковано"] += 1
            print(f"   {str(display)[:40]:42} забраковано: {str(review.get('why'))[:44]}")
            continue

        print(f"   {str(display)[:40]:42} ✓ {str(fresh[0].get('source'))[:44]}")
        if not args.apply:
            continue
        if lex_units.save_unit_card(int(unit_id), candidate, source=SOURCE):
            tally["переписано"] += 1
            with get_db_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute("""UPDATE bt_3_field_checks
                                   SET verdict='подтверждено',
                                       source='примеры переписаны и проверены вторым голосом',
                                       checked_at=NOW()
                                   WHERE unit_id=%s AND field='phrase_panel';""", (unit_id,))
                    conn.commit()

    print("\n— ИТОГ")
    for key, count in sorted(tally.items(), key=lambda kv: -kv[1]):
        print(f"   {key:16} {count:>5}")
    print(f"\n   потрачено ${spent:.2f} из ${args.budget:.2f}, время {(time.time()-started)/60:.0f} мин")
    if not args.apply:
        print("\n(холостой прогон: ничего не записано, нужен --apply)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
