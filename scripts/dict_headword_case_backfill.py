"""Немецкий глагол не должен храниться с заглавной буквы.

В чём беда. Слово часто приезжает из начала предложения — «Gelingen», «Schlank» — и так
и сохраняется. Дальше по этому написанию строится таблица спряжения, и человек видит
«ich Gelinge / wir Gelingen». Для существительного заглавная обязательна, а для глагола,
прилагательного и наречия — ошибка.

На сохранении это уже перехватывается (`_fix_headword_case_before_save`). Здесь чиним
накопленное — и сразу во ВСЕХ ТРЁХ местах, где слово живёт:

  • личная карточка человека  — её видят библиотека, тренажёр, повторения;
  • общая запись пула         — из неё отвечает словарь на повторный запрос;
  • единица слоя              — дом разбора, из неё читает поиск.

Порознь чинить нельзя: починишь карточку — «Gelingen» останется в поиске, починишь
поиск — останется в озвучке. Ровно та болезнь, которую мы весь день лечим.

Трогаем только одиночное слово с известной частью речи. Заголовок-предложение
(«Das Problem wurde behoben.») не трогаем: там заглавная стоит по делу.

Ключи поиска не меняются: они и так приведены к нижнему регистру, поэтому слово
продолжит находиться и по старому написанию.

По умолчанию НИЧЕГО НЕ ПИШЕТ: показывает «было / стало». Запись — только с --apply.

    python scripts/dict_headword_case_backfill.py           # вхолостую
    python scripts/dict_headword_case_backfill.py --apply   # записать
"""

from __future__ import annotations

import argparse
import json
import os
import sys

_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

from database import get_db_connection_context  # noqa: E402

# Те же части речи, что у стража на сохранении: у существительного заглавная обязательна.
LOWERCASE_POS = {"verb", "adjective", "adverb"}


def needs_lowering(word: str, pos: str) -> bool:
    """Нужно ли опустить регистр — и до какой степени можно доверять части речи.

    Обычная заглавная в начале («Gelingen», «Schlank») — ошибка для глагола,
    прилагательного и наречия, правило то же, что у стража на сохранении.

    Заглавная ВНУТРИ слова («eRGATTERN») — след прежней версии этой же правки. Но
    трогаем такое ТОЛЬКО у глагола: холостой прогон 05.08.2026 показал, что среди
    таких строк лежат «zEITSCHRIFT» и «eROBERUNG» — существительные с неверно
    проставленной частью речи, и опустить их значило бы испортить. У глагола заглавной
    не бывает никогда, ему верить можно."""
    body = str(word or "").strip()
    if not body or " " in body or body == body.lower():
        return False
    kind = str(pos or "").strip().lower()
    if any(ch.isupper() for ch in body[1:]):
        return kind == "verb"
    return kind in LOWERCASE_POS


def lowered(word: str) -> str:
    """Строчная буква — по-человечески, а не механической заменой первого символа.

    «ERGATTERN» набрано капсом целиком: заменишь только первую букву — получишь
    «eRGATTERN», то есть хуже, чем было. Такое слово опускаем полностью."""
    body = str(word or "").strip()
    # Заглавные внутри слова («ERGATTERN», «eRGATTERN», «GeLingen») — это сломанное
    # написание, а не орфография: опускаем целиком. Обычное «Gelingen» — только первую.
    if any(ch.isupper() for ch in body[1:]):
        return body.lower()
    return body[:1].lower() + body[1:]


def _patch_json(payload, old: str, new: str):
    """Поправить написание внутри разбора — только там, где стоит ровно это слово."""
    if not isinstance(payload, dict):
        return None
    patched = dict(payload)
    changed = False
    for key in ("word_de", "source_text", "word_source", "translation_de"):
        if str(patched.get(key) or "").strip() == old:
            patched[key] = new
            changed = True
    return patched if changed else None


def collect() -> dict:
    plan = {"cards": [], "pool": [], "units": []}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, word_de, response_json->>'part_of_speech', response_json,
                       source_lang, target_lang
                FROM bt_3_webapp_dictionary_queries
                -- Заглавная где угодно в слове, не только первая: «eRGATTERN» тоже сюда.
                WHERE word_de <> LOWER(word_de) AND position(' ' in word_de) = 0
            """)
            for entry_id, word, pos, payload, src, tgt in cursor.fetchall():
                if needs_lowering(word, pos):
                    plan["cards"].append({
                        "id": entry_id, "was": word, "now": lowered(word), "pos": pos,
                        "json": _patch_json(payload, word, lowered(word)),
                    })

            cursor.execute("""
                SELECT id, word_de, source_text, response_json->>'part_of_speech', response_json
                FROM bt_3_dictionary_entries
                -- Заглавная где угодно в слове, не только первая: «eRGATTERN» тоже сюда.
                WHERE word_de <> LOWER(word_de) AND position(' ' in word_de) = 0
            """)
            for entry_id, word, source_text, pos, payload in cursor.fetchall():
                if needs_lowering(word, pos):
                    plan["pool"].append({
                        "id": entry_id, "was": word, "now": lowered(word), "pos": pos,
                        "source_text": source_text,
                        "json": _patch_json(payload, word, lowered(word)),
                    })

            cursor.execute("""
                SELECT id, display, lemma, pos, card->>'part_of_speech'
                FROM bt_3_lex_units
                WHERE lang = 'de' AND kind = 'word'
                  AND display <> LOWER(display) AND position(' ' in display) = 0
            """)
            for unit_id, display, lemma, pos, card_pos in cursor.fetchall():
                if needs_lowering(display, pos or card_pos):
                    plan["units"].append({
                        "id": unit_id, "was": display, "now": lowered(display),
                        "pos": pos or card_pos, "lemma": lemma,
                    })
    return plan


def apply_plan(plan: dict) -> dict:
    done = {"cards": 0, "pool": 0, "units": 0, "errors": 0}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            for row in plan["cards"]:
                try:
                    if row["json"] is not None:
                        cursor.execute(
                            "UPDATE bt_3_webapp_dictionary_queries "
                            "SET word_de = %s, response_json = %s::jsonb, updated_at = NOW() "
                            "WHERE id = %s;",
                            (row["now"], json.dumps(row["json"], ensure_ascii=False), row["id"]),
                        )
                    else:
                        cursor.execute(
                            "UPDATE bt_3_webapp_dictionary_queries "
                            "SET word_de = %s, updated_at = NOW() WHERE id = %s;",
                            (row["now"], row["id"]),
                        )
                    done["cards"] += 1
                except Exception as exc:  # noqa: BLE001
                    done["errors"] += 1
                    print(f"   ! карточка {row['id']}: {exc}")

            for row in plan["pool"]:
                try:
                    # Текст запроса правим только если он и есть это слово: ключи
                    # уникальности считаются по нормализованному тексту и не меняются.
                    same_source = str(row.get("source_text") or "").strip() == row["was"]
                    if row["json"] is not None:
                        cursor.execute(
                            "UPDATE bt_3_dictionary_entries SET word_de = %s, "
                            "source_text = CASE WHEN %s THEN %s ELSE source_text END, "
                            "response_json = %s::jsonb, updated_at = NOW() WHERE id = %s;",
                            (row["now"], same_source, row["now"],
                             json.dumps(row["json"], ensure_ascii=False), row["id"]),
                        )
                    else:
                        cursor.execute(
                            "UPDATE bt_3_dictionary_entries SET word_de = %s, "
                            "source_text = CASE WHEN %s THEN %s ELSE source_text END, "
                            "updated_at = NOW() WHERE id = %s;",
                            (row["now"], same_source, row["now"], row["id"]),
                        )
                    done["pool"] += 1
                except Exception as exc:  # noqa: BLE001
                    done["errors"] += 1
                    print(f"   ! запись пула {row['id']}: {exc}")

            for row in plan["units"]:
                try:
                    # lemma_key не трогаем: он и так в нижнем регистре, по нему идёт поиск.
                    cursor.execute(
                        "UPDATE bt_3_lex_units SET display = %s, "
                        "lemma = CASE WHEN lemma = %s THEN %s ELSE lemma END, "
                        "updated_at = NOW() WHERE id = %s;",
                        (row["now"], row["was"], row["now"], row["id"]),
                    )
                    done["units"] += 1
                except Exception as exc:  # noqa: BLE001
                    done["errors"] += 1
                    print(f"   ! единица {row['id']}: {exc}")
        conn.commit()
    return done


def main() -> int:
    parser = argparse.ArgumentParser(description="Строчная буква у глаголов во всех хранилищах")
    parser.add_argument("--apply", action="store_true", help="записать (без флага — только отчёт)")
    parser.add_argument("--show", type=int, default=10, help="сколько примеров на хранилище")
    args = parser.parse_args()

    plan = collect()
    print("=" * 72)
    print("ЗАГЛАВНАЯ БУКВА У ГЛАГОЛОВ" + ("  — ЗАПИСЬ" if args.apply else "  — вхолостую"))
    print("=" * 72)
    titles = {
        "cards": "личные карточки людей",
        "pool":  "общие записи пула",
        "units": "единицы слоя",
    }
    for key, title in titles.items():
        print(f"\n{title}: {len(plan[key])}")
        for row in plan[key][: max(0, args.show)]:
            print(f"   {row['was'][:28]:<30} → {row['now'][:28]:<30} ({row['pos']})")

    total = sum(len(plan[k]) for k in plan)
    print(f"\nвсего исправлений: {total}")
    if not args.apply:
        print("\nЭто был холостой прогон. Записать — тот же вызов с --apply.")
        return 0

    print("\nПишу…")
    done = apply_plan(plan)
    print(f"\nготово: карточек {done['cards']}, записей пула {done['pool']}, "
          f"единиц {done['units']}, ошибок {done['errors']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
