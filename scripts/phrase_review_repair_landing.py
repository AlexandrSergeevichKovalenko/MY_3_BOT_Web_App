# -*- coding: utf-8 -*-
"""Доделать то, что не доехало после решений владельца по спорным фразам.

Две починки, обе — уборка последствий двух дефектов, закрытых в коде 19.08.2026:

  A. ПРАВКА РЕГИСТРА НЕ РАЗЪЕХАЛАСЬ.
     spread_correction_everywhere сравнивал старый и новый текст без учёта регистра и
     на правке «Es ist mir Latte» → «latte» решал, что менять нечего. Справочник
     владелец поправил, а в карточке человека осталось старое написание. В немецком
     регистр — это грамматика, а не оформление.

  B. ПЕРЕВОД ВЛАДЕЛЬЦА ПОТЕРЯЛСЯ.
     Перевод поднимался главным ВНУТРИ пересборки разбора, то есть после запроса к
     модели. Сорвался запрос — пропал и выбор человека. Здесь перевод ставится главным
     напрямую, без модели: какой вариант был нажат, видно по тексту, который лёг в
     справочник.

Правило отбора берётся у продукта (phrase_review_variants, clean_text), а не
придумывается заново.

    python3 scripts/phrase_review_repair_landing.py           # показать, ничего не писать
    python3 scripts/phrase_review_repair_landing.py --apply   # починить
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

from backend.database import (  # noqa: E402
    get_db_connection_context,
    phrase_review_variants,
    promote_owner_translation,
    spread_correction_everywhere,
)
from backend.dictionary_intake import clean_text  # noqa: E402


def squash(value: str) -> str:
    return " ".join(str(value or "").split()).strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    spread_todo: list[tuple] = []   # (review_id, unit_id, old, new, сколько карточек)
    ru_todo: list[tuple] = []       # (review_id, unit_id, display, перевод с кнопки, что сверху)

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT r.id, r.unit_id, r.text, r.judges, r.arbiter, u.display
                     FROM bt_3_phrase_review r
                     JOIN bt_3_lex_units u ON u.id = r.unit_id
                    WHERE r.status = 'accepted'
                    ORDER BY r.id;"""
            )
            rows = cursor.fetchall()

            for review_id, unit_id, old_text, judges, arbiter, display in rows:
                old_text, display = squash(old_text), squash(display)
                if not display or display == old_text:
                    continue

                # ── A. старый текст ещё лежит у людей ──────────────────────────
                cursor.execute(
                    """SELECT count(*) FROM bt_3_webapp_dictionary_queries
                        WHERE lex_unit_id = %s
                          AND (word_de = %s OR translation_de = %s);""",
                    (unit_id, old_text, old_text),
                )
                stale_cards = (cursor.fetchone() or (0,))[0]
                if stale_cards:
                    spread_todo.append((review_id, unit_id, old_text, display, stale_cards))

                # ── B. перевод с нажатой кнопки не главный ─────────────────────
                variants = phrase_review_variants(
                    judges if isinstance(judges, list) else [], old_text,
                    arbiter if isinstance(arbiter, dict) else None)
                chosen = next(
                    (v for v in variants
                     if squash(clean_text(v["text"]) or "") == display), None)
                button_ru = squash((chosen or {}).get("ru") or "")
                if not button_ru:
                    continue      # какой вариант нажали — неизвестно, гадать нельзя
                cursor.execute(
                    """SELECT u.display FROM bt_3_lex_links l
                         JOIN bt_3_lex_units u ON u.id = l.to_unit
                        WHERE l.from_unit = %s ORDER BY l.rank LIMIT 1;""",
                    (unit_id,),
                )
                top = squash((cursor.fetchone() or ("",))[0])
                if top.casefold() != button_ru.casefold():
                    ru_todo.append((review_id, unit_id, display, button_ru, top))

    print("\nА. ПРАВКА НЕ ДОЕХАЛА ДО КАРТОЧЕК ЛЮДЕЙ: %d\n" % len(spread_todo))
    for review_id, unit_id, old, new, cards in spread_todo:
        print(f"   #{review_id} слово {unit_id}, карточек {cards}")
        print(f"       было : {old!r}")
        print(f"       стало: {new!r}")

    print("\nБ. ПЕРЕВОД ВЛАДЕЛЬЦА НЕ ГЛАВНЫЙ: %d\n" % len(ru_todo))
    for review_id, unit_id, display, button_ru, top in ru_todo:
        print(f"   #{review_id} слово {unit_id}  {display!r}")
        print(f"       он выбрал: {button_ru!r}")
        print(f"       а сверху : {top!r}")

    if not args.apply:
        print("\nВХОЛОСТУЮ. Починить: --apply\n")
        return 0

    done = {"карточек": 0, "мест": 0, "пул": 0, "переводов": 0, "не вышло": 0}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            for _review_id, unit_id, old, new, _cards in spread_todo:
                report = spread_correction_everywhere(
                    cursor, unit_id=unit_id, old_text=old, new_text=new)
                done["карточек"] += report["cards"]
                done["мест"] += report["places"]
                done["пул"] += report["pool"]
        conn.commit()

    for _review_id, unit_id, _display, button_ru, _top in ru_todo:
        if promote_owner_translation(unit_id, button_ru):
            done["переводов"] += 1
        else:
            done["не вышло"] += 1

    print("\nГОТОВО: %s\n" % done)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
