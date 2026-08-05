"""Карточка должна указывать на СВОЁ слово, а фраза с разбором — иметь перевод.

Две накопленные беды, обе видны человеку.

ПЕРВАЯ: указатель ведёт на чужую единицу.
    карточка «Du stinkst furchtbar.»          → единица «Ты ужасно воняешь.»
    карточка «das Wetter wird zunehmend …»    → единица «der Wetter werden zunehmend …»
    карточка «menschliche Überreste»          → единица «menschlich Überrest»
Первый случай — русскую сторону завели как немецкую единицу. Второй и третий — при
заведении каждое слово предложения прогнали через приведение к начальной форме, и
получился текст, которого в языке нет.

Чем это плохо: разбор с такой единицы к карточке не приезжает (сверка заголовка его
отбивает — и правильно делает), а сама единица недостижима поиском. Слово живёт, но
как будто его нет.

Живой код давно заводит единицу по самому слову карточки, поэтому новых таких связей не
появляется. Здесь чиним накопленное: перецепляем карточку на единицу её собственного
слова. Старую единицу не трогаем — вдруг на ней висит чужой разбор.

ВТОРАЯ: у фразы есть разбор, но нет перевода. Словарь такую единицу не отдаёт вовсе
(«единица есть, а перевода нет — отдавать нечего»), и оплаченный разбор лежит мёртвым.
Собираем перевод из самого разбора — тем же кодом, что и обычная синхронизация.

По умолчанию НИЧЕГО НЕ ПИШЕТ. Запись — только с --apply.

    python scripts/dict_units_relink_cards.py           # вхолостую
    python scripts/dict_units_relink_cards.py --apply   # записать
"""

from __future__ import annotations

import argparse
import os
import re
import sys

_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

import lex_units  # noqa: E402
from database import get_db_connection_context  # noqa: E402

LATIN = re.compile(r"[A-Za-zÄÖÜäöüß]")
CYRILLIC = re.compile(r"[А-Яа-яЁё]")


def _is_german(text: str | None) -> bool:
    """Немецкая ли это сторона. Русский текст в немецкой единице — как раз то, что чиним."""
    body = str(text or "").strip()
    return bool(body) and bool(LATIN.search(body)) and not CYRILLIC.search(body)


def collect_wrong_links() -> list[dict]:
    """Карточки, чей указатель ведёт на единицу другого слова."""
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT q.id, q.word_de, u.id, u.display, u.lang
                FROM bt_3_webapp_dictionary_queries q
                JOIN bt_3_lex_units u ON u.id = q.lex_unit_id
                WHERE COALESCE(q.word_de, '') <> ''
                  AND LOWER(q.word_de) <> LOWER(COALESCE(u.display, ''))
            """)
            rows = cursor.fetchall()

    plan = []
    for card_id, word_de, unit_id, display, lang in rows:
        if not _is_german(word_de):
            continue  # нечего перецеплять: у карточки нет немецкой стороны
        # Написания совпали — значит это одно слово, просто разная запись. Не трогаем.
        if lex_units.normalize_query(word_de) == lex_units.normalize_query(display or ""):
            continue
        plan.append({
            "card_id": int(card_id), "word": word_de,
            "old_unit": int(unit_id), "old_display": display, "old_lang": lang,
        })
    return plan


def collect_phrases_without_translation() -> list[dict]:
    """Фразы, у которых разбор есть, а перевода нет — словарь их не отдаёт."""
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT u.id, u.display, u.kind, u.card
                FROM bt_3_lex_units u
                WHERE u.lang = 'de' AND u.card IS NOT NULL
                  AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links l
                                  WHERE l.from_unit = u.id AND l.rank < 900)
            """)
            rows = cursor.fetchall()
    return [{"unit_id": int(r[0]), "display": r[1], "kind": r[2], "card": r[3]} for r in rows]


def apply_relink(plan: list[dict]) -> dict:
    done = {"relinked": 0, "same_unit": 0, "errors": 0}
    for row in plan:
        try:
            unit_id = lex_units.ensure_unit(row["word"], "de")
            if not unit_id:
                done["errors"] += 1
                continue
            if int(unit_id) == int(row["old_unit"]):
                done["same_unit"] += 1
                continue
            with get_db_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE bt_3_webapp_dictionary_queries SET lex_unit_id = %s "
                        "WHERE id = %s;",
                        (int(unit_id), int(row["card_id"])),
                    )
                conn.commit()
            done["relinked"] += 1
        except Exception as exc:  # noqa: BLE001
            done["errors"] += 1
            print(f"   ! карточка {row['card_id']}: {exc}")
    return done


def apply_links(rows: list[dict]) -> dict:
    done = {"linked": 0, "nothing_to_link": 0, "errors": 0}
    for row in rows:
        try:
            report = lex_units.sync_unit_links_from_card(int(row["unit_id"]), row["card"])
            if report.get("links"):
                done["linked"] += 1
            else:
                done["nothing_to_link"] += 1
        except Exception as exc:  # noqa: BLE001
            done["errors"] += 1
            print(f"   ! единица {row['unit_id']}: {exc}")
    return done


def main() -> int:
    parser = argparse.ArgumentParser(description="Перецепить карточки и добрать переводы")
    parser.add_argument("--apply", action="store_true", help="записать (без флага — только отчёт)")
    parser.add_argument("--show", type=int, default=8, help="сколько примеров показать")
    args = parser.parse_args()

    wrong = collect_wrong_links()
    phrases = collect_phrases_without_translation()

    print("=" * 76)
    print("УКАЗАТЕЛИ И ПЕРЕВОДЫ" + ("  — ЗАПИСЬ" if args.apply else "  — вхолостую"))
    print("=" * 76)
    print(f"\nкарточек указывают на чужую единицу: {len(wrong)}")
    for row in wrong[: max(0, args.show)]:
        print(f"   карточка {str(row['word'])[:38]!r}")
        print(f"      сейчас ведёт на {str(row['old_display'])[:38]!r} (язык {row['old_lang']})")
    print(f"\nфраз с разбором, но без перевода:    {len(phrases)}")
    for row in phrases[: max(0, args.show)]:
        print(f"   {row['kind']:<12} {str(row['display'])[:46]}")

    if not args.apply:
        print("\nЭто был холостой прогон. Записать — тот же вызов с --apply.")
        return 0

    print("\nПерецепляю карточки…")
    done_links = apply_relink(wrong)
    print(f"   перецеплено {done_links['relinked']}, уже верных {done_links['same_unit']}, "
          f"ошибок {done_links['errors']}")
    print("\nСобираю переводы фразам…")
    done_tr = apply_links(phrases)
    print(f"   собрано {done_tr['linked']}, нечего собирать {done_tr['nothing_to_link']}, "
          f"ошибок {done_tr['errors']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
