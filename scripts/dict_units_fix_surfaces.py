"""Вернуть в поиск единицы, которые словарь не может найти.

Что сломано. Написание, по которому ищется единица, снимает артикль: «ein Gesetz
aushebeln» ищется по ключу «gesetz aushebeln». А часть единиц заведена массовой сборкой,
которая артикль сохранила, — их написание в базе так и лежит: «ein gesetz aushebeln».
Ключи не совпадают, и такая единица недостижима: словарь её не найдёт никогда, сколько
бы раз слово ни спросили. За 353 из них уже заплачен полный разбор — и он лежит мёртвым
грузом, а на повторный запрос мы снова идём к модели.

Что делает скрипт. Добавляет недостающее написание — то же слово, но по правильному
ключу. Ничего не удаляет и не меняет: только новая строка в таблице написаний.

Ключ уже занят другой единицей — пропускаем и пишем в отчёт. Это дубликаты одной фразы
(одна заведена с артиклем, другая без), их надо сливать осознанно, а не молча делать
так, чтобы на один запрос отвечали двое.

По умолчанию НИЧЕГО НЕ ПИШЕТ: показывает отчёт. Запись — только с --apply.

    python scripts/dict_units_fix_surfaces.py           # вхолостую
    python scripts/dict_units_fix_surfaces.py --apply   # записать
"""

from __future__ import annotations

import argparse
import os
import sys

_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

import lex_units  # noqa: E402
from database import get_db_connection_context  # noqa: E402


def collect() -> dict:
    report = {"add": [], "collision": []}
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT unit_id, lang, surface_key FROM bt_3_lex_surfaces")
            by_unit: dict[int, set[str]] = {}
            owner: dict[tuple[str, str], list[int]] = {}
            for unit_id, lang, key in cur.fetchall():
                by_unit.setdefault(int(unit_id), set()).add(key)
                owner.setdefault((lang, key), []).append(int(unit_id))

            cur.execute("""
                SELECT id, lang, kind, lemma, display,
                       COALESCE(LENGTH(card::text), 0)
                FROM bt_3_lex_units
            """)
            rows = cur.fetchall()

    for unit_id, lang, kind, lemma, display, card_size in rows:
        want = lex_units.normalize_query(display or lemma or "")
        if not want or want in by_unit.get(int(unit_id), set()):
            continue
        others = [x for x in owner.get((lang, want), []) if x != int(unit_id)]
        row = {
            "unit_id": int(unit_id), "lang": lang, "kind": kind,
            "text": display or lemma, "key": want, "card": int(card_size or 0),
        }
        if others:
            row["others"] = others[:3]
            report["collision"].append(row)
        else:
            report["add"].append(row)
    return report


def apply_report(report: dict) -> dict:
    done = {"added": 0, "errors": 0}
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for row in report["add"]:
                try:
                    cur.execute(
                        "INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind) "
                        "VALUES (%s, %s, %s, 'exact') ON CONFLICT DO NOTHING;",
                        (row["lang"], row["key"], row["unit_id"]),
                    )
                    done["added"] += 1
                except Exception as exc:  # noqa: BLE001
                    done["errors"] += 1
                    print(f"   ! единица {row['unit_id']}: {exc}")
        conn.commit()
    return done


def main() -> int:
    parser = argparse.ArgumentParser(description="Вернуть недостижимые единицы в поиск")
    parser.add_argument("--apply", action="store_true", help="записать (без флага — только отчёт)")
    args = parser.parse_args()

    report = collect()
    with_card = sum(1 for r in report["add"] if r["card"] > 800)
    print("=" * 70)
    print("НЕДОСТИЖИМЫЕ ЕДИНИЦЫ" + ("  — ЗАПИСЬ" if args.apply else "  — вхолостую"))
    print("=" * 70)
    print(f"\nдобавим написание:            {len(report['add'])}")
    print(f"   из них с готовым разбором: {with_card}")
    print(f"пропустим, ключ занят:        {len(report['collision'])}")

    print("\nПРИМЕРЫ (первые 12):")
    for row in report["add"][:12]:
        print(f"   {row['text'][:44]:<46} {row['kind']:<12} ключ {row['key'][:34]!r}")

    if report["collision"]:
        print("\nКЛЮЧ ЗАНЯТ — это дубликаты одной фразы, сливать отдельно:")
        for row in report["collision"]:
            print(f"   #{row['unit_id']:<7} {row['text'][:40]:<42} ключ {row['key'][:30]!r} "
                  f"уже у {row['others']}")

    if not args.apply:
        print("\nЭто был холостой прогон. Записать — тот же вызов с --apply.")
        return 0

    print("\nПишу…")
    done = apply_report(report)
    print(f"\nготово: добавлено написаний {done['added']}, ошибок {done['errors']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
