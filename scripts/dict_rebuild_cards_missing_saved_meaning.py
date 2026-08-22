# -*- coding: utf-8 -*-
"""Пересобрать карточки, обогащённые МИМО смысла, который сохранил человек.

ОТКУДА КУЧА. До 22.08.2026 обогатителю передавалась только немецкая фраза: перевод
человека доезжал до `_rich_enrich_card_fields` аргументом, но использовался лишь чтобы
понять, какая сторона немецкая. Модель разбирала основное значение, потому что другого
не видела. Живой случай владельца: «die Hose anhaben» сохранено как «Быть главным»
(идиома), а карточка собралась про буквальное ношение брюк — управление «etwas anhaben —
Er hat einen Anzug an», примеры «Welche Hose hast du heute an?», мнемоника про одежду.

ДЫРА ЗАКРЫТА В КОДЕ: смысл теперь едет к модели отдельным полем вместе с названием
языка, и задание ставит его главным значением, а основное — вторым (решение владельца
22.08.2026). Здесь — уборка накопленного.

ПРАВИЛО ОТБОРА: сохранённый человеком перевод не встречается в разборе НИ РАЗУ — ни
значением, ни переводом, ни в примере. Сверка по подстроке, поэтому число это ВЕРХНЯЯ
оценка: часть расхождений — просто другая формулировка того же смысла. Замер 22.08.2026:
264 из 10 306 немецких слов с разбором и переводом.

КАЖДАЯ ПЕРЕСБОРКА — ОБРАЩЕНИЕ К МОДЕЛИ, то есть деньги. Поэтому по умолчанию скрипт
только считает и показывает; запись — с --apply, и порциями (--limit), чтобы прогон
можно было остановить и продолжить.

    python3 scripts/dict_rebuild_cards_missing_saved_meaning.py
    python3 scripts/dict_rebuild_cards_missing_saved_meaning.py --apply --limit 50
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context  # noqa: E402


def squash(value) -> str:
    return " ".join(str(value or "").split()).strip().casefold()


def card_mentions(card, meaning: str) -> bool:
    """Встречается ли сохранённый смысл в разборе хоть где-нибудь."""
    want = squash(meaning)
    if not want:
        return True

    def walk(node) -> bool:
        if isinstance(node, str):
            return want in squash(node)
        if isinstance(node, list):
            return any(walk(item) for item in node)
        if isinstance(node, dict):
            return any(walk(value) for value in node.values())
        return False

    return walk(card)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=0, help="сколько пересобрать за прогон")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            rows: list = []
            last_id = 0
            while True:
                cursor.execute(
                    """SELECT u.id, u.display, u.card,
                              (SELECT t.display FROM bt_3_lex_links l
                                 JOIN bt_3_lex_units t ON t.id = l.to_unit
                                WHERE l.from_unit = u.id AND t.lang = 'ru'
                                ORDER BY l.rank, l.id LIMIT 1)
                         FROM bt_3_lex_units u
                        WHERE u.lang = 'de' AND u.card IS NOT NULL AND u.id > %s
                        ORDER BY u.id LIMIT 400;""",
                    (last_id,),
                )
                batch = cursor.fetchall()
                if not batch:
                    break
                rows.extend(batch)
                last_id = batch[-1][0]

    targets = [(uid, disp, saved) for uid, disp, card, saved in rows
               if saved and not card_mentions(card, saved)]
    print(f"\nслов с разбором и переводом: {sum(1 for r in rows if r[3])}")
    print(f"смысла человека в разборе НЕТ: {len(targets)}\n")
    for uid, disp, saved in targets[:10]:
        print(f"  {uid:>6} {disp[:44]!r} ← сохранено {saved[:40]!r}")

    if not args.apply:
        print("\nСУХОЙ ПРОГОН. Пересобрать: --apply [--limit N]\n")
        return 0

    # Импорт здесь: модуль тянет веб-приложение, а сухому прогону оно не нужно.
    import backend.backend_server as server
    from backend.dictionary_intake import answer_language_is_wrong
    from backend.lex_units import save_unit_card

    queue = targets[: args.limit] if args.limit else targets
    done = failed = 0
    for index, (uid, disp, saved) in enumerate(queue, 1):
        item = server._rich_enrich_card_fields(
            source_text=disp, target_text=saved,
            source_lang="de", target_lang="ru", timeout_seconds=90,
        )
        if not item:
            failed += 1
            print(f"  [{index}/{len(queue)}] ⚠️ {disp[:44]!r} — разбор не получен, "
                  f"карточка оставлена как была")
            continue
        # ⚠ ГЛАВНОЕ ЗНАЧЕНИЕ ОБЯЗАНО БЫТЬ НА РУССКОМ. Поймано на первой же партии
        # 22.08.2026: у «Die Kosten beliefen sich auf etwa» модель вернула главным
        # значением «belaufen sich auf» — немецкую лемму вместо перевода, у
        # «Könnten Sie bitte dort vorn…» — «anhalten». Записать такое значит завести
        # карточку, где перевод на том же языке, что и слово. Не пишем и говорим вслух.
        primary_value = ((item.get("meanings") or {}).get("primary") or {}).get("value")
        if answer_language_is_wrong(primary_value, "ru"):
            failed += 1
            print(f"  [{index}/{len(queue)}] ⚠️ {disp[:40]!r} — перевод пришёл "
                  f"по-немецки ({str(primary_value)[:34]!r}), карточка не тронута")
            continue
        # Старую карточку не трогаем, пока новая не получена: сорвался запрос — у
        # человека остаётся прежний разбор, а не пустота.
        save_unit_card(uid, item, source="пересборка со смыслом человека")
        done += 1
        primary = ((item.get("meanings") or {}).get("primary") or {}).get("value")
        print(f"  [{index}/{len(queue)}] {disp[:40]!r} → {str(primary)[:40]!r}")

    left = len(targets) - len(queue)
    print(f"\nпересобрано: {done}, не смогли: {failed}, осталось: {left}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
