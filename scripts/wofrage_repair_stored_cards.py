# -*- coding: utf-8 -*-
"""Починка УЖЕ ЛЕЖАЩИХ в очереди «работа над ошибками» карточек Wo-Fragen.

Зачем
─────
24.08.2026 владельцу выпала карточка от 21.07: «Worauf nimmst du Rücksicht? —
Ребёнок». Вопрос о ВЕЩИ задан о человеке — верный ответ «Auf wen» карточка
считала ошибкой. Стражи сборки такое не пропускают с 29.07.2026, но очередь
ошибок хранит СВОЮ копию задания и показывает её месяцами.

Решение владельца 24.08.2026: такие карточки НЕ удалять, а привести к
правильному виду.

Как чиним (ничего не придумываем — всё из банка управления):

  • объект — человек, и с этим глаголом вопрос о человеке возможен
    → переводим карточку в режим ЧЕЛОВЕКА: верный ответ = предлог + wen/wem,
      пояснение и подсказка-правило пересобираются функциями генератора;
  • объект — человек, но вопрос о человеке с этим глаголом не по-немецки
    («bereit sein zu jemandem» не говорят), либо слово двусмысленное
    (das Opfer — и человек, и приношение)
    → оставляем вопрос о вещи, а объект заменяем на ПРОВЕРЕННЫЙ из сегодняшнего
      банка того же глагола. Ответ при этом не меняется — тренируется ровно то же.

Каждая починенная карточка перед записью прогоняется через ПОЛНУЮ проверку
check_item (ту же, что стоит на сборке). Не прошла — не пишем.

    python3 scripts/wofrage_repair_stored_cards.py            # показать, что будет сделано
    python3 scripts/wofrage_repair_stored_cards.py --apply    # записать
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context          # noqa: E402
import backend.wofrage_generator as G                           # noqa: E402


def _rebuild(payload: dict) -> tuple[dict | None, str]:
    """Правильная версия карточки. Логика живёт в генераторе — там же, где банк
    управления и проверка check_item: у починки и у сборки обязан быть один источник."""
    return G.repair_stored_item(payload)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="записать изменения в базу")
    args = ap.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, user_id, payload FROM bt_3_aufgabe_mistakes "
                        "WHERE format='wofrage' ORDER BY id;")
            rows = [(r[0], r[1], r[2] if isinstance(r[2], dict) else json.loads(r[2] or "{}"))
                    for r in cur.fetchall() or []]
    broken = [(i, u, p) for i, u, p in rows if G.stored_item_problems(p)]
    print(f"карточек Wo-Fragen в очереди ошибок: {len(rows)}; кривых: {len(broken)}")

    fixed, failed = [], []
    for mid, uid, p in broken:
        new, why = _rebuild(p)
        print(f"\n#{mid} (человек {uid}): {str(p.get('s') or '')} — {p.get('clue')}")
        print(f"   было не так: {'; '.join(G.stored_item_problems(p))}")
        if not new:
            print(f"   ❌ починить нельзя: {why}")
            failed.append(mid)
            continue
        print(f"   ✅ {why}")
        print(f"   стало: {new['s']} {new['clue']}  → {new['correct']}")
        print(f"   пояснение: {new['erklaerung']}")
        fixed.append((mid, new))

    if not args.apply:
        print(f"\nЭто прогон вхолостую. Записать: --apply  (готово к записи: {len(fixed)})")
        return 0

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for mid, new in fixed:
                cur.execute(
                    "UPDATE bt_3_aufgabe_mistakes SET payload=%s, correct_answer=%s WHERE id=%s;",
                    (json.dumps(new, ensure_ascii=False), new["correct"], mid),
                )
        conn.commit()
    print(f"\nЗаписано: {len(fixed)}. Не удалось починить: {len(failed)} {failed if failed else ''}")

    # Проверка ФАКТОМ: перечитываем из базы то, что записали.
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, payload FROM bt_3_aufgabe_mistakes WHERE id = ANY(%s);",
                        ([m for m, _ in fixed],))
            for mid, payload in cur.fetchall() or []:
                pr = G.stored_item_problems(payload if isinstance(payload, dict) else json.loads(payload))
                print(f"   перечитано #{mid}: {'чисто' if not pr else 'ВСЁ ЕЩЁ КРИВО: ' + str(pr)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
