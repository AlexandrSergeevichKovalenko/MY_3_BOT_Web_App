# -*- coding: utf-8 -*-
"""Склеенные переводы («Шарик, пуля, пушечное ядро») уходят вниз, к остальной свалке.

ОТКУДА ЗАДАЧА
─────────────
02.09.2026, разбирая задвоенные записи, владелец сказал: «давай что нужно починить,
давай починим». Разбор нашёл ДВА разных дефекта, и оба видны человеку.

1. ВЫДАЧА БРАЛА ШЕСТЬ СВЯЗЕЙ, А НЕ ШЕСТЬ РАЗНЫХ ПЕРЕВОДОВ. Одно русское слово живёт в
   слое дважды (замер: 2574 написания заведены двумя единицами — у одной часть речи
   проставлена, у другой нет), обе связаны с немецким словом, и повторы съедали места
   в шестёрке. Проверено на экране:
       «die Kugel»  — 11 разных переводов в базе, на экране 3;
       «der Mangel» — 32 разных, на экране 3;
       «ahnen»      — 22 разных, на экране 4.
   Починено в `dictionary_entries._from_units`: берём с запасом, режем ПОСЛЕ схлопывания.

2. СКЛЕЙКИ СТОЯТ НАРАВНЕ С ПЕРЕВОДАМИ и занимают первое место. Это и чинит скрипт.

ЧТО СЧИТАЕТСЯ СКЛЕЙКОЙ — И ПОЧЕМУ ЭТО ДОКАЗУЕМО, А НЕ «НА ГЛАЗ»
───────────────────────────────────────────────────────────────
Строка считается склейкой, только если она распадается по запятой или «;» на части,
и ВСЕ ЭТИ ЧАСТИ лежат у ТОГО ЖЕ немецкого слова ОТДЕЛЬНЫМИ переводами. Тогда понижение
не прячет ни одного значения: всё, что было в строке, человек и так видит по отдельности.

    «die Kugel» → «Шарик, пуля, пушечное ядро»   рядом есть «шарик» и «пуля» → склейка
    «die Zielrichtung» → «направление, к которому стремятся»  рядом только «направление»
                                                              → НЕ склейка, не трогаем

Замер 02.09.2026 по живой базе: видимых переводов с запятой или «;» — 3009; из них
1734 разбираются ПОЛНОСТЬЮ (их и понижаем), а 1275 несут хотя бы одну часть, которой
рядом нет, — эти остаются на экране нетронутыми. Это тот же приём, которым 11.08.2026 понижали
свалки с номерами («1 ободрять 2 вдохновлять»), и он там уже проверен: 450 понижённых
связей, ни одна не доходит до экрана.

НИЧЕГО НЕ УДАЛЯЕТСЯ. Связь остаётся в базе, у неё меняется только ранг на
`_DEMOTED_RANK` (900) — выдача такие не показывает (`_LINK_PICK_WHERE`). Если часть
смысла жила ТОЛЬКО в склейке, она никуда не делась и её видно в базе.

    python3 scripts/dict_demote_glued_translations.py            # что будет понижено
    python3 scripts/dict_demote_glued_translations.py --apply    # понизить
"""
from __future__ import annotations

import argparse
import os
import re
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context  # noqa: E402
from backend.lex_units import _DEMOTED_RANK             # noqa: E402

РАЗДЕЛИТЕЛЬ = re.compile(r"\s*[;,]\s*")
# ⛔ ПОНИЖАЕМ, ТОЛЬКО ЕСЛИ РЯДОМ ЛЕЖАТ ВСЕ ЧАСТИ. Порог «две и больше» я поставил
# первым и сам на нём поймался: у «die Erbarmung» строка «сострадание, жалость,
# милосердие» распадается на три части, а рядом отдельно лежат только две —
# «милосердие» живёт ТОЛЬКО внутри склейки. Понизив её, я спрятал бы значение,
# которого больше нигде нет. Это ровно тот случай, о котором предупреждает разбор
# от 11.08.2026: из 450 понижённых свалок 148 были единственным носителем смысла.


def найти_склейки(cur) -> tuple[list[tuple], list[tuple]]:
    """Связи-склейки, которые ещё видны человеку (ранг ниже свалки)."""
    cur.execute(
        f"""SELECT u.id, u.display, l.id, l.rank, v.display
              FROM bt_3_lex_units u
              JOIN bt_3_lex_links l ON l.from_unit = u.id OR l.to_unit = u.id
              JOIN bt_3_lex_units v
                ON v.id = CASE WHEN l.from_unit = u.id THEN l.to_unit ELSE l.from_unit END
             WHERE u.lang = 'de' AND v.lang = 'ru' AND l.rank < {_DEMOTED_RANK}
               AND (v.display LIKE '%,%' OR v.display LIKE '%;%');"""
    )
    подозрительные = cur.fetchall()

    cur.execute(
        """SELECT u.id, lower(v.display)
             FROM bt_3_lex_units u
             JOIN bt_3_lex_links l ON l.from_unit = u.id OR l.to_unit = u.id
             JOIN bt_3_lex_units v
               ON v.id = CASE WHEN l.from_unit = u.id THEN l.to_unit ELSE l.from_unit END
            WHERE u.lang = 'de' AND v.lang = 'ru'
              AND v.display NOT LIKE '%,%' AND v.display NOT LIKE '%;%';"""
    )
    одиночные: dict[int, set[str]] = {}
    for uid, слово in cur.fetchall():
        одиночные.setdefault(int(uid), set()).add(слово)

    склейки: list[tuple] = []
    неполные: list[tuple] = []   # часть смысла живёт только внутри строки — не трогаем
    for uid, слово_de, link_id, ранг, строка in подозрительные:
        части = [ч.strip().lower() for ч in РАЗДЕЛИТЕЛЬ.split(строка) if ч.strip()]
        если_рядом = одиночные.get(int(uid), set())
        совпало = sum(1 for ч in части if ч in если_рядом)
        if len(части) >= 2 and совпало == len(части):
            склейки.append((int(link_id), слово_de, строка, совпало, len(части)))
        elif len(части) >= 2 and совпало >= 1:
            неполные.append((слово_de, строка, совпало, len(части)))
    return склейки, неполные


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--list", type=int, default=15)
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        cur = conn.cursor()
        склейки, неполные = найти_склейки(cur)
        print("склеек, где рядом лежат ВСЕ части (понижаем): %d" % len(склейки))
        print("склеек, где часть смысла есть ТОЛЬКО внутри строки (НЕ трогаем): %d"
              % len(неполные))
        for link_id, слово, строка, совпало, всего in склейки[:args.list]:
            print("   %-24s «%s» — %d частей из %d лежат рядом отдельно"
                  % (слово[:23], строка[:44], совпало, всего))
        if неполные:
            print("\n   примеры того, что НЕ трогаем:")
            for слово, строка, совпало, всего in неполные[:5]:
                print("      %-22s «%s» — рядом %d из %d" % (слово[:21], строка[:42], совпало, всего))
        if not args.apply:
            print("\n(показан план; понизить — с флагом --apply)")
            return 0
        cur.execute(
            "UPDATE bt_3_lex_links SET rank = %s, updated_at = now() WHERE id = ANY(%s);",
            (_DEMOTED_RANK, [c[0] for c in склейки]),
        )
        понижено = cur.rowcount
        conn.commit()
        print("\nпонижено связей: %d (из базы ничего не удалено)" % понижено)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
