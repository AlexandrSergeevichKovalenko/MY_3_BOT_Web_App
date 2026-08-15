# -*- coding: utf-8 -*-
"""Заголовок слова во множественном числе → словарная форма.

Зачем
─────
Карточка должна строиться от словарной формы: «die Formel», а не «Formeln». Иначе
таблица форм собирается от множественного, а человек учит слово в том виде, в каком
его в словаре нет.

Замер 15.08.2026: существительных, чей заголовок совпадает с формой множественного из
их же разбора — 12. Из них четыре НЕ дефект и в список не входят:
    das Abkommen, Polster, das Meeresungeheuer — единственное совпадает с множественным
    Kosten — слово существует только во множественном.

⚠ ПОЧЕМУ ЗДЕСЬ ЯВНАЯ ТАБЛИЦА, А НЕ ПРАВИЛО. Обрезать окончание и спросить внешний
словарь — недостаточно: для «Abkommen» такая проверка «подтвердила» единственное
«Abkomme», а это другое слово («потомок»). Каждая пара ниже выверена глазами.

Что делает скрипт
─────────────────
1. Если словарная форма УЖЕ есть отдельным словом — сливает: разбор переносится, если
   он полнее, связи, написания и карточки людей переезжают, дубликат удаляется.
   Механика взята из проверенного scripts/dict_units_merge_wrong_language.py.
2. Если словарной формы нет — переименовывает. Оба написания остаются дверью для
   поиска: и множественное (человек мог набрать его), и новое единственное — без него
   слово перестаёт находиться по собственному заголовку (наступали 15.08.2026).
3. Русскую сторону правит ТОЛЬКО там, где русская единица связана с одним немецким
   словом. Если она общая — трогать нельзя, переименование заденет чужие карточки.

    python3 scripts/dict_fix_plural_headwords.py            # сухой прогон
    python3 scripts/dict_fix_plural_headwords.py --apply    # записать
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context      # noqa: E402
from backend import lex_units as LU                         # noqa: E402

# слово → словарная форма. Каждая пара выверена глазами 15.08.2026.
GERMAN = {
    6541:  ("Handelsentscheidungen", "Handelsentscheidung"),
    14152: ("Narren", "Narr"),
    33738: ("Tonnen", "Tonne"),
    38504: ("Probleme", "Problem"),
    38552: ("Hintergrundgeräusche", "Hintergrundgeräusch"),
    41275: ("Formeln", "Formel"),
    41440: ("Elektrogeräte", "Elektrogerät"),
    44944: ("Sternschnuppen", "Sternschnuppe"),
}

# Русская сторона. Правим только единицы, связанные с ОДНИМ немецким словом.
RUSSIAN = {
    41441: ("Электроприборы", "электроприбор"),
    41635: ("фоновые шумы", "фоновый шум"),
    # Второй проход 15.08.2026, после того как немецкая сторона стала словарной:
    # у «Narr» и «Tonne» перевод так и остался во множественном.
    # Заодно снимаются знаки ударения — в словарной статье им не место.
    14153: ("шу́тники", "шутник"),
    14154: ("дураки́", "дурак"),
    39231: ("тонны", "тонна"),
    39232: ("контейнеры, баки", "контейнер, бак"),
    6542:  ("торговые решения", "торговое решение"),
    44097: ("помехи в записи", "помеха в записи"),
}


def german_words_linked_to(cur, unit_id: int) -> int:
    cur.execute(
        """
        SELECT count(DISTINCT CASE WHEN l.from_unit = %s THEN l.to_unit ELSE l.from_unit END)
        FROM bt_3_lex_links l
        JOIN bt_3_lex_units g
          ON g.id = CASE WHEN l.from_unit = %s THEN l.to_unit ELSE l.from_unit END
        WHERE (l.from_unit = %s OR l.to_unit = %s) AND g.lang = 'de';
        """,
        (unit_id, unit_id, unit_id, unit_id),
    )
    return int(cur.fetchone()[0] or 0)


def merge_into(cur, *, dead: int, alive: int, lang: str) -> None:
    """Перенести всё с дубликата на выжившее слово и убрать дубликат."""
    cur.execute("SELECT card FROM bt_3_lex_units WHERE id = %s;", (dead,))
    row = cur.fetchone()
    card = row[0] if row and isinstance(row[0], dict) else None
    if card:
        LU.save_unit_card_if_richer(alive, card, source="слияние формы множественного", cursor=cur)
    cur.execute(
        """UPDATE bt_3_lex_links l SET from_unit = %(alive)s
           WHERE l.from_unit = %(dead)s AND l.to_unit <> %(alive)s
             AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links k
                             WHERE k.from_unit = %(alive)s AND k.to_unit = l.to_unit);""",
        {"alive": alive, "dead": dead},
    )
    cur.execute(
        """UPDATE bt_3_lex_links l SET to_unit = %(alive)s
           WHERE l.to_unit = %(dead)s AND l.from_unit <> %(alive)s
             AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links k
                             WHERE k.from_unit = l.from_unit AND k.to_unit = %(alive)s);""",
        {"alive": alive, "dead": dead},
    )
    cur.execute(
        """INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
           SELECT %s, s.surface_key, %s, s.match_kind
           FROM bt_3_lex_surfaces s WHERE s.unit_id = %s
           ON CONFLICT DO NOTHING;""",
        (lang, alive, dead),
    )
    cur.execute("UPDATE bt_3_webapp_dictionary_queries SET lex_unit_id = %s WHERE lex_unit_id = %s;",
                (alive, dead))
    cur.execute("DELETE FROM bt_3_lex_links WHERE from_unit = %s OR to_unit = %s;", (dead, dead))
    cur.execute("DELETE FROM bt_3_lex_surfaces WHERE unit_id = %s;", (dead,))
    cur.execute("DELETE FROM bt_3_lex_units WHERE id = %s;", (dead,))


def rename_to(cur, *, unit_id: int, old: str, new: str, lang: str) -> None:
    cur.execute(
        "UPDATE bt_3_lex_units SET lemma = %s, lemma_key = %s, display = %s, updated_at = NOW() "
        "WHERE id = %s;",
        (new, LU.normalize_query(new), new, unit_id),
    )
    for spelling, kind in ((old, "inflected"), (new, "exact")):
        cur.execute(
            "INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;",
            (lang, LU.normalize_query(spelling), unit_id, kind),
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    merged = renamed = ru_renamed = skipped = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            print("НЕМЕЦКАЯ СТОРОНА")
            for unit_id, (plural, singular) in GERMAN.items():
                cur.execute("SELECT display FROM bt_3_lex_units WHERE id = %s;", (unit_id,))
                row = cur.fetchone()
                if not row:
                    print("   %-8s %-24s слова уже нет" % (unit_id, plural)); continue
                cur.execute(
                    "SELECT id, display FROM bt_3_lex_units "
                    "WHERE lang='de' AND lemma_key = %s AND id <> %s ORDER BY id LIMIT 1;",
                    (LU.normalize_query(singular), unit_id),
                )
                twin = cur.fetchone()
                if twin:
                    print("   %-8s %-24s → слить с %s (%s)" % (unit_id, plural, twin[1], twin[0]))
                    if args.apply:
                        merge_into(cur, dead=unit_id, alive=int(twin[0]), lang="de")
                    merged += 1
                else:
                    print("   %-8s %-24s → переименовать в %s" % (unit_id, plural, singular))
                    if args.apply:
                        rename_to(cur, unit_id=unit_id, old=plural, new=singular, lang="de")
                    renamed += 1

            print()
            print("РУССКАЯ СТОРОНА (только у связанных с одним немецким словом)")
            for unit_id, (plural, singular) in RUSSIAN.items():
                # Единицы может не быть: её могли слить раньше в этом же прогоне, а
                # номер — оказаться устаревшим. Молча падать на этом нельзя, иначе
                # вся транзакция откатывается и не проходит НИЧЕГО (наступали 15.08.2026).
                cur.execute("SELECT display FROM bt_3_lex_units WHERE id = %s;", (unit_id,))
                if not cur.fetchone():
                    print("   %-8s %-24s ПРОПУСК: такой единицы нет" % (unit_id, plural))
                    skipped += 1
                    continue
                linked = german_words_linked_to(cur, unit_id)
                if linked > 1:
                    print("   %-8s %-24s ПРОПУСК: связана с %d немецкими словами"
                          % (unit_id, plural, linked))
                    skipped += 1
                    continue
                # Словарная форма может уже существовать отдельной единицей («дурак»
                # при «дураки́»). Переименование упрётся в уникальность — сливаем.
                cur.execute(
                    "SELECT id, display FROM bt_3_lex_units "
                    "WHERE lang='ru' AND lemma_key = %s AND id <> %s ORDER BY id LIMIT 1;",
                    (LU.normalize_query(singular), unit_id),
                )
                twin = cur.fetchone()
                if twin:
                    print("   %-8s %-24s → слить с %s (%s)" % (unit_id, plural, twin[1], twin[0]))
                    if args.apply:
                        merge_into(cur, dead=unit_id, alive=int(twin[0]), lang="ru")
                else:
                    print("   %-8s %-24s → %s" % (unit_id, plural, singular))
                    if args.apply:
                        rename_to(cur, unit_id=unit_id, old=plural, new=singular, lang="ru")
                ru_renamed += 1

            if args.apply:
                conn.commit()

    print()
    print("слито: %d, переименовано немецких: %d, русских: %d, пропущено: %d"
          % (merged, renamed, ru_renamed, skipped))
    if not args.apply:
        print("СУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")


if __name__ == "__main__":
    main()
