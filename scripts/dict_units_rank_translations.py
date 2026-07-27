# -*- coding: utf-8 -*-
"""Порядок переводов у слова: какой показывать первым.

Пока перевод был один, порядок никого не волновал. Теперь он виден в двух местах сразу:
в словаре первым идёт главное значение, а в тренировке оно же показывается КРУПНО как
ответ на карточке. Значит «как легло при сборке» больше не годится.

Чем ранжируем — от надёжного к слабому:
  1. Разбор карточки прямо называет главный смысл (meanings.primary) — это лучший
     сигнал, он есть у 804 слов.
  2. Перевод стоит в списке разбора — тем выше, чем раньше в списке.
  3. Сколько РАЗНЫХ людей сохранили себе именно эту пару. Живой сигнал: 17 840 личных
     карточек совпадают с конкретным переводом. Он слабый, пока людей мало, но растёт
     сам и со временем становится главным.
  4. Длинное описательное значение («этнографическая выставка, часто с колониальным
     подтекстом») первым быть не должно, даже если других сигналов нет.

Скрипт меняет ТОЛЬКО поле порядка у связей. Ни переводы, ни разборы, ни личные карточки
не трогаются.

Запуск:
    DATABASE_URL=... python3 scripts/dict_units_rank_translations.py --dry-run
    DATABASE_URL=... python3 scripts/dict_units_rank_translations.py --apply
"""
from __future__ import annotations

import argparse
import collections
import os
import re
import time

import psycopg2
import psycopg2.extras

SPACE_RE = re.compile(r"\s+")
DEMOTED_RANK = 900
LONG_VALUE = 45          # длиннее — это уже описание, а не перевод
RANK_PRIMARY = 5         # главный смысл из разбора
RANK_IN_LIST = 10        # + позиция в списке переводов разбора
RANK_CROWD = 20          # − сколько людей сохранили (не ниже 15, нужно ≥2 человек)
RANK_DEFAULT = 100
LONG_PENALTY = 40


def connect(dsn: str):
    last = None
    for attempt in range(6):
        try:
            return psycopg2.connect(dsn, connect_timeout=20)
        except Exception as exc:
            last = exc
            print("  переподключение %d/6: %s" % (attempt + 1, exc))
            time.sleep(5)
    raise SystemExit("база недоступна: %s" % last)


def norm(text: str) -> str:
    return SPACE_RE.sub(" ", str(text or "").strip()).casefold()


def card_signals(card: dict) -> tuple[str, dict[str, int]]:
    """→ (главный смысл, {перевод: позиция в списке}) из разбора карточки."""
    if not isinstance(card, dict):
        return "", {}
    primary = ""
    meanings = card.get("meanings")
    if isinstance(meanings, dict) and isinstance(meanings.get("primary"), dict):
        primary = norm(meanings["primary"].get("value"))
    positions: dict[str, int] = {}
    translations = card.get("translations")
    if isinstance(translations, list):
        for index, item in enumerate(translations):
            value = item.get("value") if isinstance(item, dict) else item
            key = norm(value)
            if key and key not in positions:
                positions[key] = index
    if isinstance(meanings, dict) and isinstance(meanings.get("secondary"), list):
        for index, item in enumerate(meanings["secondary"]):
            if isinstance(item, dict):
                key = norm(item.get("value"))
                if key and key not in positions:
                    positions[key] = index + 1
    return primary, positions


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if not args.dry_run and not args.apply:
        raise SystemExit("укажи --dry-run или --apply")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("нужен DATABASE_URL")

    conn = connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    # Краудсигнал: сколько РАЗНЫХ людей сохранили себе эту пару «слово ↔ перевод».
    cur.execute(
        """
        SELECT q.lex_unit_id, lower(btrim(q.word_ru)), COUNT(DISTINCT q.user_id)
        FROM bt_3_webapp_dictionary_queries q
        WHERE q.lex_unit_id IS NOT NULL AND COALESCE(q.word_ru, '') <> ''
        GROUP BY 1, 2;
        """
    )
    crowd = {(r[0], r[1]): r[2] for r in cur.fetchall()}
    print("пар «слово ↔ перевод» с личными сохранениями: %d" % len(crowd))

    cur.execute(
        """
        SELECT de.id, de.display, de.card, ru.id, ru.display, l.rank
        FROM bt_3_lex_links l
        JOIN bt_3_lex_units de ON de.id = l.from_unit AND de.lang = 'de' AND de.kind = 'word'
        JOIN bt_3_lex_units ru ON ru.id = l.to_unit AND ru.lang <> 'de'
        WHERE l.rank < %s
        ORDER BY de.id, l.rank;
        """,
        (DEMOTED_RANK,),
    )
    rows = cur.fetchall()
    by_unit: dict[int, list] = collections.defaultdict(list)
    cards: dict[int, dict] = {}
    names: dict[int, str] = {}
    for de_id, de_display, card, ru_id, ru_display, rank in rows:
        by_unit[de_id].append((ru_id, ru_display, rank))
        cards[de_id] = card if isinstance(card, dict) else {}
        names[de_id] = de_display
    print("слов с переводами: %d, из них с несколькими: %d"
          % (len(by_unit), sum(1 for v in by_unit.values() if len(v) > 1)))

    updates: list[tuple] = []
    changed_units = 0
    samples: list[str] = []
    for de_id, items in by_unit.items():
        primary, positions = card_signals(cards.get(de_id) or {})
        scored = []
        for ru_id, ru_display, rank in items:
            key = norm(ru_display)
            score = RANK_DEFAULT
            reason = "прочее"
            if primary and key == primary:
                score, reason = RANK_PRIMARY, "главный смысл разбора"
            elif key in positions:
                score, reason = RANK_IN_LIST + positions[key], "список разбора"
            saves = crowd.get((de_id, key), 0)
            # Одно сохранение — это ещё не сигнал, а случайность: людей пока мало.
            # Порог в двух РАЗНЫХ людей отсекает шум и не даёт одному человеку
            # переставить значения словаря.
            if saves >= 2:
                crowd_score = max(15, RANK_CROWD - min(saves, 5))
                if crowd_score < score:
                    score, reason = crowd_score, "сохранили %d чел." % saves
            if len(ru_display) > LONG_VALUE:
                score += LONG_PENALTY
                reason += " + длинное"
            scored.append((score, ru_id, ru_display, rank, reason))
        # При равных сигналах сохраняем НЫНЕШНИЙ порядок, а не сортируем по алфавиту:
        # у разрезанных свалок это нумерация значений из самого словаря
        # («1. прикладывать, 2. накладывать, 3. надевать»), и терять её нельзя.
        scored.sort(key=lambda x: (x[0], x[3], x[1]))
        for position, (score, ru_id, ru_display, old_rank, reason) in enumerate(scored):
            new_rank = 10 + position
            if new_rank != old_rank:
                updates.append((new_rank, de_id, ru_id))
        if len(items) > 1 and any(u[1] == de_id for u in updates):
            changed_units += 1
            if len(samples) < 8:
                lines = ["  %s:" % names[de_id]]
                for position, (score, _ru_id, ru_display, _old, reason) in enumerate(scored[:4]):
                    lines.append("      %d. %-32s ← %s" % (position + 1, ru_display[:32], reason))
                samples.append("\n".join(lines))

    print("\nсвязей с новым порядком: %d, слов затронуто: %d" % (len(updates), changed_units))
    print()
    for sample in samples:
        print(sample)

    if not args.apply:
        conn.rollback()
        print("\n(--dry-run: в базу ничего не записано)")
        return 0

    psycopg2.extras.execute_values(
        cur,
        "UPDATE bt_3_lex_links AS l SET rank = v.rank, updated_at = NOW() "
        "FROM (VALUES %s) AS v(rank, from_unit, to_unit) "
        "WHERE l.from_unit = v.from_unit AND l.to_unit = v.to_unit",
        updates, page_size=1000,
    )
    conn.commit()
    print("\nзаписано: порядок переводов обновлён у %d связей." % len(updates))
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
