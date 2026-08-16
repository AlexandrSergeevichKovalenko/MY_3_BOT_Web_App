# -*- coding: utf-8 -*-
"""Заголовок карточки отстал от исправленного слова — подтянуть.

Что случилось
─────────────
Владелец правит спорные фразы на экране разбора: судья предлагает исправление, человек
его принимает. Правка меняла ТОЛЬКО слово в справочнике, а личная карточка продолжала
показывать старый текст — и именно он виден крупно на повторении.

Владелец 16.08.2026 показал это на своей фразе:
    в карточке: «Daher vornehme ich Korrekturen selbst»   ← приставка оторвана не там
    в слове:    «Daher nehme ich Korrekturen selbst vor»   ← верно, исправлено 08.08
    в примере:  «Daher nehme ich Korrekturen selbst vor.»  ← верно

Замер: 837 карточек-фраз показывают заголовок, не совпадающий со словом. Из них 804 —
у слов с меткой «пересборка после правки», то есть ровно после принятых исправлений.

Правило
───────
Тянем ТОЛЬКО фразы (kind <> 'word'): у слова карточка может законно хранить свою форму
(«Die Strümpfe» при слове «Strumpf»), и трогать её нельзя.

Ограничения по источнику разбора НЕТ, и это осознанно: у ФРАЗЫ нет законной причины
расходиться со словом. Первый прогон 16.08.2026 брал только «пересборку после правки»
(804 карточки), после него осталось 52 — и они оказались такими же настоящими:
«Rate mall» против «Rate mal», обрезанная фраза против полной.

Прежние заголовки складываются в bt_3_card_headword_backup — вернуть можно.

    python3 scripts/dict_sync_card_headword_with_unit.py            # сухой прогон
    python3 scripts/dict_sync_card_headword_with_unit.py --apply    # записать
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context      # noqa: E402
import re                                                   # noqa: E402

_SPACE = re.compile(r"\s+")


def same_text(a: str, b: str) -> bool:
    """Одинаково ли это для ЧЕЛОВЕКА.

    Сравниваем по регистру и пробелам — и НЕ отбрасываем артикль. Первый прогон
    16.08.2026 пользовался normalize_query, а он снимает артикль, и «Türgriff aus
    Metall» считалось равным «der Türgriff aus Metall». Для человека это разные
    строки: артикль — половина существительного."""
    return _SPACE.sub(" ", str(a or "").strip()).casefold() == \
           _SPACE.sub(" ", str(b or "").strip()).casefold()

# ⚠ СТАРЫЙ ТЕКСТ ЛЕЖИТ В ЧЕТЫРЁХ МЕСТАХ, А НЕ В ОДНОМ. Первый прогон 16.08.2026 починил
# только word_de — и владелец тут же прислал скриншот, где заголовок по-прежнему старый:
# экран читает и translation_de, и поля внутри разбора (word_de, target_text).
# Поэтому берём карточку, если старый текст остался ХОТЬ ГДЕ-ТО.
PICK_SQL = """
    SELECT q.id, q.user_id, q.word_de, u.id, u.display,
           q.translation_de,
           q.response_json ->> 'word_de',
           q.response_json ->> 'target_text',
           q.response_json ->> 'source_text'
    FROM bt_3_webapp_dictionary_queries q
    JOIN bt_3_lex_units u ON u.id = q.lex_unit_id
    WHERE u.lang = 'de' AND u.kind <> 'word'
      AND (
            LOWER(BTRIM(COALESCE(q.word_de, ''))) <> LOWER(BTRIM(u.display))
         OR LOWER(BTRIM(COALESCE(q.translation_de, ''))) <> LOWER(BTRIM(u.display))
         OR LOWER(BTRIM(COALESCE(q.response_json ->> 'word_de', ''))) <> LOWER(BTRIM(u.display))
         OR LOWER(BTRIM(COALESCE(q.response_json ->> 'target_text', ''))) <> LOWER(BTRIM(u.display))
      )
    ORDER BY q.id;
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(PICK_SQL)
            rows = cur.fetchall()

    # Отличие только в регистре или пробелах — не правка, а шум.
    # Карточку берём, если старый текст остался хоть в одном из мест. Пустое поле —
    # не расхождение: его просто нет.
    def stale(row) -> bool:
        display = row[4]
        return any(
            value and not same_text(value, display)
            for value in (row[2], row[5], row[6], row[7])
        )

    targets = [r for r in rows if stale(r)]
    print("карточек-фраз с отставшим заголовком: %d" % len(targets))
    print("   (совпадают с точностью до регистра и пробелов: %d)"
          % (len(rows) - len(targets)))
    print()
    for entry_id, user_id, word, unit_id, display, *_rest in targets[:12]:
        print("   карточка %-7s человек %-11s" % (entry_id, user_id))
        print("      было:  %s" % str(word)[:70])
        print("      стало: %s" % str(display)[:70])

    if not args.apply:
        print("\nСУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")
        return

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_card_headword_backup (
                    id         BIGSERIAL PRIMARY KEY,
                    saved_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    entry_id   BIGINT NOT NULL,
                    word_de    TEXT,
                    reason     TEXT
                );
                """
            )
            done = 0
            for entry_id, _user_id, word, _unit_id, display, *_rest in targets:
                cur.execute(
                    "INSERT INTO bt_3_card_headword_backup (entry_id, word_de, reason) "
                    "VALUES (%s, %s, %s);",
                    (entry_id, word, "заголовок подтянут к исправленному слову, 16.08.2026"),
                )
                # Правим ВСЕ четыре места сразу: заголовок, вторую колонку и два поля
                # внутри разбора. Экран читает их вперемешку, и починка одного оставляет
                # старый текст на виду.
                cur.execute(
                    """
                    UPDATE bt_3_webapp_dictionary_queries
                       SET word_de = %(text)s,
                           translation_de = CASE WHEN translation_de IS NULL THEN NULL
                                                 ELSE %(text)s END,
                           response_json = jsonb_set(
                               jsonb_set(COALESCE(response_json, '{}'::jsonb),
                                         '{word_de}', to_jsonb(%(text)s::text), TRUE),
                               '{target_text}', to_jsonb(%(text)s::text), TRUE),
                           updated_at = NOW()
                     WHERE id = %(id)s;
                    """,
                    {"text": display, "id": entry_id},
                )
                done += 1
        conn.commit()
    print("\nзаголовков подтянуто: %d" % done)
    print("Прежние лежат в bt_3_card_headword_backup — вернуть можно.")


if __name__ == "__main__":
    main()
