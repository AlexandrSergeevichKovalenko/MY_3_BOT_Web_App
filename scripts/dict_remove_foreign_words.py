# -*- coding: utf-8 -*-
"""Убрать из НЕМЕЦКОГО словаря слова, которые немецкими не являются.

Решение владельца 23.08.2026, дословно: «убирай эти три слова, в немецком словаре им не
место». Речь про «slay» (английский), «bore» (английский), «aspettiamo» (итальянский).

КАК ОНИ ТУДА ПОПАЛИ. Дверь словаря спрашивала справочник и читала части речи со ВСЕЙ
страницы Wiktionary. У «slay» немецкого раздела нет — есть английский, и дверь отвечала
«подтверждено, глагол», то есть заводила английское слово как немецкое. Дыра закрыта в
тот же день (backend/german_word_gate.py: подтверждает только немецкий раздел), здесь —
уборка накопленного.

СПИСОК ИМЕННОЙ, И ЭТО НАРОЧНО. Правилом «нет немецкого раздела — удалить» пользоваться
нельзя: справочник неполон, и под него попали бы настоящие немецкие слова, которых в
Wiktionary просто нет («Arbeitsumfeld», «Beantragung» — проверено 21.08.2026). Решение
владельца 19.08.2026 остаётся в силе: чужое слово не отклоняется само, а показывается
человеку. Удаляет только человек, поимённо.

ЧТО УБИРАЕТСЯ И ЧТО ОСТАЁТСЯ
    слово словаря      → удаляется, полный снимок в bt_3_lex_units_removed;
    двери поиска       → уходят вместе со словом;
    запись общего пула → удаляется (это кеш поиска, его чинить нечем);
    личная карточка    → удаляется ТОЛЬКО если она лежит в немецком словаре (de→ru).
                         Карточка на другом языке остаётся: «aspettiamo» человек сохранял
                         как ru→it, и к немецкому словарю она отношения не имеет. Ошибкой
                         было то, что из неё завели НЕМЕЦКОЕ слово, а не сама карточка.

    python3 scripts/dict_remove_foreign_words.py            # сухой прогон
    python3 scripts/dict_remove_foreign_words.py --apply    # убрать
"""
from __future__ import annotations

import argparse
import json
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context  # noqa: E402

# Поимённо, по решению владельца 23.08.2026. Правилом не вычисляется — см. шапку.
FOREIGN_WORDS = ["slay", "bore", "aspettiamo"]
REASON = "чужое слово в немецком словаре, решение владельца 23.08.2026"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, display, kind, pos FROM bt_3_lex_units
                    WHERE lang = 'de' AND lower(display) = ANY(%s);""",
                (FOREIGN_WORDS,),
            )
            units = cur.fetchall()
            cur.execute(
                """SELECT id, user_id, word_de, word_ru, source_lang, target_lang
                     FROM bt_3_webapp_dictionary_queries
                    WHERE lower(word_de) = ANY(%s) AND source_lang = 'de';""",
                (FOREIGN_WORDS,),
            )
            cards = cur.fetchall()
            cur.execute(
                """SELECT id, source_text, target_text FROM bt_3_dictionary_entries
                    WHERE source_lang = 'de' AND lower(source_text) = ANY(%s);""",
                (FOREIGN_WORDS,),
            )
            pool = cur.fetchall()

            print(f"\nслова немецкого словаря ({len(units)}):")
            for uid, display, kind, pos in units:
                print(f"      {uid:>6} {display!r} kind={kind} pos={pos!r}")
            print(f"\nличные карточки В НЕМЕЦКОМ словаре ({len(cards)}):")
            for cid, user, de, ru, sl, tl in cards:
                print(f"      {cid:>7} человек {user}: {de!r} → {ru!r} ({sl}→{tl})")
            print(f"\nзаписи общего пула ({len(pool)}):")
            for eid, src, tgt in pool:
                print(f"      {eid:>7} {src!r} → {tgt!r}")

            cur.execute(
                """SELECT id, word_de, word_ru, source_lang, target_lang
                     FROM bt_3_webapp_dictionary_queries
                    WHERE (lower(word_de) = ANY(%s) OR lower(word_ru) = ANY(%s))
                      AND source_lang <> 'de';""",
                (FOREIGN_WORDS, FOREIGN_WORDS),
            )
            kept = cur.fetchall()
            if kept:
                print("\nОСТАЁТСЯ (карточка не в немецком словаре, трогать нечего):")
                for cid, de, ru, sl, tl in kept:
                    print(f"      {cid:>7} {de!r} / {ru!r} ({sl}→{tl})")

            if not args.apply:
                print("\nСУХОЙ ПРОГОН. Ничего не удалено. Убрать: --apply\n")
                return 0

            unit_ids = [int(u[0]) for u in units]
            saved = 0
            if unit_ids:
                # Полный снимок ДО удаления: слово, его написания, связи и значения.
                # Без следа не удаляем ничего — правило появилось после ночной чистки
                # дублей, которая два месяца стирала карточки, не записывая что именно.
                cur.execute(
                    """
                    INSERT INTO bt_3_lex_units_removed (
                        reason, unit_id, lang, kind, lemma, lemma_key, pos, gender,
                        display, card, surfaces, links, senses)
                    SELECT %s, u.id, u.lang, u.kind, u.lemma, u.lemma_key, u.pos,
                           u.gender, u.display, u.card,
                           COALESCE((SELECT jsonb_agg(to_jsonb(s)) FROM bt_3_lex_surfaces s
                                      WHERE s.unit_id = u.id), '[]'::jsonb),
                           COALESCE((SELECT jsonb_agg(to_jsonb(l)) FROM bt_3_lex_links l
                                      WHERE l.from_unit = u.id OR l.to_unit = u.id), '[]'::jsonb),
                           COALESCE((SELECT jsonb_agg(to_jsonb(x)) FROM bt_3_lex_senses x
                                      WHERE x.unit_id = u.id), '[]'::jsonb)
                      FROM bt_3_lex_units u WHERE u.id = ANY(%s);
                    """,
                    (REASON, unit_ids),
                )
                saved = cur.rowcount

            # Личная карточка — тоже со снимком, в ту же кладовку: unit_id пустой,
            # содержимое строки целиком лежит в `card`. Это данные ЧЕЛОВЕКА, и вернуть
            # их должно быть можно так же, как слово словаря.
            for cid, user, de, ru, sl, tl in cards:
                cur.execute("SELECT to_jsonb(q) FROM bt_3_webapp_dictionary_queries q "
                            "WHERE q.id = %s;", (cid,))
                row = cur.fetchone()
                cur.execute(
                    """INSERT INTO bt_3_lex_units_removed
                              (reason, unit_id, lang, kind, display, card)
                       VALUES (%s, NULL, 'de', 'личная карточка', %s, %s::jsonb);""",
                    (REASON, de, json.dumps(row[0] if row else {}, ensure_ascii=False,
                                            default=str)),
                )
                cur.execute("DELETE FROM bt_3_webapp_dictionary_queries WHERE id = %s;", (cid,))

            if pool:
                cur.execute("DELETE FROM bt_3_dictionary_entries WHERE id = ANY(%s);",
                            ([int(p[0]) for p in pool],))
            if unit_ids:
                # Написания уходят по внешнему ключу вместе со словом; личные карточки
                # не страдают — fk_webapp_dictionary_lex_unit стоит на SET NULL.
                cur.execute("DELETE FROM bt_3_lex_units WHERE id = ANY(%s);", (unit_ids,))

            # Вердикт двери снимаем: слова больше нет, и старый ответ о нём — мусор.
            cur.execute("DELETE FROM bt_3_word_check WHERE lower(asked) = ANY(%s);",
                        (FOREIGN_WORDS,))
        conn.commit()

    print(f"\nснимков сохранено: {saved + len(cards)}, слов убрано: {len(units)}, "
          f"личных карточек: {len(cards)}, записей пула: {len(pool)}")
    print("Вернуть можно: всё лежит в bt_3_lex_units_removed.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
