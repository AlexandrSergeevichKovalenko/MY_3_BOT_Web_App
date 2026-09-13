# -*- coding: utf-8 -*-
"""Уборка банка анаграмм: снять несуществующее слово и починить регистр глаголов.

Повод и разбор — `backend/anagram_word_gate.py` и `scripts/anagram_bank_liveness_audit.py`.
Коротко: 13.09.2026 владельцу пришла анаграмма `Inkelgasse` («Закоулок») — слова нет,
это «Winkelgasse» без первой буквы, 0 вхождений на миллиард. Замер всего банка нашёл
рядом второй класс: 16 карточек несут глагол, записанный с большой буквы.

Решения владельца 13.09.2026
────────────────────────────
1. `Inkelgasse` — снять карточку и починить запись журнала, откуда её взяли.
2. Регистр — «fix register and keep it»: слово остаётся, написание чинится.
3. Накопленные РЕДКИЕ карточки не трогать (как у ребуса 01.09.2026). Здесь их нет:
   скрипт правит только регистр и снимает одно несуществующее слово.

Правило регистра НЕ переписано своими словами: оно берётся импортом из той двери,
через которую теперь проходит пополнение (`anagram_word_gate.spelling_by_source`).
Поэтому уборка не может разойтись с приёмкой.

    python3 scripts/anagram_bank_fix_case_and_dead_word.py            # показать, что будет
    python3 scripts/anagram_bank_fix_case_and_dead_word.py --apply    # применить
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend.anagram_word_gate import spelling_by_source                # noqa: E402
from backend.database import get_db_connection_context                  # noqa: E402

# Слово, которого нет в немецком, и его источник в журнале запросов. Оба номера сверены
# на живой базе 13.09.2026: карточка создана 21.08, ушла людям 22.08, 04.09 и 13.09;
# запись журнала #4301 — это поиск владельца «Закоулок» от 12.02.2026, ответ на который
# («die Inkelgasse») и есть корень. Правильное написание берётся НЕ из головы, а из
# статьи `bt_3_lex_units` #26429, которую владелец починил сам 02.09.2026.
МЁРТВОЕ_СЛОВО = "Inkelgasse"
ЗАПИСЬ_ЖУРНАЛА = 4301
СТАТЬЯ = 26429


def _spelling_by_all_records(cur, word: str) -> tuple[str, str]:
    """(написание, причина отказа) по ВСЕМ записям журнала об этом слове.

    Одной записи мало: у «Erschießen» их две — `das Erschießen` (расстрел, noun) и
    `das Erschießen` (расстрелять, verb). Если брать первую попавшуюся, решение о
    регистре зависит от порядка строк в таблице, то есть от случайности. Поэтому
    правило такое: пишем строчными, только если ВСЕ записи согласны, что это глагол,
    прилагательное или наречие, и ни в одной нет артикля. Записи спорят — не трогаем.
    """
    cur.execute(
        """SELECT response_json FROM bt_3_webapp_dictionary_queries
           WHERE LOWER(REGEXP_REPLACE(COALESCE(response_json->>'word_de',''),
                                      '^(der|die|das)\\s+','')) = LOWER(%s)
             AND response_json->>'part_of_speech' IS NOT NULL""",
        (word,),
    )
    варианты = set()
    for (payload,) in cur.fetchall() or []:
        if not isinstance(payload, dict):
            continue
        написание, _why = spelling_by_source(word, {"response_json": payload})
        if написание:
            варианты.add(написание)
    if not варианты:
        return "", "источник молчит о части речи"
    if len(варианты) > 1:
        return "", "записи журнала спорят о части речи"
    return варианты.pop(), ""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="применить, а не показать")
    args = ap.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            # ── 1. Несуществующее слово ──────────────────────────────────────────────
            cur.execute("SELECT display FROM bt_3_lex_units WHERE id = %s", (СТАТЬЯ,))
            row = cur.fetchone()
            if not row or not str(row[0] or "").strip():
                raise SystemExit(f"статья {СТАТЬЯ} не найдена — без источника правку не делаем")
            правильное = str(row[0]).strip()
            print(f"1. «{МЁРТВОЕ_СЛОВО}» → снять карточку; в журнале написать «die {правильное}» "
                  f"(источник: статья {СТАТЬЯ}, починена владельцем 02.09.2026)")
            if args.apply:
                cur.execute("UPDATE bt_3_anagram_cards SET retired = TRUE "
                            "WHERE LOWER(word) = LOWER(%s) AND NOT retired RETURNING card_id",
                            (МЁРТВОЕ_СЛОВО,))
                снято = len(cur.fetchall() or [])
                cur.execute(
                    """UPDATE bt_3_webapp_dictionary_queries
                       SET response_json = response_json
                             || jsonb_build_object('word_de', %s, 'translation_de', %s),
                           word_de = %s, translation_de = %s, updated_at = NOW()
                       WHERE id = %s""",
                    (f"die {правильное}", f"die {правильное}",
                     f"die {правильное}", f"die {правильное}", ЗАПИСЬ_ЖУРНАЛА),
                )
                print(f"   снято карточек: {снято}; запись журнала {ЗАПИСЬ_ЖУРНАЛА} починена")

            # ── 2. Регистр ───────────────────────────────────────────────────────────
            cur.execute("SELECT card_id, word, scrambled, hint_ru, retired "
                        "FROM bt_3_anagram_cards ORDER BY word")
            карточки = cur.fetchall()
            чинить, без_источника = [], []
            for card_id, word, scrambled, hint, retired in карточки:
                верное, почему = _spelling_by_all_records(cur, word)
                if not верное:
                    без_источника.append((word, почему))
                    continue
                if верное == word:
                    continue
                if верное.lower() != word.lower():
                    # Дверь вернула другое СЛОВО, а не другой регистр. Такого быть не
                    # должно; молча подменять написание здесь нельзя.
                    без_источника.append((word, "дверь вернула другое слово"))
                    continue
                чинить.append((card_id, word, верное, scrambled, hint, retired))

            print(f"\n2. Регистр: чинить {len(чинить)} карточек, "
                  f"не трогаем {len(set(без_источника))}:")
            for word, почему in sorted(set(без_источника)):
                print(f"   — {word:<20} {почему}")
            for _cid, word, верное, scrambled, hint, retired in чинить:
                print(f"   {word:<20} → {верное:<20} {hint[:18]:<18} "
                      f"{'снято' if retired else 'ВЫДАЁТСЯ'}")

            if args.apply:
                for card_id, word, верное, scrambled, hint, retired in чинить:
                    # Перемешанную строку не пересобираем: первая и последняя буквы в ней
                    # стоят на своих местах (`_scramble_word_preserve_ends`), поэтому у
                    # слова и у задания меняется ровно одна и та же первая буква. Пересборка
                    # выдала бы человеку другое перемешивание без всякой нужды.
                    новое_перемешанное = верное[:1] + scrambled[1:]
                    if sorted(новое_перемешанное.lower()) != sorted(верное.lower()):
                        print(f"   ПРОПУЩЕНО {word}: буквы задания не сходятся со словом")
                        continue
                    cur.execute("UPDATE bt_3_anagram_cards SET word = %s, scrambled = %s "
                                "WHERE card_id = %s", (верное, новое_перемешанное, card_id))
                print(f"   починено: {len(чинить)}")

    print("\nЭКРАН ПОСЛЕ — то же, что увидит владелец:")
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT count(*) FILTER (WHERE NOT retired),
                                  count(*) FILTER (WHERE NOT retired AND word ~ '^[A-ZÄÖÜ]'),
                                  count(*) FILTER (WHERE LOWER(word) = 'inkelgasse' AND NOT retired)
                           FROM bt_3_anagram_cards""")
            выдаются, с_заглавной, мёртвых = cur.fetchone()
            print(f"   выдаются людям: {выдаются}; из них с заглавной буквы: {с_заглавной}; "
                  f"живых карточек «Inkelgasse»: {мёртвых}")


if __name__ == "__main__":
    main()
