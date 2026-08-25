# -*- coding: utf-8 -*-
"""Заглавная буква снимается там, где русский перевод доказал: это НЕ существительное.

Откуда работа (владелец, 24–25.08.2026)
───────────────────────────────────────
Карточка «die Mies» — «Паршивый» — с выдуманными значениями про неудачников.

Корень — заглавная буква. Человек ищет слово, и с клавиатуры оно приходит с заглавной:
это первое и единственное слово запроса. В немецком заглавная означает существительное,
поэтому прилагательное «reif» легло в базу как «Reif», справочник честно ответил про
существительное «der Reif» (иней), а модель сочинила под него смыслы и примеры.

ВТОРОЙ ГОЛОС — РУССКАЯ СТОРОНА. Её писал человек, и она не врёт: «Зрелый» —
прилагательное. Источник разбора: `backend/russian_part_of_speech.py` (pymorphy3, MIT,
офлайн, по одному слову). Решение владельца 25.08.2026.

ЧТО ЧИНИТСЯ И ПОЧЕМУ ИМЕННО ТАК
1. Написание в трёх хранилищах сразу: единица (lemma / lemma_key / display), запись
   общего пула, личная карточка человека. Починить одно из трёх значит оставить два
   несогласованных: экран собирается из разных мест.
2. РАЗБОР СТИРАЕТСЯ. Это требование владельца 25.08.2026, и оно важнее самой буквы:
   значения и примеры сочинялись под НЕВЕРНОЕ слово. «Die Mies sind wieder nicht
   erfolgreich» — выдумка, и она останется на экране, даже если написание починить.
   Пустой разбор подхватывает ночной добор (`lex_units.units_needing_card` берёт слова
   с `card IS NULL`) и собирает заново — уже для правильного слова.
3. Род и часть речи сбрасываются вместе с разбором: они были получены для
   существительного, которого тут нет.

СПИСОК ЗАКРЫТЫЙ И ПРОВЕРЕН ГЛАЗАМИ. Определитель нашёл 13 кандидатов, двое сюда НЕ
попали — там врёт не немецкая сторона, а русская:
    Frühstück   «Завтракать»  — «das Frühstück» это ЗАВТРАК, существительное верное,
                                неверен ПЕРЕВОД;
    Angeklagte  «Обвиняемый»  — «der/die Angeklagte» законное существительное
                                (субстантивированное причастие), как и по-русски.
Опустить у них заглавную значило бы сломать верное. Вынесены владельцу отдельно.

    python3 scripts/dict_lowercase_by_russian_pos.py          # показать
    python3 scripts/dict_lowercase_by_russian_pos.py --apply  # записать
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context          # noqa: E402
from backend.russian_part_of_speech import (                     # noqa: E402
    part_of_speech, proves_not_a_noun,
)

# Проверено определителем И глазами 25.08.2026. Расширять только тем же путём:
# сперва определитель, потом глаза — он говорит правду про РУССКОЕ слово, а вывод про
# немецкое делаем мы.
WORDS = (
    "Mies", "Reif", "Rein", "Stumpf", "Eigen", "Übel",
    "Manche", "Einer", "Unentschieden", "Fest", "Rudern",
)

# Немецкое слово + перевод, где врёт ПЕРЕВОД. Немецкое не трогаем.
RUSSIAN_IS_WRONG = {
    "Frühstück": "«Завтракать» — надо «завтрак», das Frühstück существительное",
    "Angeklagte": "«Обвиняемый» — der/die Angeklagte законное существительное",
}


def _russian_for(cur, word: str) -> str:
    cur.execute("""
        SELECT COALESCE(NULLIF(translation_ru,''), target_text)
        FROM bt_3_dictionary_entries
        WHERE COALESCE(NULLIF(word_de,''), source_text) = %s
        LIMIT 1
    """, (word,))
    row = cur.fetchone()
    return str((row or [""])[0] or "").split(",")[0].split(";")[0].strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="записать в базу")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        if not args.apply:
            conn.set_session(readonly=True)
        with conn.cursor() as cur:
            план = []
            for word in WORDS:
                russian = _russian_for(cur, word)
                вердикт = part_of_speech(russian)
                # Страховка: если русская сторона перестала доказывать «не
                # существительное» — слово из правки ВЫПАДАЕТ. Список закрытый, но данные
                # живые: перевод могли поправить, и тогда правка стала бы порчей.
                if not proves_not_a_noun(russian):
                    план.append((word, russian, вердикт, None, "ПРОПУСК: русская сторона больше не доказывает"))
                    continue
                cur.execute(
                    "SELECT id, card IS NOT NULL FROM bt_3_lex_units "
                    "WHERE lang='de' AND lemma_key=%s", (word.lower(),))
                unit = cur.fetchone()
                план.append((word, russian, вердикт, unit[0] if unit else None, ""))

            print(f"{'слово':<16}{'перевод':<22}{'вердикт':<18}{'единица':<10}примечание")
            for word, russian, вердикт, unit_id, note in план:
                print(f"  {word:<14}{russian[:20]:<22}{вердикт:<18}{str(unit_id or '—'):<10}{note}")

            годные = [p for p in план if not p[4]]
            print(f"\nК починке: {len(годные)} из {len(WORDS)}")
            print("НЕ ТРОГАЕМ (врёт русская сторона, немецкое верно):")
            for word, why in RUSSIAN_IS_WRONG.items():
                print(f"   {word:<14}{why}")

        if not args.apply:
            print("\nПрогон вхолостую. Записать: --apply")
            return 0

        итог = {"единиц": 0, "пул": 0, "личных": 0, "разборов стёрто": 0}
        with conn.cursor() as cur:
            for word, _russian, _вердикт, unit_id, note in план:
                if note:
                    continue
                low = word.lower()
                # 1. Единица: написание + СТИРАЕМ разбор, род и часть речи. Они получены
                #    для существительного, которого здесь нет.
                if unit_id:
                    cur.execute("""
                        UPDATE bt_3_lex_units
                           SET display = %s, lemma = %s, lemma_key = %s,
                               card = NULL, card_source = NULL,
                               gender = NULL, gender_source = NULL,
                               pos = NULL, pos_source = NULL,
                               updated_at = NOW()
                         WHERE id = %s
                    """, (low, low, low, unit_id))
                    итог["единиц"] += cur.rowcount
                    итог["разборов стёрто"] += cur.rowcount
                # 2. Общий пул.
                cur.execute("""
                    UPDATE bt_3_dictionary_entries
                       SET word_de = %s,
                           source_text = CASE WHEN source_text = %s THEN %s ELSE source_text END,
                           target_text = CASE WHEN target_text = %s THEN %s ELSE target_text END,
                           updated_at = NOW()
                     WHERE COALESCE(NULLIF(word_de,''), source_text) = %s
                """, (low, word, low, word, low, word))
                итог["пул"] += cur.rowcount
                # 3. Личные карточки людей — и с артиклем, и без: «die Mies» тоже неверно.
                #
                #    ⚠ ВТОРОЕ ХРАНИЛИЩЕ ВНУТРИ ПЕРВОГО. Мало починить колонку word_de:
                #    выдуманный разбор лежит ЕЩЁ И в `response_json` той же строки, и
                #    заголовок на экране собирается ИЗ НЕГО (compose_german_headword берёт
                #    response_json.word_de и response_json.article). Проверка экраном
                #    25.08.2026: колонка стала «mies», а человек по-прежнему видел
                #    «die Mies» — потому что внутри лежало «die Mies» / article='die' /
                #    part_of_speech='noun'.
                #
                #    Эти три ключа СНИМАЮТСЯ, а не правятся: они описывали
                #    существительное, которого здесь нет. Остальной разбор карточки
                #    остаётся человеку — его смыслы и заметки не наши.
                cur.execute("""
                    UPDATE bt_3_webapp_dictionary_queries
                       SET word_de = %s,
                           response_json = COALESCE(response_json, '{}'::jsonb)
                                           - 'word_de' - 'article' - 'part_of_speech',
                           updated_at = NOW()
                     WHERE word_de IN (%s, %s, %s, %s)
                        OR lower(COALESCE(response_json->>'word_de','')) IN (%s, %s)
                """, (low, word, "der " + word, "die " + word, "das " + word,
                      low, ("die " + word).lower()))
                итог["личных"] += cur.rowcount
        conn.commit()
        print("\nЗаписано:", ", ".join(f"{k} {v}" for k, v in итог.items()))
        print("Разбор стёрт — ночной добор соберёт его заново уже для верного слова.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
