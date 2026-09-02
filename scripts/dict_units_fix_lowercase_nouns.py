# -*- coding: utf-8 -*-
"""Двадцать записей, где немецкое существительное лежало со строчной буквы.

ОТКУДА ЗАДАЧА
─────────────
01.09.2026 чинили «рыется». По дороге выяснилось: запрос «ging» вторым вариантом
показывает «das gehen — идти». Владелец 02.09.2026: «Ну разбери. Ты сначала
проанализируй хорошо, о чём ты говоришь».

Разбор (все числа перепроверены двумя разными признаками и увидены на экране):

  • записей — 20; все видны человеку в виде «die aufenthaltsgenehmigung»,
    «der bierhausschwätzer», «das gehen». В немецком существительное со строчной —
    ошибка ВСЕГДА, а это языковое приложение;
  • 19 из 20 подтверждены как существительные источником (FreeDict + наш определитель
    рода): пометка «существительное» верна, сломано другое;
  • это НЕ один класс, а четыре, и лечатся они по-разному.

ЧЕТЫРЕ КЛАССА И РЕШЕНИЕ ВЛАДЕЛЬЦА (02.09.2026, по пунктам)
──────────────────────────────────────────────────────────
  1. ТОЛЬКО РЕГИСТР (8). Настоящее существительное с верным переводом, написано со
     строчной. → Поднять букву. Род у каждого подтверждён источником.

  2. ПОД СУЩЕСТВИТЕЛЬНЫМ ЛЕЖИТ ГЛАГОЛ (4): gehen, schaffen, schwimmen, altwerden.
     Переводы глагольные («идти», «справляться», «плавать», «стареть»), а запись
     помечена существительным и печатается с артиклем. Поднять букву тут МАЛО и даже
     вредно: «das Gehen» — это «ходьба», а не «идти».
     → Владелец: «как глаголы сделай».

  3. В ОДНОЙ ЗАПИСИ ДВА РАЗНЫХ СЛОВА (5): rasen, rechen, vermögen, geschehen, vier.
     «der Rasen» (газон) и «rasen» (мчаться) — два законных прочтения одного
     написания. → Владелец: «разделяй на две записи, как делают словари». Так и
     устроены PONS, dict.cc, Duden — и так у нас уже работают «der Kiefer» (челюсть)
     и «die Kiefer» (сосна).

  4. ОБРУБОК (1): «die inkelgasse» — это «Winkelgasse» (переулок) без первой буквы.
     → Владелец: «чини».

ЧТО ЗДЕСЬ НЕ ДЕЛАЕТСЯ И ПОЧЕМУ (найдено по дороге, вынесено владельцу отдельно)
──────────────────────────────────────────────────────────────────────────────
  • В записи «rasen» лежат переводы, не подтверждённые источником ни для одного из
    прочтений: «остановка», «перерыв», «отдыхает», «быстрая езда». Похоже на путаницу
    с «Rast» (привал). Они пришли из GPT-разбора (source='разбор'). Ничего не
    удаляем молча — это отдельное решение владельца.
  • Связи в слое задвоены почти везде (две строки на один и тот же перевод). Это
    касается всего слоя, а не этих двадцати, и в эту правку не входит.

ИСТОЧНИК ДЛЯ КАЖДОГО РЕШЕНИЯ НАЗВАН В ТАБЛИЦЕ НИЖЕ. Ничего не выдумано: перевод
второй записи берётся из bt_base_dictionary (FreeDict), род — из нашего определителя
`german_surface`. Где источник молчит (Gehen, Schaffen, Altwerden как
существительные) — вторая запись НЕ создаётся, случай считается пробелом.

    python3 scripts/dict_units_fix_lowercase_nouns.py            # что будет сделано
    python3 scripts/dict_units_fix_lowercase_nouns.py --apply    # сделать
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context  # noqa: E402
from backend.lex_units import normalize_query           # noqa: E402

РЕШЕНИЕ = "владелец 02.09.2026"

# ── КЛАСС 1: поднять регистр ─────────────────────────────────────────────────
# id → как должно писаться. Род у каждого уже стоит и подтверждён определителем.
РЕГИСТР = {
    23305: "Aufenthaltsgenehmigung",
    2112: "Bierhausschwätzer",
    26695: "Eichstrich",
    25573: "Einfältigkeit",
    16042: "Eroberung",          # лежало как «eROBERUNG» — капс из текста, не из языка
    26739: "Pflanze",
    28698: "Zufriedenstellung",
    23514: "Zwischenbescheid",
    26429: "Winkelgasse",        # КЛАСС 4: обрубок «inkelgasse» → полное слово
}

# ── КЛАСС 2: запись становится глаголом ──────────────────────────────────────
# id → написание глагола. Артикль снимается, род обнуляется.
В_ГЛАГОЛ = {
    15708: "gehen",
    13548: "schaffen",
    41427: "schwimmen",
    24100: "altwerden",
}

# ── КЛАСС 3: разделить на две записи ─────────────────────────────────────────
# Существующая запись остаётся тем прочтением, которое поддержано её же
# указателями-словоформами (у всех пяти это спрягаемые формы), а ВТОРАЯ запись
# заводится с переводом ИЗ ИСТОЧНИКА.
РАЗДЕЛИТЬ = [
    # id, кем становится старая запись, новая запись: (написание, род, переводы, источник),
    # какие переводы перенести со старой на новую (точным совпадением строки)
    (12992, "verb", ("Rasen", "der", ["газон"], "FreeDict: Rasen(noun)=газон"), ["газон"]),
    (9784, "verb", ("Rechen", "der", ["грабли"], "FreeDict: Rechen(noun)=грабли"), ["грабли"]),
    (13178, "verb", ("Vermögen", "das", ["имущество", "состояние"],
                     "FreeDict: Vermögen(noun)=умение, состояние"),
     ["имущество", "Имущество, состояние", "состояние, имущество"]),
    (24397, "verb", ("Geschehen", "das", ["событие"], "FreeDict: Geschehen(noun)=событие"),
     ["случай"]),
    (41221, "numeral", ("Vier", "die", ["четвёрка"], "FreeDict: Vier(noun)=четыре, четвёрка"),
     []),
]

# ── КЛАСС 3-бис: служебные слова, помеченные существительными ────────────────
# «aber» — союз, «wenn» — союз. Существительные «das Aber» / «das Wenn» в языке есть,
# но НИ ОДИН источник не дал им перевода, поэтому второй записи не создаём: это
# посчитанный пробел, а не повод выдумать значение.
СЛУЖЕБНЫЕ = {20984: "conjunction", 20972: "conjunction"}

# ── ГЛАВНЫЙ ПЕРЕВОД НАЗНАЧАЕТ ИСТОЧНИК, А НЕ СЛУЧАЙНЫЙ ПОРЯДОК ────────────────
# После разделения на экране вылезло: «rasen (глагол) — отдыхает», «geschehen —
# Происходить; происходить, случаться». Ранги у всех связей были одинаковые (10), и
# первым вставало что попало — в том числе мусор из GPT-разбора.
# Ставим первым тот перевод, который даёт FreeDict. Это не наше мнение: источник
# назван, и он же лежит рядом в базе.
ГЛАВНЫЙ_ПЕРЕВОД = {
    15708: ("идти", "FreeDict: gehen(verb)=идти"),
    13548: ("создать", "FreeDict: schaffen(verb)=создать"),
    41427: ("плавать", "FreeDict: schwimmen(verb)=плавать"),
    12992: ("мчаться", "FreeDict: rasen(verb)=мчаться"),
    13178: ("мочь", "FreeDict: vermögen(verb)=мочь"),
    24397: ("случаться", "FreeDict: geschehen(verb)=случаться"),
    41221: ("четыре", "FreeDict: vier(numeral)=четыре"),
    9784: ("сгребать", "наш словарь: единственное глагольное значение записи"),
}

# ── ПОМЕТКА СЛОВАРЯ, ПОПАВШАЯ В ПЕРЕВОД ──────────────────────────────────────
# «das Gehen — герундий от gehen : "идти"». Это не перевод, а строка из справочного
# аппарата FreeDict, приехавшая в поле переводов при импорте. Замер 02.09.2026: во
# всём базовом словаре таких записей ДВЕ, и вторая («Superlativ — превосходная
# степень») — настоящий перевод. То есть это единичный случай, а не класс.
# Убираем строку. Перевода у «das Gehen» после этого НЕТ — и мы его не выдумываем:
# существительное просто не покажется, пока источник не даст значение. Это честный
# пробел, посчитанный ниже.
ПОМЕТКА_НЕ_ПЕРЕВОД = {"Gehen": 'герундий от gehen : "идти"'}


def _ru_unit(cur, слово: str) -> int:
    """Найти или завести русскую единицу. Ничего не переводим — слово дал источник."""
    ключ = normalize_query(слово)
    cur.execute("SELECT id FROM bt_3_lex_units WHERE lang='ru' AND lemma_key=%s LIMIT 1", (ключ,))
    строка = cur.fetchone()
    if строка:
        return int(строка[0])
    cur.execute(
        """INSERT INTO bt_3_lex_units (lang, kind, lemma, lemma_key, display)
           VALUES ('ru', 'word', %s, %s, %s) RETURNING id;""",
        (слово, ключ, слово),
    )
    return int(cur.fetchone()[0])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    сделано = {"регистр": 0, "в_глагол": 0, "разделено": 0, "перенесено_связей": 0,
               "служебные": 0, "новых_записей": 0, "главный_перевод": 0, "снято_пометок": 0}

    with get_db_connection_context() as conn:
        cur = conn.cursor()

        # ── КЛАСС 1 и 4 ──────────────────────────────────────────────────────
        for uid, написание in РЕГИСТР.items():
            cur.execute("SELECT display, gender FROM bt_3_lex_units WHERE id=%s", (uid,))
            строка = cur.fetchone()
            if not строка:
                print("  !! запись %s исчезла" % uid)
                continue
            было, род = строка
            print("  регистр: %-6s %-26s → %s" % (uid, repr(было), repr(написание)))
            if args.apply:
                cur.execute(
                    """UPDATE bt_3_lex_units
                          SET display=%s, lemma=%s, lemma_key=%s, updated_at=now()
                        WHERE id=%s;""",
                    (написание, написание, normalize_query(написание), uid),
                )
                cur.execute(
                    """INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                       VALUES ('de', %s, %s, 'exact')
                       ON CONFLICT (lang, surface_key, unit_id) DO NOTHING;""",
                    (normalize_query(написание), uid),
                )
            сделано["регистр"] += 1

        # ── КЛАСС 2 ──────────────────────────────────────────────────────────
        for uid, написание in В_ГЛАГОЛ.items():
            cur.execute("SELECT display, pos, gender FROM bt_3_lex_units WHERE id=%s", (uid,))
            было = cur.fetchone()
            print("  в глагол: %-6s %-24s (%s, %s) → %s (verb, без артикля)"
                  % (uid, repr(было[0]), было[1], было[2], repr(написание)))
            if args.apply:
                cur.execute(
                    """UPDATE bt_3_lex_units
                          SET display=%s, lemma=%s, lemma_key=%s, pos='verb', pos_source=%s,
                              gender=NULL, gender_source=NULL, updated_at=now()
                        WHERE id=%s;""",
                    (написание, написание, normalize_query(написание), РЕШЕНИЕ, uid),
                )
                # Указатель с артиклем («das gehen») больше не нужен: это не существительное.
                cur.execute(
                    """DELETE FROM bt_3_lex_surfaces
                        WHERE unit_id=%s AND surface_key ~ '^(der|die|das) ';""", (uid,))
            сделано["в_глагол"] += 1

        # ── КЛАСС 3 ──────────────────────────────────────────────────────────
        for uid, старая_роль, (написание, род, переводы, источник), перенести in РАЗДЕЛИТЬ:
            cur.execute("SELECT display FROM bt_3_lex_units WHERE id=%s", (uid,))
            было = cur.fetchone()[0]
            print("  разделить: %-6s %-16s → «%s» (%s) + новая запись «%s %s» — %s"
                  % (uid, repr(было), написание.lower(), старая_роль, род, написание,
                     ", ".join(переводы)))
            print("             источник второй записи: %s" % источник)
            if перенести:
                print("             переезжают переводы: %s" % ", ".join(перенести))
            сделано["разделено"] += 1
            if not args.apply:
                continue

            # Старая запись становится тем прочтением, которое поддержано её же
            # указателями-словоформами: у всех пяти это спрягаемые формы.
            cur.execute(
                """UPDATE bt_3_lex_units
                      SET display=%s, lemma=%s, lemma_key=%s, pos=%s, pos_source=%s,
                          gender=NULL, gender_source=NULL, updated_at=now()
                    WHERE id=%s;""",
                (написание.lower(), написание.lower(), normalize_query(написание),
                 старая_роль, РЕШЕНИЕ, uid),
            )
            cur.execute(
                """DELETE FROM bt_3_lex_surfaces
                    WHERE unit_id=%s AND surface_key ~ '^(der|die|das) ';""", (uid,))

            # ПЕРЕЕЗД ПЕРЕВОДОВ — СНЯТЬ И ПОСТАВИТЬ ЗАНОВО, А НЕ ПЕРЕАДРЕСОВАТЬ.
            # В слое связи задвоены (две строки на один и тот же перевод), а пара
            # «слово ↔ перевод» уникальна. Переадресация двух одинаковых связей упала бы
            # на уникальном индексе, и половина правки осталась бы применённой.
            # Перевод при этом не теряется: он переезжает к тому слову, которому
            # принадлежит, и это видно в отчёте.
            ru_ids: dict[int, str] = {}
            for слово in перенести:
                cur.execute(
                    """SELECT t.id, t.display FROM bt_3_lex_links l
                         JOIN bt_3_lex_units t
                           ON t.id = CASE WHEN l.from_unit=%s THEN l.to_unit ELSE l.from_unit END
                        WHERE (l.from_unit=%s OR l.to_unit=%s)
                          AND t.lang='ru' AND t.display=%s;""",
                    (uid, uid, uid, слово),
                )
                for ru_id, ru_disp in cur.fetchall():
                    ru_ids[int(ru_id)] = ru_disp
            if ru_ids:
                cur.execute(
                    """DELETE FROM bt_3_lex_links
                        WHERE (from_unit=%s AND to_unit = ANY(%s))
                           OR (to_unit=%s AND from_unit = ANY(%s));""",
                    (uid, list(ru_ids), uid, list(ru_ids)),
                )
                сделано["перенесено_связей"] += cur.rowcount

            # ПОВТОРНЫЙ ПРОГОН НЕ ДОЛЖЕН ПАДАТЬ. Единица опознаётся по
            # (язык, вид, ключ, часть речи, род) — это её уникальность в базе. Если
            # вторая запись уже заведена прошлым прогоном, берём её, а не создаём
            # заново: иначе скрипт валится на уникальном индексе и половина правки
            # остаётся неприменённой (поймано на живом прогоне 02.09.2026).
            cur.execute(
                """SELECT id FROM bt_3_lex_units
                    WHERE lang='de' AND kind='word' AND lemma_key=%s
                      AND pos='noun' AND gender=%s LIMIT 1;""",
                (normalize_query(написание), род),
            )
            уже = cur.fetchone()
            if уже:
                новый = int(уже[0])
                print("             вторая запись уже есть (id=%s) — беру её" % новый)
            else:
                cur.execute(
                    """INSERT INTO bt_3_lex_units
                           (lang, kind, lemma, lemma_key, pos, pos_source, gender, gender_source, display)
                       VALUES ('de','word',%s,%s,'noun',%s,%s,%s,%s) RETURNING id;""",
                    (написание, normalize_query(написание), источник, род, источник, написание),
                )
                новый = int(cur.fetchone()[0])
                сделано["новых_записей"] += 1
            cur.execute(
                """INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                   VALUES ('de', %s, %s, 'exact')
                   ON CONFLICT (lang, surface_key, unit_id) DO NOTHING;""",
                (normalize_query(написание), новый),
            )

            ранг = 0
            поставлено: set[int] = set()
            for слово in переводы:          # сначала переводы ИЗ ИСТОЧНИКА
                ru = _ru_unit(cur, слово)
                if ru in поставлено:
                    continue
                поставлено.add(ru)
                ранг += 10
                cur.execute(
                    """INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source)
                       VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;""",
                    (новый, ru, ранг, источник),
                )
            for ru in ru_ids:               # потом переехавшие со смешанной записи
                if ru in поставлено:
                    continue
                поставлено.add(ru)
                ранг += 10
                cur.execute(
                    """INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source)
                       VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;""",
                    (новый, ru, ранг, "переехало со смешанной записи 02.09.2026"),
                )

        # ── КЛАСС 3-бис ──────────────────────────────────────────────────────
        for uid, роль in СЛУЖЕБНЫЕ.items():
            cur.execute("SELECT display, pos FROM bt_3_lex_units WHERE id=%s", (uid,))
            было = cur.fetchone()
            print("  служебное: %-6s %-12s (%s) → %s" % (uid, repr(было[0]), было[1], роль))
            if args.apply:
                cur.execute(
                    """UPDATE bt_3_lex_units
                          SET pos=%s, pos_source=%s, gender=NULL, gender_source=NULL,
                              updated_at=now()
                        WHERE id=%s;""",
                    (роль, РЕШЕНИЕ, uid),
                )
                cur.execute(
                    """DELETE FROM bt_3_lex_surfaces
                        WHERE unit_id=%s AND surface_key ~ '^(der|die|das) ';""", (uid,))
            сделано["служебные"] += 1

        # ── ГЛАВНЫЙ ПЕРЕВОД ──────────────────────────────────────────────────
        for uid, (слово, источник) in ГЛАВНЫЙ_ПЕРЕВОД.items():
            print("  главный перевод: %-6s → «%s» (%s)" % (uid, слово, источник))
            if not args.apply:
                continue
            ru = _ru_unit(cur, слово)
            cur.execute(
                """UPDATE bt_3_lex_links SET rank=1, source=%s, updated_at=now()
                    WHERE (from_unit=%s AND to_unit=%s) OR (from_unit=%s AND to_unit=%s);""",
                (источник, uid, ru, ru, uid),
            )
            if cur.rowcount == 0:
                cur.execute(
                    """INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source)
                       VALUES (%s, %s, 1, %s) ON CONFLICT DO NOTHING;""",
                    (uid, ru, источник),
                )
            сделано["главный_перевод"] += 1

        # ── ПОМЕТКА, ПРИНЯТАЯ ЗА ПЕРЕВОД ─────────────────────────────────────
        for лемма, строка in ПОМЕТКА_НЕ_ПЕРЕВОД.items():
            print("  не перевод: %s → снимаем «%s»" % (лемма, строка))
            if not args.apply:
                continue
            cur.execute(
                """UPDATE bt_base_dictionary
                      SET translations_ru = array_remove(translations_ru, %s)
                    WHERE source_lang='de' AND lemma=%s;""",
                (строка, лемма),
            )
            сделано["снято_пометок"] += cur.rowcount

        if args.apply:
            conn.commit()

    print()
    for ключ, число in сделано.items():
        print("  %-20s %d" % (ключ, число))
    if not args.apply:
        print("\n(показан план; чтобы сделать — с флагом --apply)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
