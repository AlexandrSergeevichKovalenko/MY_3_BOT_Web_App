# -*- coding: utf-8 -*-
"""Три мелких, но доказуемых беспорядка в общем пуле. Отчёт и починка.

Владелец 11.08.2026, увидев в проверке «translation_de = der Wassertropfe»:
«Зачем же нам тут с откушенным окончанием???» Незачем. Все три случая ниже
доказуемы без модели и без выбора значения.

  A. ОБРЕЗАННЫЙ ХВОСТ ВНУТРИ ЗАПИСИ. В одной записи word_de = «der Wassertropfen»,
     а translation_de = «der Wassertropfe». Это одно и то же поле в двух местах, и
     одно из них короче ровно на хвост. Правим по длинному: гадать не о чем.
     (Родня старого дефекта: spaCy откусывал окончание при сохранении.)

  B. ТОЧНЫЙ ДУБЛЬ БЕЗ СВЯЗЕЙ. Две записи с одинаковыми языками и одинаковым текстом
     с обеих сторон. Убираем ТУ, на которую никто не ссылается — ни единица слоя,
     ни чужая карточка повторения. Если ссылки есть у обеих, не трогаем: осиротить
     чужую карточку хуже, чем оставить дубль.

  C. АРТИКЛЬ С БОЛЬШОЙ БУКВЫ. «Die Versammlung» вместо «die Versammlung». Артикль
     верный, испорчен только регистр, и слово от этого выглядит началом предложения.

Запуск:
    DATABASE_URL=... python3 scripts/dict_pool_tidy.py
    DATABASE_URL=... python3 scripts/dict_pool_tidy.py --apply
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

import psycopg2

_TEXT_FIELDS = ("source_text", "target_text", "word_de", "word_ru",
                "translation_de", "translation_ru")
_CAPITAL_ARTICLE_RE = re.compile(r"^(Der|Die|Das)\s+")


def truncated_pairs(values: list[str]) -> tuple[str, list[str]] | None:
    """Одно значение — обрезанный вариант другого. Возвращает (целое, [обрезанные]).

    Обрезанным считаем только НАСТОЯЩИЙ префикс: «der Wassertropfe» короче
    «der Wassertropfen» ровно на хвост. Два разных текста префиксами друг друга не
    бывают, поэтому ложных срабатываний тут нет."""
    clean = [v for v in values if v]
    if len(set(clean)) < 2:
        return None
    longest = max(clean, key=len)
    cut = [v for v in set(clean) if v != longest and longest.startswith(v)]
    if not cut or len(set(clean)) != len(cut) + 1:
        return None
    return longest, cut


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="починить A и B")
    # Регистр артикля — отдельным ключом: это 1 384 записи и чистая косметика, такое
    # решение принимает владелец, а не скрипт заодно с остальным.
    parser.add_argument("--apply-articles", action="store_true", help="ещё и C (регистр артикля)")
    args = parser.parse_args()
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("Нужен DATABASE_URL", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(dsn, connect_timeout=25)
    conn.autocommit = False
    with conn.cursor() as cur:
        # ── A ── обрезанный хвост внутри записи
        cur.execute(
            "SELECT id, source_text, target_text, word_de, word_ru, "
            "translation_de, translation_ru, response_json FROM bt_3_dictionary_entries;"
        )
        rows = cur.fetchall()
        a_fixes = []
        for row in rows:
            entry_id, payload = row[0], row[7]
            values = {name: (row[i + 1] or "") for i, name in enumerate(_TEXT_FIELDS)}
            # Сравниваем ТОЛЬКО пары одного языка: word_de с translation_de, word_ru с
            # translation_ru. Поля source_text/target_text сюда не годятся — в них
            # немецкое и русское лежат по разные стороны в зависимости от направления
            # записи, и сравнение немецкого с русским объявляло бы обрезкой что попало.
            for group in (("word_de", "translation_de"),
                          ("word_ru", "translation_ru")):
                found = truncated_pairs([values[name] for name in group])
                if not found:
                    continue
                whole, cut = found
                a_fixes.append({"id": entry_id, "group": group, "whole": whole,
                                "cut": cut, "payload": payload,
                                "fields": [n for n in group if values[n] in cut]})
        print(f"A. Обрезанный хвост внутри записи: {len(a_fixes)}")
        for item in a_fixes[:15]:
            print(f"   [{item['id']}] {', '.join(item['fields'])} = {item['cut']}  →  {item['whole']!r}")

        # ── B ── точный дубль без связей
        cur.execute(
            """
            SELECT e.id, e.source_lang, e.target_lang, e.source_text, e.target_text,
                   (SELECT COUNT(*) FROM bt_3_lex_unit_sources s WHERE s.entry_id = e.id)
                 + (SELECT COUNT(*) FROM bt_3_flashcard_seen f WHERE f.entry_id = e.id) AS links
            FROM bt_3_dictionary_entries e
            WHERE (e.source_lang, e.target_lang, e.source_text, e.target_text) IN (
                SELECT source_lang, target_lang, source_text, target_text
                FROM bt_3_dictionary_entries
                GROUP BY 1, 2, 3, 4 HAVING COUNT(*) > 1
            )
            ORDER BY e.source_text, e.id;
            """
        )
        groups: dict[tuple, list] = {}
        for entry_id, s_lang, t_lang, src, tgt, links in cur.fetchall():
            groups.setdefault((s_lang, t_lang, src, tgt), []).append((entry_id, links))
        b_drop = []
        for key, members in groups.items():
            keep = max(members, key=lambda m: (m[1], -m[0]))   # больше связей, при равенстве старший
            for entry_id, links in members:
                if entry_id != keep[0] and links == 0:
                    b_drop.append({"id": entry_id, "keep": keep[0], "text": key[2]})
        print(f"\nB. Точных дублей без единой связи: {len(b_drop)}"
              f"   (всего групп-дублей: {len(groups)})")
        for item in b_drop[:15]:
            print(f"   [{item['id']}] убрать, остаётся [{item['keep']}]  «{str(item['text'])[:60]}»")

        # ── C ── артикль с большой буквы
        cur.execute(
            "SELECT id, word_de, target_text, translation_de, response_json "
            "FROM bt_3_dictionary_entries "
            # ТОЛЬКО заголовок «Артикль + одно слово». Предложение «Die Kosten waren
            # höher als erwartet.» начинается с большой буквы ЗАКОННО, и первый вариант
            # этой проверки собирался «починить» 2 450 таких строк, то есть сломать
            # правильный немецкий. Заголовок из двух слов — другое дело: там «Die» это
            # артикль, а не начало предложения.
            "WHERE word_de ~ '^(Der|Die|Das) [A-ZÄÖÜ][A-Za-zÄÖÜäöüß-]*$';"
        )
        c_rows = cur.fetchall()
        print(f"\nC. Артикль с большой буквы: {len(c_rows)}")
        for entry_id, word_de, *_rest in c_rows[:15]:
            print(f"   [{entry_id}] {word_de}  →  {_CAPITAL_ARTICLE_RE.sub(lambda m: m.group(1).lower() + ' ', word_de)}")

        if not args.apply and not args.apply_articles:
            conn.rollback()
            print("\nЭто отчёт, база не изменена. Применить: --apply")
            conn.close()
            return

        a_done = b_done = c_done = 0
        if args.apply:
          for item in a_fixes:
              data = dict(item["payload"]) if isinstance(item["payload"], dict) else {}
              sets, params = [], []
              for field in item["fields"]:
                  sets.append(f"{field} = %s")
                  params.append(item["whole"])
                  data[field] = item["whole"]
              params.append(json.dumps(data, ensure_ascii=False))
              params.append(item["id"])
              cur.execute(
                  f"UPDATE bt_3_dictionary_entries SET {', '.join(sets)}, "
                  f"response_json = %s, updated_at = NOW() WHERE id = %s;",
                  params,
              )
              a_done += 1
          if b_drop:
              cur.execute("DELETE FROM bt_3_dictionary_entries WHERE id = ANY(%s);",
                          ([item["id"] for item in b_drop],))
              b_done = len(b_drop)
        for entry_id, word_de, target_text, translation_de, payload in (c_rows if args.apply_articles else []):
            def fix(value):
                if not value:
                    return value
                return _CAPITAL_ARTICLE_RE.sub(lambda m: m.group(1).lower() + " ", str(value))
            data = dict(payload) if isinstance(payload, dict) else {}
            for field, value in (("word_de", word_de), ("target_text", target_text),
                                 ("translation_de", translation_de)):
                if data.get(field):
                    data[field] = fix(data[field])
            cur.execute(
                "UPDATE bt_3_dictionary_entries SET word_de = %s, target_text = %s, "
                "translation_de = %s, response_json = %s, updated_at = NOW() WHERE id = %s;",
                (fix(word_de), fix(target_text), fix(translation_de),
                 json.dumps(data, ensure_ascii=False), entry_id),
            )
            c_done += 1
        conn.commit()
        print(f"\n✓ Починено хвостов: {a_done}, убрано дублей: {b_done}, "
              f"поправлено артиклей: {c_done}")
    conn.close()


if __name__ == "__main__":
    main()
