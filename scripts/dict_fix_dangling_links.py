# -*- coding: utf-8 -*-
"""Висячие ссылки вокруг словаря: чиним накопленное и ставим правило в саму базу.

Замер 10.08.2026. На таблицу личных карточек в базе стоит РОВНО ОДИН внешний ключ —
у `bt_3_manual_training_selection`, и именно у неё ноль сирот. Всё остальное чистится
руками в коде: основной путь удаления слова чистит журнал ответов, а пути дедупликации
(`database.py:1319`, `:1488`) — нет. Отсюда накопленные хвосты.

То же с единицами: удалять их умеют десять разных скриптов, и ни один не поправляет
`lex_unit_id` у личных карточек. Так десять человек остались с указателем на единицу
`18048` («Lang-länger-am längsten»), которую справедливо убрали как грамматическую
цепочку, а не словарное слово.

ЧТО ДЕЛАЕМ

1. Три единицы, у которых разбор написан по-немецки, поэтому связь-перевод не создалась
   (страж «перевод обязан содержать русские буквы» сработал верно). Русский перевод есть
   в личной карточке — заводим из неё русскую единицу и связываем.
2. Обнуляем `lex_unit_id` у карточек, чей указатель ведёт в пустоту.
3. Убираем накопленные хвосты в четырёх личных таблицах.
4. Ставим внешние ключи, чтобы это не повторялось НИКОГДА:
   • `lex_unit_id` → единицы, ON DELETE SET NULL (скрипт удалил единицу — указатель обнулился);
   • четыре личные таблицы → карточки, ON DELETE CASCADE (удалили слово — хвосты ушли).
   Правило в базе, а не в коде: мимо него не пройдёт ни скрипт, ни ветка, ни новый агент.

ЧЕГО НЕ ДЕЛАЕМ. Удаление личной карточки не трогает общую единицу и не будет. Единица
общая: ту же фразу держат десять человек, и уход одного не повод стирать её у всех.

Запуск:
    DATABASE_URL=... python3 scripts/dict_fix_dangling_links.py --dry-run
    DATABASE_URL=... python3 scripts/dict_fix_dangling_links.py --apply
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

import psycopg2

SPACE_RE = re.compile(r"\s+")
ANY_ARTICLE_RE = re.compile(r"^(der|die|das|ein|eine|einen|einem|einer|eines)\s+", re.I)

# Единицы без перевода и личные карточки, из которых берём русскую сторону.
MISSING_TRANSLATIONS = [
    (31945, 11993, "разбор дал «Croutons», «Röstbrot» — это немецкий, а не перевод"),
    (38823, 10984, "разбор дал «das Ladegerät anschließen» — немецкий пересказ"),
    (38848, 11057, "разбор дал «faul herumliegen» — немецкий пересказ"),
]

# Личные таблицы, которые должны уходить вместе с карточкой.
PERSONAL_TABLES = [
    ("bt_3_card_review_log", "card_id", "журнал ответов"),
    ("bt_3_flashcard_feel_feedback_queue", "entry_id", "очередь «почувствовать слово»"),
    ("bt_3_flashcard_seen", "entry_id", "показанные карточки"),
    ("bt_3_flashcard_stats", "entry_id", "счётчики карточки"),
]


def normalize_query(text: str) -> str:
    compact = SPACE_RE.sub(" ", str(text or "").strip())
    return ANY_ARTICLE_RE.sub("", compact).strip().casefold()


def kind_for_text(text: str) -> str:
    body = ANY_ARTICLE_RE.sub("", str(text or "").strip()).strip()
    if not body:
        return ""
    if " " not in body:
        return "word"
    return "sentence" if len(body.split()) > 4 or body.rstrip().endswith((".", "!", "?")) else "collocation"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--backup", default=os.getenv("DANGLING_BACKUP") or "")
    args = parser.parse_args()
    apply = bool(args.apply) and not args.dry_run
    if apply and not args.backup:
        print("с --apply нужен --backup путь.json"); return 2

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("нет DATABASE_URL"); return 2
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    backup: dict = {}

    # ───────────────────────────────── 1. перевод из личной карточки
    print("1. ТРИ ЕДИНИЦЫ БЕЗ ПЕРЕВОДА — берём русскую сторону из личной карточки")
    for unit_id, card_id, why in MISSING_TRANSLATIONS:
        cur.execute("SELECT display, lang FROM bt_3_lex_units WHERE id=%s;", (unit_id,))
        found = cur.fetchone()
        cur.execute("SELECT word_ru FROM bt_3_webapp_dictionary_queries WHERE id=%s;", (card_id,))
        card = cur.fetchone()
        if not found or not card or not str(card[0] or "").strip():
            print(f"   ⚠ {unit_id}: нечего брать, пропускаю"); continue
        display, lang = found
        ru_text = SPACE_RE.sub(" ", str(card[0]).strip())
        cur.execute("SELECT 1 FROM bt_3_lex_links WHERE from_unit=%s LIMIT 1;", (unit_id,))
        if cur.fetchone():
            print(f"   {display[:50]}: перевод уже появился, пропускаю"); continue
        key, kind = normalize_query(ru_text), kind_for_text(ru_text)
        print(f"   {display[:52]}\n      почему нет перевода: {why}\n      беру из карточки: «{ru_text}» ({kind})")
        if not apply:
            continue
        cur.execute("""
            INSERT INTO bt_3_lex_units (lang, kind, lemma, lemma_key, display, card_source)
            VALUES ('ru', %s, %s, %s, %s, 'вычитка')
            ON CONFLICT (lang, kind, lemma_key, COALESCE(pos, ''), COALESCE(gender, ''))
            DO UPDATE SET updated_at = NOW() RETURNING id;""",
            (kind, ru_text, key, ru_text))
        ru_id = int(cur.fetchone()[0])
        cur.execute("""INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                       VALUES ('ru', %s, %s, 'exact') ON CONFLICT DO NOTHING;""", (key, ru_id))
        for a, b in ((unit_id, ru_id), (ru_id, unit_id)):
            cur.execute("""INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source)
                           VALUES (%s, %s, 20, 'вычитка')
                           ON CONFLICT (from_unit, to_unit) DO UPDATE
                             SET rank = LEAST(bt_3_lex_links.rank, 20);""", (a, b))
        print(f"      заведена русская единица {ru_id}, связь в обе стороны")

    # ───────────────────────────────── 2. указатели в пустоту
    print("\n2. КАРТОЧКИ, УКАЗЫВАЮЩИЕ НА УДАЛЁННУЮ ЕДИНИЦУ")
    cur.execute("""SELECT id, user_id, lex_unit_id, left(COALESCE(word_de,''),46)
                   FROM bt_3_webapp_dictionary_queries q WHERE q.lex_unit_id IS NOT NULL
                     AND NOT EXISTS (SELECT 1 FROM bt_3_lex_units u WHERE u.id=q.lex_unit_id)
                   ORDER BY id;""")
    dangling = cur.fetchall()
    for row in dangling:
        print(f"   карточка {row[0]} (человек {row[1]}) → единица {row[2]}: {row[3]}")
    print(f"   всего: {len(dangling)} — обнуляем указатель, содержимое карточки не трогаем")
    if apply and dangling:
        backup["dangling_lex_unit_id"] = [{"card": r[0], "lex_unit_id": r[2]} for r in dangling]
        cur.execute("""UPDATE bt_3_webapp_dictionary_queries q SET lex_unit_id = NULL
                       WHERE q.lex_unit_id IS NOT NULL
                         AND NOT EXISTS (SELECT 1 FROM bt_3_lex_units u WHERE u.id=q.lex_unit_id);""")
        print(f"   обнулено: {cur.rowcount}")

    # ───────────────────────────────── 3. хвосты личных таблиц
    print("\n3. ХВОСТЫ ОТ УДАЛЁННЫХ СЛОВ")
    for table, col, human in PERSONAL_TABLES:
        cur.execute(f"""SELECT COUNT(*) FROM {table} x WHERE x.{col} IS NOT NULL
                        AND NOT EXISTS (SELECT 1 FROM bt_3_webapp_dictionary_queries q WHERE q.id=x.{col});""")
        n = int(cur.fetchone()[0])
        print(f"   {human} ({table}): {n}")
        if apply and n:
            cur.execute(f"""SELECT to_jsonb(x) FROM {table} x WHERE x.{col} IS NOT NULL
                            AND NOT EXISTS (SELECT 1 FROM bt_3_webapp_dictionary_queries q WHERE q.id=x.{col});""")
            backup[table] = [r[0] for r in cur.fetchall()]
            cur.execute(f"""DELETE FROM {table} x WHERE x.{col} IS NOT NULL
                            AND NOT EXISTS (SELECT 1 FROM bt_3_webapp_dictionary_queries q WHERE q.id=x.{col});""")
            print(f"      убрано: {cur.rowcount}")

    # ───────────────────────────────── 4. правило в саму базу
    print("\n4. ВНЕШНИЕ КЛЮЧИ — чтобы это не повторилось")
    constraints = [
        ("fk_webapp_dictionary_lex_unit",
         "ALTER TABLE bt_3_webapp_dictionary_queries ADD CONSTRAINT fk_webapp_dictionary_lex_unit "
         "FOREIGN KEY (lex_unit_id) REFERENCES bt_3_lex_units(id) ON DELETE SET NULL;",
         "указатель на единицу обнуляется, когда единицу удаляют"),
    ] + [
        (f"fk_{table[5:]}_card",
         f"ALTER TABLE {table} ADD CONSTRAINT fk_{table[5:]}_card "
         f"FOREIGN KEY ({col}) REFERENCES bt_3_webapp_dictionary_queries(id) ON DELETE CASCADE;",
         f"{human} уходит вместе с карточкой")
        for table, col, human in PERSONAL_TABLES
    ]
    for name, sql, why in constraints:
        cur.execute("SELECT 1 FROM pg_constraint WHERE conname = %s;", (name,))
        if cur.fetchone():
            print(f"   {name}: уже стоит"); continue
        print(f"   {name}: {why}")
        if apply:
            cur.execute(sql)
            print("      поставлен")

    if apply:
        with open(args.backup, "w", encoding="utf-8") as fh:
            json.dump(backup, fh, ensure_ascii=False, indent=1, default=str)
        conn.commit()
        print(f"\nстарые значения: {args.backup}\nзаписано")
    else:
        conn.rollback()
        print("\nсухой прогон — база не менялась")
    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
