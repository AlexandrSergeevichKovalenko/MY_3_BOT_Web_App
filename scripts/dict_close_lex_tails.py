# -*- coding: utf-8 -*-
"""Хвосты вычитки слоя единиц: карточка 12004, пустые значения, дубликаты, страж.

ТОЛЬКО НАЗВАННЫЕ СТРОКИ. Никаких эвристик: каждая правка перечислена поимённо и
обоснована в комментарии. По умолчанию сухой прогон.

1. КАРТОЧКА 12004. Разбор целиком про «das Tablett — поднос» (главное значение,
   оба примера), а колонки говорят «Таблет (основной перевод, предмет)» / «das Tablet»
   (планшет, одна «t»). Врут колонки, а не разбор: приводим их к разбору.

2. ПУСТЫЕ ЗНАЧЕНИЯ. Восемь строк без пояснения И без перевода — ни у значения, ни у
   единицы. В них нет данных, удаляем. Остальные шесть трогать нельзя: у них
   осмысленное русское пояснение, просто у самой единицы ещё нет перевода — это
   работа ночного добора, а не вычитки.

3. ДУБЛИКАТЫ ЕДИНИЦ. «Beschaffung» и «Kenntnisnahme» лежат дважды: строка без рода
   (а у Kenntnisnahme ещё и с частью речи «прилагательное») и правильная строка с
   «die». Ключ опознания включает род и часть речи, поэтому пустые поля завели вторую
   единицу. Переносим на правильную всё, что держится за дубликат — указатели, связи,
   происхождение, ссылки личных карточек, — и удаляем его.
   Значения дубликата НЕ переносим: у правильной единицы свои, те же самые. Связи
   переезжают, их привязка к удалённому значению обнулится сама (ON DELETE SET NULL).

4. СТРАЖ РАЗВЕДЕНИЯ. `sync_unit_links_from_card` (lex_units.py:643) отбрасывает
   значение, если справочник `bt_3_lex_gloss_rulings` говорит, что оно принадлежит
   другому роду. Значения, которые мы только что вычистили из разборов, в справочнике
   не описаны — значит новое обогащение вернуло бы их. Дописываем решения.
   Пустой артикль = «не принадлежит ни одному роду»: страж такое отбрасывает у всех,
   а сборка (`dict_units_build.py:403`) пустое значение игнорирует и род не навязывает.

Запуск:
    DATABASE_URL=... python3 scripts/dict_close_lex_tails.py --dry-run
    DATABASE_URL=... python3 scripts/dict_close_lex_tails.py --apply
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import psycopg2
import psycopg2.extras

CARD_ID = 12004
CARD_RU = "поднос"
CARD_DE = "das Tablett"

# (дубликат, правильная единица, чем плох дубликат)
DUPLICATES = [
    (23047, 8178, "строка без рода и части речи; правильная — die Beschaffung"),
    (20477, 28725, "род не проставлен, часть речи «прилагательное»; правильная — die Kenntnisnahme"),
]

# (написание, значение, кому принадлежит) — пустая строка = никому.
GLOSS_RULINGS = [
    ("kiefer", "берёза", "", "выдумка модели: сосна это die Kiefer, берёза — Birke"),
    ("gehalt", "оклад", "das", "оклад и зарплата — das Gehalt"),
    ("gehalt", "суть", "der", "суть и содержание — der Gehalt"),
    ("kunde", "заказчик", "der", "заказчик и клиент — der Kunde"),
    ("kunde", "покупатель", "der", "покупатель — der Kunde"),
    ("verdienst", "премия", "", "das Verdienst — заслуга; премия это Prämie"),
    ("verdienst", "доход", "der", "доход и заработок — der Verdienst"),
    ("schild", "герб", "der", "герб на щите — der Schild"),
    ("schild", "знак", "das", "знак и табличка — das Schild"),
    ("schild", "табличка", "das", "знак и табличка — das Schild"),
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    apply = bool(args.apply) and not args.dry_run

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("нет DATABASE_URL"); return 2
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    backup: dict = {}

    # ─────────────────────────────────────────────────────────── 1. карточка 12004
    print("1. КАРТОЧКА 12004 — колонки приводим к разбору")
    cur.execute("""SELECT word_ru, word_de, translation_ru, translation_de, response_json
                   FROM bt_3_webapp_dictionary_queries WHERE id = %s;""", (CARD_ID,))
    found = cur.fetchone()
    if not found:
        print("   карточки нет")
    else:
        word_ru, word_de, tr_ru, tr_de, payload = found
        payload = payload if isinstance(payload, dict) else {}
        primary = ((payload.get("meanings") or {}).get("primary") or {}).get("value")
        print(f"   разбор говорит: главное значение «{primary}», примеры про Tablett")
        print(f"   было : {word_ru} / {word_de}")
        print(f"   стало: {CARD_RU} / {CARD_DE}")
        if apply:
            backup["card_12004"] = {"word_ru": word_ru, "word_de": word_de,
                                    "translation_ru": tr_ru, "translation_de": tr_de,
                                    "response_json": payload}
            fresh = dict(payload)
            fresh.update({"source_text": CARD_RU, "target_text": CARD_DE,
                          "word_ru": CARD_RU, "word_de": CARD_DE,
                          "translation_ru": CARD_RU, "translation_de": CARD_DE})
            cur.execute("""UPDATE bt_3_webapp_dictionary_queries
                           SET word_ru=%s, word_de=%s, translation_ru=%s, translation_de=%s,
                               response_json=%s, updated_at=NOW()
                           WHERE id=%s;""",
                        (CARD_RU, CARD_DE, CARD_RU, CARD_DE,
                         psycopg2.extras.Json(fresh), CARD_ID))

    # ──────────────────────────────────────────────────── 2. пустые значения
    print("\n2. ЗНАЧЕНИЯ БЕЗ ПОЯСНЕНИЯ И БЕЗ ПЕРЕВОДА")
    empty_sql = """
        SELECT s.id, u.display FROM bt_3_lex_senses s JOIN bt_3_lex_units u ON u.id = s.unit_id
        WHERE COALESCE(s.note, '') = ''
          AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links l WHERE l.sense_id = s.id)
          AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links l2 WHERE l2.from_unit = s.unit_id)
        ORDER BY u.display;"""
    cur.execute(empty_sql)
    empties = cur.fetchall()
    for sense_id, display in empties:
        print(f"   удаляю значение {sense_id}: {display[:60]}")
    print(f"   всего: {len(empties)}")
    if apply and empties:
        cur.execute("DELETE FROM bt_3_lex_senses WHERE id = ANY(%s);", ([s[0] for s in empties],))

    # ──────────────────────────────────────────────────────── 3. дубликаты
    print("\n3. ДУБЛИКАТЫ ЕДИНИЦ")
    for dup, keep, why in DUPLICATES:
        cur.execute("SELECT display, COALESCE(pos,'∅'), COALESCE(gender,'∅') FROM bt_3_lex_units WHERE id=%s;", (dup,))
        dup_row = cur.fetchone()
        cur.execute("SELECT display, COALESCE(pos,'∅'), COALESCE(gender,'∅') FROM bt_3_lex_units WHERE id=%s;", (keep,))
        keep_row = cur.fetchone()
        if not dup_row or not keep_row:
            print(f"   {dup}→{keep}: одной из единиц нет, пропускаю"); continue
        print(f"   {dup} {dup_row} → {keep} {keep_row}\n      почему: {why}")
        for table, sql in (
            ("указатели", "INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind) "
                          "SELECT lang, surface_key, %(keep)s, match_kind FROM bt_3_lex_surfaces "
                          "WHERE unit_id=%(dup)s ON CONFLICT DO NOTHING;"),
            ("связи от него", "INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source, saves_count) "
                              "SELECT %(keep)s, to_unit, rank, source, saves_count FROM bt_3_lex_links "
                              "WHERE from_unit=%(dup)s AND to_unit <> %(keep)s "
                              "ON CONFLICT (from_unit, to_unit) DO UPDATE SET rank = LEAST(bt_3_lex_links.rank, EXCLUDED.rank);"),
            ("связи к нему", "INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source, saves_count) "
                             "SELECT from_unit, %(keep)s, rank, source, saves_count FROM bt_3_lex_links "
                             "WHERE to_unit=%(dup)s AND from_unit <> %(keep)s "
                             "ON CONFLICT (from_unit, to_unit) DO UPDATE SET rank = LEAST(bt_3_lex_links.rank, EXCLUDED.rank);"),
            ("происхождение", "INSERT INTO bt_3_lex_unit_sources (unit_id, entry_id, side) "
                              "SELECT %(keep)s, entry_id, side FROM bt_3_lex_unit_sources "
                              "WHERE unit_id=%(dup)s ON CONFLICT DO NOTHING;"),
            ("личные карточки", "UPDATE bt_3_webapp_dictionary_queries SET lex_unit_id=%(keep)s "
                                "WHERE lex_unit_id=%(dup)s;"),
        ):
            if apply:
                cur.execute(sql, {"dup": dup, "keep": keep})
                print(f"      {table}: перенесено {cur.rowcount}")
            else:
                print(f"      {table}: будет перенесено")
        if apply:
            cur.execute("DELETE FROM bt_3_lex_units WHERE id=%s;", (dup,))
            print(f"      дубликат удалён")

    # ──────────────────────────────────────────────────────── 4. страж
    print("\n4. СПРАВОЧНИК ЗНАЧЕНИЙ (чтобы вычищенное не вернулось обогащением)")
    for lemma_key, gloss, article, why in GLOSS_RULINGS:
        cur.execute("SELECT article FROM bt_3_lex_gloss_rulings WHERE lemma_key=%s AND gloss_key=%s;",
                    (lemma_key, gloss))
        existing = cur.fetchone()
        shown = article or "никому"
        if existing:
            print(f"   {lemma_key} / {gloss}: уже есть ({existing[0] or 'никому'})"); continue
        print(f"   {lemma_key} / {gloss} → {shown}   ({why})")
        if apply:
            cur.execute("""INSERT INTO bt_3_lex_gloss_rulings (lemma_key, gloss_key, article, source)
                           VALUES (%s, %s, %s, 'вычитка')
                           ON CONFLICT (lemma_key, gloss_key) DO NOTHING;""",
                        (lemma_key, gloss, article))

    if apply:
        if backup:
            path = os.getenv("TAILS_BACKUP") or "/tmp/lex_tails_backup.json"
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(backup, fh, ensure_ascii=False, indent=1, default=str)
            print(f"\nстарые значения: {path}")
        conn.commit()
        print("записано")
    else:
        conn.rollback()
        print("\nсухой прогон — база не менялась")
    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
