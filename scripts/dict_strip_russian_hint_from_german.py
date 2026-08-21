# -*- coding: utf-8 -*-
"""Русская подсказка застряла ВНУТРИ немецкого слова. Убрать её из всех трёх мест.

ЧТО ВИДИТ ЧЕЛОВЕК:

    die (Plural, Германия) die Wettbewerbsregeln
    der/die (сильна разница по роду: der Berufstätige для мужчин, …) Berufstätige
    abheben (снять трубку)

Скобка здесь — пояснение для человека, а не часть немецкого слова. Оно и так лежит рядом,
в переводе. Внутри заголовка оно ломает всё сразу: слово не находится поиском по своему
настоящему написанию, разбор к нему собирается по мусорной строке, а в тренировке человек
заучивает немецкое слово вместе с русским хвостом.

ЗАМЕР 21.08.2026, правило отбора — ТОЛЬКО немецкое снаружи скобки и русское внутри:

    слова общего словаря                    2
    личные карточки людей                   2  (в обоих полях)
    общий пул: написание                    8
    общий пул: наследные колонки            2

ПОЧЕМУ НЕ 442. Широкий поиск «скобка с кириллицей» даёт 442 строки, но 440 из них —
обычные РУССКИЕ переводы с законным уточнением: «деньги (разг.)», «переулок (боковая
улица)», «вынимать (из розетки и т.п.)». Их трогать нельзя, это нормальный словарный
язык. Отсекается это одним условием: снаружи скобки должно быть НЕМЕЦКОЕ.

ЧТО ДЕЛАЕМ С ДВОЙНЫМ АРТИКЛЕМ. «die (Plural, Германия) die Wettbewerbsregeln» после
снятия скобки даёт «die die Wettbewerbsregeln» — повтор схлопывается. «der/die … »
остаётся как «der/die Berufstätige»: это слово правда двухродовое, и выбирать за
владельца один род нельзя — решение про два рода он уже принимал отдельно.

    python3 scripts/dict_strip_russian_hint_from_german.py           # показать
    python3 scripts/dict_strip_russian_hint_from_german.py --apply   # починить
"""
from __future__ import annotations

import argparse
import os
import re
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import (  # noqa: E402
    _normalize_dictionary_headword_key,
    _normalize_dictionary_text_key,
    get_db_connection_context,
)
from backend.lex_units import retitle_unit  # noqa: E402

_CYRILLIC = re.compile(r"[А-Яа-яЁё]")
_LATIN = re.compile(r"[A-Za-zÄÖÜäöüß]")
_BRACKET = re.compile(r"\s*\([^)]*\)")
# Повтор артикля подряд: «die die Wettbewerbsregeln» → «die Wettbewerbsregeln».
_DOUBLE_ARTICLE = re.compile(r"^((?:der|die|das)(?:/(?:der|die|das))?)\s+(der|die|das)\s+", re.I)


def is_german_with_russian_hint(value) -> bool:
    text = str(value or "")
    if "(" not in text:
        return False
    outside = _BRACKET.sub(" ", text)
    inside = " ".join(re.findall(r"\(([^)]*)\)", text))
    return (bool(_LATIN.search(outside)) and not _CYRILLIC.search(outside)
            and bool(_CYRILLIC.search(inside)))


def strip_hint(value: str) -> str:
    text = _BRACKET.sub(" ", str(value or ""))
    text = " ".join(text.split()).strip()
    text = _DOUBLE_ARTICLE.sub(r"\1 ", text)
    return " ".join(text.split()).strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    units = cards = pool = deduped = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            # ── 1. Слова общего словаря ─────────────────────────────────────────
            cursor.execute("SELECT id, display FROM bt_3_lex_units "
                           "WHERE lang='de' AND display LIKE '%(%';")
            unit_rows = [(i, v) for i, v in cursor.fetchall()
                         if is_german_with_russian_hint(v)]
            print(f"\nСЛОВА СЛОВАРЯ ({len(unit_rows)}):\n")
            for unit_id, display in unit_rows:
                print(f"  {unit_id:>6} {display[:70]!r}\n         → {strip_hint(display)!r}")

            # ── 2. Личные карточки людей ────────────────────────────────────────
            cursor.execute("SELECT id, word_de, translation_de FROM "
                           "bt_3_webapp_dictionary_queries "
                           "WHERE word_de LIKE '%(%' OR translation_de LIKE '%(%';")
            card_rows = [(i, w, t) for i, w, t in cursor.fetchall()
                         if is_german_with_russian_hint(w) or is_german_with_russian_hint(t)]
            print(f"\nЛИЧНЫЕ КАРТОЧКИ ({len(card_rows)}):\n")
            for card_id, word, trans in card_rows:
                print(f"  {card_id:>7} {str(word)[:60]!r}\n          → {strip_hint(word)!r}")

            # ── 3. Общий пул ────────────────────────────────────────────────────
            cursor.execute("""SELECT id, source_lang, target_lang, source_text, target_text,
                                     word_de, translation_de
                                FROM bt_3_dictionary_entries
                               WHERE source_text LIKE '%(%' OR word_de LIKE '%(%'
                                  OR translation_de LIKE '%(%';""")
            pool_rows = [r for r in cursor.fetchall()
                         if is_german_with_russian_hint(r[3])
                         or is_german_with_russian_hint(r[5])
                         or is_german_with_russian_hint(r[6])]
            print(f"\nОБЩИЙ ПУЛ ({len(pool_rows)}):\n")
            for entry_id, _sl, _tl, st, _tt, _wde, _tde in pool_rows:
                shown = st if is_german_with_russian_hint(st) else _wde
                print(f"  {entry_id:>7} {str(shown)[:60]!r}\n          → {strip_hint(shown)!r}")

            if not args.apply:
                print("\nСУХОЙ ПРОГОН. Ничего не изменено. Применить: --apply\n")
                return 0

            for unit_id, display in unit_rows:
                retitle_unit(cursor, unit_id, strip_hint(display))
                units += 1

            for card_id, word, trans in card_rows:
                cursor.execute(
                    "UPDATE bt_3_webapp_dictionary_queries SET word_de=%s, "
                    "translation_de=%s, updated_at=NOW() WHERE id=%s;",
                    (strip_hint(word) if is_german_with_russian_hint(word) else word,
                     strip_hint(trans) if is_german_with_russian_hint(trans) else trans,
                     card_id))
                cards += 1

            for entry_id, sl, tl, st, tt, wde, tde in pool_rows:
                clean_source = strip_hint(st) if is_german_with_russian_hint(st) else st
                # ⚠ Написание входит в ключ строки. Если такая запись уже живёт —
                # наша дубль, и её надо снять, а не втискивать поверх чужой.
                if clean_source != st:
                    cursor.execute(
                        """SELECT id FROM bt_3_dictionary_entries
                            WHERE source_lang=%s AND target_lang=%s
                              AND source_text_norm=%s AND target_text_norm=%s AND id<>%s
                            LIMIT 1;""",
                        (sl, tl, _normalize_dictionary_text_key(clean_source),
                         _normalize_dictionary_text_key(tt), entry_id))
                    twin = cursor.fetchone()
                    if twin:
                        cursor.execute(
                            """UPDATE bt_3_webapp_dictionary_queries q SET canonical_entry_id=%s
                                WHERE q.canonical_entry_id=%s AND NOT EXISTS (
                                    SELECT 1 FROM bt_3_webapp_dictionary_queries t
                                     WHERE t.user_id=q.user_id AND t.canonical_entry_id=%s);""",
                            (int(twin[0]), entry_id, int(twin[0])))
                        cursor.execute(
                            "UPDATE bt_3_webapp_dictionary_queries SET canonical_entry_id=NULL "
                            "WHERE canonical_entry_id=%s;", (entry_id,))
                        cursor.execute("DELETE FROM bt_3_dictionary_entries WHERE id=%s;",
                                       (entry_id,))
                        deduped += 1
                        print(f"  дубль снят: {entry_id} — такая запись уже есть под {twin[0]}")
                        continue
                cursor.execute(
                    """UPDATE bt_3_dictionary_entries
                          SET source_text=%s, source_text_norm=%s, source_headword_norm=%s,
                              word_de=%s, translation_de=%s, updated_at=NOW()
                        WHERE id=%s;""",
                    (clean_source, _normalize_dictionary_text_key(clean_source),
                     _normalize_dictionary_headword_key(clean_source) or None,
                     strip_hint(wde) if is_german_with_russian_hint(wde) else wde,
                     strip_hint(tde) if is_german_with_russian_hint(tde) else tde,
                     entry_id))
                pool += 1
        conn.commit()

    print(f"\nслов: {units}, карточек: {cards}, записей пула: {pool}, дублей снято: {deduped}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
