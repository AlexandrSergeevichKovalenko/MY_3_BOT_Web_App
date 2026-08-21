# -*- coding: utf-8 -*-
"""Заголовки-шпаргалки и три перепутанных слова. Решение владельца 21.08.2026: делать всё.

ЧТО ЧИНИМ

  A. ШПАРГАЛКА В ЗАГОЛОВКЕ — 15 слов вида «die Brücke, -n», «das Cockpit, -s». Хвост
     «, -n» это школьная запись множественного числа, а не часть слова, и человек видит
     её прямо в заголовке карточки. У 7 слов чистого написания в словаре ещё нет — им
     хвост снимается. У 8 такое слово УЖЕ ЕСТЬ, поэтому не переименование, а слияние.

  B. ДУБЛЬ С ОБРЕЗАННЫМ НАПИСАНИЕМ — «Abflughall» при живом «die Abflughalle» (32585).
     Переименовывать нечего: сливаем, как и остальные дубли.

  C. ДВА СЛОВА В ОДНОМ — «das Tablet» (планшет) держал переводы «поднос» и
     «официантский поднос», то есть значения слова «das Tablett» (9067), и личную
     карточку человека про поднос. Переводы и карточка уезжают к «das Tablett», а
     «das Tablet» получает разбор про себя.

     Путаница оказалась ВЗАИМНОЙ — это нашла проверка экраном уже после первой правки:
     у «Tablett» встречно лежали «планшет» и «Таблет (основной перевод, предмет)», то
     есть значения планшета, плюс «таблетка» — а это третье слово, «die Tablette».
     Снимаем их: перевод, который слову не принадлежит, хуже отсутствующего. И сам
     заголовок стоял строчными («tablett»), хотя существительное пишется с заглавной.

  D. РАЗБОР ПРО МНОЖЕСТВЕННОЕ И СТОРОНАМИ НАОБОРОТ — у «die Tonne» разбор озаглавлен
     «тонны»/«die Tonnen», поэтому примеры показывались слева по-русски. Переводы у
     слова верные, испорчен только разбор — собираем заново.

ЧТО ПРИ СЛИЯНИИ КУДА ЕДЕТ — точно то же, что в scripts/merge_form_units_into_lemma.py,
и по той же причине: на строку словаря ссылаются восемь таблиц, простой DELETE упал бы
на внешнем ключе либо утащил чужие данные.

    написания      → к остающемуся слову: поиск по «die Brücke, -n» продолжит находить;
    личные карточки → перецепляются, человек своё слово не теряет;
    источники, связи → переносятся, совпавшие снимаются;
    значения дубля  → уходят вместе со строкой: у остающегося слова свои.

Хвост снимается И В ЛИЧНЫХ КАРТОЧКАХ людей (15 штук, ровно по одной на слово): именно их
человек и учит, и «die Brücke, -n» стоит там в обоих полях. Починить только общее слово
значило бы сделать половину работы — на экране повторения осталась бы шпаргалка.

    python3 scripts/dict_close_headword_tails_and_mixups.py           # сухой прогон
    python3 scripts/dict_close_headword_tails_and_mixups.py --apply   # применить
"""
from __future__ import annotations

import argparse
import os
import re
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context  # noqa: E402
from backend.lex_units import normalize_query  # noqa: E402

# Школьный хвост множественного числа в конце заголовка: «, -n», «, -en», «, -s».
# Правило нарочно узкое: только запятая + дефис + окончание в самом конце строки.
# Ничего кроме хвоста не трогаем — само слово остаётся как есть.
_TAIL = re.compile(r",\s*[-–—]\s*(?:e|en|n|s|er|se|nen)?\s*$", re.I)

# Дубли с испорченным написанием, разобранные глазами 21.08.2026. Слева — что убираем,
# справа — слово, которое остаётся. Список закрытый: «похожее» слияние запрещено.
_KNOWN_DUPLICATES = {
    32587: (32585, "«Abflughall» — обрезанное «die Abflughalle»"),
}


def _merge(cur, drop_id: int, keep_id: int) -> None:
    """Перенести всё с `drop_id` на `keep_id` и снять строку. Порядок важен."""
    cur.execute("""UPDATE bt_3_lex_surfaces s SET unit_id=%s
                   WHERE s.unit_id=%s AND NOT EXISTS (
                       SELECT 1 FROM bt_3_lex_surfaces t
                        WHERE t.lang=s.lang AND t.surface_key=s.surface_key
                          AND t.unit_id=%s)""", (keep_id, drop_id, keep_id))
    cur.execute("DELETE FROM bt_3_lex_surfaces WHERE unit_id=%s", (drop_id,))
    cur.execute("UPDATE bt_3_webapp_dictionary_queries SET lex_unit_id=%s "
                "WHERE lex_unit_id=%s", (keep_id, drop_id))
    cur.execute("""UPDATE bt_3_lex_unit_sources s SET unit_id=%s
                   WHERE s.unit_id=%s AND NOT EXISTS (
                       SELECT 1 FROM bt_3_lex_unit_sources t
                        WHERE t.unit_id=%s AND t.entry_id=s.entry_id AND t.side=s.side)""",
                (keep_id, drop_id, keep_id))
    cur.execute("DELETE FROM bt_3_lex_unit_sources WHERE unit_id=%s", (drop_id,))
    cur.execute("""UPDATE bt_3_lex_links l SET from_unit=%s
                   WHERE l.from_unit=%s AND l.to_unit <> %s AND NOT EXISTS (
                       SELECT 1 FROM bt_3_lex_links t
                        WHERE t.from_unit=%s AND t.to_unit=l.to_unit)""",
                (keep_id, drop_id, keep_id, keep_id))
    cur.execute("""UPDATE bt_3_lex_links l SET to_unit=%s
                   WHERE l.to_unit=%s AND l.from_unit <> %s AND NOT EXISTS (
                       SELECT 1 FROM bt_3_lex_links t
                        WHERE t.to_unit=%s AND t.from_unit=l.from_unit)""",
                (keep_id, drop_id, keep_id, keep_id))
    cur.execute("DELETE FROM bt_3_lex_links WHERE from_unit=%s OR to_unit=%s",
                (drop_id, drop_id))
    for table in ("bt_3_phrase_check", "bt_3_phrase_review"):
        cur.execute(f"UPDATE {table} SET unit_id=%s WHERE unit_id=%s", (keep_id, drop_id))
    cur.execute("DELETE FROM bt_3_lex_senses WHERE unit_id=%s", (drop_id,))
    cur.execute("DELETE FROM bt_3_lex_units WHERE id=%s", (drop_id,))


def _rename(cur, unit_id: int, clean: str) -> None:
    """Снять хвост с заголовка. Старое написание остаётся ДВЕРЬЮ для поиска."""
    key = normalize_query(clean)
    cur.execute("UPDATE bt_3_lex_units SET display=%s, lemma=%s, lemma_key=%s, "
                "updated_at=NOW() WHERE id=%s", (clean, clean, key, unit_id))
    cur.execute("""INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                   VALUES ('de', %s, %s, 'exact') ON CONFLICT DO NOTHING""", (key, unit_id))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    renamed = merged = rebuilt = cards_cleaned = foreign_links = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            # ── A + B. Шпаргалки и дубль с обрезанным написанием ──────────────────
            cur.execute("SELECT id, display FROM bt_3_lex_units WHERE lang='de' ORDER BY id")
            tails = [(uid, disp) for uid, disp in cur.fetchall() if _TAIL.search(str(disp or ""))]

            plan_rename: list[tuple[int, str, str]] = []
            plan_merge: list[tuple[int, str, int, str]] = []
            for unit_id, display in tails:
                clean = _TAIL.sub("", str(display)).strip()
                cur.execute("SELECT id, display FROM bt_3_lex_units "
                            "WHERE lang='de' AND lemma_key=%s AND id<>%s",
                            (normalize_query(clean), unit_id))
                clash = cur.fetchone()
                if clash:
                    plan_merge.append((unit_id, display, int(clash[0]), str(clash[1])))
                else:
                    plan_rename.append((unit_id, display, clean))
            for drop_id, (keep_id, why) in _KNOWN_DUPLICATES.items():
                cur.execute("SELECT display FROM bt_3_lex_units WHERE id=%s", (drop_id,))
                drop = cur.fetchone()
                cur.execute("SELECT display FROM bt_3_lex_units WHERE id=%s", (keep_id,))
                keep = cur.fetchone()
                if drop and keep:
                    plan_merge.append((drop_id, str(drop[0]), keep_id, str(keep[0])))
                else:
                    print(f"  ⚠️ {why}: одного из слов уже нет — пропускаю")

            print(f"\nСНЯТЬ ХВОСТ ({len(plan_rename)}):\n")
            for unit_id, display, clean in plan_rename:
                print(f"  {unit_id:>6} {display!r} → {clean!r}")
            print(f"\nСЛИТЬ С СУЩЕСТВУЮЩИМ СЛОВОМ ({len(plan_merge)}):\n")
            for drop_id, display, keep_id, keep in plan_merge:
                cur.execute("SELECT count(*) FROM bt_3_webapp_dictionary_queries "
                            "WHERE lex_unit_id=%s", (drop_id,))
                cards = cur.fetchone()[0]
                print(f"  {drop_id:>6} {display!r} → {keep_id} {keep!r}  (карточек людей: {cards})")

            # ── C. Планшет и поднос разъезжаются ──────────────────────────────────
            print("\nРАЗВЕСТИ «das Tablet» (планшет) И «das Tablett» (поднос):\n")
            cur.execute("""SELECT l.to_unit, t.display, l.rank, l.source
                             FROM bt_3_lex_links l JOIN bt_3_lex_units t ON t.id=l.to_unit
                            WHERE l.from_unit=38707 ORDER BY l.rank""")
            wrong_links = cur.fetchall()
            for to_unit, display, rank, source in wrong_links:
                print(f"  перевод {display!r} уезжает на «das Tablett» (9067)")
            cur.execute("""SELECT id, word_de FROM bt_3_webapp_dictionary_queries
                            WHERE lex_unit_id=38707 AND word_de ILIKE '%%Tablett'""")
            wrong_cards = cur.fetchall()
            for card_id, word in wrong_cards:
                print(f"  карточка {card_id} ({word!r}) перецепляется на «das Tablett»")

            # Хвост в личных карточках людей — то, что человек видит на повторении.
            cur.execute(r"""SELECT id, lex_unit_id, word_de, translation_de
                              FROM bt_3_webapp_dictionary_queries
                             WHERE word_de ~ ',\s*[-–—]\s*(e|en|n|s|er|se|nen)?\s*$'
                                OR translation_de ~ ',\s*[-–—]\s*(e|en|n|s|er|se|nen)?\s*$'
                             ORDER BY id""")
            personal = cur.fetchall()
            print(f"\nСНЯТЬ ХВОСТ В ЛИЧНЫХ КАРТОЧКАХ ЛЮДЕЙ ({len(personal)}):\n")
            for card_id, unit_id, word_de, translation_de in personal:
                print(f"  {card_id:>7} {word_de!r} → {_TAIL.sub('', str(word_de or '')).strip()!r}")

            if not args.apply:
                print("\nСУХОЙ ПРОГОН. Ничего не изменено. Применить: --apply\n")
                return 0

            for unit_id, display, clean in plan_rename:
                _rename(cur, unit_id, clean)
                renamed += 1
            for drop_id, display, keep_id, keep in plan_merge:
                _merge(cur, drop_id, keep_id)
                merged += 1

            for to_unit, display, rank, source in wrong_links:
                cur.execute("""UPDATE bt_3_lex_links l SET from_unit=9067
                               WHERE l.from_unit=38707 AND l.to_unit=%s AND NOT EXISTS (
                                   SELECT 1 FROM bt_3_lex_links t
                                    WHERE t.from_unit=9067 AND t.to_unit=%s)""",
                            (to_unit, to_unit))
            cur.execute("DELETE FROM bt_3_lex_links WHERE from_unit=38707")
            for card_id, _word in wrong_cards:
                cur.execute("UPDATE bt_3_webapp_dictionary_queries SET lex_unit_id=9067 "
                            "WHERE id=%s", (card_id,))

            # Встречная половина путаницы: значения планшета и таблетки на «Tablett».
            for wrong in ("планшет", "Таблет (основной перевод, предмет)", "таблетка"):
                cur.execute("""DELETE FROM bt_3_lex_links l
                                USING bt_3_lex_units t
                                WHERE l.to_unit = t.id AND l.from_unit = 9067
                                  AND t.lang = 'ru' AND t.display = %s""", (wrong,))
                foreign_links += cur.rowcount or 0
            # Существительное пишется с заглавной — это правило языка, не оформление.
            cur.execute("UPDATE bt_3_lex_units SET display='Tablett', lemma='Tablett', "
                        "updated_at=NOW() WHERE id=9067 AND display <> 'Tablett'")

            for card_id, _unit_id, word_de, translation_de in personal:
                cur.execute(
                    "UPDATE bt_3_webapp_dictionary_queries SET word_de=%s, translation_de=%s "
                    "WHERE id=%s",
                    (_TAIL.sub("", str(word_de or "")).strip() or None,
                     _TAIL.sub("", str(translation_de or "")).strip() or None,
                     card_id),
                )
                cards_cleaned += 1
        conn.commit()

    # ── C + D. Разбор собирается заново уже ПОСЛЕ переносов ───────────────────────
    if args.apply:
        from backend.database import rebuild_unit_breakdown
        for unit_id, word in ((38707, "das Tablet"), (33738, "die Tonne")):
            if rebuild_unit_breakdown(unit_id, word):
                rebuilt += 1
                print(f"  разбор для «{word}» собран заново")
            else:
                # Молчать нельзя: без разбора слово останется с прежним, чужим.
                print(f"  ⚠️ разбор для «{word}» НЕ собрался — слово осталось со старым")

    print(f"\nснято хвостов: {renamed}, слито: {merged}, личных карточек почищено: {cards_cleaned}, разборов пересобрано: {rebuilt}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
