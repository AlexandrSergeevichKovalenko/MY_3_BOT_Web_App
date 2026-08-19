# -*- coding: utf-8 -*-
"""Склеить строку-ФОРМУ с её настоящим словом. Сухой прогон по умолчанию.

Зачем. В словаре 11 строк оказались не словами, а их формами: «Botschaften» это
множественное от «Botschaft», «abgestumpft» — причастие от «abstumpfen», «Eifriger» —
склонённое «eifrig». Настоящее слово при этом в словаре УЖЕ есть. Справочник называет
исходное слово сам ({{Grundformverweis}}), поэтому склейка идёт по источнику, а не по
догадке.

ПОЧЕМУ НЕ ПРОСТО УДАЛИТЬ. Проверка вреда 19.08.2026: на строку словаря ссылаются восемь
таблиц — значения (lex_senses), поверхности (lex_surfaces), источники, связи (lex_links),
разбор фраз и личные карточки людей. Простой DELETE упал бы на внешнем ключе либо утащил
за собой чужие данные. Поэтому сначала ПЕРЕНОС, потом снятие строки.

ЧТО КУДА ЕДЕТ
    поверхности      → на настоящее слово; форма «Botschaften» становится его
                       поверхностью, то есть поиск по ней продолжит находить слово;
    личные карточки  → перецепляются, человек своё слово не теряет;
    источники        → переносятся, при совпадении пропускаются;
    связи            → перецепляются с обеих сторон;
    значения         → НЕ переносятся: они описывают форму, а у настоящего слова свои.
                       Они уходят вместе со строкой, и это осознанно — иначе у слова
                       появятся два набора значений об одном и том же.
    разбор фраз      → переносится, если есть.

    python3 scripts/merge_form_units_into_lemma.py           # сухой прогон
    python3 scripts/merge_form_units_into_lemma.py --apply   # применить
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


def _pairs(cur) -> list[tuple[int, str, int, str]]:
    cur.execute("""SELECT word, reason FROM bt_3_reference_forms_unresolved
                   WHERE reason LIKE 'дубль формы%' ORDER BY word""")
    out = []
    for word, reason in cur.fetchall() or []:
        found = re.search(r"«([^»]+)»", str(reason))
        if not found:
            continue
        target = found.group(1)
        cur.execute("SELECT id FROM bt_3_lex_units WHERE lang='de' AND lower(lemma)=%s",
                    (str(word).lower(),))
        form = cur.fetchone()
        cur.execute("SELECT id FROM bt_3_lex_units WHERE lang='de' AND lower(lemma)=%s",
                    (target.lower(),))
        keep = cur.fetchone()
        if form and keep and int(form[0]) != int(keep[0]):
            out.append((int(form[0]), str(word), int(keep[0]), target))
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            pairs = _pairs(cur)
            print(f"пар «форма → слово»: {len(pairs)}\n")
            for form_id, form, keep_id, keep in pairs:
                counts = []
                for table, col in (("bt_3_lex_surfaces", "unit_id"),
                                   ("bt_3_webapp_dictionary_queries", "lex_unit_id"),
                                   ("bt_3_lex_senses", "unit_id"),
                                   ("bt_3_lex_links", "from_unit")):
                    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {col}=%s", (form_id,))
                    n = cur.fetchone()[0]
                    if n:
                        counts.append(f"{table.replace('bt_3_lex_', '').replace('bt_3_', '')}={n}")
                print(f"  {form:20} → {keep:20} {', '.join(counts)}")

            if not args.apply:
                print("\nЭто СУХОЙ ПРОГОН. Ничего не изменено. Применить: --apply")
                return

            moved = 0
            for form_id, form, keep_id, keep in pairs:
                # Поверхности: форма становится поверхностью настоящего слова. Совпавшие
                # пропускаем — уникальность (lang, surface_key, unit_id).
                cur.execute("""UPDATE bt_3_lex_surfaces s SET unit_id=%s
                               WHERE s.unit_id=%s AND NOT EXISTS (
                                   SELECT 1 FROM bt_3_lex_surfaces t
                                    WHERE t.lang=s.lang AND t.surface_key=s.surface_key
                                      AND t.unit_id=%s)""", (keep_id, form_id, keep_id))
                cur.execute("DELETE FROM bt_3_lex_surfaces WHERE unit_id=%s", (form_id,))
                # Личные карточки — человек своё слово не теряет.
                cur.execute("UPDATE bt_3_webapp_dictionary_queries SET lex_unit_id=%s "
                            "WHERE lex_unit_id=%s", (keep_id, form_id))
                # Источники: уникальность (unit_id, entry_id, side).
                cur.execute("""UPDATE bt_3_lex_unit_sources s SET unit_id=%s
                               WHERE s.unit_id=%s AND NOT EXISTS (
                                   SELECT 1 FROM bt_3_lex_unit_sources t
                                    WHERE t.unit_id=%s AND t.entry_id=s.entry_id
                                      AND t.side=s.side)""", (keep_id, form_id, keep_id))
                cur.execute("DELETE FROM bt_3_lex_unit_sources WHERE unit_id=%s", (form_id,))
                # Связи с обеих сторон. Уникальность (from_unit, to_unit) — переносим
                # только те, которых у настоящего слова ещё нет, остальные снимаем:
                # связь «форма → X» и «слово → X» это одна и та же связь.
                # Прогон 19.08.2026 упал именно здесь, и слияние откатилось целиком.
                cur.execute("""UPDATE bt_3_lex_links l SET from_unit=%s
                               WHERE l.from_unit=%s AND l.to_unit <> %s AND NOT EXISTS (
                                   SELECT 1 FROM bt_3_lex_links t
                                    WHERE t.from_unit=%s AND t.to_unit=l.to_unit)""",
                            (keep_id, form_id, keep_id, keep_id))
                cur.execute("""UPDATE bt_3_lex_links l SET to_unit=%s
                               WHERE l.to_unit=%s AND l.from_unit <> %s AND NOT EXISTS (
                                   SELECT 1 FROM bt_3_lex_links t
                                    WHERE t.to_unit=%s AND t.from_unit=l.from_unit)""",
                            (keep_id, form_id, keep_id, keep_id))
                cur.execute("DELETE FROM bt_3_lex_links WHERE from_unit=%s OR to_unit=%s",
                            (form_id, form_id))
                # Разбор фраз.
                for table in ("bt_3_phrase_check", "bt_3_phrase_review"):
                    cur.execute(f"UPDATE {table} SET unit_id=%s WHERE unit_id=%s",
                                (keep_id, form_id))
                # Значения формы уходят вместе со строкой — см. шапку.
                cur.execute("DELETE FROM bt_3_lex_senses WHERE unit_id=%s", (form_id,))
                cur.execute("DELETE FROM bt_3_lex_units WHERE id=%s", (form_id,))
                cur.execute("DELETE FROM bt_3_reference_forms_unresolved WHERE word=%s", (form,))
                moved += 1
        conn.commit()
    print(f"\nсклеено: {moved}")


if __name__ == "__main__":
    main()
