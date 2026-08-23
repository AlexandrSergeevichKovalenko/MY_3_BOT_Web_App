# -*- coding: utf-8 -*-
"""ШАГ ТРЕТИЙ СПЛОШНОГО ПРОХОДА: закрыть клетки, где справочник молчал, и починить формы.

ОТКУДА ЗАДАЧА. После первых двух шагов реестр показал 1 716 клеток «источник молчит» и
123 глагольные клетки, которые сверка не приняла. «Молчит» — это не «хорошо»: это наряд
достроить источник. Разбор 23.08.2026 по причинам:

    49  справочника нет вовсе        → идём в сеть за страницей слова;
    41  формы правда разные          → наши выдуманы, у справочника они документированы;
    19  прочее (тот же класс)        → «abzweigen: wies ab» — форма ЧУЖОГО глагола;
     8  у нас пусто, справочник знает → заполняем;
     5  мусор в самом справочнике    → «veraltet:» в клетке Präteritum, дефект источника.

ЧТО ЭТОТ ШАГ ДЕЛАЕТ:
  1. Тянет страницу слова там, где в кэше её не было (каскад сам дописывает справочник).
  2. Пустую клетку заполняет документированной формой.
  3. Нашу форму заменяет на форму справочника ТОЛЬКО если нашей нет в таблице целиком.
     Если наша форма в таблице есть — это законный вариант («wendete ab» рядом с
     «wandte ab»), и мы её не трогаем, а штампуем «подтверждено».
  4. Клетки с мусором источника («veraltet:», «—») помечает отдельно: чинить надо
     разбор страницы, а не карточку.

⚠ НИ ОДНОЙ ФОРМЫ, ВЫВЕДЕННОЙ НАШЕЙ АРИФМЕТИКОЙ. Нет в источнике — клетка остаётся
незакрытой и попадает в число, которое видит владелец.

    python3 scripts/dict_close_silent_cells.py --field plural  --limit 50
    python3 scripts/dict_close_silent_cells.py --field verbs --apply
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

from backend.database import get_db_connection_context          # noqa: E402
from backend.german_reference_forms import noun_declension_for  # noqa: E402
from backend.german_verb_paradigms import paradigm_for_verb     # noqa: E402

CONFIRMED, SILENT = "подтверждено", "источник молчит"
FIXED = "исправлено по справочнику"
FILLED = "заполнено из справочника"
SOURCE_BROKEN = "источник испорчен"
GENDER_KEY = {"der": "m", "die": "f", "das": "n"}
JUNK = {"veraltet:", "selten:", "—", "-", "", None}
_ARTICLE = re.compile(r"^(?:der|die|das)\s+", re.I)


def bare(word: str) -> str:
    return _ARTICLE.sub("", str(word or "").strip()).strip()


def letters(value: str) -> str:
    return "".join(sorted(ch for ch in str(value or "").lower() if ch.isalpha()))


def attested(form: str, table: dict) -> bool:
    """Есть ли наша форма в таблице справочника — целиком или разнесённой по клеткам.

    Отделяемые глаголы справочник печатает раздельно («fügte zu»), у нас они слитные
    («zufügte»), поэтому сравниваем по буквам и пробуем разрез слова надвое.
    """
    SERVICE = {"er", "sie", "es", "ich", "du", "wir", "ihr", "man",
               "hat", "hatte", "ist", "war", "haben", "sind"}
    cells: set[str] = set()
    for cell in re.findall(r'"([^"]{2,80})"', json.dumps(table, ensure_ascii=False)):
        cells.add(letters(cell))
        for word in cell.split():
            if word.lower() not in SERVICE:
                cells.add(letters(word))
    words = [w for w in str(form or "").split() if w.lower() not in SERVICE]
    if not words:
        return False
    for word in words:
        key = letters(word)
        if key in cells:
            continue
        if any(letters(word[:i]) in cells and letters(word[i:]) in cells
               for i in range(2, len(word) - 1)):
            continue
        return False
    return True


def close_verbs(cur, rows, apply: bool) -> dict:
    tally = {FIXED: 0, FILLED: 0, CONFIRMED: 0, SOURCE_BROKEN: 0, SILENT: 0}
    for unit_id, display, card in rows:
        table = paradigm_for_verb(bare(display), allow_network=True)
        if not table:
            tally[SILENT] += 1
            verdict, note, patch = SILENT, "страницы спряжения нет", None
        else:
            ref_pr = (table.get("praeteritum") or {}).get("er/sie/es")
            ref_pf = (table.get("perfekt") or {}).get("er/sie/es")
            if ref_pr in JUNK and ref_pf in JUNK:
                tally[SOURCE_BROKEN] += 1
                verdict, note, patch = SOURCE_BROKEN, "в клетках справочника служебный текст", None
            elif str(table.get("source") or "") != "wiktionary-flexion":
                # ⚠ КАСКАД ПОДПИСЫВАЕТ СВОЙ ПУТЬ, и по подписи видно, можно ли писать.
                #   «wiktionary-flexion»              — своя страница слова, пишем;
                #   «wiktionary-flexion:полная форма» — ответ дан страницей ПОЛНОЙ формы
                #       («runterwerfen» → «herunterwerfen»), и оттуда пришло бы «warf
                #       herunter» в карточку слова, где верно «warf runter»;
                #   «wiktionary-flexion:основа»       — таблица ОСНОВЫ составного глагола
                #       («zurechthängen» → «hängen»), а у основы бывает своя парадигма.
                # В двух последних случаях таблица подтверждает, что глагол существует,
                # но переписывать по ней нашу форму — подмена слова, а не починка.
                tally[SILENT] += 1
                verdict = SILENT
                note = f"ответ дан не своей страницей: {table.get('source')}"
                patch = None
            else:
                forms = (card or {}).get("forms") or {}
                patch, notes = {}, []
                for key, ref in (("praeteritum", ref_pr), ("perfekt", ref_pf)):
                    ours = str(forms.get(key) or "").strip()
                    if ref in JUNK:
                        continue
                    if not ours:
                        patch[key] = ref
                        notes.append(f"{key}: пусто → «{ref}»")
                    elif attested(ours, table):
                        notes.append(f"{key}: подтверждено")
                    else:
                        patch[key] = ref
                        notes.append(f"{key}: «{ours}» → «{ref}»")
                note = "; ".join(notes)[:200]
                if patch:
                    verdict = FILLED if all("пусто" in n for n in notes if "→" in n) else FIXED
                    tally[verdict] += 1
                else:
                    verdict = CONFIRMED
                    tally[CONFIRMED] += 1
        print(f"   {display[:24]:26} {verdict:26} {note[:60]}")
        if not apply:
            continue
        if patch:
            for key, value in patch.items():
                cur.execute("""UPDATE bt_3_lex_units
                               SET card = jsonb_set(COALESCE(card,'{}'::jsonb),
                                   ARRAY['forms', %s], %s::jsonb, TRUE), updated_at = NOW()
                               WHERE id = %s;""",
                            (key, json.dumps(value, ensure_ascii=False), unit_id))
        cur.execute("""UPDATE bt_3_field_checks SET verdict=%s, source=%s, reference=%s,
                       checked_at=NOW() WHERE unit_id=%s AND field='verb_forms';""",
                    (verdict, "страница спряжения", note[:400], unit_id))
    return tally


def close_plural(cur, rows, apply: bool) -> dict:
    tally = {FILLED: 0, CONFIRMED: 0, SILENT: 0}
    for unit_id, display, gender, card in rows:
        key = GENDER_KEY.get(str(gender or "").strip().lower())
        tables = noun_declension_for(bare(display), allow_network=True) or {}
        table = tables.get(key) if key else None
        ref = ""
        for row in (table or {}).get("rows") or []:
            if row.get("case") == "nom":
                ref = bare(str(row.get("plural") or "").strip())
        ours = str(((card or {}).get("forms") or {}).get("plural") or "").strip()
        if not ref:
            verdict, note = SILENT, "страница склонения не дала множественного"
            tally[SILENT] += 1
        elif ours and bare(ours).split("(")[0].strip().lower() == ref.lower():
            verdict, note = CONFIRMED, "совпало со страницей склонения"
            tally[CONFIRMED] += 1
        elif not ours:
            verdict, note = FILLED, f"пусто → «die {ref}»"
            tally[FILLED] += 1
        else:
            verdict, note = SILENT, f"наше «{ours}» против «{ref}» — решает человек"
            tally[SILENT] += 1
        print(f"   {display[:24]:26} {verdict:26} {note[:60]}")
        if not apply:
            continue
        if verdict == FILLED:
            cur.execute("""UPDATE bt_3_lex_units
                           SET card = jsonb_set(COALESCE(card,'{}'::jsonb),
                               '{forms,plural}', %s::jsonb, TRUE), updated_at = NOW()
                           WHERE id = %s;""",
                        (json.dumps(f"die {ref}", ensure_ascii=False), unit_id))
        cur.execute("""UPDATE bt_3_field_checks SET verdict=%s, source=%s, reference=%s,
                       checked_at=NOW() WHERE unit_id=%s AND field='plural';""",
                    (verdict, "страница склонения", note[:400], unit_id))
    return tally


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--field", choices=("verbs", "plural"), required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            if args.field == "verbs":
                cur.execute("""SELECT u.id, u.display, u.card FROM bt_3_field_checks c
                               JOIN bt_3_lex_units u ON u.id=c.unit_id
                               WHERE c.field='verb_forms'
                                 AND c.verdict IN ('проверка не готова', %s)
                               ORDER BY u.id;""", (SILENT,))
            else:
                cur.execute("""SELECT u.id, u.display, u.gender, u.card FROM bt_3_field_checks c
                               JOIN bt_3_lex_units u ON u.id=c.unit_id
                               WHERE c.field='plural' AND c.verdict=%s
                               ORDER BY u.id;""", (SILENT,))
            rows = cur.fetchall()
            if args.limit:
                rows = rows[:args.limit]
            print(f"клеток к закрытию: {len(rows)}\n")
            tally = (close_verbs if args.field == "verbs" else close_plural)(cur, rows, args.apply)
            if args.apply:
                conn.commit()

    print()
    for key, count in sorted(tally.items(), key=lambda kv: -kv[1]):
        print(f"   {key:26} {count:>5}")
    if not args.apply:
        print("\n(холостой прогон: ничего не записано, нужен --apply)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
