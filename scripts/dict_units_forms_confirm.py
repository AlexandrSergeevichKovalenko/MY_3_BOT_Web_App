# -*- coding: utf-8 -*-
"""Указатели-словоформы: сверка со справочником и чистка доказанного мусора.

ЗАЧЕМ ЭТОТ ФАЙЛ ПОЯВИЛСЯ
────────────────────────
Владелец 01.09.2026 нажал слово в фильме и увидел в строке «ПЕРЕВОД» слово «рыется»,
которого в русском языке нет. Разбор показал ДВА разных дефекта на одном экране:

  1. строка «ПЕРЕВОД» брала ответ у машинного переводчика (это чинится отдельно);
  2. наш собственный указатель форм вёл голое написание НЕ К ТОМУ глаголу:

        ging  → ausgehen  («выходить»), а базового «gehen» в ответе не было вовсе
        gräbt → untergraben («подрывать») вместо graben («копать»)
        auf   → 34 глагола сразу, хотя это предлог, а не форма

Родил это `scripts/dict_units_paradigm_surfaces.py`: он резал ячейку парадигмы на слова.
У отделяемого глагола в ячейке напечатано «ging aus» — в указатели уезжали оба куска.
Дверь закрыта правилом `backend.german_grammar_tables.form_token_of_cell`; этот скрипт
разбирает то, что уже натекло.

ЧЕМ СУДИМ (источник называется вслух)
─────────────────────────────────────
`bt_3_german_verb_paradigms` — таблицы со страниц Flexion:<глагол> de.wiktionary.org,
разобранные `backend/german_verb_paradigms.py`. Правило одно:

    форма принадлежит глаголу, только если она НАПЕЧАТАНА ЦЕЛОЙ ЯЧЕЙКОЙ его таблицы.

У «ausgehen» это «ging aus», «geht aus», «ausgegangen»; одиночного «ging» там нет.
Так же печатают лидеры: dict.cc — «eingehen | ging ein | eingegangen», Wiktionary у
формы «ging» заводит статью «Konjugierte Form … des Verbs gehen», PONS — «ging → von
gehen», Duden — «ging, siehe gehen». Ни один не отдаёт голое «ging» приставочному глаголу.

ЧТО СНОСИМ, А ЧТО НЕТ — И ПОЧЕМУ ЭТО НЕ ОДНО И ТО ЖЕ
────────────────────────────────────────────────────
«В таблице не напечатано» и «не является формой» — РАЗНЫЕ утверждения. Проверка
01.09.2026: «gehauen» (настоящее причастие от hauen) в таблице отсутствует — там стоит
только «gehaut»; «auszulaugen» — настоящий zu-инфинитив, но строки zu-инфинитива в нашем
разборе таблицы нет; «umzingele», «heuchele» — настоящие варианты 1-го лица, таблица
печатает по одному. Снести их значило бы повторить ошибку «сторож работал идеально и
резал нужное».

Поэтому сносится ТОЛЬКО то, что доказано И объяснено — где назван настоящий владелец:

    A. написание — отделяемая приставка САМОЙ леммы («auf» у «aufzeichnen»).
       Владелец написания — приставка; формой глагола оно не является в принципе.
    B. написание НАПЕЧАТАНО целой ячейкой у БАЗОВОГО глагола, а лемма — его
       приставочный родич («ging» напечатано у «gehen», лемма «zugehen» им кончается).
       Владелец написания — базовый глагол.

Всё остальное остаётся на месте и попадает в отчёт:

    B2. то же, что B, но базового глагола НЕТ в справочнике — доказать нечем.
        Лечится пополнением справочника (`warm_verb_paradigms`), после чего B2 → B.
    D.  не напечатано и владельца назвать не смогли — разбирается поштучно.
    «нет в справочнике» — про сам глагол мы ещё не спрашивали.

Запуск:
    python3 scripts/dict_units_forms_confirm.py              # отчёт, база не меняется
    python3 scripts/dict_units_forms_confirm.py --apply      # снести классы A и B
    python3 scripts/dict_units_forms_confirm.py --list 40    # + примеры по классам
"""
from __future__ import annotations

import argparse
import collections
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context          # noqa: E402
from backend.german_grammar_tables import split_separable_verb  # noqa: E402
from backend.german_verb_paradigms import whole_cell_forms      # noqa: E402

# Ответы модели лежат в той же таблице под своим ключом. Судить чужие указатели ими
# нельзя: это не источник, а подтверждённая догадка — она годится, чтобы ПОКАЗАТЬ
# таблицу, но не чтобы УДАЛЯТЬ данные.
_MODEL_KEY_PREFIX = "модель:"

CONFIRMED = "подтверждено справочником"
CLASS_A = "A: приставка леммы, а не форма"
CLASS_B = "B: форма базового глагола (доказано)"
CLASS_B2 = "B2: форма базового глагола (базового нет в справочнике)"
CLASS_D = "D: не напечатано, владельца не назвали"
NO_REF = "про этот глагол справочник не спрашивали"

DELETABLE = (CLASS_A, CLASS_B)


def load_reference(cur) -> dict[str, set[str]]:
    """Глагол → множество форм, напечатанных ЦЕЛЫМИ ячейками."""
    cur.execute(
        """SELECT verb, tables FROM bt_3_german_verb_paradigms
            WHERE documented AND verb NOT LIKE %s;""",
        (_MODEL_KEY_PREFIX + "%",),
    )
    # Ключ — casefold, ТОТ ЖЕ, что у написаний в bt_3_lex_surfaces
    # (`lex_units.normalize_query`). SQL-функция lower() оставляет «ß» как есть, а
    # casefold превращает его в «ss» — из-за этого расхождения «aufgießen» не
    # находился в справочнике и его указатель «auf» переживал чистку (01.09.2026).
    return {str(verb).casefold(): whole_cell_forms(tables)
            for verb, tables in cur.fetchall() if isinstance(tables, dict)}


def load_known_verbs(cur) -> set[str]:
    """Все написания, которые мы вправе считать немецкими глаголами.

    Нужны, чтобы отличить «форму базового глагола» от случайного совпадения: базовый
    глагол обязан быть настоящим словом, а не отрезанным хвостом леммы."""
    cur.execute("SELECT DISTINCT lower(lemma) FROM bt_base_dictionary "
                "WHERE source_lang = 'de' AND pos = 'verb';")
    known = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT DISTINCT lower(display) FROM bt_3_lex_units "
                "WHERE lang = 'de' AND pos = 'verb';")
    known |= {r[0] for r in cur.fetchall()}
    return known


def classify(surface: str, lemma: str, reference: dict[str, set[str]],
             owner_of: dict[str, set[str]], known_verbs: set[str]) -> tuple[str, str]:
    """Класс указателя и НАСТОЯЩИЙ владелец написания (пустая строка — не назван)."""
    # Разбор на приставку делается по ИСХОДНОМУ написанию, а не по casefold: в Python
    # «ß».casefold() == «ss», и «aufgießen» превращается в «aufgiessen». Основа «giessen»
    # в справочнике не значится (там «gießen»), разбор срывается, и «auf» оставалось
    # висеть указателем на глагол. Поймано на живых данных 01.09.2026.
    original_lemma = str(lemma or "").strip()
    surface = surface.casefold()
    lemma = lemma.casefold()
    cells = reference.get(lemma)
    if cells is None:
        return NO_REF, ""
    if surface in cells:
        return CONFIRMED, lemma
    # A. Написание — отделяемая приставка самой леммы: «auf» у «aufzeichnen».
    #
    # Сравнивать НАЧАЛО СТРОКИ здесь нельзя, и это проверено на живых данных 01.09.2026:
    # «durchziehe» — тоже начало слова «durchziehen», но это НАСТОЯЩАЯ форма 1-го лица,
    # а не приставка. Приставку берём у разбора `split_separable_verb` — там список
    # собран прогоном по 439 отделяемым глаголам справочника, а не на глаз.
    prefix, base = split_separable_verb(original_lemma)
    if prefix and surface == prefix.casefold():
        return CLASS_A, surface
    # B. Написание напечатано у базового глагола, а лемма им заканчивается.
    bases = sorted(v for v in owner_of.get(surface, ()) if v != lemma and lemma.endswith(v))
    if bases:
        return CLASS_B, bases[0]
    # B2. Базовый глагол виден, но справочник о нём молчит — доказать нечем.
    tail = ""
    for start in range(1, len(lemma) - 2):
        rest = lemma[start:]
        if rest in known_verbs and rest not in reference and len(rest) > len(tail):
            tail = rest
    if tail:
        return CLASS_B2, tail
    return CLASS_D, ""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true",
                        help="снести указатели классов A и B (по умолчанию только отчёт)")
    parser.add_argument("--list", type=int, default=0, metavar="N",
                        help="показать по N примеров на класс")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        cur = conn.cursor()
        reference = load_reference(cur)
        known_verbs = load_known_verbs(cur) | set(reference)
        owner_of: dict[str, set[str]] = collections.defaultdict(set)
        for verb, cells in reference.items():
            for cell in cells:
                if " " not in cell:
                    owner_of[cell].add(verb)

        cur.execute(
            """SELECT s.surface_key, u.display, s.unit_id
                 FROM bt_3_lex_surfaces s
                 JOIN bt_3_lex_units u ON u.id = s.unit_id
                WHERE s.lang = 'de' AND s.match_kind = 'inflected' AND u.pos = 'verb';"""
        )
        rows = cur.fetchall()

        counts: collections.Counter = collections.Counter()
        samples: dict[str, list[str]] = collections.defaultdict(list)
        doomed: list[tuple[str, int]] = []
        for surface, display, unit_id in rows:
            verdict, owner = classify(surface, str(display), reference, owner_of, known_verbs)
            counts[verdict] += 1
            if len(samples[verdict]) < max(args.list, 6):
                samples[verdict].append("%-16s → %-18s настоящий владелец: %s"
                                        % (surface, display, owner or "не назван"))
            if verdict in DELETABLE:
                doomed.append((surface, int(unit_id)))

        print("глаголов в справочнике (документировано): %d" % len(reference))
        print("указателей-словоформ у глаголов:          %d" % len(rows))
        print()
        for name in (CONFIRMED, CLASS_A, CLASS_B, CLASS_B2, CLASS_D, NO_REF):
            print("  %-56s %5d" % (name, counts[name]))
        print()
        print("  снести можно (доказано И объяснено): %d" % len(doomed))
        print("  остаётся на месте, ждёт справочника: %d"
              % (counts[CLASS_B2] + counts[CLASS_D] + counts[NO_REF]))

        if args.list:
            for name in (CLASS_A, CLASS_B, CLASS_B2, CLASS_D, NO_REF):
                print("\n─── %s" % name)
                for line in samples[name][:args.list]:
                    print("   " + line)

        if not args.apply:
            print("\n(отчёт: в базе ничего не менялось; чистить — с флагом --apply)")
            return 0

        removed = 0
        for surface, unit_id in doomed:
            cur.execute(
                """DELETE FROM bt_3_lex_surfaces
                    WHERE lang = 'de' AND match_kind = 'inflected'
                      AND surface_key = %s AND unit_id = %s;""",
                (surface, unit_id),
            )
            removed += cur.rowcount
        conn.commit()
        print("\nснято указателей: %d" % removed)

        cur.execute(
            """SELECT count(*) FROM bt_3_lex_surfaces s
                 JOIN bt_3_lex_units u ON u.id = s.unit_id
                WHERE s.lang = 'de' AND s.match_kind = 'inflected' AND u.pos = 'verb';"""
        )
        print("осталось указателей-словоформ у глаголов: %d" % cur.fetchone()[0])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
