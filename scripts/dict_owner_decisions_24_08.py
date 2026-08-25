# -*- coding: utf-8 -*-
"""Решения владельца от 24.08.2026 по словам без артикля.

Повод. Владелец открыл карточку и увидел «die Mies» с выдуманными значениями про
неудачников. Разбор показал: немецкая сторона записана с ЗАГЛАВНОЙ буквы там, где
слово — прилагательное или глагол («Reif» вместо «reif», «Wetten» вместо «wetten»).
В немецком заглавная означает существительное, поэтому справочник честно ответил про
существительное, а модель сочинила под него смыслы.

Всего таких заголовков 111. Владелец посмотрел список глазами и разделил их сам:

    хорошие  — обычные существительные («die Mikrowelle» — «Микроволновая печь»).
               РЕШЕНИЕ: проставить род, артикль появится на экране.
    плохие   — прилагательные и глаголы с заглавной («Reif» — «Зрелый»).
               РЕШЕНИЕ: чинить немецкую сторону по русской. Делается ОТДЕЛЬНО,
               в этом скрипте их только помечаем, чтобы не трогать вслепую.

Признак деления — РУССКАЯ сторона: её писал человек осмысленно, и она не врёт.
«Зрелый» — прилагательное, «Нос» — существительное. Немецкая сторона врёт, русская нет.

Плюс три отдельных случая, разобранных с владельцем поимённо:

    Athen, Marokko      записаны ВЕРНО. Города и страны в немецком стоят без артикля,
                        и «род неизвестен» у них — не дефект, а норма. Помечаем, чтобы
                        они не всплывали в отчётах вечно.
    Drogenbeauftragte   слово живое: карточка владельца 329098 из YouTube, на экране
                        «die Drogenbeauftragte». Справочник склонений его не знает —
                        это субстантивированное причастие, они склоняются как
                        прилагательные и в таблицы существительных не попадают.
                        Род берём ИЗ КАРТОЧКИ ВЛАДЕЛЬЦА: там стоит «die», и это не
                        догадка, а то, что человеку уже показано.
    Bugfahrwerk         НЕ ТРОГАЕМ. Мой сторож «нет множественного — похоже на имя
                        собственное» отсёк его зря, но подкручивать сторож ради одного
                        слова опаснее, чем оставить как есть: он же корректно не пускает
                        «das Athen» (решение владельца 24.08.2026).

    python3 scripts/dict_owner_decisions_24_08.py          # показать
    python3 scripts/dict_owner_decisions_24_08.py --apply  # записать
"""
from __future__ import annotations

import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context                 # noqa: E402
from backend.noun_declension_reference import (                         # noqa: E402
    articles_from_declension_reference,
)

CAP = re.compile(r"^[A-ZÄÖÜ]\S*$")

# Имена собственные, разобранные с владельцем поимённо. Список закрытый и НЕ
# расширяется догадкой: «нет множественного» — признак, а не доказательство.
PROPER_NOUNS = ("Athen", "Marokko")

# Слово + род, взятый из карточки, которую человек уже видит на экране.
FROM_OWNER_CARD = {"Drogenbeauftragte": "die"}

# Русские окончания, по которым видно ПРИЛАГАТЕЛЬНОЕ. Это НЕ определение части речи
# русского слова — для этого будет отдельный источник (решение владельца 24.08.2026:
# «часть речи из русской стороны — обязательно, как второй голос»). Здесь всего лишь
# отбор кандидатов на РУЧНОЙ разбор: ни одна запись по этому признаку не правится.
_RU_ADJ_TAIL = re.compile(r"(ый|ий|ой|ая|яя|ое|ее|ые|ие)$", re.I)
# Русский инфинитив: «грести», «держать пари», «ездить верхом».
_RU_VERB_TAIL = re.compile(r"(ть|ться|ти|чь)$", re.I)


def _russian_looks_like(text: str) -> str:
    """«прилагательное» / «глагол» / «» — грубый отбор кандидатов, не вердикт."""
    first = str(text or "").strip().split(",")[0].split(";")[0].strip()
    if not first:
        return ""
    head = first.split()[0]
    tail = first.split()[-1]
    if _RU_VERB_TAIL.search(head) or _RU_VERB_TAIL.search(tail):
        return "глагол"
    if _RU_ADJ_TAIL.search(head) and len(first.split()) == 1:
        return "прилагательное"
    return ""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="записать в базу")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        if not args.apply:
            conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT COALESCE(NULLIF(word_de,''), source_text)
                FROM bt_3_dictionary_entries
            """)
            heads = [str(r[0] or "").strip() for r in cur.fetchall()]
            heads = [h for h in heads if h and CAP.match(h)]

            confirmed = {w: a for w, (a, _why) in
                         articles_from_declension_reference(heads).items() if a}

            cur.execute(
                # gender у них УЖЕ стоит — мешает не он, а слабая пометка части речи:
                # pos_source='wiktionary' отвечает «есть ли существительное с таким
                # написанием», а не «про существительное ли эта карточка».
                "SELECT id, lemma, lemma_key, gender FROM bt_3_lex_units "
                "WHERE lang='de' AND pos='noun' AND pos_source='wiktionary' "
                "AND lemma_key = ANY(%s)",
                ([w.lower() for w in confirmed],),
            )
            units = {row[2]: (row[0], row[1], row[3]) for row in cur.fetchall()}

            cur.execute("""
                SELECT lower(COALESCE(NULLIF(word_de,''), source_text)),
                       COALESCE(NULLIF(translation_ru,''), target_text)
                FROM bt_3_dictionary_entries
                WHERE lower(COALESCE(NULLIF(word_de,''), source_text)) = ANY(%s)
            """, (list(units),))
            russian = {}
            for key, text in cur.fetchall():
                russian.setdefault(key, text)

        хорошие, плохие = [], []
        for key, (unit_id, lemma, _gender) in sorted(units.items()):
            ru = str(russian.get(key) or "")
            вид = _russian_looks_like(ru)
            строка = (unit_id, lemma, confirmed.get(lemma, ""), ru, вид)
            (плохие if вид else хорошие).append(строка)

        print(f"Заголовков с подтверждённым артиклем и слабой частью речи: {len(units)}")
        print(f"  русская сторона — существительное  → ставим род : {len(хорошие)}")
        print(f"  русская сторона — не существительное → на разбор: {len(плохие)}")
        print()
        print("СТАВИМ РОД:")
        for _id, w, a, ru, _ in хорошие:
            print(f"   {a:4} {w:<26} {str(ru)[:34]}")
        print()
        print("НА РАЗБОР (немецкая сторона чинится по русской, ОТДЕЛЬНО):")
        for _id, w, a, ru, вид in плохие:
            print(f"   ({a}) {w:<24} {str(ru)[:26]:<28} ← {вид}")
        print()
        print("ОТДЕЛЬНЫЕ РЕШЕНИЯ ВЛАДЕЛЬЦА:")
        print(f"   имена собственные, артикль не ставится : {', '.join(PROPER_NOUNS)}")
        for w, a in FROM_OWNER_CARD.items():
            print(f"   род из карточки владельца              : {w} → {a}")
        print("   не трогаем                             : Bugfahrwerk")

        if not args.apply:
            print("\nПрогон вхолостую. Записать: --apply")
            return 0

        with conn.cursor() as cur:
            for unit_id, lemma, article, _ru, _ in хорошие:
                cur.execute(
                    "UPDATE bt_3_lex_units SET gender=%s, gender_source=%s, updated_at=NOW() "
                    "WHERE id=%s AND gender IS NULL",
                    (article, "справочник склонений", unit_id),
                )
            for word, article in FROM_OWNER_CARD.items():
                cur.execute(
                    "UPDATE bt_3_lex_units SET gender=%s, gender_source=%s, updated_at=NOW() "
                    "WHERE lang='de' AND lemma_key=%s AND gender IS NULL",
                    (article, "карточка владельца (решение 24.08.2026)", word.lower()),
                )
            # Имена собственные — отметкой в УЖЕ существующей таблице сплошного прохода,
            # а не новым значением части речи: заводить «proper_noun» значит трогать
            # _KNOWN_POS и всё, что на него завязано.
            for word in PROPER_NOUNS:
                cur.execute(
                    "SELECT id FROM bt_3_lex_units WHERE lang='de' AND lemma_key=%s",
                    (word.lower(),),
                )
                row = cur.fetchone()
                if not row:
                    continue
                cur.execute("""
                    INSERT INTO bt_3_field_checks
                        (unit_id, field, verdict, source, ours, reference, checked_at)
                    VALUES (%s, 'gender', %s, %s, '', '', NOW())
                    ON CONFLICT (unit_id, field) DO UPDATE
                       SET verdict = EXCLUDED.verdict, source = EXCLUDED.source,
                           checked_at = NOW()
                """, (row[0], "имя собственное — артикль не ставится",
                      "решение владельца 24.08.2026"))
        conn.commit()
        print(f"\nЗаписано: род у {len(хорошие)} слов + {len(FROM_OWNER_CARD)} из карточки; "
              f"помечено имён собственных: {len(PROPER_NOUNS)}. "
              f"На ручной разбор осталось: {len(плохие)}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
