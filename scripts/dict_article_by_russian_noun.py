# -*- coding: utf-8 -*-
"""Артикль тем словам, где РУССКАЯ сторона доказывает, что это существительное.

Откуда работа (владелец, 24.08.2026)
────────────────────────────────────
Владелец открыл карточку «die Mies» — «Паршивый» — с выдуманными значениями про
неудачников. Разбор нашёл класс: немецкая сторона записана С ЗАГЛАВНОЙ буквы там, где
слово вовсе не существительное («Reif» вместо «reif», «Wetten» вместо «wetten»). В
немецком заглавная означает существительное, поэтому справочник честно ответил про
существительное, а модель сочинила под него смыслы.

Таких заголовков 111. У всех род известен и подтверждён справочником склонений, но на
экран артикль НЕ выходит: часть речи у них помечена слабо — `pos_source='wiktionary'`
отвечает на вопрос «существует ли существительное с таким написанием», а НЕ «про
существительное ли эта карточка». Для «Mies» оба ответа «да» и «нет» одновременно.

РЕШЕНИЕ ВЛАДЕЛЬЦА: вторым голосом берём РУССКУЮ сторону. Её писал человек осмысленно,
и она не врёт: «Нос» — существительное, «Зрелый» — прилагательное, «Грести» — глагол.

ПРАВИЛО ОТБОРА — НАМЕРЕННО УЗКОЕ. Русская сторона должна быть ОДНИМ словом, не
прилагательным и не глаголом по окончанию. Почему так строго:

  • многословный перевод почти всегда означает не-существительное:
    «В стороне, вдали» (наречие), «По эту сторону» (наречие),
    «1 мед. острый 2 острый, неотложный» (прилагательное). Первая, более мягкая
    попытка 24.08.2026 ловила только прилагательные и глаголы по окончанию — и
    пропустила ровно эти наречия в «хорошие». Проверка вывода это поймала;
  • окончание русского слова — НЕ определение части речи. Это отбор кандидатов.
    Настоящий определитель (pymorphy3 / spaCy ru) в проекте пока не стоит: spaCy есть,
    но только с немецкой моделью. Когда он появится, оставшиеся 71 разберутся им.

Цена узости названа числом: закрывается 38 из 111. Остальные ждут инструмента, а не
догадки — и это лучше, чем приклеить артикль наречию.

ИСКЛЮЧЕНИЯ, снятые глазами при разборе с владельцем:
  Einer       «Один» — это числительное/местоимение, а не существительное;
  Vertriebene «Изгнанник» — субстантивированное причастие, род зависит от пола
              человека (der/die), одним артиклем не подписывается.

    python3 scripts/dict_article_by_russian_noun.py          # показать
    python3 scripts/dict_article_by_russian_noun.py --apply  # записать
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

CAPITALISED = re.compile(r"^[A-ZÄÖÜ]\S*$")
RU_ADJECTIVE_TAIL = re.compile(r"(ый|ий|ой|ая|яя|ое|ее|ые|ие)$", re.I)
RU_VERB_TAIL = re.compile(r"(ть|ться|ти|чь)$", re.I)

# Разобрано глазами с владельцем 24.08.2026, артикль не ставится.
SKIP = {"einer", "vertriebene"}


def russian_proves_a_noun(text: str) -> bool:
    """Русская сторона доказывает, что перед нами существительное.

    Доказывает — значит: ровно одно слово, без перечисления, и оно не похоже ни на
    прилагательное, ни на глагол. Всё остальное — «не доказано», а не «доказано, что нет».
    """
    value = str(text or "").strip()
    if not value or re.search(r"[;,]", value) or len(value.split()) != 1:
        return False
    return not (RU_ADJECTIVE_TAIL.search(value) or RU_VERB_TAIL.search(value))


def collect(cur) -> list:
    cur.execute("SELECT DISTINCT COALESCE(NULLIF(word_de,''), source_text) "
                "FROM bt_3_dictionary_entries")
    heads = [str(r[0] or "").strip() for r in cur.fetchall()]
    heads = [h for h in heads if h and CAPITALISED.match(h)]

    confirmed = {w: a for w, (a, _why)
                 in articles_from_declension_reference(heads).items() if a}
    if not confirmed:
        return []

    cur.execute(
        "SELECT id, lemma, lemma_key, gender FROM bt_3_lex_units "
        "WHERE lang='de' AND pos='noun' AND pos_source='wiktionary' "
        "AND lemma_key = ANY(%s)",
        ([w.lower() for w in confirmed],),
    )
    units = {row[2]: row for row in cur.fetchall()}
    if not units:
        return []

    cur.execute("""
        SELECT lower(COALESCE(NULLIF(word_de,''), source_text)),
               COALESCE(NULLIF(translation_ru,''), target_text)
        FROM bt_3_dictionary_entries
        WHERE lower(COALESCE(NULLIF(word_de,''), source_text)) = ANY(%s)
    """, (list(units),))
    russian = {}
    for key, text in cur.fetchall():
        russian.setdefault(key, str(text or ""))

    ready = []
    for key, (unit_id, lemma, _key, gender) in sorted(units.items(), key=lambda kv: kv[1][1]):
        if key in SKIP:
            continue
        ru = russian.get(key, "")
        if not russian_proves_a_noun(ru):
            continue
        ready.append((unit_id, lemma, confirmed.get(lemma, ""), gender, ru))
    return ready


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="записать в базу")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        if not args.apply:
            conn.set_session(readonly=True)
        with conn.cursor() as cur:
            ready = collect(cur)

        print(f"Русская сторона доказывает существительное: {len(ready)} слов")
        print()
        for _id, lemma, article, gender, ru in ready:
            метка = "" if str(gender or "").lower() == article else f"  ⚠ у нас {gender}"
            print(f"   {article:4} {lemma:<26} {ru[:26]}{метка}")

        расхождения = [r for r in ready if str(r[3] or "").lower() != r[2]]
        if расхождения:
            print(f"\n⚠ Род на единице расходится со справочником у {len(расхождения)} — "
                  f"их НЕ трогаем, выбирать между двумя ответами значит угадывать.")

        if not args.apply:
            print("\nПрогон вхолостую. Записать: --apply")
            return 0

        записано = 0
        with conn.cursor() as cur:
            for unit_id, _lemma, article, gender, _ru in ready:
                if str(gender or "").lower() != article:
                    continue
                # Меняется ТОЛЬКО происхождение части речи: род уже стоит и совпал со
                # справочником. Так запись выходит из-под правила «слабая часть речи»
                # и артикль появляется на экране общего словаря.
                cur.execute(
                    "UPDATE bt_3_lex_units "
                    "   SET pos_source=%s, updated_at=NOW() "
                    " WHERE id=%s AND pos='noun' AND pos_source='wiktionary'",
                    ("русская сторона (решение владельца 24.08.2026)", unit_id),
                )
                записано += cur.rowcount
        conn.commit()
        print(f"\nЗаписано: {записано}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
