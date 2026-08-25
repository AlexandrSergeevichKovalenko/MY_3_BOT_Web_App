# -*- coding: utf-8 -*-
"""Проставить род существительным, застрявшим без него, — ТОЛЬКО из справочника.

Откуда эта работа
─────────────────
23.08.2026 владелец открыл карточку «Schnapsidee» и спросил, почему она без артикля.
Разбор нашёл класс: единица помечена моделью как «выражение» (`card.part_of_speech`),
и этот ярлык вывел её из всех ночных доборов рода разом — они берут только `pos='noun'`:

    lex_units.backfill_pos_gender_from_cards   требует card->>'article' IN (der/die/das)
    lex_units.fix_gender_conflicts_from_authority  требует pos='noun' И gender IS NOT NULL
    dictionary_article_backfill                требует part_of_speech='noun'
    lex_units.units_needing_card               требует card IS NULL — а карточка есть

То есть само не рассосётся никогда. Вход закрыт отдельно (заслон колоды теперь видит
неопределённый артикль, см. backend/daily_video_quality.py) — этот скрипт разбирает то,
что уже лежит.

Правило ноль: род берётся ИЗ ИСТОЧНИКА и источник называется вслух — он пишется в
`gender_source`. Ни одного рода «по окончанию», «по шву», «по общему правилу».
Не знает справочник — единица остаётся без рода и попадает в список владельцу.

    python3 scripts/lex_units_close_gender_gaps_from_reference.py            # показать
    python3 scripts/lex_units_close_gender_gaps_from_reference.py --apply    # записать
"""
from __future__ import annotations

import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context            # noqa: E402
from backend.noun_declension_reference import declension_facts    # noqa: E402

# Заголовок существительного: одно слово с заглавной буквы. Дефис допускается
# («Stumm-Modus»), пробел — нет: у оборота рода не бывает.
NOUN_HEAD = re.compile(r"^[A-ZÄÖÜ][A-Za-zÄÖÜäöüß-]*$")

# Части речи, которые САМА карточка назвала и у которых рода нет. Если разбор сказал
# «глагол», то «das Altwerden» из справочника — правильный ответ на неверный вопрос:
# субстантивированная форма существует, но карточка не про неё (поймано 24.08.2026).
# «phrase» сюда НЕ входит намеренно: это как раз тот ошибочный ярлык, который мы чиним.
CARD_POS_WITHOUT_GENDER = {
    "verb", "adjective", "adverb", "preposition", "conjunction",
    "pronoun", "numeral", "article", "interjection", "particle", "participle", "other",
}


def _кандидаты(cursor) -> list:
    """Единицы, застрявшие без рода: слово, заголовок похож на существительное, род пуст.

    Правило отбора названо здесь один раз и берётся отсюда же отчётом, чтобы два замера
    не разошлись — на этом уже горели (см. шапку scripts/dict_defect_audit.py).
    """
    cursor.execute(
        """
        SELECT id, lemma, pos, card->>'part_of_speech'
        FROM bt_3_lex_units
        WHERE lang = 'de' AND kind = 'word' AND gender IS NULL
        """
    )
    rows = []
    for unit_id, lemma, pos, card_pos in cursor.fetchall():
        head = str(lemma or "").strip()
        if not head or not NOUN_HEAD.match(head):
            continue
        # Части речи, у которых рода нет, отсекаем сразу: глагол, прилагательное, наречие
        # и прочее стоят с заглавной в лемме по другим причинам.
        known = str(pos or "").strip().lower()
        if known and known != "noun":
            continue
        if str(card_pos or "").strip().lower() in CARD_POS_WITHOUT_GENDER:
            continue
        rows.append((unit_id, head, pos, card_pos))
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true",
                        help="записать род в базу (без флага — только показать)")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        if not args.apply:
            conn.set_session(readonly=True)
        with conn.cursor() as cursor:
            кандидаты = _кандидаты(cursor)
            # Имена собственные — те, что разобраны с владельцем поимённо.
            cursor.execute(
                "SELECT unit_id FROM bt_3_field_checks "
                "WHERE field = 'gender' AND verdict = %s",
                ("имя собственное — артикль не ставится",),
            )
            PROPER_NOUN_IDS = {row[0] for row in cursor.fetchall()}

        решено, не_знает = [], []
        for unit_id, head, pos, card_pos in кандидаты:
            article, source, _есть_множественное = declension_facts(head)
            # ⚠ 25.08.2026 отсюда УБРАН вывод «нет множественного ⇒ имя собственное».
            # Он был неверен, и цена измерена: без множественного 12 050 слов, среди них
            # обычные «Milch», «Plastikmüll», «Bürgertum», «Jünglingsalter». Имена
            # собственные опознаются ПОИМЁННОЙ пометкой в bt_3_field_checks (решение
            # владельца 24.08.2026), а не отсутствием формы.
            if PROPER_NOUN_IDS and unit_id in PROPER_NOUN_IDS:
                не_знает.append((unit_id, head,
                                 "имя собственное — артикль не ставится", pos, card_pos))
            elif article:
                решено.append((unit_id, head, article, source, pos, card_pos))
            else:
                не_знает.append((unit_id, head, source, pos, card_pos))

        print(f"Единиц без рода с заголовком-существительным: {len(кандидаты)}")
        print(f"  справочник ответил:      {len(решено)}")
        print(f"  справочник не знает:     {len(не_знает)}")
        print()
        for unit_id, head, article, source, pos, card_pos in решено:
            метка = f"pos={pos or '∅'}, card.pos={card_pos or '∅'}"
            print(f"  ✔ {head:26} → {article:4} ({source})   [{метка}]")
        for unit_id, head, source, pos, card_pos in не_знает:
            метка = f"pos={pos or '∅'}, card.pos={card_pos or '∅'}"
            print(f"  ? {head:26} → род неизвестен: {source}   [{метка}]")

        if not args.apply:
            print("\nПрогон вхолостую. Записать: --apply")
            return 0

        with conn.cursor() as cursor:
            for unit_id, head, article, source, _pos, _card_pos in решено:
                # pos ставим вместе с родом: без 'noun' единица снова выпадет из ночных
                # доборов, и завтра мы вернёмся сюда же.
                cursor.execute(
                    """
                    UPDATE bt_3_lex_units
                       SET gender = %s, gender_source = %s,
                           pos = 'noun', pos_source = COALESCE(NULLIF(pos_source,''), %s),
                           updated_at = NOW()
                     WHERE id = %s AND gender IS NULL
                    """,
                    (article, source, source, unit_id),
                )
        conn.commit()
        print(f"\nЗаписано: {len(решено)}. Осталось владельцу: {len(не_знает)}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
