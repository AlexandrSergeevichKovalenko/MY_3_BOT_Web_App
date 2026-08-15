# -*- coding: utf-8 -*-
"""Поднять ЗНАЧЕНИЯ слова из личных копий на ОБЩЕЕ СЛОВО.

Зачем
─────
Замер 14.08.2026 по живой базе. Карточек, где разбор лежит и у человека, и на слове —
12 502. Из них 8 206 нельзя развязать: человек увидит меньше. И упираются они почти
все в ОДНО поле:

    поле «значения» (dictionary_senses) есть у 542 слов из 10 217   — 5,3 %
    примеры 99,4 % · формы 99,9 % · переводы 98,0 % · управление 62,7 %

    из 8 206 карточек 6 426 держатся за копию ИМЕННО из-за значений.

То есть общее слово беднее личной копии ровно в одном месте. Пока это так, копии
снимать нельзя — и предохранитель в dict_drop_redundant_card_copies.py правильно их
не пускает.

Содержимое при этом уже написано и оплачено — оно просто лежит не в том слое.
Здесь мы его переносим: модель НЕ вызывается, ничего не выдумывается.
Из нескольких копий одного слова берётся самая полная.

Фильтр приёмки (важно!)
───────────────────────
В общее слово пишем только то, что не придётся потом оттуда выковыривать. Значение
отбраковывается, если это «свалка» — склеенный список с нумерацией («1) … 2) …»),
перенос строки внутри значения, или простыня длиннее 200 знаков. Такие карточки
остаются со своей копией и попадают в отдельный разбор (пункты 4 и 5 списка дефектов).
Правило владельца: фильтр стоит НА ВХОДЕ в базу, а не на выходе.

Откат
─────
Перед записью прежний разбор слова целиком уходит в bt_3_unit_card_backup.

    python3 scripts/dict_promote_senses_to_units.py            # сухой прогон
    python3 scripts/dict_promote_senses_to_units.py --apply    # записать
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import (                                        # noqa: E402
    get_db_connection_context,
    unit_card_is_about_the_same_word_sql,
    UNIT_OWNS_CARD_SURFACE_SQL,
    _coerce_json_object,
)

# Признаки «свалки»: несколько значений, склеенных в одну строку.
_NUMBERED_DUMP_RE = re.compile(r"\d\s*[).]\s*\S")
_MAX_SENSE_LEN = 200


def sense_is_clean(sense: dict) -> tuple[bool, str]:
    """Годится ли значение к тому, чтобы лечь в ОБЩЕЕ слово."""
    if not isinstance(sense, dict):
        return False, "не запись"
    value = str(sense.get("value") or "").strip()
    if not value:
        return False, "пусто"
    if "\n" in value:
        return False, "перенос строки внутри значения"
    if _NUMBERED_DUMP_RE.search(value):
        return False, "свалка с нумерацией"
    if len(value) > _MAX_SENSE_LEN:
        return False, "простыня длиннее %d знаков" % _MAX_SENSE_LEN
    return True, ""


def clean_senses(raw) -> tuple[list, list]:
    """Разделить значения на годные и отбракованные.

    Заодно снимаем повторы: в копиях сплошь и рядом «утилизировать · избавляться ·
    утилизировать» — одно и то же значение записано дважды. На карточке это выглядит
    как будто у слова пять смыслов, а их три. В общее слово такое не пускаем.
    """
    if not isinstance(raw, list):
        return [], []
    good, bad, seen = [], [], set()
    for sense in raw:
        ok, why = sense_is_clean(sense)
        if not ok:
            bad.append((sense, why))
            continue
        key = " ".join(str(sense.get("value") or "").lower().split()).strip(" .,;")
        if key in seen:
            bad.append((sense, "повтор значения внутри слова"))
            continue
        seen.add(key)
        good.append(sense)
    return good, bad


def senses_score(senses: list) -> int:
    """Полнота: сколько значений и сколько в них написано."""
    return len(senses) * 1000 + sum(
        len(str(s.get("value") or "")) + len(str(s.get("context") or "")) for s in senses
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT q.id, q.word_de, u.id, u.lemma_key, u.lemma,
                       q.response_json -> 'dictionary_senses',
                       """ + UNIT_OWNS_CARD_SURFACE_SQL.format(q="q", u="u") + """
                FROM bt_3_webapp_dictionary_queries q
                JOIN bt_3_lex_units u ON u.id = q.lex_unit_id
                WHERE u.card IS NOT NULL
                  AND (u.card -> 'dictionary_senses') IS NULL
                  AND jsonb_typeof(q.response_json -> 'dictionary_senses') = 'array'
                  AND jsonb_array_length(q.response_json -> 'dictionary_senses') > 0
                ORDER BY q.id;
                """
            )
            rows = cur.fetchall()

    best: dict[int, tuple] = {}
    skipped_wrong_word = 0
    rejected_cards = 0
    reject_reasons: dict[str, int] = {}
    reject_samples: list[tuple] = []

    for entry_id, word_de, unit_id, lemma_key, lemma, raw, surface_confirms in rows:
        if not unit_card_is_about_the_same_word_sql(
            unit_lemma_key=lemma_key,
            card_word=word_de,
            surface_confirms=bool(surface_confirms),
        ):
            # Заголовок карточки не про это слово — поднимать нельзя, слово получит
            # чужие значения. Такие идут в отдельный разбор.
            skipped_wrong_word += 1
            continue
        good, bad = clean_senses(raw if isinstance(raw, list) else _coerce_json_object(raw))
        for sense, why in bad:
            reject_reasons[why] = reject_reasons.get(why, 0) + 1
            if len(reject_samples) < 8:
                reject_samples.append((entry_id, str(word_de)[:30], why,
                                       str(sense.get("value") if isinstance(sense, dict) else sense)[:90]))
        if not good:
            rejected_cards += 1
            continue
        score = senses_score(good)
        current = best.get(int(unit_id))
        if not current or score > current[0]:
            best[int(unit_id)] = (score, entry_id, word_de, lemma, good)

    print("карточек со значениями, у слова которых значений НЕТ: %d" % len(rows))
    print("  заголовок карточки не про это слово (пропускаем):  %6d" % skipped_wrong_word)
    print("  все значения отбракованы фильтром:                 %6d" % rejected_cards)
    print("  РАЗНЫХ СЛОВ получат значения:                      %6d" % len(best))
    if reject_reasons:
        print()
        print("отбраковано значений по причинам:")
        for why, n in sorted(reject_reasons.items(), key=lambda kv: -kv[1]):
            print("   %6d  %s" % (n, why))
        print()
        for entry_id, word, why, value in reject_samples:
            print("   карточка %-8s «%s» — %s" % (entry_id, word, why))
            print("      %s" % value)
    print()
    for unit_id, (_score, entry_id, word_de, lemma, good) in list(sorted(best.items()))[:15]:
        print("   слово %-8s %-28s ← карточка %-8s значений: %d" % (
            unit_id, str(lemma)[:28], entry_id, len(good)))
        print("        %s" % " · ".join(str(s.get("value"))[:40] for s in good[:3]))
    if len(best) > 15:
        print("   … и ещё %d слов" % (len(best) - 15))

    if not args.apply:
        print("\nСУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")
        return

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_unit_card_backup (
                    id         BIGSERIAL PRIMARY KEY,
                    saved_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    unit_id    BIGINT NOT NULL,
                    card       JSONB,
                    reason     TEXT
                );
                """
            )
            written = 0
            for unit_id, (_score, entry_id, _word, _lemma, good) in best.items():
                cur.execute(
                    "INSERT INTO bt_3_unit_card_backup (unit_id, card, reason) "
                    "SELECT id, card, %s FROM bt_3_lex_units WHERE id = %s;",
                    ("до подъёма значений с карточки %s, 14.08.2026" % entry_id, unit_id),
                )
                cur.execute(
                    """
                    UPDATE bt_3_lex_units
                       SET card = jsonb_set(COALESCE(card, '{}'::jsonb),
                                            '{dictionary_senses}', %s::jsonb, TRUE),
                           updated_at = NOW()
                     WHERE id = %s;
                    """,
                    (json.dumps(good, ensure_ascii=False), unit_id),
                )
                written += cur.rowcount
        conn.commit()
    print("\nСлов получило значения: %d из %d" % (written, len(best)))
    print("Прежний разбор этих слов сохранён в bt_3_unit_card_backup — откат возможен.")


if __name__ == "__main__":
    main()
