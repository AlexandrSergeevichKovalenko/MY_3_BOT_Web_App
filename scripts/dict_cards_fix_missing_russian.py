"""Вернуть русский перевод карточкам, у которых в русской колонке лежит не русский текст.

Замер 06.08.2026 — 13 карточек, и это ДВЕ разные беды:

1. **Перевод есть, но лежит не в той колонке** (3 карточки). «das Sozialwesen» → в
   `word_ru` записано «Sozialwesen», хотя в оплаченном разборе рядом лежит «социальное
   обеспечение», и связи единицы тоже его знают. Здесь платить не за что — берём своё.

2. **Перевода нет вовсе** (10 карточек, все — одна и та же фраза «Ich geniere mich
   fremd» у десяти разных людей). В `word_ru` записан немецкий пересказ «Ich schäme
   mich für jemand anderen», разбор пуст, у единицы нет ни одной связи. Здесь нужен
   настоящий разбор — покупаем ОДИН на фразу и кладём его НА ЕДИНИЦУ: перевод получают
   все десять сразу, а не тот, чью карточку чинили.

Порядок поиска перевода — от бесплатного к платному: связи единицы → `translations`
разбора → `dictionary_senses` → `meanings.primary` → `source_text` (для запросов,
заданных по-русски) → и только потом модель.

По умолчанию НИЧЕГО НЕ ПИШЕТ и НИЧЕГО НЕ ПОКУПАЕТ. Запись — только с --apply.

    python scripts/dict_cards_fix_missing_russian.py           # вхолостую
    python scripts/dict_cards_fix_missing_russian.py --apply   # записать
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, ".."))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

from database import get_db_connection_context  # noqa: E402
from dictionary_intake import clean_text, has_cyrillic  # noqa: E402
import lex_units  # noqa: E402


def _russian(value) -> str:
    text = clean_text(value)
    return text if has_cyrillic(text) else ""


def russian_from_card(card: dict | None) -> str:
    """Русский перевод, уже лежащий в оплаченном разборе."""
    if not isinstance(card, dict):
        return ""
    translations = card.get("translations")
    if isinstance(translations, list):
        for item in translations:
            if isinstance(item, dict) and _russian(item.get("value")):
                return _russian(item.get("value"))
    senses = card.get("dictionary_senses")
    if isinstance(senses, list):
        for item in senses:
            if isinstance(item, dict) and _russian(item.get("value")):
                return _russian(item.get("value"))
    meanings = card.get("meanings")
    if isinstance(meanings, dict):
        primary = meanings.get("primary")
        if isinstance(primary, dict) and _russian(primary.get("value")):
            return _russian(primary.get("value"))
    for key in ("translation_ru", "word_ru", "source_text", "word_source"):
        if _russian(card.get(key)):
            return _russian(card.get(key))
    return ""


def russian_from_unit(cur, unit_id) -> str:
    if not unit_id:
        return ""
    cur.execute(
        """SELECT v.display FROM bt_3_lex_links l
           JOIN bt_3_lex_units v ON v.id = l.to_unit
           WHERE l.from_unit = %s AND v.lang = 'ru'
           ORDER BY l.rank, v.id LIMIT 1;""",
        (unit_id,),
    )
    row = cur.fetchone()
    return _russian(row[0]) if row else ""


def buy_lookup(german: str) -> tuple[str, dict]:
    """Настоящий разбор немецкой фразы. Один вызов на ФРАЗУ, не на карточку."""
    from openai_manager import run_dictionary_lookup_multilang_core_fast

    raw = asyncio.run(
        run_dictionary_lookup_multilang_core_fast(
            word=german, source_lang="de", target_lang="ru", explanation_lang="ru",
        )
    )
    if not isinstance(raw, dict):
        return "", {}
    for key in ("word_target", "translation_ru", "word_ru"):
        if _russian(raw.get(key)):
            return _russian(raw.get(key)), raw
    return russian_from_card(raw), raw


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, word_de, word_ru, translation_ru, response_json, lex_unit_id
                   FROM bt_3_webapp_dictionary_queries
                   WHERE COALESCE(word_ru, '') <> '' AND word_ru !~ '[А-яЁё]'
                   ORDER BY id;"""
            )
            rows = cur.fetchall()

            free, needs_buy = [], {}
            for cid, word_de, word_ru, translation_ru, card, unit_id in rows:
                russian = (
                    russian_from_unit(cur, unit_id)
                    or russian_from_card(card if isinstance(card, dict) else None)
                    or _russian(translation_ru)
                )
                if russian:
                    free.append((cid, word_ru, russian))
                    continue
                german = clean_text(word_de) or clean_text(
                    (card or {}).get("word_de") if isinstance(card, dict) else ""
                )
                needs_buy.setdefault(german, []).append((cid, unit_id, word_ru))

            print("ПЕРЕВОД УЖЕ ЕСТЬ У НАС (платить не за что): %d карточек" % len(free))
            for cid, was, now in free:
                print("   %s: %r → %r" % (cid, (was or "")[:55], now[:55]))
            print("НУЖЕН НАСТОЯЩИЙ РАЗБОР: %d карточек, %d фраз(ы) — столько и покупаем"
                  % (sum(len(v) for v in needs_buy.values()), len(needs_buy)))
            for german, cards in needs_buy.items():
                print("   %r → %d карточек у %d человек"
                      % (german[:55], len(cards), len({c[0] for c in cards})))

            if not args.apply:
                print()
                print("ВХОЛОСТУЮ. Записать и купить разборы: --apply")
                return 0

            fixed = 0
            for cid, _was, russian in free:
                cur.execute(
                    """UPDATE bt_3_webapp_dictionary_queries
                       SET word_ru = %s,
                           translation_ru = CASE WHEN COALESCE(translation_ru, '') = ''
                                                  OR translation_ru !~ '[А-яЁё]'
                                             THEN %s ELSE translation_ru END
                       WHERE id = %s;""",
                    (russian, russian, cid),
                )
                fixed += 1
            conn.commit()

            bought = 0
            for german, cards in needs_buy.items():
                if not german:
                    print("   пропуск: немецкой стороны нет, покупать нечего")
                    continue
                russian, raw = buy_lookup(german)
                if not russian:
                    print("   %r: разбор не дал русского перевода, оставили как есть" % german[:50])
                    continue
                bought += 1
                unit_id = next((u for _c, u, _w in cards if u), None)
                if unit_id and raw:
                    # Разбор кладём НА ЕДИНИЦУ: его получат все, кто на слово подписан.
                    try:
                        lex_units.save_unit_card_if_richer(int(unit_id), raw, source="починка перевода")
                        lex_units.sync_unit_links_from_card(int(unit_id), raw)
                    except Exception as exc:
                        print("   разбор на единицу %s не лёг: %s" % (unit_id, exc))
                for cid, _u, _w in cards:
                    cur.execute(
                        """UPDATE bt_3_webapp_dictionary_queries
                           SET word_ru = %s,
                               translation_ru = CASE WHEN COALESCE(translation_ru, '') = ''
                                                      OR translation_ru !~ '[А-яЁё]'
                                                 THEN %s ELSE translation_ru END
                           WHERE id = %s;""",
                        (russian, russian, cid),
                    )
                    fixed += 1
                print("   %r → %r (карточек: %d)" % (german[:45], russian[:45], len(cards)))
            conn.commit()
    print()
    print("ПОЧИНЕНО КАРТОЧЕК: %d (куплено разборов: %d)" % (fixed, bought))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
