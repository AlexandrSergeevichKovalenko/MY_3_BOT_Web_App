# -*- coding: utf-8 -*-
"""Перевода нет НИГДЕ: русская сторона карточки заполнена немецким. Вписать перевод.

Откуда взялась
──────────────
Немые карточки (разбор есть, перевода нет) обычно чинятся раскладкой: перевод лежит
в разборе и просто не стал связью — этим занят dict_speak_mute_unit_cards.py.

16.08.2026 после его прогона осталась одна, которую раскладывать нечего:

    слово 44725  «Das Risiko willkürlicher Festnahmen.»
        word_ru         Risiko willkürlicher Festnahmen.   ← немецкий
        translation_ru  Risiko willkürlicher Festnahmen.   ← немецкий
        target_text     Das Risiko willkürlicher Festnahmen.
        translations[0].value  Das Risiko willkürlicher Festnahmen.

Русский текст в записи был ровно один — в поле-пояснении: «Опасность произвольных
арестов.» Переводом его брать нельзя: пояснение и перевод — разные поля, и страж
раскладки правильно отказался.

Что делает скрипт
─────────────────
Вписывает перевод, просмотренный глазами, во ВСЕ поля русской стороны — и в слово
справочника, и в личную карточку: word_ru, translation_ru, target_text, word_target,
meanings.primary.value, translations[].value. Потом зовёт боевую sync_unit_links_from_card,
и перевод становится связью — как после ночного разбора.

⚠ НЕ ТРОГАЕМ: original_query, raw_text (история запроса), pronunciation (разметка
ударений), source_text / word_de / word_source (немецкая сторона).

Страж дрейфа: если в базе лежит не тот текст, что просмотрен глазами, — ПРОПУСК.

    python3 scripts/dict_write_missing_translation.py            # сухой прогон
    python3 scripts/dict_write_missing_translation.py --apply    # записать
"""
from __future__ import annotations

import argparse
import json
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context      # noqa: E402
from backend import lex_units as LU                         # noqa: E402

# слово → (что должно лежать сейчас, перевод). Каждая строка просмотрена глазами.
# «Festnahme» — задержание, арест; «willkürlich» — произвольный, без законных оснований.
# Берём «арестов», а не «задержаний»: так уже написано в собственном примере записи
# («В этом регионе существует риск произвольных арестов»), и карточка не будет
# противоречить сама себе.
TRANSLATIONS = {
    44725: ("Das Risiko willkürlicher Festnahmen.", "Риск произвольных арестов."),
}

# Поля русской стороны. Немецкую сторону и историю запроса не трогаем.
RUSSIAN_FIELDS = ("word_ru", "translation_ru", "target_text", "word_target")


def fill_russian_side(card: dict, russian: str) -> tuple[dict, list[str]]:
    """Вписать перевод во все поля русской стороны. Возвращает (разбор, что поменяли)."""
    out = dict(card)
    touched = []
    for name in RUSSIAN_FIELDS:
        if out.get(name) != russian:
            touched.append("%s: %s → %s" % (name, str(out.get(name))[:40], russian))
            out[name] = russian

    meanings = out.get("meanings")
    if isinstance(meanings, dict) and isinstance(meanings.get("primary"), dict):
        primary = dict(meanings["primary"])
        if primary.get("value") != russian:
            touched.append("meanings.primary.value: %s → %s" % (str(primary.get("value"))[:40], russian))
            primary["value"] = russian
        out["meanings"] = {**meanings, "primary": primary}

    items = out.get("translations")
    if isinstance(items, list) and items:
        fixed = []
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                fixed.append(item)
                continue
            copy = dict(item)
            if index == 0 and copy.get("value") != russian:
                touched.append("translations[0].value: %s → %s" % (str(copy.get("value"))[:40], russian))
                copy["value"] = russian
                # В поле-пояснении лежал тот же перевод другими словами. Оставить его
                # значит показать человеку перевод дважды.
                if isinstance(copy.get("context"), str) and copy["context"].strip():
                    touched.append("translations[0].context убрано: %s" % copy["context"][:40])
                    copy["context"] = None
            fixed.append(copy)
        out["translations"] = fixed
    elif isinstance(items, list):
        out["translations"] = [{"value": russian, "context": None, "is_primary": True}]
        touched.append("translations: добавлен перевод")
    return out, touched


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    units_done = cards_done = skipped = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for unit_id, (expected, russian) in sorted(TRANSLATIONS.items()):
                cur.execute("SELECT display, card FROM bt_3_lex_units WHERE id = %s AND lang = 'de';",
                            (unit_id,))
                row = cur.fetchone()
                if not row:
                    print("   %-7s слова уже нет — ПРОПУСК" % unit_id)
                    skipped += 1
                    continue
                display, card = row
                if str(display or "").strip() != expected:
                    print("   %-7s ожидали %r, лежит %r — ПРОПУСК" % (unit_id, expected, display))
                    skipped += 1
                    continue

                print("   слово %s  %s" % (unit_id, expected))
                print("      перевод: %s" % russian)
                fixed, touched = fill_russian_side(card if isinstance(card, dict) else {}, russian)
                for line in touched:
                    print("         %s" % line)
                if args.apply:
                    cur.execute(
                        "UPDATE bt_3_lex_units SET card = %s::jsonb, updated_at = NOW() WHERE id = %s;",
                        (json.dumps(fixed, ensure_ascii=False), unit_id),
                    )
                units_done += 1

                cur.execute(
                    "SELECT id, response_json FROM bt_3_webapp_dictionary_queries "
                    "WHERE lex_unit_id = %s ORDER BY id;",
                    (unit_id,),
                )
                for entry_id, payload in cur.fetchall():
                    personal, changes = fill_russian_side(
                        payload if isinstance(payload, dict) else {}, russian)
                    if not changes:
                        continue
                    print("      карточка %s: мест поправлено %d" % (entry_id, len(changes)))
                    if args.apply:
                        cur.execute(
                            "UPDATE bt_3_webapp_dictionary_queries "
                            "SET response_json = %s::jsonb, translation_ru = %s, updated_at = NOW() "
                            "WHERE id = %s;",
                            (json.dumps(personal, ensure_ascii=False), russian, entry_id),
                        )
                    cards_done += 1
            if args.apply:
                conn.commit()

    if args.apply:
        # Связь-перевод делает боевая функция — та же, что после ночного разбора.
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                for unit_id in TRANSLATIONS:
                    cur.execute("SELECT card FROM bt_3_lex_units WHERE id = %s;", (unit_id,))
                    row = cur.fetchone()
                    if not row:
                        continue
                    report = LU.sync_unit_links_from_card(
                        unit_id, row[0] if isinstance(row[0], dict) else {}, native_lang="ru")
                    print("   слово %s: связей-переводов %s" % (unit_id, report.get("links")))

    print()
    print("слов: %d, карточек: %d, пропущено: %d" % (units_done, cards_done, skipped))
    if not args.apply:
        print("СУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")


if __name__ == "__main__":
    main()
