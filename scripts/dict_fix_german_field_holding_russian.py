# -*- coding: utf-8 -*-
"""Немецкое поле личной карточки держит РУССКИЙ текст. 17 карточек, замер 21.08.2026.

ЧТО ЭТО. У карточки человека четыре поля: немецкое слово, русское слово и по переводу к
каждому. У пятнадцати карточек в НЕМЕЦКОМ поле перевода лежит русский текст:

    word_de         'nahm hier die Legende ihr Anfang'      ← немецкий, верно
    translation_de  'здесь легенда взяла своё начало'       ← ДОЛЖЕН быть немецкий
    translation_ru  'здесь легенда взяла своё начало'       ← верно

Все пятнадцать пришли из разбора видео (`origin_process = 'youtube'`): туда немецкую
сторону клали не во все поля.

ЧИНИМ БЕЗ ВЫДУМОК: немецкая сторона у карточки уже есть — это её же поле `word_de`.
Ставим его в `translation_de`. Ничего не сочиняем и к модели не ходим.

ЧЕГО НЕ ТРОГАЕМ. Две карточки из семнадцати — другой дефект, не перепутанные стороны:
    5283  'die (Plural, Германия) die Wettbewerbsregeln'
    7566  'der/die (сильна разница по роду: der Berufstätige…) Berufstätige'
Здесь в самом немецком слове застряла пояснительная скобка. Это класс «мусор в
заголовке», и правится он вместе со своим классом, а не заодно.

    python3 scripts/dict_fix_german_field_holding_russian.py           # показать
    python3 scripts/dict_fix_german_field_holding_russian.py --apply   # починить
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

_CYRILLIC = re.compile(r"[А-Яа-яЁё]")
_LATIN = re.compile(r"[A-Za-zÄÖÜäöüß]")


def is_german(value: str) -> bool:
    text = str(value or "")
    return bool(_LATIN.search(text)) and not _CYRILLIC.search(text)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    fixed = 0
    skipped: list[tuple] = []
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT id, word_de, translation_de, origin_process
                     FROM bt_3_webapp_dictionary_queries
                    WHERE word_de ~ '[А-Яа-яЁё]' OR translation_de ~ '[А-Яа-яЁё]'
                    ORDER BY id;"""
            )
            rows = cursor.fetchall()
            print(f"\nкарточек с русским в немецком поле: {len(rows)}\n")
            for card_id, word_de, translation_de, origin in rows:
                # Чиним только когда немецкая сторона у карточки ЕСТЬ и она годная.
                if not is_german(word_de):
                    skipped.append((card_id, word_de, "немецкого слова в карточке нет"))
                    continue
                if is_german(translation_de):
                    continue
                print(f"  {card_id:>7} [{origin}]")
                print(f"          было:   {translation_de!r}")
                print(f"          станет: {word_de!r}")
                if args.apply:
                    cursor.execute(
                        "UPDATE bt_3_webapp_dictionary_queries SET translation_de = %s, "
                        "updated_at = NOW() WHERE id = %s;",
                        (word_de, card_id),
                    )
                    fixed += 1
        if args.apply:
            conn.commit()

    if skipped:
        print("\nНЕ ТРОГАЮ — это другой дефект, «мусор в заголовке»:\n")
        for card_id, word, why in skipped:
            print(f"  {card_id:>7} {str(word)[:70]!r} — {why}")

    print()
    print(f"починено: {fixed}" if args.apply else "сухой прогон, в базу ничего не писалось")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
