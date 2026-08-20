# -*- coding: utf-8 -*-
"""Вернуть главным ВАШ перевод там, где после правки его придумала модель.

ОТКУДА БЕРЁТСЯ КУЧА
───────────────────
До 14.08.2026 судья не переводил свою правку: на кнопке «Принять» стоял только немецкий
текст. Русского в решении не было, поэтому после правки перевод собирала модель — и она
же вставала главным. Замер 20.08.2026: таких решений 30.

Модель при этом не «переводит хуже» — она переводит СВОЁ. Разница видна сразу:

    Wann erfolgt die Lieferung?
        вы сохраняли : «Когда будет произведена доставка?»
        модель       : «Когда будет доставка?»            — обеднила

    Vergießen - vergoss - hat vergossen
        вы сохраняли : «Пролить-пролил- пролил»
        модель       : «проливать»                        — выкинула смысл записи
                                                            (это три формы глагола)

    Sie sollen sich haben scheiden lassen
        вы сохраняли : «Говорят, что они развелись»
        модель       : «Им следует развестись»            — перевернула смысл

ЧТО ДЕЛАЕТ СКРИПТ
─────────────────
Ничего не выдумывает и ничего не переводит. Ваш перевод УЖЕ лежит в строке разбора
(`bt_3_phrase_review.translation`) — это то, что было сохранено к фразе до правки. Он и
возвращается главным, машинный уходит вниз.

Но не вслепую. Правку вы принимали как «то же самое, но грамотно» — а мы теперь знаем,
что судья иногда молча менял смысл. Поэтому перед возвратом спрашиваем ровно тем же
механизмом, что проверяет правки судей (`_check_fix_twice`): подходит ли ваш сохранённый
перевод к тому немецкому, который в итоге лёг в базу. Подходит — ставим первым.
Не подходит — НЕ ТРОГАЕМ и показываем владельцу: там решение о смысле, а его принимает он.

    python3 scripts/phrase_review_restore_saved_meaning.py           # показать, не писать
    python3 scripts/phrase_review_restore_saved_meaning.py --apply   # вернуть
"""
from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

from backend.database import (  # noqa: E402
    get_db_connection_context,
    phrase_review_variants,
    promote_owner_translation,
)
from backend.dictionary_intake import clean_text  # noqa: E402
from backend.phrase_night_check import _check_fix_twice  # noqa: E402

WORKERS = 6


def sq(value: str) -> str:
    return " ".join(str(value or "").split()).strip()


def collect() -> list[tuple]:
    """Решения, где на кнопке не было русского → перевод собрала модель."""
    out = []
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT r.id, r.unit_id, r.text, r.translation, r.judges, r.arbiter, u.display
                     FROM bt_3_phrase_review r
                     JOIN bt_3_lex_units u ON u.id = r.unit_id
                    WHERE r.status = 'accepted'
                    ORDER BY r.id;"""
            )
            rows = cursor.fetchall()
            for review_id, unit_id, old, saved_ru, judges, arbiter, display in rows:
                old, display, saved_ru = sq(old), sq(display), sq(saved_ru)
                if not display or not saved_ru:
                    continue
                cursor.execute(
                    """SELECT u.display, l.rank, l.source FROM bt_3_lex_links l
                         JOIN bt_3_lex_units u ON u.id = l.to_unit
                        WHERE l.from_unit = %s ORDER BY l.rank LIMIT 1;""",
                    (unit_id,),
                )
                top = cursor.fetchone()
                top_ru, source = sq((top or ("",))[0]), str((top or ("", 0, ""))[2] or "")
                if source == "вычитка":
                    # Наверху уже РЕШЕНИЕ ВЛАДЕЛЬЦА — оно свежее и главнее того, что было
                    # сохранено к фразе когда-то. Перебивать его нельзя ни в какой ветке.
                    # Живой случай: у слова 17739 два разбора подряд, и в позднем владелец
                    # выбрал «Вина долго меня грызла»; вернуть туда старое «Чувство вины
                    # долго грызло меня» значило бы отменить его же решение.
                    continue
                if top_ru.casefold() == saved_ru.casefold():
                    continue          # уже стоит ваш — трогать нечего
                if display == old:
                    # Немецкий не менялся — значит сохранённый смысл к нему подходит по
                    # определению, спрашивать модель незачем. Такие сюда попадают потому,
                    # что пересборка разбора всё равно случилась и развесила машинные
                    # переводы. Живой случай: «Sie sollen sich haben scheiden lassen» —
                    # наверху стояло «Им следует развестись», а сохранено было «Говорят,
                    # что они развелись», то есть смысл на экране был перевёрнут.
                    out.append((review_id, unit_id, old, display, saved_ru, top_ru,
                                source, True))
                    continue
                variants = phrase_review_variants(
                    judges if isinstance(judges, list) else [], old,
                    arbiter if isinstance(arbiter, dict) else None)
                chosen = next((v for v in variants
                               if sq(clean_text(v["text"]) or "") == display), None)
                if sq((chosen or {}).get("ru") or ""):
                    continue          # русский на кнопке был — решение владельца уже стоит
                out.append((review_id, unit_id, old, display, saved_ru, top_ru, source,
                            False))
    return out


def check(item: tuple) -> tuple:
    """Подходит ли сохранённый перевод к немецкому, который в итоге лёг в базу."""
    _rid, _uid, old, display, saved_ru, _top, _src, german_unchanged = item
    if german_unchanged:
        return item, True, "немецкий не менялся — сохранённый смысл подходит", True
    verdict = _check_fix_twice(old, saved_ru, display)
    fits = bool(verdict.get("checked")) and bool(verdict.get("meaning_kept"))
    return item, fits, str(verdict.get("why") or ""), bool(verdict.get("checked"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    items = collect()
    print(f"\nРЕШЕНИЙ, ГДЕ ПЕРЕВОД СОБРАЛА МОДЕЛЬ: {len(items)}\n")
    if not items:
        return 0

    good, bad, unknown = [], [], []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for item, fits, why, checked in pool.map(check, items):
            (good if fits else (unknown if not checked else bad)).append((item, why))

    print(f"ВАШ ПЕРЕВОД ПОДХОДИТ — вернуть главным: {len(good)}\n")
    for (rid, _uid, _old, display, saved_ru, top_ru, source, _u), _why in good:
        print(f"   #{rid} {display!r}")
        print(f"        вернём наверх: {saved_ru!r}")
        print(f"        сейчас сверху: {top_ru!r} ({source})")

    if bad:
        print(f"\n⚠ НЕ ПОДХОДИТ — НЕ ТРОГАЮ, решать владельцу: {len(bad)}\n")
        for (rid, _uid, _old, display, saved_ru, top_ru, _src, _u), why in bad:
            print(f"   #{rid} {display!r}")
            print(f"        вы сохраняли : {saved_ru!r}")
            print(f"        сейчас сверху: {top_ru!r}")
            print(f"        почему не подходит: {why}")
    if unknown:
        print(f"\n⚠ ПРОВЕРИТЬ НЕ УДАЛОСЬ — тоже не трогаю: {len(unknown)}")
        for (rid, _uid, _old, display, *_rest), _why in unknown:
            print(f"   #{rid} {display!r}")

    if not args.apply:
        print("\nВХОЛОСТУЮ. Вернуть: --apply\n")
        return 0

    done = failed = 0
    for (_rid, unit_id, _old, _display, saved_ru, _top, _src, _u), _why in good:
        if promote_owner_translation(unit_id, saved_ru):
            done += 1
        else:
            failed += 1
    print(f"\nВЕРНУЛИ ГЛАВНЫМ: {done}. Не получилось: {failed}.")
    print(f"Оставлено владельцу: {len(bad) + len(unknown)}.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
