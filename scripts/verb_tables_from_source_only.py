# -*- coding: utf-8 -*-
"""Глаголы, у которых таблица спряжения СЧИТАЛАСЬ, а не бралась из источника.

ЧТО БЫЛО. До 23.08.2026 `german_grammar_tables.build_verb_conjugation` при молчании
справочника досчитывал таблицу сам: резал основу, приклеивал окончания, недостающее брал
из полей карточки, которые модель приписала мимоходом. На настоящих немецких глаголах
чаще всего совпадало, но заголовком бывает не глагол — и на экран уходило «ich boree»,
«ich aspettiamoe», «ich besagte».

Владелец 23.08.2026: «конечно же мы должны починить эти 96 глаголов, кроме того что
делать на будущее механика не годится». Арифметика удалена из движка; здесь — уборка
накопленного: по каждому такому слову спрашивается модель (дважды, ответ принимается
только при полном совпадении), подтверждённая таблица кладётся в справочник глаголов.

ТРИ ИСХОДА, и каждый считается:
  • подтверждено — таблица теперь есть, и она из источника;
  • не глагол — оба спроса сказали, что спрягать нечего (у слова неверная часть речи);
  • не подтвердилось — ответы разошлись; таблицы нет, слово уходит владельцу.

    python3 scripts/verb_tables_from_source_only.py            # показать список
    python3 scripts/verb_tables_from_source_only.py --apply    # спросить модель
"""
from __future__ import annotations

import argparse
import os
import sys
import time

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context  # noqa: E402
from backend.german_grammar_tables import looks_like_zu_infinitive  # noqa: E402
from backend.german_verb_paradigms import (  # noqa: E402
    _MODEL_KEY_PREFIX,
    DISAGREED,
    NOT_A_VERB,
    NO_ANSWER,
    ensure_german_verb_paradigm_schema,
    fetch_documented_tables,
    load_paradigm,
    paradigm_for_verb,
    paradigm_from_model,
    store_paradigm,
)


def _headwords() -> list[str]:
    """Немецкие заголовки, помеченные глаголом, ИЗ ВСЕХ ТРЁХ ХРАНИЛИЩ.

    Таблица спряжения нигде не лежит — она строится в момент показа по заголовку.
    Поэтому считать только слова словаря мало: тот же заголовок приходит на экран из
    личной карточки человека и из общего пула, и там он тоже спрягался счётом.
    """
    words: set[str] = set()
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                -- `kind` НЕ ограничиваем: таблицу строит часть речи, а не вид записи.
                SELECT DISTINCT lower(display) FROM bt_3_lex_units
                 WHERE lang = 'de'
                   AND (pos = 'verb' OR card->>'part_of_speech' = 'verb')
                   AND display ~ '^[a-zäöüßA-ZÄÖÜ]+$';
                """)
            words.update(r[0] for r in cur.fetchall())
            cur.execute(
                """
                SELECT DISTINCT lower(word_de) FROM bt_3_webapp_dictionary_queries
                 WHERE word_de ~ '^[a-zäöüßA-ZÄÖÜ]+$'
                   AND response_json->>'part_of_speech' = 'verb';
                """)
            words.update(r[0] for r in cur.fetchall())
            cur.execute(
                """
                SELECT DISTINCT lower(source_text) FROM bt_3_dictionary_entries
                 WHERE source_lang = 'de' AND source_text ~ '^[a-zäöüßA-ZÄÖÜ]+$'
                   AND response_json->>'part_of_speech' = 'verb';
                """)
            words.update(r[0] for r in cur.fetchall())
    return sorted(w for w in words if w)


def candidates() -> list[str]:
    """Заголовки, у которых источника нет, — раньше им таблица считалась.

    Справочник читается ОДНИМ запросом в память: иначе на каждое слово приходится по
    три похода в базу через PgBouncer, и замер идёт десять минут вместо десяти секунд.
    """
    import backend.german_verb_paradigms as V

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT verb, tables, documented FROM bt_3_german_verb_paradigms;")
            known = {v: (t if d else {}) for v, t, d in cur.fetchall()}

    original = V.load_paradigm
    V.load_paradigm = lambda verb: known.get(str(verb or "").strip().lower())
    try:
        return [w for w in _headwords()
                if not looks_like_zu_infinitive(w) and not paradigm_for_verb(w)]
    finally:
        V.load_paradigm = original


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    ensure_german_verb_paradigm_schema()
    words = candidates()
    print(f"\nбез источника, таблица строилась счётом: {len(words)}")
    for word in words:
        print(f"      {word}")

    if not args.apply:
        print("\nСУХОЙ ПРОГОН. Спросить модель: --apply [--limit N]\n")
        return 0

    queue = words[: args.limit] if args.limit else words

    # ── Шаг 1: СПРАВОЧНИК. Половина списка — обычные немецкие глаголы («leuchten»,
    # «schämen», «urteilen»), которых просто ещё не спрашивали: ночной прогрев берёт по
    # десятку за раз. Платить модели за то, что напечатано в de.wiktionary, незачем.
    # Пауза и остановка при молчании — как в ночном прогреве: справочник уходит в лимит
    # примерно на десятом запросе подряд, и без остановки мы получили бы пачку ложных
    # «страницы нет».
    from_reference: list[str] = []
    still_open: list[str] = []
    silent_in_a_row = 0
    for index, word in enumerate(queue, 1):
        tables = None
        if silent_in_a_row < 3:
            tables = fetch_documented_tables(word)
            if tables is None:
                # Одно молчание — ещё не лимит: 23.08.2026 прогон оборвал ВЕСЬ шаг
                # справочника на первом же слове из-за мигнувшей сети, и 46 глаголов
                # ушли к модели зря. Переспрашиваем один раз, и только три молчания
                # подряд считаем упором в лимит.
                time.sleep(5.0)
                tables = fetch_documented_tables(word)
        if tables is None:
            silent_in_a_row += 1
            if silent_in_a_row == 3:
                print(f"  [{index}/{len(queue)}] справочник молчит третий раз подряд — "
                      f"остальное к модели")
            still_open.append(word)
            continue
        silent_in_a_row = 0
        store_paradigm(word, tables)
        time.sleep(1.5)
        if paradigm_for_verb(word):
            from_reference.append(word)
            print(f"  [{index}/{len(queue)}] {word} — есть в справочнике")
        else:
            still_open.append(word)

    # ── Шаг 2: МОДЕЛЬ, дважды, только при полном совпадении ответов.
    confirmed = 0
    by_reason: dict[str, list[str]] = {}
    for index, word in enumerate(still_open, 1):
        answer, reason = paradigm_from_model(word)
        if answer:
            store_paradigm(_MODEL_KEY_PREFIX + word, answer)
            confirmed += 1
            print(f"  [{index}/{len(still_open)}] {word} → "
                  f"{answer['praesens'].get('ich')!r} ({answer.get('auxiliary')})")
            continue
        if reason != NO_ANSWER:
            store_paradigm(_MODEL_KEY_PREFIX + word, {"reason": reason})
        by_reason.setdefault(reason, []).append(word)
        print(f"  [{index}/{len(still_open)}] ⚠️ {word} — {reason}")

    print(f"\nзакрыто справочником: {len(from_reference)}, "
          f"подтверждено моделью: {confirmed}")
    for reason, items in by_reason.items():
        print(f"{reason}: {len(items)}")
    left = len(words) - len(queue)
    if left:
        print(f"осталось в очереди: {left}")
    if by_reason.get(NOT_A_VERB):
        print("\nВЛАДЕЛЬЦУ — помечено глаголом, но глаголом не является "
              "(таблицы спряжения не будет, чинить нужно ЧАСТЬ РЕЧИ):")
        for word in by_reason[NOT_A_VERB]:
            print(f"      {word}")
    if by_reason.get(DISAGREED):
        print("\nВЛАДЕЛЬЦУ — два ответа модели разошлись, таблицы нет:")
        for word in by_reason[DISAGREED]:
            print(f"      {word}")
    if by_reason.get(NO_ANSWER):
        print("\nНЕ СПРОСИЛИ (связь оборвалась) — вопрос остался, повторить прогон:")
        for word in by_reason[NO_ANSWER]:
            print(f"      {word}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
