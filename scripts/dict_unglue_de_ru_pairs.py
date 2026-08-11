# -*- coding: utf-8 -*-
"""Расклейка записей, где немецкая фраза и её русский перевод лежат в одном поле.

Что нашлось 11.08.2026. Владелец прислал карточку повторения, на которой немецкая
сторона несла ДВЕ фразы подряд. В сырой записи оказалось хуже:

    source_lang = 'ru'
    source_text = 'Die häufigen Ermahnungen halfen nicht… — Частые замечания…'
    target_text = 'Die häufigen Ermahnungen helfen nicht… - Häufige Ermahnungen…'

То есть в source_text склеены НЕМЕЦКАЯ фраза и её РУССКИЙ перевод, а в target_text
лежат две немецкие формулировки. И так во всех шести текстовых полях записи, включая
разбор. Отсюда и «две фразы» на экране.

Замер: 68 записей, все из одного захода — апрель 1, май 57, июнь 10.

Почему это чинится БЕЗ модели. Граница однозначна и проверяема: слева от тире
латиница, справа кириллица. Ничего угадывать не нужно, а значит и нельзя.

Что делаем со вторым немецким вариантом. Решение владельца: он становится ОТДЕЛЬНОЙ
карточкой с тем же русским переводом — «wegen der Mieterhöhung» это живой синоним
«infolge der steigenden Mieten», и учить его отдельно осмысленно.

Записи чиним НА МЕСТЕ, не удаляя и не пересоздавая: к ним привязаны единицы слоя и
чужие карточки повторения, и новый id осиротил бы их.

Запуск:
    DATABASE_URL=... python3 scripts/dict_unglue_de_ru_pairs.py           # отчёт
    DATABASE_URL=... python3 scripts/dict_unglue_de_ru_pairs.py --apply
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

import psycopg2

_CYRILLIC = re.compile(r"[А-Яа-яЁё]")
_LATIN = re.compile(r"[A-Za-zÄÖÜäöüß]")
# Тире-разделитель: длинное, среднее или дефис, обязательно с пробелами по бокам.
# Без пробелов это часть слова («E-Mail»), и трогать её нельзя.
_SEP = re.compile(r"\s+[—–-]\s+")


def split_de_ru(text: str):
    """«немецкое — русское» → (немецкое, русское), либо None.

    Требуем именно такой порядок и чистоту сторон: слева не должно быть кириллицы,
    справа она обязана быть. Всё остальное — не наш случай, и мы его не трогаем.

    Тире в записи бывает НЕ ОДНО, и резать по первому нельзя. Живой пример:
    «Kommst du morgen? — Und ob! — Ты приедешь завтра? — Ещё бы!» — по первому тире
    немецкий ответ «Und ob!» уехал бы в русскую часть. Поэтому берём ПОСЛЕДНИЙ
    разделитель, слева от которого ещё нет кириллицы: он и есть граница языков."""
    value = str(text or "").strip()
    cut = None
    for match in _SEP.finditer(value):
        left, right = value[:match.start()].strip(), value[match.end():].strip()
        if not left or not right:
            continue
        if _CYRILLIC.search(left):
            break          # кириллица слева — граница осталась позади
        if not _LATIN.search(left) or not _CYRILLIC.search(right):
            continue
        cut = (left, right)
    return cut


def german_variants(text: str) -> list[str]:
    """Немецкие формулировки из target_text. Их бывает две, разделённых тире.

    Кириллица здесь означала бы, что поле устроено как-то ещё, — тогда молчим и
    отдаём поле целиком одним куском, чтобы ничего не разрезать наугад."""
    value = str(text or "").strip()
    if not value or _CYRILLIC.search(value):
        return [value] if value else []
    parts = [p.strip() for p in _SEP.split(value) if p.strip()]
    return parts or [value]


def rebuild_payload(payload, *, german: str, russian: str) -> str:
    data = dict(payload) if isinstance(payload, dict) else {}
    data.update({
        "source_text": russian, "target_text": german,
        "word_ru": russian, "translation_ru": russian,
        "word_de": german, "translation_de": german,
    })
    return json.dumps(data, ensure_ascii=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("Нужен DATABASE_URL", file=sys.stderr)
        sys.exit(1)
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    conn = psycopg2.connect(dsn, connect_timeout=25)
    conn.autocommit = False
    fixed = 0
    new_cards: list[dict] = []
    skipped = 0

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, source_lang, target_lang, source_text, target_text, response_json
            FROM bt_3_dictionary_entries
            ORDER BY id;
            """
        )
        rows = cur.fetchall()

        for entry_id, s_lang, t_lang, source_text, target_text, payload in rows:
            split = split_de_ru(source_text)
            if not split:
                continue
            german, russian = split
            variants = german_variants(target_text)
            # Первая формулировка target_text обычно повторяет немецкую часть source.
            # Всё, что от неё отличается, — самостоятельный синоним.
            extras = [v for v in variants if v and v.casefold() != german.casefold()]

            print(f"\n[{entry_id}]  {s_lang}→{t_lang}")
            print(f"   было   DE: {str(target_text)[:100]}")
            print(f"          RU: {str(source_text)[:100]}")
            print(f"   стало  DE: {german[:100]}")
            print(f"          RU: {russian[:100]}")
            for extra in extras:
                print(f"   + новая карточка: {extra[:90]}")

            if not args.apply:
                skipped += 1
                continue

            cur.execute(
                """
                UPDATE bt_3_dictionary_entries
                SET source_text = %s, target_text = %s,
                    word_ru = %s, translation_ru = %s,
                    word_de = %s, translation_de = %s,
                    response_json = %s, updated_at = NOW()
                WHERE id = %s;
                """,
                (russian, german, russian, russian, german, german,
                 rebuild_payload(payload, german=german, russian=russian), entry_id),
            )
            fixed += 1
            for extra in extras:
                new_cards.append({
                    "source_lang": s_lang, "target_lang": t_lang,
                    "russian": russian, "german": extra,
                    "payload": rebuild_payload(payload, german=extra, russian=russian),
                })

        if args.apply:
            from backend.database import upsert_dictionary_pool_entry
            created = 0
            for card in new_cards:
                try:
                    upsert_dictionary_pool_entry(
                        source_lang=card["source_lang"], target_lang=card["target_lang"],
                        source_text=card["russian"], target_text=card["german"],
                        word_ru=card["russian"], translation_ru=card["russian"],
                        word_de=card["german"], translation_de=card["german"],
                        response_json=json.loads(card["payload"]),
                    )
                    created += 1
                except Exception as exc:
                    print(f"   ! вторая карточка не завелась ({card['german'][:40]}): {exc}")
            conn.commit()
            print(f"\n✓ Расклеено записей: {fixed}. Заведено вторых карточек: {created}.")
        else:
            conn.rollback()
            print(f"\nНайдено записей: {skipped}. База НЕ изменена. Применить: --apply")
    conn.close()


if __name__ == "__main__":
    main()
