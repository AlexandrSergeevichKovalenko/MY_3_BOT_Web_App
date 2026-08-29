# -*- coding: utf-8 -*-
"""Вернуть фразе прежнее написание по прямому решению владельца.

ПОВОД, 29.08.2026. Ночь применила вердикт третьего судьи по записи #350:

    ein Stück rücken   →   ein Stück Rücken

«подвинуться» стало «кусок спины». Правку предложил судья, наша проверка её
пропустила, третий судья согласился — три сигнала, и всё равно мимо. Владелец
посмотрел глазами и сказал: «верни как было».

ЧЕМ ВОЗВРАЩАЕТ. Ничего своего: тем же `apply_phrase_review_decision`, которым
правит и ночь, и кнопка на экране. Возврат — это такая же правка текста, только в
обратную сторону: заголовок, карточки людей, разбор, пул и кеш обязаны переехать
все вместе, а не только заголовок.

⚠ И СРАЗУ ЖЕ ЗАКРЫВАЕТ ФРАЗУ ОТ НОЧИ. Без этого возврат живёт одну ночь: ночная
проверка берёт в работу фразы, у которых НЕТ строки в `bt_3_phrase_check`
(`database.pick_phrases_for_grammar_check`), а применение решения эту строку как раз
удаляет. То есть следующей же ночью два судьи снова предложили бы «Rücken», сошлись
бы дословно, категория «rechtschreibung» стоит в `SILENT_CATEGORIES` — и правка
вернулась бы молча, поверх решения человека. Поэтому после возврата фраза
помечается проверенной: её проверил владелец, и это выше мнения модели.

    python3 scripts/phrase_revert_owner_call.py            # только посмотреть
    python3 scripts/phrase_revert_owner_call.py --apply    # вернуть
"""
import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend.database import (  # noqa: E402
    apply_phrase_review_decision, get_db_connection_context, phrase_check_text_hash,
)

# Что возвращаем: (номер записи проверки, каким текст должен стать снова).
# Список именной, а не выборка по правилу: это точечные решения владельца по
# конкретным фразам, а не класс, который можно описать условием.
ВОЗВРАТЫ = [
    (350, "ein Stück rücken", "«подвинуться» стало «кусок спины» (владелец, 29.08.2026)"),
    (346, "Zwei Schlaganfälle mit Anfang 30",
     "убранное «mit» меняет смысл: «в начале тридцати» → «начало 30» "
     "(владелец, 29.08.2026)"),
]


def _состояние(review_id: int) -> tuple[int, str, str]:
    """(номер слова, что в заголовке сейчас, статус записи проверки)."""
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT r.unit_id, u.lemma, r.status FROM bt_3_phrase_review r
                     JOIN bt_3_lex_units u ON u.id = r.unit_id WHERE r.id = %s;""",
                (int(review_id),))
            row = cur.fetchone()
    return (int(row[0]), str(row[1]), str(row[2])) if row else (0, "", "")


def вернуть(review_id: int, было: str) -> dict:
    """Новая запись проверки на текущий текст — и решение «вписать своё» по ней.

    Новая, а не правка старой: старая запись — след того, ЧТО произошло, и переписывать
    историю мы не станем. У возврата свой след, и в нём видно, кто его сделал.
    """
    unit_id, сейчас, _ = _состояние(review_id)
    if not unit_id:
        return {"пропущено": "записи нет"}
    if сейчас.strip() == было.strip():
        return {"пропущено": "уже вернули"}
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM bt_3_phrase_review WHERE unit_id=%s AND status='open';",
                        (unit_id,))
            if cur.fetchone():
                return {"пропущено": "по слову уже открыт вопрос"}
            cur.execute("""
                INSERT INTO bt_3_phrase_review (unit_id, text, translation, judges,
                                                status, kind)
                     SELECT %s, %s, COALESCE(r.translation,''), '[]'::jsonb, 'open', 'grammar'
                       FROM bt_3_phrase_review r WHERE r.id = %s
                  RETURNING id;""", (unit_id, сейчас, int(review_id)))
            новый = int(cur.fetchone()[0])
        conn.commit()

    итог = apply_phrase_review_decision(новый, "replace", было, 0, "")

    # Фраза проверена ЧЕЛОВЕКОМ — ночь её больше не берёт (см. шапку файла).
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO bt_3_phrase_check (unit_id, text_hash, verdict, checked_at)
                   VALUES (%s, %s, 'ok', NOW())
                   ON CONFLICT (unit_id) DO UPDATE
                     SET text_hash = EXCLUDED.text_hash, verdict = 'ok', checked_at = NOW();""",
                (итог.get("unit_id") or unit_id, phrase_check_text_hash(было)))
        conn.commit()
    return {"вопрос": новый, "стало": итог.get("text") or "",
            "разбор пересобран": bool(итог.get("breakdown_rebuilt"))}


def проверить(unit_id: int, ожидаем: str) -> str:
    """Сверка ПО ФАКТУ: что теперь лежит в заголовке и в карточках людей."""
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT lemma FROM bt_3_lex_units WHERE id=%s;", (unit_id,))
            строка = cur.fetchone()
            cur.execute("""SELECT id, user_id, word_de FROM bt_3_webapp_dictionary_queries
                            WHERE lex_unit_id=%s;""", (unit_id,))
            карточки = cur.fetchall() or []
            cur.execute("SELECT verdict FROM bt_3_phrase_check WHERE unit_id=%s;", (unit_id,))
            проверка = cur.fetchone()
    криво = [к for к in карточки if (к[2] or "").strip() != ожидаем.strip()]
    строки = [f"заголовок: {строка[0]!r}" if строка else "заголовка нет",
              f"карточек {len(карточки)}, с чужим текстом {len(криво)}",
              f"метка ночной проверки: {проверка[0] if проверка else 'НЕТ — ночь возьмёт снова'}"]
    return "; ".join(строки)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="вернуть, а не только смотреть")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    for review_id, было, почему in ВОЗВРАТЫ:
        unit_id, сейчас, статус = _состояние(review_id)
        print(f"\n#{review_id}  [{статус}]  {почему}")
        print(f"   сейчас в словаре : {сейчас!r}")
        print(f"   вернуть к        : {было!r}")
        if not args.apply:
            continue
        итог = вернуть(review_id, было)
        print(f"   → {итог}")
        print(f"   {проверить(unit_id, было)}")
    if not args.apply:
        print("\n(это только просмотр — вернуть: --apply)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
