# -*- coding: utf-8 -*-
"""Убрать из базы поддельные задания «Дополни предложение».

ПОВОД. Запасной путь строил неверные варианты ИЗ ТОГО ЖЕ ПРЕДЛОЖЕНИЯ, а когда их не
хватало — из шести вшитых в код глаголов. Замер 28.08.2026: 162 из 260 сохранённых
заданий (62%) собраны так. Пример из базы:

    «das ___ sich nicht»  →  sich · das · eignet · nicht

Все четыре варианта — слова этого же предложения. Решается без знания немецкого.

Сам источник убран (`_build_fallback_sentence_context_quiz` удалена), но уже
записанное так и лежит. Это вторая половина работы: почистить накопленное.

ЧТО ДЕЛАЕТ. Убирает ключ `sentence_gap_v2` у испорченных записей — само слово, разбор
и всё остальное не трогает. Задание пересоберётся честно, когда до этого слова дойдёт
живой человек или ночной прогрев.

ПРИЗНАК ПОДДЕЛКИ (осторожный, намеренно): два и более неверных варианта стоят в самом
предложении, ЛИБО два и более из шести вшитых глаголов. Один общий вариант бывает и у
честного задания — модель вправе взять слово из предложения как отвлекающее.

Запуск:  python3 scripts/purge_fake_sentence_quizzes.py [--apply]
Без --apply только считает и показывает примеры, ничего не меняя.
"""
import json
import re
import sys

sys.path.insert(0, ".")

from backend.database import get_db_connection_context  # noqa: E402

ВШИТЫЕ = {"gehen", "machen", "geben", "nehmen", "stellen", "tragen", "setzen", "legen"}


def поддельное(payload: dict) -> str | None:
    """Вернуть причину, если задание поддельное, иначе None."""
    опции = [str(o) for o in (payload.get("options") or [])]
    верное = str(payload.get("correct_word") or "")
    предложение = str(payload.get("correct_full_sentence") or "")
    if not опции or not предложение:
        return None
    неверные = [o for o in опции if o != верное]
    слова = set(re.findall(r"[A-Za-zÄÖÜäöüß]+", предложение))
    из_предложения = sum(1 for o in неверные if o in слова)
    вшитых = sum(1 for o in неверные if o.lower() in ВШИТЫЕ)
    if из_предложения >= 2:
        return f"{из_предложения} неверных варианта из этого же предложения"
    if вшитых >= 2:
        return f"{вшитых} неверных варианта из вшитых глаголов"
    return None


def main() -> int:
    применять = "--apply" in sys.argv
    негодные: list[tuple[int, str, str]] = []
    всего = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, response_json FROM bt_3_dictionary_entries "
                        "WHERE response_json ? 'sentence_gap_v2';")
            for id_, rj in cur.fetchall():
                d = rj if isinstance(rj, dict) else (json.loads(rj) if rj else {})
                payload = ((d or {}).get("sentence_gap_v2") or {}).get("payload") or {}
                if not payload:
                    continue
                всего += 1
                причина = поддельное(payload)
                if причина:
                    негодные.append((int(id_), str(payload.get("sentence_with_gap") or ""), причина))

    print(f"заданий с пропуском в базе: {всего}")
    print(f"из них поддельных:          {len(негодные)}")
    for id_, пропуск, причина in негодные[:10]:
        print(f"   id={id_}  «{пропуск[:56]}»  — {причина}")
    if len(негодные) > 10:
        print(f"   …и ещё {len(негодные) - 10}")

    if not негодные:
        print("\nЧистить нечего.")
        return 0
    if not применять:
        print("\nЭто пробный прогон. Ничего не изменено. Для уборки: --apply")
        return 0

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bt_3_dictionary_entries "
                "SET response_json = response_json - 'sentence_gap_v2' "
                "WHERE id = ANY(%s);",
                ([i for i, _, _ in негодные],),
            )
            убрано = cur.rowcount
        conn.commit()
    print(f"\nУбрано заданий: {убрано}. Слова, разборы и всё остальное не тронуты.")
    print("Задания пересоберутся честно, когда до них дойдёт ночной прогрев или человек.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
