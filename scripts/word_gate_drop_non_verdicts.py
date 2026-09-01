# -*- coding: utf-8 -*-
"""Убрать из тетрадки двери слова строки, которые НЕ ЯВЛЯЮТСЯ ВЕРДИКТАМИ.

Классов два, и оба — не «плохие ответы», а строки, которых там быть не должно:

    1. ВОПРОС НЕ К ЭТОЙ ДВЕРИ — спрашивали про фразу или предложение;
    2. ОТВЕТА НЕ БЫЛО ВОВСЕ — источник молчал, ответы модели разошлись.

Повод — жалоба владельца 01.09.2026: «Приходят слова и пишут, что модель не знает таких
слов, ну конечно не знает, потому что это ПРЕДЛОЖЕНИЯ. Зачем эти предложения попадаются
в разбор слов?!»

Что это за строки. `bt_3_word_check` — это НЕ словарь и не список слов человека, а
тетрадка одной проверки: «кого мы спрашивали у справочника и что он ответил». Владельцу
из неё уходит личка `backend/word_review.py` с кнопками «убрать из словаря / слово
настоящее». Строка вида

    asked  = 'Es löst Kopfschütteln aus.'
    status = 'не подтверждено'
    source = 'модель предложила другое написание, справочник не подтвердил'

вердиктом не является вовсе: это ответ на вопрос, которого мы не имели права задавать —
«существует ли в немецком слово „Es löst Kopfschütteln aus.“». Владелец 01.09.2026:
«зачем мы будем обманывать, писать проверено, если оно не проверено». Поэтому строки
именно УДАЛЯЮТСЯ, а не помечаются разобранными.

ВТОРОЙ КЛАСС: «ОТВЕТА НЕ БЫЛО». Источники `ответы разошлись`, `справочник молчал`,
`второй справочник молчал`, `не спрашивали справочник`, `модель не спрашивали` означают
ровно одно — мы НЕ УЗНАЛИ. Сама дверь такое не кеширует (`german_word_gate._is_final`),
но ночное переписывание приговоров клало это в таблицу в обход, да ещё и со снятой
отметкой `reviewed`: отвеченный владельцем вопрос приходил заново. Дырка закрыта
01.09.2026 (`word_gate_apply.переспросить`), а эта уборка снимает уже накопленное.

У такой строки беда двойная: она врёт вердиктом, которого не было, И одновременно
запирает слово — ночной проход `warm_word_gate` берёт только те слова, которых в тетрадке
ЕЩЁ НЕТ (`NOT EXISTS`). Пока строка лежит, слово не переспросят никогда. Снятие строки
возвращает слово в обычный ночной проход за настоящим ответом.

ЧТО ОСТАЁТСЯ НА МЕСТЕ. Сами фразы не трогаются ничем: запись словаря (`bt_3_lex_units`),
карточка человека, перевод, разбор, очередь спорных фраз. Грамматику фразы проверяет свой
механизм — `backend/phrase_night_check.py` → `bt_3_phrase_check` → `bt_3_phrase_review` →
экран спорных фраз. Он про эту тетрадку не знает и в неё не смотрит (проверено grep'ом
01.09.2026: `bt_3_word_check.reviewed` читается ровно в одном месте —
`german_word_gate.words_awaiting_owner`).

ВЕРНУТЬСЯ ОНИ НЕ МОГУТ. Сторож стоит в самой двери (`german_word_gate.check_word`,
01.09.2026): многословный вопрос отклоняется ДО справочника и модели и в кеш не пишется.
Ночной проход `warm_word_gate` фразы не берёт по построению (`kind='word'` +
`position(' ' in lemma)=0`), так что удалённая строка не заведётся заново.

Правило отбора берётся ИЗ ПРОДУКТА (`german_word_gate.is_single_word`), а не пишется
здесь заново — иначе отчёт разойдётся с дверью.

    python3 scripts/word_gate_drop_non_verdicts.py            # только показать и выгрузить
    python3 scripts/word_gate_drop_non_verdicts.py --apply    # выгрузить и удалить
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context          # noqa: E402
from backend.german_word_gate import is_single_word             # noqa: E402

СНИМОК = "scripts/data/word_gate_non_verdicts_dropped.jsonl"

# Источники, означающие «мы не узнали». Список — тот же, что у самой двери
# (`german_word_gate._is_final`) и у ночного переписывания приговоров: три копии одного
# списка разъезжаются, поэтому если он меняется — меняется во всех трёх сразу.
НЕ_ОТВЕТ = ("ответы разошлись", "справочник молчал", "второй справочник молчал",
            "не спрашивали справочник", "модель не спрашивали")


def собрать() -> list[dict]:
    """Все строки тетрадки, которые вердиктами не являются."""
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT asked, text, status, pos, source, note, checked_at, "
                        "       reviewed, applied_at "
                        "  FROM bt_3_word_check ORDER BY checked_at DESC;")
            строки = cur.fetchall() or []
    лишние = []
    for asked, text, status, pos, source, note, checked_at, reviewed, applied_at in строки:
        if not is_single_word(asked):
            почему = "вопрос не к двери слова: это фраза"
        elif str(source or "") in НЕ_ОТВЕТ:
            почему = "ответа не было: " + str(source)
        else:
            continue
        лишние.append({"asked": asked, "text": text, "status": status, "pos": pos,
                       "source": source, "note": note, "почему": почему,
                       "checked_at": checked_at.isoformat() if checked_at else None,
                       "reviewed": bool(reviewed),
                       "applied_at": applied_at.isoformat() if applied_at else None})
    return лишние


def выгрузить(лишние: list[dict], *, удаляем: bool) -> str:
    """След обязателен: удаление в этом проекте не бывает бесследным.

    Сухой прогон тоже пишется — но помечается `удалено: false`, чтобы снимок не врал
    о том, чего не делали.
    """
    os.makedirs(os.path.dirname(СНИМОК), exist_ok=True)
    метка = datetime.now(timezone.utc).isoformat()
    with open(СНИМОК, "a", encoding="utf-8") as f:
        for строка in лишние:
            f.write(json.dumps({"когда": метка, "удалено": удаляем, **строка},
                               ensure_ascii=False) + "\n")
    return СНИМОК


def удалить(лишние: list[dict]) -> int:
    ключи = [с["asked"] for с in лишние]
    if not ключи:
        return 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM bt_3_word_check WHERE asked = ANY(%s);", (ключи,))
            снято = cur.rowcount or 0
        conn.commit()
    logging.info("дверь слова: снято %d строк, которые не про слово", снято)
    return снято


def main() -> int:
    парсер = argparse.ArgumentParser()
    парсер.add_argument("--apply", action="store_true", help="удалить, а не только показать")
    аргументы = парсер.parse_args()

    лишние = собрать()
    в_очереди = [с for с in лишние
                 if с["status"] in ("не слово", "не подтверждено") and not с["reviewed"]]
    print(f"не вердикты: {len(лишние)} строк, из них ждали владельца: {len(в_очереди)}")
    for с in лишние:
        метка = "→ владельцу" if с in в_очереди else "           "
        print(f"  {метка}  {с['почему'][:34]:<34} {с['asked'][:60]}")

    путь = выгрузить(лишние, удаляем=bool(аргументы.apply))
    print(f"\nвыгружено в {путь}")
    if not аргументы.apply:
        print("это сухой прогон. Удалить: --apply")
        return 0
    print(f"удалено строк: {удалить(лишние)}")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    raise SystemExit(main())
