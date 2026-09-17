"""
Ночной добор английской стороны: у немецкого слова появляется английский перевод.

ЗАЧЕМ. 16.09.2026 мост прошёл по всей базе разово: 19 398 связей немецкий→английский
там, где до этого было НОЛЬ. Но база растёт примерно на 64 немецкие единицы в сутки, и
без ночного добора копия отстанет за неделю. Плюс остаётся хвост: 3 037 единиц, у
которых русская сторона есть, а английской нет.

ЧТО ЭТА РАБОТА ДЕЛАЕТ И ЧЕГО НЕ ДЕЛАЕТ.
  · делает: заводит английскую единицу и связь немецкое→английское;
  · НЕ делает: не трогает немецкую и русскую стороны, ничего не удаляет и не понижает.
    Операция только ДОБАВЛЯЕТ. Проверено на полной копии прода 16.09.2026: все счётчики
    немецкой стороны до и после совпали.
  · НЕ обогащает английскую карточку. Формы, произношение, примеры — отдельная задача,
    и поля у неё ДРУГИЕ: у английского нет рода, артикля и падежей, зато есть фразовые
    глаголы, три формы неправильного глагола и исчисляемость. Просить у модели немецкие
    поля для английского слова — значит получить пустоту или выдумку.

⛔ КОГО НЕ ТРОГАЕМ. Слова и фразы, по которым человеку УЖЕ отправлено предложение
поправить и он ещё не ответил. Решение владельца 16.09.2026: перевести их сейчас значит
перевести текст, который через неделю изменится. Они получат английский следующей
ночью после того, как человек нажмёт «оставить» или «принять правку».
"""
from __future__ import annotations

import asyncio

from backend import llm_loop
import json
import logging
import os

ПАЧКА = 50
ПОТОЛОК = max(1, int((os.getenv("EN_BRIDGE_NIGHTLY_LIMIT") or "500").strip() or "500"))
ИСТОЧНИК = "мост de→en, ночь"

# ⚠ УСЛОВИЕ ОТБОРА ОДНО НА ДВА ЗАПРОСА — и выборку, и счётчик «сколько осталось».
# Разъедутся два списка условий — и отчёт начнёт врать про объём работы, а поймать это
# будет нечем. Поэтому условие живёт здесь, в одном месте, и подставляется в оба.
_УСЛОВИЕ = """
       u.lang = 'de'
       AND EXISTS (SELECT 1 FROM bt_3_lex_links l JOIN bt_3_lex_units r ON r.id = l.to_unit
                    WHERE l.from_unit = u.id AND r.lang = 'ru' AND l.rank < 900)
       AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links l JOIN bt_3_lex_units e ON e.id = l.to_unit
                        WHERE l.from_unit = u.id AND e.lang = 'en')
       AND u.id NOT IN (SELECT unit_id FROM bt_3_phrase_review
                         WHERE status = 'open' AND unit_id IS NOT NULL)
       AND u.id NOT IN (SELECT q.lex_unit_id FROM bt_3_user_word_review r
                          JOIN bt_3_webapp_dictionary_queries q ON q.id = r.entry_id
                         WHERE r.status = 'pending' AND q.lex_unit_id IS NOT NULL)
"""

# Кандидаты: немецкая единица, у которой ЕСТЬ русская сторона (есть от чего переводить)
# и НЕТ английской. Порядок — свежие первыми: вчерашнее слово человеку нужнее, чем
# то, что лежит с февраля.
_КАНДИДАТЫ = f"""
    SELECT u.id, u.kind, u.display,
           (SELECT string_agg(r.display, '; ' ORDER BY l.rank, r.display)
              FROM bt_3_lex_links l
              JOIN bt_3_lex_units r ON r.id = l.to_unit AND r.lang = 'ru'
             WHERE l.from_unit = u.id AND l.rank < 900) AS ru,
           (SELECT NULLIF(BTRIM(CONCAT_WS(' · ', s.label, s.note)), '')
              FROM bt_3_lex_senses s WHERE s.unit_id = u.id ORDER BY s.sense_no LIMIT 1) AS sense
      FROM bt_3_lex_units u
     WHERE {_УСЛОВИЕ}
     ORDER BY u.created_at DESC
     LIMIT %s
"""

_СКОЛЬКО = f"SELECT count(*) FROM bt_3_lex_units u WHERE {_УСЛОВИЕ}"


def _спросить(пачка: list[dict]) -> list[dict]:
    from backend.openai_manager import run_en_bridge
    return llm_loop.run(run_en_bridge(items=пачка))


def sweep(limit: int | None = None) -> dict:
    """Один ночной заход. Возвращает счётчики — их читает утренний отчёт."""
    from backend.database import get_db_connection_context
    from backend.lex_units import ensure_unit

    потолок = int(limit or ПОТОЛОК)
    итог = {"взято": 0, "переведено": 0, "связей": 0,
            "модель не знает": 0, "дверь единиц отказала": 0, "не легло": 0}

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(_КАНДИДАТЫ, (потолок,))
            кандидаты = [{"key": str(r[0]), "kind": r[1], "de": r[2], "ru": r[3], "sense": r[4]}
                         for r in (cur.fetchall() or [])]
    итог["взято"] = len(кандидаты)
    if not кандидаты:
        return итог

    ответы: dict[str, str] = {}
    for st in range(0, len(кандидаты), ПАЧКА):
        for о in _спросить(кандидаты[st:st + ПАЧКА]):
            en = str(о.get("en") or "").strip()
            if en:
                ответы[str(о.get("key"))] = en
            else:
                итог["модель не знает"] += 1
    итог["переведено"] = len(ответы)

    # Одна пара — одна строка: два значения слова часто дают один и тот же английский
    # перевод, и Postgres не разрешает тронуть строку дважды за команду.
    видели: set[tuple[int, int]] = set()
    for ключ, en in ответы.items():
        uid = int(ключ)
        eid = ensure_unit(en, "en")
        if not eid:
            итог["дверь единиц отказала"] += 1
            continue
        if (uid, eid) in видели:
            continue
        видели.add((uid, eid))
        try:
            with get_db_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO bt_3_lex_links (from_unit, to_unit, sense_id, rank, source)
                           VALUES (%s, %s, NULL, 1, %s)
                           ON CONFLICT (from_unit, to_unit)
                           DO UPDATE SET source = EXCLUDED.source, updated_at = NOW()""",
                        (uid, eid, ИСТОЧНИК),
                    )
                conn.commit()
            итог["связей"] += 1
        except Exception:
            # Молчания нет: причина уходит в лог целиком, счётчик растёт. Пустой
            # результат от ошибки обязан отличаться от пустого результата от «нечего».
            logging.warning("мост de→en: связь не легла для единицы %s", uid, exc_info=True)
            итог["не легло"] += 1

    logging.info("мост de→en за ночь: %s", итог)
    return итог


def сколько_осталось() -> int:
    """Сколько немецких единиц ещё без английской стороны. Для утреннего отчёта.

    Считаем ЧИСЛОМ, а не длиной выборки: тянуть тысячи строк ради счётчика — это
    лишняя работа для базы на каждом утреннем отчёте.
    """
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(_СКОЛЬКО)
            row = cur.fetchone()
            return int(row[0]) if row else 0
