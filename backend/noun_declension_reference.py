# -*- coding: utf-8 -*-
"""Род существительного из НАПЕЧАТАННОЙ таблицы склонения — один читатель на всех.

Зачем отдельный модуль
──────────────────────
23.08.2026 в `bt_3_german_noun_declensions` легла офлайн-выгрузка на 87 тысяч
существительных: было 2 909 таблиц, стало 89 704. В каждой таблице именительный падеж
единственного числа уже написан ВМЕСТЕ с артиклем — «die Ratte», «der Realkredit».
Это готовый источник рода, который не надо ни выводить арифметикой, ни спрашивать по сети.

Мест, которым он нужен, сразу два: заслон колоды утренней ленты
(`backend/daily_video_quality.py`) и сборка заголовка общего словаря
(`backend/database.py`, `search_dictionary_pool`). Комментарий в
`daily_video_quality.py` сам жалуется на то, как это кончается: «проверки были рассыпаны
по генератору, судье и тестам… одно правило жило в трёх местах и в двух из них
устаревало». Поэтому правило читается ЗДЕСЬ, один раз, а вызывающие только спрашивают.

Правило ноль: ответ берётся из источника. Источник называется вслух — таблица
`bt_3_german_noun_declensions`, и он же возвращается вторым значением, чтобы в отчёте
владельцу было написано, откуда взялся артикль.

Чего этот модуль НЕ делает
──────────────────────────
Не угадывает. Не выводит род по окончанию, по шву композита, по чему бы то ни было.
Не знает слова — говорит «не знаю», и это не повод подставить «der».
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Ключ рода в таблице → определённый артикль именительного единственного.
_GENDER_TO_ARTICLE = {"m": "der", "f": "die", "n": "das"}

SOURCE_NAME = "справочник склонений"


def _nominative_singular(table: dict) -> str:
    """Именительный единственного из таблицы одного рода: «die Ratte» → «Ratte»."""
    rows = (table or {}).get("rows") or []
    for row in rows:
        if str(row.get("case") or "").lower() == "nom":
            singular = str(row.get("singular") or "").strip()
            if singular:
                # «die Ratte» → «Ratte». Артикль отделяется по пробелу, а не разбором.
                parts = singular.split()
                return parts[-1] if parts else ""
    return ""


def article_from_declension_tables(word: str, tables: dict) -> tuple:
    """Артикль по УЖЕ прочитанным таблицам. Вынесено отдельно, чтобы можно было
    проверить правило тестом, не поднимая базу.

    Возвращает (артикль, источник) или (None, причина).

    Три случая, когда мы отказываемся отвечать, и все три — из живых данных 24.08.2026:

    • **Таблиц нет / рода в них нет.** Отвечать нечем.
    • **Родов несколько.** «der/das Liter» — два законных рода. Выбрать один значило бы
      угадать; правило ноль это запрещает прямо.
    • **Слово не совпало с именительным единственного.** Ровно так ловится форма
      множественного: у «Türen» таблица найдётся по ключу «tür», и её артикль «die»
      относится к единственному числу «die Tür», а не к тому, что стоит в заголовке.
      Проверено на живых данных: из 306 слов пула с известным родом 13 оказались формой
      множественного или обрезком — «Türen», «Bücher», «Ausprägungen», «Aufwandsarten».
      Без этой проверки им приклеился бы артикль единственного числа.
    """
    text = str(word or "").strip()
    if not text or not isinstance(tables, dict):
        return (None, "справочник склонений не знает слова")
    genders = [g for g in tables if g in _GENDER_TO_ARTICLE]
    if not genders:
        return (None, "справочник склонений не знает слова")
    if len(genders) > 1:
        found = "/".join(sorted(_GENDER_TO_ARTICLE[g] for g in genders))
        return (None, f"у слова несколько родов ({found}) — выбирать не наше дело")
    gender = genders[0]
    nominative = _nominative_singular(tables[gender])
    if not nominative:
        return (None, "в таблице нет именительного единственного")
    if nominative.casefold() != text.casefold():
        return (None,
                f"заголовок «{text}» — не именительный единственного "
                f"(справочник склоняет «{nominative}»)")
    return (_GENDER_TO_ARTICLE[gender], SOURCE_NAME)


def article_from_declension_reference(word: str) -> tuple:
    """Спросить справочник склонений про одно слово. (артикль, источник) либо (None, причина).

    Ключ таблицы записан вразнобой: старые 2 909 строк лежат с заглавной («Realkredit»),
    вчерашняя выгрузка на 87 тысяч — со строчной («übernachtungsmöglichkeit»). Поэтому
    ищем по `lower(noun)`, иначе половина справочника невидима (поймано 24.08.2026:
    поиск по «Abflughalle» не находил ничего, поиск по «abflughalle» находил).
    """
    text = str(word or "").strip()
    if not text:
        return (None, "пустое слово")
    from backend.database import get_db_connection_context

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT tables FROM bt_3_german_noun_declensions WHERE lower(noun) = %s LIMIT 1",
                (text.casefold(),),
            )
            row = cursor.fetchone()
    if not row or not row[0]:
        return (None, "справочник склонений не знает слова")
    return article_from_declension_tables(text, row[0])


def articles_from_declension_reference(words) -> dict:
    """То же самое ПАЧКОЙ: {слово: (артикль, источник|причина)}.

    Нужно там, где слов сразу много и по одному ходить в базу нельзя — например при
    сборке выдачи общего словаря, где на один запрос человека приходится до сорока строк.
    Один запрос вместо сорока.
    """
    wanted = [str(w or "").strip() for w in (words or [])]
    wanted = [w for w in wanted if w]
    if not wanted:
        return {}
    keys = sorted({w.casefold() for w in wanted})
    from backend.database import get_db_connection_context

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT lower(noun), tables FROM bt_3_german_noun_declensions "
                "WHERE lower(noun) = ANY(%s)",
                (keys,),
            )
            found = {row[0]: row[1] for row in cursor.fetchall()}
    return {
        word: article_from_declension_tables(word, found.get(word.casefold()) or {})
        for word in wanted
    }
