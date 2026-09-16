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


def has_documented_plural(tables: dict) -> bool:
    """Есть ли у слова множественное число — ПО ФЛАГУ ИСТОЧНИКА, а не по догадке.

    В каждой таблице справочника напечатано прямо: `has_plural` и `has_singular`. Это
    не наш вывод, а пустая клетка в самой статье Wiktionary. Флаги есть у 89 670 записей
    из 89 709 (замер 25.08.2026); там, где их нет, смотрим на сами формы.

    ⚠ ЧЕГО ЭТА ФУНКЦИЯ БОЛЬШЕ НЕ ДЕЛАЕТ И ПОЧЕМУ.
    До 25.08.2026 отсюда делался вывод «нет множественного — похоже на ИМЯ СОБСТВЕННОЕ»,
    и по нему отсекался артикль. Вывод неверен, и цена ошибки измерена: без
    множественного 12 050 слов, а не горстка. Среди них обычные существительные —
    «Milch», «Plastikmüll», «Bürgertum», «Brachialgewalt», «Jünglingsalter»,
    «Interkostalmuskulatur». Имён собственных среди них меньшинство.

    «Нет множественного» значит ровно одно: у слова нет множественного. Про «Athen» это
    верно, и про «Milch» тоже — но «die Milch» существует, а «das Athen» нет. Разницу
    решает не число, а часть речи, и признак для неё отдельный: поимённая пометка
    «имя собственное — артикль не ставится» в bt_3_field_checks (решение владельца
    24.08.2026). Косвенный признак заменён прямым — соседняя сессия (agent/dver,
    владелец таблиц склонения) назвала флаги и число 25.08.2026.
    """
    if not isinstance(tables, dict):
        return False
    for gender in tables:
        if gender not in _GENDER_TO_ARTICLE:
            continue
        block = tables[gender] or {}
        flag = block.get("has_plural")
        if isinstance(flag, bool):
            if flag:
                return True
            continue
        # Флага нет (39 записей из 89 709) — смотрим на сами формы.
        for row in block.get("rows") or []:
            if str(row.get("plural") or "").strip():
                return True
    return False


def has_documented_singular(tables: dict) -> bool:
    """Есть ли у слова единственное число — по тому же флагу источника.

    Нужен там, где важно не спутать слово с формой множественного: у plurale tantum
    («Badesachen», «Eltern») единственного нет вовсе, и артикль у них всегда «die».
    Замер 25.08.2026: таких 134 записи.
    """
    if not isinstance(tables, dict):
        return False
    for gender in tables:
        if gender not in _GENDER_TO_ARTICLE:
            continue
        block = tables[gender] or {}
        flag = block.get("has_singular")
        if isinstance(flag, bool):
            if flag:
                return True
            continue
        for row in block.get("rows") or []:
            if str(row.get("singular") or "").strip():
                return True
    return False


def declension_facts(word: str) -> tuple:
    """(артикль, источник|причина, есть ли множественное) — для тех, кому мало артикля."""
    text = str(word or "").strip()
    if not text:
        return (None, "пустое слово", False)
    from backend.database import get_db_connection_context

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT tables FROM bt_3_german_noun_declensions WHERE lower(noun) = %s LIMIT 1",
                (text.casefold(),),
            )
            row = cursor.fetchone()
    if not row or not row[0]:
        return (None, "справочник склонений не знает слова", False)
    article, source = article_from_declension_tables(text, row[0])
    return (article, source, has_documented_plural(row[0]))


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


# ── УКАЗАТЕЛЬ МНОЖЕСТВЕННОГО: тот же справочник, прочитанный с другого конца ──────────
#
# ПОВОД, 16.09.2026. Заголовки «Forderungen des Gläubigers», «Regeln der
# Satzzeichensetzung», «Teile des Geländes» оставались без артикля, и причина была не в
# том, что источник их не знает, а в том, что мы спрашивали его НЕ С ТОЙ СТОРОНЫ.
# `article_from_declension_reference` ищет по ключу `noun`, то есть по единственному
# числу, и на «Forderungen» не находит ничего. При этом форма «die Forderungen»
# НАПЕЧАТАНА в таблице слова «Forderung» — строкой именительного падежа.
#
# ЗАЧЕМ ЭТО ВООБЩЕ РАБОТАЕТ. У множественного числа в немецком определённый артикль
# ВСЕГДА «die», независимо от рода: die Forderungen (f), die Teile (n), die Protokolle (n).
# Поэтому вопрос к источнику здесь не «какой артикль», а «это форма множественного или
# нет», — и ответ на него напечатан. Ничего не выводится ни из окончания, ни из рода.
#
# Замер 16.09.2026 по живой базе: из 12 заголовков, на которых справочник молчал, этот
# конец закрывает 8, и НИ ОДНОГО спорного — ни одно из восьми не оказалось ещё и чьим-то
# именительным единственного.
#
# ┌─ ПРОВЕРЕНО 16.09.2026. НЕ ПРЕДЛАГАТЬ СЮДА `article_authority.authoritative_article`. ─┐
# │ Он выглядит как «источник побольше» (выгрузка Wiktionary + банк + живой Wiktionary),  │
# │ и соблазн добавить его в эту лестницу возникает сам собой. Замер: на тех же 12 словах │
# │ он добавляет РОВНО ОДНО — «Vorsitzender → der» — и это единственное слово даёт        │
# │ НЕВЕРНЫЙ немецкий: «der Vorsitzender des Vorstands» вместо «der Vorsitzende des       │
# │ Vorstands». Vorsitzender — субстантивированное прилагательное, при определённом       │
# │ артикле окончание обязано смениться с сильного на слабое. Это тот же класс, что       │
# │ «das Adriatisches Meer» (жалоба владельца 22.08.2026).                                │
# │ ПРИЧИНА РАЗНИЦЫ: здешние функции отвечают, только если написание СОВПАЛО с            │
# │ напечатанным именительным; authoritative_article такой проверки не делает — он        │
# │ отвечает про слово, а не про эту его форму. Чистый итог добавления: +1 ошибка, 0      │
# │ пользы. Перемерить: scripts/... или ladder-прогон по «не знаем» из                    │
# │ backend/genitive_phrase_article.sweep_missing_genitive_articles(dry_run=True).        │
# └──────────────────────────────────────────────────────────────────────────────────────┘

PLURAL_SOURCE_NAME = "справочник склонений: форма множественного"

# Именительный падеж напечатан вместе с артиклем — «die Forderungen». Берём последнее
# слово строки, как это делает _nominative_singular, только средствами Postgres, потому
# что искать надо ПО ВСЕМ 89 704 таблицам, а не по одной уже прочитанной.
_REVERSE_PLURAL_SQL = """
SELECT lower(regexp_replace(btrim(r.value ->> 'plural'), '^.*\\s', '')) AS plural_key,
       d.noun,
       g.key AS gender,
       lower(regexp_replace(btrim(COALESCE(r.value ->> 'singular', '')), '^.*\\s', '')) AS singular_key
  FROM bt_3_german_noun_declensions d
  CROSS JOIN LATERAL jsonb_each(d.tables) AS g(key, value)
  CROSS JOIN LATERAL jsonb_array_elements(g.value -> 'rows') AS r(value)
 WHERE g.key IN ('m', 'f', 'n')
   AND lower(r.value ->> 'case') = 'nom'
   AND lower(regexp_replace(btrim(COALESCE(r.value ->> 'plural', '')), '^.*\\s', '')) = ANY(%s)
"""

# Та же проверка с другой стороны: не является ли это написание ещё и чьим-то
# ИМЕНИТЕЛЬНЫМ ЕДИНСТВЕННОГО. Если да — прочтений два, и выбирать за человека нельзя.
_ALSO_SINGULAR_SQL = """
SELECT DISTINCT lower(regexp_replace(btrim(r.value ->> 'singular'), '^.*\\s', '')) AS key
  FROM bt_3_german_noun_declensions d
  CROSS JOIN LATERAL jsonb_each(d.tables) AS g(key, value)
  CROSS JOIN LATERAL jsonb_array_elements(g.value -> 'rows') AS r(value)
 WHERE g.key IN ('m', 'f', 'n')
   AND lower(r.value ->> 'case') = 'nom'
   AND lower(regexp_replace(btrim(COALESCE(r.value ->> 'singular', '')), '^.*\\s', '')) = ANY(%s)
"""


def plural_verdict(word: str, *, plural_of: list, also_singular: bool) -> tuple:
    """Вердикт по УЖЕ прочитанным данным — чтобы правило проверялось тестом без базы.

    (артикль, источник) либо ("", причина). Три исхода, и все три из живых данных:
      • написание числится чьим-то напечатанным множественным и больше ничьим
        единственным — артикль «die», сомнений нет;
      • числится и тем и другим — ДВА законных прочтения, выбирает человек, не мы
        (правило владельца 26.08.2026 «не решаем за пользователя»);
      • не числится множественным вовсе — не знаем, и это отдельный исход.
    """
    if not str(word or "").strip():
        return ("", "пустое слово")
    if not plural_of:
        return ("", "справочник склонений не знает этого написания как множественное")
    if also_singular:
        return ("", "два прочтения: и множественное, и единственное — выбирает человек")
    откуда = ", ".join(sorted({str(n) for n, _g in plural_of})[:3])
    return ("die", f"{PLURAL_SOURCE_NAME} от «{откуда}»")


def plural_articles_from_declension_reference(words) -> dict:
    """{слово: (артикль|"", источник|причина)} ПАЧКОЙ — два запроса на любой список."""
    wanted = [str(w or "").strip() for w in (words or []) if str(w or "").strip()]
    if not wanted:
        return {}
    keys = sorted({w.casefold() for w in wanted})
    from backend.database import get_db_connection_context

    множественные: dict[str, list] = {}
    единственные: set[str] = set()
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(_REVERSE_PLURAL_SQL, (keys,))
            for plural_key, noun, gender, _singular_key in cursor.fetchall() or []:
                множественные.setdefault(plural_key, []).append((noun, gender))
            cursor.execute(_ALSO_SINGULAR_SQL, (keys,))
            единственные = {row[0] for row in cursor.fetchall() or []}
    return {
        word: plural_verdict(word,
                             plural_of=множественные.get(word.casefold()) or [],
                             also_singular=word.casefold() in единственные)
        for word in wanted
    }


def plural_article_from_declension_reference(word: str) -> tuple:
    """То же про одно слово."""
    return plural_articles_from_declension_reference([word]).get(
        str(word or "").strip(), ("", "пустое слово"))
