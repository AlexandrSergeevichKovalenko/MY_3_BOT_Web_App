# -*- coding: utf-8 -*-
"""СУДЬЕ ПЕРЕВОДА ДАЁТСЯ СЛОВАРНАЯ СТАТЬЯ, А НЕ ТОЛЬКО ДВЕ СТРОКИ.

ЧТО БЫЛО СЛОМАНО. Судья перевода (`openai_manager.run_translation_pair_check`) получал
ровно две строки: немецкую и русскую. Всё остальное он брал из памяти модели — и
ошибался предсказуемо. Разбор ВСЕХ 46 открытых вопросов 16.09.2026, каждая спорная
претензия проверена по словарю: 24 справедливы, 12 ложны, 8 — придирки к стилю,
2 не проверены. Двенадцать ложных распадаются на три приёма, и два из них лечатся
словарём:

  • ЗНАЕТ ОДНО ЗНАЧЕНИЕ, ОТРИЦАЕТ ВТОРОЕ — 5 случаев. «Laster означает порок, а не
    грузовик» (в DWDS оба), «Police — не полиция» (верно, но перевод «полис» он
    забраковал), festsetzen, auslösen, Kunde;
  • ПУТАЕТ ПАДЕЖНУЮ РАМКУ — «Ich kündige die Stelle» объявлено «увольняю кого-то»,
    хотя «jemandem kündigen» (дательный, лицо) и «etwas kündigen» (винительный, вещь) —
    разные вещи, и у нас второе;
  • НЕ ВИДИТ ОТРИЦАНИЯ ИЛИ РАВЕНСТВА — словарём не лечится, остаётся как есть.

ПОЧЕМУ ИМЕННО de.wiktionary, А НЕ DWDS. У DWDS в открытом API (`german_reference_dwds`)
есть часть речи и ссылка, но НЕ толкования — их пришлось бы вынимать со страницы.
У Wiktionary толкования приходят готовым разделом `{{Bedeutungen}}`, и там же напечатана
падежная рамка: «[2] jemandem (umgangssprachlich: jemanden) kündigen: das
Arbeitsverhältnis eines Mitarbeiters einseitig auflösen». Механизм запроса в проекте уже
стоит и уже вежлив к сайту (`article_wiktionary_ref._fetch_wikitext`: пачки по 45,
отступ при 429/503).

ЧЕГО ЗДЕСЬ НЕТ. Мы не выводим лемму арифметикой. Глагол внутри фразы называет
СПРАВОЧНИК ФОРМ (`german_verb_paradigms.verbs_of_form` — таблицы со страниц Flexion), а
существительные берутся как напечатано, заглавной буквой. Не нашлось — справки просто
нет, и судья работает как раньше. Пустая справка — не ошибка и не приговор.
"""
from __future__ import annotations

import logging
import re

# Сколько слов из одной фразы спрашиваем. Три — потому что длинная справка вытесняет из
# внимания модели сам вопрос, а платим мы за каждый лишний токен в каждом запросе.
СЛОВ_НА_ФРАЗУ = 3
# Одна статья длиннее этого обрезается: у частотных слов Bedeutungen занимает экран.
ЗНАКОВ_НА_СТАТЬЮ = 700

_БЛОК = re.compile(r"\{\{Bedeutungen\}\}(.*?)(?=\n\{\{[A-ZÄÖÜ])", re.S)
_ССЫЛКА = re.compile(r"\[\[([^\]|]+)\|([^\]]+)\]\]")
_ССЫЛКА_ПРОСТАЯ = re.compile(r"\[\[([^\]]+)\]\]")
_ШАБЛОН_ПОМЕТЫ = re.compile(r"\{\{K\|([^}]*)\}\}")
# Служебные параметры шаблона помет (t1=;, ft=…, spr=…) — это разметка, а не текст
# словаря: в справке они выглядят мусором и занимают место настоящих значений.
_ПАРАМЕТР = re.compile(r"\b[a-z]{1,3}\d?=[^,;]*")
_ШАБЛОН_ЛЮБОЙ = re.compile(r"\{\{[^}]*\}\}")
_СНОСКА = re.compile(r"<ref[^>]*>.*?</ref>|<ref[^>]*/>", re.S)


def _очистить(викитекст: str) -> str:
    """Викиразметка → человеческий текст. Пометы (ugs., trans., veraltet) сохраняем:
    именно они говорят судье, что значение редкое, а не несуществующее."""
    t = _СНОСКА.sub("", викитекст)
    t = _ШАБЛОН_ПОМЕТЫ.sub(
        lambda m: _ПАРАМЕТР.sub("", m.group(1).replace("|", ", ")), t)
    t = _ШАБЛОН_ЛЮБОЙ.sub("", t)
    t = _ССЫЛКА.sub(r"\2", t)
    t = _ССЫЛКА_ПРОСТАЯ.sub(r"\1", t)
    t = t.replace("'''", "").replace("''", "")
    t = re.sub(r"[ ,;]{2,}", " ", t).replace(" ,", ",")
    строки = [s.strip(" :,;") for s in t.split("\n")]
    return "; ".join(s for s in строки if s)[:ЗНАКОВ_НА_СТАТЬЮ]


def _кеш_прочитать(слова: list[str]) -> dict[str, str]:
    """Что уже спрашивали. Толкования слова не меняются от ночи к ночи, а Wiktionary
    на пачке ночных вопросов отвечает 429 — проверено 16.09.2026 прямо на прогоне."""
    from backend.database import get_db_connection_context

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS bt_3_wiktionary_senses_cache (
                       title      TEXT PRIMARY KEY,
                       senses     TEXT NOT NULL,
                       checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                   );""")
            conn.commit()
            cursor.execute(
                "SELECT title, senses FROM bt_3_wiktionary_senses_cache "
                "WHERE title = ANY(%s);", (слова,))
            строки = cursor.fetchall() or []
    # Пустая строка в кеше — «спрашивали, статьи нет». Это ОТВЕТ, а не отсутствие
    # ответа: второй раз за ним не ходим.
    return {str(r[0]): str(r[1] or "") for r in строки}


def _кеш_записать(ответы: dict[str, str]) -> None:
    from backend.database import get_db_connection_context

    if not ответы:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            for title in sorted(ответы):      # один порядок блокировок — нет взаимоблокировки
                cursor.execute(
                    """INSERT INTO bt_3_wiktionary_senses_cache (title, senses)
                       VALUES (%s, %s)
                       ON CONFLICT (title) DO UPDATE
                         SET senses = EXCLUDED.senses, checked_at = NOW();""",
                    (title, ответы[title][:4000]))
        conn.commit()


def значения_слова(слова: list[str]) -> dict[str, str]:
    """Толкования из de.wiktionary для списка слов. Нет статьи — слова нет в ответе.

    Берутся ВСЕ блоки `{{Bedeutungen}}` немецкого раздела, а не первый: у «Laster» их
    два (das Laster — порок, der Laster — грузовик), и именно на этом судья ошибался."""
    from backend.article_wiktionary_ref import _fetch_wikitext, _german_section

    чистые = [w for w in dict.fromkeys(str(s or "").strip() for s in слова) if w]
    if not чистые:
        return {}
    try:
        из_кеша = _кеш_прочитать(чистые)
    except Exception:
        logging.debug("кеш словарной справки недоступен", exc_info=True)
        из_кеша = {}
    спросить = [w for w in чистые if w not in из_кеша]
    готовое = {w: t for w, t in из_кеша.items() if t}
    if not спросить:
        return готовое
    try:
        тексты = _fetch_wikitext(спросить)
    except Exception:
        # Словарь не ответил — это «не спросили», а не «значений нет». Судья получит
        # вопрос без справки, ровно как до 17.09.2026.
        logging.warning("словарная справка судье: Wiktionary не ответил", exc_info=True)
        return готовое
    # ⚠ ТРИ РАЗНЫХ ИСХОДА, И ПУТАТЬ ИХ НЕЛЬЗЯ (`_fetch_wikitext`):
    #   ключа нет в ответе  — НЕ СПРОСИЛИ (сеть, 429 после всех попыток). В кеш не
    #                         пишем: иначе молчание сети застынет как «статьи нет»;
    #   значение None       — словарь ОТВЕТИЛ «такой страницы нет». Это ответ, и он
    #                         кешируется пустой строкой: второй раз не ходим;
    #   викитекст           — статья есть.
    свежее: dict[str, str] = {}
    for слово in спросить:
        if слово not in (тексты or {}):
            continue
        текст = тексты[слово]
        куски = ([_очистить(b) for b in _БЛОК.findall(_german_section(текст))]
                 if текст else [])
        свежее[слово] = " // ".join(k for k in куски if k)[:ЗНАКОВ_НА_СТАТЬЮ]
    try:
        _кеш_записать(свежее)
    except Exception:
        logging.debug("кеш словарной справки не записался", exc_info=True)
    готовое.update({w: t for w, t in свежее.items() if t})
    return готовое


def слова_для_справки(текст: str) -> list[str]:
    """О каких словах фразы спрашивать словарь. Пусто — спрашивать не о чем.

    Одиночное слово — оно само. Во фразе: сначала глаголы, которых НАЗВАЛ справочник
    форм (не наша догадка о лемме), затем существительные — они напечатаны заглавной.
    Первое слово предложения заглавное всегда, поэтому как существительное оно берётся
    только если это не начало (иначе «Ich» и «Wir» тянули бы за собой статьи о
    местоимениях и съедали бы место под настоящие слова)."""
    слова = re.findall(r"[A-Za-zÄÖÜäöüß]+", str(текст or ""))
    if not слова:
        return []
    if len(слова) == 1:
        return слова[:1]

    from backend.german_verb_paradigms import verbs_of_form

    глаголы: list[str] = []
    for w in слова:
        if len(глаголы) >= СЛОВ_НА_ФРАЗУ:
            break
        try:
            найдено = verbs_of_form(w, limit=2)
        except Exception:
            logging.debug("справочник форм не ответил про %r", w, exc_info=True)
            найдено = []
        for лемма in найдено:
            if лемма not in глаголы:
                глаголы.append(лемма)
    существительные = [w for w in слова[1:]
                       if w[:1].isupper() and len(w) > 2 and w not in глаголы]
    return (глаголы + существительные)[:СЛОВ_НА_ФРАЗУ]


def справка_для_судьи(текст: str) -> dict:
    """Что показать судье про эту фразу и след о том, что мы спрашивали.

    Возвращает {"text": "<для промпта>", "lemmas": [...], "found": N}. `lemmas` и
    `found` кладутся в сам вопрос владельцу: молчащий механизм неотличим от сломанного,
    и через неделю иначе не ответить, доходила ли справка вообще."""
    слова = слова_для_справки(текст)
    if not слова:
        return {"text": "", "lemmas": [], "found": 0}
    найденное = значения_слова(слова)
    if not найденное:
        return {"text": "", "lemmas": слова, "found": 0}
    строки = [f"{слово}: {значения}" for слово, значения in найденное.items()]
    return {"text": "\n".join(строки), "lemmas": слова, "found": len(найденное)}
