# -*- coding: utf-8 -*-
"""DWDS — второй печатный справочник немецкого. Спрашивается, когда молчит первый.

ЗАЧЕМ ОН ПОЯВИЛСЯ
─────────────────
Дверь слова сверяет каждое сохранённое слово с de.wiktionary. Замер 21.08.2026 на живом
словаре владельца показал, чем это оборачивается для человека: из 12 слов, ушедших ему
на проверку, 8 оказались обычными немецкими словами, которых просто НЕТ в Wiktionary —
«Vergleichbarkeit», «Arbeitsumfeld», «Sozialschmarotzer», «Gurkenhobel», «aufkeilen».
Страниц у них там действительно нет, дверь не ошибалась. Ошибался охват источника.

Экран проверки при этом удаляет всё, что человек не отметил. То есть один неполный
справочник превращал хорошие слова в предложение их стереть.

Так эту задачу решают все словари: не одним корпусом, а несколькими. DWDS (Berlin-
Brandenburgische Akademie der Wissenschaften) — академический словарь современного
немецкого, он ЗНАЕТ 5 из тех 8 слов и при этом честно НЕ знает ни одного обрубка
(«Abschiebu», «Scheinwerfergla») и ни одного англицизма («Ragebait», «Sweatpants»).
То есть он режет шум, не пропуская мусор.

ПОЧЕМУ ЕМУ МОЖНО ВЕРИТЬ КАК ИСТОЧНИКУ
──────────────────────────────────────
Ответ точный, а не «похожий»: проверено 21.08.2026 — на «Vergleichbarkeitt», «Katzee»,
«Häuser», «gelaufen», «betäubung» API возвращает ПУСТО, а не ближайшее слово. Никакого
fuzzy-подбора, за который можно было бы принять догадку. Часть речи приходит той же
словарной разметкой, что у Wiktionary («Substantiv», «Verb», «Adjektiv»).

Молчание DWDS (сеть, таймаут, отказ) возвращается как None и означает «не спросили»,
а не «слова нет». Пустой список от самого DWDS — это ответ «не знаю такого», и он
означает ровно это: не приговор, а отсутствие подтверждения.
"""
from __future__ import annotations

import json
import logging
import urllib.parse
import urllib.request

API = "https://www.dwds.de/api/wb/snippet?q={query}"
UA = "DeutscheSprache/1.0 (Sprachlern-App; Wortpruefung)"
TIMEOUT = 12


def dwds_pos(word: str) -> str | None:
    """Часть речи по DWDS или '' если словарь слова не знает. None — не спросили.

    Три разных исхода тремя разными значениями: перепутать «не знаю» с «нет такого»
    здесь нельзя — от этого зависит, увидит ли человек предложение удалить своё слово.
    """
    text = str(word or "").strip()
    if not text:
        return ""
    request = urllib.request.Request(
        API.format(query=urllib.parse.quote(text)), headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        logging.debug("DWDS не ответил про %s", text, exc_info=True)
        return None
    if not isinstance(payload, list):
        logging.warning("DWDS ответил не списком про %s: %r", text, str(payload)[:200])
        return None
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        # Только ТОЧНОЕ совпадение заголовка. DWDS сам похожего не подсовывает, но
        # сравнение стоит здесь навсегда: источник вправе поменять поведение, а
        # правило «похожее вместо точного» в этом приложении запрещено.
        if str(entry.get("lemma") or "").strip() == text:
            return str(entry.get("wortart") or "").strip()
    return ""


def dwds_says_about_all(words: list[str]) -> dict[str, str] | None:
    """{написание: часть речи} по тем словам, которые DWDS знает.

    Пакетного запроса у DWDS нет, поэтому спрашиваем по одному — но список короткий
    (слово и варианты починки), и до него доходит только то, чего не знает Wiktionary.
    Как только первый кандидат подтвердился, остальные не спрашиваются.

    None означает «DWDS молчал» — ни одного ответа не получено.
    """
    out: dict[str, str] = {}
    answered = False
    for word in words or []:
        pos = dwds_pos(word)
        if pos is None:
            continue
        answered = True
        if pos:
            out[word] = pos
            break
    return out if answered else None
