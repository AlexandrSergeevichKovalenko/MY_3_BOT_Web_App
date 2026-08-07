# -*- coding: utf-8 -*-
"""Вычитка слова перед тем, как платить за разбор. Общая для веба и бота.

Зачем отдельный модуль. Дверь стояла только в веб-приложении, а самый большой вход —
поиск в личном чате бота (699 сохранений из 1786 за месяц, 39%) — шёл в модель напрямую.
Ярлык идёт тем же путём, значит и он был не прикрыт. Держать вторую копию правил в
`bot_3.py` нельзя: разойдутся, и «одна дверь входа» перестанет быть одной.

Денежное правило то же, что в вебе: вычитку зовём ТОЛЬКО после того, как наш собственный
словарь ответить не смог. Слово, которое у нас уже есть, верное по определению —
спрашивать не о чем. А на промахе дешёвая вычитка идёт перед дорогим разбором, который
мы всё равно собираемся купить.

Модуль намеренно не знает ни про базу, ни про биллинг: чем спрашивать «знаем ли мы это
слово» и куда писать расход, решает вызывающая сторона. Здесь только правила и кеш.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable

from backend.dictionary_intake import clean_text, worth_language_check

# Кеш на процесс: один и тот же промах повторяется часто (человек набирает слово, потом
# сохраняет его — это два обращения к одной и той же строке). Держим и ответ «ошибок
# нет»: без него молчание корректора стоило бы денег каждый раз.
_CACHE: dict[str, tuple[float, str]] = {}
_CACHE_LOCK = threading.Lock()
_CACHE_TTL_SEC = 6 * 60 * 60
_CACHE_MAX_ITEMS = 5000


def _cache_key(text: str, lang: str) -> str:
    return f"{lang}|{text}"


def _cached(key: str) -> str | None:
    now = time.time()
    with _CACHE_LOCK:
        row = _CACHE.get(key)
        if not row:
            return None
        if row[0] <= now:
            _CACHE.pop(key, None)
            return None
        return row[1]


def _remember(key: str, value: str) -> None:
    with _CACHE_LOCK:
        _CACHE[key] = (time.time() + _CACHE_TTL_SEC, value)
        if len(_CACHE) > _CACHE_MAX_ITEMS:
            overflow = len(_CACHE) - _CACHE_MAX_ITEMS
            for stale in sorted(_CACHE, key=lambda k: _CACHE[k][0])[:overflow]:
                _CACHE.pop(stale, None)


def proofread(text: str, *, lang: str, on_usage: Callable[[], None] | None = None) -> str:
    """Исправленное написание, или "" когда править нечего либо что-то не заладилось.

    Никогда не бросает и никогда не задерживает вызывающего дольше своего таймаута:
    сохранение и поиск не имеют права падать из-за вычитки. `on_usage` зовётся ровно
    тогда, когда обращение к модели СОСТОЯЛОСЬ, — чтобы вызывающая сторона записала
    расход. Из кеша ответ отдаётся молча: денег он не стоил."""
    value = clean_text(text)
    code = str(lang or "").strip().lower()
    if not worth_language_check(value, code):
        return ""
    key = _cache_key(value, code)
    hit = _cached(key)
    if hit is not None:
        return hit
    try:
        from backend.openai_manager import run_quick_correct
        corrected = clean_text(run_quick_correct(text=value, source_lang=code) or "")
    except Exception:
        logging.warning("дверь словаря: корректор не ответил на %r", value[:60], exc_info=True)
        return ""
    if on_usage is not None:
        try:
            on_usage()
        except Exception:
            logging.debug("дверь словаря: запись расхода не удалась", exc_info=True)
    if corrected == value:
        corrected = ""
    _remember(key, corrected)
    return corrected


def word_to_look_up(
    word: str,
    *,
    lang: str,
    is_known: Callable[[str], bool] | None = None,
    on_usage: Callable[[], None] | None = None,
) -> str:
    """С каким написанием идти дальше: наш словарь → промах → вычитка → это написание.

    Возвращает ВСЕГДА непустую строку (в худшем случае — то, что набрал человек), чтобы
    вызывающему не приходилось думать о пустом значении. Если наш словарь слово знает,
    корректор не зовётся вовсе — и денег не стоит."""
    value = clean_text(word)
    if not value:
        return str(word or "").strip()
    if is_known is not None:
        try:
            if is_known(value):
                return value
        except Exception:
            logging.debug("дверь словаря: проверка «знаем ли слово» не удалась", exc_info=True)
    corrected = proofread(value, lang=lang, on_usage=on_usage)
    if corrected and corrected != value:
        logging.info("дверь словаря: вход %r исправлен на %r перед разбором",
                     value[:60], corrected[:60])
        return corrected
    return value
