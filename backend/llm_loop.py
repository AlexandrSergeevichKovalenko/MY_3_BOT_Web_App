# -*- coding: utf-8 -*-
"""Один живой цикл на весь процесс — для всех разговоров с моделью.

ОТКУДА ЗАДАЧА. Владелец 16.09.2026: «очень много перебоев… словарь часто не
работает». На экране это выглядело как «разбор пока недоступен» и как долгое
ожидание карточки.

ЧТО НАШЛОСЬ В ЛОГАХ ПРОДА (16.09.2026, окно 14:59–20:01 UTC, сервис BACKEND_WEB):

    17 × RuntimeError('Event loop is closed')
    dictionary_assistant_multilang_core_fast attempt 1/1 failed in 1ms
    длительность главного запроса: 5.4 5.7 6.6 7.9 10.3 11.2 11.4 12.6
                                   12.7 12.8 13.7 14.2 34.9 (секунды)

Быстрая половина разбора (бюджет 7 с, повтора у неё нет) умирала за 1–2 мс, и
человек ждал медленную: 9 запросов из 13 — дольше 10 секунд, один 35 секунд.

ПОЧЕМУ. Клиент OpenAI один на весь процесс (`openai_manager.client`) и держит
открытые TLS-соединения. А каждый веб-запрос открывал СВОЙ цикл событий через
`asyncio.run(...)` и закрывал его на выходе. Соединения из закрытого цикла
оставались лежать в общем клиенте, и следующий запрос об них спотыкался — падение
приходило из `httpcore`, когда пул пытался прибрать чужое соединение:

    File "httpcore2/_async/connection_pool.py", line 374, in _close_connections
    File "asyncio/base_events.py", line 515, in _check_closed
    RuntimeError: Event loop is closed

Это не сеть, не OpenAI и не лимиты: падение приходит за 1–2 мс, до выхода наружу.

РЕШЕНИЕ. Цикл событий живёт столько же, сколько процесс: отдельный поток-демон
крутит `loop.run_forever()`, а синхронный код (обработчики Flask, фоновые
исполнители, ночные актёры) отдаёт корутину сюда и ждёт ответа. Цикл один и тот же
всегда, поэтому соединения в пуле остаются ЖИВЫМИ — уходит не только падение, но и
рукопожатие TLS на каждый запрос.

ЧЕГО ЗДЕСЬ НЕТ. Никакого «не получилось — попробуем иначе»: ошибка модели или сети
поднимается наружу как есть, ровно как её поднимал `asyncio.run`. Этот модуль
меняет ТОЛЬКО то, в каком цикле выполняется корутина.
"""
from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any, Coroutine

_loop: asyncio.AbstractEventLoop | None = None
_thread: threading.Thread | None = None
_lock = threading.Lock()


def _spin(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


def get_loop() -> asyncio.AbstractEventLoop:
    """Общий цикл процесса. Поднимается при первом обращении и живёт до конца."""
    global _loop, _thread
    with _lock:
        if _loop is not None and not _loop.is_closed():
            return _loop
        loop = asyncio.new_event_loop()
        thread = threading.Thread(target=_spin, args=(loop,), name="llm-loop", daemon=True)
        thread.start()
        _loop, _thread = loop, thread
        logging.info("llm_loop: поднят общий цикл событий (поток %s)", thread.name)
        return loop


def run(coro: Coroutine[Any, Any, Any], *, timeout: float | None = None) -> Any:
    """Выполнить корутину на общем цикле и дождаться результата.

    Замена `asyncio.run(...)` в синхронном коде. Исключения пробрасываются как есть.

    `timeout` — верхняя граница ожидания В СЕКУНДАХ на случай, если у самой корутины
    своего срока нет. По умолчанию его нет и здесь: сроки запросов к модели заданы в
    `openai_manager`, дублировать их вторым числом — значит завести два разных
    представления об одном и том же и разойтись с ними при первой же правке.
    """
    loop = get_loop()
    if threading.current_thread() is _thread:
        # Отдать работу самому себе и ждать её же — это вечная остановка. Раньше на
        # этом месте `asyncio.run` честно падал («cannot be called from a running
        # event loop»), и падение остаётся падением: зависший процесс хуже ошибки.
        coro.close()
        raise RuntimeError(
            "llm_loop.run вызван из самого цикла: корутину внутри цикла нужно просто await"
        )
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    try:
        return future.result(timeout)
    except BaseException:
        # Ждать перестали — работу тоже прекращаем, иначе ответ модели уедет в никуда,
        # а соединение останется занятым.
        future.cancel()
        raise
