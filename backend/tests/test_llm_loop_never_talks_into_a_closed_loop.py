# -*- coding: utf-8 -*-
"""Разговор с моделью не идёт в закрытый цикл событий.

ПОВОД (16.09.2026). Владелец: «очень много перебоев… словарь часто не работает».
В логах прода за 5 часов — 17 × RuntimeError('Event loop is closed'), быстрая
половина разбора умирала за 1–2 мс, и человек ждал медленную (9 запросов из 13
дольше 10 секунд, один 35).

Корень: клиент OpenAI один на процесс и держит открытые соединения, а каждый
веб-запрос открывал СВОЙ цикл событий (`asyncio.run`) и закрывал его на выходе.
Соединение от закрытого цикла оставалось в пуле и роняло следующий запрос.

Здесь проверяются обе половины починки:
  1. `llm_loop.run` выполняет всё на ОДНОМ живом цикле — он не закрывается между
     запросами, поэтому соединения в пуле остаются годными;
  2. клиент привязан к своему циклу: чужому циклу достаётся свой клиент, а не
     соединения покойника;
  3. дверь, через которую поломка вошла, закрыта: в веб-сервисе не осталось
     `asyncio.run(` — иначе назавтра всё вернётся на место.

Сети здесь нет: проверяется именно устройство, а не ответ OpenAI.
"""
import asyncio
import re
from pathlib import Path

from backend import llm_loop


def test_run_uses_one_live_loop():
    """Два вызова подряд идут в один и тот же цикл, и он остаётся живым."""
    async def which_loop():
        return id(asyncio.get_running_loop())

    first = llm_loop.run(which_loop())
    second = llm_loop.run(which_loop())

    assert first == second, "каждый вызов попал в свой цикл — соединения снова будут умирать"
    assert not llm_loop.get_loop().is_closed(), "общий цикл закрылся: пул соединений опять протухнет"


def test_result_and_errors_pass_through():
    """Ответ возвращается как есть, ошибка поднимается как есть — без подмен."""
    async def answer():
        return "готово"

    async def fails():
        raise ValueError("сеть не ответила")

    assert llm_loop.run(answer()) == "готово"

    try:
        llm_loop.run(fails())
    except ValueError as exc:
        assert str(exc) == "сеть не ответила"
    else:
        raise AssertionError("ошибка проглочена — наружу ушёл бы 'нормальный' ответ")


def test_client_is_bound_to_its_own_loop():
    """Каждому циклу — свой клиент; внутри одного цикла клиент один и тот же."""
    from backend.openai_manager import _LoopBoundOpenAI

    made: list[object] = []

    def factory():
        made.append(object())
        return made[-1]

    proxy = _LoopBoundOpenAI(factory)

    async def take():
        return proxy._resolve()

    in_first_loop = asyncio.run(take())      # цикл создан и закрыт
    in_second_loop = asyncio.run(take())     # уже другой цикл
    assert in_first_loop is not in_second_loop, (
        "клиент пережил свой цикл: именно так соединение от покойника попадало в новый запрос"
    )

    async def twice():
        return proxy._resolve(), proxy._resolve()

    same_a, same_b = asyncio.run(twice())
    assert same_a is same_b, "внутри одного цикла клиент должен быть один, иначе пул не греется"


def test_web_service_has_no_asyncio_run_left():
    """Дверь закрыта: веб-сервис не открывает собственные циклы на каждый запрос."""
    server = Path(__file__).resolve().parents[1] / "backend_server.py"
    text = server.read_text(encoding="utf-8")
    leftovers = [
        line.strip()
        for line in text.split("\n")
        if re.search(r"(?<![\w.])asyncio\.run\(", line)
    ]
    assert not leftovers, (
        "вернулся asyncio.run — каждый такой вызов закрывает цикл под открытыми "
        f"соединениями: {leftovers[:5]}"
    )
