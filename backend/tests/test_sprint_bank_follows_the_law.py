# -*- coding: utf-8 -*-
"""Банк спринта и тренажёра растёт по закону, а не по вбитому числу.

Закон: банк ≥ расход за срок отдыха. Иначе выдача каждый раз не находит
отдохнувшую карточку, срабатывает запасной ход, и человек получает одно и то же
раньше срока — тихо, без пустого экрана.

Повод (замер 01.09.2026): цель пополнения была жёсткой шестёркой, в банке лежало
16 синонимов и 18 антонимов, шестнадцать больше шести — и ночная задача с 27.06.2026
просыпалась и не делала НИЧЕГО. Расход 1,6 карточки в день при отдыхе 21 день: за
срок уходило 33 карточки из 34, отдохнуть успевала ОДНА.
"""

from unittest.mock import patch

import pytest


def _target(per_day: float, *, bank: int = 34):
    import bot_3
    pressure = {"relation": "synonym", "window_days": 21, "bank": bank,
                "sprint_per_day": per_day, "trainer_per_day": per_day,
                "per_day": per_day}
    with patch("backend.database.measure_sprint_bank_pressure", return_value=pressure):
        return bot_3._sprint_pool_target("synonym")[0]


def test_без_расхода_держим_нижний_пол():
    import bot_3
    assert _target(0.0) == bot_3.SPRINT_POOL_FLOOR


def test_цель_растёт_вместе_с_расходом():
    """Главное свойство: людей стало больше — цель поехала вверх сама, без правки кода."""
    small = _target(0.8)
    big = _target(4.0)
    assert big > small, "цель не отреагировала на рост расхода"
    assert small >= 20, f"при 0,8 карточки в день и отдыхе 21 день цель обязана быть ≥20, вышло {small}"


def test_цель_покрывает_расход_за_срок_отдыха():
    """Ровно закон: цель не может быть меньше, чем уйдёт за время отдыха."""
    import bot_3
    for per_day in (0.5, 1.0, 1.6, 3.0):
        target = _target(per_day)
        spent_during_rest = per_day * bot_3.SPRINT_COOLDOWN_DAYS
        assert target >= spent_during_rest, (
            f"при расходе {per_day}/день за {bot_3.SPRINT_COOLDOWN_DAYS} дней уйдёт "
            f"{spent_during_rest:.0f} карточек, а цель {target} — банк снова кончится"
        )


def test_берётся_БОЛЬШИЙ_из_двух_расходов():
    """Одну карточку расходуют две выдачи независимо — спринт и тренажёр, у каждой
    свой счётчик дат. Считать надо по той, что давит сильнее."""
    from backend.database import measure_sprint_bank_pressure  # noqa: F401
    import bot_3
    pressure = {"relation": "synonym", "window_days": 21, "bank": 34,
                "sprint_per_day": 0.4, "trainer_per_day": 1.6, "per_day": 1.6}
    with patch("backend.database.measure_sprint_bank_pressure", return_value=pressure):
        target = bot_3._sprint_pool_target("synonym")[0]
    assert target >= 1.6 * bot_3.SPRINT_COOLDOWN_DAYS, (
        "цель посчитана по слабому расходу — тренажёр снова будет повторяться"
    )


def test_пополнение_считает_ОТДОХНУВШИЕ_карточки_а_не_все():
    """Тот самый корень: счётчик шёл с cooldown_days=0 и считал отдыхающие карточки
    доступными. Он отвечал на вопрос «сколько их всего», а решать нужно было
    «сколько можно выдать сегодня»."""
    import bot_3

    seen: dict = {}

    def fake_count(*, relation, cooldown_days):
        seen[relation] = cooldown_days
        return 3

    pressure = {"relation": "x", "window_days": 21, "bank": 99,
                "sprint_per_day": 0.1, "trainer_per_day": 0.1, "per_day": 0.1}

    async def noop_topup(relation, want):
        return 0

    import asyncio

    with patch.object(bot_3, "count_available_sprint_items", side_effect=fake_count), \
         patch("backend.database.measure_sprint_bank_pressure", return_value=pressure), \
         patch.object(bot_3, "ensure_sprint_schema", lambda: None), \
         patch.object(bot_3, "_sprint_topup", noop_topup):
        asyncio.run(bot_3.prepare_sprint_pool_job(None))

    assert seen, "пополнение вообще не спросило, сколько карточек доступно"
    for relation, cooldown in seen.items():
        assert cooldown == bot_3.SPRINT_COOLDOWN_DAYS, (
            f"{relation}: посчитали с отдыхом {cooldown} вместо "
            f"{bot_3.SPRINT_COOLDOWN_DAYS} — отдыхающие карточки снова считаются доступными"
        )
