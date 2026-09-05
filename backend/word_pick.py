# -*- coding: utf-8 -*-
"""«Слова со вчерашних тренировок»: чистые правила без базы и без бота.

Стратегия: docs/tasks/word_pick_review_strategy.md. Решение владельца 04.09.2026:
два прохода в день, утром 07:25 и вечером 19:35, один и тот же набор. Здесь лежит то,
что обязано совпадать у трёх сторон — у рассылки (bot_3), у сервера (backend_server) и у
отчёта, — чтобы граница «утро/вечер» не жила в трёх местах тремя разными числами.
"""
from __future__ import annotations

import re
from datetime import date, datetime

# Решение владельца, 04.09.2026.
WORD_PICK_SLOTS: dict[str, tuple[int, int]] = {"am": (7, 25), "pm": (19, 35)}
_SLOT_NO = {"am": 1, "pm": 2}
_DAY_RE = re.compile(r"^(\d{4})-?(\d{2})-?(\d{2})$")


def slot_now(now_local: datetime) -> str:
    """Какой проход идёт сейчас: до 19:35 утренний, с 19:35 вечерний."""
    return "pm" if (now_local.hour, now_local.minute) >= WORD_PICK_SLOTS["pm"] else "am"


def parse_day(raw) -> date | None:
    """День из ссылки/запроса: ГГГГММДД или ГГГГ-ММ-ДД. Всё остальное — None (не дефолт)."""
    if not isinstance(raw, str):
        return None
    m = _DAY_RE.match(raw.strip())
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def day_id(day: date, slot: str) -> int:
    """Номер строки в ведомости: ГГГГММДД·10 + 1 (утро) / 2 (вечер). У постера нет своего
    dispatch, как и у «Работы над ошибками», поэтому ключ — день; два постера в день
    обязаны быть двумя строками, иначе вечерний затирал бы утренний."""
    return int(day.strftime("%Y%m%d")) * 10 + _SLOT_NO[slot]


def deeplink_for(day: date) -> str:
    return f"ans_wp_{day:%Y%m%d}"
