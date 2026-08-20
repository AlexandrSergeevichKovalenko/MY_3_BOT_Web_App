"""Состояние пула рубрики «Стендап дня» — числом, само, раз в неделю.

Зачем это отдельная вещь. Рубрика выдаёт ролик через день и никогда не повторяется: раз
показанное лежит в вечном реестре и больше не выбирается. Значит пул конечен и однажды
кончится — и кончится он молча, если никто не считает. Молчащий механизм неотличим от
сломанного, поэтому число оставшихся роликов приходит владельцу само, а не по команде.

Отдельно считаются ролики с субтитрами, положенными РУКАМИ: владелец 20.08.2026 решил
ставить их первыми, а машинную расшифровку держать вторым эшелоном. Когда ручные подойдут
к концу, владелец должен узнать об этом заранее — а не по тому, что субтитры вдруг стали
хуже.

Замер идёт по тому же свипу каналов, что и выбор ролика (кэш на 6 часов), поэтому
еженедельный отчёт почти ничего не стоит по квоте YouTube.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def standup_pool_state() -> dict:
    """Сколько роликов в пуле, сколько израсходовано, сколько осталось и на сколько дней.

    Ошибки НЕ глушатся: пустой отчёт от сбоя неотличим от честного «пул кончился», а это
    два разных мира — в одном надо чинить сеть, в другом пополнять набор каналов.
    """
    from backend.daily_video_rubrics import STANDUP_PROFILE
    from backend.database import get_shown_daily_video_ids
    from backend.world_news_generator import _gather_candidates, _yt_api_video_details

    candidates = _gather_candidates(STANDUP_PROFILE)
    details = _yt_api_video_details([c["video_id"] for c in candidates])

    in_range: list[str] = []
    manual: set[str] = set()
    for cand in candidates:
        vid = cand["video_id"]
        det = details.get(vid) or {}
        dur = int(det.get("duration_seconds") or 0)
        if not dur or not (STANDUP_PROFILE.min_seconds <= dur <= STANDUP_PROFILE.max_seconds):
            continue
        in_range.append(vid)
        if det.get("has_manual_captions"):
            manual.add(vid)

    shown = get_shown_daily_video_ids(STANDUP_PROFILE.key)
    remaining = [v for v in in_range if v not in shown]
    remaining_manual = [v for v in remaining if v in manual]

    return {
        "channels": len(STANDUP_PROFILE.channel_ids),
        "scanned": len(candidates),
        "in_range": len(in_range),
        "shown": len([v for v in in_range if v in shown]),
        "shown_total": len(shown),
        "remaining": len(remaining),
        "remaining_manual": len(remaining_manual),
        # Рубрика выходит через день, поэтому запас в днях — вдвое больше числа роликов.
        "days_left": len(remaining) * 2,
        "days_left_manual": len(remaining_manual) * 2,
    }


def _plural_days_ru(n: int) -> str:
    """«1292 дня», а не «1292 дней» — отчёт читает человек."""
    n = abs(int(n))
    if 11 <= n % 100 <= 14:
        return "дней"
    d = n % 10
    if d == 1:
        return "день"
    if 2 <= d <= 4:
        return "дня"
    return "дней"


def format_standup_pool_report(state: dict) -> str:
    """Человеческий текст отчёта: взглянул — понял — знаешь, надо ли что-то делать."""
    remaining = int(state.get("remaining") or 0)
    manual = int(state.get("remaining_manual") or 0)
    days = int(state.get("days_left") or 0)
    days_manual = int(state.get("days_left_manual") or 0)

    if remaining <= 0:
        verdict = ("📭 <b>Ролики закончились.</b> Рубрика не сможет подобрать выступление — "
                   "нужно добавить каналы в набор.")
    elif days < 30:
        verdict = (f"⚠️ <b>Запаса меньше месяца.</b> Пора добавить каналы: "
                   f"хватит примерно на {days} {_plural_days_ru(days)}.")
    else:
        verdict = f"✅ Запаса хватит примерно на {days} {_plural_days_ru(days)} вещания."

    lines = [
        "🎤 <b>Стендап дня — состояние пула</b>",
        "",
        verdict,
        "",
        f"Непоказанных выступлений: <b>{remaining}</b> "
        f"(из {state.get('in_range', 0)} подходящих по длине)",
        f"Уже показано: {state.get('shown_total', 0)}",
        f"Каналов в наборе: {state.get('channels', 0)}",
    ]
    if manual <= 0 and remaining > 0:
        lines += [
            "",
            "📝 Выступления с субтитрами, положенными руками, <b>закончились</b>. "
            "Дальше рубрика берёт машинную расшифровку — она без знаков препинания и "
            "угадывает слова на слух.",
        ]
    elif remaining > 0:
        lines += [
            "",
            f"📝 С ручными субтитрами осталось: <b>{manual}</b> "
            f"(≈{days_manual} {_plural_days_ru(days_manual)}). "
            "Дальше — машинная расшифровка.",
        ]
    return "\n".join(lines)
