"""Состояние пула рубрики «Стендап дня» — числом, само, раз в неделю.

Зачем это отдельная вещь. Рубрика выдаёт ролик через день и никогда не повторяется: раз
показанное лежит в вечном реестре и больше не выбирается. Значит пул конечен и однажды
кончится — и кончится он молча, если никто не считает. Молчащий механизм неотличим от
сломанного, поэтому число оставшихся роликов приходит владельцу само, а не по команде.

Отдельно считаются ролики с субтитрами, положенными РУКАМИ: владелец 20.08.2026 решил
ставить их первыми, а машинную расшифровку держать вторым эшелоном. Когда ручные подойдут
к концу, владелец должен узнать об этом заранее — а не по тому, что субтитры вдруг стали
хуже.

── Почему отчёт НЕ ходит в YouTube (переделано 21.08.2026) ────────────────────
Первая версия обходила все каналы заново — около 170 единиц квоты за отчёт, плюс столько
же за каждый вызов вручную. То есть ОТЧЁТ тратил ровно тот ресурс, который нужен самому
продукту; 21.08.2026 суточная квота кончилась, и рубрика не смогла подобрать ролик.

Теперь отчёт складывается из вещей, за которые уже заплачено:
  • снимок пула — пишется в момент, когда обход каналов и так идёт (вечерний поиск с
    колёс и ночное пополнение полки);
  • полка — наша таблица заранее отобранных роликов;
  • вечный реестр показанного — наша таблица.
Обращений к YouTube: ноль. Возраст снимка называется вслух.

── Что считается ЗАПАСОМ (переделано 06.09.2026) ──────────────────────────────
┌─ НАЙДЕНО 06.09.2026 владельцем: «Запаса меньше месяца. Пора добавить каналы» ────────┐
│ при 90 годных роликах у каналов. Отчёт считал запас ТОЛЬКО по полке — так было верно  │
│ 21.08, когда полка была единственным источником. 29.08 владелец перестроил рубрику:   │
│ выпуск ищет ролик с колёс, а полка — аварийный склад на семь штук. Семь роликов через  │
│ день это 14 дней, это меньше 30, и тревога срабатывала КАЖДОЕ воскресенье при любом    │
│ состоянии каналов. Ложная тревога по устройству: когда пул вправду иссякнет, письмо   │
│ будет неотличимо от еженедельного.                                                    │
│ Теперь запас = полка + годные у каналов (по снимку, минус показанные после снимка).    │
│ Полка показывается отдельно как склад «N из цели».                                    │
└───────────────────────────────────────────────────────────────────────────────────────┘
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Порог тревоги: меньше месяца вещания. Решение владельца 21.08.2026, применяется к запасу
# целиком, а не к полке (06.09.2026).
ALARM_DAYS = 30


def standup_pool_state() -> dict:
    """Сколько роликов рубрика ещё может показать, сколько израсходовано и на сколько дней
    хватит. Читает готовое: полку, снимок пула и реестр показанного. В сеть НЕ ходит.

    Ошибки НЕ глушатся: пустой отчёт от сбоя неотличим от честного «пул кончился», а это
    два разных мира — в одном надо чинить базу, в другом пополнять набор каналов.
    """
    from backend.daily_video_rubrics import STANDUP_PROFILE
    from backend.database import (count_shown_daily_videos, count_shown_from_pool_since,
                                  get_daily_video_pool_snapshot, standup_shelf_counts)
    from backend.standup_shelf import shelf_target
    from backend.world_news_generator import archive_slice_per_channel

    shelf = standup_shelf_counts()
    snapshot = get_daily_video_pool_snapshot(STANDUP_PROFILE.key)
    shown_total = count_shown_daily_videos(STANDUP_PROFILE.key)

    shelf_unused = int(shelf.get("unused") or 0)
    shelf_manual = int(shelf.get("unused_manual") or 0)

    state = {
        "measured": True,
        "channels": len(STANDUP_PROFILE.channel_ids),
        "slice_per_channel": archive_slice_per_channel(STANDUP_PROFILE),
        "shelf_total": int(shelf.get("total") or 0),
        "shelf_unused": shelf_unused,
        "shelf_manual": shelf_manual,
        "shelf_target": int(shelf_target()),
        "shown_total": shown_total,
        # Снимка нет — обход каналов ещё ни разу не доходил до записи. Это отдельное
        # состояние «не знаем», а не ноль: отчёт обязан сказать об этом словами.
        "pool_measured": snapshot is not None,
        "pool_measured_on": (snapshot or {}).get("measured_on"),
        "pool_usable": None,
        "pool_manual": None,
        "reserve": None,
        "reserve_manual": None,
        "days_left": None,
        "days_left_manual": None,
    }
    if snapshot is None:
        return state

    # Годных у каналов на момент снимка минус показанные ПОСЛЕ снимка не с полки.
    since = count_shown_from_pool_since(STANDUP_PROFILE.key, snapshot["updated_at"])
    pool_usable = max(0, int(snapshot.get("in_range") or 0) - since["total"])
    pool_manual = max(0, int(snapshot.get("manual_captions") or 0) - since["manual"])
    reserve = shelf_unused + pool_usable
    reserve_manual = shelf_manual + pool_manual
    state.update({
        "pool_usable": pool_usable,
        "pool_manual": pool_manual,
        "reserve": reserve,
        "reserve_manual": reserve_manual,
        # Рубрика выходит через день, поэтому запас в днях — вдвое больше числа роликов.
        "days_left": reserve * 2,
        "days_left_manual": reserve_manual * 2,
    })
    return state


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


def _days(n: int) -> str:
    return f"{n} {_plural_days_ru(n)}"


def report_calls_for_channels(text: str) -> bool:
    """Зовёт ли текст отчёта добавлять каналы. Одна фраза, одно место, — по ней проверяется
    обещание «ложной тревоги больше нет» (backend/fix_promises.py)."""
    return "добавить каналы" in text


def format_standup_pool_report(state: dict) -> str:
    """Человеческий текст отчёта: взглянул — понял — знаешь, надо ли что-то делать."""
    shelf_unused = int(state.get("shelf_unused") or 0)
    shelf_target = int(state.get("shelf_target") or 0)
    lines = ["🎤 <b>Стендап дня — состояние пула</b>", ""]

    if not state.get("pool_measured"):
        lines += [
            "⏳ <b>Каналы ещё не обходили</b> — сколько у них годных роликов, пока не знаем. "
            "Снимок появится после первого поиска ролика или пополнения полки.",
            "",
            f"Аварийный склад: <b>{shelf_unused}</b> из {shelf_target}",
            f"Уже показано: {state.get('shown_total', 0)}",
            "",
            "<i>Квоту YouTube отчёт не тратит</i>",
        ]
        return "\n".join(lines)

    reserve = int(state.get("reserve") or 0)
    manual = int(state.get("reserve_manual") or 0)
    days = int(state.get("days_left") or 0)
    pool_usable = int(state.get("pool_usable") or 0)

    if reserve <= 0:
        verdict = ("📭 <b>Показывать нечего: у каналов годных не осталось, склад пуст.</b> "
                   "Нужно добавить каналы в набор.")
    elif days < ALARM_DAYS:
        verdict = (f"⚠️ <b>Запаса меньше месяца</b> — хватит примерно на {_days(days)}. "
                   "Пора добавить каналы.")
    else:
        verdict = f"✅ Запаса хватит примерно на {_days(days)} вещания."

    lines += [
        verdict,
        "",
        f"Годных непоказанных: <b>{reserve}</b> — у {state.get('channels', 0)} каналов "
        f"{pool_usable} (смотрим последние {state.get('slice_per_channel', 0)} роликов на "
        f"каждом) + на аварийном складе {shelf_unused} из {shelf_target}",
        f"Уже показано: {state.get('shown_total', 0)}",
    ]
    if reserve > 0 and manual <= 0:
        lines += [
            "",
            "📝 Выступления с субтитрами, положенными руками, <b>закончились</b>. "
            "Дальше рубрика берёт машинную расшифровку — она без знаков препинания и "
            "угадывает слова на слух.",
        ]
    elif reserve > 0:
        lines += [
            "",
            f"📝 С ручными субтитрами осталось: <b>{manual}</b> "
            f"(≈{_days(int(state.get('days_left_manual') or 0))}). Дальше — машинная расшифровка.",
        ]
    # Возраст данных называется вслух: снимок обновляется при каждом обходе каналов, то
    # есть в вечер каждого стендапа; показанное после снимка уже вычтено.
    lines += ["", f"<i>Каналы смотрели {state['pool_measured_on']} · "
                  f"квоту YouTube отчёт не тратит</i>"]
    return "\n".join(lines)
