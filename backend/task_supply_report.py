# -*- coding: utf-8 -*-
"""Отчёт владельцу: на сколько дней хватает заданий и что дозаказать ночью.

Смысл отчёта в одном: дно должно быть видно ДО того, как его увидят люди. Поэтому
каждая строка отвечает на вопрос «что делать», а не просто показывает число.
"""

from backend.task_supply import TARGET_SUPPLY_DAYS, verdict


def _days_text(days: float) -> str:
    if days == float("inf"):
        return "—"
    if days > 999:
        return "999+"
    return f"{days:.0f}"


def build_task_supply_report(rows: list) -> str:
    """Собрать текст отчёта из замеров `measure_all_task_supply`."""
    if not rows:
        return "Замер не дал ни одной строки — похоже, память ротации ещё пуста."

    lines = ["🗂 <b>Запас заданий</b>", ""]
    trouble, quiet = [], []
    for r in rows:
        if r.get("error"):
            lines.append(f"• {r.get('kind')}: {r['error']}")
            continue
        (quiet if r["per_day"] <= 0 else trouble).append(r)

    if trouble:
        trouble.sort(key=lambda r: r["supply_days"])
        for r in trouble:
            days = r["supply_days"]
            mark = "🔴" if days < 7 else ("🟡" if days < TARGET_SUPPLY_DAYS else "🟢")
            lines.append(
                f"{mark} <b>{r['title']}</b> — хватит на {_days_text(days)} дн. "
                f"({verdict(days)})")
            lines.append(
                f"    в банке {r['bank_total']}, человеку доступно {r['available']}, "
                f"расход {r['per_day']}/сутки")
            if r["order_now"] > 0:
                lines.append(f"    ▸ дозаказать сегодня ночью: <b>{r['order_now']}</b>")
        lines.append("")

    if quiet:
        names = ", ".join(r["title"] for r in quiet)
        lines.append(f"Не расходуются (никто не берёт за последний месяц): {names}.")
        lines.append("Для них дозаказывать нечего — деньги не тратим.")

    lines.append("")
    lines.append(f"Держим запас в {TARGET_SUPPLY_DAYS} дн. у самого продвинутого "
                 f"человека. Банк зависит не от числа людей, а от расхода: разным "
                 f"людям можно давать одно и то же задание.")
    return "\n".join(lines)


def task_supply_alerts(rows: list) -> list:
    """Строки, которые нельзя пропустить: где дно ближе недели или замер сломался."""
    out = []
    for r in rows or []:
        if r.get("error"):
            out.append(f"{r.get('kind')}: {r['error']}")
        elif r.get("per_day", 0) > 0 and r.get("supply_days", 0) < 7:
            out.append(f"{r['title']}: осталось на {_days_text(r['supply_days'])} дн., "
                       f"дозаказать {r['order_now']}")
    return out
