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
    broken, quiet, live = [], [], []
    for r in rows:
        if r.get("error"):
            broken.append(r)
        elif r.get("per_day", 0) <= 0:
            quiet.append(r)
        else:
            live.append(r)

    # Все живые виды показываем ПОИМЁННО и по порядку «где раньше кончится». Прятать
    # что-то в сводную строку нельзя: 15.08.2026 отчёт свернул пять живых игр в
    # «не расходуются» и оставил на виду самую редкую — читалось как бред.
    live.sort(key=lambda r: r["supply_days"])
    for r in live:
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
    if live:
        lines.append("")

    if quiet:
        lines.append("<b>Не выдавались за последний месяц</b> — расход посчитать не по "
                     "чему, дозаказывать нечего:")
        for r in quiet:
            lines.append(f"    • {r['title']} — в банке {r['bank_total']}")
        lines.append("")

    if broken:
        lines.append("<b>Замер не удался:</b>")
        for r in broken:
            lines.append(f"    • {r.get('title') or r.get('kind')}: {r['error']}")
        lines.append("")

    lines.append(f"Расход берётся из журнала выдачи за 30 дней — сколько заданий этого "
                 f"вида человек реально получил. Держим запас в {TARGET_SUPPLY_DAYS} дн. "
                 f"у самого продвинутого. Банк зависит не от числа людей, а от расхода: "
                 f"разным людям можно давать одно и то же задание.")
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
