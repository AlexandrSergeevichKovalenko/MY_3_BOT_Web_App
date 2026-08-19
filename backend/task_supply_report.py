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
        measured, avg = r.get("per_day_measured"), r.get("per_day_avg")
        rate_text = (f"расход у самого активного {measured}/сутки"
                     + (f" (в среднем {avg})" if avg is not None else "")
                     + f" + запас 20% = {r['per_day']}"
                     if measured is not None else f"расход {r['per_day']}/сутки")
        # «Дозревают» — сделанные, но пока не выдаваемые (кроссворду не нарисована
        # картинка, аудированию не доехал звук). Без них отчёт читается как «банк
        # не растёт»: замер идёт в 04:25, а дорисовка догоняет позже.
        # Ключа нет только у рукописных строк в тестах — тогда приписки просто нет.
        # Подставлять сюда число «на всякий случай» нельзя: приписка «+0 дозревают»
        # соврала бы ровно там, где мы ничего не мерили.
        ripening = int(r["bank_ripening"]) if "bank_ripening" in r else 0
        bank_text = (f"в банке {r['bank_total']}"
                     + (f" (+{ripening} дозревают)" if ripening else ""))
        lines.append(f"    {bank_text}, человеку доступно {r['available']}, {rate_text}")
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

    lines.append(f"Расход — у САМОГО активного из живых за 30 дней (кто хоть раз "
                 f"отвечал), плюс 20% запаса: дно наступает у него первым, среднее его "
                 f"недосчитает. Держим запас в {TARGET_SUPPLY_DAYS} дн. "
                 f"Банк зависит не от числа людей, а от расхода: "
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
