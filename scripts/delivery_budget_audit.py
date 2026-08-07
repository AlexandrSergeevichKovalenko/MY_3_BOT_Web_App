"""Замер: получил ли человек столько заданий, сколько ему положено по его выбору.

Обещание продукта — бесплатный 6 заданий в день, «редко» 8, «обычно» 12, «интенсив» 20,
и задания должны быть РАЗНЫЕ. Проверить это на глаз нельзя: часть приходит слотами,
часть капельной выдачей (у кого выставлены свои часы), часть сверх нормы, а ещё
владелец сам достаёт задания кнопкой «Следующее задание».

Скрипт кладёт рядом норму и факт по каждому человеку за последние дни.

    python scripts/delivery_budget_audit.py        # за 3 дня
    python scripts/delivery_budget_audit.py 7      # за неделю

Как читать:
  • Текущий день неполный — смотреть надо на вчерашний.
  • Аномально большие числа у аккаунта владельца — это не рассылка, а его собственные
    нажатия «Следующее задание» и тестовые команды. Отличаются по времени: слоты
    приходят в круглые минуты (10:00, 11:45, 15:10), ручные — в произвольные (07:22).
  • Награда за серию раз в 5 дней делает бесплатный аккаунт оплаченным на сутки — в
    такой день у него меняются и норма, и путь доставки.
"""
from __future__ import annotations

import os
import sys

# Запуск как `python scripts/delivery_budget_audit.py` кладёт в путь папку scripts/,
# а не корень репозитория — без этого не найдутся ни bot_3, ни backend.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TZ = "Europe/Berlin"
PRESET_RU = {"intensive": "интенсив", "normal": "обычно", "rare": "редко",
             "silent": "тишина", "custom": "своё"}


def main(days: int = 3) -> None:
    import bot_3
    from backend.database import (get_db_connection_context, is_user_pro,
                                  get_user_display_names, get_user_prefs_bulk)

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT to_char(NOW() AT TIME ZONE %s,'DD.MM HH24:MI');", (TZ,))
            now_txt = cursor.fetchone()[0]
            # Границы — КАЛЕНДАРНЫЕ дни, а не «минус N часов»: иначе самый старый день
            # обрезается посередине и выглядит недобором, которого не было.
            cursor.execute(
                """SELECT user_id, (created_at AT TIME ZONE %s)::date d, kind
                   FROM bt_3_interactive_inbox
                   WHERE (created_at AT TIME ZONE %s)::date > CURRENT_DATE - %s
                     AND kind <> 'rv';""",
                (TZ, TZ, int(days)),
            )
            rows = cursor.fetchall() or []

    by_user_day: dict = {}
    all_days: set = set()
    for uid, day, kind in rows:
        all_days.add(day)
        by_user_day.setdefault(int(uid), {}).setdefault(day, []).append(str(kind))

    uids = sorted(by_user_day)
    if not uids:
        print("За этот период выдач нет.")
        return
    names = get_user_display_names(uids) or {}
    prefs = get_user_prefs_bulk(tuple(uids)) or {}
    ordered_days = sorted(all_days)

    print(f"сейчас: {now_txt}   (последний столбец — сегодняшний, он ещё неполный)\n")
    header = f"{'кто':<22}{'тариф':<12}{'режим':<10}{'часы':<7}{'норма':<7}"
    for day in ordered_days:
        header += f"{day.strftime('%d.%m'):>8}"
    print(header)
    print("-" * len(header))

    for uid in uids:
        pro = is_user_pro(uid)
        pref = prefs.get(uid) or {}
        preset = str(pref.get("preset") or "normal")
        own_hours = bool(pref.get("schedule")) and pro
        budget = bot_3._preset_budget(preset) if pro else bot_3.FREE_SEND_BUDGET
        mode = PRESET_RU.get(preset, preset) if pro else "—"
        line = (f"{(names.get(uid) or f'id {uid}')[:20]:<22}"
                f"{'платный' if pro else 'бесплатный':<12}{mode:<10}"
                f"{('да' if own_hours else 'нет'):<7}{budget:<7}")
        for day in ordered_days:
            got = by_user_day[uid].get(day, [])
            line += f"{str(len(got)) + ('✅' if len(got) >= budget else ''):>8}"
        print(line)

    print("\n--- что именно приходило (и что задвоилось) ---")
    for uid in uids:
        who = str(names.get(uid) or uid)[:20]
        for day in ordered_days:
            got = by_user_day[uid].get(day, [])
            if not got:
                continue
            dupes = {k: got.count(k) for k in sorted(set(got)) if got.count(k) > 1}
            tail = f"  ⚠️ дубли: {dupes}" if dupes else ""
            print(f"{who:<22}{day.strftime('%d.%m')}  {len(got):>2} шт · "
                  f"{len(set(got)):>2} разных · {' '.join(sorted(got))}{tail}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 3)
