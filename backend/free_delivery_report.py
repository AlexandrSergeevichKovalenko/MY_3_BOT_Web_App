"""Вечерний отчёт владельцу: получил ли бесплатный человек свои шесть заданий за день.

Обещание бесплатного тарифа — шесть заданий в сутки. Проверить это на глаз нельзя:
часть карточек похожа на «разгон» (тренировка перед спринтом), часть приходит сверх
плана (работа над ошибками, спринт по рельсу, повтор тренировки, новость дня), и
пересчёт в чате даёт «вроде пять». Отчёт кладёт рядом ПЛАН дня (одинаковый для всех
бесплатных — ротация выбирает слоты на календарный день) и ФАКТ по каждому человеку,
и называет, чего именно не хватило.

Факт берём из ведомости `bt_3_interactive_inbox`: туда пишет каждый отправитель
засчитываемого задания, по ней же дневной лимит считает сама рассылка. Сверхплановое
(работа над ошибками, спринт, повтор тренировки) считается отдельной строкой и в шесть
не входит.

План приходит снаружи (из бота, где живёт ротация) — здесь только запросы и текст,
чтобы всё это можно было проверить тестом без телеграма и планировщика.
"""
from __future__ import annotations

import logging
from datetime import date

JOB_KEY = "free_delivery_daily"

from backend.database import INBOX_BONUS_KINDS

# Бонусы: приходят сверх нормы и в счёт не идут. Список один на весь проект — в database.
BONUS_INBOX_KINDS = set(INBOX_BONUS_KINDS)

# Ротационный вид задания → код в ведомости.
KIND_TO_INBOX_CODE = {
    "rebus": "rb", "crossword": "cw", "anagram": "ag", "aufgabe": "au",
    "listening": "ls", "numdict": "nd", "adjektiv_sprint": "ad",
    "wofrage_sprint": "wf", "artikel_sprint": "as", "artikel_learn": "al",
    "trainer": "tr", "article_quiz": "aq", "mc": "mc",
}

KIND_TITLE = {
    "rebus": "🧩 Ребус", "crossword": "🧩 Кроссворд", "anagram": "🧩 Анаграмма",
    "aufgabe": "🧩 Aufgabe", "listening": "🎧 Аудирование", "numdict": "🎧 Zahlen-Diktat",
    "adjektiv_sprint": "⚡ Adjektiv Sprint", "wofrage_sprint": "⚡ Wo-Frage Sprint",
    "artikel_sprint": "⚡ Artikel Sprint", "artikel_learn": "📚 Artikel Trainer",
    "trainer": "🧩 Тренировка синонимов/антонимов", "article_quiz": "🧩 Артикль-квиз",
    "mc": "🧩 Квиз",
}

_WEEKDAY_RU = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]

INBOX_CODE_TO_KIND = {code: kind for kind, code in KIND_TO_INBOX_CODE.items()}


def _title(kind: str) -> str:
    return KIND_TITLE.get(kind, kind)


def _title_by_code(code: str) -> str:
    return _title(INBOX_CODE_TO_KIND.get(code, code))


def _plural(n: int, one: str, few: str, many: str) -> str:
    n = abs(int(n))
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


def _delivered_by_user(day: date, tz_name: str) -> dict[int, dict[str, int]]:
    """{user_id: {код: сколько}} — всё, что за день записано в ведомость."""
    from backend.database import get_db_connection_context
    out: dict[int, dict[str, int]] = {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, kind, COUNT(*)
                FROM bt_3_interactive_inbox
                WHERE (created_at AT TIME ZONE %s)::date = %s
                GROUP BY user_id, kind;
                """,
                (tz_name, day),
            )
            for uid, kind, n in cursor.fetchall() or []:
                out.setdefault(int(uid), {})[str(kind)] = int(n)
    return out


def _bonus_recipients(day: date, repeat_slot_hours: tuple) -> dict[str, set]:
    """Кто получил сверхплановое: спринт по рельсу и повтор тренировки."""
    from backend.database import get_db_connection_context
    out = {"sprint": set(), "trainer_repeat": set()}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT DISTINCT target_user_id FROM bt_3_sprint_dispatches WHERE slot_date = %s;",
                (day,),
            )
            out["sprint"] = {int(r[0]) for r in cursor.fetchall() or [] if r[0]}
            if repeat_slot_hours:
                cursor.execute(
                    "SELECT DISTINCT target_user_id FROM bt_3_trainer_dispatches "
                    "WHERE slot_date = %s AND slot_hour = ANY(%s);",
                    (day, [int(h) for h in repeat_slot_hours]),
                )
                out["trainer_repeat"] = {int(r[0]) for r in cursor.fetchall() or [] if r[0]}
    return out


def _news_line(day: date) -> str:
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT status, is_pinned FROM bt_3_world_news_daily WHERE news_date = %s;",
                    (day,),
                )
                row = cursor.fetchone()
    except Exception:
        logging.warning("free_delivery_report: news lookup failed", exc_info=True)
        return "📰 Новость дня: не смог проверить"
    if not row:
        return "📰 Новость дня: не готовилась"
    status, pinned = str(row[0] or ""), bool(row[1])
    if status == "sent":
        return "📰 Новость дня: ушла утром"
    if not pinned:
        return "📰 Новость дня: НЕ ушла — не одобрена вечером"
    return "📰 Новость дня: НЕ ушла — одобрена, но рассылка не отработала"


def build_free_delivery_text(
    *,
    day: date,
    tz_name: str,
    plan: list,
    budget: int,
    free_user_ids,
    repeat_slot_hours=(),
) -> str:
    """Текст отчёта. `plan` — [(kind, hour, minute)] на сегодня для бесплатного тарифа,
    `repeat_slot_hours` — слоты повторной тренировки (бонус) в виде ЧЧММ."""
    from backend.database import get_user_display_names

    free_ids = sorted({int(u) for u in free_user_ids or []})
    head = f"🧾 <b>Бесплатные за {day.strftime('%d.%m')} ({_WEEKDAY_RU[day.weekday()]})</b>"
    if not free_ids:
        return f"{head}\n\nБесплатных получателей сегодня нет."

    planned_by_kind: dict[str, int] = {}
    for kind, _h, _m in plan:
        planned_by_kind[str(kind)] = planned_by_kind.get(str(kind), 0) + 1
    plan_line = " · ".join(f"{_title(k)} {int(h):02d}:{int(m):02d}" for k, h, m in plan)

    delivered = _delivered_by_user(day, tz_name)
    try:
        names = get_user_display_names(free_ids) or {}
    except Exception:
        names = {}

    def _name(uid: int) -> str:
        return str(names.get(uid) or "").strip() or f"id {uid}"

    # Две РАЗНЫЕ беды, и мешать их нельзя: «пришло меньше шести» — человек недополучил,
    # это поломка доставки; «шесть пришло, но не те» — норма выполнена, а слот плана не
    # отработал и дырку закрыл вечерний добор другим типом. Раньше обе шли под словом
    # «недобрали», и отчёт сам себе противоречил строкой «6 из 6 · не дошло».
    full, short, swapped = [], [], []
    for uid in free_ids:
        got = delivered.get(uid, {})
        counted = sum(n for code, n in got.items() if code not in BONUS_INBOX_KINDS)
        missing, extra = [], []
        for kind, want in planned_by_kind.items():
            have = int(got.get(KIND_TO_INBOX_CODE.get(kind, kind), 0))
            for _ in range(max(0, want - have)):
                missing.append(_title(kind))
        for code, have in got.items():
            if code in BONUS_INBOX_KINDS:
                continue
            want = planned_by_kind.get(INBOX_CODE_TO_KIND.get(code, code), 0)
            for _ in range(max(0, int(have) - want)):
                extra.append(_title_by_code(code))
        if not missing and counted >= budget:
            full.append(_name(uid))
        elif counted >= budget:
            swapped.append((_name(uid), missing, extra))
        else:
            short.append((counted, _name(uid), missing))

    served = len(full) + len(short) + len(swapped)
    got_budget = len(full) + len(swapped)
    if not served:
        verdict = "Сегодня заданий не ждал никто."
    elif not short and not swapped:
        verdict = f"Все свои {budget} получили ({served} {_plural(served, 'человек', 'человека', 'человек')})."
    else:
        parts = []
        if short:
            parts.append(f"Недобрали {len(short)} из {served} "
                         f"{_plural(served, 'человека', 'человек', 'человек')}.")
        elif got_budget:
            parts.append(f"Все свои {budget} получили "
                         f"({got_budget} {_plural(got_budget, 'человек', 'человека', 'человек')}).")
        if swapped:
            parts.append(f"Но у {len(swapped)} "
                         f"{_plural(len(swapped), 'человека', 'человек', 'человек')} "
                         f"пришло не то, что стояло в плане.")
        verdict = " ".join(parts)
    lines = [head, "", verdict, "", f"<b>План дня — {budget}:</b>", plan_line, "", "<b>Дошло:</b>"]
    if full:
        lines.append(f"✅ {budget} из {budget} — {', '.join(full)}")
    for name, missing, extra in sorted(swapped):
        tail = f" · не дошло: {', '.join(missing)}"
        if extra:
            tail += f" · вместо этого: {', '.join(extra)}"
        lines.append(f"🔀 {budget} из {budget} — {name}{tail}")
    for counted, name, missing in sorted(short):
        if counted == 0:
            lines.append(f"⛔ 0 из {budget} — {name} · не дошло ничего")
            continue
        tail = f" · не дошло: {', '.join(missing)}" if missing else ""
        lines.append(f"⚠️ {counted} из {budget} — {name}{tail}")
    if swapped:
        lines.append("<i>🔀 — норму человек получил, но слот плана не отработал и дырку "
                     "закрыл вечерний добор другим заданием.</i>")

    try:
        bonus = _bonus_recipients(day, tuple(repeat_slot_hours or ()))
    except Exception:
        logging.warning("free_delivery_report: bonus lookup failed", exc_info=True)
        bonus = {"sprint": set(), "trainer_repeat": set()}
    review = sum(1 for uid in free_ids for code in delivered.get(uid, {}) if code == "rv")
    sprint = len([u for u in free_ids if u in bonus["sprint"]])
    repeat = len([u for u in free_ids if u in bonus["trainer_repeat"]])
    lines += ["", "<b>Сверх плана</b> (в шесть не входит):",
              f"🔁 работа над ошибками — {review} "
              f"{_plural(review, 'человек', 'человека', 'человек')} · "
              f"🏃 спринт — {sprint} · 🔁 повтор тренировки — {repeat}",
              _news_line(day)]
    if not sprint:
        lines.append("<i>Спринт уходит только тем, кто до этого прошёл тренировку — ноль тут нормально.</i>")
    return "\n".join(lines)
