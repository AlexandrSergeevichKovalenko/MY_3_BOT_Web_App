# -*- coding: utf-8 -*-
"""Короткий ежедневный отчёт: сколько словарь обслужил сам, сколько ушло в GPT и почём.

Зачем. Слой единиц включён, а старый путь пока лежит рядом отключённым — как кнопка
«вернуть как было». Решать, можно ли его убирать, надо ПО ЧИСЛАМ: сколько запросов
слой обслужил без GPT, растёт ли эта доля, не появились ли ошибки. Раньше это было
не измерить: метки телеметрии не отличали слой от старого банка.

Отчёт уходит владельцу в личку сам, чтобы не спрашивать «покажи цифры».
"""
from __future__ import annotations

import logging

from backend.database import get_db_connection_context

# Задачи GPT, которые относятся именно к словарю: их стоимость слой и должен снижать.
DICTIONARY_LLM_TASKS = (
    "dictionary_assistant_multilang",
    # Фоновая сборка карточки (досбор после сохранения, ночной добор, пересбор). До
    # 02.08.2026 писалась под именем живого разбора; без этой строки отчёт потерял бы
    # самую большую статью словаря.
    "dictionary_card_enrichment",
    "dictionary_assistant_multilang_reader",
    "dictionary_assistant_multilang_core_fast",
    "enrich_word_multilang",
    "enrich_word",
)


def _fetch_counts(cur, days: int) -> dict:
    cur.execute(
        """
        SELECT COALESCE(metadata->>'cache_scope', '—') AS scope, COUNT(*)
        FROM bt_3_limit_runtime_events
        WHERE feature_code LIKE '%%dictionary%%'
          AND created_at > NOW() - (%s || ' days')::interval
          AND event_type IN ('cache_hit', 'llm_call')
        GROUP BY 1;
        """,
        (str(days),),
    )
    rows = dict(cur.fetchall())
    served = int(rows.get("lex_units", 0))
    seeded = int(rows.get("lex_units_seed", 0))
    old_pool = int(rows.get("shared_pool", 0)) + int(rows.get("shared_pool_reverse_seed", 0))
    gpt = int(rows.get("gpt", 0))
    return {"served": served, "seeded": seeded, "old_pool": old_pool, "gpt": gpt,
            "total": served + seeded + old_pool + gpt}


def _fetch_cost(cur, days: int) -> tuple[float, int]:
    cur.execute(
        """
        SELECT COALESCE(SUM(cost_amount), 0), COUNT(*)
        FROM bt_3_billing_events
        WHERE action_type = ANY(%s) AND created_at > NOW() - (%s || ' days')::interval;
        """,
        (list(DICTIONARY_LLM_TASKS), str(days)),
    )
    row = cur.fetchone() or (0, 0)
    return float(row[0] or 0), int(row[1] or 0)


def build_report_text() -> str:
    """Готовый текст для личных сообщений (HTML)."""
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                day = _fetch_counts(cur, 1)
                week = _fetch_counts(cur, 7)
                cost_day, calls_day = _fetch_cost(cur, 1)
                cost_week, calls_week = _fetch_cost(cur, 7)
                cur.execute(
                    "SELECT COUNT(*) FROM bt_3_lex_units "
                    "WHERE lang = 'de' AND kind = 'word' AND card IS NULL;"
                )
                without_card = int((cur.fetchone() or [0])[0])
                cur.execute(
                    "SELECT COUNT(*) FROM bt_3_lex_units WHERE lang = 'de' AND kind = 'word';"
                )
                words_total = int((cur.fetchone() or [0])[0])
    except Exception as exc:
        logging.warning("dictionary layer report failed: %s", exc, exc_info=True)
        return "📚 <b>Словарь за сутки</b>\n\n⚠️ Не удалось собрать цифры, подробности в логах."

    def _share(block: dict) -> str:
        own = block["served"] + block["seeded"] + block["old_pool"]
        if not block["total"]:
            return "—"
        return "%d%%" % round(100 * own / block["total"])

    nights = (without_card + 199) // 200 if without_card else 0
    lines = [
        "📚 <b>Словарь за сутки</b>",
        "",
        f"Ответил свой словарь: <b>{day['served']}</b>",
        f"Дал основу, GPT только дополнил: {day['seeded']}",
        f"Пришлось идти в GPT: {day['gpt']}",
    ]
    if day["old_pool"]:
        lines.append(f"Ответил старый банк: {day['old_pool']}")
    lines += [
        f"Обслужено своими силами: <b>{_share(day)}</b>",
        "",
        f"Потрачено на разбор слов: <b>€{cost_day:.2f}</b> ({calls_day} вызовов)",
        "",
        "<b>За неделю</b>",
        f"свой словарь {week['served']} · основа {week['seeded']} · GPT {week['gpt']} · "
        f"своими силами {_share(week)}",
        f"расход €{cost_week:.2f} ({calls_week} вызовов)",
        "",
        f"Разбор есть у {words_total - without_card} слов из {words_total}.",
    ]
    if without_card:
        lines.append(f"Осталось наполнить {without_card} — это ещё ~{nights} ноч"
                     f"{'ь' if nights == 1 else 'и' if nights < 5 else 'ей'}.")
    else:
        lines.append("✅ Все слова разобраны.")
    return "\n".join(lines)


def send_dictionary_layer_report() -> dict:
    """Отправить отчёт админам в личку. Возвращает короткий итог для журнала."""
    import os

    import requests

    from backend.database import get_admin_telegram_ids

    text = build_report_text()
    token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
    admin_ids = sorted(int(a) for a in (get_admin_telegram_ids() or []) if int(a) > 0)
    if not token or not admin_ids:
        return {"sent": 0, "reason": "нет токена или админов"}
    sent = 0
    for uid in admin_ids:
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": uid, "text": text, "parse_mode": "HTML",
                      "disable_web_page_preview": True},
                timeout=20,
            )
            sent += 1
        except Exception:
            logging.warning("dictionary layer report send failed for %s", uid, exc_info=True)
    return {"sent": sent}
