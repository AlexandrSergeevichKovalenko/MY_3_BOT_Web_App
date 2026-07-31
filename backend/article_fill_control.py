# -*- coding: utf-8 -*-
"""Наполнение тем: отчёт раз в три дня и три кнопки на выдохшуюся тему.

Зачем. Цель «150 слов на тему» ничего не решает: в «Чувствах» ходовых слов навалом, в
«Вечеринках» их физически нет. Неприкосновенная цифра заставляла генератор скрести дно —
так в банк попали «центрифуга для салата», «тёрка для муската», «лопатка для спагетти».
Любая другая цифра была бы так же неверна, потому что для каждой темы она своя.

Решает прирост. Два прогона подряд дали меньше трёх слов — тема выдохлась, наполнение
встаёт на паузу (в игре тема остаётся), и владелец решает сам:

  • остановить      — тема живёт как есть, больше не дёргаем;
  • продолжить      — счётчик пустых прогонов обнуляется, добор идёт дальше;
  • дам свои слова  — присылаем список слов, которые в теме уже есть, и ждём ответ.

Список нужен обязательно: без него владелец пишет дубликаты, а поняв, что добавить
нечего, не может об этом сказать. Поэтому у сообщения со списком есть и кнопка отмены,
и срок: не ответил за неделю — тема тихо возвращается на паузу.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import requests

JOB_KEY = "article_fill_control_dm"
_HTTP_TIMEOUT = 15
# Раз в три дня: чаще — шум, реже — тема неделю стоит и никто не знает.
REPORT_EVERY_DAYS = max(1, int((os.getenv("FILL_CONTROL_EVERY_DAYS") or "3").strip() or "3"))
# Сколько слов темы влезает в одно сообщение со списком. Больше — телеграм режет.
WORDS_PER_MESSAGE = 120


def _keyboard(theme_key: str) -> dict[str, Any]:
    return {"inline_keyboard": [
        [{"text": "⏸ остановить", "callback_data": f"artfill:stop:{theme_key}"},
         {"text": "▶️ продолжить", "callback_data": f"artfill:go:{theme_key}"}],
        [{"text": "✍️ дам свои слова", "callback_data": f"artfill:mine:{theme_key}"}],
    ]}


def _state_ru(state: str) -> str:
    return {"auto": "добираем", "paused": "на паузе",
            "awaiting_words": "ждём твои слова"}.get(str(state), str(state))


def _words_ru(count: int) -> str:
    n = int(count)
    tail = n % 100
    if 11 <= tail <= 14:
        form = "слов"
    else:
        form = {1: "слово", 2: "слова", 3: "слова", 4: "слова"}.get(n % 10, "слов")
    return f"{n} {form}"


def report_lines(rows: list[dict]) -> list[str]:
    """Сводка на один взгляд: подробно — только то, что требует решения.

    Перечислять все 37 тем нельзя: получается стена, которую перестают читать. Растущие
    темы решения не требуют — им хватает одной строки с числом. Разворачиваем те, где
    прирост кончается или уже кончился."""
    if not rows:
        return ["Тем пока нет."]
    growing = [r for r in rows if r["state"] == "auto" and r["fresh"]]
    # Тему, набравшую цель, ночной прогон не трогает вовсе — значит и прироста у неё нет
    # по совершенно здоровой причине. Смешивать её с выдыхающейся значит пугать зря.
    full = [r for r in rows if r["state"] == "auto" and not r["fresh"]
            and r["words"] >= r["target"] > 0]
    stalled = [r for r in rows if r["state"] == "auto" and not r["fresh"] and r not in full]
    paused = [r for r in rows if r["state"] != "auto"]
    total = sum(r["words"] for r in rows)
    lines = [f"📚 <b>Наполнение тем</b>", f"Всего в игре {_words_ru(total)} в {len(rows)} темах.", ""]

    if growing:
        best = sorted(growing, key=lambda x: -x["fresh"])[:3]
        top = ", ".join(f"{r['label']} +{r['fresh']}" for r in best)
        lines.append(f"✅ <b>Растут: {len(growing)}</b> — больше всех {top}.")
        lines.append("Тут делать нечего.")
        lines.append("")
    if full:
        names = ", ".join(r["label"] for r in sorted(full, key=lambda x: -x["words"])[:4])
        more = f" и ещё {len(full) - 4}" if len(full) > 4 else ""
        lines.append(f"🎯 <b>Набрали цель: {len(full)}</b> — {names}{more}. Добор им не нужен.")
        lines.append("")
    if stalled:
        lines.append("<b>Прирост кончается</b>")
        close = 0
        for r in sorted(stalled, key=lambda x: -x["dry_streak"]):
            tail = (f" · {r['dry_streak']} пустой прогон подряд" if r["dry_streak"] == 1
                    else f" · {r['dry_streak']} пустых прогонов подряд" if r["dry_streak"]
                    else "")
            close += 1 if r["dry_streak"] >= 1 else 0
            lines.append(f"• {r['label']} — {_words_ru(r['words'])} из {r['target']}{tail}")
        if close:
            lines.append("Ещё один пустой прогон — и спрошу, что с темой делать.")
        lines.append("")
    if paused:
        lines.append("<b>Добор не идёт</b>")
        for r in paused:
            lines.append(f"• {r['label']} — {_words_ru(r['words'])} · {_state_ru(r['state'])}")
    return lines


def send_fill_control_dm(*, force: bool = False) -> dict[str, Any]:
    """Сводка по темам + кнопки под каждой выдохшейся. Run-guard, чтобы не задваивалось."""
    from backend.database import (
        get_admin_telegram_ids, list_theme_fill_report, expire_stale_awaiting_themes,
        claim_scheduler_run_guard, finish_scheduler_run_guard,
    )
    now = datetime.now(timezone.utc)
    # Период — не дата, а номер трёхдневки: иначе guard пропустит прогон через два дня.
    run_period = str(now.toordinal() // REPORT_EVERY_DAYS)
    if not force and not claim_scheduler_run_guard(
        job_key=JOB_KEY, run_period=run_period, target_scope="global",
        metadata={"every_days": REPORT_EVERY_DAYS},
    ):
        return {"ok": True, "skipped": True, "reason": "already_claimed"}
    try:
        token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
        admin_ids = sorted(int(a) for a in (get_admin_telegram_ids() or []) if int(a) > 0)
        if not token or not admin_ids:
            return {"ok": False, "error": "no_token_or_admins"}

        expired = expire_stale_awaiting_themes()
        rows = list_theme_fill_report()
        # Кнопки — только у тем, где добор встал сам. Вешать их на все 37 тем значит
        # прислать стену, которую перестанут читать.
        need_decision = [r for r in rows
                         if r["state"] == "paused" and r["dry_streak"] >= 2]
        text = "\n".join(report_lines(rows))
        if expired:
            text += ("\n\n⏳ Ждали твои слова и не дождались: "
                     + ", ".join(expired) + ". Эти темы вернул на паузу.")
        # Считаем ДОСТАВЛЕННОЕ, а не число админов в списке. Прежний счётчик рос всегда,
        # поэтому отчёт бодро говорил «отправлено 1» даже когда Telegram отвечал 401 и
        # в личку не приходило ничего.
        sent = 0
        failed: list[str] = []

        def _post(payload: dict) -> bool:
            try:
                resp = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                                     json=payload, timeout=_HTTP_TIMEOUT)
                if resp.status_code >= 400:
                    failed.append(f"{resp.status_code} {resp.text[:120]}")
                    logging.warning("fill control DM rejected: %s", resp.text[:200])
                    return False
                return True
            except Exception as exc:
                failed.append(str(exc)[:120])
                logging.warning("fill control DM failed", exc_info=True)
                return False

        for uid in admin_ids:
            ok = _post({"chat_id": uid, "text": text, "parse_mode": "HTML",
                        "disable_web_page_preview": True})
            for r in need_decision:
                body = (f"⏸ <b>{r['label']}</b> — новых слов для темы больше не находится.\n"
                        f"В теме {r['words']} слов, они остаются в игре. Последние два "
                        f"прогона не дали почти ничего: ходовых слов по этой теме, похоже, "
                        f"больше нет.\n\nЧто делаем?")
                _post({"chat_id": uid, "text": body, "parse_mode": "HTML",
                       "reply_markup": _keyboard(r["theme_key"])})
            sent += 1 if ok else 0
        if not force:
            finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                       target_scope="global",
                                       status="completed" if sent else "failed",
                                       metadata={"sent": sent, "decisions": len(need_decision),
                                                 "failed": failed[:3]})
        # ok=False, если не дошло ни до кого: «ок, отправлено 0» — это отчёт, которому
        # верят, а потом ищут сообщение, которого нет.
        return {"ok": bool(sent), "sent": sent, "themes": len(rows),
                "decisions": len(need_decision), "failed": failed[:3]}
    except Exception as exc:
        if not force:
            finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                       target_scope="global", status="failed",
                                       metadata={"error": str(exc)})
        logging.exception("fill control DM failed")
        return {"ok": False, "error": str(exc)}


def word_list_messages(theme_key: str) -> list[str]:
    """Слова темы порциями — чтобы владелец видел, чего НЕ надо присылать."""
    from backend.database import get_article_sprint_theme, list_article_sprint_words
    theme = get_article_sprint_theme(theme_key) or {}
    label = theme.get("label_ru") or theme_key
    words = sorted(list_article_sprint_words(theme_key), key=str.lower)
    if not words:
        return [f"В теме «{label}» слов пока нет — присылай любые."]
    out = []
    for i in range(0, len(words), WORDS_PER_MESSAGE):
        chunk = words[i:i + WORDS_PER_MESSAGE]
        head = (f"📋 В теме «{label}» уже есть {len(words)} слов"
                if i == 0 else f"…продолжение списка «{label}»")
        out.append(head + ":\n" + ", ".join(chunk))
    return out


def ask_for_words_text(theme_key: str) -> str:
    from backend.database import get_article_sprint_theme
    theme = get_article_sprint_theme(theme_key) or {}
    label = theme.get("label_ru") or theme_key
    return (f"✍️ <b>Свои слова для темы «{label}»</b>\n\n"
            "Ответь на ЭТО сообщение списком слов — через запятую или по одному в строке. "
            "Артикль писать не обязательно: проверю по справочнику и поставлю сам. "
            "Перевода тоже не нужно — переведу.\n\n"
            "Ничего не нашлось — нажми «не нашёл слов», и тема останется как есть.")


def apply_fill_decision(action: str, theme_key: str) -> str:
    """Тап по кнопке отчёта. Возвращает готовый текст ответа в личку."""
    from backend.database import get_article_sprint_theme, set_theme_fill_state
    theme = get_article_sprint_theme(theme_key) or {}
    label = theme.get("label_ru") or theme_key
    act = str(action or "").strip().lower()
    if act == "stop":
        set_theme_fill_state(theme_key, "paused")
        return (f"⏸ «{label}» — новых слов больше не добираем. В игре тема остаётся, слова "
                f"никуда не деваются.")
    if act == "go":
        set_theme_fill_state(theme_key, "auto")
        return (f"▶️ «{label}» — добор снова идёт. Если следующие два прогона опять дадут "
                f"пусто, спрошу ещё раз.")
    if act == "cancel":
        set_theme_fill_state(theme_key, "paused")
        return (f"🚫 «{label}» — понял, слов не нашлось. Тема на паузе, дёргать тебя по ней "
                f"больше не буду.")
    return "Не понял действие."


def decision_keyboard(action: str, theme_key: str) -> dict[str, Any] | None:
    """Кнопка обратного хода под ответом: решение не должно становиться необратимым.

    Раньше после тапа сообщение перерисовывалось в голый текст, кнопки исчезали, а вернуть
    добор можно было только командой с ключом темы, который надо где-то посмотреть."""
    act = str(action or "").strip().lower()
    if act in ("stop", "cancel"):
        return {"inline_keyboard": [[
            {"text": "▶️ включить добор", "callback_data": f"artfill:go:{theme_key}"},
            {"text": "✍️ дам свои слова", "callback_data": f"artfill:mine:{theme_key}"},
        ]]}
    if act == "go":
        return {"inline_keyboard": [[
            {"text": "⏸ остановить добор", "callback_data": f"artfill:stop:{theme_key}"}]]}
    return None


def after_words_keyboard(theme_key: str) -> dict[str, Any]:
    """Кнопки прямо под ответом о записи.

    Текст «включить кнопкой продолжить» отсылал к сообщению, которое к этому моменту
    уехало вверх по переписке на десяток экранов. Ссылаться на кнопку, которой рядом нет,
    — значит заставлять человека искать её глазами в ленте."""
    return {"inline_keyboard": [[
        {"text": "▶️ включить добор", "callback_data": f"artfill:go:{theme_key}"},
        {"text": "✍️ ещё слова", "callback_data": f"artfill:mine:{theme_key}"},
    ]]}


def split_words(raw: str) -> list[str]:
    """Список слов из сообщения: через запятую, по строкам или вперемешку."""
    parts = [p.strip(" .;·-—\t") for chunk in str(raw or "").splitlines()
             for p in chunk.split(",")]
    return [p for p in parts if p]


def review_words_text(theme_key: str, rows: list[dict], selected: list[bool]) -> str:
    """Разбор присланных слов по одному: что беру, что нет и почему.

    Раньше ответ был «добавлено 1, не взял 2» — по нему нельзя ни проверить решение, ни
    возразить. Владелец прислал три слова, два из которых глаголы, и не увидел, какие
    именно отсеялись: а вдруг машина ошиблась и выбросила нужное."""
    from backend.database import get_article_sprint_theme
    theme = get_article_sprint_theme(theme_key) or {}
    label = theme.get("label_ru") or theme_key
    lines = [f"✍️ <b>{label}</b> — проверил {_words_ru(len(rows))}:", ""]
    for i, r in enumerate(rows):
        mark = "☑️" if (i < len(selected) and selected[i]) else "⬜️"
        head = f"{mark} <b>{i + 1}. {r['article']} {r['word']}</b>".replace("  ", " ")
        tail = f" — {r['meaning_ru']}" if r.get("meaning_ru") else ""
        lines.append(head + tail)
        if r.get("reason"):
            lines.append(f"      <i>{r['reason']}</i>")
    lines.append("")
    lines.append("Отмеченные добавлю. Считаешь, что слово отсеяли зря, — отметь его сам, "
                 "твоё решение главнее.")
    return "\n".join(lines)


def commit_selected_words(theme_key: str, rows: list[dict], selected: list[bool]) -> str:
    """Записать отмеченные слова и отчитаться поимённо, а не числами."""
    from backend.article_sprint_generator import commit_manual_words
    from backend.database import set_theme_fill_state
    take = [r for i, r in enumerate(rows) if i < len(selected) and selected[i]]
    if not take:
        return "Ничего не отмечено — в тему ничего не добавил."
    res = commit_manual_words(theme_key, take) or {}
    # После своих слов тема остаётся на паузе: включать автодобор за владельца нельзя —
    # он его и остановил.
    set_theme_fill_state(theme_key, "paused")
    added = int(res.get("added") or 0)
    names = ", ".join(f"{r['article']} {r['word']}" for r in take)
    lines = [f"✅ Добавил в тему: {names}."]
    if added < len(take):
        lines.append(f"⚠️ Легло {added} из {len(take)} — остальные уже были в банке.")
    unverified = [r for r in take if not r.get("verified", True)]
    if unverified:
        lines.append("У " + ", ".join(r["word"] for r in unverified)
                     + " род не подтверждён справочником — пришлю отдельно, поставишь артикль тапом.")
    total = res.get("final_verified")
    lines.append("")
    lines.append(f"Всего в теме {_words_ru(total) if total is not None else '?'}. "
                 f"Автодобор на паузе.")
    return "\n".join(lines)
