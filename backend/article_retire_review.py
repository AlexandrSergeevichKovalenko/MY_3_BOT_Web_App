"""Разбор снятых слов в личке: слово → «вернуть в игру» или «нет, мусор».

Зачем. Чистка словника сняла с показа 2 929 слов. Автоматика ошибается в обе стороны, а
владелец снятое не видит: слово просто исчезает, и заметить это можно только случайно.
Смотреть весь список глазами тоже нельзя — 2 784 слова там вообще нет в частотном списке
(«Schmetterlings-Tramete»), и их проверять незачем.

Поэтому спрашиваем ТОЛЬКО про спорное: снятые слова, которые по частотности выглядят
ходовыми (ранг лучше RETIRE_REVIEW_MAX_RANK). Таких 145, а не три тысячи — это пачек на
две недели по 10 штук.

Цикл замкнут: раз в день приходит порция с двумя кнопками. «Вернуть» — слово снова в
игре. «Мусор» — остаётся снятым и уходит в стоп-лист, чтобы набор не предложил его
заново. Спрошенное слово второй раз не приходит (retire_reviewed).
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import requests

JOB_KEY = "article_retire_review_dm"
_HTTP_TIMEOUT = 15
# Сколько слов за раз. Больше — личка превращается в свалку и её перестают читать.
BATCH = max(1, int((os.getenv("RETIRE_REVIEW_BATCH") or "10").strip() or "10"))
# Докуда слово считается ходовым и достойным вопроса. Дальше — снято по делу.
MAX_RANK = max(1, int((os.getenv("RETIRE_REVIEW_MAX_RANK") or "60000").strip() or "60000"))


def _keyboard(row_id: int) -> dict[str, Any]:
    """Вернуть / оставить снятым. callback_data: artret:<действие>:<id>."""
    return {
        "inline_keyboard": [[
            {"text": "✅ вернуть в игру", "callback_data": f"artret:back:{row_id}"},
            {"text": "🚫 мусор", "callback_data": f"artret:keep:{row_id}"},
        ]]
    }


def _word_text(item: dict, *, index: int, total: int, left: int) -> str:
    word = str(item.get("word") or "")
    article = str(item.get("article") or "")
    stored = str(item.get("stored_article") or "")
    meaning = str(item.get("meaning_ru") or "").strip()
    rank = item.get("rank")
    lines = [f"<b>{article} {word}</b>".strip()]
    if meaning:
        lines.append(f"<i>{meaning}</i>")
    lines.append("")
    lines.append("Слово убрано из игры при чистке словника.")
    if rank:
        lines.append(f"Но по частоте оно на {int(rank)}-м месте в немецком — похоже на ходовое.")
    if stored and stored.lower() != article.lower():
        # Кривой артикль часто и был причиной снятия. Показываем ПРОВЕРЕННЫЙ и говорим
        # прямо, что вернём именно его — иначе человек вернёт в игру старую ошибку.
        lines.append(f"⚠️ В базе стояло «{stored} {word}» — справочник говорит «{article}». "
                     f"Вернём с «{article}».")
    lines.append("")
    lines.append(f"<i>{index} из {total} · ещё в очереди: {left}</i>")
    return "\n".join(lines)


def send_retire_review_dm(*, force: bool = False) -> dict[str, Any]:
    """Порция спорных снятых слов админам. Run-guard, чтобы не задваивалось."""
    from backend.database import (
        get_admin_telegram_ids,
        list_retired_review_candidates,
        count_retired_review_candidates,
        claim_scheduler_run_guard,
        finish_scheduler_run_guard,
    )
    now = datetime.now(timezone.utc)
    run_period = now.strftime("%Y-%m-%d")
    if not force and not claim_scheduler_run_guard(
        job_key=JOB_KEY, run_period=run_period, target_scope="global",
        metadata={"batch": BATCH, "max_rank": MAX_RANK},
    ):
        return {"ok": True, "skipped": True, "reason": "already_claimed"}
    try:
        token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
        admin_ids = sorted(int(a) for a in (get_admin_telegram_ids() or []) if int(a) > 0)
        if not token or not admin_ids:
            return {"ok": False, "error": "no_token_or_admins"}

        items = list_retired_review_candidates(limit=BATCH, max_rank=MAX_RANK)
        left = count_retired_review_candidates(max_rank=MAX_RANK)
        if not items:
            if not force:
                finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                           target_scope="global", status="completed",
                                           metadata={"sent": 0, "reason": "nothing_to_review"})
            return {"ok": True, "sent": 0, "reason": "nothing_to_review"}

        head = ("🧹 <b>Снятые слова — проверь, не лишнее ли убрали</b>\n"
                "Эти слова чистка убрала из игры, но по частоте они выглядят ходовыми.\n"
                "«Вернуть» — слово снова в тренировке. «Мусор» — остаётся снятым навсегда.")
        sent = 0
        for uid in admin_ids:
            try:
                requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                              json={"chat_id": uid, "text": head, "parse_mode": "HTML",
                                    "disable_web_page_preview": True}, timeout=_HTTP_TIMEOUT)
                for i, item in enumerate(items, start=1):
                    resp = requests.post(
                        f"https://api.telegram.org/bot{token}/sendMessage",
                        json={"chat_id": uid,
                              "text": _word_text(item, index=i, total=len(items), left=left),
                              "parse_mode": "HTML",
                              "reply_markup": _keyboard(int(item["id"]))},
                        timeout=_HTTP_TIMEOUT,
                    )
                    if resp.status_code >= 400:
                        logging.warning("retire review DM failed uid=%s: %s", uid, resp.text[:200])
                sent += 1
            except Exception:
                logging.warning("retire review DM failed uid=%s", uid, exc_info=True)
        if not force:
            finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                       target_scope="global", status="completed",
                                       metadata={"sent": sent, "words": len(items)})
        return {"ok": True, "sent": sent, "words": len(items), "left": left}
    except Exception as exc:
        if not force:
            finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                       target_scope="global", status="failed",
                                       metadata={"error": str(exc)})
        logging.exception("retire review DM failed")
        return {"ok": False, "error": str(exc)}


def apply_retire_review(action: str, row_id: int) -> str:
    """Применить тап админа. Возвращает готовый текст для ответа в личку."""
    from backend.database import (
        restore_retired_article_noun, keep_retired_article_noun,
        count_retired_review_candidates,
    )
    act = str(action or "").strip().lower()
    if act == "back":
        res = restore_retired_article_noun(int(row_id))
        if not res:
            return "Слово уже разобрано."
        if res.get("blocked"):
            return (f"⚠️ <b>{res['word']}</b> не вернул: род не подтверждён ({res.get('reason')}).\n"
                    "Вернуть слово с догадкой вместо артикля — значит учить человека ошибке.")
        head = f"✅ <b>{res['article']} {res['word']}</b> — снова в игре."
        stored = str(res.get("stored_article") or "")
        if stored and stored.lower() != str(res["article"]).lower():
            head += f"\nАртикль поправлен: в базе стояло «{stored}», записал «{res['article']}» ({res.get('source')})."
    elif act == "keep":
        res = keep_retired_article_noun(int(row_id))
        if not res:
            return "Слово уже разобрано."
        head = f"🚫 <b>{res['word']}</b> — остаётся снятым, в набор больше не попадёт."
    else:
        return "Не понял действие."
    left = count_retired_review_candidates(max_rank=MAX_RANK)
    tail = f"\nЕщё спорных: {left}." if left else "\nСпорных слов больше нет 🎉"
    return head + tail
