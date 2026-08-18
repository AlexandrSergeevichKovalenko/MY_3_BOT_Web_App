# -*- coding: utf-8 -*-
"""Слова, которым каскад не нашёл форм: разбор владельцем в личке, три раза в неделю.

Зачем. Каскад форм (`backend/german_reference_forms.py`) закрывает слово справочником,
разбором составного слова или моделью с двумя совпавшими ответами. Что не закрылось
ничем — НЕ исчезает и НЕ подставляется наугад: оно попадает сюда.

Владелец 17.08.2026: «отчёт мне обязательно в личку по количеству слов, и сами эти слова
нужно чтобы приходили с кнопками, чтобы я их мог самостоятельно на моё усмотрение
проработать и дать им артикль». Периодичность — три раза в неделю (пн/ср/пт).

Устроено как разбор снятых слов (`article_retire_review.py`): порция с кнопками,
спрошенное второй раз не приходит. Второго механизма рассылки в проекте заводить нельзя.

Кнопки разные по частям речи, потому что вопрос разный:
    существительное — der / die / das (артикль владельца ложится в банк как «owner»)
    прилагательное  — «без степеней» (слово не сравнивается — это законный ответ)
    оба             — «пропустить» (не знаю / разберусь позже)
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import requests

JOB_KEY = "reference_forms_review"
BATCH = max(1, int((os.getenv("REFERENCE_FORMS_REVIEW_BATCH") or "20").strip() or "20"))


def _keyboard(word: str, pos: str) -> dict[str, Any]:
    """callback_data: reffrm:<действие>:<слово>. Слово короткое, в 64 байта влезает."""
    safe = str(word or "")[:40]
    if pos == "noun":
        row = [{"text": "der", "callback_data": f"reffrm:der:{safe}"},
               {"text": "die", "callback_data": f"reffrm:die:{safe}"},
               {"text": "das", "callback_data": f"reffrm:das:{safe}"}]
    else:
        row = [{"text": "без степеней", "callback_data": f"reffrm:nodeg:{safe}"}]
    return {"inline_keyboard": [row, [{"text": "пропустить",
                                       "callback_data": f"reffrm:skip:{safe}"}]]}


def _word_text(word: str, pos: str, *, index: int, total: int, left: int) -> str:
    kind = {"noun": "существительное", "adjective": "прилагательное",
            "adverb": "наречие"}.get(pos, pos)
    return (f"<b>{word}</b> — {kind}\n"
            f"Форм нет ни в справочнике, ни через разбор составного слова, "
            f"ни от модели.\n"
            f"<i>{index} из {total} · всего ждёт разбора: {left}</i>")


def send_reference_forms_review_dm(*, force: bool = False) -> dict[str, Any]:
    """Порция слов без форм — админам. Run-guard, чтобы не задваивалось."""
    from backend.database import (
        claim_scheduler_run_guard,
        finish_scheduler_run_guard,
        get_admin_telegram_ids,
    )
    from backend.german_reference_forms import unresolved_batch, unresolved_count

    now = datetime.now(timezone.utc)
    run_period = now.strftime("%Y-%m-%d")
    if not force and not claim_scheduler_run_guard(
        job_key=JOB_KEY, run_period=run_period, target_scope="global",
        metadata={"batch": BATCH},
    ):
        return {"ok": True, "skipped": True, "reason": "already_claimed"}

    token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
    admin_ids = sorted(int(a) for a in (get_admin_telegram_ids() or []) if int(a) > 0)
    if not token or not admin_ids:
        return {"ok": False, "error": "no_token_or_admins"}

    items = unresolved_batch(limit=BATCH)
    left = unresolved_count()
    if not items:
        if not force:
            finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                       target_scope="global", status="completed",
                                       metadata={"sent": 0, "reason": "nothing_to_review"})
        return {"ok": True, "sent": 0, "reason": "nothing_to_review"}

    head = ("📇 <b>Слова без форм — нужен твой ответ</b>\n"
            f"Каскад не смог закрыть {left} слов: справочник их не знает, составными они "
            "не разбираются, и два ответа модели разошлись.\n"
            "Ничего не подставлено — эти слова ждут решения.")
    sent = 0
    delivered_to = 0
    for uid in admin_ids:
        ok_here = False
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": uid, "text": head, "parse_mode": "HTML"}, timeout=20)
            ok_here = resp.status_code < 400
        except Exception:
            logging.warning("разбор форм: шапка не ушла uid=%s", uid, exc_info=True)
        for i, (word, pos, _reason) in enumerate(items, 1):
            try:
                resp = requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": uid,
                          "text": _word_text(word, pos, index=i, total=len(items), left=left),
                          "parse_mode": "HTML",
                          "reply_markup": _keyboard(word, pos)}, timeout=20)
                if resp.status_code < 400:
                    sent += 1
                    ok_here = True
                else:
                    logging.warning("разбор форм: слово не ушло uid=%s word=%s: %s",
                                    uid, word, resp.text[:200])
            except Exception:
                logging.warning("разбор форм: слово не ушло uid=%s word=%s", uid, word,
                                exc_info=True)
        delivered_to += 1 if ok_here else 0

    if not force:
        # «Выполнено» ставится по ФАКТУ доставки хотя бы одному админу, а не по факту
        # отправки. Иначе день, когда Telegram ответил ошибкой, выглядит успешным, и
        # порция не повторится.
        finish_scheduler_run_guard(
            job_key=JOB_KEY, run_period=run_period, target_scope="global",
            status="completed" if delivered_to else "failed",
            metadata={"sent": sent, "left": left, "admins": delivered_to})
    return {"ok": bool(delivered_to), "sent": sent, "left": left}


def apply_reference_forms_review(action: str, word: str) -> str:
    """Нажатие кнопки. Возвращает человеческий текст для замены сообщения."""
    from backend.german_reference_forms import mark_unresolved_reviewed

    name = str(word or "").strip()
    if not name:
        return "Пустое слово — пропускаю."

    if action in ("der", "die", "das"):
        stored = _store_owner_article(name, action)
        mark_unresolved_reviewed(name)
        return (f"✅ <b>{action} {name}</b> — записал как твоё решение."
                if stored else
                f"⚠️ <b>{name}</b>: не смог записать артикль, слово осталось в очереди.")
    if action == "nodeg":
        mark_unresolved_reviewed(name)
        return f"✅ <b>{name}</b> — отмечено как несравнимое, больше не спрошу."
    if action == "skip":
        mark_unresolved_reviewed(name)
        return f"⏭ <b>{name}</b> — пропущено."
    return f"Не понял действие «{action}»."


def _store_owner_article(word: str, article: str) -> bool:
    """Решение владельца ложится в банк артиклей источником «owner».

    Не в кэш Wiktionary: тот обязан оставаться слепком справочника, иначе мы перестанем
    отличать «так напечатано в источнике» от «так решил владелец».
    """
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bt_3_article_sprint_nouns (word, article, source, verified)
                    VALUES (%s, %s, 'owner', TRUE)
                    ON CONFLICT (word) DO UPDATE
                       SET article = EXCLUDED.article, source = 'owner', verified = TRUE,
                           updated_at = NOW();
                    """,
                    (word, article),
                )
            conn.commit()
        return True
    except Exception:
        logging.warning("разбор форм: не записал артикль владельца %s", word, exc_info=True)
        return False
