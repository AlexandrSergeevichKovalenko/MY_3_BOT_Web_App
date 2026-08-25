"""Разбор артиклей в личке: слово → тап по der/die/das → слово в тренировке.

Зачем. Арбитр рода (article_authority) честно молчит, когда ни Wiktionary, ни правило
композита род не подтверждают: такая строка ложится verified=False и в игру не идёт —
чтобы человек не заучил выдуманный моделью артикль. Но лежать молча она не должна:
владелец её не видит, а слово не работает.

Цикл замкнут здесь: раз в N дней приходит личка с этими словами, у каждого три кнопки.
Один тап — артикль записан, verified=True, слово в тренировке. Кнопка «не нужно» —
слово уходит из показа (двухродовое, мусор, опечатка).

Отправка защищена run-guard'ом (как остальные наши отчёты), чтобы при перезапуске
планировщика письмо не задваивалось.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import requests

JOB_KEY = "article_review_dm"
_HTTP_TIMEOUT = 15
# Сколько слов за один раз. Больше — личка превращается в свалку и её перестают читать.
BATCH = max(1, int((os.getenv("ARTICLE_REVIEW_BATCH") or "8").strip() or "8"))
# Раз в сколько дней спрашивать. Слишком часто — раздражает, слишком редко — слова стоят.
EVERY_DAYS = max(1, int((os.getenv("ARTICLE_REVIEW_EVERY_DAYS") or "3").strip() or "3"))
# Через сколько дней вернуть в очередь слово, на которое владелец не ответил.
# Решение владельца 25.08.2026: «напомнить через неделю». Ноль здесь недопустим —
# неотвеченное слово без рода не попадает в игру вообще, и потеряться оно не имеет права.
REASK_DAYS = max(1, int((os.getenv("ARTICLE_REVIEW_REASK_DAYS") or "7").strip() or "7"))


def _tz_name() -> str:
    return (os.getenv("ARTICLE_REVIEW_TZ") or os.getenv("TRIAL_POLICY_TZ") or "Europe/Vienna")


def _keyboard(row_id: int) -> dict[str, Any]:
    """der / die / das + «не нужно». callback_data: artrev:<действие>:<id>."""
    return {
        "inline_keyboard": [
            [
                {"text": "der", "callback_data": f"artrev:der:{row_id}"},
                {"text": "die", "callback_data": f"artrev:die:{row_id}"},
                {"text": "das", "callback_data": f"artrev:das:{row_id}"},
            ],
            [{"text": "🚫 не нужно в игре", "callback_data": f"artrev:skip:{row_id}"}],
        ]
    }


def _word_text(item: dict, *, index: int, total: int, left: int) -> str:
    word = item.get("word") or ""
    meaning = str(item.get("meaning_ru") or "").strip()
    draft = str(item.get("draft_article") or "").strip()
    lines = [f"<b>{word}</b>"]
    if meaning:
        lines.append(f"<i>{meaning}</i>")
    lines.append("")
    lines.append("Род не подтвердили ни Wiktionary, ни правило композита.")
    if draft:
        lines.append(f"Модель предполагала «{draft}» — но это только догадка.")
    asked = item.get("asked_at")
    if asked:
        # Повтор виден словами: иначе залежавшееся слово неотличимо от нового, и
        # владелец не понимает, почему одно и то же приходит снова (25.08.2026).
        lines.append(f"⏳ Спрашиваю повторно — ждёт с {asked.strftime('%d.%m')}.")
    lines.append("")
    lines.append(f"<i>{index} из {total} · всего ждёт: {left}</i>")
    return "\n".join(lines)


def send_article_review_dm(*, force: bool = False) -> dict[str, Any]:
    """Разослать админам порцию слов без подтверждённого рода. Run-guard по периоду."""
    from backend.database import (
        get_admin_telegram_ids,
        list_unverified_article_nouns,
        count_unverified_article_nouns,
        mark_article_nouns_asked,
        claim_scheduler_run_guard,
        finish_scheduler_run_guard,
    )
    now = datetime.now(timezone.utc)
    # Период = номер N-дневного окна, чтобы «раз в 3 дня» держалось само собой.
    run_period = f"{now.year}-{now.timetuple().tm_yday // EVERY_DAYS}"
    if not force and not claim_scheduler_run_guard(
        job_key=JOB_KEY, run_period=run_period, target_scope="global",
        metadata={"every_days": EVERY_DAYS},
    ):
        return {"ok": True, "skipped": True, "reason": "already_claimed"}
    try:
        token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
        admin_ids = sorted(int(a) for a in (get_admin_telegram_ids() or []) if int(a) > 0)
        if not token or not admin_ids:
            return {"ok": False, "error": "no_token_or_admins"}

        left = count_unverified_article_nouns()
        # Спрошенное в очередь не попадает: его карточки уже висят у владельца в чате.
        items = list_unverified_article_nouns(limit=BATCH, reask_days=REASK_DAYS)
        if not items and left:
            # Слова есть, но все уже спрошены. Это НЕ «разбирать нечего»: молчать здесь
            # значит соврать, что очередь пуста. Говорим числом, сколько висит.
            # Захваченный run-guard закрываем ЗДЕСЬ ЖЕ, иначе он остался бы 'running'
            # и следующий запуск в этом же окне не состоялся бы вовсе.
            if not force:
                finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                           target_scope="global", status="completed",
                                           metadata={"sent": 0, "reason": "already_asked",
                                                     "left": left})
            return {"ok": True, "sent": 0, "reason": "already_asked", "left": left}
        if not items:
            if not force:
                finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                           target_scope="global", status="completed",
                                           metadata={"sent": 0, "reason": "nothing_to_review"})
            return {"ok": True, "sent": 0, "reason": "nothing_to_review"}

        head = (f"🔤 <b>Артикли на подтверждение</b>\n"
                f"Эти слова ждут рода — без него они не попадают в тренировку.\n"
                f"Жми der/die/das: слово сразу уходит в игру.")
        sent = 0
        for uid in admin_ids:
            try:
                # Шапка идёт через честную доставку: Telegram отвечает на отказ телом,
                # а не исключением, и прежний вызов не смотрел на ответ вовсе.
                from backend.telegram_delivery import send_telegram_message
                дошла, почему = send_telegram_message(
                    chat_id=uid, text=head, token=token, what="шапка разбора артиклей")
                if not дошла:
                    logging.warning("шапка разбора не дошла до %s: %s", uid, почему)
                    continue
                все_дошли = True
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
                        logging.warning("article review DM failed uid=%s: %s", uid, resp.text[:200])
                        все_дошли = False
                if все_дошли:
                    # «Отправлено» — это доставлено, а не «вызвали отправку». Раньше
                    # счётчик рос даже когда отказ был записан строкой выше, и работа
                    # отчитывалась «выполнено» с числом, которого не было.
                    sent += 1
            except Exception:
                logging.warning("article review DM failed uid=%s", uid, exc_info=True)
        if sent:
            # Пометка ставится ПОСЛЕ доставки. Пометить недоставленное значило бы
            # спрятать слово из очереди, ничего не спросив, — оно осталось бы без рода
            # и вне игры навсегда, и никто бы этого не увидел.
            mark_article_nouns_asked([int(i["id"]) for i in items])
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
        logging.exception("article review DM failed")
        return {"ok": False, "error": str(exc)}


def apply_review(action: str, row_id: int, *, admin_id: int | None = None) -> str:
    """Применить тап админа. Возвращает готовый текст для ответа в личку."""
    from backend.database import (
        confirm_article_noun, skip_article_noun, count_unverified_article_nouns,
    )
    act = str(action or "").strip().lower()
    if act == "skip":
        res = skip_article_noun(int(row_id))
        if not res:
            return "Слово уже обработано."
        return f"🚫 <b>{res['word']}</b> убрано из игры."
    if act not in ("der", "die", "das"):
        return "Не понял действие."
    res = confirm_article_noun(int(row_id), act, admin_id=admin_id)
    if not res:
        return "Слово уже обработано."
    if res.get("rejected"):
        # Заголовок негодный — тап владельца его не исправляет. Говорим прямо, что
        # именно не так, и показываем правильное написание: решение всё равно за ним,
        # но принимать его вслепую он больше не будет.
        return (f"⛔️ <b>{res['word']}</b> — в игру не пустил.\n"
                f"В заголовке карточки стоит артикль, а должно быть только слово: "
                f"<b>{res['suggested']}</b>.\n"
                f"Такая карточка показывала бы ответ прямо в вопросе.")
    left = count_unverified_article_nouns()
    tail = f"\nОсталось на подтверждение: {left}." if left else "\nБольше слов на подтверждение нет 🎉"
    return f"✅ <b>{res['article']} {res['word']}</b> — записано, слово в тренировке.{tail}"
