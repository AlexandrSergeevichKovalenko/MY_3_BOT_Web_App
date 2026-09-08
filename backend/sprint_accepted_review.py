"""Письмо владельцу: синонимы/антонимы, которые дверь приёма не пропустила.

Владелец 06.09.2026: «не подтвердился — не показывается, считается и раз в неделю уходит
мне пачкой с кнопками оставить / убрать. Но перед этим нужно, чтобы модель проставляла
артикли, иначе откуда я узнаю правильный артикль? Как я буду решать без понимания того,
что предлагает модель?» Поэтому у каждого кандидата в письме стоят: форма модели с её
артиклем, перевод, вердикт справочника рода и кто из источников синонимию подтвердил.

Образец механики — `article_retire_review.py` (одно слово = одно сообщение, две кнопки,
run-guard, статус прогона по факту доставки). Кнопки `sacc:<keep|der|die|das|drop>:<id>`,
обработчик — bot_3.handle_synonym_review_callback, ручной вызов — /admin_synonym_review.
Каждый день в 12:45 по 20 (SYNONYM_REVIEW_BATCH) — число назвал владелец 06.09.2026.

┌─ ОТМЕНЕНО 08.09.2026. Письмо больше не планируется (scheduler_service), в очередь ───┐
│ владельцу ничего не кладётся: судья (synonym_judge) ставит точку сам. Модуль оставлен │
│ ради кнопок на уже отправленных письмах (sacc:*) и ручного вызова из консоли.        │
│ Владелец: «модель ставит итоговую точку… мне не нужно там участвовать».              │
└──────────────────────────────────────────────────────────────────────────────────────┘
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

JOB_KEY = "sprint_accepted_review_dm"
BATCH = max(1, int((os.getenv("SYNONYM_REVIEW_BATCH") or "20").strip() or "20"))

_REL_RU = {"synonym": "синоним", "antonym": "антоним"}
_SOURCE_RU = {"openthesaurus": "OpenThesaurus", "wiktionary": "Wiktionary"}
_WIKT_RU = {"listed": "есть в списке", "not_listed": "нет в списке",
            "no_page": "статьи нет", "unknown": "не удалось проверить", "": ""}


def _keyboard(row: dict) -> dict[str, Any]:
    rid = int(row["id"])
    reason = str(row.get("reason") or "")
    noun = str(row.get("noun") or "")
    ref = str(row.get("reference_article") or "")
    if reason == "article_unknown" or (noun and not ref and not row.get("stored_article")):
        # Справочник рода слова не знает — артикль называет владелец, это и есть источник.
        return {"inline_keyboard": [
            [{"text": "der", "callback_data": f"sacc:der:{rid}"},
             {"text": "die", "callback_data": f"sacc:die:{rid}"},
             {"text": "das", "callback_data": f"sacc:das:{rid}"}],
            [{"text": "🗑 убрать", "callback_data": f"sacc:drop:{rid}"}],
        ]}
    keep_text = f"✅ оставить как {ref} {noun}" if (noun and ref and ref != row.get("stored_article")) \
        else "✅ оставить"
    return {"inline_keyboard": [[
        {"text": keep_text, "callback_data": f"sacc:keep:{rid}"},
        {"text": "🗑 убрать", "callback_data": f"sacc:drop:{rid}"},
    ]]}


def _card_text(row: dict, *, index: int, total: int, left: int) -> str:
    rel = _REL_RU.get(str(row.get("relation") or ""), "синоним")
    wort = str(row.get("wort") or "")
    hint = str(row.get("hint_ru") or "").strip()
    de, ru = str(row.get("de") or ""), str(row.get("ru") or "").strip()
    reasons = list(row.get("reasons") or [])
    noun, stored, ref = (str(row.get("noun") or ""), str(row.get("stored_article") or ""),
                         str(row.get("reference_article") or ""))
    src = str(row.get("reference_source") or "")
    by = [_SOURCE_RU.get(s, s) for s in (row.get("confirmed_by") or [])]

    lines = [f"🧩 <b>{wort}</b>" + (f" <i>({hint})</i>" if hint else "") + f" — {rel} от модели:",
             f"<b>{de}</b>" + (f" · {ru}" if ru else "") , ""]
    if "article_mismatch" in reasons:
        lines.append(f"⚠️ Артикль: модель написала «{stored or 'без артикля'}», "
                     f"справочник рода говорит <b>{ref} {noun}</b> ({src}).")
    elif "article_unknown" in reasons:
        lines.append(f"❓ Артикль: справочник рода слово «{noun}» не знает ({src}). "
                     f"Модель написала «{stored or 'без артикля'}». Какой артикль верен?")
    elif noun and ref:
        lines.append(f"Артикль: {ref} {noun} — справочник согласен ({src}).")
    if str(row.get("relation") or "") == "synonym":
        if by:
            lines.append("Синонимия подтверждена: " + ", ".join(by) + ".")
        else:
            ot = row.get("ot_knows_candidate")
            ot_txt = ("слово знает, но в одном гнезде с «" + wort + "» его нет" if ot
                      else "слова не знает вовсе" if ot is False else "не проверялся")
            wt = _WIKT_RU.get(str(row.get("wikt_target") or ""), "")
            wc = _WIKT_RU.get(str(row.get("wikt_candidate") or ""), "")
            lines.append("❓ Синонимия НЕ подтверждена: OpenThesaurus — " + ot_txt
                         + (f"; Wiktionary у «{wort}» — {wt}" if wt else "")
                         + (f", у «{de}» — {wc}" if wc else "") + ".")
    jv = str(row.get("judge_verdict") or "")
    if jv:
        voice = {"gemini": "Gemini", "openai": "GPT"}.get(str(row.get("judge_voice") or ""), "модель")
        head = {"yes": "считает, что взаимозаменяемы", "unsure": "сомневается",
                "no": "считает, что НЕ взаимозаменяемы"}.get(jv, jv)
        lines.append(f"⚖️ Судья ({voice}) {head}: {row.get('judge_reason') or ''}")
        et, ec = str(row.get("judge_example_target") or ""), str(row.get("judge_example_candidate") or "")
        if et and ec:
            lines.append(f"   «{et}» → «{ec}»")
    lines.append("")
    lines.append("«Оставить» — слово войдёт в список и будет засчитываться в спринте и "
                 "показываться в тренировке. «Убрать» — не покажем никогда.")
    lines.append(f"<i>{index} из {total} · ещё в очереди: {left}</i>")
    return "\n".join(lines)


def send_synonym_review_dm(*, force: bool = False) -> dict[str, Any]:
    from backend.database import (get_admin_telegram_ids, claim_scheduler_run_guard,
                                  finish_scheduler_run_guard)
    from backend.sprint_intake import list_open_reviews, count_open_reviews, mark_asked
    from backend.telegram_delivery import send_telegram_message
    now = datetime.now(timezone.utc)
    run_period = now.strftime("%Y-%m-%d")
    if not force and not claim_scheduler_run_guard(
            job_key=JOB_KEY, run_period=run_period, target_scope="global", metadata={"batch": BATCH}):
        return {"ok": True, "skipped": True, "reason": "already_claimed"}
    try:
        token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
        admin_ids = sorted(int(a) for a in (get_admin_telegram_ids() or []) if int(a) > 0)
        if not token or not admin_ids:
            return {"ok": False, "error": "no_token_or_admins"}
        items = list_open_reviews(BATCH)
        left = max(0, count_open_reviews() - len(items))
        if not items:
            if not force:
                finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period, target_scope="global",
                                           status="completed", metadata={"sent": 0, "reason": "nothing_to_review"})
            return {"ok": True, "sent": 0, "reason": "nothing_to_review"}
        head = ("🧩 <b>Синонимы и антонимы, где судья засомневался</b>\n"
                "Список к слову пишет модель; в игру идёт то, что подтвердил словарь "
                "(OpenThesaurus или Wiktionary) или судья-модель подстановкой. Здесь — только то, "
                "где судья сомневается или справочник не знает артикль. По одному, с тем, что известно.")
        sent = 0
        for uid in admin_ids:
            дошла, почему = send_telegram_message(chat_id=uid, text=head, token=token, what="шапка разбора синонимов")
            if not дошла:
                logging.warning("шапка разбора синонимов не дошла до %s: %s", uid, почему)
                continue
            все_дошли = True
            for i, row in enumerate(items, start=1):
                ок, причина = send_telegram_message(
                    chat_id=uid, text=_card_text(row, index=i, total=len(items), left=left),
                    token=token, reply_markup=_keyboard(row), what="карточка синонима")
                if not ок:
                    logging.warning("карточка синонима не дошла до %s: %s", uid, причина)
                    все_дошли = False
            if все_дошли:
                sent += 1
        if sent:
            mark_asked([int(r["id"]) for r in items])
        if not force:
            finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period, target_scope="global",
                                       status="completed" if sent else "failed",
                                       metadata={"sent": sent, "cards": len(items), "left": left})
        if not sent:
            logging.error("sprint_accepted_review: письмо НЕ ДОСТАВЛЕНО ни одному админу")
            return {"ok": False, "sent": 0, "cards": len(items), "left": left}
        return {"ok": True, "sent": sent, "cards": len(items), "left": left}
    except Exception as exc:
        if not force:
            finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period, target_scope="global",
                                       status="failed", metadata={"error": str(exc)})
        logging.exception("synonym review DM failed")
        return {"ok": False, "error": str(exc)}


def apply_synonym_review(action: str, row_id: int) -> str:
    """Тап владельца → готовый HTML для замены сообщения."""
    from backend.sprint_intake import apply_owner_decision, count_open_reviews
    act = str(action or "").strip().lower()
    if act not in ("keep", "drop", "der", "die", "das"):
        return "Не понял действие."
    res = apply_owner_decision(int(row_id), act)
    if res is None:
        return "Уже разобрано."
    if res["kept"]:
        head = f"✅ <b>{res['de']}</b> — вошёл в список к «{res['wort']}»."
        if res.get("article_changed"):
            head += " Артикль записан по решению."
        if res.get("unretired"):
            head += f"\nСлово «{res['wort']}» снова в показе: синонимов стало достаточно."
    else:
        head = f"🗑 <b>{res['de']}</b> — убран, к «{res['wort']}» больше не предложим."
    left = count_open_reviews()
    return head + (f"\nЕщё в очереди: {left}." if left else "\nОчередь пуста 🎉")
