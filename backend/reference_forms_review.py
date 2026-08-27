# -*- coding: utf-8 -*-
"""Слова, которым каскад не нашёл форм: разбор владельцем в личке, три раза в неделю.

Зачем. Каскад форм (`backend/german_reference_forms.py`) закрывает слово справочником,
разбором составного слова или моделью с двумя совпавшими ответами. Что не закрылось
ничем — НЕ исчезает и НЕ подставляется наугад: оно попадает сюда.

Владелец 17.08.2026: «отчёт мне обязательно в личку по количеству слов, и сами эти слова
нужно чтобы приходили с кнопками, чтобы я их мог самостоятельно на моё усмотрение
проработать». Периодичность — три раза в неделю (пн/ср/пт).

┌─ ПЕРЕДЕЛАНО 27.08.2026 ПО ЗАМЕЧАНИЮ ВЛАДЕЛЬЦА. НЕ ВОЗВРАЩАТЬ КАК БЫЛО. ───────────┐
│ «Что я должен принять? В чём задача? Где подсказка модели относительно того, что   │
│ она предлагает?» — на экране было слово, строка «форм нет нигде» и кнопки          │
│ der/die/das. Три вещи были не так:                                                 │
│                                                                                    │
│ 1. Спрашивали НЕ О ТОМ. Дыра — таблица форм, а кнопки собирали артикль. Даже       │
│    верное нажатие не создавало таблицу: карточка читает кэш склонений, а артикль   │
│    ложился в банк слов игры «спринт артиклей». Артикль собирает СВОЙ механизм —    │
│    `backend/article_review.py`, и он работает.                                     │
│ 2. Кнопка не могла записать НИКОГДА: `ON CONFLICT (word)` при отсутствии такого    │
│    ключа падал всегда, а слово при этом всё равно помечалось разобранным. Ответ    │
│    «слово осталось в очереди» был неправдой.                                       │
│ 3. Подсказки не было вовсе: два ответа модели, которые как раз и разошлись,        │
│    выбрасывались. Человека звали решать, не показав ему ничего.                    │
│                                                                                    │
│ Теперь карточка показывает перевод, чего именно нет, что предложила модель и чем   │
│ варианты отличаются, а кнопка кладёт выбранное В КЭШ ФОРМ, откуда его берёт        │
│ карточка слова. «Отложить» откладывает на срок, а не хоронит навсегда.             │
└────────────────────────────────────────────────────────────────────────────────────┘
"""
from __future__ import annotations

import logging
import os
import urllib.parse
from datetime import datetime, timezone
from typing import Any

import requests

JOB_KEY = "reference_forms_review"
BATCH = max(1, int((os.getenv("REFERENCE_FORMS_REVIEW_BATCH") or "20").strip() or "20"))
_HTTP_TIMEOUT = 20

_KIND_RU = {"noun": "существительное", "adjective": "прилагательное", "adverb": "наречие"}
# Порядок полей в ответе модели и человеческие подписи к ним.
_CASES = (("nom_sg", "им."), ("gen_sg", "род."), ("dat_sg", "дат."), ("akk_sg", "вин."))
_CASES_PL = (("nom_pl", "им."), ("gen_pl", "род."), ("dat_pl", "дат."), ("akk_pl", "вин."))
_DEGREES = (("positive", "положительная"), ("comparative", "сравнительная"),
            ("superlative", "превосходная"))


def _reference_link(word: str, pos: str) -> str:
    from backend.german_reference_forms import _reference_title
    title = _reference_title(word, pos) or word
    return "https://de.wiktionary.org/wiki/" + urllib.parse.quote(title)


def _translation(word: str) -> str:
    """Перевод из словарной единицы. Пусто — значит пусто, ничего не выдумываем."""
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT card->>'word_ru' FROM bt_3_lex_units "
                    " WHERE lang = 'de' AND lower(lemma) = lower(%s) "
                    " ORDER BY updated_at DESC NULLS LAST LIMIT 1;", (str(word or "").strip(),))
                row = cur.fetchone()
    except Exception:
        logging.debug("разбор форм: перевод не прочитан для %s", word, exc_info=True)
        return ""
    return str((row or [""])[0] or "").strip()


def _variant_line(answer: dict, pos: str, number: int) -> str:
    """Один вариант модели человеческой строкой."""
    mark = "①②③④"[number - 1] if 1 <= number <= 4 else str(number)
    if pos == "noun":
        singular = " · ".join(str(answer.get(k) or "—") for k, _ in _CASES)
        plural_values = [str(answer.get(k) or "").strip() for k, _ in _CASES_PL]
        plural = " · ".join(v or "—" for v in plural_values) if any(plural_values) else "нет"
        return f"{mark} ед.: {singular}\n    мн.: {plural}"
    degrees = " · ".join(str(answer.get(k) or "—") for k, _ in _DEGREES)
    return f"{mark} {degrees}"


def _difference(answers: list[dict], pos: str) -> str:
    """Чем именно варианты расходятся. Человеку важно ровно это место, а не вся таблица."""
    if len(answers) < 2:
        return ""
    spots = []
    fields = _CASES if pos == "noun" else _DEGREES
    for key, label in fields:
        values = {str(a.get(key) or "").strip() for a in answers}
        if len(values) > 1:
            printed = " / ".join(v or "—" for v in sorted(values))
            spots.append(f"{label} {printed}")
    if pos == "noun":
        # Множественное число — ОДНОЙ строкой. Когда один вариант его вовсе не даёт, а
        # другой даёт, разница у всех четырёх падежей одна и та же, и печатать её
        # четыре раза значит утопить настоящее расхождение в шуме.
        plurals = {" · ".join(str(a.get(k) or "").strip() for k, _ in _CASES_PL)
                   for a in answers}
        if len(plurals) > 1:
            printed = " / ".join(p.strip(" ·") or "нет" for p in sorted(plurals))
            spots.append(f"мн. {printed}")
    return "Расходятся: " + "; ".join(spots) if spots else ""


def _keyboard(row_id: int, variants: int) -> dict[str, Any]:
    """callback_data: reffrm:<действие>:<номер строки>. Номер, а не слово: слово
    приходилось резать до 40 знаков, и длинное потом не находилось при нажатии."""
    rows = []
    if variants >= 1:
        rows.append([{"text": f"{'①②③④'[i]} вариант {i + 1}",
                      "callback_data": f"reffrm:v{i + 1}:{row_id}"}
                     for i in range(min(variants, 4))])
        rows.append([{"text": "🚫 ни один не верен",
                      "callback_data": f"reffrm:bad:{row_id}"}])
    else:
        rows.append([{"text": "🚫 негодный заголовок",
                      "callback_data": f"reffrm:bad:{row_id}"}])
    rows.append([{"text": "⏳ отложить", "callback_data": f"reffrm:later:{row_id}"}])
    return {"inline_keyboard": rows}


def _word_text(item: dict, *, index: int, total: int, left: int) -> str:
    from backend.german_reference_forms import POSTPONE_DAYS

    word, pos = item["word"], item["pos"]
    kind = _KIND_RU.get(pos, pos)
    translation = _translation(word)
    head = f"<b>{word}</b> — {kind}"
    if translation:
        head += f" · {translation}"

    чего_нет = ("таблицы склонения" if pos == "noun" else "степеней сравнения")
    lines = [head, f"Нет {чего_нет}: справочник их не печатает, из составного слова "
                   f"не выводятся."]

    variants = [a for a in (item.get("candidates") or []) if isinstance(a, dict)]
    if variants:
        lines.append("")
        lines.append("Модель спрошена дважды и ответила по-разному:")
        lines.append("<code>" + "\n".join(
            _variant_line(a, pos, i + 1) for i, a in enumerate(variants)) + "</code>")
        разница = _difference(variants, pos)
        if разница:
            lines.append(f"<i>{разница}</i>")
        lines.append("")
        lines.append("Выберешь вариант — он ляжет в карточку слова, и человек увидит "
                     "именно эти формы.")
    else:
        lines.append("")
        lines.append("Модель тоже не ответила — предложить нечего. Если слово настоящее, "
                     "формы придётся достать вручную; если заголовок негодный "
                     "(форма слова, опечатка, чужая часть речи) — скажи кнопкой.")

    lines.append(f"«Отложить» вернёт слово через {POSTPONE_DAYS} дней.")
    lines.append(f'<a href="{_reference_link(word, pos)}">статья в справочнике</a>')
    lines.append(f"<i>{index} из {total} · ждут ответа: {left}</i>")
    return "\n".join(lines)


def _head_text(left: int, sent: int) -> str:
    return ("📇 <b>Слова без форм — нужен твой ответ</b>\n"
            f"Не собралась таблица форм у {left} слов(а): справочник их не печатает, "
            "из составных они не выводятся, а модель либо молчит, либо дала два разных "
            "ответа.\n"
            "Ничего не подставлено. Ниже по карточке на слово: что известно, что "
            "предлагает модель и что сделает каждая кнопка.\n"
            f"Сейчас пришло: {sent}.")


def send_reference_forms_review_dm(*, force: bool = False) -> dict[str, Any]:
    """Порция слов без форм — админам. Run-guard, чтобы не задваивалось."""
    from backend.database import (
        claim_scheduler_run_guard,
        finish_scheduler_run_guard,
        get_admin_telegram_ids,
    )
    from backend.german_reference_forms import mark_asked, unresolved_batch, unresolved_count

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

    sent = 0
    delivered_to = 0
    for uid in admin_ids:
        ok_here = False
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": uid, "text": _head_text(left, len(items)),
                      "parse_mode": "HTML"}, timeout=_HTTP_TIMEOUT)
            ok_here = resp.status_code < 400
        except Exception:
            logging.warning("разбор форм: шапка не ушла uid=%s", uid, exc_info=True)
        for i, item in enumerate(items, 1):
            try:
                resp = requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": uid,
                          "text": _word_text(item, index=i, total=len(items), left=left),
                          "parse_mode": "HTML",
                          "disable_web_page_preview": True,
                          "reply_markup": _keyboard(
                              item["id"], len(item.get("candidates") or []))},
                    timeout=_HTTP_TIMEOUT)
                if resp.status_code < 400:
                    sent += 1
                    ok_here = True
                else:
                    logging.warning("разбор форм: слово не ушло uid=%s word=%s: %s",
                                    uid, item["word"], resp.text[:200])
            except Exception:
                logging.warning("разбор форм: слово не ушло uid=%s word=%s", uid,
                                item["word"], exc_info=True)
        delivered_to += 1 if ok_here else 0

    if delivered_to:
        # Отмечаем ФАКТ отправки: неотвеченное слово вернётся само через REASK_DAYS,
        # а не будет приходить каждую отправку заново.
        mark_asked([int(x["id"]) for x in items])

    if not force:
        # «Выполнено» ставится по ФАКТУ доставки хотя бы одному админу, а не по факту
        # отправки. Иначе день, когда Telegram ответил ошибкой, выглядит успешным, и
        # порция не повторится.
        finish_scheduler_run_guard(
            job_key=JOB_KEY, run_period=run_period, target_scope="global",
            status="completed" if delivered_to else "failed",
            metadata={"sent": sent, "left": left, "admins": delivered_to})
    return {"ok": bool(delivered_to), "sent": sent, "left": left,
            "words": [x["word"] for x in items]}


def apply_reference_forms_review(action: str, row_id: str | int) -> str:
    """Нажатие кнопки. Возвращает человеческий текст для замены сообщения.

    Ни одна ветка не помечает слово разобранным, если запись НЕ УДАЛАСЬ: прежний код
    делал ровно это, и слово исчезало из очереди с текстом «осталось в очереди».
    """
    from backend.german_reference_forms import (
        POSTPONE_DAYS,
        apply_owner_choice,
        mark_headword_defect,
        postpone_unresolved,
        unresolved_row,
    )

    try:
        rid = int(str(row_id).strip())
    except (TypeError, ValueError):
        return "Не понял, о каком слове речь."

    row = unresolved_row(rid)
    if not row:
        return "Это слово уже закрыто — в очереди его нет."

    if action in ("v1", "v2", "v3", "v4"):
        applied = apply_owner_choice(rid, int(action[1:]))
        if not applied:
            return (f"⚠️ <b>{row['word']}</b>: не смог записать этот вариант. "
                    "Слово ОСТАЁТСЯ в очереди — придёт снова.")
        formes = "склонение" if row["pos"] == "noun" else "степени сравнения"
        return (f"✅ <b>{row['word']}</b> — {formes} записано с твоих слов. "
                "Теперь эти формы видит человек в карточке слова.")

    if action == "bad":
        mark_headword_defect(row["word"], row["pos"], "решение владельца: формы не годятся")
        return (f"🚫 <b>{row['word']}</b> — отмечено как негодный заголовок. "
                "Больше не спрошу; слово попадёт в разбор заголовков.")

    if action == "later":
        if not postpone_unresolved(rid, POSTPONE_DAYS):
            return (f"⚠️ <b>{row['word']}</b>: не смог отложить. "
                    "Слово остаётся в очереди.")
        return f"⏳ <b>{row['word']}</b> — отложено, вернётся через {POSTPONE_DAYS} дней."

    return f"Не понял действие «{action}»."
