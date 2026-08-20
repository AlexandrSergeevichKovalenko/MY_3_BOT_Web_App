# -*- coding: utf-8 -*-
"""Отправка в Telegram, которая ЗНАЕТ, дошло ли сообщение.

ЗАЧЕМ ЭТОТ ФАЙЛ ПОЯВИЛСЯ
────────────────────────
20.08.2026 владелец сказал: «мне ничего не приходило». Отчёт о ночной проверке фраз
существует, стоит в расписании на 07:05 и должен приходить с кнопкой «Разобрать спорные
фразы». Разбор показал, что все девять мест, откуда бот пишет владельцу, устроены так:

    try:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json=payload)
    except Exception:
        logging.debug("... send failed", exc_info=True)

Здесь две дыры сразу. Первая: `except` ловит только обрыв связи — а Telegram отвечает на
отказ КОДОМ 200 или 4xx с телом `{"ok": false, "description": "..."}`, и такой отказ не
исключение, он просто игнорируется. Вторая: результат `post` никто не смотрит.

Поймано на живом примере: с мёртвым токеном Telegram вернул `401 Unauthorized`, а код
напечатал «отправлено» и пошёл дальше. То есть отчёт мог не доходить до владельца
неделями, и ни в логах, ни на экране не было бы ни следа. Это ровно тот класс, который
запрещён правилом ноль: молчаливая деградация, неотличимая от нормальной работы.

Здесь отправка возвращает ЧЕСТНЫЙ результат, а отказ Telegram попадает в лог со своим
текстом («bot was blocked by the user», «chat not found», «Unauthorized») — по нему сразу
видно, чинить токен, права или адресата.
"""
from __future__ import annotations

import logging
import os

import requests

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def send_telegram_message(
    *,
    chat_id: int | str,
    text: str,
    token: str | None = None,
    parse_mode: str | None = "HTML",
    reply_markup: dict | None = None,
    disable_web_page_preview: bool = True,
    timeout: int = 20,
    what: str = "сообщение",
) -> tuple[bool, str]:
    """Отправить и УБЕДИТЬСЯ, что Telegram принял.

    Возвращает (дошло, причина). `причина` пустая при успехе и содержит текст отказа
    Telegram или сети при неудаче — её можно положить в отчёт, а не только в лог.

    Ничего не глушит и ничего не подменяет: не дошло — так и сказано.
    """
    token = str(token or os.getenv("TELEGRAM_Deutsch_BOT_TOKEN") or "").strip()
    if not token:
        reason = "нет токена бота"
        logging.warning("телеграм: %s не отправлено — %s", what, reason)
        return False, reason
    if not chat_id:
        reason = "нет адресата"
        logging.warning("телеграм: %s не отправлено — %s", what, reason)
        return False, reason

    payload: dict = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if disable_web_page_preview:
        payload["disable_web_page_preview"] = True
    if reply_markup:
        payload["reply_markup"] = reply_markup

    try:
        response = requests.post(
            TELEGRAM_API.format(token=token), json=payload, timeout=timeout)
    except Exception as exc:  # обрыв связи, таймаут
        reason = f"сеть: {exc}"
        logging.warning("телеграм: %s не дошло до %s — %s", what, chat_id, reason)
        return False, reason

    # Отказ приходит телом ответа, а не исключением: {"ok": false, "description": "..."}
    try:
        body = response.json()
    except Exception:
        body = {}
    if response.status_code == 200 and body.get("ok"):
        return True, ""
    reason = str(body.get("description") or f"HTTP {response.status_code}").strip()
    logging.warning("телеграм: %s не дошло до %s — %s", what, chat_id, reason)
    return False, reason


def send_telegram_message_to_all(
    chat_ids, *, text: str, what: str = "сообщение", **kwargs
) -> tuple[int, list[tuple[int, str]]]:
    """То же самое для списка адресатов.

    Возвращает (сколько дошло, [(адресат, причина отказа), …]). Список отказов ПУСТОЙ,
    когда всё дошло, — по нему и решается, надо ли поднимать шум.
    """
    delivered, failures = 0, []
    for chat_id in chat_ids or []:
        ok, reason = send_telegram_message(chat_id=chat_id, text=text, what=what, **kwargs)
        if ok:
            delivered += 1
        else:
            failures.append((chat_id, reason))
    return delivered, failures
