# -*- coding: utf-8 -*-
"""Слова, которых не подтвердил ни один справочник: разбор владельцем в личке.

ЗАЧЕМ. Дверь словаря (`backend/german_word_gate.py`) чинит написание по источнику, а
когда источник молчит — НЕ выбрасывает слово и НЕ придумывает ответ: слово ложится с
пометкой и ждёт человека. Функция `words_awaiting_owner` собирала такие слова с
19.08.2026 — и её не вызывал никто. Слова копились, до владельца не доходили.

Владелец 23.08.2026 нашёл три чужих слова в немецком словаре («slay», «bore»,
«aspettiamo») только потому, что я наткнулся на них руками, разбирая глаголы. Это тот
самый мёртвый список, который правилами запрещён: «положил в список» — не результат.

    «Я иду по улице с телефоном, приходит такое сообщение — мои действия какие?»

Отсюда устройство: одно слово — одно сообщение, под ним кнопки, одно нажатие решает.
Никаких списков, которые надо куда-то нести.

ЧТО ДЕЛАЕТ «УБРАТЬ». Слово, его двери поиска и запись общего кеша уходят всегда —
никто больше это слово не найдёт. Личные карточки людей стираются ТОЛЬКО у обрубков и
опечаток: их человек учит наизусть. Настоящее чужое слово («Sweatpants») человек сохранил
осознанно, и его карточка остаётся (решение владельца 23.08.2026). Снимок — всегда.

ЧТО СПРАШИВАЕТСЯ. Только то, где машина исчерпала себя: вердикт «не слово» или «не
подтверждено». «Справочник молчал» сюда не попадает по построению — такой ответ не
считается окончательным и в кеш не ложится, его переспросит ночь.

КНОПКИ ЗАВИСЯТ ОТ ВОПРОСА, потому что вопросы разные:
    чужое слово / не слово     убрать из словаря · оставить · пропустить
    форма нескольких слов      сами эти слова кнопками · убрать · пропустить
Спрошенное второй раз не приходит (отметка `reviewed`), иначе рассылку перестанут читать.

Устроено как разбор форм (`reference_forms_review.py`) и родов (`article_review.py`).
Третьего механизма рассылки в проекте заводить нельзя.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

import requests

JOB_KEY = "word_review_dm"
# Пачка маленькая: это личка, и её читают с телефона. Больше десятка сообщений подряд —
# уже свалка, а свалку пролистывают не глядя.
BATCH = max(1, int((os.getenv("WORD_REVIEW_BATCH") or "8").strip() or "8"))
_HTTP_TIMEOUT = 20

# «справочник: это форма слов rasten, rasen» → ['rasten', 'rasen']
_FORM_BASES = re.compile(r"^справочник: это форма слов\s+(.+)$")


def bases_from_source(source: str) -> list[str]:
    """Слова-кандидаты, названные справочником. Пусто — вопрос не про форму."""
    found = _FORM_BASES.match(str(source or "").strip())
    if not found:
        return []
    return [part.strip() for part in found.group(1).split(",") if part.strip()][:3]


def _keyboard(word: str, source: str) -> dict[str, Any]:
    """callback_data: wrev:<действие>:<слово>. Слово одиночное, в 64 байта влезает."""
    safe = str(word or "")[:40]
    rows: list[list[dict[str, str]]] = []
    for base in bases_from_source(source):
        # Действие «это форма слова X»: заголовок станет X тем же путём, что и у
        # починки по справочнику — со слиянием, если такое слово уже есть.
        rows.append([{"text": f"это форма слова «{base}»",
                      "callback_data": f"wrev:form|{base[:24]}:{safe}"}])
    rows.append([{"text": "🗑 убрать из словаря", "callback_data": f"wrev:drop:{safe}"},
                 {"text": "✅ слово настоящее", "callback_data": f"wrev:keep:{safe}"}])
    rows.append([{"text": "пропустить", "callback_data": f"wrev:skip:{safe}"}])
    return {"inline_keyboard": rows}


def _why(status: str, source: str) -> str:
    """Человеческое объяснение, почему слово здесь. Без имён функций и таблиц."""
    if bases_from_source(source):
        names = " и ".join(f"«{b}»" for b in bases_from_source(source))
        return (f"Это не самостоятельное слово, а форма — и подходит сразу к {names}. "
                f"Какое имелось в виду, знаешь только ты.")
    if "язык" in source:
        return ("Слово настоящее, но НЕ немецкое — модель назвала другой язык. "
                "В немецком словаре ему, скорее всего, не место.")
    if status == "не слово":
        return ("Ни один справочник такого слова не знает, и модель говорит, что его "
                "не существует. Похоже на обрубок или опечатку.")
    if "другое написание" in source:
        return ("Модель предложила другое написание, но справочник его не подтвердил. "
                "Само мы такое не чиним — это была бы догадка.")
    return ("Справочники немецкого этого слова не знают, но модель говорит, что слово "
            "настоящее: редкое, разговорное или совсем новое.")


def _word_text(word: str, status: str, source: str, pos: str,
               *, index: int, total: int, left: int) -> str:
    head = f"<b>{word}</b>"
    if pos:
        head += f" — {pos}"
    return (f"{head}\n{_why(status, source)}\n"
            f"<i>{index} из {total} · всего ждёт: {left}</i>")


def send_word_review_dm(*, force: bool = False) -> dict[str, Any]:
    """Порция слов без подтверждения — админам, по одному сообщению с кнопками."""
    from backend.database import (
        claim_scheduler_run_guard,
        finish_scheduler_run_guard,
        get_admin_telegram_ids,
    )
    from backend.german_word_gate import (
        count_words_awaiting_owner,
        ensure_word_check_schema,
        words_awaiting_owner,
    )

    now = datetime.now(timezone.utc)
    run_period = now.strftime("%Y-%m-%d")
    if not force and not claim_scheduler_run_guard(
        job_key=JOB_KEY, run_period=run_period, target_scope="global",
        metadata={"batch": BATCH},
    ):
        return {"ok": True, "skipped": True, "reason": "already_claimed"}

    ensure_word_check_schema()
    token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
    admin_ids = sorted(int(a) for a in (get_admin_telegram_ids() or []) if int(a) > 0)
    if not token or not admin_ids:
        return {"ok": False, "error": "no_token_or_admins"}

    items = words_awaiting_owner(limit=BATCH)
    left = count_words_awaiting_owner()
    if not items:
        if not force:
            finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                       target_scope="global", status="completed",
                                       metadata={"sent": 0, "reason": "nothing_to_review"})
        return {"ok": True, "sent": 0, "reason": "nothing_to_review"}

    head = ("🔎 <b>Слова, которых не знает ни один справочник</b>\n"
            f"Таких {left}. Машина сделала всё, что могла: написание чинить нечем, "
            "выдумывать запрещено.\nОдно нажатие на слово — и вопрос закрыт.")
    sent = 0
    delivered_to = 0
    for uid in admin_ids:
        ok_here = False
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": uid, "text": head, "parse_mode": "HTML"},
                timeout=_HTTP_TIMEOUT)
            ok_here = resp.status_code < 400
        except Exception:
            logging.warning("разбор слов: шапка не ушла uid=%s", uid, exc_info=True)
        for i, (word, status, source, pos) in enumerate(items, 1):
            try:
                resp = requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": uid,
                          "text": _word_text(word, status, source, pos,
                                             index=i, total=len(items), left=left),
                          "parse_mode": "HTML",
                          "reply_markup": _keyboard(word, source)},
                    timeout=_HTTP_TIMEOUT)
                if resp.status_code < 400:
                    sent += 1
                    ok_here = True
                else:
                    logging.warning("разбор слов: слово не ушло uid=%s word=%s: %s",
                                    uid, word, resp.text[:200])
            except Exception:
                logging.warning("разбор слов: слово не ушло uid=%s word=%s", uid, word,
                                exc_info=True)
        delivered_to += 1 if ok_here else 0

    if not force:
        # «Выполнено» — по ФАКТУ доставки, а не по факту отправки: иначе день, когда
        # Telegram ответил ошибкой, выглядит успешным и порция не повторится.
        finish_scheduler_run_guard(
            job_key=JOB_KEY, run_period=run_period, target_scope="global",
            status="completed" if delivered_to else "failed",
            metadata={"sent": sent, "left": left, "admins": delivered_to})
    return {"ok": bool(delivered_to), "sent": sent, "left": left}


def apply_word_review(action: str, word: str) -> str:
    """Нажатие кнопки. Возвращает человеческий текст на замену сообщения."""
    from backend.german_word_gate import confirm_word_by_owner, mark_word_reviewed

    name = str(word or "").strip()
    if not name:
        return "Пустое слово — пропускаю."

    if action.startswith("form|"):
        base = action[len("form|"):].strip()
        moved, why = _retitle_to_base(name, base)
        if not moved:
            return f"⚠️ <b>{name}</b>: не смог переименовать — {why}. Слово осталось."
        mark_word_reviewed(name)
        return f"✅ <b>{name}</b> → <b>{base}</b> — {why}."
    if action == "drop":
        dropped, why = _drop_word(name)
        if not dropped:
            return f"⚠️ <b>{name}</b>: не смог убрать — {why}. Слово осталось."
        mark_word_reviewed(name)
        return f"🗑 <b>{name}</b> убрано из словаря — {why}.\nВернуть можно, снимок сохранён."
    if action == "keep":
        # НЕ «молчи», а «слово настоящее». Дальше оно идёт обычным ночным путём:
        # род — справочник/банк родов/правило составного слова, формы — справочник,
        # разбор составного, модель с двумя совпавшими ответами. Что не закроется —
        # вернётся отдельным узким вопросом («какой род?» с кнопками der/die/das).
        confirm_word_by_owner(name)
        return (f"✅ <b>{name}</b> — записал как настоящее слово.\n"
                f"Род и формы доберёт ночь: справочник, разбор составного слова, "
                f"модель с двойной проверкой. Чего не найдёт — спрошу отдельно.")
    if action == "skip":
        mark_word_reviewed(name)
        return f"⏭ <b>{name}</b> — пропущено."
    return f"Не понял действие «{action}»."


def _retitle_to_base(word: str, base: str) -> tuple[bool, str]:
    """Заголовок-форма становится словарным словом. Тем же путём, что ночная починка."""
    from backend.database import get_db_connection_context
    from backend.lex_units import retitle_unit
    if not base:
        return False, "не назван"
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM bt_3_lex_units WHERE lang='de' "
                            "AND lower(lemma)=lower(%s);", (word,))
                row = cur.fetchone()
                if not row:
                    return False, "такого слова в словаре уже нет"
                cur.execute("SELECT id FROM bt_3_lex_units WHERE lang='de' "
                            "AND lower(lemma)=lower(%s) AND id<>%s;", (base, int(row[0])))
                twin = cur.fetchone()
                if twin:
                    # Настоящее слово уже есть — это дубль, и его сливают, а не сносят:
                    # на строку словаря ссылаются восемь таблиц. Слияние делает
                    # scripts/merge_form_units_into_lemma.py, здесь только ставим метку.
                    cur.execute(
                        """INSERT INTO bt_3_reference_forms_unresolved
                                  (word, pos, reason, reviewed, checked_at)
                           VALUES (%s, '', %s, TRUE, NOW())
                           ON CONFLICT (word) DO UPDATE SET reason = EXCLUDED.reason;""",
                        (word, f"дубль формы: настоящее слово «{base}», нужно слияние"))
                    conn.commit()
                    return True, f"такое слово уже есть, поставил на слияние"
                retitle_unit(cur, int(row[0]), base)
            conn.commit()
        return True, "заголовок исправлен, старое написание осталось дверью поиска"
    except Exception:
        logging.warning("разбор слов: не переименовал %s → %s", word, base, exc_info=True)
        return False, "ошибка записи, подробности в логах"


def word_is_garbage(word: str) -> bool:
    """Обрубок или опечатка (вердикт «не слово») — в отличие от настоящего чужого слова.

    От этого зависит, трогаем ли мы карточки людей. Решение владельца 23.08.2026:
    «да, но только для обрубков и опечаток — это мусор, и человек его учит. Для чужих
    слов вроде Sweatpants — нет: человек сохранил его осознанно, это его право.»
    """
    from backend.german_word_gate import NOT_A_WORD
    try:
        return _verdict_of(word) == NOT_A_WORD
    except Exception:
        # Не смогли прочитать вердикт — считаем, что слово НЕ мусор. Ошибаться нужно в
        # сторону сохранности чужих данных: лишняя карточка безобиднее стёртой.
        logging.warning("разбор слов: не прочитал вердикт %s", word, exc_info=True)
        return False


def _verdict_of(word: str) -> str:
    """Вердикт двери по этому написанию. Пусто — двери о нём ничего не известно."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM bt_3_word_check "
                        "WHERE lower(asked) = lower(%s);", (str(word or "").strip(),))
            row = cur.fetchone()
    return str(row[0]) if row else ""


def _drop_word(word: str) -> tuple[bool, str]:
    """Убрать слово из немецкого словаря. Снимок сохраняется всегда — вернуть можно.

    ЧТО УБИРАЕТСЯ ВСЕГДА: слово общего словаря, его двери поиска, запись общего кеша
    переводов. После этого слово не находится никем.

    ЧТО УБИРАЕТСЯ ТОЛЬКО У МУСОРА: личные карточки людей. Обрубок («Abschiebu»,
    «-künfte», «Tärigkeiten») человек учит наизусть, и оставлять его в личном списке
    нельзя. Настоящее чужое слово («Sweatpants») человек сохранил осознанно — его
    карточка остаётся. Решение владельца 23.08.2026.
    """
    from backend.database import get_db_connection_context
    reason = "решение владельца в личке (разбор слов)"
    garbage = word_is_garbage(word)
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM bt_3_lex_units WHERE lang='de' "
                            "AND lower(lemma)=lower(%s);", (word,))
                row = cur.fetchone()
                unit_id = int(row[0]) if row else 0
                if unit_id:
                    cur.execute(
                        """
                        INSERT INTO bt_3_lex_units_removed (
                            reason, unit_id, lang, kind, lemma, lemma_key, pos, gender,
                            display, card, surfaces, links, senses)
                        SELECT %s, u.id, u.lang, u.kind, u.lemma, u.lemma_key, u.pos,
                               u.gender, u.display, u.card,
                               COALESCE((SELECT jsonb_agg(to_jsonb(s))
                                           FROM bt_3_lex_surfaces s
                                          WHERE s.unit_id = u.id), '[]'::jsonb),
                               COALESCE((SELECT jsonb_agg(to_jsonb(l))
                                           FROM bt_3_lex_links l
                                          WHERE l.from_unit = u.id OR l.to_unit = u.id),
                                        '[]'::jsonb),
                               COALESCE((SELECT jsonb_agg(to_jsonb(x))
                                           FROM bt_3_lex_senses x
                                          WHERE x.unit_id = u.id), '[]'::jsonb)
                          FROM bt_3_lex_units u WHERE u.id = %s;
                        """,
                        (reason, unit_id),
                    )
                    cur.execute("DELETE FROM bt_3_lex_units WHERE id = %s;", (unit_id,))
                cur.execute("DELETE FROM bt_3_dictionary_entries "
                            "WHERE source_lang='de' AND lower(source_text)=lower(%s);",
                            (word,))
                cards = 0
                if garbage:
                    # Снимок КАЖДОЙ карточки целиком: это данные человека, и вернуть их
                    # должно быть можно так же, как слово словаря.
                    cur.execute(
                        """INSERT INTO bt_3_lex_units_removed
                                  (reason, unit_id, lang, kind, display, card)
                           SELECT %s, NULL, 'de', 'личная карточка', q.word_de,
                                  to_jsonb(q)
                             FROM bt_3_webapp_dictionary_queries q
                            WHERE q.source_lang = 'de' AND lower(q.word_de) = lower(%s);""",
                        (reason, word),
                    )
                    cur.execute(
                        """DELETE FROM bt_3_webapp_dictionary_queries
                            WHERE source_lang = 'de' AND lower(word_de) = lower(%s);""",
                        (word,))
                    cards = cur.rowcount
            conn.commit()
        if not unit_id and not cards:
            return False, "такого слова в словаре уже нет"
        if garbage:
            return True, (f"обрубок: слово, поиск и {cards} личных карточек сняты"
                          if cards else "обрубок: слово и поиск сняты")
        return True, "слово и общий поиск сняты, личные карточки людей не тронуты"
    except Exception:
        logging.warning("разбор слов: не убрал %s", word, exc_info=True)
        return False, "ошибка записи, подробности в логах"
