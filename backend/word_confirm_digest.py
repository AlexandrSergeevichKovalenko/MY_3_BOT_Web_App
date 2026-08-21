# -*- coding: utf-8 -*-
"""Раз в неделю: «эти слова мы не смогли подтвердить» — человеку, одной пачкой.

Владелец 19.08.2026: «Пользователь может что-то сохранить, и наша система может это
пропустить даже при многих точках контроля. Раз в неделю отправлять ему короткое
сообщение: у тебя есть такие слова, мы не можем подтвердить, что они существуют. К
сообщению прикрепить чекбоксы — отмеченные остаются, остальные удаляются.»
И отдельно: «слова должны приходить ГРУППОЙ, а не по одному».

ПОЧЕМУ ДВА ДЕЙСТВИЯ НА СЛОВО. Владелец заметил случай, который одной галочкой не
закрыть: слово человека устраивает, а перевод — нет. Поэтому у каждого слова две
кнопки: «оставить» и «перевод неверный». Второе не удаляет слово, а ставит карточку
в очередь на пересборку ночью.

КАК ЭТО ВЫГЛЯДИТ. Одно сообщение на всю пачку. Нажатие перерисовывает то же сообщение,
человек видит своё решение сразу и не уходит в конец чата (правило проекта: отвечать
в то же сообщение). Пока «Готово» не нажато, ничего не удаляется.

ЧТО ПОПАДАЕТ В ПАЧКУ. Только слова этого человека, по которым дверь сказала
«не подтверждено» или «не слово». Подтверждённые и исправленные не тревожат никого.
"""
from __future__ import annotations

import logging
import os
from typing import Any

JOB_KEY = "word_confirm_digest"
BATCH = max(1, int((os.getenv("WORD_CONFIRM_DIGEST_BATCH") or "12").strip() or "12"))

KEEP = "keep"
BAD_TRANSLATION = "badtr"
DONE = "done"

# ── Одно опознание слова на все три места ────────────────────────────────────
# Личный словарь хранит существительное ВМЕСТЕ с артиклем: «die Abschiebung»
# (7829 строк из 25240, замер 21.08.2026). Дверь же спрашивает голое слово —
# артикль есть лишь у 1 записи из 149. Пока сравнение шло по сырому word_de,
# соединение рвалось на каждом существительном: экран проверки показывал 24
# слова вместо 100, и очевидный обрубок «das Scheinwerfergla» человек не видел.
#
# Поэтому слово опознаётся ТОЛЬКО по голой форме — и в списке, и в решениях, и в
# плашке при сохранении. Артикль здесь не часть имени слова, а часть карточки.
_BARE = "regexp_replace({col}, '^(der|die|das)[[:space:]]+', '', 'i')"


def _cleaned(text: str) -> str:
    """Общая чистка входа — та же, что стоит на всех дверях записи слова.

    Отдельной функцией, потому что чистка обязана быть ОДНОЙ на все входы: как
    только у ручного ввода появляется своя, послабленная, — через неё и приезжает
    то, от чего дверь стережёт остальных.

    Если чистка недоступна (её нет в окружении теста), возвращаем строку как есть,
    а не глушим ошибку молча: место записи всё равно одно и видно.
    """
    raw = str(text or "").strip()
    if not raw:
        return ""
    try:
        from backend.dictionary_intake import clean_text
    except Exception:
        logging.warning("проверка слов: чистка входа недоступна", exc_info=True)
        return raw
    return str(clean_text(raw) or "").strip()


def bare_word(text: str) -> str:
    """Слово без артикля — то, чем его знает дверь."""
    import re
    return re.sub(r"^(der|die|das)\s+", "", str(text or "").strip(), flags=re.I).strip()


def ensure_word_confirm_schema() -> None:
    """Состояние пачки: что человек уже отметил. Хранится до нажатия «Готово»."""
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bt_3_word_confirm_digest (
                        user_id     BIGINT NOT NULL,
                        word        TEXT NOT NULL,
                        decision    TEXT NOT NULL DEFAULT '',
                        sent_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        closed_at   TIMESTAMPTZ,
                        PRIMARY KEY (user_id, word)
                    );
                    """
                )
            conn.commit()
    except Exception:
        logging.warning("сводка слов: схема не создана", exc_info=True)


def words_for_user(user_id: int, limit: int = BATCH) -> list[tuple[str, str]]:
    """[(слово, перевод)] — неподтверждённые слова ИМЕННО этого человека."""
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT {bare}, COALESCE(q.translation_ru, '')
                      FROM bt_3_webapp_dictionary_queries q
                      JOIN bt_3_word_check w ON w.asked = {bare}
                     WHERE q.user_id = %s
                       AND w.status IN ('не подтверждено', 'не слово')
                       AND NOT EXISTS (SELECT 1 FROM bt_3_word_confirm_digest d
                                        WHERE d.user_id = q.user_id AND d.word = {bare}
                                          AND d.closed_at IS NOT NULL)
                     ORDER BY 1
                     LIMIT %s;
                    """.format(bare=_BARE.format(col="q.word_de")),
                    (int(user_id), int(limit)),
                )
                return [(str(a), str(b)) for a, b in (cur.fetchall() or [])]
    except Exception:
        logging.warning("сводка слов: не прочитал список для %s", user_id, exc_info=True)
        return []


def _decisions(user_id: int) -> dict[str, str]:
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT word, decision FROM bt_3_word_confirm_digest "
                            "WHERE user_id=%s AND closed_at IS NULL;", (int(user_id),))
                return {str(a): str(b) for a, b in (cur.fetchall() or [])}
    except Exception:
        return {}


def render(user_id: int, words: list[tuple[str, str]]) -> tuple[str, dict[str, Any]]:
    """(текст сообщения, клавиатура). Одно сообщение на всю пачку."""
    chosen = _decisions(user_id)
    lines = [
        "🔍 <b>Проверь свои слова</b>",
        "",
        "Эти слова ты сохранял, а мы не смогли подтвердить, что они есть в немецком.",
        "Отметь те, что нужно оставить. Остальные удалим.",
        "",
    ]
    for i, (word, translation) in enumerate(words, 1):
        state = chosen.get(word, "")
        mark = {"": "⬜", KEEP: "✅", BAD_TRANSLATION: "✏️"}.get(state, "⬜")
        tail = f" — {translation}" if translation else ""
        note = "  <i>перевод переделаем</i>" if state == BAD_TRANSLATION else ""
        lines.append(f"{mark} <b>{word}</b>{tail}{note}")
    lines += ["", "<i>Ничего не удаляется, пока не нажмёшь «Готово».</i>"]

    rows = []
    for i, (word, _t) in enumerate(words):
        state = chosen.get(word, "")
        rows.append([
            {"text": ("✅ " if state == KEEP else "") + word[:18],
             "callback_data": f"wconf:{KEEP}:{i}"},
            {"text": ("✏️ перевод" if state == BAD_TRANSLATION else "✏️"),
             "callback_data": f"wconf:{BAD_TRANSLATION}:{i}"},
        ])
    rows.append([{"text": "Готово — остальные удалить", "callback_data": f"wconf:{DONE}:0"}])
    return "\n".join(lines), {"inline_keyboard": rows}


def preview(user_id: int, words: list[tuple[str, str]] | None = None) -> str:
    """Текстовый макет для владельца: как это увидит человек в личке."""
    items = words if words is not None else words_for_user(user_id)
    text, keyboard = render(user_id, items)
    plain = text.replace("<b>", "").replace("</b>", "").replace("<i>", "").replace("</i>", "")
    out = [plain, "", "┌─ кнопки под сообщением ─────────────────"]
    for row in keyboard["inline_keyboard"]:
        out.append("│  " + "   ".join(f"[ {b['text']} ]" for b in row))
    out.append("└─────────────────────────────────────────")
    return "\n".join(out)


# ── Что показывает экран проверки ────────────────────────────────────────────
def audit_items(user_id: int, limit: int = 200) -> list[dict[str, Any]]:
    """Слова этого человека, которые дверь не подтвердила, с причиной и подсказкой.

    Владелец 20.08.2026: «давать пользователю все слова, а он уже решит, сколько
    обработать». Поэтому потолок высокий — это не пачка, а полный список.
    """
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON ({bare})
                           {bare}, COALESCE(q.translation_ru, ''),
                           w.status, w.source, COALESCE(s.suggestion, '')
                      FROM bt_3_webapp_dictionary_queries q
                      JOIN bt_3_word_check w ON w.asked = {bare}
                      LEFT JOIN bt_3_word_suggestion s ON s.asked = {bare}
                     WHERE q.user_id = %s
                       AND w.status IN ('не подтверждено', 'не слово')
                       AND NOT EXISTS (SELECT 1 FROM bt_3_word_confirm_digest d
                                        WHERE d.user_id = q.user_id AND d.word = {bare}
                                          AND d.closed_at IS NOT NULL)
                     ORDER BY 1
                     LIMIT %s;
                    """.format(bare=_BARE.format(col="q.word_de")),
                    (int(user_id), int(limit)),
                )
                rows = cur.fetchall() or []
    except Exception:
        logging.warning("экран проверки: не прочитал список для %s", user_id, exc_info=True)
        return []
    return [{"word": str(a), "translation": str(b), "status": str(c),
             "why": _human_reason(str(c), str(d)), "suggestion": str(e),
             # Слово, существование которого подтвердила модель, молчанием НЕ удаляется:
             # решение владельца 21.08.2026. Справочники неполны, и предлагать человеку
             # стереть настоящее слово только потому, что страницы нет, — вред.
             "safe": _model_confirmed(str(d))}
            for a, b, c, d, e in rows]


def _model_confirmed(source: str) -> bool:
    """Сказал ли источник, что слово СУЩЕСТВУЕТ. Тогда молчание его не удаляет.

    Два таких случая, и оба означают «слово настоящее»:
      «модель: слово есть, …»                  — редкое или иноязычное, страницы нет;
      «модель предложила другое написание, …»  — слово есть, спорно лишь написание.

    Второй случай попадает сюда по той же причине, что и первый: удалить слово
    человека за то, что он пролистал экран, нельзя ни при каком из них. Написание
    он поправит кнопкой, а стёртое слово не вернуть.
    """
    text = str(source or "")
    return text.startswith("модель: слово есть") or text.startswith("модель предложила другое")


def _human_reason(status: str, source: str) -> str:
    """Причина ЧЕЛОВЕЧЕСКИМ языком. Никаких «не подтверждено источником X»."""
    if "язык en" in source:
        return "Настоящее слово, но английское — в немецких справочниках его нет."
    if "язык ru" in source:
        return "Это русское слово, а лежит оно в немецком словаре."
    if source.startswith("модель: слово есть"):
        return "Слово настоящее, просто редкое — справочник его не знает."
    if source.startswith("модель: такого слова нет"):
        return "Похоже, при сохранении слово потеряло часть букв."
    if source == "справочник молчал":
        return "Справочник не ответил, когда мы проверяли. Проверим ещё раз."
    return "Мы не нашли это слово в немецких справочниках."


def ensure_word_suggestion_schema() -> None:
    """Подсказка правильного написания. Считается ночью, показывается человеку кнопкой."""
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bt_3_word_suggestion (
                        asked       TEXT PRIMARY KEY,
                        suggestion  TEXT NOT NULL DEFAULT '',
                        checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
            conn.commit()
    except Exception:
        logging.warning("подсказка написания: схема не создана", exc_info=True)


def _with_article(word: str) -> str:
    """Исправленное слово в том виде, в каком оно ляжет в карточку.

    Артикль берётся ТОЛЬКО у справочника рода. Старый артикль строки сюда не
    переносится: «der Schwarzflieger» после починки может оказаться словом
    другого рода, и перенос превратил бы опечатку в грамматическую ошибку.
    Если род неизвестен — пишем голое слово: артикль допишет ночная программа
    рода, когда узнает его. Пустое место честнее выдуманного «der».
    """
    text = bare_word(word)
    if not text or not text[:1].isupper():
        return text  # артикль бывает у существительного, а не у глагола
    try:
        from backend.article_authority import authoritative_article
        art, _src = authoritative_article(text, allow_network=False)
    except Exception:
        art = None
    return f"{art} {text}" if art else text


def apply_decisions(user_id: int, decisions: list[dict[str, Any]]) -> dict[str, Any]:
    """Применить решения человека. Удаляем ТОЛЬКО то, что он не отметил.

    keep    — слово верное, больше не спрашиваем
    fixed   — принял нашу подсказку: заголовок правится на предложенное написание
    manual  — вписал своё написание
    retrans — слово верное, перевод плохой: карточка пересобирается ночью
    (нет решения) — удаляем у этого человека
    """
    from backend.database import get_db_connection_context
    counts = {"оставлено": 0, "исправлено": 0, "на пересборку": 0, "удалено": 0}
    if not decisions:
        return counts
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                # Строки этого человека ищем по ГОЛОМУ слову: в словаре лежит
                # «die Abschiebu», а решение пришло про «Abschiebu».
                where_bare = ("user_id=%s AND "
                              + _BARE.format(col="word_de") + "=%s")
                # Какие из присланных слов признаны настоящими. Молчание про такое
                # слово НЕ удаляет его (решение владельца 21.08.2026): справочники
                # неполны, и «Vergleichbarkeit» не должно исчезать оттого, что человек
                # пролистал экран не нажимая. Спрашиваем базу, а не верим экрану:
                # решение приходит с клиента, и подделать флаг «меня не удалять»
                # не должно быть возможно.
                asked_words = [bare_word(i.get("word") or "") for i in decisions]
                asked_words = [w for w in asked_words if w]
                safe: set[str] = set()
                if asked_words:
                    cur.execute("SELECT asked, source FROM bt_3_word_check "
                                "WHERE asked = ANY(%s);", (asked_words,))
                    safe = {str(a) for a, b in (cur.fetchall() or [])
                            if _model_confirmed(str(b))}
                for item in decisions:
                    word = bare_word(item.get("word") or "")
                    action = str(item.get("action") or "").strip()
                    # Вписанное человеком идёт через ТУ ЖЕ чистку, что и все остальные
                    # пути записи (`dictionary_intake.clean_text`): невидимые символы,
                    # хвостовая пунктуация, двойные пробелы, кавычки-ёлочки. Своё поле
                    # ввода — такой же вход, как модель или ярлык, и поблажки ему нет.
                    text = bare_word(_cleaned(item.get("text") or ""))
                    if not word:
                        continue
                    translation = _cleaned(item.get("translation") or "")
                    if action == "manual" and translation:
                        # «Свой вариант» — человек вписал и слово, и перевод. Перевод
                        # его собственный, спорить с ним нечем: он видел исходный текст,
                        # а мы нет.
                        cur.execute(
                            "UPDATE bt_3_webapp_dictionary_queries SET translation_ru=%s, "
                            "updated_at=NOW() WHERE " + where_bare,
                            (translation, int(user_id), word))
                    if action in ("fixed", "manual") and text and text != word:
                        cur.execute(
                            "UPDATE bt_3_webapp_dictionary_queries SET word_de=%s, updated_at=NOW() "
                            "WHERE " + where_bare, (_with_article(text), int(user_id), word))
                        # Исправленное написание — снова через дверь, уже без спешки.
                        cur.execute("DELETE FROM bt_3_word_check WHERE asked=%s", (text,))
                        counts["исправлено"] += 1
                    elif action == "keep":
                        counts["оставлено"] += 1
                    elif action == "retrans":
                        cur.execute(
                            """INSERT INTO bt_3_word_confirm_digest
                                      (user_id, word, decision, closed_at)
                               VALUES (%s, %s, 'retrans', NOW())
                               ON CONFLICT (user_id, word) DO UPDATE
                                  SET decision='retrans', closed_at=NOW();""",
                            (int(user_id), word))
                        counts["на пересборку"] += 1
                    elif word in safe:
                        # Слово настоящее, человек ничего не выбрал — оставляем и
                        # больше не спрашиваем. Удалять настоящее слово за молчание
                        # мы не имеем права.
                        cur.execute(
                            """INSERT INTO bt_3_word_confirm_digest
                                      (user_id, word, decision, closed_at)
                               VALUES (%s, %s, 'keep', NOW())
                               ON CONFLICT (user_id, word) DO UPDATE
                                  SET decision='keep', closed_at=NOW();""",
                            (int(user_id), word))
                        counts["оставлено"] += 1
                    else:
                        cur.execute("DELETE FROM bt_3_webapp_dictionary_queries "
                                    "WHERE " + where_bare, (int(user_id), word))
                        counts["удалено"] += 1
                    if action in ("keep", "fixed", "manual"):
                        cur.execute(
                            """INSERT INTO bt_3_word_confirm_digest
                                      (user_id, word, decision, closed_at)
                               VALUES (%s, %s, %s, NOW())
                               ON CONFLICT (user_id, word) DO UPDATE
                                  SET decision=EXCLUDED.decision, closed_at=NOW();""",
                            (int(user_id), word, action))
            conn.commit()
    except Exception:
        logging.warning("экран проверки: решения не применены для %s", user_id, exc_info=True)
    return counts


# ── Напоминание в личку: два раза в неделю ──────────────────────────────────
def _reminder_text(count: int) -> str:
    """Текст без ребусов: что, откуда, зачем, что будет, как делать, сколько.

    Владелец 20.08.2026: «в сообщении всё очень детально и без ребусов описать: что это
    за слова, откуда они появились, зачем мы просим их проверить, что будет, если не
    проверить, механику проверки, сколько делать, что потом происходит».
    """
    word = "слово" if count % 10 == 1 and count % 100 != 11 else (
        "слова" if 2 <= count % 10 <= 4 and not 12 <= count % 100 <= 14 else "слов")
    return (
        f"🦊 <b>{count} {word} в твоём словаре ждут проверки</b>\n\n"
        "<b>Что это за слова.</b> Это слова, которые ты сам сохранил. Каждое сохранённое "
        "слово мы сверяем с немецкими справочниками — эти не нашлись.\n\n"
        "<b>Почему так вышло.</b> Причины бывают разные: слово редкое и его нет в "
        "справочнике; слово из другого языка; при сохранении потерялась буква — так "
        "бывает, когда текст распознаётся с картинки.\n\n"
        "<b>Зачем проверять.</b> Если слово с ошибкой останется, ты будешь учить его "
        "в таком виде и запомнишь неправильно. Мы не удаляем ничего сами — решаешь ты.\n\n"
        "<b>Как это работает.</b> Откроется экран со списком. У каждого слова написано, "
        "почему оно там. Если мы догадались, как оно пишется правильно, будет готовая "
        "кнопка «Да, это …» — одно касание. Можно оставить слово как есть, можно "
        "попросить переделать перевод, можно вписать правильное написание руками.\n\n"
        "<b>Что будет с неотмеченными.</b> Они удалятся — из словаря и из тренировок. "
        "Но только после того, как ты нажмёшь «Готово». До этого не меняется ничего.\n\n"
        "<b>Сколько это займёт.</b> Меньше минуты на слово. Можно разобрать часть и "
        "вернуться позже — непроверенные придут снова.\n\n"
        "<b>Что потом.</b> Карточки достроим сами: часть речи, род, формы. "
        "Подтверждённые слова больше не спросим."
    )


def send_word_audit_reminders(*, force: bool = False) -> dict[str, Any]:
    """Разослать напоминания всем, у кого накопились неподтверждённые слова.

    Отправка идёт через `telegram_delivery`, а не своим `requests.post`: Telegram
    отвечает на отказ не исключением, а телом `{"ok": false, "description": …}`,
    и своя отправка печатала «доставлено» при мёртвом токене. Ровно из-за этого
    владелец месяц не видел отчёт о фразах (разбор 20.08.2026).
    """
    from datetime import datetime, timezone
    from backend.database import (
        claim_scheduler_run_guard, finish_scheduler_run_guard, get_db_connection_context,
    )
    from backend.telegram_delivery import send_telegram_message

    now = datetime.now(timezone.utc)
    run_period = now.strftime("%Y-%m-%d")
    if not force and not claim_scheduler_run_guard(
            job_key=JOB_KEY, run_period=run_period, target_scope="global", metadata={}):
        return {"ok": True, "skipped": True, "reason": "already_claimed"}

    token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
    bot_username = os.getenv("TELEGRAM_BOT_USERNAME") or ""
    if not token or not bot_username:
        return {"ok": False, "error": "no_token_or_username"}

    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT q.user_id, COUNT(DISTINCT {bare})
                      FROM bt_3_webapp_dictionary_queries q
                      JOIN bt_3_word_check w ON w.asked = {bare}
                     WHERE w.status IN ('не подтверждено', 'не слово')
                       AND NOT EXISTS (SELECT 1 FROM bt_3_word_confirm_digest d
                                        WHERE d.user_id = q.user_id AND d.word = {bare}
                                          AND d.closed_at IS NOT NULL)
                     GROUP BY q.user_id HAVING COUNT(DISTINCT {bare}) > 0;
                    """.format(bare=_BARE.format(col="q.word_de"))
                )
                targets = [(int(a), int(b)) for a, b in (cur.fetchall() or [])]
    except Exception:
        logging.warning("напоминание о словах: не собрал получателей", exc_info=True)
        targets = []

    link = f"https://t.me/{bot_username}?startapp=woerter"
    delivered = 0
    failures: list[tuple[int, str]] = []
    for user_id, count in targets:
        ok, reason = send_telegram_message(
            chat_id=user_id, text=_reminder_text(count), token=token,
            reply_markup={"inline_keyboard": [[{"text": "Открыть проверку", "url": link}]]},
            what="напоминание о проверке слов")
        if ok:
            delivered += 1
        else:
            failures.append((user_id, reason))

    if not force:
        # «Выполнено» ставится по ФАКТУ доставки, а не по факту отправки.
        finish_scheduler_run_guard(
            job_key=JOB_KEY, run_period=run_period, target_scope="global",
            status="completed" if (delivered or not targets) else "failed",
            metadata={"получателей": len(targets), "доставлено": delivered,
                      "отказы": [f"{uid}: {why}" for uid, why in failures[:20]]})
    return {"ok": True, "получателей": len(targets), "доставлено": delivered,
            "отказы": failures}


def suggestion_for(word: str) -> str:
    """Готовая подсказка написания, если она уже посчитана ночью. Иначе пусто.

    В сеть и к модели отсюда не ходим: плашка показывается человеку, который ЖДЁТ,
    и заставлять его ждать модель нельзя. Нет подсказки — плашка просто предложит
    вписать свой вариант.
    """
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT suggestion FROM bt_3_word_suggestion WHERE asked=%s;",
                            (bare_word(word),))
                row = cur.fetchone()
                return str(row[0]) if row and row[0] else ""
    except Exception:
        return ""
