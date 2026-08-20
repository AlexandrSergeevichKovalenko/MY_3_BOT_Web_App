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
                    SELECT DISTINCT q.word_de, COALESCE(q.translation_ru, '')
                      FROM bt_3_webapp_dictionary_queries q
                      JOIN bt_3_word_check w ON w.asked = q.word_de
                     WHERE q.user_id = %s
                       AND w.status IN ('не подтверждено', 'не слово')
                       AND NOT EXISTS (SELECT 1 FROM bt_3_word_confirm_digest d
                                        WHERE d.user_id = q.user_id AND d.word = q.word_de
                                          AND d.closed_at IS NOT NULL)
                     ORDER BY q.word_de
                     LIMIT %s;
                    """,
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
                    SELECT DISTINCT ON (q.word_de)
                           q.word_de, COALESCE(q.translation_ru, ''),
                           w.status, w.source, COALESCE(s.suggestion, '')
                      FROM bt_3_webapp_dictionary_queries q
                      JOIN bt_3_word_check w ON w.asked = q.word_de
                      LEFT JOIN bt_3_word_suggestion s ON s.asked = q.word_de
                     WHERE q.user_id = %s
                       AND w.status IN ('не подтверждено', 'не слово')
                       AND NOT EXISTS (SELECT 1 FROM bt_3_word_confirm_digest d
                                        WHERE d.user_id = q.user_id AND d.word = q.word_de
                                          AND d.closed_at IS NOT NULL)
                     ORDER BY q.word_de
                     LIMIT %s;
                    """,
                    (int(user_id), int(limit)),
                )
                rows = cur.fetchall() or []
    except Exception:
        logging.warning("экран проверки: не прочитал список для %s", user_id, exc_info=True)
        return []
    return [{"word": str(a), "translation": str(b), "status": str(c),
             "why": _human_reason(str(c), str(d)), "suggestion": str(e)}
            for a, b, c, d, e in rows]


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
                for item in decisions:
                    word = str(item.get("word") or "").strip()
                    action = str(item.get("action") or "").strip()
                    text = str(item.get("text") or "").strip()
                    if not word:
                        continue
                    if action in ("fixed", "manual") and text and text != word:
                        cur.execute(
                            "UPDATE bt_3_webapp_dictionary_queries SET word_de=%s, updated_at=NOW() "
                            "WHERE user_id=%s AND word_de=%s", (text, int(user_id), word))
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
                    else:
                        cur.execute("DELETE FROM bt_3_webapp_dictionary_queries "
                                    "WHERE user_id=%s AND word_de=%s", (int(user_id), word))
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
