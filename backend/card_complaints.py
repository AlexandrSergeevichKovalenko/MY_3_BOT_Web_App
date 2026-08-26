# -*- coding: utf-8 -*-
"""Жалоба человека на разбор карточки: приём, ночной судья, пачка владельцу.

╔══════════════════════════════════════════════════════════════════════════════════╗
║  ПОЧЕМУ ЖАЛОБА, А НЕ АВТОМАТИЧЕСКАЯ ПЕРЕСБОРКА.                                  ║
║  Решение владельца 26.08.2026, дословно: «пользователь может открыть, посмотреть, ║
║  и если что-то не соответствует — кнопка "пожаловаться на некорректный разбор".   ║
║  По нажатию информация прилетает мне… причём это должно проходить также через     ║
║  модель, и модель должна все эти 10 слов отработать и сказать, прав ли            ║
║  пользователь, и дать свои предложения. А я уже на основании предложения от       ║
║  модели по каждому из 10 слов принимаю решение».                                  ║
║                                                                                  ║
║  Разбор лежит на ОБЩЕМ слове: поправили — поправили всем, кто это слово учит.     ║
║  Поэтому мнение одного человека это ПОВОД ПЕРЕПРОВЕРИТЬ, а не команда изменить.   ║
║  Автоматической правки здесь нет вовсе: ночь только готовит материал.             ║
╚══════════════════════════════════════════════════════════════════════════════════╝

Что заменяет. Кнопка «Перевод не тот» на экране проверки слов писала строку в дневник,
которую НИКТО не читал, и при этом закрывала слово навсегда. Проверено grep'ом по
backend/ и scripts/ 26.08.2026: потребителей у `decision='retrans'` не было ни одного.
Нажатий за всё время — ноль, так что чинить накопленное не пришлось.

Состояния жалобы:
    новая              — человек нажал, ночь ещё не смотрела;
    разобрана          — модель дала вердикт и предложение, ждёт владельца;
    отправлена         — ушла владельцу в пачке, ждёт его решения;
    решена             — владелец решил; в `decision` записано, что именно.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

# Порог пачки и срок — «что раньше»: решение владельца 26.08.2026. При малом потоке
# жалоба не должна лежать месяц, при большом — не должна заваливать владельца.
BATCH = max(1, int((os.getenv("CARD_COMPLAINT_BATCH") or "10").strip() or "10"))
MAX_WAIT_DAYS = max(1, int((os.getenv("CARD_COMPLAINT_MAX_WAIT_DAYS") or "7").strip() or "7"))
# Сколько жалоб судим за одну ночь. Каждая — один поход к модели.
JUDGE_NIGHT_CAP = max(1, int((os.getenv("CARD_COMPLAINT_JUDGE_CAP") or "50").strip() or "50"))

НОВАЯ = "новая"
РАЗОБРАНА = "разобрана"
ОТПРАВЛЕНА = "отправлена"
РЕШЕНА = "решена"


def ensure_card_complaint_schema() -> None:
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bt_3_card_complaints (
                        id          BIGSERIAL PRIMARY KEY,
                        user_id     BIGINT NOT NULL,
                        unit_id     BIGINT,
                        entry_id    BIGINT,
                        word        TEXT NOT NULL,
                        note        TEXT NOT NULL DEFAULT '',
                        status      TEXT NOT NULL DEFAULT 'новая',
                        verdict     JSONB,
                        decision    TEXT NOT NULL DEFAULT '',
                        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        judged_at   TIMESTAMPTZ,
                        sent_at     TIMESTAMPTZ,
                        decided_at  TIMESTAMPTZ,
                        told_at     TIMESTAMPTZ
                    );
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS bt_3_card_complaints_status_idx "
                            "ON bt_3_card_complaints (status, created_at);")
                # Одна открытая жалоба на слово от одного человека: повторное нажатие
                # не плодит очередь, а обновляет строку (см. add_complaint).
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS bt_3_card_complaints_open_idx "
                            "ON bt_3_card_complaints (user_id, word) "
                            "WHERE status <> 'решена';")
            conn.commit()
    except Exception:
        logging.warning("жалобы на разбор: схема не создана", exc_info=True)


def add_complaint(*, user_id: int, word: str, note: str = "",
                  unit_id: int | None = None, entry_id: int | None = None) -> dict[str, Any]:
    """Принять жалобу. В разборе НИЧЕГО не меняется — это заявка, а не правка."""
    from backend.database import get_db_connection_context
    from backend.dictionary_intake import clean_text

    слово = str(word or "").strip()
    if not слово:
        return {"ok": False, "reason": "no_word"}
    # Строка от человека проходит ту же чистку, что и любой другой вход: невидимые
    # символы и кавычки-ёлочки одинаково мешают и модели, и экрану.
    примечание = clean_text(str(note or ""))[:500]
    ensure_card_complaint_schema()
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bt_3_card_complaints
                           (user_id, unit_id, entry_id, word, note, status)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, word) WHERE status <> 'решена'
                    DO UPDATE SET note = CASE WHEN EXCLUDED.note <> ''
                                              THEN EXCLUDED.note
                                              ELSE bt_3_card_complaints.note END,
                                  -- Повторная жалоба возвращает строку на суд: человек
                                  -- дописал, что именно не так, и старый вердикт устарел.
                                  status = %s, verdict = NULL, judged_at = NULL
                    RETURNING id;
                    """,
                    (int(user_id), unit_id, entry_id, слово, примечание, НОВАЯ, НОВАЯ),
                )
                строка = cur.fetchone()
            conn.commit()
    except Exception:
        logging.warning("жалобы на разбор: не принял жалобу от %s на %r",
                        user_id, слово, exc_info=True)
        return {"ok": False, "reason": "error"}
    logging.info("жалоба на разбор: человек %s, слово %r, примечание %r",
                 user_id, слово, примечание[:80])
    return {"ok": True, "id": int(строка[0]) if строка else 0}


def count_open() -> int:
    """Сколько жалоб ждут решения владельца. −1 — не смогли посчитать.

    Отдельное «не знаю» вместо нуля: молчащий счётчик неотличим от «жалоб нет».
    """
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM bt_3_card_complaints "
                            "WHERE status <> %s;", (РЕШЕНА,))
                return int((cur.fetchone() or [0])[0])
    except Exception:
        logging.warning("жалобы на разбор: счётчик не прочитан", exc_info=True)
        return -1


# ── Ночной судья ────────────────────────────────────────────────────────────────
def judge_new_complaints(limit: int = JUDGE_NIGHT_CAP) -> dict[str, Any]:
    """Ночь: по каждой новой жалобе — вердикт модели и готовое предложение.

    ⚠ НИЧЕГО НЕ ПРИМЕНЯЕТ. Решение принимает владелец, здесь только материал к нему.
    Модель не ответила — жалоба остаётся новой и придёт на суд следующей ночью;
    молча «разобранной» она не становится.
    """
    from backend.database import get_db_connection_context
    from backend.openai_manager import run_card_complaint_verdict

    ensure_card_complaint_schema()
    итог = {"взято": 0, "разобрано": 0, "не ответила": 0, "правы": 0}
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT c.id, c.word, c.note, u.card
                         FROM bt_3_card_complaints c
                         LEFT JOIN bt_3_lex_units u ON u.id = c.unit_id
                        WHERE c.status = %s
                        ORDER BY c.created_at
                        LIMIT %s;""",
                    (НОВАЯ, int(limit)),
                )
                жалобы = cur.fetchall() or []
    except Exception:
        logging.warning("жалобы на разбор: список к суду не прочитан", exc_info=True)
        return итог

    итог["взято"] = len(жалобы)
    for номер, слово, примечание, разбор in жалобы:
        try:
            вердикт = run_card_complaint_verdict(
                word=str(слово), note=str(примечание or ""),
                card=разбор if isinstance(разбор, dict) else {})
        except Exception:
            logging.warning("жалобы на разбор: судья не ответил по жалобе %s", номер,
                            exc_info=True)
            вердикт = None
        # None — это «не судили», а не «человек неправ». Жалоба остаётся новой и
        # придёт на суд следующей ночью.
        if not isinstance(вердикт, dict) or not вердикт:
            итог["не ответила"] += 1
            continue
        if вердикт.get("card_is_wrong"):
            итог["правы"] += 1
        try:
            with get_db_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE bt_3_card_complaints SET verdict=%s, status=%s, "
                        "judged_at=NOW() WHERE id=%s AND status=%s;",
                        (json.dumps(вердикт, ensure_ascii=False), РАЗОБРАНА, номер, НОВАЯ),
                    )
                conn.commit()
            итог["разобрано"] += 1
        except Exception:
            logging.warning("жалобы на разбор: вердикт по %s не записан", номер, exc_info=True)
    logging.info("жалобы на разбор: %s", итог)
    return итог


# ── Пачка владельцу ─────────────────────────────────────────────────────────────
def due_for_owner() -> tuple[bool, int]:
    """(пора ли слать, сколько разобранных ждёт).

    Порог 10 ИЛИ неделя — что раньше (решение владельца 26.08.2026): при малом потоке
    жалоба не должна лежать месяц, при большом — не должна заваливать владельца.
    """
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT COUNT(*),
                              MIN(judged_at) < NOW() - (%s || ' days')::interval
                         FROM bt_3_card_complaints WHERE status = %s;""",
                    (int(MAX_WAIT_DAYS), РАЗОБРАНА),
                )
                сколько, залежались = cur.fetchone() or (0, False)
    except Exception:
        logging.warning("жалобы на разбор: не понял, пора ли слать", exc_info=True)
        return False, -1
    сколько = int(сколько or 0)
    return (сколько >= BATCH or bool(залежались)) and сколько > 0, сколько


def owner_items(limit: int = 50) -> list[dict[str, Any]]:
    """Что показать владельцу: слово, нынешний разбор, жалоба, вердикт, предложение."""
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT c.id, c.user_id, c.word, c.note, c.verdict, c.unit_id,
                              c.entry_id, u.card, c.created_at
                         FROM bt_3_card_complaints c
                         LEFT JOIN bt_3_lex_units u ON u.id = c.unit_id
                        WHERE c.status IN (%s, %s)
                        ORDER BY c.created_at
                        LIMIT %s;""",
                    (РАЗОБРАНА, ОТПРАВЛЕНА, int(limit)),
                )
                строки = cur.fetchall() or []
                if строки:
                    cur.execute(
                        "UPDATE bt_3_card_complaints SET status=%s, sent_at=NOW() "
                        "WHERE id = ANY(%s) AND status=%s;",
                        (ОТПРАВЛЕНА, [int(r[0]) for r in строки], РАЗОБРАНА),
                    )
            conn.commit()
    except Exception:
        logging.warning("жалобы на разбор: список владельцу не собрался", exc_info=True)
        return []
    готово = []
    for номер, кто, слово, примечание, вердикт, unit_id, entry_id, разбор, когда in строки:
        в = вердикт if isinstance(вердикт, dict) else {}
        готово.append({
            "id": int(номер),
            "user_id": int(кто),
            "word": str(слово),
            "note": str(примечание or ""),
            "unit_id": int(unit_id or 0),
            "entry_id": int(entry_id or 0),
            "created_at": когда.isoformat() if когда else "",
            # Что человек видит в карточке сейчас — иначе решать не о чем.
            "now": _кратко(разбор if isinstance(разбор, dict) else {}),
            # true — карточку надо менять. Модель, поддержавшая жалобу БЕЗ единой
            # правки, сюда не доходит: сверка стоит в openai_manager.
            "card_is_wrong": bool(в.get("card_is_wrong")),
            "why": str(в.get("chto_ne_tak") or ""),
            "field": str(в.get("pole") or ""),
            "proposal": в.get("predlozhenie") if isinstance(в.get("predlozhenie"), dict) else {},
            "proposal_words": str(в.get("predlozhenie_slovami") or ""),
            "confidence": str(в.get("uverennost") or ""),
        })
    return готово


def _кратко(разбор: dict) -> dict[str, Any]:
    """Выжимка карточки для экрана решения: перевод, значения, два примера."""
    значения = разбор.get("meanings")
    примеры = разбор.get("usage_examples")
    return {
        "translation": str(разбор.get("translation_ru") or разбор.get("target_text") or ""),
        "part_of_speech": str(разбор.get("part_of_speech") or ""),
        "meanings": значения[:3] if isinstance(значения, list) else [],
        "examples": примеры[:2] if isinstance(примеры, list) else [],
    }


def apply_owner_decision(complaint_id: int, decision: str, *,
                         own_text: str = "") -> dict[str, Any]:
    """Решение владельца по одной жалобе.

    принять    — поля из предложения модели ложатся на ОБЩЕЕ слово через ту же дверь
                 `save_unit_card` (второй голос обязателен);
    пересобрать— слово ставится на пересборку тем же механизмом, что и кнопка владельца
                 «Пересобрать разбор»: разбор снимается во всех хранилищах, ночь соберёт
                 заново. Модель здесь не зовётся;
    отклонить  — карточка верна, жалоба закрывается;
    своё       — владелец вписал перевод сам.

    Человеку в любом случае уйдёт ответ (см. `unanswered_for_user`): он нажал кнопку и
    ждёт, чем кончилось.
    """
    from backend.database import get_db_connection_context

    решение = str(decision or "").strip()
    if решение not in {"принять", "пересобрать", "отклонить", "своё"}:
        return {"ok": False, "reason": "unknown_decision"}
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT unit_id, entry_id, user_id, word, verdict "
                            "FROM bt_3_card_complaints WHERE id=%s AND status<>%s;",
                            (int(complaint_id), РЕШЕНА))
                строка = cur.fetchone()
    except Exception:
        logging.warning("жалобы на разбор: жалоба %s не прочитана", complaint_id, exc_info=True)
        return {"ok": False, "reason": "error"}
    if not строка:
        return {"ok": False, "reason": "not_found"}
    unit_id, entry_id, user_id, слово, вердикт = строка
    вердикт = вердикт if isinstance(вердикт, dict) else {}

    сделано = ""
    if решение == "пересобрать":
        from backend.database import reset_dictionary_card_for_rebuild
        итог = reset_dictionary_card_for_rebuild(user_id=int(user_id),
                                                 entry_id=int(entry_id or 0))
        if not итог.get("ok"):
            return {"ok": False, "reason": "rebuild_failed"}
        сделано = "поставлено на пересборку"
    elif решение in {"принять", "своё"}:
        from backend import lex_units
        правка: dict[str, Any] = {}
        if решение == "своё":
            текст = str(own_text or "").strip()
            if not текст:
                return {"ok": False, "reason": "empty_text"}
            правка = {"translation_ru": текст}
        else:
            предложение = вердикт.get("predlozhenie")
            правка = предложение if isinstance(предложение, dict) else {}
            if not правка:
                return {"ok": False, "reason": "no_proposal"}
        if not int(unit_id or 0):
            return {"ok": False, "reason": "no_unit"}
        # Правка ложится на СЛОВО, а не в личную карточку: разбор общий, и карточки
        # читают его оттуда. Дверь та же, что у ночи, со вторым голосом.
        try:
            with get_db_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT card FROM bt_3_lex_units WHERE id=%s;", (int(unit_id),))
                    было = (cur.fetchone() or [None])[0]
        except Exception:
            logging.warning("жалобы на разбор: разбор слова %s не прочитан", unit_id,
                            exc_info=True)
            return {"ok": False, "reason": "error"}
        новый = dict(было) if isinstance(было, dict) else {}
        новый.update(правка)
        источник = "правка владельца по жалобе" if решение == "своё" else "жалоба: правка модели"
        if not lex_units.save_unit_card(int(unit_id), новый, source=источник):
            return {"ok": False, "reason": "not_saved"}
        сделано = "исправлено"
    else:
        сделано = "оставлено как есть"

    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE bt_3_card_complaints SET status=%s, decision=%s, "
                            "decided_at=NOW() WHERE id=%s;",
                            (РЕШЕНА, сделано, int(complaint_id)))
            conn.commit()
    except Exception:
        logging.warning("жалобы на разбор: решение по %s не записано", complaint_id,
                        exc_info=True)
        return {"ok": False, "reason": "error"}
    logging.info("жалоба %s на слово %r: %s (человек %s)", complaint_id, слово, сделано, user_id)
    return {"ok": True, "result": сделано, "word": str(слово), "user_id": int(user_id)}


def answers_by_user(limit_per_user: int = 20) -> dict[int, list[dict[str, Any]]]:
    """Кому и что мы должны сказать: решённые жалобы, о которых человек ещё не знает.

    Человек нажал кнопку и ждёт ответа. Молчание здесь — то же самое, что и кнопка,
    которая ничего не делает: снаружи неотличимо.

    ⚠ ЗДЕСЬ НИЧЕГО НЕ ПОМЕЧАЕТСЯ СКАЗАННЫМ. Отметку ставит `mark_told` и только ПОСЛЕ
    того, как сообщение вправду ушло: пометить до отправки значит потерять ответ
    навсегда, если Telegram откажет.
    """
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT id, user_id, word, decision FROM bt_3_card_complaints
                        WHERE status=%s AND told_at IS NULL
                        ORDER BY user_id, decided_at;""",
                    (РЕШЕНА,),
                )
                строки = cur.fetchall() or []
    except Exception:
        logging.warning("жалобы на разбор: ответы людям не собраны", exc_info=True)
        return {}
    готово: dict[int, list[dict[str, Any]]] = {}
    for номер, кто, слово, решение in строки:
        куча = готово.setdefault(int(кто), [])
        if len(куча) < int(limit_per_user):
            куча.append({"id": int(номер), "word": str(слово), "result": str(решение)})
    return готово


def mark_told(ids: list[int]) -> None:
    """Отметить, что человеку сказали. Только после успешной отправки."""
    from backend.database import get_db_connection_context
    if not ids:
        return
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE bt_3_card_complaints SET told_at=NOW() "
                            "WHERE id = ANY(%s);", ([int(i) for i in ids],))
            conn.commit()
    except Exception:
        logging.warning("жалобы на разбор: отметка «сказали» не поставлена", exc_info=True)
