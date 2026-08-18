# -*- coding: utf-8 -*-
"""Разобрано и решено: слово больше не всплывает в проверках как дефект.

ЗАЧЕМ
─────
Проверки словаря находят подозрительное по правилу, а правило не знает, что владелец
это уже смотрел и решил оставить. Без памяти о решении один и тот же список приходит
снова и снова: «slay — английское слово», «umgewandelt — причастие, а не инфинитив»,
«Die Maße — множественное число». Владелец 18.08.2026: «чтобы потом не было вопросов
каких-то, что тут что-то не так, чтобы ты потом опять не показывал, что это
некорректные слова».

Поэтому решение хранится рядом с данными, а не в моей голове и не в комментарии:
    слово + КЛАСС дефекта + что решили + почему.

Класс обязателен: «slay» решено оставить как сленг, но это НЕ значит, что у того же
слова нельзя потом найти, например, неверный артикль. Решение закрывает один вопрос,
а не слово целиком.

Пользуются им проверки (`scripts/dict_verify_fixes.py`, аудиты): прежде чем показать
находку, спрашивают `is_decided(word, класс)`.
"""
from __future__ import annotations

import logging
import re

_SPACE = re.compile(r"\s+")


def _key(value: str) -> str:
    return _SPACE.sub(" ", str(value or "").strip()).casefold()


def ensure_lex_review_decisions_schema() -> None:
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bt_3_lex_review_decisions (
                        surface_key   TEXT NOT NULL,
                        defect_class  TEXT NOT NULL,
                        decision      TEXT NOT NULL,
                        reason        TEXT NOT NULL,
                        decided_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (surface_key, defect_class)
                    );
                    """
                )
            conn.commit()
    except Exception:
        logging.warning("реестр решений: схема не создана", exc_info=True)


def record_decision(surface: str, defect_class: str, *, decision: str, reason: str) -> None:
    """Записать решение владельца. Повтор обновляет причину, а не плодит строки."""
    from backend.database import get_db_connection_context
    key = _key(surface)
    if not key or not defect_class:
        return
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bt_3_lex_review_decisions
                        (surface_key, defect_class, decision, reason)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (surface_key, defect_class) DO UPDATE
                       SET decision = EXCLUDED.decision, reason = EXCLUDED.reason,
                           decided_at = NOW();
                    """,
                    (key, str(defect_class).strip(), str(decision).strip(), str(reason).strip()),
                )
            conn.commit()
    except Exception:
        logging.warning("реестр решений: не записал %s/%s", surface, defect_class, exc_info=True)


def decided_surfaces(defect_class: str) -> set[str]:
    """Все написания, по которым решение в этом классе уже принято."""
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT surface_key FROM bt_3_lex_review_decisions WHERE defect_class = %s;",
                    (str(defect_class).strip(),),
                )
                return {row[0] for row in cur.fetchall()}
    except Exception:
        logging.debug("реестр решений: чтение не удалось", exc_info=True)
        return set()


def is_decided(surface: str, defect_class: str) -> bool:
    return _key(surface) in decided_surfaces(defect_class)
