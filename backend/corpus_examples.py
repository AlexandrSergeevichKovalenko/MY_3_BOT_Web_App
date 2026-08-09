# -*- coding: utf-8 -*-
"""Живые примеры из корпуса Tatoeba — вместо выдуманных моделью.

Зачем. Владелец с самого начала сомневался, что Яндекс и Reverso берут примеры у
модели: «они как будто из книжки». Он прав — это корпуса. У нас же примеры сочиняла
модель, и проверить их было нечем.

Что замерено 09.08.2026, прежде чем что-то строить:
  • пар «немецкое ↔ русское предложение» в Tatoeba — 225 027;
  • наших немецких слов — 5 073;
  • для 3 233 из них (64%) пример в корпусе НАЙДЁТСЯ — считано немецким стеммером
    самого Postgres, тем же, которым идёт поиск здесь;
  • оставшаяся треть — редкие и составные («Rettungswache», «Turnbeutelvergesser»),
    им пример по-прежнему даёт модель.

Денег это почти не экономит: примеры приезжают внутри разбора, который мы всё равно
покупаем. Выигрыш в другом — предложение настоящее, и видно, откуда оно.

Лицензия Tatoeba — CC BY 2.0 FR: требует указания источника. Мы его и так показываем,
поэтому храним автора рядом с предложением.
"""

from __future__ import annotations

import logging

CORPUS_SOURCE_TATOEBA = "tatoeba"

# Длина примера. Слишком короткое («Ja.») ничему не учит, слишком длинное не читают
# с телефона. Границы подобраны по корпусу: в этот коридор попадает большинство пар.
MIN_EXAMPLE_CHARS = 12
MAX_EXAMPLE_CHARS = 120


CORPUS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS bt_3_corpus_examples (
    id            BIGSERIAL PRIMARY KEY,
    source        TEXT NOT NULL DEFAULT 'tatoeba',
    -- Идентификатор предложения в источнике: по нему повторный импорт обновляет,
    -- а не плодит копии.
    source_id     BIGINT NOT NULL,
    text_de       TEXT NOT NULL,
    text_ru       TEXT NOT NULL,
    author        TEXT,
    license       TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source, source_id)
);

-- Поиск идёт по НЕМЕЦКОМУ словарю Postgres: он приводит «erschöpft» и «erschöpfen»
-- к одной основе, а без этого треть попаданий теряется (замер: 53% против 64%).
CREATE INDEX IF NOT EXISTS idx_corpus_examples_de_fts
    ON bt_3_corpus_examples USING GIN (to_tsvector('german', text_de));

-- Короткие примеры показываем первыми, поэтому длина участвует в отборе.
CREATE INDEX IF NOT EXISTS idx_corpus_examples_len
    ON bt_3_corpus_examples (length(text_de));
"""


def ensure_corpus_schema() -> None:
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(CORPUS_SCHEMA_SQL)
            conn.commit()
    except Exception as exc:
        logging.warning("схема корпуса примеров не создана: %s", exc)


def examples_for_word(word: str, *, limit: int = 2) -> list[dict]:
    """Примеры для немецкого слова. Пусто — значит корпус молчит, и это не ошибка:
    для трети наших слов его там нет, пример даст модель.

    Отбор: сперва короткие. Учащемуся полезнее фраза, которую он дочитает.
    """
    query = str(word or "").strip()
    if not query:
        return []
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT text_de, text_ru, author, license
                    FROM bt_3_corpus_examples
                    WHERE to_tsvector('german', text_de) @@ plainto_tsquery('german', %s)
                      AND length(text_de) BETWEEN %s AND %s
                    ORDER BY length(text_de)
                    LIMIT %s;
                    """,
                    (query, MIN_EXAMPLE_CHARS, MAX_EXAMPLE_CHARS, int(limit)),
                )
                rows = cur.fetchall()
    except Exception as exc:
        logging.debug("корпус примеров недоступен: %s", exc)
        return []
    return [
        {
            "source": row[0],
            "target": row[1],
            "origin": "Tatoeba",
            "author": row[2] or "",
            "license": row[3] or "CC BY 2.0 FR",
        }
        for row in rows
    ]
