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
import re

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


def examples_for_word(word: str, *, limit: int = 2, pos: str = "") -> list[dict]:
    """Примеры для немецкого слова. Пусто — значит корпус молчит, и это не ошибка:
    для трети наших слов его там нет, пример даст модель.

    Отбор: сперва короткие. Учащемуся полезнее фраза, которую он дочитает.

    pos — часть речи выбранной статьи. Немецкий даёт тут бесплатную и надёжную
    примету: СУЩЕСТВИТЕЛЬНЫЕ ВСЕГДА С ЗАГЛАВНОЙ, остальные слова — со строчной.
    Написание у разных слов может совпадать («die Wehe» — схватка, «wehe» — горе;
    «wehen» — веять, «die Wehen» — схватки), и без этой приметы корпус их путает:
    владелец 15.08.2026 увидел перевод «схватка», а под ним пример «Wehe den
    Besiegten» — Горе побеждённым.

    Заглавную в НАЧАЛЕ предложения не считаем: там с заглавной пишется всё подряд,
    и примета ничего не значит. Поэтому для существительного требуем, чтобы перед
    словом был хотя бы один символ.
    """
    query = str(word or "").strip()
    if not query:
        return []
    escaped = re.escape(query)
    kind = str(pos or "").strip().lower()
    if kind == "noun":
        # Существительное: слово с заглавной И не в начале предложения (точка перед
        # \m как раз требует, чтобы слева был хотя бы один символ).
        shape_sql = "text_de ~ ('.\\m' || %(cap)s || '\\M')"
    elif kind in {"verb", "adj", "adverb", "adv"}:
        # Не существительное: слово со строчной. Заглавная означала бы другое слово.
        shape_sql = "text_de ~ ('\\m' || %(low)s || '\\M')"
    else:
        # Часть речи неизвестна — не выдумываем, требуем только слово целиком.
        shape_sql = "text_de ~* ('\\m' || %(esc)s || '\\M')"

    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT text_de, text_ru, author, license
                    FROM bt_3_corpus_examples
                    -- Полнотекстовый поиск — только чтобы быстро сузить круг по индексу.
                    WHERE to_tsvector('german', text_de) @@ plainto_tsquery('german', %(q)s)
                    -- А это — страж. Немецкий полнотекстовый поиск режет слова до
                    -- основы, и «Wehe» с «weh» получают одну и ту же: на запрос «die
                    -- Wehe» (схватка) человеку показывали «Tut das weh?» — Болит?,
                    -- пример от ТРЕТЬЕГО слова wehtun. Владелец увидел это 15.08.2026.
                    -- Поэтому требуем, чтобы слово стояло в предложении целиком, а
                    -- регистр совпадал с частью речи (см. описание функции).
                    -- \m и \M — границы слова в регулярных выражениях Postgres.
                      AND """ + shape_sql + """
                      AND length(text_de) BETWEEN %(lo)s AND %(hi)s
                    ORDER BY length(text_de)
                    LIMIT %(lim)s;
                    """,
                    {
                        "q": query,
                        # В корпусе ищем ровно набранное слово, поэтому спецсимволы
                        # регулярного выражения обезвреживаем.
                        "esc": escaped,
                        "cap": re.escape(query[:1].upper() + query[1:]),
                        "low": re.escape(query[:1].lower() + query[1:]),
                        "lo": MIN_EXAMPLE_CHARS,
                        "hi": MAX_EXAMPLE_CHARS,
                        "lim": int(limit),
                    },
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
