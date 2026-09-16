# -*- coding: utf-8 -*-
"""Последняя ступень: артикль головного слова, которого не знает НИ ОДИН справочник.

┌─ РЕШЕНИЕ ВЛАДЕЛЬЦА 16.09.2026: «да, распространяем — модель ставит точку сама» ──────┐
│ Это распространение его же правила от 25.08.2026, записанного в                      │
│ backend/article_authority.py: «если род не знает ни справочник, ни база, а знает      │
│ только модель — то на кого нам ещё надеяться? Это редкие слова, их почти никто не     │
│ спросит. Мы за этот ответ уже заплатили.»                                            │
└──────────────────────────────────────────────────────────────────────────────────────┘

ПОЧЕМУ ЗДЕСЬ НЕТ ПРОВЕРКИ ПО ВТОРОМУ ИСТОЧНИКУ, И ЭТО НЕ НЕБРЕЖНОСТЬ.
Изначально ступень проектировалась как «модель предлагает — Wiktionary подтверждает».
Замер 16.09.2026 эту схему отменил: до этой ступени доходят РОВНО ТЕ слова, которых не
знает ни один справочник, поэтому подтверждать будет нечем ВСЕГДА. Живой замер по всем
четырём словам, дошедшим сюда, — Wiktionary спрошен в том числе по сети:
    Aussetzen        без сети: нет данных   с сетью: нет данных
    Buntheit         без сети: нет данных   с сетью: нет данных
    Schutzpflichten  без сети: нет данных   с сетью: нет данных
    Vorsitzender     без сети: der          с сетью: der   ← и этот ответ НЕВЕРЕН
Обещать проверку, которой не будет, нельзя — поэтому её здесь и нет, а вместо неё стоит
ФОРМА ВОПРОСА (см. ниже) и «спрашиваем один раз» (правило владельца 04.09.2026: два
спроса одной модели — не проверка, поэтому голосований здесь нет).

ВОПРОС ИЗ ДВУХ ЧАСТЕЙ, И ЭТО НЕ ФОРМАЛЬНОСТЬ.
Спросить «какой артикль» недостаточно. `Vorsitzender` — субстантивированное прилагательное:
любой справочник и любая модель бодро скажут «der», и получится
    «der Vorsitzender des Vorstands»   вместо   «der Vorsitzende des Vorstands».
При определённом артикле окончание обязано смениться с сильного на слабое. Это тот же
класс, что «das Adriatisches Meer» — жалоба владельца 22.08.2026. Поэтому первый вопрос:
МОЖЕТ ЛИ это написание стоять после определённого артикля, НЕ МЕНЯЯ формы. Сказала «нет» —
не трогаем ничего: заголовок правится не дописыванием артикля, и это отдельное решение.

НАСТОЯЩИЙ КЕШ — ЭТА ТАБЛИЦА, А НЕ КЕШ ПРЕФИКСА.
Владелец спрашивал про batch и cached request ради цены. Замер: класс прирастает 5–7
записями в месяц, сюда доходят 1–2; кеш префикса живёт 5 минут и между ночами не
доживает, batch-скидка 50% от почти нуля — почти ноль. Экономит здесь другое: про одно
и то же слово мы спрашиваем ОДИН РАЗ ЗА ВСЮ ЖИЗНЬ — ответ ложится в
`bt_3_genitive_head_article` и больше не покупается никогда. Пачкой спрашиваем всё равно:
один запрос на всю ночь вместо запроса на слово, и код один на любой объём.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

MODEL = os.getenv("GENITIVE_HEAD_JUDGE_MODEL", "gpt-4.1-2025-04-14").strip()
TIMEOUT_SEC = max(5, int((os.getenv("GENITIVE_HEAD_JUDGE_TIMEOUT") or "60").strip() or "60"))
# Потолок за ночь. Не про деньги (их тут почти нет), а про то, чтобы одна кривая ночь не
# унесла в модель половину словаря, если отбор вдруг разъедется.
CAP = max(1, int((os.getenv("GENITIVE_HEAD_JUDGE_CAP") or "20").strip() or "20"))

SOURCE_NAME = "модель (справочники молчат)"

# Вердикты. Их три, и «не знаю» — полноправный, а не отсутствие ответа.
STABLE = "stable"            # написание стоит после артикля как есть
FORM_CHANGES = "form_changes"  # артикль дописать нельзя, надо менять саму форму
UNKNOWN = "unknown"          # модель не взялась

SYSTEM = """ROLE: You are the final referee of German lexicography for a B2-C1 vocabulary
app. You are asked about the HEAD NOUN of dictionary headwords of the shape
"<noun> + <genitive attribute>", e.g. "Vollstrecker einer Anordnung", "Teile des Geländes".
No dictionary we can query (de.wiktionary, our declension reference of 89704 tables) lists
these particular spellings, which is why you are asked. Your verdict is final - nobody
reviews it after you. A wrong article is memorised by a learner and stays wrong for years,
so answering "unknown" is strictly better than guessing.

INPUT JSON: {"words": ["Aussetzen", "Vorsitzender", ...]}

For EACH word, in input order, answer TWO questions, in this order:

1. stands_after_definite_article: can THIS EXACT SPELLING stand directly after a definite
   article in the nominative WITHOUT changing its form?
   - "Aussetzen" -> true  ("das Aussetzen", a nominalised infinitive, unchanged)
   - "Buntheit"  -> true  ("die Buntheit", unchanged)
   - "Vorsitzender" -> FALSE. It is a nominalised adjective in the strong declension.
     After a definite article it must become "Vorsitzende" ("der Vorsitzende"). The
     spelling changes, so the answer is false.
   - any plural form ("Schutzpflichten") -> true, and its article is always "die".
   This question is about the SPELLING, not about the word. Get it right before question 2.

2. If and only if stands_after_definite_article is true: article = "der" | "die" | "das".
   Plural forms always take "die", whatever the gender of their singular.
   If stands_after_definite_article is false: article = "" and corrected_form = the form
   the word must take after a definite article (e.g. "Vorsitzende").

If you are genuinely unsure about a word - it may not be German, may be a typo, may be
two readings with different genders ("der/das Liter") - set verdict "unknown". Do not
guess. "unknown" costs us an empty cell; a wrong article costs a learner wrong German.

OUTPUT: strict JSON, no prose:
{"rows": [{"word": "<echoed exactly as given>",
           "verdict": "stable" | "form_changes" | "unknown",
           "article": "der" | "die" | "das" | "",
           "corrected_form": "<only when verdict is form_changes, else empty>",
           "why_ru": "<one short sentence in Russian, for the owner's report>"}]}
Return exactly one row per input word, in the same order, echoing each word verbatim."""


def _schema_sql() -> str:
    """Ответ модели живёт вечно: про одно слово спрашиваем ОДИН раз за всю жизнь."""
    return """
        CREATE TABLE IF NOT EXISTS bt_3_genitive_head_article (
            head            TEXT PRIMARY KEY,
            verdict         TEXT NOT NULL,
            article         TEXT NOT NULL DEFAULT '',
            corrected_form  TEXT NOT NULL DEFAULT '',
            why_ru          TEXT NOT NULL DEFAULT '',
            model           TEXT NOT NULL DEFAULT '',
            asked_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """


def ensure_schema() -> None:
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(_schema_sql())
        conn.commit()


def parse_rows(raw: str, asked: list) -> list | None:
    """Разобрать ответ модели. None — ответ негоден ЦЕЛИКОМ, ни одной строки не берём.

    Строгость намеренная: если модель вернула не то число строк или переставила слова,
    сопоставление «ответ ↔ слово» держится на честном слове, а цена ошибки — неверный
    артикль в карточке. Лучше не судить в эту ночь и повторить в следующую.
    """
    try:
        data = json.loads(str(raw or ""))
    except Exception:
        return None
    rows = (data or {}).get("rows")
    if not isinstance(rows, list) or len(rows) != len(asked):
        return None
    out = []
    for слово, row in zip(asked, rows):
        if not isinstance(row, dict):
            return None
        if str(row.get("word") or "").strip().casefold() != str(слово).strip().casefold():
            return None
        вердикт = str(row.get("verdict") or "").strip().lower()
        if вердикт not in (STABLE, FORM_CHANGES, UNKNOWN):
            return None
        артикль = str(row.get("article") or "").strip().lower()
        if вердикт == STABLE and артикль not in ("der", "die", "das"):
            return None            # «stable» без артикля — противоречие, ответ негоден
        if вердикт != STABLE:
            артикль = ""
        форма = str(row.get("corrected_form") or "").strip()
        if вердикт == FORM_CHANGES and not форма:
            return None            # «форма меняется», а на какую — не сказано
        out.append({"word": str(слово), "verdict": вердикт, "article": артикль,
                    "corrected_form": форма if вердикт == FORM_CHANGES else "",
                    "why_ru": str(row.get("why_ru") or "").strip()})
    return out


def _ask_openai(words: list) -> tuple:
    api_key = str(os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return None, "нет ключа OPENAI_API_KEY"
    try:
        from backend.synthetic_load import build_sync_openai_client
        client = build_sync_openai_client(api_key=api_key, timeout=TIMEOUT_SEC)
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "system", "content": SYSTEM},
                      {"role": "user", "content": json.dumps({"words": words},
                                                             ensure_ascii=False)}],
            temperature=0, response_format={"type": "json_object"},
        )
        rows = parse_rows(str(resp.choices[0].message.content or ""), words)
        return (rows, "") if rows is not None else (None, "ответ модели не разобран")
    except Exception as exc:                       # noqa: BLE001 — причину называем наверх
        logging.warning("судья артикля головного слова не ответил: %s", exc)
        return None, f"{type(exc).__name__}"


def known_verdicts(words: list) -> dict:
    """Что уже спрошено раньше: {слово: строка таблицы}. За это мы больше не платим."""
    хотим = [str(w or "").strip() for w in (words or []) if str(w or "").strip()]
    if not хотим:
        return {}
    # Таблицу заводим здесь тоже, а не только в judge_heads: сухой прогон (и обещание в
    # fix_promises, которое им меряется) читает её ПЕРВЫМ, ещё до первой ночи с моделью.
    # Без этого первое же измерение обещания упало бы в исход «не измерено».
    ensure_schema()
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT head, verdict, article, corrected_form, why_ru "
                "FROM bt_3_genitive_head_article WHERE head = ANY(%s);", (хотим,))
            return {row[0]: {"word": row[0], "verdict": row[1], "article": row[2],
                             "corrected_form": row[3], "why_ru": row[4]}
                    for row in cur.fetchall() or []}


def judge_heads(words: list, *, ask=None) -> dict:
    """{слово: вердикт} по всем `words`. Уже спрошенное берётся из таблицы, новое — одним
    запросом к модели, ответ сразу ложится в таблицу.

    Не ответила модель — слово НЕ судится и в таблицу НЕ пишется: ночь повторит. Пустая
    строка в таблице была бы неотличима от честного «не знаю» самой модели.
    """
    хотим = [str(w or "").strip() for w in (words or []) if str(w or "").strip()]
    if not хотим:
        return {}
    ensure_schema()
    готовые = known_verdicts(хотим)
    новые = [w for w in хотим if w not in готовые][:CAP]
    if not новые:
        return готовые
    rows, почему = (ask or _ask_openai)(новые)
    if rows is None:
        logging.warning("артикль головного слова не судили (%s): %s", почему,
                        ", ".join(новые))
        return готовые
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    "INSERT INTO bt_3_genitive_head_article "
                    "(head, verdict, article, corrected_form, why_ru, model) "
                    "VALUES (%s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (head) DO NOTHING;",
                    (row["word"], row["verdict"], row["article"],
                     row["corrected_form"], row["why_ru"], MODEL))
        conn.commit()
    готовые.update({row["word"]: row for row in rows})
    return готовые


def article_from_judge(verdict_row: dict | None) -> tuple:
    """(артикль, источник) либо ("", причина) — из уже полученного вердикта."""
    row = verdict_row or {}
    вердикт = str(row.get("verdict") or "")
    if вердикт == STABLE and row.get("article"):
        return (str(row["article"]), SOURCE_NAME)
    if вердикт == FORM_CHANGES:
        # Артикль дописывать НЕЛЬЗЯ: заголовок надо переписывать, а это другое решение.
        return ("", f"форму надо менять на «{row.get('corrected_form')}» — "
                    f"артикль тут не дописывается")
    if вердикт == UNKNOWN:
        return ("", f"модель не взялась: {row.get('why_ru') or 'без причины'}")
    return ("", "у модели ещё не спрашивали")
