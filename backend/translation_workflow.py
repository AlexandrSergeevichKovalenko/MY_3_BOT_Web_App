import asyncio
import hashlib
import logging
import os
import re
import json
from datetime import datetime
from typing import Any
from urllib.parse import quote

import openai
import psycopg2

from backend.config_mistakes_data import (
    VALID_CATEGORIES,
    VALID_SUBCATEGORIES,
)
from backend.openai_manager import (
    generate_sentences_multilang,
    llm_execute,
    run_check_translation_multilang,
    run_check_translation_story,
    run_check_story_guess_semantic,
)
from backend.database import (
    get_db_connection_context,
    apply_skill_events_for_error,
)


DATABASE_URL = os.getenv("DATABASE_URL_RAILWAY")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL_RAILWAY не установлен для translation_workflow.")


def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def _get_active_session_id(
    cursor,
    user_id: int,
    source_lang: str | None = None,
    target_lang: str | None = None,
) -> str | None:
    if source_lang and target_lang:
        cursor.execute(
            """
            SELECT up.session_id
            FROM bt_3_user_progress up
            WHERE up.user_id = %s
              AND up.completed = FALSE
              AND EXISTS (
                SELECT 1
                FROM bt_3_daily_sentences ds
                WHERE ds.user_id = up.user_id
                  AND ds.session_id = up.session_id
                  AND COALESCE(ds.source_lang, 'ru') = %s
                  AND COALESCE(ds.target_lang, 'de') = %s
              )
            ORDER BY up.start_time DESC
            LIMIT 1;
            """,
            (user_id, source_lang, target_lang),
        )
    else:
        cursor.execute(
            """
            SELECT session_id
            FROM bt_3_user_progress
            WHERE user_id = %s AND completed = FALSE
            ORDER BY start_time DESC
            LIMIT 1;
            """,
            (user_id,),
        )
    row = cursor.fetchone()
    return row[0] if row else None


def correct_numbering(sentences: list[str]) -> list[str]:
    corrected_sentences = []
    for sentence in sentences:
        cleaned_sentence = re.sub(r"^(\d+)\.\s*\d+\.\s*", r"\1. ", sentence).strip()
        corrected_sentences.append(cleaned_sentence)
    return corrected_sentences


def _normalize_level(level: str | None) -> str:
    normalized = (level or "c1").strip().lower().replace(" ", "")
    if normalized in {"c1-c2", "c1c2", "c1/c2"}:
        return "c2"
    allowed = {"a2", "b1", "b2", "c1", "c2"}
    return normalized if normalized in allowed else "c1"


def _sentence_word_count(text: str) -> int:
    return len(re.findall(r"[A-Za-zÀ-ÿА-Яа-яЁё0-9]+(?:[-'][A-Za-zÀ-ÿА-Яа-яЁё0-9]+)*", str(text or ""), flags=re.UNICODE))


def _sentence_has_any_marker(text: str, markers: list[str]) -> bool:
    lowered = str(text or "").lower()
    return any(marker in lowered for marker in markers)


def _sentence_fits_level(sentence: str, level: str | None) -> bool:
    text = " ".join(str(sentence or "").strip().split())
    if not text:
        return False
    level_key = _normalize_level(level)
    word_count = _sentence_word_count(text)
    comma_count = text.count(",")
    hard_punctuation = sum(text.count(mark) for mark in ";:()")
    dash_count = text.count(" — ") + text.count(" - ")

    medium_markers = [
        " если ", " когда ", " потому что ", " чтобы ", " который", " которая", " которые",
        " although ", " because ", " when ", " if ", " that ", " which ",
        " obwohl ", " weil ", " wenn ", " dass ", " damit ",
        " aunque ", " porque ", " cuando ", " para que ",
        " sebbene ", " perché ", " quando ", " affinché ",
    ]
    advanced_markers = [
        " несмотря на", " в то время как", " едва ", " поскольку ", " хотя ", " так как ",
        " whereby ", " whereas ", " provided that ", " unless ", " however ",
        " während ", " nachdem ", " sofern ", " indem ", " weshalb ",
        " sin embargo ", " por lo tanto ", " cuyo ", " cuya ",
        " nonostante ", " pertanto ", " qualora ", " il quale ",
    ]
    has_medium = _sentence_has_any_marker(text, medium_markers)
    has_advanced = _sentence_has_any_marker(text, advanced_markers)

    if level_key == "a2":
        if word_count < 4 or word_count > 12:
            return False
        if comma_count > 1 or hard_punctuation > 0 or dash_count > 0:
            return False
        if has_advanced:
            return False
        return True

    if level_key == "b1":
        if word_count < 6 or word_count > 18:
            return False
        if comma_count > 2 or hard_punctuation > 1:
            return False
        if has_advanced and word_count > 14:
            return False
        return True

    if level_key == "b2":
        if word_count < 9 or word_count > 24:
            return False
        if comma_count > 3 or hard_punctuation > 2:
            return False
        return has_medium or has_advanced or word_count >= 12

    if level_key == "c1":
        if word_count < 12 or word_count > 30:
            return False
        if comma_count < 1 and not has_medium and not has_advanced and word_count < 16:
            return False
        return True

    if level_key == "c2":
        if word_count < 15 or word_count > 36:
            return False
        if comma_count < 1 and not has_advanced and word_count < 20:
            return False
        return True

    return True


def _filter_sentences_for_level(items: list[str], level: str | None) -> list[str]:
    accepted: list[str] = []
    seen: set[str] = set()
    for item in items or []:
        text = " ".join(str(item or "").strip().split())
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        if _sentence_fits_level(text, level):
            accepted.append(text)
    return accepted


def _is_legacy_ru_de_pair(source_lang: str | None, target_lang: str | None) -> bool:
    return (source_lang or "").strip().lower() == "ru" and (target_lang or "").strip().lower() == "de"


VALID_CATEGORIES_EN = [
    "Nouns", "Pronouns", "Verbs", "Tenses & Aspect", "Adjectives", "Adverbs",
    "Prepositions", "Word Order", "Articles & Determiners", "Other mistake",
]
VALID_SUBCATEGORIES_EN = {
    "Nouns": ["Pluralization", "Countable vs Uncountable", "Possessives", "Articles/Determiners"],
    "Pronouns": ["Subject/Object", "Possessive Pronouns", "Reflexive Pronouns"],
    "Verbs": ["Conjugation/Agreement", "Auxiliaries (do/be/have)", "Modal Verbs", "Phrasal Verbs", "Verb Patterns (to V / V-ing)"],
    "Tenses & Aspect": ["Present Simple", "Present Continuous", "Past Simple", "Past Continuous", "Present Perfect", "Past Perfect", "Future (will/going to)", "Conditionals"],
    "Adjectives": ["Order of Adjectives", "Comparative", "Superlative"],
    "Adverbs": ["Placement", "Frequency Adverbs", "Too/Enough"],
    "Prepositions": ["Time", "Place", "Verb+Preposition Collocations"],
    "Word Order": ["Questions (aux inversion)", "Negation", "Indirect Questions", "Relative Clauses"],
    "Articles & Determiners": ["a/an/the/zero", "Some/Any", "Much/Many", "This/That"],
    "Other mistake": ["Unclassified mistake"],
}

VALID_CATEGORIES_ES = [
    "Nouns", "Pronouns", "Verbs", "Tenses", "Moods", "Prepositions",
    "Word Order", "Adjectives/Adverbs", "Orthography", "Other mistake",
]
VALID_SUBCATEGORIES_ES = {
    "Nouns": ["Gendered Articles", "Pluralization", "Agreement (gender/number)"],
    "Pronouns": ["Subject Pronouns", "Object Pronouns (lo/la/le)", "Clitic Placement", "Reflexive (se)"],
    "Verbs": ["Conjugation", "Ser vs Estar", "Reflexive Verbs", "Modal/Periphrasis (ir a, tener que)", "Imperatives"],
    "Tenses": ["Present", "Preterito Perfecto", "Preterito Indefinido", "Imperfecto", "Pluscuamperfecto", "Future", "Conditional"],
    "Moods": ["Subjunctive (Present)", "Subjunctive (Past)", "Indicative vs Subjunctive"],
    "Prepositions": ["Por vs Para", "A personal", "De/En/Con", "Preposition Usage"],
    "Word Order": ["Questions", "Negation", "Clitic order (se lo)", "Placement with infinitive/gerund"],
    "Adjectives/Adverbs": ["Agreement", "Comparative/Superlative", "Adverb Formation"],
    "Orthography": ["Accent Marks", "Punctuation (¿¡)", "Spelling"],
    "Other mistake": ["Unclassified mistake"],
}

VALID_CATEGORIES_IT = [
    "Nouns", "Pronouns", "Verbs", "Tenses", "Moods", "Prepositions",
    "Word Order", "Adjectives/Adverbs", "Orthography", "Other mistake",
]
VALID_SUBCATEGORIES_IT = {
    "Nouns": ["Gendered Articles", "Pluralization", "Agreement (gender/number)", "Partitive (del, della)"],
    "Pronouns": ["Direct/Indirect (lo/la/gli/le)", "Clitic Placement", "Reflexive (si)", "Ci/Ne"],
    "Verbs": ["Conjugation", "Essere vs Avere (aux)", "Reflexive Verbs", "Modal Verbs", "Imperatives"],
    "Tenses": ["Presente", "Passato Prossimo", "Imperfetto", "Trapassato Prossimo", "Futuro", "Condizionale"],
    "Moods": ["Congiuntivo (Present)", "Congiuntivo (Past)", "Indicative vs Congiuntivo"],
    "Prepositions": ["Articulated (nel, sul)", "Di/A/Da/In/Con/Su/Per/Tra", "Preposition Usage"],
    "Word Order": ["Questions", "Negation", "Clitic with infinitive", "Double pronouns"],
    "Adjectives/Adverbs": ["Agreement", "Comparative/Superlative", "Adverbs"],
    "Orthography": ["Accents", "Spelling"],
    "Other mistake": ["Unclassified mistake"],
}


def _get_language_taxonomy(target_lang: str | None) -> tuple[list[str], dict[str, list[str]], dict[str, list[str]]]:
    lang = (target_lang or "de").strip().lower()
    if lang == "en":
        categories = VALID_CATEGORIES_EN
        subcategories = VALID_SUBCATEGORIES_EN
    elif lang == "es":
        categories = VALID_CATEGORIES_ES
        subcategories = VALID_SUBCATEGORIES_ES
    elif lang == "it":
        categories = VALID_CATEGORIES_IT
        subcategories = VALID_SUBCATEGORIES_IT
    else:
        categories = VALID_CATEGORIES
        subcategories = VALID_SUBCATEGORIES
    subcategories_lower = {k.lower(): [v.lower() for v in values] for k, values in subcategories.items()}
    return categories, subcategories, subcategories_lower


def _extract_categories_from_feedback(text: str) -> tuple[list[str], list[str]]:
    if not text:
        return [], []
    categories: list[str] = []
    subcategories: list[str] = []

    cat_match = re.search(r"Mistake Categories?:\s*(.+)", text, flags=re.IGNORECASE)
    if cat_match:
        categories.extend([x.strip() for x in cat_match.group(1).split(",") if x.strip()])
    single_cat = re.search(r"Mistake category:\s*(.+)", text, flags=re.IGNORECASE)
    if single_cat:
        categories.append(single_cat.group(1).strip())

    sub_match = re.search(r"Subcategories?:\s*(.+)", text, flags=re.IGNORECASE)
    if sub_match:
        subcategories.extend([x.strip() for x in sub_match.group(1).split(",") if x.strip()])
    first_sub = re.search(r"First subcategory:\s*(.+)", text, flags=re.IGNORECASE)
    second_sub = re.search(r"Second subcategory:\s*(.+)", text, flags=re.IGNORECASE)
    if first_sub:
        subcategories.append(first_sub.group(1).strip())
    if second_sub:
        subcategories.append(second_sub.group(1).strip())

    categories = [re.sub(r"[^0-9a-zA-Z\u00C0-\u024F\s,+\-–&/()¿¡]", "", cat).strip() for cat in categories if cat.strip()]
    subcategories = [re.sub(r"[^0-9a-zA-Z\u00C0-\u024F\s,+\-–&/()¿¡]", "", sub).strip() for sub in subcategories if sub.strip()]
    return categories, subcategories


def _extract_json_payload(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```[a-zA-Z]*\n?", "", stripped)
        stripped = stripped.rstrip("`").strip()
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", stripped, re.DOTALL)
        if not match:
            return None
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return None


def _normalize_guess(text: str) -> str:
    cleaned = re.sub(r"[^\w\s\-]", " ", text.lower())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _normalize_story_difficulty(value: str | None) -> str:
    if not value:
        return "intermediate"
    normalized = value.strip().lower()
    if normalized in {"начальный", "beginner", "a2"}:
        return "beginner"
    if normalized in {"средний", "intermediate", "b1", "b2"}:
        return "intermediate"
    if normalized in {"продвинутый", "advanced", "c1", "c2"}:
        return "advanced"
    return "intermediate"


async def generate_mystery_story(
    story_type: str,
    difficulty: str,
    topic: str = "ЗАГАДОЧНАЯ ИСТОРИЯ",
) -> dict[str, Any]:
    task_name = "generate_mystery_story"
    system_instruction_key = "generate_mystery_story"

    user_message = f"""
    Story Type: "{story_type}"
    Difficulty: "{difficulty}"
    Topic: "{topic}"
    """

    for attempt in range(4):
        try:
            content = await llm_execute(
                task_name=task_name,
                system_instruction_key=system_instruction_key,
                user_message=user_message,
                poll_interval_seconds=1.0,
            )

            payload = _extract_json_payload(content)
            if payload:
                return payload
        except openai.RateLimitError:
            await asyncio.sleep((attempt + 1) * 2)
        except Exception:
            break

    return {}


def _parse_story_feedback(text: str) -> dict[str, Any]:
    score = None
    feedback = ""

    match_score = re.search(r"Score:\s*(\d{1,3})\s*/\s*100", text, flags=re.IGNORECASE)
    if match_score:
        score = int(match_score.group(1))

    # Preferred format starts with "Feedback:" and then structured blocks.
    match_feedback = re.search(r"Feedback:\s*(.+)$", text, flags=re.IGNORECASE | re.DOTALL)
    if match_feedback:
        feedback = match_feedback.group(1).strip()
    else:
        # Fallback: keep full text except first score line.
        feedback = re.sub(r"^Score:\s*\d{1,3}\s*/\s*100\s*$", "", text.strip(), flags=re.IGNORECASE | re.MULTILINE).strip()

    return {
        "score": score if score is not None else 0,
        "feedback": feedback,
    }


def _build_story_source_links(answer: str) -> list[dict[str, str]]:
    normalized = (answer or "").strip()
    if not normalized:
        return []
    slug = quote(normalized.replace(" ", "_"))
    return [
        {"lang": "DE", "url": f"https://de.wikipedia.org/wiki/{slug}"},
        {"lang": "EN", "url": f"https://en.wikipedia.org/wiki/{slug}"},
        {"lang": "RU", "url": f"https://ru.wikipedia.org/wiki/{slug}"},
    ]


def _insert_story_session_sentences(
    cursor,
    user_id: int,
    session_id: str,
    sentences: list[str],
) -> None:
    cursor.execute(
        """
        SELECT COALESCE(MAX(unique_id), 0)
        FROM bt_3_daily_sentences
        WHERE user_id = %s AND date = CURRENT_DATE;
        """,
        (user_id,),
    )
    row = cursor.fetchone()
    start_index = (row[0] or 0) + 1

    for i, sentence in enumerate(sentences, start=start_index):
        cursor.execute(
            """
            SELECT id_for_mistake_table
            FROM bt_3_daily_sentences
            WHERE sentence = %s
            LIMIT 1;
            """,
            (sentence,),
        )
        result = cursor.fetchone()

        if result:
            id_for_mistake_table = result[0]
        else:
            cursor.execute("SELECT MAX(id_for_mistake_table) FROM bt_3_daily_sentences;")
            result = cursor.fetchone()
            max_id = result[0] if result and result[0] is not None else 0
            id_for_mistake_table = max_id + 1

        cursor.execute(
            """
            INSERT INTO bt_3_daily_sentences (date, sentence, unique_id, user_id, session_id, id_for_mistake_table)
            VALUES (CURRENT_DATE, %s, %s, %s, %s, %s);
            """,
            (sentence, i, user_id, session_id, id_for_mistake_table),
        )
        cursor.execute(
            """
            UPDATE bt_3_daily_sentences
            SET source_lang = COALESCE(source_lang, 'ru'),
                target_lang = COALESCE(target_lang, 'de')
            WHERE user_id = %s AND session_id = %s AND sentence = %s
              AND source_lang IS NULL AND target_lang IS NULL;
            """,
            (user_id, session_id, sentence),
        )


def _save_story_bank(
    cursor,
    title: str | None,
    answer: str,
    aliases: list[str],
    extra_de: str,
    story_type: str,
    difficulty: str,
    sentences: list[str],
) -> int:
    cursor.execute(
        """
        INSERT INTO bt_3_story_bank (title, answer, answer_aliases, extra_de, story_type, difficulty)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id;
        """,
        (title or None, answer, json.dumps(aliases, ensure_ascii=False), extra_de, story_type, difficulty),
    )
    story_id = cursor.fetchone()[0]

    for idx, sentence in enumerate(sentences, start=1):
        cursor.execute(
            """
            INSERT INTO bt_3_story_sentences (story_id, sentence_index, sentence)
            VALUES (%s, %s, %s);
            """,
            (story_id, idx, sentence),
        )
    return story_id


def get_story_history_webapp(user_id: int, limit: int = 10) -> list[dict[str, Any]]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT s.story_id, b.title, b.story_type, b.difficulty, s.created_at
                FROM bt_3_story_sessions s
                JOIN bt_3_story_bank b ON b.id = s.story_id
                WHERE s.user_id = %s
                ORDER BY s.created_at DESC
                LIMIT %s;
                """,
                (user_id, limit),
            )
            rows = cursor.fetchall()
    return [
        {
            "story_id": row[0],
            "title": row[1],
            "story_type": row[2],
            "difficulty": row[3],
            "created_at": row[4].isoformat() if row[4] else None,
        }
        for row in rows
    ]


def get_active_session_type(user_id: int) -> dict[str, Any]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT session_id
                FROM bt_3_user_progress
                WHERE user_id = %s AND completed = FALSE
                ORDER BY start_time DESC
                LIMIT 1;
                """,
                (user_id,),
            )
            row = cursor.fetchone()
            if not row:
                return {"type": "none"}
            session_id = row[0]

            cursor.execute(
                """
                SELECT story_id
                FROM bt_3_story_sessions
                WHERE user_id = %s AND session_id = %s
                ORDER BY created_at DESC
                LIMIT 1;
                """,
                (user_id, str(session_id)),
            )
            story_row = cursor.fetchone()
            if story_row:
                return {"type": "story", "story_id": story_row[0]}

            return {"type": "regular"}


async def start_story_session_webapp(
    user_id: int,
    username: str | None,
    mode: str,
    story_type: str,
    difficulty: str,
    story_id: int | None = None,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> dict[str, Any]:
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        active_session_id = _get_active_session_id(cursor, user_id, source_lang=source_lang, target_lang=target_lang)
        if active_session_id:
            return {"session_id": active_session_id, "created": False, "blocked": True}

        cursor.execute(
            """
            UPDATE bt_3_user_progress
            SET end_time = NOW(), completed = TRUE
            WHERE user_id = %s AND completed = FALSE;
            """,
            (user_id,),
        )

        session_id = int(hashlib.md5(f"{user_id}{datetime.now()}".encode()).hexdigest(), 16) % (10**12)
        cursor.execute(
            """
            INSERT INTO bt_3_user_progress (session_id, user_id, username, start_time, completed)
            VALUES (%s, %s, %s, NOW(), FALSE);
            """,
            (session_id, user_id, username),
        )

        story_payload: dict[str, Any] | None = None
        story_sentences: list[str] = []
        story_title = None
        story_answer = None
        story_aliases: list[str] = []
        story_extra_de = None

        if mode == "repeat":
            if not story_id:
                cursor.execute(
                    """
                    SELECT story_id
                    FROM bt_3_story_sessions
                    WHERE user_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1;
                    """,
                    (user_id,),
                )
                row = cursor.fetchone()
                story_id = row[0] if row else None
            if not story_id:
                return {"error": "Нет сохранённых историй для повтора.", "created": False}

            cursor.execute(
                """
                SELECT title, answer, answer_aliases, extra_de, story_type, difficulty
                FROM bt_3_story_bank
                WHERE id = %s;
                """,
                (story_id,),
            )
            row = cursor.fetchone()
            if not row:
                return {"error": "История не найдена.", "created": False}
            story_title, story_answer, aliases_json, story_extra_de, story_type, difficulty = row
            if isinstance(aliases_json, str):
                try:
                    story_aliases = json.loads(aliases_json)
                except json.JSONDecodeError:
                    story_aliases = []
            else:
                story_aliases = aliases_json or []
            cursor.execute(
                """
                SELECT sentence
                FROM bt_3_story_sentences
                WHERE story_id = %s
                ORDER BY sentence_index ASC;
                """,
                (story_id,),
            )
            story_sentences = [row[0] for row in cursor.fetchall()]
        else:
            normalized_difficulty = _normalize_story_difficulty(difficulty)
            story_payload = await generate_mystery_story(story_type, normalized_difficulty)
            story_title = story_payload.get("title")
            story_answer = story_payload.get("answer")
            story_aliases = story_payload.get("aliases") or []
            story_extra_de = story_payload.get("extra_de")
            story_sentences = story_payload.get("story_ru") or []

            if not story_answer or not story_extra_de or len(story_sentences) != 7:
                return {"error": "Не удалось сформировать историю.", "created": False}

            story_id = _save_story_bank(
                cursor,
                story_title,
                story_answer,
                story_aliases,
                story_extra_de,
                story_type,
                normalized_difficulty,
                story_sentences,
            )

        cursor.execute(
            """
            INSERT INTO bt_3_story_sessions (user_id, session_id, story_id, mode)
            VALUES (%s, %s, %s, %s);
            """,
            (user_id, str(session_id), story_id, mode),
        )

        _insert_story_session_sentences(cursor, user_id, session_id, story_sentences)
        conn.commit()

        return {
            "session_id": session_id,
            "created": True,
            "count": len(story_sentences),
            "story_id": story_id,
            "title": story_title,
        }
    finally:
        cursor.close()
        conn.close()


async def submit_story_translation_webapp(
    user_id: int,
    username: str | None,
    translations: list[dict[str, Any]],
    guess: str,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> dict[str, Any]:
    if not translations:
        return {"error": "translations обязательны"}

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT session_id
            FROM bt_3_user_progress
            WHERE user_id = %s AND completed = FALSE
            ORDER BY start_time DESC
            LIMIT 1;
            """,
            (user_id,),
        )
        row = cursor.fetchone()
        if not row:
            return {"error": "Активная сессия не найдена."}
        session_id = row[0]

        cursor.execute(
            """
            SELECT s.story_id, b.answer, b.answer_aliases, b.extra_de
            FROM bt_3_story_sessions s
            JOIN bt_3_story_bank b ON b.id = s.story_id
            WHERE s.user_id = %s AND s.session_id = %s
            ORDER BY s.created_at DESC
            LIMIT 1;
            """,
            (user_id, str(session_id)),
        )
        story_row = cursor.fetchone()
        if not story_row:
            return {"error": "История для этой сессии не найдена."}

        story_id, answer, aliases_json, extra_de = story_row
        if isinstance(aliases_json, str):
            try:
                aliases = json.loads(aliases_json)
            except json.JSONDecodeError:
                aliases = []
        else:
            aliases = aliases_json or []

        cursor.execute(
            """
            SELECT id, id_for_mistake_table, sentence, unique_id
            FROM bt_3_daily_sentences
            WHERE user_id = %s AND session_id = %s
            ORDER BY unique_id ASC;
            """,
            (user_id, session_id),
        )
        daily_rows = cursor.fetchall()
        if not daily_rows:
            return {"error": "Предложения истории не найдены."}

        if len(daily_rows) != 7:
            logging.warning("Story session has %s sentences, expected 7", len(daily_rows))

        translations_by_id = {
            int(item.get("id_for_mistake_table")): (item.get("translation") or "").strip()
            for item in translations
            if item.get("id_for_mistake_table")
        }

        original_sentences = [row[2] for row in daily_rows]
        user_sentences = [translations_by_id.get(row[1], "") for row in daily_rows]

        if any(not text for text in user_sentences):
            return {"error": "Нужно заполнить все 7 предложений истории."}

        cursor.execute(
            """
            SELECT COUNT(*) FROM bt_3_translations
            WHERE user_id = %s AND session_id = %s;
            """,
            (user_id, session_id),
        )
        if (cursor.fetchone()[0] or 0) > 0:
            return {"error": "История уже была отправлена."}

        original_text = "\n".join(original_sentences)
        user_text = "\n".join(user_sentences)
        raw_feedback = await run_check_translation_story(original_text, user_text)
        parsed = _parse_story_feedback(raw_feedback)
        score_value = parsed["score"]
        feedback = parsed["feedback"] or raw_feedback

        for row, user_sentence in zip(daily_rows, user_sentences):
            sentence_pk_id = row[0]
            sentence_id_for_mistake = row[1]
            cursor.execute(
                """
                INSERT INTO bt_3_translations (user_id, id_for_mistake_table, session_id, username, sentence_id,
                user_translation, score, feedback, source_lang, target_lang)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
                """,
                (
                    user_id,
                    sentence_id_for_mistake,
                    session_id,
                    username,
                    sentence_pk_id,
                    user_sentence,
                    score_value,
                    feedback,
                    (source_lang or "ru"),
                    (target_lang or "de"),
                ),
            )

        normalized_guess = _normalize_guess(guess)
        normalized_answer = _normalize_guess(answer or "")
        alias_matches = {_normalize_guess(item) for item in aliases if item}
        heuristic_match = bool(
            normalized_guess and (
                normalized_guess == normalized_answer
                or normalized_guess in alias_matches
                or normalized_answer in normalized_guess
                or any(alias and alias in normalized_guess for alias in alias_matches)
            )
        )

        semantic_result = {"is_correct": False, "reason": ""}
        try:
            semantic_result = await run_check_story_guess_semantic(
                canonical_answer=answer or "",
                aliases=[item for item in aliases if item],
                user_guess=guess,
            )
        except Exception as exc:
            logging.warning("Semantic guess check failed: %s", exc)

        is_correct = bool(heuristic_match or semantic_result.get("is_correct"))
        guess_reason = (semantic_result.get("reason") or "").strip()
        source_links = _build_story_source_links(answer or "")

        cursor.execute(
            """
            UPDATE bt_3_story_sessions
            SET completed_at = NOW(), guess = %s, guess_correct = %s, score = %s, feedback = %s
            WHERE user_id = %s AND session_id = %s AND story_id = %s;
            """,
            (guess, is_correct, score_value, feedback, user_id, str(session_id), story_id),
        )

        conn.commit()

        return {
            "ok": True,
            "score": score_value,
            "feedback": feedback,
            "guess_correct": is_correct,
            "guess_reason": guess_reason,
            "answer": answer,
            "extra_de": extra_de,
            "source_links": source_links,
        }
    finally:
        cursor.close()
        conn.close()


async def generate_sentences_webapp(
    user_id: int,
    num_sentences: int,
    topic: str = "Random sentences",
    level: str | None = None,
) -> list[str]:
    task_name = "generate_sentences"
    level_key = _normalize_level(level)
    system_instruction_key = f"generate_sentences_{level_key}"
    target_count = int(max(1, num_sentences))
    level_notes = {
        "a2": "A2 only: short, concrete, everyday sentences. No heavy subordinate clauses. Prefer 4-12 words.",
        "b1": "B1 only: moderately simple sentences with at most one light subordinate clause. Prefer 6-18 words.",
        "b2": "B2 only: clearly more developed sentences with some clause complexity. Prefer 9-24 words.",
        "c1": "C1 only: advanced sentences with visible syntactic complexity. Prefer 12-30 words.",
        "c2": "C2 only: very advanced, nuanced, syntactically dense sentences. Prefer 15-36 words.",
    }

    user_message = f"""
    Number of sentences: {target_count}. Topic: "{topic}".
    Required level: {level_key.upper()}.
    {level_notes.get(level_key, "")}
    Reject sentences that are clearly easier or harder than the requested level.
    """

    for attempt in range(5):
        try:
            sentences = await llm_execute(
                task_name=task_name,
                system_instruction_key=system_instruction_key,
                user_message=user_message,
                poll_interval_seconds=1.0,
            )

            raw_lines = [s.strip() for s in sentences.split("\n") if s.strip()]
            filtered = _filter_sentences_for_level(raw_lines, level_key)
            if len(filtered) >= target_count:
                return filtered[:target_count]
        except openai.RateLimitError:
            wait_time = (attempt + 1) * 2
            await asyncio.sleep(wait_time)
        except Exception:
            break

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT sentence FROM bt_3_spare_sentences ORDER BY RANDOM() LIMIT 30;
                """
            )
            spare_rows = cursor.fetchall()
    if spare_rows:
        spare_filtered = _filter_sentences_for_level(
            [row[0].strip() for row in spare_rows if row[0] and row[0].strip()],
            level_key,
        )
        if spare_filtered:
            return spare_filtered[:target_count]
    return []


async def get_original_sentences_webapp(
    user_id: int,
    topic: str = "Random sentences",
    level: str | None = None,
) -> list[str]:
    conn = get_db_connection()
    cursor = conn.cursor()
    level_key = _normalize_level(level)

    try:
        cursor.execute("SELECT sentence FROM bt_3_sentences ORDER BY RANDOM() LIMIT 20;")
        rows = _filter_sentences_for_level([row[0] for row in cursor.fetchall()], level_key)[:1]

        cursor.execute(
            """
            SELECT sentence, sentence_id
            FROM bt_3_detailed_mistakes
            WHERE user_id = %s
            ORDER BY mistake_count DESC, last_seen ASC;
            """,
            (user_id,),
        )

        already_given_sentence_ids = set()
        unique_sentences = set()
        mistake_sentences = []

        for sentence, sentence_id in cursor.fetchall():
            if sentence_id and sentence_id not in already_given_sentence_ids:
                if sentence_id not in unique_sentences:
                    if not _sentence_fits_level(sentence, level_key):
                        continue
                    unique_sentences.add(sentence_id)
                    mistake_sentences.append(sentence)
                    already_given_sentence_ids.add(sentence_id)
                    if len(mistake_sentences) == 5:
                        break

        num_sentences = 7 - len(rows) - len(mistake_sentences)
        gpt_sentences = []
        if num_sentences > 0:
            gpt_sentences = await generate_sentences_webapp(user_id, num_sentences, topic, level)

        def normalize_sentences(items: list[str]) -> list[str]:
            normalized: list[str] = []
            seen: set[str] = set()
            for item in items:
                if not item:
                    continue
                text = str(item).strip()
                if not text:
                    continue
                for line in text.split("\n"):
                    candidate = re.sub(r"^\s*\d+\.\s*", "", line).strip()
                    if not candidate or candidate in seen:
                        continue
                    if not _sentence_fits_level(candidate, level_key):
                        continue
                    seen.add(candidate)
                    normalized.append(candidate)
            return normalized

        final_sentences = normalize_sentences(rows + mistake_sentences + gpt_sentences)
        attempts = 0
        while len(final_sentences) < 7 and attempts < 3:
            needed = 7 - len(final_sentences)
            extra_sentences = await generate_sentences_webapp(user_id, needed, topic, level)
            final_sentences = normalize_sentences(final_sentences + extra_sentences)
            attempts += 1

        return final_sentences
    finally:
        cursor.close()
        conn.close()


async def start_translation_session_webapp(
    user_id: int,
    username: str | None = None,
    topic: str = "Random sentences",
    level: str | None = None,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> dict[str, Any]:
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        active_session_id = _get_active_session_id(
            cursor,
            user_id,
            source_lang=source_lang,
            target_lang=target_lang,
        )
        if active_session_id:
            # Keep blocking only when there are still pending sentences in this
            # active session for the same language pair.
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM bt_3_daily_sentences ds
                LEFT JOIN bt_3_translations tr
                  ON tr.user_id = ds.user_id
                 AND tr.sentence_id = ds.id
                 AND tr.session_id = ds.session_id
                 AND COALESCE(tr.source_lang, 'ru') = %s
                 AND COALESCE(tr.target_lang, 'de') = %s
                WHERE ds.user_id = %s
                  AND ds.session_id = %s
                  AND COALESCE(ds.source_lang, 'ru') = %s
                  AND COALESCE(ds.target_lang, 'de') = %s
                  AND tr.id IS NULL;
                """,
                (
                    source_lang,
                    target_lang,
                    user_id,
                    active_session_id,
                    source_lang,
                    target_lang,
                ),
            )
            row = cursor.fetchone()
            pending_count = int(row[0] or 0) if row else 0
            if pending_count > 0:
                return {"session_id": active_session_id, "created": False, "blocked": True}

            # Auto-close stale empty session and allow creating a fresh one.
            cursor.execute(
                """
                UPDATE bt_3_user_progress
                SET end_time = NOW(), completed = TRUE
                WHERE user_id = %s AND session_id = %s;
                """,
                (user_id, active_session_id),
            )
            conn.commit()

        cursor.execute(
            """
            UPDATE bt_3_user_progress
            SET end_time = NOW(), completed = TRUE
            WHERE user_id = %s AND start_time::date < CURRENT_DATE AND completed = FALSE;
            """,
            (user_id,),
        )

        session_id = int(hashlib.md5(f"{user_id}{datetime.now()}".encode()).hexdigest(), 16) % (10**12)
        cursor.execute(
            """
            INSERT INTO bt_3_user_progress (session_id, user_id, username, start_time, completed)
            VALUES (%s, %s, %s, NOW(), FALSE);
            """,
            (session_id, user_id, username),
        )
        conn.commit()

        if _is_legacy_ru_de_pair(source_lang, target_lang):
            sentences = [s.strip() for s in await get_original_sentences_webapp(user_id, topic, level) if s.strip()]
        else:
            generated = await generate_sentences_multilang(
                num_sentences=7,
                topic=topic,
                level=level,
                source_lang=source_lang,
                target_lang=target_lang,
            )
            sentences = _filter_sentences_for_level([s.strip() for s in generated if s.strip()], level)
        sentences = correct_numbering(sentences)

        if not sentences:
            return {"session_id": session_id, "created": True, "count": 0}

        cursor.execute(
            """
            SELECT COALESCE(MAX(unique_id), 0)
            FROM bt_3_daily_sentences
            WHERE user_id = %s
              AND date = CURRENT_DATE
              AND COALESCE(source_lang, 'ru') = %s
              AND COALESCE(target_lang, 'de') = %s;
            """,
            (user_id, source_lang, target_lang),
        )
        row = cursor.fetchone()
        start_index = (row[0] or 0) + 1

        for i, sentence in enumerate(sentences, start=start_index):
            cursor.execute(
                """
                SELECT id_for_mistake_table
                FROM bt_3_daily_sentences
                WHERE sentence = %s
                  AND COALESCE(source_lang, 'ru') = %s
                  AND COALESCE(target_lang, 'de') = %s
                LIMIT 1;
                """,
                (sentence, source_lang, target_lang),
            )
            result = cursor.fetchone()

            if result:
                id_for_mistake_table = result[0]
            else:
                cursor.execute("SELECT MAX(id_for_mistake_table) FROM bt_3_daily_sentences;")
                result = cursor.fetchone()
                max_id = result[0] if result and result[0] is not None else 0
                id_for_mistake_table = max_id + 1

            cursor.execute(
                """
                INSERT INTO bt_3_daily_sentences (
                    date,
                    sentence,
                    unique_id,
                    user_id,
                    session_id,
                    id_for_mistake_table,
                    source_lang,
                    target_lang
                )
                VALUES (CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s);
                """,
                (sentence, i, user_id, session_id, id_for_mistake_table, source_lang, target_lang),
            )

        conn.commit()
        return {"session_id": session_id, "created": True, "count": len(sentences)}
    finally:
        cursor.close()
        conn.close()


async def check_translation(
    original_text: str,
    user_translation: str,
    sentence_number: int | None = None,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> tuple[str, list[str], list[str], int | None, str | None]:
    language_categories, language_subcategories, language_subcategories_lower = _get_language_taxonomy(target_lang)
    if not _is_legacy_ru_de_pair(source_lang, target_lang):
        feedback = await run_check_translation_multilang(
            original_text=original_text,
            user_translation=user_translation,
            source_lang=source_lang,
            target_lang=target_lang,
            allowed_categories=language_categories,
            allowed_subcategories=language_subcategories,
        )
        score = None
        correct_translation = None
        categories, subcategories = _extract_categories_from_feedback(feedback)
        valid_categories: list[str] = []
        valid_subcategories: list[str] = []
        score_match = re.search(r"Score:\s*(\d+)", feedback, flags=re.IGNORECASE)
        if score_match:
            score = int(score_match.group(1))
        translation_match = re.search(r"Correct Translation:\s*(.+?)(?:\n|\Z)", feedback, flags=re.IGNORECASE)
        if translation_match:
            correct_translation = translation_match.group(1).strip()
        for cat in categories:
            cat_lower = str(cat or "").lower().strip()
            canonical_cat = next((x for x in language_categories if x.lower() == cat_lower), None)
            if canonical_cat:
                valid_categories.append(canonical_cat)
        for sub in subcategories:
            sub_lower = str(sub or "").lower().strip()
            for cat, values in language_subcategories.items():
                if sub_lower in [v.lower() for v in values]:
                    canonical_sub = next((x for x in values if x.lower() == sub_lower), None)
                    if canonical_sub:
                        valid_subcategories.append(canonical_sub)
                        break
        sentence_label = sentence_number if sentence_number is not None else "—"
        if score is not None and "Sentence number" not in feedback and "Mistake Categories" not in feedback:
            feedback = (
                f"🟢 *Sentence number:* {sentence_label}\n"
                f"✅ *Score:* {score}/100\n"
                f"🔵 *Original Sentence:* {original_text}\n"
                f"🟡 *User Translation:* {user_translation}\n"
                f"🟣 *Correct Translation:* {correct_translation or '—'}\n"
                f"{feedback}"
            )
        return feedback, list(set(valid_categories)), list(set(valid_subcategories)), score, correct_translation

    task_name = "check_translation"
    system_instruction_key = "check_translation"

    score = None
    categories: list[str] = []
    subcategories: list[str] = []
    correct_translation = None

    user_message = f"""

    **Original sentence (Russian):** "{original_text}"
    **User's translation (German):** "{user_translation}"

    """

    for attempt in range(3):
        try:
            logging.info("GPT started working on sentence %s", original_text)
            collected_text = await llm_execute(
                task_name=task_name,
                system_instruction_key=system_instruction_key,
                user_message=user_message,
                poll_interval_seconds=2.0,
            )

            logging.info("GPT response for sentence %s: %s", original_text, collected_text)

            score_str = (
                collected_text.split("Score: ")[-1].split("/")[0].strip()
                if "Score:" in collected_text
                else None
            )
            categories, subcategories = _extract_categories_from_feedback(collected_text)

            match = re.search(r"Correct Translation:\s*(.+?)(?:\n|\Z)", collected_text)
            if match:
                correct_translation = match.group(1).strip()

            categories = [
                re.sub(r"[^0-9a-zA-Z\s,+\-–]", "", cat).strip()
                for cat in categories
                if cat.strip()
            ]
            subcategories = [
                re.sub(r"[^0-9a-zA-Z\s,+\-–]", "", subcat).strip()
                for subcat in subcategories
                if subcat.strip()
            ]
            categories = [cat.strip() for cat in categories if cat.strip()]
            subcategories = [subcat.strip() for subcat in subcategories if subcat.strip()]

            if score_str and correct_translation:
                if score_str.isdigit():
                    score = int(score_str)
                    if score == 0:
                        score = await recheck_score_only(original_text, user_translation)
                else:
                    score = await recheck_score_only(original_text, user_translation)
                score_display = score if score is not None else 0
                sentence_label = sentence_number if sentence_number is not None else "—"

                result_text = (
                    f"🟢 *Sentence number:* {sentence_label}\n"
                    f"✅ *Score:* {score_display}/100\n"
                    f"🔵 *Original Sentence:* {original_text}\n"
                    f"🟡 *User Translation:* {user_translation}\n"
                    f"🟣 *Correct Translation:* {correct_translation}\n"
                )

                return result_text, categories, subcategories, score, correct_translation

        except Exception as exc:
            logging.error(
                "Ошибка при проверке перевода (attempt %s, sentence %s): %s",
                attempt + 1,
                sentence_number,
                exc,
                exc_info=True,
            )
            await asyncio.sleep(1)

    if score is None:
        score = await recheck_score_only(original_text, user_translation)
    if correct_translation is None:
        correct_translation = "—"

    sentence_label = sentence_number if sentence_number is not None else "—"
    result_text = (
        f"🟢 *Sentence number:* {sentence_label}\n"
        f"✅ *Score:* {score}/100\n"
        f"🔵 *Original Sentence:* {original_text}\n"
        f"🟡 *User Translation:* {user_translation}\n"
        f"🟣 *Correct Translation:* {correct_translation}\n"
    )

    return result_text, categories, subcategories, score, correct_translation


async def recheck_score_only(original_text: str, user_translation: str) -> int:
    task_name = "recheck_translation"
    system_instruction_key = "recheck_translation"

    user_message = (
        f'Original sentence (Russian): "{original_text}"\n'
        f'User\'s translation (German): "{user_translation}"'
    )

    for attempt in range(3):
        try:
            collected_text = await llm_execute(
                task_name=task_name,
                system_instruction_key=system_instruction_key,
                user_message=user_message,
                poll_interval_seconds=1.0,
            )

            match = re.search(r"Score:\s*(\d+)", collected_text, flags=re.IGNORECASE)
            if match:
                return int(match.group(1))

        except Exception as exc:
            logging.warning("Recheck failed (attempt %s): %s", attempt + 1, exc)

    return 0


def _extract_correct_translation(feedback: str | None, sentence_number: int | None = None) -> str | None:
    if not feedback:
        return None
    text = str(feedback or "")
    if sentence_number is not None:
        try:
            sentence_idx = int(sentence_number)
        except Exception:
            sentence_idx = None
        if sentence_idx:
            story_patterns = [
                rf"(?ms)^{sentence_idx}\)\s*Верный вариант\s*\(DE\):\s*(.+?)\s*$",
                rf"(?ms)^Satz\s*{sentence_idx}\s*[:\-].*?^3\)\s*Верный вариант\s*\(DE\):\s*(.+?)\s*$",
                rf"(?ms)^Sentence\s*{sentence_idx}\s*[:\-].*?^3\)\s*Correct Translation\s*:\s*(.+?)\s*$",
            ]
            for pattern in story_patterns:
                match = re.search(pattern, text, flags=re.IGNORECASE)
                if match:
                    candidate = str(match.group(1) or "").strip()
                    if candidate:
                        return candidate

    generic_patterns = [
        r"Correct Translation:\*?\s*(.+?)(?:\n|\Z)",
        r"Korrigierte Version:\*?\s*(.+?)(?:\n|\Z)",
        r"Исправленный вариант:\*?\s*(.+?)(?:\n|\Z)",
        r"Верный вариант\s*\(DE\):\s*(.+?)(?:\n|\Z)",
    ]
    for pattern in generic_patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            candidate = str(match.group(1) or "").strip()
            if candidate:
                return candidate
    return None


def get_daily_translation_history(
    user_id: int,
    limit: int = 50,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> list[dict[str, Any]]:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    t.id,
                    t.score,
                    t.user_translation,
                    t.feedback,
                    t.timestamp,
                    ds.sentence,
                    ds.unique_id,
                    t.source_lang,
                    t.target_lang
                FROM bt_3_translations t
                JOIN bt_3_daily_sentences ds
                    ON ds.id = t.sentence_id
                WHERE t.user_id = %s
                  AND COALESCE(t.source_lang, 'ru') = %s
                  AND COALESCE(t.target_lang, 'de') = %s
                  AND t.timestamp::date = CURRENT_DATE
                ORDER BY t.timestamp DESC
                LIMIT %s;
                """,
                (user_id, source_lang, target_lang, limit),
            )
            rows = cursor.fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        feedback = row[3]
        items.append(
            {
                "id": row[0],
                "score": row[1],
                "user_translation": row[2],
                "feedback": feedback,
                "created_at": row[4].isoformat() if row[4] else None,
                "original_text": row[5],
                "sentence_number": row[6],
                "correct_translation": _extract_correct_translation(feedback, row[6]),
                "source_lang": row[7] or source_lang,
                "target_lang": row[8] or target_lang,
            }
        )
    return items


async def log_translation_mistake(
    user_id: int,
    original_text: str,
    user_translation: str,
    categories: list[str],
    subcategories: list[str],
    score: int,
    correct_translation: str | None,
    source_lang: str = "ru",
    target_lang: str = "de",
) -> None:
    language_categories, language_subcategories, language_subcategories_lower = _get_language_taxonomy(target_lang)
    if categories:
        logging.info("Categories from log_translation_mistake: %s", ", ".join(categories))
    if subcategories:
        logging.info("Subcategories from log_translation_mistake: %s", ", ".join(subcategories))

    valid_combinations = []
    for cat in categories:
        cat_lower = cat.lower()
        for subcat in subcategories:
            subcat_lower = subcat.lower()
            if cat_lower in language_subcategories_lower and subcat_lower in language_subcategories_lower[cat_lower]:
                valid_combinations.append((cat_lower, subcat_lower))

    if not valid_combinations:
        valid_combinations.append(("Other mistake", "Unclassified mistake"))

    valid_combinations = list(set(valid_combinations))

    for main_category, sub_category in valid_combinations:
        main_category = next(
            (cat for cat in language_categories if cat.lower() == main_category),
            main_category,
        )
        sub_category = next(
            (
                subcat
                for subcat in language_subcategories.get(main_category, [])
                if subcat.lower() == sub_category
            ),
            sub_category,
        )

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id_for_mistake_table
                    FROM bt_3_daily_sentences
                    WHERE sentence=%s
                      AND COALESCE(source_lang, 'ru') = %s
                      AND COALESCE(target_lang, 'de') = %s
                    LIMIT 1;
                    """,
                    (original_text, source_lang or "ru", target_lang or "de"),
                )
                result = cursor.fetchone()
                sentence_id = result[0] if result else None

                cursor.execute(
                    """
                    INSERT INTO bt_3_detailed_mistakes (
                        user_id, sentence, added_data, main_category, sub_category, mistake_count, sentence_id,
                        correct_translation, score
                    ) VALUES (%s, %s, NOW(), %s, %s, 1, %s, %s, %s)
                    ON CONFLICT (user_id, sentence, main_category, sub_category)
                    DO UPDATE SET
                        mistake_count = bt_3_detailed_mistakes.mistake_count + 1,
                        attempt = bt_3_detailed_mistakes.attempt + 1,
                        last_seen = NOW(),
                        score = EXCLUDED.score;
                    """,
                    (user_id, original_text, main_category, sub_category, sentence_id, correct_translation, score),
                )
                try:
                    apply_skill_events_for_error(
                        user_id=int(user_id),
                        source_lang=source_lang or "ru",
                        target_lang=target_lang or "de",
                        error_category=main_category,
                        error_subcategory=sub_category,
                        event_type="fail",
                        fail_delta=-3.0,
                        success_delta=2.0,
                    )
                except Exception as exc:
                    logging.warning("Skill fail update skipped: %s", exc)


async def check_user_translation_webapp(
    user_id: int,
    username: str | None,
    translations: list[dict[str, Any]],
    source_lang: str = "ru",
    target_lang: str = "de",
    daily_session_id: str | int | None = None,
) -> list[dict[str, Any]]:
    if not translations:
        return []

    conn = get_db_connection()
    cursor = conn.cursor()
    language_categories, language_subcategories, language_subcategories_lower = _get_language_taxonomy(target_lang)

    try:
        latest_session_id = daily_session_id
        if isinstance(latest_session_id, str):
            stripped_session_id = latest_session_id.strip()
            if not stripped_session_id:
                latest_session_id = None
            elif stripped_session_id.isdigit():
                latest_session_id = int(stripped_session_id)
            else:
                latest_session_id = stripped_session_id
        if latest_session_id is None:
            cursor.execute(
                """
                SELECT session_id
                FROM bt_3_daily_sentences
                WHERE user_id = %s
                  AND COALESCE(source_lang, 'ru') = %s
                  AND COALESCE(target_lang, 'de') = %s
                ORDER BY id DESC
                LIMIT 1;
                """,
                (user_id, (source_lang or "ru"), (target_lang or "de")),
            )
            latest_session = cursor.fetchone()
            if not latest_session:
                return []
            latest_session_id = latest_session[0]
        if latest_session_id is None:
            return []

        cursor.execute(
            """
            SELECT unique_id, id_for_mistake_table, id, sentence, session_id
            FROM bt_3_daily_sentences
            WHERE session_id = %s
              AND user_id = %s
              AND COALESCE(source_lang, 'ru') = %s
              AND COALESCE(target_lang, 'de') = %s;
            """,
            (latest_session_id, user_id, (source_lang or "ru"), (target_lang or "de")),
        )
        allowed_rows = cursor.fetchall()
        allowed_by_mistake_id = {
            row[1]: {
                "unique_id": row[0],
                "sentence_id": row[2],
                "sentence": row[3],
                "session_id": row[4],
            }
            for row in allowed_rows
        }

        results: list[dict[str, Any]] = []

        for entry in translations:
            sentence_id_for_mistake = entry.get("id_for_mistake_table")
            if isinstance(sentence_id_for_mistake, str) and sentence_id_for_mistake.isdigit():
                sentence_id_for_mistake = int(sentence_id_for_mistake)
            user_translation = (entry.get("translation") or "").strip()
            if not sentence_id_for_mistake or not user_translation:
                continue

            if sentence_id_for_mistake not in allowed_by_mistake_id:
                results.append(
                    {
                        "sentence_number": None,
                        "error": "Предложение не принадлежит пользователю или не найдено.",
                    }
                )
                continue

            sentence_info = allowed_by_mistake_id[sentence_id_for_mistake]
            sentence_number = sentence_info["unique_id"]
            original_text = sentence_info["sentence"]
            session_id = sentence_info["session_id"]
            sentence_pk_id = sentence_info["sentence_id"]

            cursor.execute(
                """
                SELECT id FROM bt_3_translations
                WHERE user_id = %s
                  AND sentence_id = %s
                  AND COALESCE(source_lang, 'ru') = %s
                  AND COALESCE(target_lang, 'de') = %s
                  AND timestamp::date = CURRENT_DATE;
                """,
                (user_id, sentence_pk_id, (source_lang or "ru"), (target_lang or "de")),
            )

            existing_translation = cursor.fetchone()
            if existing_translation:
                results.append(
                    {
                        "sentence_number": sentence_number,
                        "error": "Вы уже переводили это предложение.",
                    }
                )
                continue

            try:
                feedback, categories, subcategories, score, correct_translation = await check_translation(
                    original_text,
                    user_translation,
                    sentence_number,
                    source_lang=source_lang,
                    target_lang=target_lang,
                )
            except Exception as exc:
                logging.error("Ошибка при проверке перевода №%s: %s", sentence_number, exc, exc_info=True)
                results.append(
                    {
                        "sentence_number": sentence_number,
                        "error": "Ошибка: не удалось проверить перевод.",
                    }
                )
                continue

            score_value = int(score) if score and str(score).isdigit() else 0

            cursor.execute(
                """
                INSERT INTO bt_3_translations (user_id, id_for_mistake_table, session_id, username, sentence_id,
                user_translation, score, feedback, source_lang, target_lang)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
                """,
                (
                    user_id,
                    sentence_id_for_mistake,
                    session_id,
                    username,
                    sentence_pk_id,
                    user_translation,
                    score_value,
                    feedback,
                    (source_lang or "ru"),
                    (target_lang or "de"),
                ),
            )
            created_translation = cursor.fetchone()
            translation_id = int(created_translation[0]) if created_translation and created_translation[0] else None
            conn.commit()

            cursor.execute(
                """
                SELECT COUNT(*) FROM bt_3_detailed_mistakes
                WHERE sentence_id = %s AND user_id = %s;
                """,
                (sentence_id_for_mistake, user_id),
            )

            was_in_mistakes = cursor.fetchone()[0] > 0

            if was_in_mistakes:
                if score_value >= 85:
                    cursor.execute(
                        """
                        SELECT DISTINCT
                            COALESCE(NULLIF(main_category, ''), 'Other mistake') AS main_category,
                            COALESCE(NULLIF(sub_category, ''), 'Unclassified mistake') AS sub_category
                        FROM bt_3_detailed_mistakes
                        WHERE sentence_id = %s AND user_id = %s;
                        """,
                        (sentence_id_for_mistake, user_id),
                    )
                    resolved_skill_targets = cursor.fetchall()

                    cursor.execute(
                        """
                        SELECT attempt
                        FROM bt_3_attempts
                        WHERE id_for_mistake_table = %s AND user_id = %s;
                        """,
                        (sentence_id_for_mistake, user_id),
                    )

                    result = cursor.fetchone()
                    total_attempts = (result[0] or 0) + 1

                    cursor.execute(
                        """
                        INSERT INTO bt_3_successful_translations (user_id, sentence_id, score, attempt, date)
                        VALUES (%s, %s, %s, %s, NOW());
                        """,
                        (user_id, sentence_id_for_mistake, score_value, total_attempts),
                    )

                    cursor.execute(
                        """
                        DELETE FROM bt_3_detailed_mistakes
                        WHERE sentence_id = %s AND user_id = %s;
                        """,
                        (sentence_id_for_mistake, user_id),
                    )

                    cursor.execute(
                        """
                        DELETE FROM bt_3_attempts
                        WHERE id_for_mistake_table = %s AND user_id= %s;
                        """,
                        (sentence_id_for_mistake, user_id),
                    )

                    conn.commit()
                    for main_category, sub_category in resolved_skill_targets:
                        try:
                            apply_skill_events_for_error(
                                user_id=int(user_id),
                                source_lang=source_lang or "ru",
                                target_lang=target_lang or "de",
                                error_category=str(main_category or "Other mistake"),
                                error_subcategory=str(sub_category or "Unclassified mistake"),
                                event_type="success",
                                success_delta=2.0,
                                fail_delta=-3.0,
                            )
                        except Exception as exc:
                            logging.warning("Skill success update skipped: %s", exc)
                else:
                    cursor.execute(
                        """
                        INSERT INTO bt_3_attempts (user_id, id_for_mistake_table, timestamp)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT (user_id, id_for_mistake_table)
                        DO UPDATE SET
                            attempt = bt_3_attempts.attempt + 1,
                            timestamp= NOW();
                        """,
                        (user_id, sentence_id_for_mistake),
                    )
                    conn.commit()
                    await log_translation_mistake(
                        user_id,
                        original_text,
                        user_translation,
                        categories,
                        subcategories,
                        score_value,
                        correct_translation,
                        source_lang=source_lang or "ru",
                        target_lang=target_lang or "de",
                    )
            else:
                if score_value >= 80:
                    cursor.execute(
                        """
                        INSERT INTO bt_3_successful_translations (user_id, sentence_id, score, attempt, date)
                        VALUES(%s, %s, %s, %s, NOW());
                        """,
                        (user_id, sentence_id_for_mistake, score_value, 1),
                    )
                    conn.commit()
                    if categories and subcategories:
                        valid_success_combinations: list[tuple[str, str]] = []
                        for cat in categories:
                            cat_lower = str(cat or "").lower()
                            for subcat in subcategories:
                                subcat_lower = str(subcat or "").lower()
                                if cat_lower in language_subcategories_lower and subcat_lower in language_subcategories_lower[cat_lower]:
                                    canonical_cat = next(
                                        (x for x in language_categories if x.lower() == cat_lower),
                                        str(cat or ""),
                                    )
                                    canonical_sub = next(
                                        (
                                            x
                                            for x in language_subcategories.get(canonical_cat, [])
                                            if x.lower() == subcat_lower
                                        ),
                                        str(subcat or ""),
                                    )
                                    valid_success_combinations.append((canonical_cat, canonical_sub))
                        for main_category, sub_category in set(valid_success_combinations):
                            try:
                                apply_skill_events_for_error(
                                    user_id=int(user_id),
                                    source_lang=source_lang or "ru",
                                    target_lang=target_lang or "de",
                                    error_category=main_category,
                                    error_subcategory=sub_category,
                                    event_type="success",
                                    success_delta=2.0,
                                    fail_delta=-3.0,
                                )
                            except Exception as exc:
                                logging.warning("Skill success update skipped: %s", exc)
                else:
                    cursor.execute(
                        """
                        INSERT INTO bt_3_attempts (user_id, id_for_mistake_table)
                        VALUES (%s, %s)
                        ON CONFLICT (user_id, id_for_mistake_table)
                        DO UPDATE SET attempt = bt_3_attempts.attempt + 1;
                        """,
                        (user_id, sentence_id_for_mistake),
                    )
                    conn.commit()

                    await log_translation_mistake(
                        user_id,
                        original_text,
                        user_translation,
                        categories,
                        subcategories,
                        score_value,
                        correct_translation,
                        source_lang=source_lang or "ru",
                        target_lang=target_lang or "de",
                    )

            results.append(
                {
                    "translation_id": translation_id,
                    "audio_grammar_opt_in": False,
                    "sentence_number": sentence_number,
                    "score": score_value,
                    "original_text": original_text,
                    "user_translation": user_translation,
                    "correct_translation": correct_translation,
                    "feedback": feedback,
                }
            )

        results.sort(key=lambda item: item.get("sentence_number") or 0)
        return results

    finally:
        cursor.close()
        conn.close()


def finish_translation_webapp(user_id: int) -> dict[str, Any]:
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT session_id
            FROM bt_3_user_progress
            WHERE user_id = %s AND completed = FALSE
            ORDER BY start_time DESC
            LIMIT 1;
            """,
            (user_id,),
        )
        session = cursor.fetchone()
        if not session:
            return {
                "message": (
                    "❌ У вас нет активных сессий! Используйте кнопки: "
                    "'📌 Выбрать тему' -> '🚀 Начать перевод' чтобы начать."
                ),
                "status": "no_session",
            }

        session_id = session[0]

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM bt_3_daily_sentences
            WHERE user_id = %s AND session_id = %s;
            """,
            (user_id, session_id),
        )
        total_sentences = cursor.fetchone()[0] or 0

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM bt_3_translations
            WHERE user_id = %s AND session_id = %s;
            """,
            (user_id, session_id),
        )
        translated_count = cursor.fetchone()[0] or 0

        cursor.execute(
            """
            UPDATE bt_3_user_progress
            SET end_time = NOW(), completed = TRUE
            WHERE user_id = %s AND session_id = %s AND completed = FALSE;
            """,
            (user_id, session_id),
        )
        conn.commit()

        if translated_count == 0:
            message = (
                f"😔 Вы не перевели ни одного предложения из {total_sentences} в этой сессии.\n"
                "Попробуйте начать новую сессию с помощью кнопок "
                "'📌 Выбрать тему' -> '🚀 Начать перевод'."
            )
        elif translated_count < total_sentences:
            message = (
                f"⚠️ Вы перевели {translated_count} из {total_sentences} предложений.\n"
                "Перевод завершён, но не все предложения переведены. "
                "Это повлияет на ваш итоговый балл."
            )
        else:
            message = (
                "🎉 Вы успешно завершили перевод!\n"
                f"Все {total_sentences} предложений этой сессии переведены! 🚀"
            )

        return {
            "message": message,
            "status": "completed",
            "total_sentences": total_sentences,
            "translated_count": translated_count,
        }
    finally:
        cursor.close()
        conn.close()


def build_user_daily_summary(user_id: int, username: str | None) -> str | None:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(DISTINCT ds.id) AS total_sentences,
                    COUNT(DISTINCT t.id) AS translated,
                    (COUNT(DISTINCT ds.id) - COUNT(DISTINCT t.id)) AS missed,
                    COALESCE(p.avg_time, 0) AS avg_time_minutes,
                    COALESCE(p.total_time, 0) AS total_time_minutes,
                    COALESCE(AVG(t.score), 0) AS avg_score,
                    COALESCE(AVG(t.score), 0)
                        - (COALESCE(p.avg_time, 0) * 1)
                        - ((COUNT(DISTINCT ds.id) - COUNT(DISTINCT t.id)) * 20) AS final_score
                FROM bt_3_daily_sentences ds
                LEFT JOIN bt_3_translations t
                    ON ds.user_id = t.user_id
                    AND ds.id = t.sentence_id
                LEFT JOIN (
                    SELECT user_id,
                        AVG(EXTRACT(EPOCH FROM (end_time - start_time))/60) AS avg_time,
                        SUM(EXTRACT(EPOCH FROM (end_time - start_time))/60) AS total_time
                    FROM bt_3_user_progress
                    WHERE completed = TRUE
                        AND start_time::date = CURRENT_DATE
                    GROUP BY user_id
                ) p ON ds.user_id = p.user_id
                WHERE ds.date = CURRENT_DATE AND ds.user_id = %s
                GROUP BY ds.user_id, p.avg_time, p.total_time;
                """,
                (user_id,),
            )
            row = cursor.fetchone()

    if not row:
        return None

    total_sentences, translated, missed, avg_minutes, total_minutes, avg_score, final_score = row
    display_name = username or f"user_{user_id}"

    return (
        f"📅 Сегодняшняя статистика ({display_name})\n"
        f"📜 Всего предложений: {total_sentences}\n"
        f"✅ Переведено: {translated}\n"
        f"🚨 Не переведено: {missed}\n"
        f"⏱ Среднее время сессии: {avg_minutes:.1f} мин\n"
        f"⏱ Время общее: {total_minutes:.1f} мин\n"
        f"🎯 Средняя оценка: {avg_score:.1f}/100\n"
        f"🏆 Итоговый балл: {final_score:.1f}"
    )
