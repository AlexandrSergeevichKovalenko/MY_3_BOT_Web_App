"""Deep-dive actions for a quiz-result card, served by the web tier.

The Telegram result card carries ONE Mini-App button (?startapp=dive_<id>)
instead of four callback buttons that used to dump a new message at the bottom
of the chat. The overlay opens in place and triggers these actions:

    📌 feel    — "почувствовать слово" (run_feel_word)
    🧩 phrase  — a short collocation with the correct variant (+ save to dict)
    🔊 audio   — handled by the existing /api/webapp/tts/* endpoints (not here)
    ❓ ask     — handled by the existing /api/webapp/ask endpoint (AskOverlay)

This module is pure orchestration over openai_manager + database, with NO
dependency on the bot process (whose in-memory pending_* dicts the web tier
can't see). Card context is persisted in bt_3_deepdive_cards.
"""

import asyncio
import logging
import re

from backend.openai_manager import (
    run_feel_word,
    run_feel_word_multilang,
    run_dictionary_collocations,
    run_dictionary_collocations_multilang,
)
from backend.database import (
    get_deepdive_card,
    save_webapp_dictionary_query_returning_id_with_inserted,
    normalize_dictionary_semantic_tag,
    get_or_create_dictionary_semantic_folder,
)

logger = logging.getLogger(__name__)

_LEGACY_RU_DE = {("ru", "de"), ("de", "ru")}


def _norm_compare_key(text: str) -> str:
    """Mirror bot_3._normalize_dictionary_compare_key so phrase ranking matches."""
    normalized = str(text or "").strip()
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = normalized.strip(" \t\r\n.,!?;:()[]{}\"'«»")
    return normalized.casefold()


def load_card(*, card_id: int, user_id: int) -> dict | None:
    """Load a deep-dive card the user owns. Returns the render context or None.

    Ownership matters: the four actions reveal the correct answer's collocations
    / pronunciation, which is fine — the user has already answered this quiz.
    But only the author should reopen their own card.
    """
    card = get_deepdive_card(card_id)
    if not card:
        return None
    if int(card.get("user_id") or 0) != int(user_id):
        return None
    return {
        "id": int(card["id"]),
        "source_text": str(card.get("source_text") or "").strip(),
        "target_text": str(card.get("target_text") or "").strip(),
        "source_lang": str(card.get("source_lang") or "").strip().lower(),
        "target_lang": str(card.get("target_lang") or "").strip().lower(),
        "context_text": str(card.get("context_text") or "").strip(),
    }


def generate_feel(card: dict) -> str:
    """'Почувствовать слово' — a feel-the-word explanation for the card pair."""
    source_text = str(card.get("source_text") or "").strip()
    target_text = str(card.get("target_text") or "").strip()
    source_lang = str(card.get("source_lang") or "").strip().lower()
    target_lang = str(card.get("target_lang") or "").strip().lower()
    if not source_text:
        raise ValueError("missing_source_text")

    if source_lang == "ru" and target_lang == "de":
        feel = asyncio.run(run_feel_word(source_text, target_text))
    elif source_lang == "de" and target_lang == "ru":
        feel = asyncio.run(run_feel_word(target_text or source_text, source_text))
    else:
        feel = asyncio.run(
            run_feel_word_multilang(
                source_text=source_text,
                target_text=target_text,
                source_lang=source_lang or "auto",
                target_lang=target_lang or "auto",
            )
        )
    return str(feel or "").strip()


def _rank_phrase_candidate(item, base_source_norm, base_len):
    source_value = str(item.get("source") or "").strip()
    source_norm = _norm_compare_key(source_value)
    source_word_count = len([p for p in re.split(r"\s+", source_value) if p])
    char_count = len(source_value)
    is_bare_word = 1 if source_norm == base_source_norm else 0
    contains_base = 0 if base_source_norm in source_norm else 1
    return (
        is_bare_word,
        contains_base,
        abs(source_word_count - 3),
        abs(char_count - max(12, base_len)),
        source_value.lower(),
    )


def generate_phrase(card: dict) -> dict:
    """'Дать сочетание' — pick the best short collocation for the correct word.

    Mirrors bot_3._generate_quiz_phrase_suggestion: prefer a real multi-word
    collocation that contains the input word, not the bare word itself.
    """
    source_text = str(card.get("source_text") or "").strip()
    target_text = str(card.get("target_text") or "").strip()
    source_lang = str(card.get("source_lang") or "").strip().lower()
    target_lang = str(card.get("target_lang") or "").strip().lower()
    if not source_text or not target_text or not source_lang or not target_lang:
        raise ValueError("missing_phrase_fields")

    direction = f"{source_lang}-{target_lang}"
    if (source_lang, target_lang) in _LEGACY_RU_DE:
        generated = asyncio.run(run_dictionary_collocations(direction, source_text, target_text))
    else:
        generated = asyncio.run(
            run_dictionary_collocations_multilang(
                source_lang=source_lang,
                target_lang=target_lang,
                word_source=source_text,
                word_target=target_text,
            )
        )

    # The generator now returns the semantic category in the SAME call (no second
    # LLM round-trip just to tag the save) — carry it through so a save lands in
    # the right folder immediately.
    semantic_tag = ""
    if isinstance(generated, dict):
        semantic_tag = normalize_dictionary_semantic_tag(generated.get("semantic_category"))

    items = generated.get("items") if isinstance(generated, dict) else []
    base_key = (_norm_compare_key(source_text), _norm_compare_key(target_text))
    base_source_norm = _norm_compare_key(source_text)
    seen: set = set()
    candidates: list[dict] = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        sv = str(item.get("source") or "").strip()
        tv = str(item.get("target") or "").strip()
        if not sv or not tv:
            continue
        ck = (_norm_compare_key(sv), _norm_compare_key(tv))
        if ck in seen or ck == base_key:
            continue
        seen.add(ck)
        candidates.append({"source": sv, "target": tv})

    if candidates:
        best = min(candidates, key=lambda c: _rank_phrase_candidate(c, base_source_norm, len(source_text)))
    else:
        best = {"source": source_text, "target": target_text}

    return {
        "source_text": str(best.get("source") or "").strip() or source_text,
        "target_text": str(best.get("target") or "").strip() or target_text,
        "source_lang": source_lang,
        "target_lang": target_lang,
        "semantic_category": semantic_tag,
    }


def save_phrase(
    *,
    user_id: int,
    source_text: str,
    target_text: str,
    source_lang: str,
    target_lang: str,
    semantic_tag: str = "",
) -> tuple[bool, int]:
    """Save a collocation pair to the user's dictionary. Returns (ok, entry_id).

    Mirrors the core of bot_3._save_dictionary_option_for_user: builds the
    canonical RU<->DE legacy columns + response_json, then inserts. The semantic
    category comes free with the collocation generation (no second LLM call), so
    when present the entry is routed straight into the matching semantic folder —
    no waiting for the nightly tag backfill. (Free-save limits stay bot-side.)
    """
    source = str(source_text or "").strip()
    target = str(target_text or "").strip()
    source_lang = str(source_lang or "").strip().lower()
    target_lang = str(target_lang or "").strip().lower()
    if not source or not target or not source_lang or not target_lang:
        return False, 0

    semantic_tag = normalize_dictionary_semantic_tag(semantic_tag)
    folder_id = None
    if semantic_tag:
        try:
            folder = get_or_create_dictionary_semantic_folder(int(user_id), semantic_tag)
            if folder and folder.get("id"):
                folder_id = int(folder["id"])
        except Exception:
            logger.warning("deepdive folder resolve failed user_id=%s tag=%s", user_id, semantic_tag, exc_info=True)

    response_json = {
        "word_source": source,
        "word_target": target,
        "source_text": source,
        "target_text": target,
        "source_lang": source_lang,
        "target_lang": target_lang,
        "language_pair": {"source_lang": source_lang, "target_lang": target_lang},
    }

    if source_lang == "ru" and target_lang == "de":
        word_ru, word_de, translation_de, translation_ru = source, target, target, source
    elif source_lang == "de" and target_lang == "ru":
        word_ru, word_de, translation_de, translation_ru = target, source, source, target
    else:
        word_ru = source if source_lang == "ru" else (target if target_lang == "ru" else None)
        word_de = source if source_lang == "de" else (target if target_lang == "de" else None)
        translation_de = target if target_lang == "de" else (source if source_lang == "de" else None)
        translation_ru = target if target_lang == "ru" else (source if source_lang == "ru" else None)

    if word_ru:
        response_json["word_ru"] = word_ru
    if word_de:
        response_json["word_de"] = word_de
    if translation_de:
        response_json["translation_de"] = translation_de
    if translation_ru:
        response_json["translation_ru"] = translation_ru
    if semantic_tag:
        response_json["semantic_category"] = semantic_tag

    try:
        entry_id, _inserted = save_webapp_dictionary_query_returning_id_with_inserted(
            user_id=int(user_id),
            word_ru=word_ru,
            translation_de=translation_de,
            word_de=word_de,
            translation_ru=translation_ru,
            response_json=response_json,
            folder_id=folder_id,
            source_lang=source_lang,
            target_lang=target_lang,
            origin_process="webapp_deepdive_save",
            origin_meta={"flow": "deepdive_phrase", "source": "miniapp"},
            semantic_tag=semantic_tag or None,
        )
    except Exception:
        logger.exception("deepdive save_phrase failed user_id=%s", user_id)
        return False, 0
    return True, int(entry_id or 0)
