"""Страж мнемоник артикля: не пускает в базу подсказку, которая учит неправде.

Зачем. Артикль слова у нас защищён (род берётся из Wiktionary, см. article_wiktionary_ref),
а вот ТЕКСТ подсказки — свободная генерация, и его не проверял никто. В проде это дало
«das Körper (тело) → значит der Wirbelkörper» (Körper — der, и фраза сама себе противоречит)
и «-nis — почти всегда die» (наша же таблица правил говорит обратное: обычно das).

Три детерминированные проверки, ноль обращений к модели:

  1. АРТИКЛЬ ВНУТРИ ТЕКСТА. Каждое «der/die/das Существительное» сверяется с известным
     родом. Двухродовые слова (der/die Leiter — руководитель/лестница) пропускаются:
     для них артикль зависит от значения, и подсказка вправе назвать любой.

  2. ПРАВИЛО СУФФИКСА. Утверждение вида «-nis — почти всегда die» сверяется с нашей
     собственной таблицей _STRONG/_WEAK_SUFFIX_RULES. Противоречит — подсказка не проходит.

  3. САМОПРОТИВОРЕЧИЕ КОМПОЗИТА. Если подсказка называет род головы, он обязан совпасть
     с артиклем самого слова: род немецкого композита = род последней части.

Отвергнутая подсказка не сохраняется; вызывающий код падает на детерминированный
gender_tip() из article_learn — он строится по той же таблице правил и врать не может.
"""
from __future__ import annotations

import logging
import re
import threading
import time

# «der Hund», «die Katze» — артикль + существительное с заглавной буквы
_ART_NOUN_RE = re.compile(r"\b(der|die|das)\s+([A-ZÄÖÜ][a-zäöüßA-ZÄÖÜ-]{2,})")
# «-nis — почти всегда die», «-chen → das», «-ung: die»
_SUFFIX_CLAIM_RE = re.compile(
    r"[«\"'\s(]-([a-zäöüß]{2,7})[»\"']?\s*(?:—|-|–|→|:)?[^.;]{0,45}?\b(der|die|das)\b"
)

_CACHE_TTL_SEC = 900
_lock = threading.Lock()
_known: dict[str, str] = {}
_ambiguous: set[str] = set()
_loaded_at: float = 0.0


def _two_gender_words() -> set[str]:
    try:
        from backend.article_two_gender import TWO_GENDER_NOUNS
        return {str(item.get("word") or "").strip().lower()
                for item in TWO_GENDER_NOUNS if item.get("word")}
    except Exception:
        return set()


def _load_genders() -> tuple[dict[str, str], set[str]]:
    """{lemma: article} + множество двухродовых. Кешируется на 15 минут."""
    global _known, _ambiguous, _loaded_at
    now = time.time()
    with _lock:
        if _known and (now - _loaded_at) < _CACHE_TTL_SEC:
            return _known, _ambiguous
    known: dict[str, str] = {}
    by_word: dict[str, set] = {}
    try:
        from backend.database import get_db_connection_context
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT title, genus FROM bt_3_wiktionary_genus_cache "
                    "WHERE COALESCE(genus,'') <> ''"
                )
                g2a = {"m": "der", "f": "die", "n": "das"}
                for title, genus in cur.fetchall() or []:
                    code = str(genus or "").strip().lower()
                    # только ОДНОЗНАЧНЫЕ: "mf"/"nf" означают два рода, правило неприменимо
                    if len(code) == 1 and code in g2a:
                        known[str(title).strip().lower()] = g2a[code]
                cur.execute(
                    "SELECT word, article FROM bt_3_article_sprint_nouns "
                    "WHERE COALESCE(article,'') <> ''"
                )
                for word, article in cur.fetchall() or []:
                    by_word.setdefault(str(word).strip().lower(), set()).add(
                        str(article).strip().lower())
    except Exception:
        logging.warning("mnemonic guard: не смог загрузить роды", exc_info=True)
        return {}, set()

    ambiguous = {w for w, arts in by_word.items() if len(arts) > 1} | _two_gender_words()
    for w, arts in by_word.items():
        if len(arts) == 1:
            known.setdefault(w, next(iter(arts)))
    for w in ambiguous:
        known.pop(w, None)

    with _lock:
        _known, _ambiguous, _loaded_at = known, ambiguous, now
    return known, ambiguous


def _suffix_table() -> list[tuple[str, str]]:
    """(суффикс, артикль) из нашей же таблицы правил — единственный источник истины."""
    try:
        from backend.article_learn import _STRONG_SUFFIX_RULES, _WEAK_SUFFIX_RULES
        return ([(s, a) for s, a, _ in _STRONG_SUFFIX_RULES]
                + [(s, a) for s, a, _ in _WEAK_SUFFIX_RULES])
    except Exception:
        return []


def validate_mnemonic(*, word: str, article: str, text: str) -> tuple[bool, str]:
    """(ок, причина отказа). Пустая причина — подсказка безопасна."""
    w = str(word or "").strip()
    art = str(article or "").strip().lower()
    body = str(text or "").strip()
    if not w or not body:
        return False, "пустая подсказка"

    known, ambiguous = _load_genders()

    # 1. артикль при существительном внутри текста
    for m in _ART_NOUN_RE.finditer(body):
        said_art, noun = m.group(1).lower(), m.group(2)
        low = noun.lower()
        # Самый прямой случай: подсказка называет артикль САМОГО слова. Он обязан совпасть
        # с тем, что стоит в базе, — иначе карточка и подсказка учат разному (так и вышло
        # с «die Platzbauch» после исправления артикля на der).
        if low == w.lower():
            if said_art != art:
                return False, f"подсказка говорит «{said_art} {w}», а в базе «{art} {w}»"
            continue
        if low in ambiguous:
            continue
        real = known.get(low)
        if real and real != said_art:
            return False, f"в тексте «{said_art} {noun}», а на самом деле «{real} {noun}»"

    # 2. утверждение про суффикс против нашей таблицы
    table = _suffix_table()
    for m in _SUFFIX_CLAIM_RE.finditer(body):
        suffix, claimed = m.group(1).lower(), m.group(2).lower()
        for suf, expected in table:
            if suf == suffix and expected != claimed:
                return False, f"правило «-{suffix} → {claimed}» противоречит нашему «-{suf} → {expected}»"

    # 3. самопротиворечие композита: род головы должен совпасть с артиклем слова
    for m in _ART_NOUN_RE.finditer(body):
        said_art, noun = m.group(1).lower(), m.group(2)
        low = noun.lower()
        if low == w.lower() or low in ambiguous:
            continue
        if w.lower().endswith(low) and len(low) >= 4 and said_art != art:
            return False, (f"голова композита названа «{said_art} {noun}», "
                           f"но слово в базе «{art} {w}» — композит наследует род головы")

    return True, ""
