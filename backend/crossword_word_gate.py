# -*- coding: utf-8 -*-
"""Приёмка слов для кроссворда: пропускаем только живой немецкий.

Разбор банка 31.07 (72 кроссворда, 539 слов) показал, чем он был набит:
  • 36 % загаданных слов не встречаются даже в 50 000 самых частых слов;
  • только 8 % входят в первые 2000 — то есть в то, чем реально говорят;
  • в 31 % кроссвордов стояло слово, которого в немецком языке НЕТ:
    TUGENDHAFTIG (есть tugendhaft), DEONTOLIGIE и ETIMOLOGIE (опечатки),
    AMPLELICHT, MIVVERKEHR, TIERWOEHNDE, обрубки VERSUCHSAN и VEROEFFENT,
    родительный падеж PARKPLATZES, английские NIECE и BAGGAGE.

Причина была не в модели, а в отсутствии приёмки: старая проверка смотрела только
длину слова и что символы буквенные — MIVVERKEHR проходил её идеально.

Здесь слово проходит два сита:
  1. само слово стоит в частотном списке живого немецкого (первые 30 000);
  2. либо это понятное сложное слово: обе части — ходовые слова
     (Regenschirm = Regen + Schirm, Spülmaschine = spül + Maschine).
Не прошло ни то, ни другое — слова не существует, и в кроссворд оно не попадает.

Порог 30 000, а не 12 000, потому что частотный список построен на текстах и
занижает ПРЕДМЕТЫ: «Spülmaschine» стоит на 33 467 месте, «Fußgänger» на 25 227 —
вещи эти есть у каждого. Их спасает второе сито (разбор на части).

Отдельная планка для ЗАГАДАННЫХ слов (`HIDDEN_MAX_RANK`): то, что человек набирает
руками, должно быть словом из его собственной речи, а не просто существующим словом.

Замер на живом банке: сито пропускает 29 из 30 бытовых слов и отсекает 29 из 31
выдуманного.
"""
from __future__ import annotations

from backend.article_word_gate import word_rank

# ─── Пороги ───────────────────────────────────────────────────────────────────

DIRECT_MAX_RANK = 30_000   # слово стоит в живом языке само по себе
PART_MAX_RANK = 15_000     # обе части сложного слова должны быть ходовыми
HIDDEN_MAX_RANK = 12_000   # загаданное слово — только из обиходной речи

MIN_LEN = 4
MAX_LEN = 13   # WASCHMASCHINE и KOPFSCHMERZEN — ходовые слова, терять их незачем

_ALLOWED = set("ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÜ")

# Соединительные вставки в сложных словах: Arbeit-s-platz, Straße-n-bahn.
_LINKS = ("", "s", "n", "en", "e", "es", "er")

# Суффиксы, а не слова: «robust + heit» — это не сложное слово, а одно
# производное, и второй частью такое считать нельзя.
_SUFFIXES = {"heit", "keit", "ung", "nis", "schaft", "tum", "lich", "isch", "bar"}

_MIN_PART_HEAD = 4   # первая часть короче — совпадение случайное
_MIN_PART_TAIL = 5   # вторая часть короче — обычно суффикс, а не слово


# ─── Написание ────────────────────────────────────────────────────────────────

_UMLAUT_PAIRS = (("AE", "Ä"), ("OE", "Ö"), ("UE", "Ü"))


def _variants(word: str) -> list[str]:
    """Написания слова, которые стоит проверить по словарю.

    Модель пишет умляуты то настоящими буквами (KÜHLSCHRANK), то транслитом
    (REISEGEPAECK), то теряет их вовсе (KUNSTLER). Наугад заменять AE→Ä нельзя:
    в STEUERZAHLER и в BAUER это обычные буквы. Поэтому мы не «исправляем», а
    перебираем написания и берём то, которое в языке действительно есть.

    Заодно ищем ß: в частотном списке стоит «straßenbahn», а модель пишет
    STRASSENBAHN — без этой пары слово выглядело бы несуществующим.
    """
    out = [word]
    for src, dst in (*_UMLAUT_PAIRS, ("SS", "ß")):
        if src not in word:
            continue
        for candidate in list(out):
            if src in candidate:
                out.append(candidate.replace(src, dst))
    # Одиночная потерянная умляут-буква: KUNSTLER → KÜNSTLER, ARZTEHAUS → ÄRZTEHAUS.
    for base in list(out):
        for plain, uml in (("A", "Ä"), ("O", "Ö"), ("U", "Ü")):
            for i, ch in enumerate(base):
                if ch == plain:
                    out.append(base[:i] + uml + base[i + 1:])
    seen: set[str] = set()
    uniq: list[str] = []
    for w in out:
        if w not in seen:
            seen.add(w)
            uniq.append(w)
    return uniq


def _display(word: str) -> str:
    """Вид для сетки: сетка заглавная, а заглавная ß в немецком — это SS."""
    return word.replace("ß", "SS")


def normalize_word(raw: str) -> str:
    """Слово в том виде, в каком оно ляжет в сетку.

    Из всех написаний выбираем подтверждённое словарём: сначала то, что стоит
    в частотном списке целиком, потом то, что разбирается на две ходовые части.
    Не подтвердилось ничего — возвращаем как есть, дальше его отсеет `check_word`.
    """
    word = str(raw or "").strip().upper().replace("-", "").replace(" ", "")
    if not word:
        return ""
    variants = _variants(word)
    ranked = [(r, v) for r, v in ((word_rank(v), v) for v in variants) if r]
    if ranked:
        return _display(min(ranked)[1])
    for v in variants:
        if split_compound(v):
            return _display(v)
    return _display(word)


# ─── Сито ─────────────────────────────────────────────────────────────────────

def split_compound(word: str) -> tuple[str, str] | None:
    """Разбор сложного слова на две ходовые части, если он возможен."""
    low = str(word or "").lower()
    for i in range(_MIN_PART_HEAD, max(_MIN_PART_HEAD, len(low) - _MIN_PART_TAIL) + 1):
        head, tail = low[:i], low[i:]
        if len(tail) < _MIN_PART_TAIL or tail in _SUFFIXES:
            continue
        tail_rank = word_rank(tail)
        if not tail_rank or tail_rank > PART_MAX_RANK:
            continue
        for link in _LINKS:
            if link and not head.endswith(link):
                continue
            stem = head[: len(head) - len(link)] if link else head
            if len(stem) < _MIN_PART_HEAD:
                continue
            stem_rank = word_rank(stem)
            if stem_rank and stem_rank <= PART_MAX_RANK:
                return stem, tail
    return None


def _attested_rank(word: str) -> int | None:
    """Место слова в живом языке — по любому из его написаний (ß, умляуты)."""
    ranks = [r for r in (word_rank(v) for v in _variants(word)) if r]
    return min(ranks) if ranks else None


def _attested_split(word: str) -> tuple[str, str] | None:
    for v in _variants(word):
        parts = split_compound(v)
        if parts:
            return parts
    return None


def check_word(word: str) -> tuple[bool, str]:
    """→ (брать ли слово, причина отказа). Слово уже нормализовано."""
    clean = str(word or "").strip()
    if not clean:
        return False, "пустое слово"
    if len(clean) < MIN_LEN or len(clean) > MAX_LEN:
        return False, f"длина {len(clean)} вне {MIN_LEN}-{MAX_LEN}"
    if not set(clean) <= _ALLOWED:
        return False, "посторонние символы"

    rank = _attested_rank(clean)
    if rank and rank <= DIRECT_MAX_RANK:
        return True, ""
    if _attested_split(clean):
        return True, ""
    return False, "в живом немецком такого слова нет"


def everyday_rank(word: str) -> int | None:
    """Насколько слово обиходное: меньше — привычнее. None — не измеряется.

    У сложного слова берём место его РЕДКОЙ части: Kühlschrank человек знает
    ровно настолько, насколько знает Schrank.
    """
    rank = _attested_rank(str(word or "").strip())
    if rank:
        return rank
    parts = _attested_split(str(word or "").strip())
    if parts:
        ranks = [word_rank(p) or 10 ** 9 for p in parts]
        return max(ranks)
    return None


def is_everyday(word: str) -> bool:
    """Годится ли слово в ЗАГАДАННЫЕ — те, что человек набирает руками."""
    rank = everyday_rank(word)
    return bool(rank and rank <= HIDDEN_MAX_RANK)
