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

from backend.article_word_gate import frequency_table
from backend.article_word_gate import word_rank as part_rank  # с догадкой по окончанию


def word_rank(word: str) -> int | None:
    """Место слова в частотном списке — СТРОГО по написанию, без догадок.

    Общий `article_word_gate.word_rank` при промахе пробует дописать окончание
    множественного числа, и для тренажёра артиклей это правильно. Здесь — нет:
    из-за этой догадки «FÜTTER» получал ранг слова «füttern» (4876 вместо 39575)
    и уезжал в кроссворд как отдельное слово, хотя такого слова нет — есть глагол
    «füttern» и существительное «das Futter».
    """
    low = str(word or "").strip().lower()
    return frequency_table().get(low) if low else None

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
    """Разбор сложного слова на две ходовые части, если он возможен.

    Части ищем с догадкой по окончанию (`part_rank`), а не строго: в немецких
    сложных словах первой частью стоит как раз основа — Spül-maschine, Wasch-becken,
    Koch-topf. Целое слово проверяется строго, а вот часть основой быть обязана.
    """
    low = str(word or "").lower()
    for i in range(_MIN_PART_HEAD, max(_MIN_PART_HEAD, len(low) - _MIN_PART_TAIL) + 1):
        head, tail = low[:i], low[i:]
        if len(tail) < _MIN_PART_TAIL or tail in _SUFFIXES:
            continue
        tail_rank = part_rank(tail)
        if not tail_rank or tail_rank > PART_MAX_RANK:
            continue
        for link in _LINKS:
            if link and not head.endswith(link):
                continue
            stem = head[: len(head) - len(link)] if link else head
            if len(stem) < _MIN_PART_HEAD:
                continue
            stem_rank = part_rank(stem)
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


# Окончания, которые превращают основу глагола в инфинитив: fütter → füttern,
# sammel → sammeln, atme → atmen.
_INFINITIVE_TAILS = ("n", "en")


def form_lookup_keys(words) -> list[str]:
    """Что именно спрашивать у словаря форм: само слово, его написания и инфинитивы.

    Спрашиваем разом и «fütter», и «füttern» — по тому, чего в словаре нет, а что
    есть, и видно, что перед нами основа, а не слово.
    """
    keys: list[str] = []
    seen: set[str] = set()
    for raw in words or []:
        word = str(raw or "").strip().upper()
        if not word:
            continue
        for variant in _variants(word):
            for candidate in (variant, *(variant + t.upper() for t in _INFINITIVE_TAILS)):
                low = candidate.lower()
                if low not in seen:
                    seen.add(low)
                    keys.append(low)
    return keys


def wrong_dictionary_form(word: str, lemma_pos: dict[str, str] | None) -> str:
    """Причина, по которой слово стоит не в словарной форме. Пусто — всё в порядке.

    `lemma_pos` — словарные формы из FreeDict (lemma → часть речи). Сам корпус
    частотности этого не различает: он собран из живой речи, где «fütter ihn»
    встречается наравне с «füttern». Словарь различает: «füttern» в нём есть,
    «fütter» — нет.
    """
    if not lemma_pos:
        return ""
    forms = [v.lower() for v in _variants(word)]
    if any(f in lemma_pos for f in forms):
        return ""   # слово само стоит в словаре — форма правильная
    for form in forms:
        for tail in _INFINITIVE_TAILS:
            if lemma_pos.get(form + tail) == "verb":
                return f"это основа глагола, а не слово (есть {form + tail})"
    return ""


def check_word(word: str, *, lemma_pos: dict[str, str] | None = None) -> tuple[bool, str]:
    """→ (брать ли слово, причина отказа). Слово уже нормализовано."""
    clean = str(word or "").strip()
    if not clean:
        return False, "пустое слово"
    if len(clean) < MIN_LEN or len(clean) > MAX_LEN:
        return False, f"длина {len(clean)} вне {MIN_LEN}-{MAX_LEN}"
    if not set(clean) <= _ALLOWED:
        return False, "посторонние символы"

    bad_form = wrong_dictionary_form(clean, lemma_pos)
    if bad_form:
        return False, bad_form

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


# ─── Второе мнение о форме ────────────────────────────────────────────────────

_JUDGE_PROMPT = """\
Ниже немецкие слова для кроссворда. По каждому ответь, годится ли оно.

ДА — если выполнено ВСЁ:
  • это правильная словарная форма: существительное — именительный падеж
    единственного числа (или устойчивое множественное вроде Kopfschmerzen,
    Eltern, Ferien); глагол — инфинитив; прилагательное — базовая форма;
  • так действительно говорят по-немецки: слово стоит в словаре или это
    обычное составное слово, которое немцы правда употребляют
    (Regenjacke, Trinkflasche, Ladekabel).

НЕТ — если верно хотя бы одно:
  • это форма другого слова: BAUERN (мн. ч. от Bauer), FÜTTER (основа от füttern),
    GEKOMMEN (причастие), HÄUSER (мн. ч.);
  • это склейка, которую никто не говорит: PASSTASCHE, KAFFEEBEUTEL, PULSMESSER;
  • пишется раздельно: SAUBERMACHEN (sauber machen), KENNENLERNEN — допустимо,
    но SPAZIERENGEHEN уже нет;
  • диалектное или областное слово: KEHRWOCHE, SEMMEL, ERDÄPFEL.

Сомневаешься — отвечай НЕТ: лишнее слово в кроссворде хуже, чем недостающее.

Ответь СТРОГИМ JSON: {{"слово": true|false, ...}} — ключи ровно как даны.

{words}"""


class FormJudgeUnavailable(Exception):
    """Ответа от модели не было (сеть, ключ, таймаут) — это НЕ «слово плохое»."""


def judge_word_forms(words: list[str]) -> dict[str, bool]:
    """Спросить модель, стоят ли слова в словарной форме и говорят ли так.

    Частотный список и словарь форм отвечают на вопрос «слово существует?».
    На вопрос «так вообще говорят?» offline-данные ответить не могут: REGENJACKE и
    PASSTASCHE одинаково отсутствуют и в списке, и в словаре, а первое нормально,
    второе — выдумка. Это и есть работа второго мнения.

    Один запрос на весь набор слов кроссворда — дёшево. Сбой запроса поднимает
    FormJudgeUnavailable: решение, что делать, принимает вызывающий.
    """
    clean = [str(w).strip() for w in words if str(w or "").strip()]
    if not clean:
        return {}
    import json
    import os

    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise FormJudgeUnavailable("OPENAI_API_KEY not set")
    model = (os.getenv("OPENAI_CROSSWORD_JUDGE_MODEL") or "gpt-4.1").strip()
    try:
        import requests as _requests
        resp = _requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": model,
                "temperature": 0,
                "response_format": {"type": "json_object"},
                "messages": [{
                    "role": "user",
                    "content": _JUDGE_PROMPT.format(words="\n".join("- " + w for w in clean)),
                }],
            },
            timeout=60,
        )
        if not resp.ok:
            raise FormJudgeUnavailable(f"OpenAI HTTP {resp.status_code}")
        payload = resp.json()
        data = json.loads((payload.get("choices") or [{}])[0].get("message", {}).get("content") or "{}")
    except FormJudgeUnavailable:
        raise
    except Exception as exc:
        raise FormJudgeUnavailable(str(exc)) from exc

    try:
        from backend.openai_usage_logging import log_openai_raw_usage
        log_openai_raw_usage(action_type="pool_crossword_judge", model=model,
                             usage=payload.get("usage"), user_id=None)
    except Exception:
        pass

    # Слово, о котором модель промолчала, считаем годным: молчание — не приговор,
    # а сито существования оно уже прошло.
    return {w: bool(data.get(w, True)) for w in clean}
