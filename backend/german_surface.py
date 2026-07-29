"""Опознание немецкой поверхности: это СЛОВО или ФОРМА слова.

Зачем понадобился. Артикль принадлежит слову, а не форме слова, и в именительном
множественного он ВСЕГДА «die», независимо от рода. Мы же брали артикль леммы и
печатали его рядом с формой: «Проблемы» → «das Probleme» (das — артикль леммы
«das Problem»). Замер на проде: быстрый путь спрашивал артикль у модели, и на
множественном числе она ошибалась в 10 случаях из 20 (Probleme→das, Bücher→das,
Häuser→das, Tische→der, Männer→der…). Перепись данных нашла 242 разные формы,
живущие в заголовках карточек как самостоятельные слова.

Ступени опознания — от надёжной к слабой, сеть на горячем пути не трогаем:

  1. PLURALIA TANTUM: слова, у которых множественное И ЕСТЬ лемма (Eltern, Ferien,
     Kosten). Рода у них нет ни в одном справочнике, артикль всегда «die».
  2. КЭШ РОДОВ (bt_3_wiktionary_genus_cache, ~21k): род документирован у самой
     поверхности ⇒ это лемма единственного числа, артикль берём оттуда.
  3. ИНДЕКС ФОРМ (bt_3_german_form_index): наполняется ночным прогревом из
     de.wiktionary («Deklinierte Form» + Grundformverweis) и из forms_json. Даёт
     точный ответ «форма такого-то слова, число такое-то».
  4. ПРАВИЛО обратного словообразования (окончания + возврат умлаута) — срабатывает
     только если выведенная лемма документирована, а сама поверхность нет. Это
     ДОГАДКА: она годится, чтобы НЕ печатать артикль леммы рядом с формой, но не
     годится, чтобы что-то утверждать. Поэтому confidence='low' и article=''.
  5. Иначе — «не знаю». Пустой артикль честнее выдуманного.

Контракт: `article` — это то, что вызывающему РАЗРЕШЕНО напечатать. Пусто ⇒ не
печатать ничего. Так одно решение принимается в одном месте, а не в пяти.
"""
from __future__ import annotations

import logging
import re
import threading
import time

SG = "sg"
PL = "pl"
UNKNOWN = "unknown"

_CACHE_TTL_SEC = 900
_lock = threading.Lock()
_form_index: dict[str, tuple[str, str, str]] = {}
_index_loaded_at = 0.0

_CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")
_LEADING_ARTICLE_RE = re.compile(r"^(der|die|das)\s+", re.IGNORECASE)

# Существуют только во множественном: рода нет, артикль всегда «die». Список
# переехал сюда из backend_server — теперь у «слова или формы» один адрес.
PLURALIA_TANTUM = {
    "eltern", "großeltern", "urgroßeltern", "schwiegereltern", "stiefeltern",
    "pflegeeltern", "adoptiveltern", "geschwister", "leute", "ferien",
    "kosten", "unkosten", "einkünfte", "spesen", "finanzen", "personalien",
    "lebensmittel", "möbel", "trümmer", "masern", "pocken", "röteln",
}


def _clean_surface(word: str) -> str:
    """Голое немецкое существительное без артикля и хвостовой пунктуации, либо ''."""
    text = re.sub(r"\s+", " ", str(word or "").strip())
    text = _LEADING_ARTICLE_RE.sub("", text).strip()
    text = text.strip(".,!?;:«»\"'()")
    if not text or " " in text or _CYRILLIC_RE.search(text):
        return ""
    return text


def ensure_german_form_index_schema() -> None:
    """Индекс форм: поверхность → лемма + число. Ключ нормализован (casefold)."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bt_3_german_form_index (
                    surface_key  TEXT PRIMARY KEY,
                    surface      TEXT NOT NULL,
                    lemma        TEXT NOT NULL,
                    number_tag   TEXT NOT NULL,
                    source       TEXT NOT NULL,
                    checked_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        conn.commit()


def upsert_form_index(rows: list[dict]) -> int:
    """Записать [{surface, lemma, number_tag, source}]. Возвращает число строк.
    Пишем в отсортированном порядке ключей — два параллельных прогрева не
    заблокируют друг друга на пересекающихся словах."""
    items = []
    for row in rows or []:
        surface = _clean_surface(row.get("surface"))
        lemma = _clean_surface(row.get("lemma")) or surface
        number_tag = str(row.get("number_tag") or "").strip().lower()
        if not surface or number_tag not in (SG, PL):
            continue
        items.append((surface.casefold(), surface, lemma, number_tag,
                      str(row.get("source") or "wiktionary").strip()))
    if not items:
        return 0
    items.sort(key=lambda it: it[0])
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO bt_3_german_form_index
                    (surface_key, surface, lemma, number_tag, source, checked_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (surface_key) DO UPDATE SET
                    surface    = EXCLUDED.surface,
                    lemma      = EXCLUDED.lemma,
                    number_tag = EXCLUDED.number_tag,
                    source     = EXCLUDED.source,
                    checked_at = NOW();
                """,
                items,
            )
        conn.commit()
    return len(items)


def _load_form_index() -> dict[str, tuple[str, str, str]]:
    """{surface_key: (lemma, number_tag, source)}. Кеш в процессе на 15 минут.
    Недоступная база — не авария: возвращаем пусто, детектор просто теряет одну
    ступень и честнее говорит «не знаю»."""
    global _form_index, _index_loaded_at
    now = time.time()
    with _lock:
        if _form_index and (now - _index_loaded_at) < _CACHE_TTL_SEC:
            return _form_index
    index: dict[str, tuple[str, str, str]] = {}
    try:
        from backend.database import get_db_connection_context
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT surface_key, lemma, number_tag, source FROM bt_3_german_form_index;"
                )
                for key, lemma, number_tag, source in cursor.fetchall() or []:
                    index[str(key)] = (str(lemma), str(number_tag), str(source or ""))
    except Exception:
        logging.warning("german_surface: индекс форм недоступен", exc_info=True)
        return {}
    with _lock:
        _form_index, _index_loaded_at = index, now
    return index


def _documented_singular(word: str, *, allow_network: bool) -> str:
    """Артикль, если род САМОЙ поверхности документирован в Wiktionary, иначе ''.
    Правило композита сюда не пускаем: оно отвечает про род, а не про число, и на
    форме множественного («Fußballen») выдаст род головы."""
    try:
        from backend.article_authority import authoritative_article
        article, source = authoritative_article(word, allow_network=allow_network)
    except Exception:
        logging.warning("german_surface: справочник родов недоступен для %s", word, exc_info=True)
        return ""
    if article and str(source).startswith("wiktionary"):
        return article
    return ""


def _unumlaut(text: str) -> str:
    return (text.replace("ä", "a").replace("ö", "o").replace("ü", "u")
                .replace("Ä", "A").replace("Ö", "O").replace("Ü", "U"))


def _singular_candidates(surface: str) -> list[str]:
    """Возможные леммы, если `surface` — множественное. Порядок = от длинного
    окончания к короткому, чтобы «Freundinnen» проверилось как -innen, а не как -n."""
    out: list[str] = []

    def add(candidate: str) -> None:
        if candidate and len(candidate) >= 3 and candidate.casefold() != surface.casefold():
            if candidate not in out:
                out.append(candidate)
            back = _unumlaut(candidate)
            if back != candidate and back not in out:
                out.append(back)

    if surface.endswith("innen"):
        add(surface[:-3])                      # Freundinnen → Freundin
    if surface.endswith("en"):
        add(surface[:-2])                      # Frauen → Frau
        add(surface[:-1])                      # Straßen → Straße
    if surface.endswith("er"):
        add(surface[:-2])                      # Kinder → Kind
    if surface.endswith("e"):
        add(surface[:-1])                      # Probleme → Problem
    if surface.endswith("n"):
        add(surface[:-1])                      # Regeln → Regel
    if surface.endswith("s"):
        add(surface[:-1])                      # Autos → Auto
    return out


def german_surface(word: str, *, allow_network: bool = False) -> dict:
    """Опознать поверхность.

    Возвращает {"number", "lemma", "article", "source", "confidence"}, где
    `article` — то, что РАЗРЕШЕНО напечатать рядом с этой поверхностью:
      • единственное число  → артикль по роду («das» рядом с «Problem»);
      • множественное число → «die» («die Probleme»), но только когда число
        подтверждено справочником, а не выведено правилом;
      • не знаем            → '' , и печатать нельзя ничего.
    """
    surface = _clean_surface(word)
    if not surface:
        return {"number": UNKNOWN, "lemma": "", "article": "", "source": "не слово",
                "confidence": "low"}

    low = surface.casefold()

    # 1. Существует только во множественном — рода нет ни у кого, артикль «die».
    if low in PLURALIA_TANTUM:
        return {"number": PL, "lemma": surface, "article": "die",
                "source": "pluralia_tantum", "confidence": "high"}

    # 2. Род документирован у самой поверхности ⇒ это лемма единственного числа.
    documented = _documented_singular(surface, allow_network=allow_network)
    if documented:
        return {"number": SG, "lemma": surface, "article": documented,
                "source": "wiktionary", "confidence": "high"}

    # 3. Индекс форм: точный ответ «форма такого-то слова».
    entry = _load_form_index().get(low)
    if entry:
        lemma, number_tag, source = entry
        if number_tag == PL:
            return {"number": PL, "lemma": lemma or surface, "article": "die",
                    "source": source or "form_index", "confidence": "high"}
        article = _documented_singular(lemma, allow_network=False) if lemma else ""
        return {"number": SG, "lemma": lemma or surface, "article": article,
                "source": source or "form_index", "confidence": "high"}

    # 4. Догадка по окончанию: годится, чтобы промолчать, но не чтобы утверждать.
    for candidate in _singular_candidates(surface):
        if _documented_singular(candidate, allow_network=False):
            return {"number": PL, "lemma": candidate, "article": "",
                    "source": "правило окончания", "confidence": "low"}

    return {"number": UNKNOWN, "lemma": "", "article": "", "source": "нет данных",
            "confidence": "low"}


def reset_caches() -> None:
    """Сбросить внутренний кеш (тесты и ночной прогрев после записи в индекс)."""
    global _form_index, _index_loaded_at
    with _lock:
        _form_index, _index_loaded_at = {}, 0.0
