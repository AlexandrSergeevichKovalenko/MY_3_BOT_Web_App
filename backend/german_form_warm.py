"""Ночной прогрев индекса форм: «эта поверхность — слово или его форма».

Зачем. Опознание (`german_surface`) без индекса вынуждено догадываться по окончанию,
а догадка ошибается в обе стороны: реальные леммы, которых нет в кэше родов, она
принимает за формы («der Stürmer», «das Abkommen», «der Laster» — все настоящие
слова). Догадкой чинить данные нельзя, поэтому индекс — обязательная ступень между
опознанием и уборкой.

Страница Wiktionary отвечает на вопрос однозначно и без догадок:
  • «Probleme» → {{Wortart|Deklinierte Form}} + «Nominativ Plural des Substantivs
    Problem» ⇒ форма множественного числа от «Problem»;
  • «Stürmer» → шапка {{Deutsch Substantiv Übersicht}} ⇒ это слово, а заодно оттуда
    бесплатно достаётся его множественное число;
  • «Eltern» → шапка есть, но единственного числа нет ⇒ живёт только во множественном.

Темп такой же, как у прогрева родов: маленькая ночная порция с паузой и отступлением
при 429 — за месяц это тысячи слов и ни одного упора в лимит. Разовый жадный прогон
на 1969 словах словил 429 на седьмой пачке, поэтому жадности здесь нет.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

from backend.german_surface import PL, SG, UNKNOWN, ensure_german_form_index_schema, upsert_form_index

WARM_JOB_KEY = "german_form_index_warm_nightly"

NIGHTLY_LIMIT = max(10, int((os.getenv("GERMAN_FORM_WARM_LIMIT") or "300").strip() or "300"))
BATCH = 45                     # предел MediaWiki — 50 заголовков за запрос, оставляем запас
PAUSE_SEC = float((os.getenv("GERMAN_FORM_WARM_PAUSE_SEC") or "1.5").strip() or "1.5")

_SURFACE_SOURCES = (
    """SELECT COALESCE(NULLIF(word_de, ''),
              CASE WHEN source_lang = 'de' THEN source_text ELSE target_text END)
       FROM bt_3_dictionary_entries WHERE source_lang = 'de' OR target_lang = 'de'""",
    """SELECT COALESCE(NULLIF(word_de, ''),
              CASE WHEN source_lang = 'de' THEN word_ru ELSE translation_de END)
       FROM bt_3_webapp_dictionary_queries WHERE source_lang = 'de' OR target_lang = 'de'""",
    "SELECT display FROM bt_3_lex_units WHERE lang = 'de' AND kind = 'word'",
    "SELECT word FROM bt_3_article_sprint_nouns WHERE NOT retired",
)


def _bare_noun(text: str) -> str:
    from backend.german_surface import _clean_surface  # noqa: PLC2701 — свой же модуль
    word = _clean_surface(text)
    return word if word[:1].isupper() else ""


def build_candidates(limit: int) -> list[str]:
    """Поверхности, о числе которых мы ещё не спрашивали.

    Не спрашиваем про то, чей род уже документирован: род есть ⇒ это лемма, вопрос
    закрыт. Не спрашиваем повторно про то, что уже лежит в индексе, — включая строки
    «страницы нет», иначе одна и та же дюжина слов съедала бы каждую ночь.
    """
    from backend.database import get_db_connection_context
    surfaces: dict[str, str] = {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            for sql in _SURFACE_SOURCES:
                try:
                    cursor.execute(sql)
                except Exception:
                    logging.debug("form warm: источник недоступен", exc_info=True)
                    conn.rollback()
                    continue
                for (raw,) in cursor.fetchall() or []:
                    word = _bare_noun(raw)
                    if word:
                        surfaces.setdefault(word.casefold(), word)
            cursor.execute("SELECT lower(title) FROM bt_3_wiktionary_genus_cache "
                           "WHERE COALESCE(genus, '') NOT IN ('', '-')")
            documented = {str(r[0]) for r in cursor.fetchall() or []}
            cursor.execute("SELECT surface_key FROM bt_3_german_form_index")
            known = {str(r[0]) for r in cursor.fetchall() or []}

    pending = [word for key, word in sorted(surfaces.items())
               if key not in documented and key not in known]
    # Сначала те, на кого правило окончания уже показывает пальцем: именно там ответ
    # справочника меняет поведение (либо подтверждает форму, либо снимает ложное
    # подозрение с настоящего слова). Остальные — следующей ночью. Алфавитный порядок
    # тратил бы ночную порцию на редкие композиты из начала алфавита.
    from backend.german_surface import PL, german_surface
    suspicious, rest = [], []
    for word in pending:
        (suspicious if german_surface(word)["number"] == PL else rest).append(word)
    return (suspicious + rest)[:max(1, int(limit))]


def _rows_from_facts(surface: str, facts: dict) -> list[dict]:
    """Разбор страницы → строки индекса. Пишем только однозначное."""
    rows: list[dict] = []
    number = str(facts.get("number") or "")
    lemma = str(facts.get("lemma") or "")
    if facts.get("is_lemma"):
        # Слово. Даже когда это всё, что мы узнали, строка ценна: она навсегда
        # выключает догадку по окончанию, из-за которой «Stürmer» считался формой.
        rows.append({"surface": surface, "lemma": surface,
                     "number_tag": PL if number == PL else SG, "source": "wiktionary"})
        plural = str(facts.get("plural_surface") or "")
        if plural and plural.casefold() != surface.casefold():
            rows.append({"surface": plural, "lemma": surface,
                         "number_tag": PL, "source": "wiktionary_übersicht"})
        return rows
    if number in (SG, PL) and lemma:
        rows.append({"surface": surface, "lemma": lemma,
                     "number_tag": number, "source": "wiktionary"})
    return rows


def run_warm(*, limit: int | None = None, words: list[str] | None = None) -> dict[str, Any]:
    """Спросить Wiktionary про очередную порцию поверхностей и наполнить индекс."""
    from backend.article_wiktionary_ref import form_facts_for_titles

    ensure_german_form_index_schema()
    started = time.time()
    titles = [w for w in (words or []) if str(w).strip()] or build_candidates(limit or NIGHTLY_LIMIT)
    stats = {"requested": 0, "lemmas": 0, "forms": 0, "no_page": 0,
             "batches_ok": 0, "batches_failed": 0, "status": "ok", "written": 0}
    if not titles:
        stats["status"] = "nothing_to_do"
        return stats

    for i in range(0, len(titles), BATCH):
        batch = titles[i:i + BATCH]
        stats["requested"] += len(batch)
        facts: dict = {}
        for attempt in range(3):
            try:
                facts = form_facts_for_titles(batch)
            except Exception:
                facts = {}
                logging.warning("form warm: пачка упала", exc_info=True)
            if facts:
                break
            # Пустой ответ = сеть или 429. Ждём дольше и пробуем ещё раз: лимит у
            # Wiktionary скользящий, пауза его отпускает. Три пустых подряд — значит
            # сегодня хватит, остаток спросим завтра.
            time.sleep(PAUSE_SEC * (attempt + 1) * 3)
        if not facts:
            stats["batches_failed"] += 1
            stats["status"] = "rate_limited"
            break
        stats["batches_ok"] += 1
        rows: list[dict] = []
        for surface in batch:
            page = facts.get(surface)
            if page is None:
                # Успешный запрос, страницы нет — помечаем, чтобы не спрашивать снова.
                rows.append({"surface": surface, "lemma": surface,
                             "number_tag": UNKNOWN, "source": "wiktionary_нет_страницы"})
                stats["no_page"] += 1
                continue
            new_rows = _rows_from_facts(surface, page)
            if not new_rows:
                rows.append({"surface": surface, "lemma": surface,
                             "number_tag": UNKNOWN, "source": "wiktionary_неоднозначно"})
                stats["no_page"] += 1
                continue
            stats["lemmas" if page.get("is_lemma") else "forms"] += 1
            rows.extend(new_rows)
        try:
            stats["written"] += upsert_form_index(rows)
        except Exception:
            stats["status"] = "partial"
            logging.warning("form warm: не смог записать индекс", exc_info=True)
        time.sleep(PAUSE_SEC)

    from backend.german_surface import reset_caches
    reset_caches()
    stats["duration_sec"] = round(time.time() - started, 2)
    logging.info("form warm: %s", stats)
    return stats


def seed_from_local_forms() -> int:
    """Бесплатная затравка без сети: множественное число, которое у нас УЖЕ записано.

    В bt_wiktionary_dictionary лежат разобранные статьи с forms_json.plural — это
    готовые пары «форма → слово», за которые не нужно платить ни одним запросом.
    """
    ensure_german_form_index_schema()
    from backend.database import get_db_connection_context
    rows: list[dict] = []
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT lemma, article, forms_json->>'plural'
                FROM bt_wiktionary_dictionary
                WHERE source_lang = 'de' AND pos = 'noun'
                  AND COALESCE(forms_json->>'plural', '') <> ''
                """
            )
            for lemma, _article, plural in cursor.fetchall() or []:
                lemma_clean = _bare_noun(lemma)
                plural_clean = _bare_noun(plural)
                if not lemma_clean or not plural_clean:
                    continue
                rows.append({"surface": lemma_clean, "lemma": lemma_clean,
                             "number_tag": SG, "source": "forms_json"})
                if plural_clean.casefold() != lemma_clean.casefold():
                    rows.append({"surface": plural_clean, "lemma": lemma_clean,
                                 "number_tag": PL, "source": "forms_json"})
    written = upsert_form_index(rows)
    from backend.german_surface import reset_caches
    reset_caches()
    return written
