# -*- coding: utf-8 -*-
"""Дверь слова: починить и подписать источником ДО записи в базу.

ЗАЧЕМ. Владелец 19.08.2026: «Мы обязательно должны проверять ПЕРЕД сохранением… наша
задача — сохранить правильную версию слова: грамматически, лексически, с верной
заглавной буквой, с верной частью речи. Найти, откуда происходит некорректное
сохранение, проверить ВСЕ пути и свести их в единый механизм без костылей.»

Замер 19.08.2026 показал, ЧТО попадает в базу сегодня и откуда:

    Abschiebu, Scheinwerfergla, inkelgasse, -künfte   обрезки распознавания с экрана
    Sweatpants, lowkey, buzzkill, behest              английские слова
    городок                                           русское слово в немецком слоте
    Grundlegend, betäubung                            неверный регистр заголовка

Семь из них получили прописку в общем словаре не от человека, а от нашего же ночного
дообогащения: оно видело текст в чьей-то карточке и заводило его как слово.

ЧТО ДЕЛАЕТ ЭТА ДВЕРЬ. Она НЕ решает «пускать или нет». Она ЧИНИТ и ПОДПИСЫВАЕТ:

    подтверждено      справочник знает это слово
    исправлено        обрезка, умлаут, ß/ss, регистр, сторона языка — починили по источнику
    не подтверждено   слово настоящее, но справочник немецкого его не знает
                      (английское, разговорное, слишком редкое) — кладём с пометкой
    не слово          починить нечем; в словарь не заводим, спрашиваем человека

В общий словарь не попадает только последняя строка. Всё остальное попадает — потому что
общий словарь это КЕШ ДЛЯ ПОИСКА, а не список слов на изучение. Слова на изучение у
каждого свои: `bt_3_webapp_dictionary_queries` + состояние повторений. Проверено по коду
19.08.2026: очередь карточек читает только личные строки человека, общего пула в подборе
на тренировку нет вовсе. Поэтому «lowkey» в общем словаре никому не навредит — он лишь
сэкономит поход к модели, если это слово кто-нибудь спросит.

ПОЧЕМУ НЕ ОТКЛОНЯЕМ ПО ЯЗЫКУ. Владелец собирается достраивать английский. Дверь, которая
отклоняет «не немецкое», завтра пойдёт под снос. Поэтому она не судит язык, а помечает
источник подтверждения — а что делать с неподтверждённым, решает продукт.

ЦЕНА. Дорогие ступени (сеть, модель) идут ПОСЛЕДНИМИ и только для слов, которых не знают
наши собственные данные. Вердикт кешируется, поэтому одно и то же слово проверяется один
раз.
"""
from __future__ import annotations

import logging
import re
from typing import Any

# Статусы. Строки, а не флаги: они уходят в базу и в отчёты владельцу.
CONFIRMED = "подтверждено"
REPAIRED = "исправлено"
UNCONFIRMED = "не подтверждено"
NOT_A_WORD = "не слово"

_UMLAUT_PAIRS = (("a", "ä"), ("o", "ö"), ("u", "ü"), ("A", "Ä"), ("O", "Ö"), ("U", "Ü"))
_MAX_REPAIR_CANDIDATES = 8


def ensure_word_check_schema() -> None:
    """Кеш вердиктов. Одно слово проверяется один раз, а не на каждом сохранении."""
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bt_3_word_check (
                        asked       TEXT PRIMARY KEY,
                        text        TEXT NOT NULL,
                        status      TEXT NOT NULL,
                        pos         TEXT NOT NULL DEFAULT '',
                        source      TEXT NOT NULL DEFAULT '',
                        note        TEXT NOT NULL DEFAULT '',
                        checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
            conn.commit()
    except Exception:
        logging.warning("дверь слова: схема не создана", exc_info=True)


def _cached(asked: str) -> dict | None:
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT text, status, pos, source, note FROM bt_3_word_check "
                            "WHERE asked = %s;", (asked,))
                row = cur.fetchone()
    except Exception:
        logging.debug("дверь слова: кеш недоступен", exc_info=True)
        return None
    if not row:
        return None
    return {"text": row[0], "status": row[1], "pos": row[2], "source": row[3], "note": row[4]}


def _remember(asked: str, verdict: dict) -> None:
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bt_3_word_check (asked, text, status, pos, source, note, checked_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (asked) DO UPDATE
                       SET text=EXCLUDED.text, status=EXCLUDED.status, pos=EXCLUDED.pos,
                           source=EXCLUDED.source, note=EXCLUDED.note, checked_at=NOW();
                    """,
                    (asked, verdict.get("text", ""), verdict.get("status", ""),
                     verdict.get("pos", ""), verdict.get("source", ""), verdict.get("note", "")),
                )
            conn.commit()
    except Exception:
        logging.warning("дверь слова: вердикт не записан для %s", asked, exc_info=True)


# ── Кандидаты починки: только детерминированные, без выдумки ─────────────────
def repair_candidates(word: str) -> list[str]:
    """Написания, которые ИМЕЕТ СМЫСЛ спросить у справочника.

    Ни одно из них не становится ответом само по себе — каждое проверяется по источнику.
    Классы взяты из живых дефектов 19.08.2026, а не придуманы:
        умлаут потерян при вводе     Argernisse → Ärgernisse, Boder → Böder
        дореформенная орфография     Verläßlich → verlässlich, Zielbewußt → zielbewusst
        обрезка при распознавании    Sauerstoffk → Sauerstoff, Felg → Felge
        регистр                      betäubung → Betäubung, Grundlegend → grundlegend
    """
    text = str(word or "").strip()
    if not text:
        return []
    out: list[str] = []

    def add(value: str) -> None:
        if value and value != text and value not in out:
            out.append(value)

    add(text[:1].upper() + text[1:])
    add(text[:1].lower() + text[1:])
    if "ß" in text:
        add(text.replace("ß", "ss"))
    if "ss" in text:
        add(text.replace("ss", "ß"))
    for i, ch in enumerate(text):
        for plain, umlaut in _UMLAUT_PAIRS:
            if ch == plain:
                add(text[:i] + umlaut + text[i + 1:])
    # Обрезка: пробуем и снять последнюю букву, и дописать частые окончания.
    if len(text) > 3:
        add(text[:-1])
        for tail in ("e", "en", "ung", "er"):
            add(text + tail)
    return out[:_MAX_REPAIR_CANDIDATES]


# ── Дешёвая ступень: знают ли слово НАШИ собственные данные ─────────────────
def _known_by_our_data(word: str) -> tuple[bool, str, str]:
    """(знаем ли, чем именно, часть речи). Сеть и модель не трогаются.

    Часть речи возвращается обязательно, иначе регистр заголовка не починить: правило
    продукта работает только при явно названной части речи. Артикль в справочнике родов
    сам по себе означает существительное — этого достаточно.
    """
    try:
        from backend.article_authority import authoritative_article
        article, where = authoritative_article(word, allow_network=False)
        if article and str(where).startswith("wiktionary"):
            return True, "справочник родов", "noun"
    except Exception:
        logging.debug("дверь слова: справочник родов недоступен", exc_info=True)
    return False, "", ""


def _reference_says_about_all(words: list[str]) -> dict | None:
    """{написание: [части речи]} для слова И всех вариантов починки ОДНИМ запросом.

    По одному спрашивать нельзя: прогон 19.08.2026 упёрся в 429 на седьмом слове.
    Справочник отдаёт до 50 названий за раз — этого хватает на слово и восемь вариантов.
    None означает «справочник молчит», а не «слов нет».
    """
    try:
        from backend.german_reference_forms import fetch_sources_bulk
        sources = fetch_sources_bulk(words)
    except Exception:
        logging.debug("дверь слова: справочник недоступен", exc_info=True)
        return None
    if sources is None:
        return None
    out: dict[str, list[str]] = {}
    for name in words:
        text = sources.get(name) or ""
        if not text:
            continue
        # Старая орфография: у «verläßlich» страница ЕСТЬ, но она помечена как устаревшее
        # написание и указывает на современное. Брать её как ответ нельзя — человек
        # выучит форму, которой больше нет. Прогон 19.08.2026 поймал это на
        # «Verläßlich» и «Zielbewußt».
        old_spelling = re.findall(r"\{\{Alte Schreibweise\|([^|}]+)", text)
        if old_spelling:
            out[name] = ["__устаревшее__" + old_spelling[0].strip()]
            continue
        out[name] = re.findall(r"\{\{Wortart\|([^|}]+)", text)
    return out


_POS_BY_WORTART = {
    "Substantiv": "noun", "Verb": "verb", "Adjektiv": "adjective", "Adverb": "adverb",
    "Präposition": "preposition", "Konjunktion": "conjunction", "Pronomen": "pronoun",
}


def check_word(word: str, *, pos_hint: str = "", allow_network: bool = True,
               allow_model: bool = True) -> dict:
    """Вердикт двери. Никогда не бросает и никогда не возвращает пустой текст.

    Возвращает {'text', 'status', 'pos', 'source', 'note'}.
    """
    asked = str(word or "").strip()
    if not asked:
        return {"text": "", "status": NOT_A_WORD, "pos": "", "source": "",
                "note": "пустая строка"}

    remembered = _cached(asked)
    if remembered:
        return remembered

    verdict = _decide(asked, pos_hint=pos_hint, allow_network=allow_network,
                      allow_model=allow_model)
    _remember(asked, verdict)
    return verdict


def _decide(asked: str, *, pos_hint: str, allow_network: bool, allow_model: bool) -> dict:
    from backend.dictionary_intake import clean_text
    from backend.german_grammar_tables import german_dictionary_headword

    text = clean_text(asked) or asked
    # Правила заголовка, которые у нас уже есть: zu-инфинитив и неопределённый артикль.
    text = german_dictionary_headword(text) or text
    repaired = text != asked

    known, where, known_pos = _known_by_our_data(text)
    if known:
        return _finish(text, REPAIRED if repaired else CONFIRMED, known_pos or pos_hint,
                       where, asked)

    if not allow_network:
        return _finish(text, UNCONFIRMED, pos_hint, "не спрашивали справочник", asked)

    # ОДИН запрос: само слово и все варианты починки сразу.
    candidates = [text] + repair_candidates(text)
    answer = _reference_says_about_all(candidates)
    if answer is None:
        # Справочник не ответил — это НЕ «слова нет». Приговор не запоминаем.
        return {"text": text, "status": UNCONFIRMED, "pos": pos_hint,
                "source": "справочник молчал", "note": "спросим позже"}

    modern = ""
    for candidate in candidates:
        kinds = answer.get(candidate)
        if kinds is None:
            continue
        if kinds and str(kinds[0]).startswith("__устаревшее__"):
            modern = modern or str(kinds[0])[len("__устаревшее__"):]
            continue
        pos = next((_POS_BY_WORTART[k] for k in kinds if k in _POS_BY_WORTART), pos_hint)
        if candidate == text:
            return _finish(text, REPAIRED if repaired else CONFIRMED, pos, "справочник", asked)
        return _finish(candidate, REPAIRED, pos, "справочник (исправлено написание)", asked)

    if modern:
        # Справочник сам назвал современное написание — берём его, не выдумывая.
        return _finish(modern, REPAIRED, pos_hint, "справочник (устаревшее написание)", asked)

    if not allow_model:
        return _finish(text, UNCONFIRMED, pos_hint, "модель не спрашивали", asked)

    # Последняя ступень: существует ли слово вообще. Справочник неполон — «Arbeitsumfeld»,
    # «Beantragung» настоящие, но страницы у них нет. Отклонять их нельзя.
    try:
        from backend.german_reference_forms import word_exists_by_model
        said = word_exists_by_model(text)
    except Exception:
        logging.warning("дверь слова: модель недоступна для %s", text, exc_info=True)
        said = None
    if said is None:
        return {"text": text, "status": UNCONFIRMED, "pos": pos_hint,
                "source": "ответы разошлись", "note": "спросим позже"}
    if not said.get("existiert"):
        return _finish(text, NOT_A_WORD, pos_hint, "модель: такого слова нет", asked)
    fixed = str(said.get("korrekt") or text).strip() or text
    pos = _POS_BY_WORTART.get(str(said.get("wortart") or ""), pos_hint)
    lang = str(said.get("sprache") or "").strip().lower()
    # Слово настоящее, но не немецкое («Sweatpants», «lowkey»). Отклонять НЕЛЬЗЯ —
    # решение владельца 19.08.2026: сохраняем и спрашиваем человека позже. Тем более
    # что впереди английский, и дверь, отклоняющая по языку, пойдёт под снос.
    source = ("модель: слово есть, справочник не знает" if lang in ("", "de")
              else f"модель: слово есть, язык {lang}")
    status = REPAIRED if fixed != text else UNCONFIRMED
    return _finish(fixed, status, pos, source, asked)


def _finish(text: str, status: str, pos: str, source: str, asked: str) -> dict:
    """Последняя ступень для любого исхода — регистр по части речи."""
    from backend.german_grammar_tables import german_headword_case
    final = text
    if pos:
        try:
            fixed = german_headword_case(text, pos)
            if fixed:
                final = fixed
        except Exception:
            logging.debug("дверь слова: правило регистра недоступно", exc_info=True)
    if pos == "noun" and final[:1].islower():
        # Немецкое существительное пишется с заглавной. Правило продукта опускает
        # заглавную у не-существительных, но не поднимает её — доводим здесь.
        final = final[:1].upper() + final[1:]
    if status == CONFIRMED and final != asked:
        status = REPAIRED
    return {"text": final, "status": status, "pos": pos or "", "source": source,
            "note": f"спрошено «{asked}»" if final != asked else ""}
