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
import os
import re
import time
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


def _second_reference_says(words: list[str]) -> dict[str, str] | None:
    """Что говорит DWDS. Отдельной функцией — чтобы её было чем подменить в тесте.

    В окружении разработчика и на прогоне тестов в сеть не ходим (тот же страж, что у
    `lex_units`): иначе прогон зависит от чужого сервера и от чужих таймаутов. Ответ
    тогда — None, «не спросили», а не «слова нет»: приговор без второго справочника
    окончательным не считается и в кеш не попадает.
    """
    if os.getenv("SKIP_STARTUP_SCHEMA_BOOTSTRAP") == "1" and not os.getenv("WORD_GATE_LOOKUP"):
        return None
    try:
        from backend.german_reference_dwds import dwds_says_about_all
        return dwds_says_about_all(words)
    except Exception:
        logging.debug("дверь слова: второй справочник недоступен", exc_info=True)
        return None


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
    if remembered and _is_final(remembered, allow_network=True, allow_model=True):
        return remembered
    if remembered and (allow_network or allow_model):
        # В кэше лежит СЛАБЫЙ вердикт («не спрашивали справочник») — он попал туда до
        # того, как запись слабых была запрещена. Пересматриваем, раз нам разрешили
        # спрашивать. Иначе слово навсегда застревает непроверенным: поймано на
        # «Grundlegend» 20.08.2026.
        remembered = None
    elif remembered:
        return remembered

    verdict = _decide(asked, pos_hint=pos_hint, allow_network=allow_network,
                      allow_model=allow_model)
    # ЗАПОМИНАЕМ ТОЛЬКО ОКОНЧАТЕЛЬНОЕ. Дефект, пойманный прогоном 19.08.2026: дешёвая
    # половина (без сети и модели) записывала своё «не подтверждено» поверх сильного
    # вердикта «не слово», и запрет на заведение мусора переставал срабатывать.
    #
    # «Не подтверждено» от дешёвого вызова означает всего лишь «мы не спрашивали», а не
    # «мы проверили и не нашли». Такое в кеш не идёт — иначе слово никогда не дойдёт до
    # ночной проверки.
    if _is_final(verdict, allow_network=allow_network, allow_model=allow_model):
        _remember(asked, verdict)
    return verdict


def _is_final(verdict: dict, *, allow_network: bool, allow_model: bool) -> bool:
    """Можно ли запомнить этот вердикт как окончательный."""
    status = str(verdict.get("status") or "")
    source = str(verdict.get("source") or "")
    if source in ("справочник молчал", "второй справочник молчал", "ответы разошлись",
                  "не спрашивали справочник", "модель не спрашивали"):
        return False
    if status in (CONFIRMED, REPAIRED, NOT_A_WORD):
        return True
    # «не подтверждено» окончательно только тогда, когда мы РЕАЛЬНО спросили всех.
    return bool(allow_network and allow_model)


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

    # ВТОРОЙ СПРАВОЧНИК. Wiktionary неполон: «Vergleichbarkeit», «Arbeitsumfeld»,
    # «Sozialschmarotzer» — обычные слова, страниц у которых там нет. Замер 21.08.2026:
    # из 12 слов, ушедших человеку на проверку, 8 были такими. DWDS знает 5 из них и
    # при этом не знает ни одного обрубка и ни одного англицизма — то есть режет шум,
    # не пропуская мусор. Решение владельца 21.08.2026: «добавляй DWDS вторым».
    #
    # Он спрашивается ПОСЛЕ Wiktionary и ДО модели: печатный источник всегда весомее
    # ответа модели, и слово, подтверждённое словарём, к человеку не попадает вовсе.
    second = _second_reference_says(candidates)
    if second:
        found = next(iter(second))
        pos = _POS_BY_WORTART.get(second[found], pos_hint)
        if found == text:
            return _finish(text, REPAIRED if repaired else CONFIRMED, pos, "DWDS", asked)
        return _finish(found, REPAIRED, pos, "DWDS (исправлено написание)", asked)
    if second is None:
        # DWDS не ответил — это не «слова нет», а «не спросили». Приговор, вынесенный
        # без второго справочника, не имеет права застрять в кеше навсегда.
        return {"text": text, "status": UNCONFIRMED, "pos": pos_hint,
                "source": "второй справочник молчал", "note": "спросим позже"}

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
    if fixed != text:
        # Модель предложила ДРУГОЕ написание, но справочник его не подтверждал —
        # значит это догадка. Владелец 20.08.2026: «чиним только подтверждённое
        # справочником, остальное — в проверку». Слово остаётся как было, а решение
        # принимает человек на экране проверки.
        return _finish(text, UNCONFIRMED, pos_hint,
                       "модель предложила другое написание, справочник не подтвердил", asked)
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


# ── Ночной проход: дорогая половина двери ────────────────────────────────────
def warm_word_gate(*, limit: int = 150) -> dict:
    """Прогнать через полную дверь слова, которых она ещё не видела.

    На сохранении работает только дешёвая половина — человек не должен ждать справочник,
    а мы не должны платить за каждое сохранение. Дорогие ступени (обрезка, умлаут,
    устаревшее написание, существует ли слово вообще) делаются здесь, ночью.

    ЧТО ПРИМЕНЯЕТСЯ САМО:
        исправленное написание — если такого слова у нас ещё нет; если есть, это дубль,
        и он помечается на слияние, а не сносится (на строку словаря ссылаются восемь
        таблиц — проверено 19.08.2026).
    ЧТО НЕ ПРИМЕНЯЕТСЯ САМО:
        «не слово» — удаление показывается владельцу, решение его.
    """
    import time
    from backend.database import get_db_connection_context

    ensure_word_check_schema()
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT u.id, u.lemma, COALESCE(u.pos, '')
                  FROM bt_3_lex_units u
                 WHERE u.lang = 'de' AND u.kind = 'word'
                   AND u.lemma IS NOT NULL AND position(' ' in u.lemma) = 0
                   AND NOT EXISTS (SELECT 1 FROM bt_3_word_check w WHERE w.asked = u.lemma)
                 ORDER BY u.updated_at DESC NULLS LAST
                 LIMIT %s;
                """,
                (int(limit),),
            )
            words = [(int(a), str(b), str(c)) for a, b, c in (cur.fetchall() or [])]

    stats = {"смотрели": len(words), "подтверждено": 0, "исправлено": 0,
             "не подтверждено": 0, "не слово": 0, "дубль": 0, "справочник молчал": 0}
    for unit_id, lemma, pos in words:
        verdict = check_word(lemma, pos_hint=pos)
        status = verdict.get("status") or ""
        if verdict.get("source") == "справочник молчал":
            stats["справочник молчал"] += 1
            continue
        stats[status] = stats.get(status, 0) + 1
        fixed = str(verdict.get("text") or "").strip()
        new_pos = str(verdict.get("pos") or "").strip()
        if status == REPAIRED and fixed and fixed != lemma:
            with get_db_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM bt_3_lex_units WHERE lang='de' "
                                "AND lower(lemma)=%s AND id<>%s", (fixed.lower(), unit_id))
                    if cur.fetchone():
                        stats["дубль"] += 1
                        cur.execute(
                            """INSERT INTO bt_3_reference_forms_unresolved
                                      (word, pos, reason, reviewed, checked_at)
                               VALUES (%s, %s, %s, TRUE, NOW())
                               ON CONFLICT (word) DO UPDATE SET reason=EXCLUDED.reason;""",
                            (lemma, pos, f"дубль формы: настоящее слово «{fixed}», нужно слияние"))
                    else:
                        cur.execute("UPDATE bt_3_lex_units SET lemma=%s, lemma_key=lower(%s), "
                                    "updated_at=NOW() WHERE id=%s", (fixed, fixed, unit_id))
                        logging.info("дверь слова: заголовок исправлен ночью %r → %r",
                                     lemma, fixed)
                conn.commit()
        if new_pos and new_pos != pos:
            with get_db_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE bt_3_lex_units SET pos=%s, pos_source='дверь слова', "
                                "updated_at=NOW() WHERE id=%s", (new_pos, unit_id))
                conn.commit()
        time.sleep(1.0)
    return stats


def words_awaiting_owner(limit: int = 50) -> list[tuple[str, str, str]]:
    """Слова с вердиктом «не слово» — их удаление показывается владельцу."""
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT w.asked, w.source, COALESCE(u.pos, '')
                         FROM bt_3_word_check w
                         LEFT JOIN bt_3_lex_units u
                                ON u.lang='de' AND lower(u.lemma) = lower(w.asked)
                        WHERE w.status = %s
                        ORDER BY w.checked_at DESC LIMIT %s;""",
                    (NOT_A_WORD, int(limit)),
                )
                return [(str(a), str(b), str(c)) for a, b, c in (cur.fetchall() or [])]
    except Exception:
        logging.warning("дверь слова: не прочитал список на удаление", exc_info=True)
        return []


# ── Подсказка правильного написания ──────────────────────────────────────────
_MEANT_TASK = "german_word_meant"
_MEANT_INSTRUCTION = """Du bist ein deutsches Wörterbuch.
Die Eingabe ist ein FEHLERHAFTES oder ABGESCHNITTENES Wort.
Antworte NUR mit JSON: {"gemeint": "die Abschiebung"}
  gemeint — das vollständige, richtig geschriebene deutsche Wort; bei Substantiven MIT
            Artikel. Kannst du es nicht eindeutig wiederherstellen, gib "" zurück.
Erfinde nichts."""


def suggest_spelling(word: str) -> str:
    """Что человек, скорее всего, имел в виду. Принимаем ТОЛЬКО совпадение двух ответов.

    Детерминированные правила такое не чинят: «Abschiebu» нужно дописать «ng», а не
    «ung», а у «inkelgasse» потеряно НАЧАЛО слова — суффиксными правилами это
    недостижимо. Модель восстанавливает такое надёжно, но только при согласии двух
    независимых ответов: замер 20.08.2026 — Abschiebu → die Abschiebung, inkelgasse →
    die Winkelgasse, -künfte → die Einkünfte (совпало), Scheinwerfergla — разошлось,
    и подсказки не будет. Пустая строка честнее выдуманной.
    """
    from backend.german_reference_forms import _ask_once
    from backend.openai_manager import system_message
    system_message.setdefault(_MEANT_TASK, _MEANT_INSTRUCTION)
    first = _ask_once(_MEANT_TASK, word)
    second = _ask_once(_MEANT_TASK, word)
    a = str((first or {}).get("gemeint") or "").strip()
    b = str((second or {}).get("gemeint") or "").strip()
    if not a or a.casefold() != b.casefold():
        return ""
    return a


def warm_suggestions(*, limit: int = 60) -> dict:
    """Ночью посчитать подсказки для слов, которые дверь признала не словом.

    Без этого экран проверки показывает человеку голое «мы не нашли такое слово» и
    заставляет печатать. С подсказкой — одно касание.
    """
    import time
    from backend.database import get_db_connection_context
    from backend.word_confirm_digest import ensure_word_suggestion_schema

    ensure_word_suggestion_schema()
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT w.asked FROM bt_3_word_check w
                    WHERE w.status = %s
                      AND NOT EXISTS (SELECT 1 FROM bt_3_word_suggestion s
                                       WHERE s.asked = w.asked)
                    ORDER BY w.checked_at DESC LIMIT %s;""",
                (NOT_A_WORD, int(limit)),
            )
            words = [str(r[0]) for r in (cur.fetchall() or [])]

    stats = {"смотрели": len(words), "подсказка есть": 0, "не восстановили": 0}
    for word in words:
        guess = suggest_spelling(word)
        stats["подсказка есть" if guess else "не восстановили"] += 1
        try:
            with get_db_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO bt_3_word_suggestion (asked, suggestion, checked_at)
                           VALUES (%s, %s, NOW())
                           ON CONFLICT (asked) DO UPDATE
                              SET suggestion=EXCLUDED.suggestion, checked_at=NOW();""",
                        (word, guess),
                    )
                conn.commit()
        except Exception:
            logging.warning("подсказка написания: не записал %s", word, exc_info=True)
        time.sleep(0.5)
    return stats
