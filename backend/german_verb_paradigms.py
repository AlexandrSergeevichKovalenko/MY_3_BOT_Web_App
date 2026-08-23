# -*- coding: utf-8 -*-
"""Спряжение глагола — ДОСЛОВНО из таблицы de.wiktionary. Своих правил здесь нет.

ЗАЧЕМ ЭТОТ МОДУЛЬ ПОЯВИЛСЯ
──────────────────────────
Владелец 17.08.2026, открыв карточку «klarkommen»: «мы не пользуемся грамматическими
словарями, базами данных, реальными данными, а ты выдумываешь какие-то правила сам».
Про спряжение это было верно. Род и число в проекте давно берутся из справочника
(`bt_3_wiktionary_genus_cache`, `bt_3_german_form_index`), а таблица спряжения строилась
иначе: регулярные окончания приклеивал код, а неправильные части присылала МОДЕЛЬ в поле
`seed`. Отсюда «ich klarzukomme», «kam klarst», «ich ankomme» — форм, которых нет в языке.

Источник есть, и он покрывает нас. У de.wiktionary для каждого глагола есть страница
`Flexion:<глагол>` с ПОЛНОЙ напечатанной таблицей. Замер 17.08.2026 на случайной выборке
100 глаголов справочника: страница есть у 95.

ПОЧЕМУ ЧИТАЕМ ГОТОВУЮ ТАБЛИЦУ, А НЕ ШАБЛОН
──────────────────────────────────────────
Первая версия этого модуля разбирала шаблон `{{Deutsch Verb unregelmäßig|…}}` и получала
основы: `2=halt`, `6=hält`, `7=e`. Основы документированы, но окончания 2-3 лица из них
НЕ выводятся: у сильного глагола «du hältst» (а не «hältest»), у слабого «du arbeitest»
(а не «arbeitst»), у «lesen» — «du liest». Параметры 7/8/9 это кодируют, но их разбор —
снова мои правила поверх источника. Прогон это и показал: «du hältest», «er arbeit».

Поэтому берём формы там, где они УЖЕ НАПЕЧАТАНЫ. На странице Flexion таблица устроена
одинаково для всех глаголов:

    Präsens │ Person             │ Indikativ      │ Konjunktiv I │ …пассивы…
            │ 1. Person Singular │ ich halte      │ ich halte    │
            │ 2. Person Singular │ du hältst      │ du haltest   │
            │ 3. Person Singular │ er/sie/es hält │ …            │

Мы читаем столбец Indikativ и кладём форму как есть. Ни одного окончания код не
дописывает.

ОТДЕЛЯЕМАЯ ПРИСТАВКА ТОЖЕ ПРИХОДИТ ИЗ ИСТОЧНИКА
───────────────────────────────────────────────
В таблице «klarkommen» напечатано «ich komme klar» — приставка стоит там, где ей место в
главном предложении. Списка приставок и правил отделения здесь нет вовсе.

⚠ В той же таблице есть и «ich klarkomme» — это конъюгация ПРИДАТОЧНОГО предложения
(«…, dass ich klarkomme»). Она документирована и верна, но принадлежит другому столбцу,
и в таблице главного предложения ей не место. Именно эту подмену владелец и увидел.

⚠ НЕТ СТРАНИЦЫ — НЕТ ТАБЛИЦЫ. Спряжение, не подтверждённое справочником, не
показывается. То же правило, что для артикля: «не знаем — не печатаем».

⚠ ТЕМП. Массовый залп запрещён: жадный прогон по внешнему API уже приносил HTTP 429 и
2737 ложных пометок «страницы нет». Ночная порция с паузой, при первом молчании — стоп
до следующей ночи. На выдаче спрашиваем про ОДНО слово и только если его нет в кэше.
Молчание справочника НЕ записывается как «нет страницы»: авария — не данные.
"""
from __future__ import annotations

import html as _html
import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

API_URL = "https://de.wiktionary.org/w/api.php"
USER_AGENT = "DerSchlaufuchs/1.0 (German learning app)"
FETCH_TIMEOUT_SEC = float((os.getenv("VERB_PARADIGM_TIMEOUT_SEC") or "8").strip() or "8")

_PERSON_LABELS = {
    "1. Person Singular": "ich",
    "2. Person Singular": "du",
    "3. Person Singular": "er/sie/es",
    "1. Person Plural": "wir",
    "2. Person Plural": "ihr",
    "3. Person Plural": "sie/Sie",
}
_PRONOUNS = ("ich", "du", "er/sie/es", "wir", "ihr", "sie/Sie", "sie", "Sie")


def ensure_german_verb_paradigm_schema() -> None:
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bt_3_german_verb_paradigms (
                        verb        TEXT PRIMARY KEY,
                        tables      JSONB,
                        documented  BOOLEAN NOT NULL DEFAULT TRUE,
                        checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
            conn.commit()
    except Exception:
        logging.warning("парадигмы глаголов: схема не создана", exc_info=True)


def _api(params: dict) -> dict | None:
    """Ответ справочника. None — справочник МОЛЧИТ (сеть, 429), а не «данных нет»."""
    url = API_URL + "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=FETCH_TIMEOUT_SEC) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        logging.warning("парадигмы глаголов: HTTP %s от справочника", exc.code)
        return None
    except Exception:
        logging.warning("парадигмы глаголов: справочник не ответил", exc_info=True)
        return None


def _table_cells(rendered_html: str) -> list[str]:
    text = _html.unescape(re.sub(r"<[^>]+>", "\t", str(rendered_html or "")))
    cells = [re.sub(r"\s+", " ", chunk).strip() for chunk in text.split("\t")]
    return [c for c in cells if c]


def _strip_pronoun(form: str) -> str:
    """«ich halte» → «halte»: в таблице форма напечатана вместе с местоимением."""
    text = str(form or "").strip().rstrip(",").strip()
    for pronoun in _PRONOUNS:
        if text.lower().startswith(pronoun.lower() + " "):
            return text[len(pronoun) + 1:].strip()
    return text


def _column_forms(cells: list[str], start: int, *, column: int) -> dict[str, str]:
    """Формы одного столбца для шести лиц, начиная от заголовка блока.

    Две особенности разметки, обе поймал прогон:
      • у третьего лица местоимение вынесено отдельной ячейкой
        («3. Person Singular» | «er/sie/es» | «hält»);
      • запятая на конце ячейки значит, что следующая — ВТОРОЙ вариант той же клетки
        («du hieltest,» | «du hieltst»), а не соседний столбец. Без этого конъюнктив
        съезжал на форму из другого столбца."""
    out: dict[str, str] = {}
    index, limit = start, min(len(cells), start + 240)
    while index < limit and len(out) < 6:
        person = _PERSON_LABELS.get(cells[index])
        if not person:
            index += 1
            continue
        # Клетка может содержать НЕСКОЛЬКО задокументированных вариантов, разделённых
        # запятой: «ich dämm ein, ich dämme ein», «du hieltest, du hieltst». Собираем
        # их вместе и берём ПОЛНУЮ форму — усечённая («dämm», «säuber») это разговорное
        # выпадение -e, и словарной статье она не годится. Оба варианта из источника,
        # выбор между ними — по длине, а не по порядку: порядок у Wiktionary разный.
        group: list[list[str]] = []
        cursor, pending_variant = index + 1, False
        while cursor < limit and cells[cursor] not in _PERSON_LABELS:
            value = cells[cursor]
            if value in _PRONOUNS:
                cursor += 1
                continue
            if pending_variant and group:
                group[-1].append(value)
            else:
                group.append([value])
            pending_variant = value.endswith(",")
            cursor += 1
        if len(group) > column and person not in out:
            variants = [_strip_pronoun(v).rstrip("!,") for v in group[column]]
            variants = [v for v in variants if v]
            if variants:
                out[person] = max(variants, key=len)
        index = cursor
    return out if len(out) == 6 else {}


def documented_tables(rendered_html: str) -> dict[str, Any]:
    """Präsens / Präteritum / Konjunktiv II / Perfekt / Imperativ — как напечатано."""
    cells = _table_cells(rendered_html)
    tables: dict[str, Any] = {}

    def at(header: str) -> int:
        """Заголовок блока ЛИЧНЫХ ФОРМ, а не любое совпадение слова.

        У возвратных глаголов страница начинается таблицей инфинитивов, и слово
        «Präsens» встречается в ней раньше: «sich schämen | sich zu schämen». Взяв
        первое вхождение, разбор не находил лиц и возвращал пустоту — «schämen»,
        «sehnen», «nähern», «wehren» остались без таблицы, хотя страницы у них есть.
        Поэтому берём то вхождение, за которым в ближайших ячейках ИДУТ метки лиц."""
        for i, cell in enumerate(cells):
            if cell != header:
                continue
            window = cells[i + 1:i + 40]
            if any(w in _PERSON_LABELS for w in window):
                return i
        return -1

    for header, key, column in (
        ("Präsens", "praesens", 0),
        ("Präteritum", "praeteritum", 0),
        ("Präteritum", "konjunktiv2", 1),
        ("Perfekt", "perfekt", 0),
    ):
        start = at(header)
        if start < 0:
            continue
        forms = _column_forms(cells, start, column=column)
        if forms:
            tables[key] = forms

    imperative_at = at("Imperative")
    if imperative_at >= 0:
        # Берём ПЕРВОЕ вхождение каждой строки: дальше по странице те же метки лиц
        # встречаются в блоке презенса, и без остановки повелительное перезаписывалось
        # формой «du hältst» вместо «halt».
        imperativ: dict[str, str] = {}
        index = imperative_at
        while index + 1 < len(cells) and index < imperative_at + 60 and len(imperativ) < 2:
            if cells[index] == "2. Person Singular" and "du" not in imperativ:
                imperativ["du"] = _strip_pronoun(cells[index + 1]).rstrip("!,")
            elif cells[index] == "2. Person Plural" and "ihr" not in imperativ:
                imperativ["ihr"] = _strip_pronoun(cells[index + 1]).rstrip("!,")
            index += 1
        if imperativ:
            tables["imperativ"] = imperativ

    if tables.get("perfekt"):
        first = str((tables["perfekt"] or {}).get("ich") or "")
        parts = first.split()
        if len(parts) >= 2:
            tables["auxiliary"] = "sein" if parts[0].lower() in ("bin", "ist", "sind") else "haben"
            tables["partizip2"] = parts[-1]
    return tables


def fetch_documented_tables(verb: str) -> dict[str, Any] | None:
    """Скачать напечатанную таблицу. {} — страницы нет. None — справочник молчит."""
    name = str(verb or "").strip()
    if not name:
        return {}
    payload = _api({"action": "parse", "page": "Flexion:" + name,
                    "prop": "text", "format": "json", "formatversion": "2"})
    if payload is None:
        return None
    if payload.get("error"):
        return {}
    rendered = (payload.get("parse") or {}).get("text") or ""
    return documented_tables(rendered)


def store_paradigm(verb: str, tables: dict | None) -> None:
    """Записать результат. tables={} значит «страницы нет», None не пишем вовсе."""
    from backend.database import get_db_connection_context
    key = str(verb or "").strip().lower()
    if not key or tables is None:
        return
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bt_3_german_verb_paradigms (verb, tables, documented, checked_at)
                    VALUES (%s, %s::jsonb, %s, NOW())
                    ON CONFLICT (verb) DO UPDATE
                       SET tables = EXCLUDED.tables,
                           documented = EXCLUDED.documented,
                           checked_at = NOW();
                    """,
                    (key, json.dumps(tables, ensure_ascii=False), bool(tables.get("praesens"))),
                )
            conn.commit()
    except Exception:
        logging.warning("парадигмы глаголов: не записал %s", key, exc_info=True)


def load_paradigm(verb: str) -> dict | None:
    """Из кэша. None — не спрашивали. {} — спрашивали, страницы нет."""
    from backend.database import get_db_connection_context
    key = str(verb or "").strip().lower()
    if not key:
        return None
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT tables, documented FROM bt_3_german_verb_paradigms WHERE verb = %s;",
                    (key,),
                )
                row = cur.fetchone()
    except Exception:
        logging.debug("парадигмы глаголов: чтение кэша не удалось", exc_info=True)
        return None
    if not row:
        return None
    tables, documented = row
    if not documented or not isinstance(tables, dict):
        return {}
    return tables


# Разговорные усечения приставок. Это НЕ догадка: «rauf-», «rein-», «runter-», «raus-»,
# «ran-», «rum-» — стяжения от «herauf-», «herein-», «herunter-», «heraus-», «heran-»,
# «herum-», и словарь описывает их именно так. Решение показывать полную форму принял
# владелец 18.08.2026: «ну да, показывай».
_COLLOQUIAL_PREFIX = {
    "rauf": "herauf", "rein": "herein", "runter": "herunter", "raus": "heraus",
    "ran": "heran", "rum": "herum", "rüber": "herüber", "raus": "heraus",
}


def _full_form_of_colloquial(verb: str) -> str:
    """«rangehen» → «herangehen». Пустая строка, если это не усечение."""
    low = str(verb or "").strip().casefold()
    for short, full in _COLLOQUIAL_PREFIX.items():
        if low.startswith(short) and len(low) > len(short) + 3:
            return full + verb[len(short):]
    return ""


def _paradigm_from_base_verb(verb: str, *, allow_network: bool) -> dict | None:
    """Таблица составного глагола из таблицы его ОСНОВЫ.

    У части глаголов своей страницы в справочнике нет: «abschnallen», «einleben»,
    «ausstecken». Но составной глагол спрягается РОВНО как его основа, а приставка в
    личной форме уходит в конец — «ich schnalle ab» от «ich schnalle». Основа при этом
    документирована: замер 18.08.2026 подтвердил schnallen, leben, stecken, sehen.

    Это не догадка и не модель: обе части взяты из источника — формы у основы, а
    отделяемость у самого написания составного глагола."""
    from backend.german_grammar_tables import split_separable_verb
    prefix, base = split_separable_verb(verb)
    if not prefix or base.casefold() == verb.casefold():
        return None
    tables = load_paradigm(base)
    if tables is None and allow_network:
        fetched = fetch_documented_tables(base)
        store_paradigm(base, fetched)
        tables = fetched
    if not tables or not tables.get("praesens"):
        return None

    def attach(form: str) -> str:
        text = str(form or "").strip()
        return f"{text} {prefix}".strip() if text and text != "—" else text

    built: dict[str, Any] = {}
    for key in ("praesens", "praeteritum", "konjunktiv2", "imperativ"):
        block = tables.get(key)
        if isinstance(block, dict):
            built[key] = {person: attach(form) for person, form in block.items()}
    # В Perfekt приставка НЕ отделяется: «habe abgeschnallt». Причастие основы
    # («geschnallt») получает приставку впереди — так устроено причастие составного.
    perfekt = tables.get("perfekt")
    participle = str(tables.get("partizip2") or "").strip()
    if isinstance(perfekt, dict) and participle:
        joined = prefix + participle
        built["perfekt"] = {p: str(f).replace(participle, joined) for p, f in perfekt.items()}
        built["partizip2"] = joined
        built["auxiliary"] = tables.get("auxiliary")
    return built or None


# ── Ступень «спросить модель», когда справочника нет ─────────────────────────
#
# Владелец 23.08.2026, дословно: «как мы можем просто брать и механически что-то делать,
# когда это касается языка? у нас же есть либо справочник, либо, если справочника нет,
# нужно запрашивать у модели. а как мы строим это механически — что это за бред?»
#
# До этого дня у глаголов ступени с моделью НЕ БЫЛО ВООБЩЕ: каскад обрывался на
# справочнике, а дальше `german_grammar_tables.build_verb_conjugation` досчитывал формы
# нашей арифметикой — резал основу и приклеивал окончания. На настоящих немецких
# глаголах это чаще всего совпадало, а на всём остальном давало несуществующие слова:
# «ich aspettiamoe» (итальянское слово), «ich boree» (английское), «ich besagte» (не
# инфинитив, а форма). Замер 22.08.2026 — 96 таких записей.
#
# Существительные и прилагательные этот путь прошли 17.08.2026: справочник → композит →
# модель с двойным подтверждением → честное «не знаю». Здесь ровно он же, без изобретений.
#
# Ответ модели лежит в той же таблице, но под своим ключом: строка самого глагола
# принадлежит справочнику (там `{}` значит «страницы в Wiktionary нет»), и подменять её
# ответом модели нельзя — иначе завтрашний поход в справочник её уже не перезапишет.
_MODEL_KEY_PREFIX = "модель:"

_PARADIGM_TASK = "german_verb_paradigm_reference"

_PARADIGM_INSTRUCTION = """Du bist ein deutsches Flexionswörterbuch.
Gib für das genannte Verb die Konjugation als JSON zurück, sonst nichts.
Format exakt:
{"praesens":{"ich":"gehe","du":"gehst","er/sie/es":"geht","wir":"gehen","ihr":"geht","sie/Sie":"gehen"},
 "praeteritum":{"ich":"ging","du":"gingst","er/sie/es":"ging","wir":"gingen","ihr":"gingt","sie/Sie":"gingen"},
 "konjunktiv2":{"ich":"ginge","du":"gingest","er/sie/es":"ginge","wir":"gingen","ihr":"ginget","sie/Sie":"gingen"},
 "imperativ":{"du":"geh","ihr":"geht"},
 "partizip2":"gegangen",
 "auxiliary":"sein"}
Regeln:
- "auxiliary" ist genau "haben" oder "sein".
- Trennbare Verben: die Vorsilbe steht am Ende der finiten Form ("ich komme an"), im
  Partizip II in der Mitte ("angekommen").
- Ist das genannte Wort KEIN Verb im Infinitiv (Partizip, flektierte Form, Fremdsprache,
  Unsinn), gib exakt {"not_a_verb": true} zurück.
- Erfinde nichts."""

_PARADIGM_CELLS = ("ich", "du", "er/sie/es", "wir", "ihr", "sie/Sie")


def _ask_paradigm_once(verb: str) -> dict | None:
    """Один спрос модели о спряжении.

    `None` — МОДЕЛЬ НЕ ОТВЕТИЛА: сеть, таймаут, нечитаемый ответ. Это не «не знаю»,
    а «мы не спросили», и путать их нельзя: 22.08.2026 прогон шёл на рвущейся сети,
    и настоящие глаголы («wehren», «entpuppen») были записаны как неподтверждённые
    навсегда — то есть ошибка притворилась ответом.
    `{}` — ответила, но пусто. Словарь — разобранный ответ.
    """
    import asyncio
    from backend.openai_manager import llm_execute, parse_llm_json_object, system_message
    system_message.setdefault(_PARADIGM_TASK, _PARADIGM_INSTRUCTION)
    try:
        text = asyncio.run(llm_execute(
            task_name=_PARADIGM_TASK, system_instruction_key=_PARADIGM_TASK,
            user_message=str(verb or "").strip(), poll_interval_seconds=1.0,
        ))
    except RuntimeError:
        logging.warning("спряжение: модель вызвана из асинхронного контекста")
        return None
    except Exception:
        logging.warning("спряжение: модель не ответила про %s", verb, exc_info=True)
        return None
    parsed = parse_llm_json_object(text, context=_PARADIGM_TASK)
    return parsed if isinstance(parsed, dict) else None


def _paradigms_agree(first: dict, second: dict) -> dict:
    """Ответ принимается ТОЛЬКО когда оба спроса дали одно и то же, ячейка в ячейку.

    Сверять со справочником нечего — слова там нет, иначе мы бы сюда не дошли. Поэтому
    подтверждение = согласие двух независимых ответов. Разошлись хоть в одной клетке —
    молчим, и глагол уходит в отчёт владельцу, а не подставляется наугад.
    """
    if not first or not second:
        return {}
    if first.get("not_a_verb") or second.get("not_a_verb"):
        # Хотя бы один спрос говорит «это не глагол» — таблицы не будет. Это ответ,
        # а не отказ: «besagt», «aspettiamo», «bore» спрягать нельзя.
        return {}

    def norm(value) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    out: dict = {}
    for block in ("praesens", "praeteritum", "konjunktiv2", "imperativ"):
        a, b = first.get(block), second.get(block)
        if not isinstance(a, dict) or not isinstance(b, dict):
            if a or b:
                return {}
            continue
        cells = _PARADIGM_CELLS if block != "imperativ" else ("du", "ihr")
        merged: dict[str, str] = {}
        for cell in cells:
            left, right = norm(a.get(cell)), norm(b.get(cell))
            if left.lower() != right.lower():
                return {}
            if left:
                merged[cell] = left
        if merged:
            out[block] = merged
    for flat in ("partizip2", "auxiliary"):
        left, right = norm(first.get(flat)), norm(second.get(flat))
        if left.lower() != right.lower():
            return {}
        if left:
            out[flat] = left
    if out.get("auxiliary") not in ("haben", "sein"):
        # Вспомогательный глагол бывает только один из двух. Всё прочее — не ответ.
        return {}
    return out if out.get("praesens") else {}


# Почему спряжения нет. Хранится вместе с записью и уходит владельцу словами: без
# причины «таблицы нет» одинаково выглядит и у итальянского слова, и у нашего обрыва сети.
NOT_A_VERB = "не глагол"
DISAGREED = "ответы разошлись"
NO_ANSWER = "модель не ответила"


def paradigm_from_model(verb: str) -> tuple[dict | None, str]:
    """Спряжение от модели при совпадении двух независимых ответов.

    Возвращает (таблица, причина). Таблица есть — причина пустая. Таблицы нет —
    причина называется словом, и от неё зависит, спросим ли мы ещё раз: `NO_ANSWER`
    означает, что вопрос ОСТАЛСЯ, а не получил отрицательный ответ.
    """
    first = _ask_paradigm_once(verb)
    if first is None:
        return None, NO_ANSWER
    second = _ask_paradigm_once(verb)
    if second is None:
        return None, NO_ANSWER
    if first.get("not_a_verb") and second.get("not_a_verb"):
        return None, NOT_A_VERB
    agreed = _paradigms_agree(first, second)
    if agreed:
        return agreed, ""
    if first.get("not_a_verb") or second.get("not_a_verb"):
        # Один спрос сказал «не глагол», другой всё-таки проспрягал. Согласия нет —
        # таблицы не будет, но и ярлык «не глагол» вешать не на чем.
        return None, DISAGREED
    return None, DISAGREED


def paradigm_for_verb(infinitive: str, *, allow_network: bool = False,
                      allow_model: bool = False) -> dict | None:
    """Документированная таблица спряжения или None, если справочник её не подтвердил.

    Три пути, все опираются на источник:
      1. своя страница Flexion;
      2. полная форма разговорного усечения («rangehen» → «herangehen»);
      3. таблица ОСНОВЫ составного глагола плюс отделяемая приставка.
    Ни один не выдумывает форм: код нигде не дописывает окончаний."""
    verb = str(infinitive or "").strip()
    if not verb or " " in verb:
        return None
    tables = load_paradigm(verb)
    if tables is None and allow_network:
        fetched = fetch_documented_tables(verb)
        store_paradigm(verb, fetched)
        tables = fetched if fetched is not None else None
    if tables and tables.get("praesens"):
        return {**tables, "infinitive": verb, "source": "wiktionary-flexion"}

    full = _full_form_of_colloquial(verb)
    if full:
        from_full = load_paradigm(full)
        if from_full is None and allow_network:
            fetched = fetch_documented_tables(full)
            store_paradigm(full, fetched)
            from_full = fetched
        if from_full and from_full.get("praesens"):
            return {**from_full, "infinitive": verb, "full_form": full,
                    "source": "wiktionary-flexion:полная форма"}
        # У полной формы своей страницы тоже нет — но её ОСНОВА документирована:
        # «rausbringen» → «herausbringen» → «heraus» + «bringen» → «ich bringe heraus».
        # Цепочка идёт до конца, и каждое звено остаётся справочником.
        from_full_base = _paradigm_from_base_verb(full, allow_network=allow_network)
        if from_full_base:
            return {**from_full_base, "infinitive": verb, "full_form": full,
                    "source": "wiktionary-flexion:полная форма, основа"}

    from_base = _paradigm_from_base_verb(verb, allow_network=allow_network)
    if from_base:
        return {**from_base, "infinitive": verb, "source": "wiktionary-flexion:основа"}

    # ── Справочник молчит. Раньше здесь каскад кончался, и таблицу досчитывала наша
    # арифметика в german_grammar_tables. Теперь спрашиваем модель — но НЕ на глазах у
    # человека: два спроса это секунды и деньги, поэтому на выдаче читаем только уже
    # подтверждённое, а спрашивает ночь (warm_verb_paradigms). Ровно так же устроены
    # существительные и прилагательные с 17.08.2026.
    remembered = load_paradigm(_MODEL_KEY_PREFIX + verb.lower())
    if remembered and remembered.get("praesens"):
        return {**remembered, "infinitive": verb, "source": "модель"}
    if remembered is not None:
        # Уже спрашивали, и это был ОТВЕТ: «не глагол» или «ответы разошлись». Второй
        # раз за то же самое не платим. Обрыв связи сюда не попадает — он не пишется.
        return None
    if allow_model:
        answer, reason = paradigm_from_model(verb)
        if reason != NO_ANSWER:
            store_paradigm(_MODEL_KEY_PREFIX + verb.lower(), answer or {"reason": reason})
        if answer:
            return {**answer, "infinitive": verb, "source": "модель"}
    return None


def warm_verb_paradigms(*, limit: int = 200, pause_sec: float = 1.5) -> dict:
    """Ночной прогрев: спросить справочник про глаголы, о которых ещё не спрашивали.

    Порция маленькая и с паузой. При первом молчании справочника проход прекращается —
    иначе упор в лимит превратился бы в сотни ложных «страницы нет»."""
    from backend.database import get_db_connection_context
    ensure_german_verb_paradigm_schema()
    report = {"asked": 0, "documented": 0, "no_page": 0, "stopped_early": False}
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT lower(u.display) FROM bt_3_lex_units u
                     WHERE u.lang = 'de' AND u.kind = 'word'
                       AND (u.pos = 'verb' OR u.card->>'part_of_speech' = 'verb')
                       AND u.display ~ '^[a-zäöüßA-ZÄÖÜ]+$'
                       AND NOT EXISTS (SELECT 1 FROM bt_3_german_verb_paradigms p
                                        WHERE p.verb = lower(u.display))
                     ORDER BY 1 LIMIT %s;
                    """,
                    (int(limit),),
                )
                verbs = [r[0] for r in cur.fetchall()]
    except Exception:
        logging.warning("парадигмы глаголов: не выбрал кандидатов", exc_info=True)
        return report

    for verb in verbs:
        tables = fetch_documented_tables(verb)
        if tables is None:
            report["stopped_early"] = True
            break
        store_paradigm(verb, tables)
        report["asked"] += 1
        if tables.get("praesens"):
            report["documented"] += 1
        else:
            report["no_page"] += 1
        time.sleep(pause_sec)
    report.update(warm_verb_paradigms_from_model(limit=limit))
    return report


def warm_verb_paradigms_from_model(*, limit: int = 60) -> dict:
    """Ночной добор моделью: глаголы, которых нет в справочнике.

    Спрашивать в момент показа нельзя — это два обращения к модели прямо на глазах у
    человека. Поэтому днём выдача читает уже подтверждённое, а спрашивает ночь.
    Не совпали два ответа — записываем `{}` («спрашивали, не подтвердилось») и уходим
    в отчёт владельцу; выдумывать таблицу вместо этого запрещено.
    """
    from backend.database import get_db_connection_context
    ensure_german_verb_paradigm_schema()
    report = {"model_asked": 0, "model_confirmed": 0, "model_unclear": 0,
              "model_no_answer": 0}
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT lower(u.display) FROM bt_3_lex_units u
                     JOIN bt_3_german_verb_paradigms p ON p.verb = lower(u.display)
                     WHERE u.lang = 'de' AND u.kind = 'word'
                       AND (u.pos = 'verb' OR u.card->>'part_of_speech' = 'verb')
                       AND p.documented IS FALSE
                       AND NOT EXISTS (SELECT 1 FROM bt_3_german_verb_paradigms m
                                        WHERE m.verb = %s || lower(u.display))
                     ORDER BY 1 LIMIT %s;
                    """,
                    (_MODEL_KEY_PREFIX, int(limit)),
                )
                verbs = [r[0] for r in cur.fetchall()]
    except Exception:
        logging.warning("парадигмы глаголов: не выбрал кандидатов для модели", exc_info=True)
        return report

    for verb in verbs:
        # Полная форма и основа проверяются раньше: если «rausbringen» закрывается
        # справочником через «herausbringen», платить модели незачем.
        if paradigm_for_verb(verb, allow_model=False):
            continue
        answer, reason = paradigm_from_model(verb)
        report["model_asked"] += 1
        if reason == NO_ANSWER:
            # Не записываем НИЧЕГО: вопрос остался, и завтрашняя ночь спросит снова.
            report["model_no_answer"] += 1
            continue
        store_paradigm(_MODEL_KEY_PREFIX + verb, answer or {"reason": reason})
        report["model_confirmed" if answer else "model_unclear"] += 1
    return report


def _printed_words(tables: dict) -> set[str]:
    """Все слова, КАК ОНИ НАПЕЧАТАНЫ в таблице: и целые формы, и их части.

    В ячейке стоит «bist losgeworden», а спросить нас могут про одно слово
    «losgeworden» — поэтому разбираем ячейку на слова. Ничего не достраиваем:
    берём ровно то, что напечатано на странице Flexion.
    """
    words: set[str] = set()

    def walk(node) -> None:
        if isinstance(node, str):
            value = node.strip()
            if value:
                words.add(value)
                for part in value.split():
                    if part:
                        words.add(part)
        elif isinstance(node, dict):
            for item in node.values():
                walk(item)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    for key, value in (tables or {}).items():
        if key in ("auxiliary", "infinitive", "full_form", "source"):
            continue
        walk(value)
    return words


def form_is_documented(form: str) -> str:
    """Напечатана ли такая словоформа в справочнике. Возвращает глагол или пустую строку.

    ЗАЧЕМ. Модель, которую мы просим проверить чужую правку, сама ошибается на трудном
    написании: правку «Er war froh, dass er das Schwein losgeworden war» она забраковала
    со словами «пишется раздельно» — а `losgeworden` напечатано в таблице `loswerden`
    ровно так, слитно. Спорить с моделью нечем, а со справочником — есть чем.

    Отбор в два шага: дешёвая выборка по тексту JSON сужает круг, а потом слово
    сверяется ТОЧНО со списком напечатанных форм. Без второго шага «geworden» находило
    бы себя внутри «losgeworden», то есть подтверждало бы то, чего в таблице нет.
    """
    from backend.database import get_db_connection_context

    word = str(form or "").strip()
    if not word or " " in word:
        return ""
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    # Строки с ключом «модель:…» сюда НЕ ПОПАДАЮТ. Это ответ модели,
                    # подтверждённый вторым спросом, — им можно построить таблицу
                    # спряжения, но нельзя возражать модели её же словами: тогда
                    # проверка правки сверялась бы сама с собой.
                    """SELECT verb, tables FROM bt_3_german_verb_paradigms
                        WHERE documented AND verb NOT LIKE %s
                          AND tables::text ILIKE %s LIMIT 40;""",
                    (_MODEL_KEY_PREFIX + "%", "%" + word + "%"),
                )
                rows = cur.fetchall()
    except Exception:
        logging.debug("справочник форм: чтение не удалось", exc_info=True)
        return ""
    for verb, tables in rows:
        if isinstance(tables, dict) and word in _printed_words(tables):
            return str(verb or "")
    return ""


def confirm_form_growing_the_reference(form: str, *, sentence: str = "") -> str:
    """Подтвердить словоформу справочником, ДОСТРАИВАЯ справочник, если он молчит.

    Два шага, и второй — не догадка:

      1. Форма ищется среди уже напечатанных таблиц (`form_is_documented`).
      2. Справочник молчит — спрашиваем модель, НА КАКУЮ СТРАНИЦУ смотреть
         (`run_infinitive_of_form`). Модель здесь указатель, а не источник: её ответ
         признаётся только тогда, когда на скачанной странице Flexion наша форма
         НАПЕЧАТАНА. Ошиблась моделью — страница не подтвердит, и подтверждения не
         будет. Выдумать форму этим путём нельзя.

    Скачанная таблица сохраняется, поэтому справочник растёт сам: следующий раз этот
    глагол найдётся на первом шаге, без модели и без сети. Это и есть «не подставляем
    догадку, а достраиваем ИСТОЧНИК» (CLAUDE.md, правило ноль).

    Возвращает инфинитив-подтверждение или пустую строку.
    """
    word = str(form or "").strip()
    if not word or " " in word:
        return ""
    known = form_is_documented(word)
    if known:
        return known

    try:
        from backend.openai_manager import run_infinitive_of_form
        candidates = run_infinitive_of_form(word=word, sentence=sentence)
    except Exception:
        logging.debug("указатель форм не ответил", exc_info=True)
        return ""

    for infinitive in candidates:
        tables = load_paradigm(infinitive)
        if tables is None:                     # про этот глагол ещё не спрашивали
            tables = fetch_documented_tables(infinitive)
            store_paradigm(infinitive, tables)   # справочник вырос — навсегда
        if tables and word in _printed_words(tables):
            logging.info("справочник дополнен: %s подтверждает форму %s", infinitive, word)
            return infinitive
    return ""
