# -*- coding: utf-8 -*-
"""Склонение существительных и степени сравнения — ДОСЛОВНО из таблицы de.wiktionary.

ЗАЧЕМ ЭТОТ МОДУЛЬ ПОЯВИЛСЯ
──────────────────────────
Владелец 17.08.2026: «МЫ РАБОТАЕМ С БАЗАМИ, СПРАВОЧНИКАМИ, СЛОВАРЯМИ, используем модель.
Но мы не даём механически сделанный ответ. МЫ НИЧЕГО НЕ ПРИДУМЫВАЕМ. Вообще.»

До этого модуля падежи и степени сравнения СЧИТАЛ КОД. Замер 18.08.2026 показал, что
наполнение данных этого не лечит — неверна сама конструкция: из источника брался только
родительный падеж, а винительный и дательный печатались голым словом ВСЕГДА:

    Nominativ  der Student
    Akkusativ  den Student      ← надо den Studenten
    Dativ      dem Student      ← надо dem Studenten
    Genitiv    des Studenten    ← верно, потому что пришло из данных

И степени сравнения дописывались окончанием: «gut → guter / am gutesten»,
«alt → alter / am altesten», «hoch → hocher / am hochsten».

ГДЕ ЛЕЖИТ ИСТОЧНИК (проверено запросами 18.08.2026)
───────────────────────────────────────────────────
У ГЛАГОЛОВ таблица живёт на отдельной странице «Flexion:<глагол>» — на этом построен
`backend/german_verb_paradigms.py`. У СУЩЕСТВИТЕЛЬНЫХ И ПРИЛАГАТЕЛЬНЫХ таких страниц НЕТ:
«Flexion:Student» отдаёт 404. Их таблицы напечатаны на ОБЫЧНОЙ странице слова:

    Substantiv, m
              Singular          Plural
    Nominativ der Student       die Studenten
    Genitiv   des Studenten     der Studenten
    Dativ     dem Studenten     den Studenten
    Akkusativ den Studenten     die Studenten

    Adjektiv
    Positiv  Komparativ  Superlativ
    alt      älter       am ältesten

Поэтому здесь свой адрес страницы и свой поиск нужного раздела, а всё остальное —
разбор ячеек, кэш, три состояния — повторяет глагольный модуль.

ТРИ СОСТОЯНИЯ, КОТОРЫЕ НЕЛЬЗЯ ПУТАТЬ
────────────────────────────────────
    None  — справочник МОЛЧИТ (сеть, 429). В кэш не пишем, спросим позже.
    {}    — страницы нет. Пишем в кэш как «нет», больше не ходим.
    табл. — форма напечатана. Кладём дословно и подписываем источником.

Ни одного окончания этот модуль не дописывает. Формы кладутся как напечатаны.

ОМОГРАФЫ
────────
У «Kiefer» на странице ДВА раздела: «Substantiv, m» (челюсть) и «Substantiv, f» (сосна),
и таблицы у них разные. Поэтому склонение хранится по родам: {"m": {...}, "f": {...}},
а выбирает вызывающий — по артиклю, который у него уже есть.
"""
from __future__ import annotations

import html as _html
import json
import logging
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

API_URL = "https://de.wiktionary.org/w/api.php"
USER_AGENT = "DerSchlaufuchs/1.0 (German learning app)"
FETCH_TIMEOUT_SEC = float((os.getenv("REFERENCE_FORMS_TIMEOUT_SEC") or "8").strip() or "8")

# Формы артикля, по которым отделяем колонку Singular от колонки Plural.
_ARTICLES = {"der", "die", "das", "des", "dem", "den"}
_CASE_LABELS = ("Nominativ", "Genitiv", "Dativ", "Akkusativ")
# «—» и его типографские родственники: колонки у слова просто нет
# (singularia/pluralia tantum). Это НЕ ошибка и не повод молчать обо всей таблице.
_ABSENT = {"—", "-", "–", "‐", "", "?"}
_GENDER_BY_LABEL = {"m": "m", "f": "f", "n": "n"}


def ensure_german_reference_forms_schema() -> None:
    """Две таблицы кэша. Ключ — само слово в нижнем регистре."""
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bt_3_german_noun_declensions (
                        noun        TEXT PRIMARY KEY,
                        tables      JSONB,
                        documented  BOOLEAN NOT NULL DEFAULT FALSE,
                        checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    CREATE TABLE IF NOT EXISTS bt_3_german_adjective_degrees (
                        adjective   TEXT PRIMARY KEY,
                        degrees     JSONB,
                        documented  BOOLEAN NOT NULL DEFAULT FALSE,
                        checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    -- Слова, которые не закрыла НИ ОДНА ступень каскада. Это не «мусор»,
                    -- а наряд на работу: владелец разбирает их руками в личке. Без этой
                    -- таблицы дыра стала бы незаметной — ровно то, чего мы избегаем.
                    CREATE TABLE IF NOT EXISTS bt_3_reference_forms_unresolved (
                        word        TEXT PRIMARY KEY,
                        pos         TEXT NOT NULL,
                        reason      TEXT NOT NULL DEFAULT '',
                        reviewed    BOOLEAN NOT NULL DEFAULT FALSE,
                        checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
            conn.commit()
    except Exception:
        logging.warning("формы из справочника: схема не создана", exc_info=True)


# ── Доступ к справочнику ─────────────────────────────────────────────────────
def _api(params: dict) -> dict | None:
    """Ответ справочника. None — справочник МОЛЧИТ (сеть, 429), а не «данных нет»."""
    url = API_URL + "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=FETCH_TIMEOUT_SEC) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        logging.warning("формы из справочника: HTTP %s", exc.code)
        return None
    except Exception:
        logging.warning("формы из справочника: справочник не ответил", exc_info=True)
        return None


def _text(fragment: str) -> str:
    """Разметка → чистый текст одной ячейки."""
    return re.sub(r"\s+", " ", _html.unescape(re.sub(r"<[^>]+>", " ", str(fragment or "")))).strip()


def _first_variant(cell_html: str) -> str:
    """«dem Haus<br>dem Hause» → «dem Haus».

    В таблице у падежа бывает несколько равноправных вариантов, напечатанных через
    перенос строки. Мы берём первый — это НЕ выбор «на глаз», а первая напечатанная
    форма источника; остальные варианты не выдумываются и не смешиваются в одну строку.
    """
    parts = re.split(r"<br\s*/?>|\n", str(cell_html or ""))
    for part in parts:
        text = _text(part)
        if text:
            return text
    return ""


def _rows_of(table_html: str) -> list[list[str]]:
    """Таблица → строки → ячейки (в порядке разметки, первый вариант в каждой)."""
    rows: list[list[str]] = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.S):
        cells = [_first_variant(c) for c in
                 re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", tr, re.S)]
        if any(cells):
            rows.append(cells)
    return rows


def _squeeze(value: str) -> str:
    """«Student ( Deutsch )» → «student(deutsch)».

    Теги вычищаются заменой на ПРОБЕЛ (иначе склеятся соседние слова), поэтому внутри
    скобок появляются пробелы, и наивная проверка на подстроку «(Deutsch)» не срабатывает.
    Сравниваем заголовки в сжатом виде.
    """
    return re.sub(r"\s+", "", str(value or "")).lower()


def _german_section(page_html: str) -> str:
    """Только немецкий раздел страницы.

    Страница «alt» содержит ещё итальянский и каталанский разделы со своими таблицами
    словоизменения. Без этого среза мы бы читали чужой язык как немецкий.
    """
    html = str(page_html or "")
    heads = list(re.finditer(r"<h2[^>]*>(.*?)</h2>", html, re.S))
    for i, head in enumerate(heads):
        if "(deutsch)" not in _squeeze(_text(head.group(1))):
            continue
        end = heads[i + 1].start() if i + 1 < len(heads) else len(html)
        return html[head.end():end]
    return html if not heads else ""


def _sections(german_html: str) -> list[tuple[str, str]]:
    """[(заголовок раздела, разметка до следующего заголовка)] внутри немецкой части."""
    out: list[tuple[str, str]] = []
    heads = list(re.finditer(r"<h3[^>]*>(.*?)</h3>", german_html, re.S))
    for i, head in enumerate(heads):
        end = heads[i + 1].start() if i + 1 < len(heads) else len(german_html)
        out.append((_text(head.group(1)), german_html[head.end():end]))
    return out


def _inflection_tables(fragment: str) -> list[str]:
    return [t for t in re.findall(r"<table[^>]*>.*?</table>", fragment, re.S)
            if "inflection-table" in (re.match(r"<table[^>]*>", t, re.S) or
                                      re.compile("")).group(0)]


def _fetch_page(word: str) -> str | None:
    """Разметка немецкого раздела. None — молчит. "" — страницы нет."""
    name = str(word or "").strip()
    if not name:
        return ""
    payload = _api({"action": "parse", "page": name, "prop": "text",
                    "format": "json", "formatversion": "2"})
    if payload is None:
        return None
    if payload.get("error"):
        return ""
    return _german_section((payload.get("parse") or {}).get("text") or "")


# ── Разбор таблицы склонения ─────────────────────────────────────────────────
def _genders_of(heading: str) -> list[str]:
    """«Substantiv, m, n» → ['m','n']. «Substantiv» (pluralia tantum) → ['pl']."""
    parts = [p.strip().lower() for p in str(heading or "").split(",")]
    genders = [_GENDER_BY_LABEL[p] for p in parts[1:] if p in _GENDER_BY_LABEL]
    return genders or ["pl"]


def _declension_table(table_html: str) -> dict[str, Any]:
    """Одна таблица склонения → {'rows': [...]} или {}."""
    rows = _rows_of(table_html)
    if not rows:
        return {}
    header = [c.lower() for c in rows[0]]
    if not any("singular" in c or "plural" in c for c in header):
        return {}
    # В какой колонке Singular, в какой Plural — берём из шапки, а не по позиции:
    # у pluralia tantum колонка Singular отсутствует вовсе.
    sg_at = pl_at = -1
    for i, c in enumerate(header):
        if "singular" in c and sg_at < 0:
            sg_at = i
        if "plural" in c and pl_at < 0:
            pl_at = i
    out_rows: list[dict[str, str]] = []
    for row in rows[1:]:
        if not row:
            continue
        label = row[0].strip()
        if label not in _CASE_LABELS:
            continue
        sg = row[sg_at].strip() if 0 <= sg_at < len(row) else ""
        pl = row[pl_at].strip() if 0 <= pl_at < len(row) else ""
        out_rows.append({
            "case": label.lower()[:3],
            "label": label,
            "singular": "" if sg in _ABSENT else sg,
            "plural": "" if pl in _ABSENT else pl,
        })
    if len(out_rows) != 4:
        return {}
    return {"rows": out_rows,
            "has_singular": any(r["singular"] for r in out_rows),
            "has_plural": any(r["plural"] for r in out_rows)}


_ARTICLE_GENDER = {"der": "m", "die": "f", "das": "n"}


def _agrees_with_gender(table: dict[str, Any], gender: str) -> bool:
    """Артикль в именительном падеже обязан совпадать с родом раздела.

    Это не «улучшение» ответа источника, а защита от того, что я взял ЧУЖУЮ таблицу.
    Прогон 18.08.2026: у «Junge» в раздел женского рода попадала таблица с «der Junge»
    и выдуманным «die Junges» — противоречие артикля и рода поймало именно это.
    Для pluralia tantum ('pl') проверять нечего: единственного числа у слова нет.
    """
    if gender == "pl":
        return True
    for row in table.get("rows") or []:
        if row.get("case") != "nom":
            continue
        first = str(row.get("singular") or "").split(" ")[0].lower()
        if not first:
            return True
        return _ARTICLE_GENDER.get(first) == gender
    return True


def _declension_from_html(german_html: str) -> dict[str, Any]:
    """{'m': {...}, 'f': {...}} — по родам, потому что у омографов таблицы разные."""
    out: dict[str, Any] = {}
    for heading, fragment in _sections(german_html):
        if not _squeeze(heading).startswith("substantiv"):
            continue
        tables = [t for t in (_declension_table(x) for x in _inflection_tables(fragment)) if t]
        if not tables:
            continue
        genders = _genders_of(heading)
        # Заголовок «Substantiv, m, n» описывает ДВА слова, и таблиц под ним тоже две.
        # Раскладываем по порядку; если таблица одна, а родов несколько — отдаём её
        # только тому роду, с которым согласуется артикль.
        for i, gender in enumerate(genders):
            table = tables[i] if i < len(tables) else (tables[0] if len(tables) == 1 else None)
            if not table or not _agrees_with_gender(table, gender):
                continue
            out.setdefault(gender, table)
    return out


def _degrees_from_html(german_html: str) -> dict[str, str]:
    """{'positive','comparative','superlative'} или {} — если сравнения нет."""
    for heading, fragment in _sections(german_html):
        if not _squeeze(heading).startswith("adjektiv"):
            continue
        for table_html in _inflection_tables(fragment):
            rows = _rows_of(table_html)
            if len(rows) < 2:
                continue
            header = [c.strip().lower() for c in rows[0]]
            if "positiv" not in header or "superlativ" not in header:
                continue
            values = rows[1]
            try:
                positive = values[header.index("positiv")].strip()
                comparative = values[header.index("komparativ")].strip()
                superlative = values[header.index("superlativ")].strip()
            except (ValueError, IndexError):
                continue
            if positive in _ABSENT or comparative in _ABSENT:
                return {}
            return {"positive": positive, "comparative": comparative,
                    "superlative": superlative}
    return {}


# ── Скачивание ───────────────────────────────────────────────────────────────
def fetch_noun_declension(noun: str) -> dict[str, Any] | None:
    """{} — страницы или таблицы нет. None — справочник молчит."""
    html = _fetch_page(noun)
    if html is None:
        return None
    return _declension_from_html(html)


def fetch_adjective_degrees(adjective: str) -> dict[str, str] | None:
    html = _fetch_page(adjective)
    if html is None:
        return None
    return _degrees_from_html(html)


# ── Кэш ──────────────────────────────────────────────────────────────────────
def _store(table: str, key_col: str, val_col: str, key: str, value: dict | None) -> None:
    """value={} — «страницы нет», это тоже ответ и он запоминается. None не пишем."""
    from backend.database import get_db_connection_context
    word = str(key or "").strip().lower()
    if not word or value is None:
        return
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {table} ({key_col}, {val_col}, documented, checked_at)
                    VALUES (%s, %s::jsonb, %s, NOW())
                    ON CONFLICT ({key_col}) DO UPDATE
                       SET {val_col} = EXCLUDED.{val_col},
                           documented = EXCLUDED.documented,
                           checked_at = NOW();
                    """,
                    (word, json.dumps(value, ensure_ascii=False), bool(value)),
                )
            conn.commit()
    except Exception:
        logging.warning("формы из справочника: не записал %s", word, exc_info=True)


def _load_cached(table: str, key_col: str, val_col: str, key: str) -> dict | None:
    """None — не спрашивали. {} — спрашивали, таблицы нет."""
    from backend.database import get_db_connection_context
    word = str(key or "").strip().lower()
    if not word:
        return None
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {val_col}, documented FROM {table} WHERE {key_col} = %s;",
                    (word,),
                )
                row = cur.fetchone()
    except Exception:
        logging.debug("формы из справочника: чтение кэша не удалось", exc_info=True)
        return None
    if not row:
        return None
    value, documented = row
    if not documented or not isinstance(value, dict):
        return {}
    return value


def store_noun_declension(noun: str, tables: dict | None) -> None:
    _store("bt_3_german_noun_declensions", "noun", "tables", noun, tables)


def load_noun_declension(noun: str) -> dict | None:
    return _load_cached("bt_3_german_noun_declensions", "noun", "tables", noun)


def store_adjective_degrees(adjective: str, degrees: dict | None) -> None:
    _store("bt_3_german_adjective_degrees", "adjective", "degrees", adjective, degrees)


def load_adjective_degrees(adjective: str) -> dict | None:
    return _load_cached("bt_3_german_adjective_degrees", "adjective", "degrees", adjective)


# ── Ступень 2: составное слово ───────────────────────────────────────────────
def _compose(word: str, head: str, head_form: str) -> str:
    """(«Haustürschlüssel», «schlüssel», «des Schlüssels») → «des Haustürschlüssels».

    Замена ХВОСТА на его же склонённую форму: приставка слова остаётся как есть,
    склоняется только голова — ровно так устроено немецкое составное слово. Ни одного
    окончания мы не придумываем, окончание приходит из напечатанной таблицы головы.
    """
    parts = str(head_form or "").split(" ")
    if len(parts) < 2:
        return ""
    article, noun_form = parts[0], " ".join(parts[1:])
    prefix = word[: len(word) - len(head)]
    if not prefix or not noun_form:
        return ""
    # Внутри слитного композита голова пишется со строчной («Haustür» + «schlüssel»),
    # а ПОСЛЕ ДЕФИСА сохраняет заглавную: «Pro-Kopf-Einkommen», а не «Pro-Kopf-einkommen».
    # Поймано прогоном 18.08.2026 на этом самом слове.
    tail = noun_form if prefix.endswith("-") else noun_form[:1].lower() + noun_form[1:]
    return f"{article} {prefix}{tail}"


def declension_from_compound(noun: str) -> dict[str, Any] | None:
    """Склонение составного слова по его ГОЛОВЕ. None — шва нет или голова не покрыта."""
    from backend.article_authority import compound_head
    word = str(noun or "").strip()
    head = compound_head(word)
    if not head or len(word) <= len(head):
        return None
    head_tables = load_noun_declension(head)
    if not head_tables:
        return None
    for gender, table in head_tables.items():
        rows = []
        for row in table.get("rows") or []:
            sg = _compose(word, head, row.get("singular") or "") if row.get("singular") else ""
            pl = _compose(word, head, row.get("plural") or "") if row.get("plural") else ""
            rows.append({**row, "singular": sg, "plural": pl})
        if len(rows) == 4 and any(r["singular"] or r["plural"] for r in rows):
            return {gender: {"rows": rows,
                             "has_singular": any(r["singular"] for r in rows),
                             "has_plural": any(r["plural"] for r in rows)},
                    "head": head}
    return None


# ── Ступень 3: модель, и только при совпадении двух независимых ответов ──────
_DECLENSION_TASK = "german_noun_declension_reference"
_DEGREES_TASK = "german_adjective_degrees_reference"

_DECLENSION_INSTRUCTION = """Du bist ein deutsches Flexionswörterbuch.
Gib für das genannte Substantiv die vollständige Deklination als JSON zurück, sonst nichts.
Format exakt:
{"nom_sg":"der Hund","gen_sg":"des Hundes","dat_sg":"dem Hund","akk_sg":"den Hund",
 "nom_pl":"die Hunde","gen_pl":"der Hunde","dat_pl":"den Hunden","akk_pl":"die Hunde"}
Regeln: Artikel immer mitschreiben. Existiert eine Zahl nicht (Pluraletantum,
Singularetantum), schreibe "" in die betroffenen Felder. Erfinde nichts."""

_DEGREES_INSTRUCTION = """Du bist ein deutsches Flexionswörterbuch.
Gib für das genannte Adjektiv die Steigerung als JSON zurück, sonst nichts.
Format exakt: {"positive":"alt","comparative":"älter","superlative":"am ältesten"}
Regeln: Superlativ immer mit "am". Ist das Wort nicht steigerbar, gib
{"positive":"","comparative":"","superlative":""} zurück. Erfinde nichts."""


def _register_instructions() -> None:
    """Инструкции живут рядом с кодом, который их использует, а не в общем файле на 9000
    строк. `llm_execute` принимает только КЛЮЧ из реестра, поэтому регистрируем свои."""
    from backend.openai_manager import system_message
    system_message.setdefault(_DECLENSION_TASK, _DECLENSION_INSTRUCTION)
    system_message.setdefault(_DEGREES_TASK, _DEGREES_INSTRUCTION)


def _ask_once(task: str, word: str) -> dict:
    """Один спрос модели. {} — не ответила или ответ не разобрался."""
    import asyncio
    from backend.openai_manager import llm_execute, parse_llm_json_object
    _register_instructions()
    try:
        text = asyncio.run(llm_execute(
            task_name=task, system_instruction_key=task,
            user_message=str(word or "").strip(), poll_interval_seconds=1.0,
        ))
    except RuntimeError:
        # Уже внутри работающего цикла событий — здесь синхронный путь неприменим.
        logging.warning("формы из справочника: модель вызвана из асинхронного контекста")
        return {}
    except Exception:
        logging.warning("формы из справочника: модель не ответила про %s", word, exc_info=True)
        return {}
    parsed = parse_llm_json_object(text, context=task)
    return parsed if isinstance(parsed, dict) else {}


def _agreed(first: dict, second: dict, keys: tuple[str, ...]) -> dict:
    """Ответ принимается ТОЛЬКО если оба спроса дали одно и то же.

    Сверять со справочником нечего — слова там нет, иначе мы бы сюда не дошли.
    Поэтому подтверждение = согласие двух независимых ответов. Разошлись — молчим
    и слово уходит в отчёт владельцу, а не подставляется наугад.
    """
    if not first or not second:
        return {}
    out: dict[str, str] = {}
    for key in keys:
        a = re.sub(r"\s+", " ", str(first.get(key) or "")).strip()
        b = re.sub(r"\s+", " ", str(second.get(key) or "")).strip()
        if a.lower() != b.lower():
            return {}
        out[key] = a
    return out if any(out.values()) else {}


_DECL_KEYS = ("nom_sg", "gen_sg", "dat_sg", "akk_sg", "nom_pl", "gen_pl", "dat_pl", "akk_pl")
_DEG_KEYS = ("positive", "comparative", "superlative")


def declension_from_model(noun: str) -> dict[str, Any] | None:
    """Склонение от модели при совпадении двух ответов. None — не подтвердилось."""
    agreed = _agreed(_ask_once(_DECLENSION_TASK, noun),
                     _ask_once(_DECLENSION_TASK, noun), _DECL_KEYS)
    if not agreed:
        return None
    rows = []
    for case, label in (("nom", "Nominativ"), ("gen", "Genitiv"),
                        ("dat", "Dativ"), ("akk", "Akkusativ")):
        rows.append({"case": case, "label": label,
                     "singular": agreed.get(f"{case}_sg", ""),
                     "plural": agreed.get(f"{case}_pl", "")})
    gender = _ARTICLE_GENDER.get(str(rows[0]["singular"]).split(" ")[0].lower(), "pl")
    return {gender: {"rows": rows,
                     "has_singular": any(r["singular"] for r in rows),
                     "has_plural": any(r["plural"] for r in rows)}}


def degrees_from_model(adjective: str) -> dict[str, str] | None:
    """Степени сравнения от модели при совпадении двух ответов."""
    agreed = _agreed(_ask_once(_DEGREES_TASK, adjective),
                     _ask_once(_DEGREES_TASK, adjective), _DEG_KEYS)
    return agreed or None


# ── Каскад: справочник → композит → модель → счётчик ─────────────────────────
def noun_declension_for(noun: str, *, allow_network: bool = False,
                        allow_model: bool = False) -> dict[str, Any] | None:
    """Склонение с подписью источника. None — не закрыто ничем (это в отчёт владельцу)."""
    word = str(noun or "").strip()
    if not word or " " in word:
        return None
    tables = load_noun_declension(word)
    if tables is None and allow_network:
        fetched = fetch_noun_declension(word)
        store_noun_declension(word, fetched)
        tables = fetched
    if tables:
        return {**tables, "source": "wiktionary-deklination"}
    compound = declension_from_compound(word)
    if compound:
        return {**compound, "source": "правило композита"}
    if allow_model:
        guessed = declension_from_model(word)
        if guessed:
            return {**guessed, "source": "модель"}
    return None


def adjective_degrees_for(adjective: str, *, allow_network: bool = False,
                          allow_model: bool = False) -> dict[str, Any] | None:
    """Степени сравнения с подписью источника. None — не закрыто ничем."""
    word = str(adjective or "").strip()
    if not word or " " in word:
        return None
    degrees = load_adjective_degrees(word)
    if degrees is None and allow_network:
        fetched = fetch_adjective_degrees(word)
        store_adjective_degrees(word, fetched)
        degrees = fetched
    if degrees:
        return {**degrees, "source": "wiktionary-steigerung"}
    if allow_model:
        guessed = degrees_from_model(word)
        if guessed:
            return {**guessed, "source": "модель"}
    return None


# ── Учёт непокрытых слов ─────────────────────────────────────────────────────
def mark_unresolved(word: str, pos: str, reason: str) -> None:
    """Слово, которое не закрыла ни одна ступень. Уходит владельцу на разбор."""
    from backend.database import get_db_connection_context
    key = str(word or "").strip()
    if not key:
        return
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bt_3_reference_forms_unresolved (word, pos, reason, checked_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (word) DO UPDATE
                       SET reason = EXCLUDED.reason, checked_at = NOW();
                    """,
                    (key, str(pos or ""), str(reason or "")),
                )
            conn.commit()
    except Exception:
        logging.warning("формы из справочника: не записал непокрытое %s", key, exc_info=True)


def clear_unresolved(word: str) -> None:
    """Слово закрылось — снимаем его с разбора."""
    from backend.database import get_db_connection_context
    key = str(word or "").strip()
    if not key:
        return
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM bt_3_reference_forms_unresolved WHERE word = %s;", (key,))
            conn.commit()
    except Exception:
        logging.debug("формы из справочника: не снял непокрытое %s", key, exc_info=True)


def unresolved_batch(limit: int = 20) -> list[tuple[str, str, str]]:
    """Порция для разбора в личке: [(слово, часть речи, причина)]."""
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT word, pos, reason FROM bt_3_reference_forms_unresolved "
                    "WHERE reviewed = FALSE ORDER BY checked_at ASC LIMIT %s;",
                    (int(limit),),
                )
                return [(str(a), str(b), str(c)) for a, b, c in (cur.fetchall() or [])]
    except Exception:
        logging.warning("формы из справочника: не прочитал очередь разбора", exc_info=True)
        return []


def unresolved_count() -> int:
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM bt_3_reference_forms_unresolved "
                            "WHERE reviewed = FALSE;")
                return int((cur.fetchone() or [0])[0])
    except Exception:
        logging.warning("формы из справочника: не посчитал очередь разбора", exc_info=True)
        return -1


def mark_unresolved_reviewed(word: str) -> None:
    """Спрошенное второй раз не приходит — как в разборе снятых слов."""
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE bt_3_reference_forms_unresolved SET reviewed = TRUE "
                            "WHERE word = %s;", (str(word or "").strip(),))
            conn.commit()
    except Exception:
        logging.warning("формы из справочника: не пометил разобранным", exc_info=True)


# ── Прогрев ──────────────────────────────────────────────────────────────────
def warm_reference_forms(*, limit: int = 200, pause_sec: float = 1.5,
                         allow_model: bool = False) -> dict:
    """Пройти по словам справочника и наполнить кэш. Только фоном: справочник даёт 429.

    Порядок ступеней тот же, что на выдаче. Что не закрылось ничем — попадает в
    очередь разбора владельцем, а не исчезает молча.
    """
    import time
    from backend.database import get_db_connection_context

    picked: list[tuple[str, str]] = []
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.lemma, u.pos FROM bt_3_lex_units u
                     WHERE u.lang = 'de' AND u.pos IN ('noun','adjective','adverb')
                       AND u.lemma IS NOT NULL AND u.lemma <> '' AND position(' ' in u.lemma) = 0
                       AND NOT EXISTS (SELECT 1 FROM bt_3_german_noun_declensions d
                                        WHERE d.noun = lower(u.lemma))
                       AND NOT EXISTS (SELECT 1 FROM bt_3_german_adjective_degrees a
                                        WHERE a.adjective = lower(u.lemma))
                     ORDER BY u.updated_at DESC NULLS LAST
                     LIMIT %s;
                    """,
                    (int(limit),),
                )
                picked = [(str(a), str(b)) for a, b in (cur.fetchall() or [])]
    except Exception:
        logging.warning("формы из справочника: не выбрал слова для прогрева", exc_info=True)
        return {"picked": 0}

    stats = {"picked": len(picked), "справочник": 0, "композит": 0, "модель": 0,
             "не закрыто": 0, "справочник молчал": 0}
    # Пауза самонастраивается. Замер 18.08.2026: при фиксированных 2.5 с справочник
    # отвечал 429 на каждый второй запрос. Молчание — не «нет данных», поэтому такие
    # слова НЕ попадают в кэш и будут переспрошены; но гнать в стену бессмысленно,
    # поэтому после отказа ждём дольше, а после успеха возвращаемся к обычному темпу.
    delay = max(0.0, float(pause_sec))
    ceiling = max(delay * 12, 30.0)
    for word, pos in picked:
        time.sleep(delay)
        if pos == "noun":
            fetched = fetch_noun_declension(word)
            if fetched is None:
                stats["справочник молчал"] += 1
                delay = min(ceiling, max(delay * 2, 1.0))
                continue
            delay = max(float(pause_sec), delay / 1.5)
            store_noun_declension(word, fetched)
            result = noun_declension_for(word, allow_model=allow_model)
        else:
            fetched = fetch_adjective_degrees(word)
            if fetched is None:
                stats["справочник молчал"] += 1
                delay = min(ceiling, max(delay * 2, 1.0))
                continue
            delay = max(float(pause_sec), delay / 1.5)
            store_adjective_degrees(word, fetched)
            result = adjective_degrees_for(word, allow_model=allow_model)
        if not result:
            stats["не закрыто"] += 1
            mark_unresolved(word, pos, "ни справочник, ни композит, ни модель")
            continue
        source = str(result.get("source") or "")
        stats["композит" if source == "правило композита"
              else "модель" if source == "модель" else "справочник"] += 1
        clear_unresolved(word)
    return stats


# ── Быстрый путь: исходник страницы пачкой по 50 слов ────────────────────────
# Разметку страницы приходится просить по одной (action=parse), и справочник уходит в
# лимит примерно на десятом запросе. Но ФОРМЫ напечатаны и в исходнике страницы —
# шаблоном «Deutsch Substantiv Übersicht» / «Deutsch Adjektiv Übersicht», — а исходник
# отдаётся пачкой до 50 названий за один запрос. 6469 слов превращаются в ~130 запросов.
#
# Это НЕ другой источник и не вывод формы из основы: в шаблоне лежат ровно те же
# напечатанные формы, из которых справочник и рисует свою таблицу. Мы дописываем только
# артикль по роду и падежу — закрытая таблица из 16 клеток, та же, что рисует сам
# справочник. Совпадение обоих путей проверяется тестом на реальных словах.
_ART_SG_BY_GENDER = {
    "m": {"nom": "der", "gen": "des", "dat": "dem", "akk": "den"},
    "f": {"nom": "die", "gen": "der", "dat": "der", "akk": "die"},
    "n": {"nom": "das", "gen": "des", "dat": "dem", "akk": "das"},
}
_ART_PL = {"nom": "die", "gen": "der", "dat": "den", "akk": "die"}
_CASE_KEYS = (("nom", "Nominativ"), ("gen", "Genitiv"),
              ("dat", "Dativ"), ("akk", "Akkusativ"))


def _template_params(block: str) -> dict[str, str]:
    """Параметры шаблона. Резать по переносам строк НЕЛЬЗЯ.

    Ошибка 18.08.2026: часть шаблонов записана в ОДНУ строку
    («{{Deutsch Adjektiv Übersicht|Positiv=arrogant|Komparativ=arroganter|…}}»), и разбор
    по «\n|» не находил у них ни одного параметра. 71 слово из первой сотни получило
    ложную пометку «страницы нет» — тот же класс, что стоил глаголам 2737 таких пометок.
    Поэтому режем по «|» на нулевой глубине вложенности: внутри значений встречаются и
    вложенные шаблоны {{...}}, и ссылки [[...|...]].
    """
    text = str(block or "")
    text = text[text.find("|") + 1:] if "|" in text else ""
    if text.endswith("}}"):
        text = text[:-2]
    parts, depth, current = [], 0, []
    i = 0
    while i < len(text):
        two = text[i:i + 2]
        if two in ("{{", "[["):
            depth += 1
            current.append(two)
            i += 2
            continue
        if two in ("}}", "]]"):
            depth = max(0, depth - 1)
            current.append(two)
            i += 2
            continue
        if text[i] == "|" and depth == 0:
            parts.append("".join(current))
            current = []
            i += 1
            continue
        current.append(text[i])
        i += 1
    parts.append("".join(current))
    params: dict[str, str] = {}
    for chunk in parts:
        if "=" not in chunk:
            continue
        key, _, value = chunk.partition("=")
        params[key.strip()] = value.strip()
    return params


def _blocks(text: str, kind: str) -> list[str]:
    out, needle = [], "{{Deutsch " + kind + " Übersicht"
    start = text.find(needle)
    while start >= 0:
        depth, i = 0, start
        while i < len(text):
            if text.startswith("{{", i):
                depth += 1
                i += 2
                continue
            if text.startswith("}}", i):
                depth -= 1
                i += 2
                if depth == 0:
                    break
                continue
            i += 1
        out.append(text[start:i])
        start = text.find(needle, i)
    return out


def declension_from_source(text: str) -> dict[str, Any]:
    """Исходник страницы → {'m': {...}, 'f': {...}} или {}."""
    result: dict[str, Any] = {}
    for block in _blocks(str(text or ""), "Substantiv"):
        params = _template_params(block)
        # У омографов и у слов с двумя множественными шаблон нумерует варианты:
        # «Genus 1=m / Genus 2=n», «Nominativ Plural 1=Männer». Причём нумерация
        # НЕЗАВИСИМА по полям: у «Kiefer» род пронумерован, а множественное — нет.
        # Поэтому каждое поле ищем сначала под своим номером, потом без номера,
        # потом под первым. Сверка с разметкой 18.08.2026 поймала ровно это: у Mann,
        # Land, Wagen, Möbel, Junge множественное терялось.
        suffixes = [x for x in ("", " 1", " 2", " 3") if f"Genus{x}" in params] or [""]

        def pick(field: str, suffix: str) -> str:
            for probe in (f"{field}{suffix}", field, f"{field} 1"):
                value = params.get(probe)
                if value:
                    return value.strip()
            return ""

        for suffix in dict.fromkeys(suffixes):
            gender = (params.get(f"Genus{suffix}") or "").strip().lower()
            if not gender:
                # Блок без рода — не наш случай (имя собственное, чужая часть речи).
                # Раньше такой блок молча ложился в «pl» и добавлял слову несуществующее
                # множественное: поймано на «Herz».
                continue
            rows = []
            for case, label in _CASE_KEYS:
                sg = pick(f"{label} Singular", suffix)
                pl = pick(f"{label} Plural", suffix)
                art_sg = _ART_SG_BY_GENDER.get(gender, {}).get(case, "")
                rows.append({
                    "case": case, "label": label,
                    "singular": f"{art_sg} {sg}".strip() if sg and sg not in _ABSENT and art_sg else "",
                    "plural": f"{_ART_PL[case]} {pl}".strip() if pl and pl not in _ABSENT else "",
                })
            if not any(r["singular"] or r["plural"] for r in rows):
                continue
            key = gender if gender in ("m", "f", "n") else "pl"
            result.setdefault(key, {
                "rows": rows,
                "has_singular": any(r["singular"] for r in rows),
                "has_plural": any(r["plural"] for r in rows),
            })
    return result


def degrees_from_source(text: str) -> dict[str, str]:
    """Исходник страницы → степени сравнения. «am» дописывается: так печатает справочник."""
    for block in _blocks(str(text or ""), "Adjektiv"):
        params = _template_params(block)
        positive = (params.get("Positiv") or "").strip()
        comparative = (params.get("Komparativ") or "").strip()
        superlative = (params.get("Superlativ") or "").strip()
        if not positive:
            continue
        if comparative in _ABSENT or superlative in _ABSENT or not comparative or not superlative:
            # Справочник ЗНАЕТ это слово и говорит: степеней нет («absichtlich»,
            # «außergewöhnlich» помечены Komparativ=—). Это ОТВЕТ, а не отсутствие
            # страницы. Раньше он складывался в «страницы нет», и слово выглядело
            # непокрытым — 217 из 300 в первом прогоне оказались как раз такими.
            return {"positive": positive, "comparative": "", "superlative": "",
                    "gradable": False}
        if not superlative.lower().startswith("am "):
            superlative = "am " + superlative
        return {"positive": positive, "comparative": comparative,
                "superlative": superlative, "gradable": True}
    return {}


def fetch_sources_bulk(titles: list[str]) -> dict[str, str] | None:
    """{название: исходник} для пачки до 50 слов. None — справочник молчит."""
    names = [str(t).strip() for t in titles if str(t or "").strip()][:50]
    if not names:
        return {}
    payload = _api({"action": "query", "prop": "revisions", "rvprop": "content",
                    "rvslots": "main", "format": "json", "formatversion": "2",
                    "titles": "|".join(names)})
    if payload is None:
        return None
    pages = (payload.get("query") or {}).get("pages")
    if not isinstance(pages, list):
        # Ответ БЕЗ списка страниц — это «справочник не ответил», а НЕ «страниц нет».
        # Ошибка 18.08.2026: пустой список молча превращался в «страницы нет» у всей
        # пачки, и 1002 слова получили ложную пометку. Тот же класс дефекта, что мы
        # вычищаем из проекта: молчание нельзя записывать как отсутствие данных.
        logging.warning("формы из справочника: ответ без списка страниц (%d слов)", len(names))
        return None
    out: dict[str, str] = {}
    for page in pages:
        title = str(page.get("title") or "")
        if page.get("missing"):
            out[title] = ""
            continue
        try:
            out[title] = page["revisions"][0]["slots"]["main"]["content"]
        except Exception:
            out[title] = ""
    return out


def _reference_title(word: str, pos: str) -> str:
    """Написание, о котором надо спрашивать справочник.

    В нашем словаре заголовки бывают с заглавной — «Grundlegend», «Kaufmännisch»: так
    слово пришло из сохранения. В немецком существительное пишется с заглавной, а
    прилагательное и наречие — со строчной, и страницы «Grundlegend» не существует.
    Замер 18.08.2026: из-за этого прилагательные закрылись только на 39%.

    Правило регистра в проекте уже есть — `german_grammar_tables.german_headword_case`;
    берём его, чтобы не завести второе.
    """
    name = str(word or "").strip()
    if not name:
        return ""
    try:
        from backend.german_grammar_tables import german_headword_case
        fixed = german_headword_case(name, pos)
        if fixed:
            return fixed
    except Exception:
        logging.debug("формы из справочника: правило регистра недоступно", exc_info=True)
    return name[:1].lower() + name[1:] if pos in ("adjective", "adverb") else name


def warm_from_source_bulk(*, limit: int = 0, batch: int = 50,
                          pause_sec: float = 1.5) -> dict:
    """Полный прогрев через ИСХОДНИКИ страниц: до 50 слов за один запрос.

    Почему так, а не по одной странице разметки: разметку приходится просить поштучно
    (action=parse), и справочник уходит в лимит примерно на десятом запросе — 6469 слов
    заняли бы недели. Формы напечатаны и в исходнике, а исходники отдаются пачкой.

    Сверка обоих путей на 21 слове (18.08.2026) показала, что исходник ТОЧНЕЕ: на «Zeit»
    разбор разметки хватал чужой раздел и выдавал «der Zeit / die Zeits», чего в языке
    нет. Поэтому основной путь — исходник, а разбор разметки остаётся для проверок.
    """
    import time
    from backend.database import get_db_connection_context

    sql = """
        SELECT u.lemma, u.pos FROM bt_3_lex_units u
         WHERE u.lang = 'de' AND u.pos IN ('noun','adjective','adverb')
           AND u.lemma IS NOT NULL AND u.lemma <> '' AND position(' ' in u.lemma) = 0
           AND NOT EXISTS (SELECT 1 FROM bt_3_german_noun_declensions d
                            WHERE d.noun = lower(u.lemma))
           AND NOT EXISTS (SELECT 1 FROM bt_3_german_adjective_degrees a
                            WHERE a.adjective = lower(u.lemma))
         ORDER BY u.pos, u.lemma
    """ + (f" LIMIT {int(limit)}" if limit else "")
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            words = [(str(a), str(b)) for a, b in (cur.fetchall() or [])]

    stats = {"слов": len(words), "склонений": 0, "степеней": 0, "несравнимое": 0,
             "страницы нет": 0, "справочник молчал": 0}
    for start in range(0, len(words), max(1, int(batch))):
        chunk = words[start:start + max(1, int(batch))]
        sources = None
        for attempt in range(5):
            sources = fetch_sources_bulk([_reference_title(w, p) for w, p in chunk])
            if sources is not None:
                break
            time.sleep(10 * (attempt + 1))
        if sources is None:
            stats["справочник молчал"] += len(chunk)
            continue
        for word, pos in chunk:
            text = (sources.get(_reference_title(word, pos)) or sources.get(word) or "")
            if pos == "noun":
                table = declension_from_source(text)
                store_noun_declension(word, table)
                stats["склонений" if table else "страницы нет"] += 1
            else:
                degrees = degrees_from_source(text)
                store_adjective_degrees(word, degrees)
                stats["степеней" if degrees.get("gradable")
                      else "несравнимое" if degrees else "страницы нет"] += 1
        time.sleep(max(0.0, float(pause_sec)))
        if (start // max(1, int(batch))) % 10 == 0:
            logging.info("формы из справочника: прогрет %d/%d", start + len(chunk), len(words))
    return stats


def warm_with_model(*, limit: int = 0) -> dict:
    """Добить остаток моделью и ЗАПИСАТЬ ответ в кэш с пометкой происхождения.

    Выдача ходит только в кэш (в сеть и к модели на горячем пути не идём), поэтому
    ответ модели обязан туда лечь — иначе он не доедет до человека. Пометка
    «модель» хранится ВНУТРИ записи: владелец 17.08.2026 решил пользователю
    происхождение не показывать, но различать его мы обязаны.
    """
    from backend.database import get_db_connection_context
    sql = """
        SELECT u.lemma, u.pos FROM bt_3_lex_units u
         WHERE u.lang = 'de' AND u.pos IN ('noun','adjective','adverb')
           AND position(' ' in u.lemma) = 0
           AND NOT EXISTS (SELECT 1 FROM bt_3_german_noun_declensions d
                            WHERE d.noun = lower(u.lemma) AND d.documented)
           AND NOT EXISTS (SELECT 1 FROM bt_3_german_adjective_degrees a
                            WHERE a.adjective = lower(u.lemma) AND a.documented)
         ORDER BY u.pos, u.lemma
    """ + (f" LIMIT {int(limit)}" if limit else "")
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            words = [(str(a), str(b)) for a, b in (cur.fetchall() or [])]

    stats = {"слов": len(words), "композит": 0, "модель": 0, "не закрыто": 0}
    for word, pos in words:
        if pos == "noun":
            table = declension_from_compound(word)
            if table:
                store_noun_declension(word, {**{k: v for k, v in table.items() if k != "head"},
                                             "source": "правило композита",
                                             "head": table.get("head")})
                stats["композит"] += 1
                clear_unresolved(word)
                continue
            guessed = declension_from_model(word)
            if guessed:
                store_noun_declension(word, {**guessed, "source": "модель"})
                stats["модель"] += 1
                clear_unresolved(word)
                continue
        else:
            guessed = degrees_from_model(_reference_title(word, pos))
            if guessed:
                store_adjective_degrees(word, {**guessed, "source": "модель"})
                stats["модель"] += 1
                clear_unresolved(word)
                continue
        stats["не закрыто"] += 1
        mark_unresolved(word, pos, "ни справочник, ни композит, ни модель")
    return stats
