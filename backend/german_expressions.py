# -*- coding: utf-8 -*-
"""Справочник немецких УСТОЙЧИВЫХ ВЫРАЖЕНИЙ: идиомы и пословицы.

┌─ ПОВОД, владелец 09.09.2026 ────────────────────────────────────────────────────────┐
│ «А зачем нам отделять фразу от предложения? В чём разница?»                          │
│                                                                                     │
│ Разница в том, кто отвечает СВЕРХУ. Предложение переводят целиком, и правильный      │
│ ответ даёт переводчик. Выражение — словарная единица: его смысл не складывается из   │
│ слов, и машинный переводчик врёт на нём буквально. Замер 09.09.2026: DeepL перевёл   │
│ «Haare auf den Zähnen haben» как «иметь волосы на зубах».                            │
│                                                                                     │
│ До сегодня форму ввода решал СЧЁТ СЛОВ: пять слов и больше — предложение. Идиома из  │
│ пяти слов при этом получала машинный буквальный перевод крупным шрифтом.             │
│                                                                                     │
│ Так эту задачу решают словари, а не счётчики: PONS сначала ищет ввод в своём         │
│ словаре (там лежат и многословные статьи) и только потом включает перевод текста.    │
│ Этот модуль — наш такой словарь.                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘

ИСТОЧНИК ИСТИНЫ НАЗЫВАЕТСЯ ВСЛУХ: немецкий Викисловарь, категории
«Kategorie:Redewendung (Deutsch)» (2739 страниц на 09.09.2026) и
«Kategorie:Sprichwort (Deutsch)» (194 страницы), лицензия CC-BY-SA. Оттуда же берётся
НЕМЕЦКОЕ значение выражения (раздел {{Bedeutungen}}).

Русского перевода у идиом в Викисловаре нет, и мы его здесь НЕ ВЫДУМЫВАЕМ. Задача
этого справочника одна: ответить на вопрос «это устойчивое выражение или обычный
текст». Ответ «да» меняет две вещи: разбору уходит пометка «выражение» вместо
«предложение», и крупный заголовок перестаёт быть буквальным машинным переводом.
Русский смысл даёт разбор — там он строится с опорой на то же немецкое значение.

Готовые русские переводы многословных выражений у нас уже есть в двух других
источниках (bt_base_dictionary — FreeDict, 657 штук; bt_wiktionary_dictionary —
WikDict, 283 штуки), и их отдаёт слой статей (dictionary_entries.py).
"""
from __future__ import annotations

import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request

_API = "https://de.wiktionary.org/w/api.php"
_UA = "DeutschBot/1.0 (Sprachlern-App; Kontakt über Telegram)"
_CATEGORIES = (
    ("Kategorie:Redewendung (Deutsch)", "idiom"),
    ("Kategorie:Sprichwort (Deutsch)", "proverb"),
)
_FETCH_RETRIES = 5
_FETCH_BACKOFF_SECONDS = 3.0
_RETRY_CODES = {429, 500, 502, 503, 504}
_TITLES_PER_REQUEST = 50
# Пауза между запросами. Викимедиа отвечает 429 на сплошной поток (проверено
# 09.09.2026, загрузка упала на середине), и правильный ответ на это — не «повторить
# быстрее», а ходить реже: справочник грузится раз в месяц, спешить некуда.
_PAUSE_BETWEEN_REQUESTS_SEC = 1.0

_SPACE_RE = re.compile(r"\s+")
_BEDEUTUNGEN_RE = re.compile(r"\{\{Bedeutungen\}\}(.*?)(?=\n\{\{|\Z)", re.S)
_WIKI_LINK_RE = re.compile(r"\[\[(?:[^\]|]*\|)?([^\]]+)\]\]")
_TEMPLATE_RE = re.compile(r"\{\{([^}]*)\}\}")
_SENSE_MARK_RE = re.compile(r"^[:*]*\s*\[[\d,\s–-]+\]\s*")


def normalize_expression_key(text: str) -> str:
    """Ключ поиска выражения: схлопнутые пробелы, нижний регистр.

    Артикль здесь НЕ снимается, в отличие от ключа одиночного слова: «die Katze aus
    dem Sack lassen» начинается с артикля, и снять его значило бы получить другое
    выражение."""
    return _SPACE_RE.sub(" ", str(text or "").strip()).casefold()


def _clean_wiki(text: str) -> str:
    """Викиразметку — в человеческую строку. Ссылки разворачиваем в их текст, шаблоны
    вида {{ugs.}} выбрасываем вместе с содержимым: это пометки стиля, а не смысл."""
    out = _WIKI_LINK_RE.sub(r"\1", str(text or ""))
    out = _TEMPLATE_RE.sub("", out)
    out = out.replace("'''", "").replace("''", "")
    return _SPACE_RE.sub(" ", out).strip(" ;:,")


def meaning_from_wikitext(wikitext: str | None) -> str:
    """Немецкое значение выражения из раздела {{Bedeutungen}}. Пусто — значит в статье
    его нет; выдумывать нечего, и пустое значение честно уезжает в счётчик."""
    if not wikitext:
        return ""
    block = _BEDEUTUNGEN_RE.search(wikitext)
    if not block:
        return ""
    senses: list[str] = []
    for line in block.group(1).splitlines():
        line = line.strip()
        if not line or not line.startswith((":", "*")):
            continue
        line = _SENSE_MARK_RE.sub("", line)
        cleaned = _clean_wiki(line)
        if cleaned:
            senses.append(cleaned)
    return "; ".join(senses[:3])


# ── схема ─────────────────────────────────────────────────────────────────────────────

def ensure_schema() -> None:
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_german_expressions (
                    id BIGSERIAL PRIMARY KEY,
                    lemma TEXT NOT NULL,
                    lemma_key TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    meaning_de TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_bt_3_german_expressions_kind
                ON bt_3_german_expressions (kind);
            """)
        conn.commit()


# ── чтение (горячий путь) ─────────────────────────────────────────────────────────────

def expression_of(text: str) -> dict | None:
    """Статья справочника про это написание, или None — «в справочнике такого нет».

    None НЕ значит «не выражение»: справочник знает 2933 самых ходовых, а не все. Это
    честное «мы про него не знаем», и решение тогда принимает правило формы ввода.
    Сбой базы наружу НЕ ГЛУШИМ: тихое None на сбое неотличимо от «нет в справочнике»,
    а это два разных мира (правило ноль, пункт 4)."""
    key = normalize_expression_key(text)
    if not key or " " not in key:
        return None
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT lemma, kind, meaning_de, source FROM bt_3_german_expressions "
                "WHERE lemma_key = %s LIMIT 1;",
                (key,),
            )
            row = cursor.fetchone()
    if not row:
        return None
    return {"lemma": row[0], "kind": row[1], "meaning_de": row[2] or "", "source": row[3]}


def counters() -> dict[str, int]:
    """Сколько выражений в справочнике и у скольких нет немецкого значения. Второе
    число — наряд на работу, а не «и так сойдёт»."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT count(*), count(*) FILTER (WHERE meaning_de = ''), "
                "count(*) FILTER (WHERE kind = 'idiom'), count(*) FILTER (WHERE kind = 'proverb') "
                "FROM bt_3_german_expressions;"
            )
            total, without_meaning, idioms, proverbs = cursor.fetchone()
    return {"всего": int(total or 0), "без_значения": int(without_meaning or 0),
            "идиом": int(idioms or 0), "пословиц": int(proverbs or 0)}


# ── загрузка из справочника ───────────────────────────────────────────────────────────

def _api(params: dict) -> dict:
    """Один запрос к Викисловарю. Сеть либо отвечает, либо мы честно падаем."""
    query = {**params, "format": "json", "formatversion": "2"}
    url = _API + "?" + urllib.parse.urlencode(query)
    request = urllib.request.Request(url, headers={"User-Agent": _UA})
    last_error: Exception | None = None
    for attempt in range(_FETCH_RETRIES):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in _RETRY_CODES:
                raise
            # Сервер сам говорит, сколько ждать, — слушаем его, а не свой таймер.
            retry_after = 0.0
            try:
                retry_after = float((exc.headers or {}).get("Retry-After") or 0)
            except (TypeError, ValueError):
                retry_after = 0.0
            time.sleep(max(retry_after, _FETCH_BACKOFF_SECONDS * (attempt + 1) ** 2))
        except Exception as exc:
            last_error = exc
            time.sleep(_FETCH_BACKOFF_SECONDS * (attempt + 1))
    raise RuntimeError(f"справочник {_API} не ответил за {_FETCH_RETRIES} попыток: {last_error}")


def category_members(category: str) -> list[str]:
    """Все страницы категории. Продолжение (continue) обходим до конца: половина
    категории — это половина справочника, а не «достаточно»."""
    titles: list[str] = []
    cont: dict = {}
    while True:
        if cont:
            time.sleep(_PAUSE_BETWEEN_REQUESTS_SEC)
        payload = _api({"action": "query", "list": "categorymembers", "cmtitle": category,
                        "cmlimit": "500", "cmtype": "page", **cont})
        titles.extend(str(m.get("title") or "") for m in payload.get("query", {}).get("categorymembers", []))
        cont = payload.get("continue") or {}
        if not cont:
            break
    return [t for t in titles if t]


def fetch_meanings(titles: list[str]) -> dict[str, str]:
    """Немецкие значения для пачки заголовков."""
    out: dict[str, str] = {}
    for start in range(0, len(titles), _TITLES_PER_REQUEST):
        if start:
            time.sleep(_PAUSE_BETWEEN_REQUESTS_SEC)
        batch = titles[start:start + _TITLES_PER_REQUEST]
        payload = _api({"action": "query", "prop": "revisions", "rvprop": "content",
                        "rvslots": "main", "titles": "|".join(batch)})
        for page in payload.get("query", {}).get("pages", []):
            title = str(page.get("title") or "")
            if not title or page.get("missing"):
                continue
            try:
                wikitext = page["revisions"][0]["slots"]["main"]["content"]
            except (KeyError, IndexError):
                continue
            out[title] = meaning_from_wikitext(wikitext)
    return out


def load_from_wiktionary(*, only_multiword: bool = True, resume: bool = True) -> dict[str, int]:
    """Залить справочник целиком. Возвращает числа, а не «готово».

    only_multiword: односложные записи категории («aha», «hurra») нам не нужны — их и
    так знает словарь слов, а вопрос этого модуля только про многословный ввод.

    resume: пропускать написания, у которых значение уже загружено. Загрузка идёт
    ПАРТИЯМИ и пишет каждую сразу — 09.09.2026 первая попытка упала на 429 в середине
    третьей тысячи, и вся вычитанная работа пропала бы, копи мы её до конца.
    """
    ensure_schema()
    from backend.database import get_db_connection_context
    итог = {"прочитано": 0, "записано": 0, "без_значения": 0, "пропущено": 0}
    for category, kind in _CATEGORIES:
        titles = category_members(category)
        if only_multiword:
            titles = [t for t in titles if " " in t.strip()]
        итог["прочитано"] += len(titles)
        известные: set[str] = set()
        if resume:
            with get_db_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT lemma_key FROM bt_3_german_expressions WHERE meaning_de <> '';")
                    известные = {row[0] for row in cursor.fetchall()}
        todo = [t for t in titles if normalize_expression_key(t) not in известные]
        итог["пропущено"] += len(titles) - len(todo)
        for start in range(0, len(todo), _TITLES_PER_REQUEST):
            if start:
                time.sleep(_PAUSE_BETWEEN_REQUESTS_SEC)
            batch = todo[start:start + _TITLES_PER_REQUEST]
            meanings = fetch_meanings(batch)
            with get_db_connection_context() as conn:
                with conn.cursor() as cursor:
                    for title in batch:
                        meaning = meanings.get(title, "")
                        if not meaning:
                            итог["без_значения"] += 1
                        cursor.execute(
                            """
                            INSERT INTO bt_3_german_expressions (lemma, lemma_key, kind, meaning_de, source)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (lemma_key) DO UPDATE
                              SET lemma = EXCLUDED.lemma, kind = EXCLUDED.kind,
                                  meaning_de = CASE WHEN EXCLUDED.meaning_de <> '' THEN EXCLUDED.meaning_de
                                                    ELSE bt_3_german_expressions.meaning_de END,
                                  source = EXCLUDED.source, updated_at = NOW();
                            """,
                            (title, normalize_expression_key(title), kind, meaning, f"de.wiktionary:{category}"),
                        )
                        итог["записано"] += 1
                conn.commit()
            logging.info("справочник выражений: %s — %d/%d", category,
                         min(start + _TITLES_PER_REQUEST, len(todo)), len(todo))
    return итог


if __name__ == "__main__":
    print(load_from_wiktionary())
    print(counters())
