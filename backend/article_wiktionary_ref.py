"""
German Wiktionary genus reference — authoritative der/die/das lookup for the
Artikel Sprint correctness audit.

For a noun we ask de.wiktionary.org for the page wikitext, isolate the German
language section, and read the grammatical genus (Genus=m/f/n) from the noun
template. Multiple distinct genera → the word itself is two-gender (ambiguous).
When a compound has no direct entry we decompose it: the head (Grundwort) is the
longest trailing substring that is itself a German noun with a single genus, and
the compound inherits that genus (der Wert → der Börsenwert).

Results are cached per title in bt_3_wiktionary_genus_cache so repeat runs and the
apply-fix pass never re-hit the network. Genus codes stored: 'm'/'f'/'n' (single),
'x' (two-gender), '-' (page missing or no noun genus).
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
_UA = "DeutschBot/1.0 (Artikel Sprint article audit; contact via Telegram bot)"
_BATCH = 45  # MediaWiki allows 50 titles/query; keep margin.
# 429 — «слишком часто», 503 — «сейчас занято»: и то и другое проходит само, надо лишь
# подождать. Отступ растёт с попыткой (8, 16, 24 с), потолок держим низким: аудит
# админский и не должен висеть минутами.
_RETRY_CODES = (429, 503)
_FETCH_RETRIES = 4
_FETCH_BACKOFF_SECONDS = 8

_GENUS_TO_ARTICLE = {"m": "der", "f": "die", "n": "das"}

# German section header, e.g. "== Börsenwert ({{Sprache|Deutsch}}) =="
_DE_HEADER = re.compile(r"==\s*[^=]*\(\{\{Sprache\|Deutsch\}\}\)\s*==")
_NEXT_LANG = re.compile(r"\n==\s*[^=].*\(\{\{Sprache\|")
# Noun overview templates carry the canonical grammatical gender(s). Reading genus
# only from these (not stray page text) avoids picking up unrelated mentions.
_OVERVIEW = re.compile(r"\{\{Deutsch[- ]Substantiv[- ]Übersicht[^}]*\}\}", re.DOTALL)
_GENUS = re.compile(r"\bGenus\s*\d*\s*=\s*([mfn])\b")


def _genera_from_wikitext(wikitext: str) -> str:
    """Sorted genus letters documented for the German noun, e.g. 'm', 'fm', 'fn';
    '-' if none. A word with several documented genera (regional/rare variants or
    genuinely two-gender) yields more than one letter — the audit treats a stored
    article as correct if it's among them."""
    m = _DE_HEADER.search(wikitext)
    blk = wikitext[m.end():] if m else None
    if blk is not None:
        nxt = _NEXT_LANG.search(blk)
        if nxt:
            blk = blk[:nxt.start()]
    if blk is None:
        return "-"
    genera: set[str] = set()
    for tmpl in _OVERVIEW.findall(blk):
        genera.update(_GENUS.findall(tmpl))
    if not genera:
        return "-"
    return "".join(sorted(genera))


def genera_to_articles(code: str | None) -> set:
    """'fm' → {'die','der'}; '-'/'' → set()."""
    return {_GENUS_TO_ARTICLE[c] for c in str(code or "") if c in _GENUS_TO_ARTICLE}


def _fetch_wikitext(titles: list[str]) -> dict[str, str | None]:
    """Raw network fetch → {title: wikitext}. Missing page → None. Never raises;
    on error returns {} so the caller treats the whole batch as unanswered."""
    params = {
        "action": "query", "format": "json", "formatversion": "2",
        "prop": "revisions", "rvprop": "content", "rvslots": "main",
        "redirects": "1", "titles": "|".join(titles),
    }
    url = _API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    # Справочник отбивает частые запросы кодом 429, а аудит идёт по тысячам слов —
    # значит, упирается в лимит почти наверняка. Без повтора это выглядит не как
    # авария, а как «Wiktionary не знает рода»: пустой ответ доходит до аудита, слова
    # уезжают в корзину «рода не знает никто», и отчёт бодро врёт. Ждём и повторяем.
    data = None
    for attempt in range(1, _FETCH_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
            break
        except urllib.error.HTTPError as exc:
            if exc.code not in _RETRY_CODES or attempt == _FETCH_RETRIES:
                logging.warning("wiktionary fetch failed HTTP %s (%d titles)", exc.code, len(titles))
                return {}
            wait = _FETCH_BACKOFF_SECONDS * attempt
            logging.warning("wiktionary HTTP %s, ждём %d сек (попытка %d из %d)",
                            exc.code, wait, attempt, _FETCH_RETRIES)
            time.sleep(wait)
        except Exception:
            logging.warning("wiktionary fetch failed (%d titles)", len(titles), exc_info=True)
            return {}
    if data is None:
        return {}

    out: dict[str, str | None] = {}
    # Map any redirect normalizations back so we key on the requested title.
    query = data.get("query") or {}
    for p in query.get("pages") or []:
        title = str(p.get("title") or "")
        if p.get("missing"):
            out[title] = None
            continue
        try:
            out[title] = p["revisions"][0]["slots"]["main"]["content"]
        except (KeyError, IndexError, TypeError):
            out[title] = None
    return out


def _api_fetch(titles: list[str]) -> dict[str, str]:
    """Raw network fetch → {title: genus}. Missing pages map to '-'. Never raises;
    on error returns {} so the caller treats those titles as unknown this run."""
    pages = _fetch_wikitext(titles)
    return {title: (_genera_from_wikitext(text) if text else "-")
            for title, text in pages.items()}


# ── Число и лемма: «это слово или его форма» ──────────────────────────────────
# Страница склонённой формы устроена однозначно и разбирается без догадок:
#   === {{Wortart|Deklinierte Form|Deutsch}} ===
#   {{Grammatische Merkmale}}
#   *Nominativ Plural des Substantivs '''[[Problem]]'''
# Страница слова, наоборот, несёт шапку {{Deutsch Substantiv Übersicht}}, откуда
# заодно бесплатно достаётся его множественное число.
_DECLINED_HEAD = re.compile(r"\{\{Wortart\|Deklinierte Form\|Deutsch\}\}")
_GRAM_LINE = re.compile(
    r"^\*\s*(Nominativ|Genitiv|Dativ|Akkusativ)\s+(Singular|Plural)\s+des\s+Substantivs\s+"
    r"'''\[\[([^\]|]+)",
    re.MULTILINE,
)
_GRUNDFORM = re.compile(r"\{\{Grundformverweis(?:\s+Dekl)?\|([^}|]+)")
# Поля шапки. У двуродовых слов Wiktionary НУМЕРУЕТ их («Nominativ Singular 1=Spind»,
# «Nominativ Singular 2=Spind»), поэтому номер обязателен в шаблоне: без него разбор
# не находил единственного числа и объявлял «der Spind», «das Versäumnis», «das Zubehör»
# словами, живущими только во множественном.
_UEBERSICHT_FIELD = re.compile(
    r"\|\s*Nominativ\s+(Singular|Plural)\s*\d*\*?\s*=\s*([^\n|}]*)"
)
_EMPTY_FIELD_VALUES = {"", "—", "-", "–", "?"}


def _german_section(wikitext: str) -> str:
    """Немецкая часть страницы: на одном заголовке живут и другие языки."""
    text = str(wikitext or "")
    head = _DE_HEADER.search(text)
    block = text[head.end():] if head else text
    nxt = _NEXT_LANG.search(block)
    return block[:nxt.start()] if nxt else block


def form_facts_from_wikitext(wikitext: str) -> dict:
    """Что страница говорит о числе: {"is_lemma","number","lemma","plural_surface"}.

    `number` заполняется ТОЛЬКО когда все грамматические строки согласны между собой:
    «Kindern» — везде Plural, а вот форма, которая читается и как единственное, и как
    множественное, оставляется без ответа. Догадываться здесь нельзя: этот индекс потом
    решает, печатать ли артикль.
    """
    block = _german_section(wikitext)
    is_lemma = bool(_OVERVIEW.search(block))
    facts: dict = {"is_lemma": is_lemma, "number": "", "lemma": "", "plural_surface": ""}

    if is_lemma:
        singulars, plurals = [], []
        for kind, value in _UEBERSICHT_FIELD.findall(block):
            (singulars if kind == "Singular" else plurals).append(value.strip())
        plural = next((p for p in plurals if p not in _EMPTY_FIELD_VALUES), "")
        if plural:
            facts["plural_surface"] = plural
        # «Живёт только во множественном» (Eltern, Magenbeschwerden) — это когда графа
        # единственного ЕСТЬ и в ней прочерк. Отсутствие графы ничего не доказывает:
        # у двуродовых слов она называется иначе, и молчать здесь безопаснее.
        if plural and singulars and all(s in _EMPTY_FIELD_VALUES for s in singulars):
            facts["number"] = "pl"
        return facts

    if not _DECLINED_HEAD.search(block):
        return facts

    numbers = {m[1] for m in _GRAM_LINE.findall(block)}
    lemmas = {m[2].strip() for m in _GRAM_LINE.findall(block)}
    if len(numbers) == 1:
        facts["number"] = "pl" if numbers.pop() == "Plural" else "sg"
    if len(lemmas) == 1:
        facts["lemma"] = lemmas.pop()
    if not facts["lemma"]:
        ref = _GRUNDFORM.search(block)
        if ref:
            facts["lemma"] = ref.group(1).strip()
    return facts


def form_facts_for_titles(titles: list[str]) -> dict[str, dict]:
    """Сетевой разбор пачки страниц → {title: form_facts}. Пустой ответ на пачку
    (сеть или 429) возвращается как {} — вызывающий отступает и не помечает слова."""
    pages = _fetch_wikitext([str(t).strip() for t in titles if str(t).strip()])
    return {title: form_facts_from_wikitext(text)
            for title, text in pages.items() if text}


def genus_for_titles(titles: list[str]) -> dict[str, str]:
    """Cache-aware genus lookup for a set of exact page titles.
    Returns {title: genus-code} — sorted genus letters ('m','fm',…) or '-' — for
    every title we could resolve (cached or fetched). Failed fetches are omitted."""
    from backend.database import (
        ensure_wiktionary_genus_cache_schema, get_cached_genus, upsert_genus_cache,
    )
    uniq = sorted({str(t).strip() for t in titles if t and str(t).strip()})
    if not uniq:
        return {}
    ensure_wiktionary_genus_cache_schema()
    result = dict(get_cached_genus(uniq))
    missing = [t for t in uniq if t not in result]

    fresh: dict[str, str] = {}
    for i in range(0, len(missing), _BATCH):
        batch = missing[i:i + _BATCH]
        got = _api_fetch(batch)
        if not got:
            # ВЕСЬ запрос не удался (сеть или HTTP 429) — ответа не было вовсе.
            # Раньше здесь всё равно проставлялось '-' («страницы нет»), и один
            # упор в лимит навсегда помечал сотни настоящих слов несуществующими:
            # больше их никто не переспрашивал, а арбитр рода вечно отвечал «не знаю».
            # Ничего не записываем и прекращаем проход — переспросим в следующий раз.
            logging.warning("wiktionary genus: пачка не ответила (%d слов), "
                            "прекращаем и НЕ помечаем их как отсутствующие", len(batch))
            break
        # Титул, которого нет в ответе на УСПЕШНЫЙ запрос, — действительно без страницы.
        for t in batch:
            if t not in got:
                got[t] = "-"
        fresh.update(got)
        time.sleep(0.1)  # be polite to the API
    if fresh:
        upsert_genus_cache(fresh)
        result.update(fresh)
    return result


def reference_articles(words: list[str]) -> dict[str, dict]:
    """Authoritative verdict per word from the word's OWN Wiktionary page.
    Returns {word: {"articles": set[str], "code": str, "basis": str}} where
    `articles` is the SET of documented genders (empty if the page has none) and
    `basis` ∈ {"direct", "none"}. The audit accepts a stored article that is among
    `articles`, and only proposes a fix when a single gender is documented.

    Deliberately NO compound-head decomposition: naive suffix matching grabs
    garbage substrings (Betriebs‑kosten→"Osten", Kassen‑bereich→"Reich") and can't
    handle plurale-tantum, producing FALSE corrections. Words with no direct entry
    fall to the curated deterministic guard (audit layer) or 'unknown'."""
    words = [str(w).strip() for w in words if str(w).strip()]
    titles = {w: (w[:1].upper() + w[1:]) for w in words}
    genus = genus_for_titles(list(set(titles.values())))

    out: dict[str, dict] = {}
    for w in words:
        code = genus.get(titles[w], "-")
        arts = genera_to_articles(code)
        out[w] = {"articles": arts, "code": code, "basis": "direct" if arts else "none"}
    return out
