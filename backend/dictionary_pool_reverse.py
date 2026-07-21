"""Обратное сопоставление в общем пуле слов.

Задача: слово, попавшее к нам как ЦЕЛЬ перевода (запись «слияние → die Fusion», пара
ru→de), должно находиться и при запросе в обратную сторону («die Fusion», пара de→ru).
Замер на 2026-07-21: так достижимы 5470 немецких слов, недоступных прямым поиском.

Почему нельзя просто поменять поля местами: часть карточки ИНВАРИАНТНА к направлению
(немецкое слово, артикль, формы, немецкие примеры и синонимы — немецкий и там и там
изучаемый язык), а часть — НАПРАВЛЕННАЯ: `dictionary_senses`/`meanings`/`translations`
для ru→de содержат НЕМЕЦКИЕ значения. Перенести их в карточку de→ru — это ровно тот баг,
когда в графе перевода стоит немецкий. Поэтому здесь белый список: переносим только то,
про что мы точно знаем, что оно про немецкое слово, а перевод берём из отдельной колонки
записи, где он лежит настоящим, а не выведенным.

Модуль намеренно без тяжёлых зависимостей — чтобы его можно было прогонять офлайн по
живым данным до включения в проде.
"""

from __future__ import annotations

import re

# Факты о НЕМЕЦКОМ слове: одинаковы в обе стороны (немецкий — изучаемый язык всегда).
# usage_examples сюда НЕ входят: они хранятся как {source, target} и при развороте
# направления их надо переворачивать, а не копировать (см. _flip_examples).
INVARIANT_KEYS = frozenset({
    "article",
    "forms",
    "part_of_speech",
    "is_separable",
    "prefixes",
    "prefix",
    "base_verb",
    "pronunciation",
    "government_patterns",
    "grammar_tables",
    "word_formation",
    "common_collocations",
    "synonyms",
    "antonyms",
    "related_words",
    "false_friends",
    "frequency",
    "level",
    "register",
    "phrase_kind",
    "is_base_dict",
    "wikt_fetched",
})

# Пояснения, написанные на РОДНОМ языке ученика. Переносим только если родной язык
# записи совпадает с родным языком запрашиваемого направления, иначе получим текст не на
# том языке.
EXPLANATION_KEYS = frozenset({
    "memory_tip",
    "etymology_note",
    "usage_note",
    "when_to_use",
    "connotation",
    "common_mistakes",
    "synonym_differences",
    "real_life_usage",
    "expression_note",
    "register_note",
    "part_of_speech_note",
    "literal_meaning",
})

_CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")


def _clean(value) -> str:
    return str(value or "").strip()


def _looks_like_lang(text: str, lang: str) -> bool:
    """Грубая проверка языка строки — защита от того, чтобы немецкое слово не уехало в
    русскую графу и наоборот. Для языков вне ru мы проверять не умеем и не мешаем."""
    has_cyrillic = bool(_CYRILLIC_RE.search(text))
    if lang == "ru":
        return has_cyrillic
    if lang == "de":
        return not has_cyrillic
    return True


_BILINGUAL_SEPARATORS = (" — ", " – ", " -- ", " → ", " | ")


def split_bilingual(text: str, want_lang: str) -> str:
    """В базе есть склейки вида «Das erfordert … — Это требует …» (см. запись 32681).
    Достаём ту половину, которая на нужном языке. Если склейки нет — возвращаем как есть."""
    raw = _clean(text)
    if not raw or want_lang not in {"ru", "de"}:
        return raw
    for sep in _BILINGUAL_SEPARATORS:
        if sep not in raw:
            continue
        left, _, right = raw.partition(sep)
        left, right = _clean(left), _clean(right)
        if not left or not right:
            continue
        if _looks_like_lang(left, want_lang) != _looks_like_lang(right, want_lang):
            return left if _looks_like_lang(left, want_lang) else right
    return raw


def _flip_examples(examples, *, want_source_lang: str, want_target_lang: str) -> list:
    """Примеры направленные: {source: русский, target: немецкий} для ru→de. При развороте
    стороны меняются местами — иначе в карточке de→ru «исходным» примером окажется
    русское предложение. Голые строки (просто немецкий текст) переносим как есть."""
    if not isinstance(examples, list):
        return []
    out: list = []
    for example in examples:
        if isinstance(example, str):
            text = _clean(example)
            if text and _looks_like_lang(text, want_source_lang):
                out.append(text)
            continue
        if not isinstance(example, dict):
            continue
        src = _clean(example.get("source") or example.get("example_source"))
        tgt = _clean(example.get("target") or example.get("example_target"))
        if not src and not tgt:
            continue
        # После разворота: то, что было целью, становится источником.
        new_source, new_target = tgt, src
        if new_source and not _looks_like_lang(new_source, want_source_lang):
            continue
        if new_target and not _looks_like_lang(new_target, want_target_lang):
            continue
        if not new_source:
            continue
        out.append({"source": new_source, "target": new_target})
    return out


def build_reverse_pool_item(
    row: dict,
    *,
    want_source_lang: str,
    want_target_lang: str,
) -> dict | None:
    """Собрать карточку нужного направления из записи пула, лежащей в обратном.

    Возвращает None, если запись не подходит (не то направление, нет текстов, язык не
    сходится) — вызывающая сторона тогда просто идёт дальше, как будто в пуле пусто.

    ВАЖНО: результат почти всегда получается НЕПОЛНЫМ (один смысл без примеров) — это
    нормально и задумано: его отдают мгновенно как предварительный, а следом идёт ОДИН
    вызов дообогащения вместо полного поиска."""
    if not isinstance(row, dict):
        return None
    row_source_lang = _clean(row.get("source_lang")).lower()
    row_target_lang = _clean(row.get("target_lang")).lower()
    want_source_lang = _clean(want_source_lang).lower()
    want_target_lang = _clean(want_target_lang).lower()
    if not want_source_lang or not want_target_lang:
        return None
    # Работаем только с честным разворотом: запись ровно противоположного направления.
    if row_source_lang != want_target_lang or row_target_lang != want_source_lang:
        return None

    payload = row.get("response_json") if isinstance(row.get("response_json"), dict) else {}
    # Заголовок нового направления = цель старого; перевод = источник старого. Берём из
    # колонок записи (там настоящие значения), payload — только как запасной вариант.
    headword = _clean(row.get("target_text")) or _clean(payload.get("target_text"))
    gloss = _clean(row.get("source_text")) or _clean(payload.get("source_text"))
    if want_source_lang == "de":
        headword = headword or _clean(row.get("word_de")) or _clean(payload.get("word_de"))
    if want_target_lang == "ru":
        gloss = gloss or _clean(row.get("word_ru")) or _clean(payload.get("word_ru"))
    # Расклеиваем возможную двуязычную склейку до проверки языка.
    headword = split_bilingual(headword, want_source_lang)
    gloss = split_bilingual(gloss, want_target_lang)
    if not headword or not gloss:
        return None
    if not _looks_like_lang(headword, want_source_lang) or not _looks_like_lang(gloss, want_target_lang):
        return None
    if headword.casefold() == gloss.casefold():
        return None  # битая запись: обе стороны одинаковые

    item: dict = {}
    for key in INVARIANT_KEYS:
        if key in payload and payload.get(key) not in (None, "", [], {}):
            item[key] = payload[key]
    # Пояснения переносим, только если родной язык не поменялся.
    if row_source_lang == want_target_lang:
        for key in EXPLANATION_KEYS:
            if key in payload and payload.get(key) not in (None, "", [], {}):
                item[key] = payload[key]

    flipped_examples = _flip_examples(
        payload.get("usage_examples"),
        want_source_lang=want_source_lang,
        want_target_lang=want_target_lang,
    )
    if flipped_examples:
        item["usage_examples"] = flipped_examples

    item.update({
        "source_lang": want_source_lang,
        "target_lang": want_target_lang,
        "source_text": headword,
        "target_text": gloss,
        "language_pair": {
            "code": f"{want_source_lang}-{want_target_lang}",
            "source_lang": want_source_lang,
            "target_lang": want_target_lang,
        },
        # Один достоверный смысл — сам перевод. Немецкие значения из старого направления
        # сюда НЕ переносятся: они относятся к русскому заголовку, а не к немецкому.
        "dictionary_senses": [{
            "rank": 1,
            "label": "main",
            "value": gloss,
            "context": "",
            "example_source": "",
            "example_target": "",
        }],
        "__reverse_of_entry_id": row.get("id"),
    })
    if want_source_lang == "de":
        item["word_de"] = headword
        item["translation_de"] = headword
    if want_target_lang == "ru":
        item["word_ru"] = gloss
        item["translation_ru"] = gloss
    return item
