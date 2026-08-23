# -*- coding: utf-8 -*-
"""Заголовок — это СЛОВО или ФОРМА слова? И если форма, то форма чего.

ОТКУДА ЗАДАЧА. Владелец 23.08.2026, разбирая глаголы без спряжения: «конечно нужно
поменять часть речи по источнику в этих 30 словах, которые не глаголы». Когда я спросил
справочник, оказалось, что беда глубже подписи: у 21 заголовка из 30 нет своей словарной
статьи ВООБЩЕ — это причастия и личные формы: «abgezogen», «zurückgetreten», «hätte»,
«rast», «lehne». Подписать им «причастие» значит оставить человека учить форму вместо
слова. Правильный ответ по решению владельца от 18.08.2026 — «заголовок = словарная
форма»: «abgezogen» должно стать «abziehen».

ИСТОЧНИК ОТВЕЧАЕТ БЕЗ ДОГАДОК. Страница de.wiktionary помечает раздел словом `Wortart`,
и у формы там стоит «Partizip II», «Konjugierte Form», «Deklinierte Form», а рядом —
шаблон `{{Grundformverweis Konj|abziehen}}`, прямо называющий словарное слово. Ничего
выводить окончанием не нужно: и вид записи, и базовое слово НАПЕЧАТАНЫ.

    == abgezogen ({{Sprache|Deutsch}}) ==
    === {{Wortart|Partizip II|Deutsch}} ===
    * Partizip Perfekt des Verbs '''[[abziehen]]'''
    {{Grundformverweis Konj|abziehen}}

ГДЕ ИСТОЧНИК САМ НЕ ЗНАЕТ — мы тоже не знаем. У «rast» на странице ДВА раздела и два
разных базовых слова: «rasten» (иди отдыхай) и «rasen» (мчаться). Это не наша неполнота,
а настоящая двусмысленность немецкого, и решать её обязан человек. Такое уходит владельцу
кнопками, а не подставляется первым из списка.
"""
from __future__ import annotations

import logging
import re

# Разделы страницы, у которых есть СВОЯ словарная статья. Значение — наша часть речи.
# Разновидности наречия сведены к «adverb»: в продукте одно слово, а не четыре.
POS_BY_WORTART = {
    "Substantiv": "noun",
    "Verb": "verb",
    "Hilfsverb": "verb",
    "Adjektiv": "adjective",
    "Adverb": "adverb",
    "Temporaladverb": "adverb",
    "Lokaladverb": "adverb",
    "Modaladverb": "adverb",
    "Konjunktionaladverb": "adverb",
    "Pronominaladverb": "adverb",
    "Interrogativadverb": "adverb",
    "Präposition": "preposition",
    "Konjunktion": "conjunction",
    "Subjunktion": "conjunction",
    "Pronomen": "pronoun",
    "Personalpronomen": "pronoun",
    "Possessivpronomen": "pronoun",
    "Demonstrativpronomen": "pronoun",
    "Interjektion": "interjection",
    "Numerale": "numeral",
    "Artikel": "article",
    "Redewendung": "phrase",
    "Sprichwort": "phrase",
}

# Разделы, которые означают «это форма другого слова, своей статьи нет».
# Значение — часть речи ТОГО САМОГО слова, формой которого заголовок является: причастие
# и личная форма бывают только у глагола, и это знание печатное, а не выведенное.
# У «Deklinierte Form» базовым может быть и существительное, и прилагательное — там
# пусто, и часть речи мы не трогаем.
FORM_WORTART = {
    "Partizip I": "verb",
    "Partizip II": "verb",
    "Konjugierte Form": "verb",
    "Erweiterter Infinitiv": "verb",
    "Deklinierte Form": "",
    "Komparativ": "",
    "Superlativ": "",
    "Grundformverweis": "",
}

# Ссылка на словарное слово. У формы она стоит явным шаблоном; вариантов написания
# несколько («Grundformverweis Konj», «Grundformverweis Dekl», просто «Grundformverweis»),
# и последний параметр шаблона — само слово.
_GRUNDFORM = re.compile(r"\{\{Grundformverweis[^|}]*\|(?:[^|}]*\|)*([^|}]+)\}\}")
# Запасная запись того же факта прозой: «Partizip Perfekt des Verbs '''[[abziehen]]'''».
_DES_VERBS = re.compile(r"des (?:Verbs|Substantivs|Adjektivs)\s*'{0,3}\[\[([^\]|]+)")
_WORTART = re.compile(r"\{\{Wortart\|([^|}]+)")
# Немецкий раздел страницы: на одной странице живут разные языки, и брать чужой нельзя.
_DE_SECTION = re.compile(
    r"^==\s*[^=\n]*\(\{\{Sprache\|Deutsch\}\}\)\s*==(.*?)(?=^==[^=]|\Z)",
    re.DOTALL | re.MULTILINE)


def german_section(wikitext: str | None) -> str:
    """Только немецкая часть страницы. Пусто — немецкого раздела на странице нет."""
    found = _DE_SECTION.search(str(wikitext or ""))
    return found.group(1) if found else ""


def headword_kind(wikitext: str | None) -> dict:
    """Что справочник говорит о заголовке.

    Возвращает {'kind', 'pos', 'bases', 'wortarten'}:
      kind='word' — есть своя словарная статья, 'pos' — наша часть речи;
      kind='form' — только формы, 'bases' — словарные слова, названные страницей,
      'pos' — часть речи базового слова, когда вид формы её называет однозначно;
      kind='unknown' — немецкого раздела нет или разделы нам незнакомы. Это НЕ «слово
      плохое», это «мы не знаем»: подставлять сюда догадку запрещено.
    """
    text = german_section(wikitext)
    if not text:
        return {"kind": "unknown", "pos": "", "bases": [], "wortarten": []}
    wortarten = [w.strip() for w in _WORTART.findall(text)]
    real = [POS_BY_WORTART[w] for w in wortarten if w in POS_BY_WORTART]
    if real:
        # У страницы есть своя статья. Если разделов несколько (у «gefeiert» —
        # «Adjektiv» и «Partizip II»), берём словарную: форма живёт рядом, но
        # заголовок стоит в словаре по праву.
        return {"kind": "word", "pos": real[0], "bases": [], "wortarten": wortarten}
    if not any(w in FORM_WORTART for w in wortarten):
        return {"kind": "unknown", "pos": "", "bases": [], "wortarten": wortarten}
    base_pos = next((FORM_WORTART[w] for w in wortarten
                     if FORM_WORTART.get(w)), "")
    bases: list[str] = []
    for found in _GRUNDFORM.findall(text) + _DES_VERBS.findall(text):
        base = str(found or "").strip()
        if base and base not in bases:
            bases.append(base)
    return {"kind": "form", "pos": base_pos, "bases": bases, "wortarten": wortarten}


def headword_kinds(words: list[str]) -> dict[str, dict]:
    """То же для пачки заголовков. Слово, о котором справочник промолчал, в ответ НЕ
    попадает: «не спросили» и «спросили, ответа нет» — разные вещи."""
    from backend.article_headword import _fetch_wikitext

    asked = [str(w or "").strip() for w in words if str(w or "").strip()]
    if not asked:
        return {}
    try:
        pages = _fetch_wikitext(asked)
    except Exception:
        logging.warning("вид заголовка: справочник не ответил", exc_info=True)
        return {}
    out: dict[str, dict] = {}
    for word in asked:
        text = pages.get(word)
        if text is None:
            continue          # страницы нет — это ответ, но не наш случай
        out[word] = headword_kind(text)
    return out
