# -*- coding: utf-8 -*-
"""Регистр заголовка чинится в ОБЕ стороны, а не только вниз.

До 02.09.2026 правило `german_headword_case` умело только опускать заглавную у
глагола («Gehen» → «gehen») и НИКОГДА не поднимало её у существительного. Вторую
половину просто не построили — и в базе спокойно жили «die aufenthaltsgenehmigung»,
«der bierhausschwätzer», «das gehen». Владелец увидел их 02.09.2026 и сказал:
«достраивать вторую половину правила, чтобы существительное со строчной больше не
могло войти».

В немецком существительное со строчной — ошибка ВСЕГДА. Это языковое приложение:
что человек увидел, то он и запомнил.

Обе стороны работают только при ЯВНО названной части речи. Пустая часть речи — не
разрешение ни вверх, ни вниз: под ней прячется всё подряд, и это тот же урок, что с
русскими переводами, где проверка «а это существительное?» написала «афины».
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.german_grammar_tables import german_headword_case  # noqa: E402


def test_существительное_поднимается():
    assert german_headword_case("pflanze", "noun") == "Pflanze"
    assert german_headword_case("zwischenbescheid", "noun") == "Zwischenbescheid"


def test_артикль_внутри_заголовка_не_прячет_слово():
    """«die pflanze» — заглавную надо поднять у СЛОВА, а не у строки."""
    assert german_headword_case("die pflanze", "noun") == "die Pflanze"
    assert german_headword_case("das gehen", "noun") == "das Gehen"


def test_искалеченный_капс_приводится_к_нормальному_виду():
    """«eROBERUNG» — выделение из текста, а не немецкий язык. «EROBERUNG» не лучше."""
    assert german_headword_case("eROBERUNG", "noun") == "Eroberung"


def test_настоящая_аббревиатура_не_трогается():
    """У «CDU» и «die CDU» первая буква слова уже заглавная — правило молчит."""
    assert german_headword_case("CDU", "noun") == "CDU"
    assert german_headword_case("die CDU", "noun") == "die CDU"


def test_глагол_и_прилагательное_по_прежнему_опускаются():
    assert german_headword_case("Gehen", "verb") == "gehen"
    assert german_headword_case("Nahtlos", "adjective") == "nahtlos"
    assert german_headword_case("ERNEUERBARE", "adjective") == "erneuerbare"


def test_без_части_речи_не_трогаем_ничего():
    """Молчание о части речи — не разрешение. Ни вверх, ни вниз."""
    assert german_headword_case("pflanze", "") == "pflanze"
    assert german_headword_case("Pflanze", "") == "Pflanze"
    assert german_headword_case("gehen", None) == "gehen"


def test_пустая_строка_остаётся_пустой():
    assert german_headword_case("", "noun") == ""
    assert german_headword_case(None, "noun") == ""
