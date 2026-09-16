# -*- coding: utf-8 -*-
"""Артикль головному слову группы «сущ. + родительный» — и НИЧЕМУ больше.

Класс, а не случай. Владелец 15.09.2026 прислал одну карточку «Vollstrecker einer
Anordnung», но чинится разнобой целиком: 45 карточек и 37 записей пула без артикля
против 88 и 57 с артиклем в той же конструкции.

Тест держит ОБЕ границы, и вторая важнее первой:
  • группа «сущ. + родительный» артикль получает;
  • всё остальное НЕ трогается — обороты с глаголом, предлоги с заглавной, фразы с
    прилагательным, предложения, и всё, чего не знает справочник склонений.
"""
from __future__ import annotations

import pytest

from backend.genitive_phrase_article import (
    head_noun_of_genitive_phrase,
    headword_with_genitive_article,
)
from backend.noun_declension_reference import plural_verdict


def _tables(gender: str, nominative: str, *, plural: str = "") -> dict:
    """Кусок справочника склонений в том виде, в каком его читает
    backend/noun_declension_reference.py — чтобы проверить правило без базы."""
    return {gender: {"has_singular": True, "has_plural": bool(plural),
                     "rows": [{"case": "nom", "singular": f"– {nominative}".replace("– ", ""),
                               "plural": plural}]}}


# ── форма строки: что вообще считается группой «сущ. + родительный» ──────────────────

@pytest.mark.parametrize("text, head", [
    ("Vollstrecker einer Anordnung", "Vollstrecker"),      # жалоба владельца 15.09.2026
    ("Bestandsaufnahme der Situation", "Bestandsaufnahme"),  # разошлась по 10 людям
    ("Robustheit des Systems", "Robustheit"),
    ("Opfer einer Entführung", "Opfer"),
    ("Stadt der Sünde", "Stadt"),
    ("Verletzung des Schutzes personenbezogener Daten", "Verletzung"),
    ("Besprechung der Pläne für das Wochenende", "Besprechung"),
])
def test_группа_сущ_плюс_родительный_опознана(text, head):
    assert head_noun_of_genitive_phrase(text) == head


@pytest.mark.parametrize("text, почему", [
    ("der Vollstrecker der Strafe", "артикль уже стоит — чинить нечего"),
    ("Die Fälschung von Daten", "артикль уже стоит, пусть и с заглавной"),
    ("Kontrolle verlieren", "сущ. + глагол: оборот, артикль ему не нужен"),
    ("Fehler begehen", "сущ. + глагол"),
    ("in Kraft treten", "начинается с предлога строчной буквой"),
    ("sich Mühe geben", "начинается с местоимения"),
    ("Schritt für Schritt", "второе слово не родительный артикль"),
    ("Hand in Hand", "второе слово не родительный артикль"),
    ("Unter der Woche", "предлог с заглавной; справочник знает «der Unter» как фигуру"),
    ("Aus der Liste streichen", "предлог с заглавной И глагол в конце"),
    ("Von einer Einnahmequelle abhängig sein", "предлог с заглавной И глагол в конце"),
    ("Mitglied der Gewerkschaft werden", "кончается глаголом — это не именная группа"),
    ("Adriatisches Meer", "прилагательное впереди: артикль сменил бы его окончание"),
    ("Ende gut, alles gut", "второе слово не родительный артикль"),
    ("Stadt der Sünde.", "знак конца предложения — это не словарный заголовок"),
    ("Opfer einer", "слов меньше трёх"),
])
def test_чужие_строки_не_трогаются(text, почему):
    assert head_noun_of_genitive_phrase(text) == "", почему


# ── две ошибки, найденные сухим прогоном 16.09.2026 на живой базе ────────────────────
#
# Обе прошли ВСЕ прежние условия и были бы записаны людям в карточки. Держатся отдельным
# тестом, потому что это два разных класса, и каждый вернётся своей дорогой.

@pytest.mark.parametrize("text, почему", [
    # 1. Предлоги, управляющие родительным. Стоят там же, где головное существительное,
    #    и ровно перед родительным падежом — то есть попадают под условие точь-в-точь.
    ("Laut eines Zeitungsberichts", "«согласно сообщению», а не «звук сообщения»"),
    ("Angesichts der Lage", "предлог родительного"),
    ("Infolge der steigenden Arbeitslosigkeit", "предлог родительного"),
    ("Oberhalb der Waldgrenze", "предлог родительного"),
    ("Ungeachtet der Schwere der Vorwürfe", "предлог родительного"),
    ("Vonseiten der Regierung", "предлог родительного"),
    ("Entlang des Stadions", "предлог родительного"),
    ("Während der Sitzung", "предлог родительного"),
    ("Innerhalb der Frist", "предлог родительного"),
    ("Aufgrund des Wetters", "предлог родительного"),
    ("Mithilfe der Nachbarn", "предлог родительного"),
    ("Zugunsten der Kinder", "предлог родительного"),
    # 2. Разделительный оборот «eines der …»: третьим словом стоит ещё один артикль.
    ("Streich eines der beiden Wörter", "повелительное от streichen, а не «проделка»"),
    ("Eines der beiden Wörter", "начинается с артикля"),
])
def test_самозванцы_из_прогона_16_09_не_получают_артикль(text, почему):
    assert head_noun_of_genitive_phrase(text) == "", почему


# ── артикль берётся из справочника, а отказ справочника остаётся отказом ─────────────

def test_артикль_приписан_из_справочника():
    заголовок, источник = headword_with_genitive_article(
        "Vollstrecker einer Anordnung", tables=_tables("m", "Vollstrecker"))
    assert заголовок == "der Vollstrecker einer Anordnung"
    assert источник == "справочник склонений"


def test_средний_род_тоже_из_справочника():
    заголовок, _ = headword_with_genitive_article(
        "Opfer einer Entführung", tables=_tables("n", "Opfer"))
    assert заголовок == "das Opfer einer Entführung"


def test_справочник_не_знает_слова_ничего_не_приписываем():
    заголовок, причина = headword_with_genitive_article(
        "Vollstrecker einer Anordnung", tables={})
    assert заголовок == "Vollstrecker einer Anordnung"
    assert "не знает слова" in причина


def test_два_рода_выбирать_не_наше_дело():
    таблицы = {"m": _tables("m", "Liter")["m"], "n": _tables("n", "Liter")["n"]}
    заголовок, причина = headword_with_genitive_article("Liter des Wassers", tables=таблицы)
    assert заголовок == "Liter des Wassers"
    assert "несколько родов" in причина


def test_форма_множественного_не_получает_артикль_единственного():
    """ПЕРВАЯ ступень обязана молчать на форме множественного — иначе «das Bücher».

    ┌─ Ожидание переписано 16.09.2026, и вот почему. ─────────────────────────────────┐
    │ До второй ступени лестницы этот тест требовал, чтобы у «Truppen des Gegners» НЕ  │
    │ было артикля вовсе. Тогда это было верно: знать про множественное нам было       │
    │ неоткуда, и молчание было единственным честным исходом. Теперь указатель         │
    │ множественного отвечает, и «die Truppen des Gegners» — правильный немецкий.      │
    │ Ожидание было про НЕДОСТАЮЩИЙ ИСТОЧНИК, а не про грамматику; источник появился.  │
    │ Что тест обязан держать дальше: артикль ЕДИНСТВЕННОГО на форму множественного не │
    │ навязывается. Поэтому слово взято среднего рода — у него «das Buch», и ошибка,   │
    │ будь она, выглядела бы как «das Bücher», а не пряталась бы за совпадением «die». │
    └─────────────────────────────────────────────────────────────────────────────────┘
    """
    заголовок, причина = headword_with_genitive_article(
        "Bücher des Lehrers", tables=_tables("n", "Buch", plural="Bücher"))
    assert заголовок == "Bücher des Lehrers", "das Bücher — так нельзя"
    assert "не именительный единственного" in причина


def test_на_форме_множественного_отвечает_вторая_ступень():
    """Та же строка, но со второй ступенью: артикль множественного, а не рода слова."""
    заголовок, источник = headword_with_genitive_article(
        "Bücher des Lehrers", tables=_tables("n", "Buch", plural="Bücher"),
        plural_verdict=("die", "справочник склонений: форма множественного от «buch»"))
    assert заголовок == "die Bücher des Lehrers"
    assert "форма множественного" in источник


def test_заданные_таблицы_означают_в_базу_не_ходить():
    """Прогон тестов не имеет права молча уйти в базу: в окружении разработчика она
    БОЕВАЯ (backend/tests/conftest.py). Поймано 16.09.2026 — тест вернул ответ, которого
    в его собственных данных не было."""
    заголовок, причина = headword_with_genitive_article(
        "Bücher des Lehrers", tables=_tables("n", "Buch", plural="Bücher"))
    assert заголовок == "Bücher des Lehrers"
    assert "не именительный единственного" in причина


# ── вторая ступень лестницы: форма множественного ────────────────────────────────────
#
# Замер 16.09.2026: закрывает 8 заголовков из 12, на которых первая ступень молчала.
# Работает потому, что у множественного числа определённый артикль ВСЕГДА «die» —
# независимо от рода. Вопрос к источнику здесь не «какой артикль», а «это множественное
# или нет», и ответ на него НАПЕЧАТАН в таблице склонения.

def test_форма_множественного_получает_die():
    от_женского = plural_verdict("Forderungen", plural_of=[("forderung", "f")],
                                 also_singular=False)
    от_среднего = plural_verdict("Teile", plural_of=[("teil", "n")], also_singular=False)
    assert от_женского[0] == "die", "die Forderungen"
    assert от_среднего[0] == "die", "die Teile — род среднего слова на артикль не влияет"
    assert "форма множественного" in от_женского[1]


def test_и_множественное_и_единственное_выбирает_человек():
    """Два ЗАКОННЫХ прочтения — решает человек, а не наш вес или частота.
    Правило владельца 26.08.2026."""
    артикль, причина = plural_verdict("Teile", plural_of=[("teil", "n")],
                                      also_singular=True)
    assert артикль == ""
    assert "два прочтения" in причина


def test_не_числится_множественным_остаётся_не_знаем():
    артикль, причина = plural_verdict("Buntheit", plural_of=[], also_singular=False)
    assert артикль == ""
    assert "не знает" in причина


def test_лестница_спускается_ко_второй_ступени():
    """Первая ступень молчит (таблиц нет), вторая отвечает — заголовок чинится."""
    заголовок, источник = headword_with_genitive_article(
        "Forderungen des Gläubigers", tables={},
        plural_verdict=("die", "справочник склонений: форма множественного от «forderung»"))
    assert заголовок == "die Forderungen des Gläubigers"
    assert "форма множественного" in источник


def test_обе_ступени_молчат_ничего_не_приписываем():
    заголовок, причина = headword_with_genitive_article(
        "Buntheit der Menschenwelt", tables={}, plural_verdict=("", "не числится"))
    assert заголовок == "Buntheit der Menschenwelt"
    assert "не знает слова" in причина


def test_первая_ступень_главнее_второй():
    """Если слово знают ОБЕ, отвечает единственное число: «der Wechsel», а не «die»."""
    заголовок, _ = headword_with_genitive_article(
        "Wechsel der Geschäftsleitung", tables=_tables("m", "Wechsel"),
        plural_verdict=("die", "форма множественного"))
    assert заголовок == "der Wechsel der Geschäftsleitung"


def test_правило_идемпотентно():
    """Ночной проход гоняется каждую ночь, дверь — на каждом сохранении. Второй проход
    по уже починенной строке обязан ничего не менять, иначе появится «der der …»."""
    один, _ = headword_with_genitive_article(
        "Vollstrecker einer Anordnung", tables=_tables("m", "Vollstrecker"))
    два, _ = headword_with_genitive_article(один, tables=_tables("m", "Vollstrecker"))
    assert два == один == "der Vollstrecker einer Anordnung"
