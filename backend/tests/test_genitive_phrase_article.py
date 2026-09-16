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
    """У «Truppen» таблица найдётся по ключу «truppe», и её «die» относится к «die Truppe».
    Справочник ловит это сам: написание не совпало с именительным единственного."""
    заголовок, причина = headword_with_genitive_article(
        "Truppen des Gegners", tables=_tables("f", "Truppe", plural="Truppen"))
    assert заголовок == "Truppen des Gegners"
    assert "не именительный единственного" in причина


def test_правило_идемпотентно():
    """Ночной проход гоняется каждую ночь, дверь — на каждом сохранении. Второй проход
    по уже починенной строке обязан ничего не менять, иначе появится «der der …»."""
    один, _ = headword_with_genitive_article(
        "Vollstrecker einer Anordnung", tables=_tables("m", "Vollstrecker"))
    два, _ = headword_with_genitive_article(один, tables=_tables("m", "Vollstrecker"))
    assert два == один == "der Vollstrecker einer Anordnung"
