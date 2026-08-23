# -*- coding: utf-8 -*-
"""У отделяемого глагола приставка в личной форме УХОДИТ В КОНЕЦ.

Владелец, 17.08.2026, на карточке «klarkommen»: таблица печатала «ich klarzukomme»,
а после исправления заголовка — «ich klarkomme». Ни того, ни другого в немецком нет:
правильно «ich komme klar».

ЧТО ИЗМЕНИЛОСЬ 23.08.2026. Тогда правило отделения приставки стояло в нашей арифметике:
движок сам резал основу, приклеивал окончания и переставлял приставку. Владелец эту
арифметику отменил целиком — «как мы можем механически что-то делать, когда это касается
языка?». Теперь таблица берётся напечатанной: своя страница Flexion, полная форма
разговорного усечения, основа составного глагола, а если справочника нет — модель,
спрошенная дважды с полным совпадением ответов.

Поэтому здесь осталось два предмета проверки:
  • `split_separable_verb` — он живёт дальше, им пользуется поиск основы в справочнике
    («rausbringen» → «heraus» + «bringen»);
  • `build_verb_conjugation` — что он ОТДАЁТ НАПЕЧАТАННОЕ и не сочиняет своего.
"""
import pytest

from backend.german_grammar_tables import (
    build_verb_conjugation,
    split_separable_verb,
)


@pytest.mark.parametrize("verb, prefix, base", [
    ("klarkommen", "klar", "kommen"),
    ("ankommen", "an", "kommen"),
    ("aufstehen", "auf", "stehen"),
    ("zusammenarbeiten", "zusammen", "arbeiten"),
    ("weitermachen", "weiter", "machen"),
])
def test_separable_prefix_is_recognised(verb, prefix, base):
    assert split_separable_verb(verb) == (prefix, base)


@pytest.mark.parametrize("verb", [
    # Неотделяемые: приставка остаётся на месте.
    "verstehen", "bekommen", "erklären", "gehören", "entstehen",
    # Отделяемость зависит от ЗНАЧЕНИЯ, а написание одно: «übersetzen» —
    # переводить (неотделяемая) и переправлять через реку (отделяемая). Молчим.
    "übersetzen", "umfahren", "durchschauen", "unterhalten", "wiederholen",
    # Не глаголы и слишком короткие основы.
    "machen", "gehen", "sein",
])
def test_left_joined_when_not_certainly_separable(verb):
    assert split_separable_verb(verb) == ("", verb)


def _reference(monkeypatch, table):
    """Подставить справочник: движок обязан отдать ровно то, что в нём напечатано."""
    import backend.german_grammar_tables as G
    seen: list[str] = []

    def fake(infinitive):
        seen.append(infinitive)
        return dict(table) if table else None

    monkeypatch.setattr(G, "_documented_conjugation", fake)
    return seen


class TestTheTableComesFromTheReference:
    def test_the_printed_prefix_position_is_kept(self, monkeypatch):
        """«ich komme klar» напечатано в справочнике — так и уходит на экран."""
        _reference(monkeypatch, {
            "praesens": {"ich": "komme klar", "du": "kommst klar", "wir": "kommen klar"},
            "praeteritum": {"du": "kamst klar", "wir": "kamen klar"},
            "partizip2": "klargekommen", "auxiliary": "sein",
            "source": "wiktionary-flexion",
        })
        table = build_verb_conjugation(word_de="klarkommen",
                                       seed={"praeteritum": "выдумка", "perfekt": "выдумка"})
        assert table["praesens"]["ich"] == "komme klar"
        assert table["praeteritum"]["du"] == "kamst klar"
        assert table["partizip2"] == "klargekommen"

    def test_the_seed_from_the_card_no_longer_builds_anything(self, monkeypatch):
        """`seed` — непроверенные поля модели. Формы из них больше НЕ строятся.

        Раньше именно они дописывали таблицу там, где справочник молчал, и на не-глаголе
        давали «ich boree», «ich aspettiamoe». Замер 22.08.2026 — 96 таких таблиц.
        """
        _reference(monkeypatch, None)
        assert build_verb_conjugation(
            word_de="bore",
            seed={"present_3sg": "bort", "praeteritum": "borte",
                  "perfekt": "hat gebort", "imperative_sg": "bor"}) is None

    def test_the_reference_is_asked_about_the_lowercase_word(self, monkeypatch):
        """Заголовок бывает с заглавной («Aufwachen»), справочник знает строчное."""
        seen = _reference(monkeypatch, {"praesens": {"ich": "wache auf"}})
        build_verb_conjugation(word_de="Aufwachen")
        assert seen == ["aufwachen"]


def test_zu_infinitive_still_builds_no_table():
    assert build_verb_conjugation(word_de="klarzukommen") is None
