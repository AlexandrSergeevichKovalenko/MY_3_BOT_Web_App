# -*- coding: utf-8 -*-
"""Дверь слова: что чинится, что помечается, что не заводится в словарь.

Ни сети, ни модели, ни боевой базы — справочник и модель подменяются. Проверяется НАША
логика: порядок ступеней и вердикты. Каждый случай взят из живого дефекта 19.08.2026,
а не придуман.
"""
from __future__ import annotations

import pytest

import backend.german_word_gate as G


@pytest.fixture(autouse=True)
def _no_cache_no_network(monkeypatch):
    """Кеш и наши данные отключены: тест проверяет ступени, а не базу."""
    monkeypatch.setattr(G, "_cached", lambda asked: None)
    monkeypatch.setattr(G, "_remember", lambda asked, verdict: None)
    monkeypatch.setattr(G, "_known_by_our_data", lambda word: (False, "", ""))


def _reference(pages: dict[str, list[str]]):
    """Подставной справочник: {написание: [части речи]}."""
    def _fake(words):
        return {name: pages[name] for name in words if name in pages}
    return _fake


def test_обрезок_не_заводится_в_словарь(monkeypatch):
    monkeypatch.setattr(G, "_reference_says_about_all", _reference({}))
    monkeypatch.setattr("backend.german_reference_forms.word_exists_by_model",
                        lambda w: {"existiert": False})
    verdict = G.check_word("Abschiebu")
    assert verdict["status"] == G.NOT_A_WORD


def test_потерянный_умлаут_чинится_по_справочнику(monkeypatch):
    monkeypatch.setattr(G, "_reference_says_about_all",
                        _reference({"Ärgernisse": ["Deklinierte Form"]}))
    verdict = G.check_word("Argernisse")
    assert verdict["text"] == "Ärgernisse"
    assert verdict["status"] == G.REPAIRED


def test_устаревшее_написание_ведёт_к_современному(monkeypatch):
    """У «verläßlich» страница ЕСТЬ, но она помечена как устаревшее написание.
    Взять её как ответ нельзя — человек выучит форму, которой больше нет."""
    monkeypatch.setattr(G, "_reference_says_about_all",
                        _reference({"Verläßlich": ["__устаревшее__verlässlich"]}))
    verdict = G.check_word("Verläßlich")
    assert verdict["text"] == "verlässlich"
    assert verdict["status"] == G.REPAIRED


def test_короткий_обрезок_тоже_проверяется(monkeypatch):
    """«Felg» — четыре буквы. Раньше порог отсекал такие от починки."""
    assert "Felge" in G.repair_candidates("Felg")


def test_существительное_получает_заглавную(monkeypatch):
    monkeypatch.setattr(G, "_reference_says_about_all",
                        _reference({"Betäubung": ["Substantiv"]}))
    verdict = G.check_word("betäubung")
    assert verdict["text"] == "Betäubung"
    assert verdict["pos"] == "noun"


def test_прилагательное_получает_строчную(monkeypatch):
    monkeypatch.setattr(G, "_reference_says_about_all",
                        _reference({"grundlegend": ["Adjektiv"]}))
    verdict = G.check_word("Grundlegend")
    assert verdict["text"] == "grundlegend"
    assert verdict["pos"] == "adjective"


def test_подтверждённое_слово_не_трогается(monkeypatch):
    monkeypatch.setattr(G, "_reference_says_about_all", _reference({"Haus": ["Substantiv"]}))
    verdict = G.check_word("Haus")
    assert verdict["text"] == "Haus"
    assert verdict["status"] == G.CONFIRMED


def test_английское_слово_сохраняется_с_пометкой_языка(monkeypatch):
    """Решение владельца 19.08.2026: не отклонять. Впереди английский, и дверь,
    отклоняющая по языку, пойдёт под снос."""
    monkeypatch.setattr(G, "_reference_says_about_all", _reference({}))
    monkeypatch.setattr("backend.german_reference_forms.word_exists_by_model",
                        lambda w: {"existiert": True, "sprache": "en",
                                   "wortart": "Substantiv", "korrekt": "Sweatpants"})
    verdict = G.check_word("Sweatpants")
    assert verdict["status"] != G.NOT_A_WORD
    assert "en" in verdict["source"]


def test_редкое_немецкое_слово_не_выбрасывается(monkeypatch):
    """Справочник неполон: «Arbeitsumfeld» настоящее, но страницы у него нет."""
    monkeypatch.setattr(G, "_reference_says_about_all", _reference({}))
    monkeypatch.setattr("backend.german_reference_forms.word_exists_by_model",
                        lambda w: {"existiert": True, "sprache": "de",
                                   "wortart": "Substantiv", "korrekt": "Arbeitsumfeld"})
    verdict = G.check_word("Arbeitsumfeld")
    assert verdict["status"] == G.UNCONFIRMED
    assert verdict["text"] == "Arbeitsumfeld"


def test_молчание_справочника_не_приговор(monkeypatch):
    """Справочник не ответил — это НЕ «слова нет». Приговор не запоминается."""
    monkeypatch.setattr(G, "_reference_says_about_all", lambda words: None)
    verdict = G.check_word("Haus")
    assert verdict["status"] == G.UNCONFIRMED
    assert "молчал" in verdict["source"]


def test_дверь_не_ходит_в_сеть_когда_запрещено(monkeypatch):
    def _boom(words):
        raise AssertionError("дверь пошла в справочник, хотя ей запретили")
    monkeypatch.setattr(G, "_reference_says_about_all", _boom)
    verdict = G.check_word("Haus", allow_network=False)
    assert verdict["status"] == G.UNCONFIRMED


def test_пустая_строка_не_слово():
    assert G.check_word("   ")["status"] == G.NOT_A_WORD
