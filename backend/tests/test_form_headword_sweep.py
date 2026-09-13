"""Заголовок карточки — словарное слово, а не форма (backend/form_headword_sweep.py).

Класс дефекта, 13.09.2026: в словаре стояли «beruhte» вместо «beruhen» и «Blähungen»
вместо «die Blähung», и они уезжали в общий словарь. Каждый тест здесь — одна из защит,
которую сняли бы «для простоты», а она поймана на живых данных."""
from __future__ import annotations

from unittest import mock

from backend import form_headword_sweep as fh


def _ответы(таблица):
    """Заглушка справочника: {написание: {'kind','bases'}}."""
    def fake(words):
        return {w: таблица.get(w, {"kind": "", "bases": []}) for w in words}
    return fake


def test_форма_с_одной_базой_принимается():
    with mock.patch("backend.german_form_headword.headword_kinds",
                    _ответы({"beruhte": {"kind": "form", "bases": ["beruhen"]}})):
        assert fh._ask_reference(["beruhte"])["beruhte"] == {"класс": "форма", "база": "beruhen"}


def test_законное_слово_не_форма():
    with mock.patch("backend.german_form_headword.headword_kinds",
                    _ответы({"arbeiten": {"kind": "word", "wortarten": ["Verb"]}})):
        assert fh._ask_reference(["arbeiten"])["arbeiten"]["класс"] == "слово"


def test_несколько_баз_не_чиним_молча():
    """«Форма слов X и Y» — вопрос к человеку, а не повод выбрать первое."""
    with mock.patch("backend.german_form_headword.headword_kinds",
                    _ответы({"lesen": {"kind": "form", "bases": ["lesen", "Lese"]}})):
        assert fh._ask_reference(["lesen"])["lesen"]["класс"] == "молчит"


def test_заглавное_чей_строчный_вариант_слово_становится_спорным():
    """Капкан, стоивший бы 24 карточек: у нас «Hüten» — глагол «охранять», а справочник
    знает это написание только как форму от «Hut» (шляпа)."""
    таблица = {
        "Hüten": {"kind": "form", "bases": ["Hut"]},
        "hüten": {"kind": "word", "wortarten": ["Verb"]},
    }
    with mock.patch("backend.german_form_headword.headword_kinds", _ответы(таблица)):
        ответ = fh._ask_reference(["Hüten"])["Hüten"]
    assert ответ["класс"] == "спорно"
    assert "hüten" in ответ["почему"]


def test_заглавное_без_строчного_слова_чинится():
    таблица = {
        "Blähungen": {"kind": "form", "bases": ["Blähung"]},
        "blähungen": {"kind": "", "bases": []},
    }
    with mock.patch("backend.german_form_headword.headword_kinds", _ответы(таблица)):
        assert fh._ask_reference(["Blähungen"])["Blähungen"]["класс"] == "форма"


def test_артикль_снимается_с_написания():
    assert fh.bare("die Behörden") == "Behörden"
    assert fh.bare("Behörden") == "Behörden"
    assert fh.bare("") == ""


def test_части_речи_читаются_на_обоих_языках():
    """На живых данных часть речи лежит и «preposition», и «предлог» — по одному языку
    защита пропускала карточку (13.09.2026, карточка 3419 «Angesichts»)."""
    assert "preposition" in fh.НЕ_СУЩЕСТВИТЕЛЬНОЕ
    assert "предлог" in fh.НЕ_СУЩЕСТВИТЕЛЬНОЕ
    assert "verb" in fh.НЕ_СУЩЕСТВИТЕЛЬНОЕ and "глагол" in fh.НЕ_СУЩЕСТВИТЕЛЬНОЕ
