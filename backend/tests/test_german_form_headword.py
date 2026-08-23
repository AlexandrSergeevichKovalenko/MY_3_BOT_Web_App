# -*- coding: utf-8 -*-
"""Заголовок — слово или форма слова? Разбор страницы справочника.

Тексты страниц взяты С ЖИВОГО de.wiktionary 23.08.2026 и урезаны до значимых строк:
проверяется наш разбор, а не сеть. Каждый случай — из тех 30 заголовков, которые
владелец велел починить по источнику.
"""
from backend.german_form_headword import headword_kind

ABGEZOGEN = """== abgezogen ({{Sprache|Deutsch}}) ==
{{Wortart fehlt|Adjektiv}}
=== {{Wortart|Partizip II|Deutsch}} ===
{{Grammatische Merkmale}}
* Partizip Perfekt des Verbs '''[[abziehen]]'''
{{Grundformverweis Konj|abziehen}}"""

HAETTE = """{{Siehe auch|[[hatte]]}}
== hätte ({{Sprache|Deutsch}}) ==
=== {{Wortart|Konjugierte Form|Deutsch}} ===
{{Grammatische Merkmale}}
* 1. Person Singular Konjunktiv Präteritum Aktiv des Verbs '''[[haben]]'''
{{Grundformverweis Konj|haben}}"""

RAST = """{{Siehe auch|[[Rast]]}}
== rast ({{Sprache|Deutsch}}) ==
=== {{Wortart|Konjugierte Form|Deutsch}} ===
* 2. Person Singular Imperativ Präsens Aktiv des Verbs '''[[rasten]]'''
{{Grundformverweis Konj|rasten}}
=== {{Wortart|Konjugierte Form|Deutsch}} ===
* 2. Person Singular Indikativ Präsens Aktiv des Verbs '''[[rasen]]'''
{{Grundformverweis Konj|rasen}}"""

GEFEIERT = """== gefeiert ({{Sprache|Deutsch}}) ==
=== {{Wortart|Adjektiv|Deutsch}} ===
{{Bedeutungen}}
:[1] allgemein bekannt und beliebt
=== {{Wortart|Partizip II|Deutsch}} ===
{{Grundformverweis Konj|feiern}}"""

HEUTE = """== heute ({{Sprache|Deutsch}}) ==
=== {{Wortart|Temporaladverb|Deutsch}} ===
{{Bedeutungen}}
:[1] an dem Tag, an dem gesprochen wird"""

WOHINGEGEN = """== wohingegen ({{Sprache|Deutsch}}) ==
=== {{Wortart|Subjunktion|Deutsch}} ==="""

ASPETTIAMO = """== aspettiamo ({{Sprache|Italienisch}}) ==
=== {{Wortart|Konjugierte Form|Italienisch}} ===
{{Grundformverweis Konj|aspettare|spr=it}}"""


class TestФормаЭтоНеСлово:
    def test_причастие_называет_свой_глагол(self):
        seen = headword_kind(ABGEZOGEN)
        assert seen["kind"] == "form"
        assert seen["bases"] == ["abziehen"]

    def test_личная_форма_тоже(self):
        assert headword_kind(HAETTE)["bases"] == ["haben"]

    def test_два_глагола_у_одной_формы_видны_оба(self):
        """«rast» — и «rasten», и «rasen». Брать первый нельзя: это разные слова."""
        seen = headword_kind(RAST)
        assert seen["kind"] == "form"
        assert seen["bases"] == ["rasten", "rasen"]


class TestСловоОстаётсяСловом:
    def test_у_прилагательного_есть_своя_статья(self):
        """У «gefeiert» рядом лежит и раздел причастия — словарная статья главнее."""
        seen = headword_kind(GEFEIERT)
        assert seen["kind"] == "word"
        assert seen["pos"] == "adjective"

    def test_наречие_времени_это_наречие(self):
        assert headword_kind(HEUTE) == {
            "kind": "word", "pos": "adverb", "bases": [], "wortarten": ["Temporaladverb"]}

    def test_подчинительный_союз_это_союз(self):
        assert headword_kind(WOHINGEGEN)["pos"] == "conjunction"


class TestЧужойЯзыкНеОтвет:
    def test_итальянское_слово_немецким_разделом_не_считается(self):
        """У страницы есть итальянский раздел и нет немецкого. Ответа НЕТ — и это
        не «слово плохое», а «мы не знаем». Подставлять сюда догадку запрещено."""
        assert headword_kind(ASPETTIAMO) == {
            "kind": "unknown", "pos": "", "bases": [], "wortarten": []}

    def test_пустая_страница_это_не_знаю(self):
        assert headword_kind("")["kind"] == "unknown"
        assert headword_kind(None)["kind"] == "unknown"


SLAY = """== slay ({{Sprache|Englisch}}) ==
=== {{Wortart|Verb|Englisch}} ===
{{Bedeutungen}}
:[1] erschlagen"""


def test_английский_раздел_не_подтверждает_немецкий_заголовок():
    """«slay» лежит в справочнике английским глаголом, и до 23.08.2026 дверь отвечала
    «подтверждено, глагол» — то есть заводила английское слово как немецкое."""
    assert headword_kind(SLAY)["kind"] == "unknown"
    from backend.german_form_headword import german_section
    assert german_section(SLAY) == "", "английский раздел принят за немецкий"


def test_у_причастия_названа_часть_речи_базового_глагола():
    assert headword_kind(ABGEZOGEN)["pos"] == "verb"


def test_у_склонённой_формы_часть_речи_не_называется():
    """«Deklinierte Form» бывает и у существительного, и у прилагательного."""
    text = """== laueren ({{Sprache|Deutsch}}) ==
=== {{Wortart|Deklinierte Form|Deutsch}} ===
{{Grundformverweis Dekl|lau}}"""
    seen = headword_kind(text)
    assert seen["bases"] == ["lau"] and seen["pos"] == ""
