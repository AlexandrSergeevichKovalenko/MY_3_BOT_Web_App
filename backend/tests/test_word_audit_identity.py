# -*- coding: utf-8 -*-
"""Опознание слова в проверке: артикль — часть карточки, а не часть имени слова.

ЧТО СЛОМАЛОСЬ 21.08.2026. Личный словарь хранит существительное вместе с артиклем
(«die Abschiebung» — 7829 строк из 25240), а дверь спрашивает голое слово. Сравнение
шло по сырому word_de, поэтому соединение рвалось на каждом существительном: экран
проверки показывал человеку 2 слова вместо 12, и обрубок «das Scheinwerfergla» до
него не доезжал. Решения из плашки по той же причине не находили строку и молча
не применялись.

Здесь закреплено обратное: слово опознаётся по голой форме везде — в списке, в
решениях и в плашке при сохранении.
"""
import re
import unittest

from backend.word_confirm_digest import _BARE, bare_word


class BareWordTest(unittest.TestCase):
    def test_артикль_снимается(self):
        for stored, expected in (
            ("die Abschiebung", "Abschiebung"),
            ("Der Unterhalt", "Unterhalt"),
            ("das Scheinwerfergla", "Scheinwerfergla"),
            ("DIE  Entscheidung", "Entscheidung"),
        ):
            self.assertEqual(bare_word(stored), expected, stored)

    def test_слово_без_артикля_не_трогаем(self):
        for word in ("Abschiebung", "laufen", "dieser", "Dieselmotor", "Dasein", "Derwisch"):
            self.assertEqual(bare_word(word), word, word)

    def test_питон_и_sql_снимают_артикль_одинаково(self):
        """Список собирает SQL, а решения применяет питон — расхождение снова
        разорвало бы связь между экраном и базой."""
        pattern = re.compile(r"^(der|die|das)[ \t\n\r\f\v]+", re.I)
        for stored in ("die Abschiebung", "Der Unterhalt", "Dasein", "laufen", "das  Haus"):
            self.assertEqual(pattern.sub("", stored.strip()).strip(), bare_word(stored), stored)

    def test_выражение_для_sql_собирается_с_любой_колонкой(self):
        sql = _BARE.format(col="q.word_de")
        self.assertIn("q.word_de", sql)
        self.assertIn("der|die|das", sql)


class WithArticleTest(unittest.TestCase):
    """Артикль к ИСПРАВЛЕННОМУ слову берётся у справочника рода, а не из старой строки."""

    def setUp(self):
        from backend import word_confirm_digest as mod
        self.mod = mod

    def _with_stub(self, answer):
        import sys
        import types
        stub = types.ModuleType("backend.article_authority")
        stub.authoritative_article = lambda word, *, allow_network=False: (answer, "тест")
        saved = sys.modules.get("backend.article_authority")
        sys.modules["backend.article_authority"] = stub
        try:
            return self.mod._with_article("Abschiebung")
        finally:
            if saved is None:
                sys.modules.pop("backend.article_authority", None)
            else:
                sys.modules["backend.article_authority"] = saved

    def test_род_известен_артикль_дописан(self):
        self.assertEqual(self._with_stub("die"), "die Abschiebung")

    def test_род_неизвестен_пишем_голое_слово(self):
        """Пустое место честнее выдуманного «der»: артикль допишет ночная
        программа рода, когда узнает его."""
        self.assertEqual(self._with_stub(None), "Abschiebung")

    def test_у_глагола_артикля_не_просим(self):
        self.assertEqual(self.mod._with_article("laufen"), "laufen")



class SafeWordsAreNotDeletedBySilenceTest(unittest.TestCase):
    """Молчание не удаляет слово, про которое известно, что оно настоящее.

    Решение владельца 21.08.2026. Справочники неполны: «Vergleichbarkeit»,
    «Arbeitsumfeld», «Sozialschmarotzer» — обычные слова, страниц у которых нет.
    Экран удаляет всё неотмеченное, и без этого правила он предлагал бы человеку
    стереть хорошие слова просто за то, что тот пролистал список.
    """

    def test_признак_ставится_по_ответу_модели(self):
        from backend.word_confirm_digest import _model_confirmed
        self.assertTrue(_model_confirmed("модель: слово есть, справочник не знает"))
        self.assertTrue(_model_confirmed("модель: слово есть, язык en"))
        # Слово есть, спорно только написание — тоже не удаляем молчанием.
        self.assertTrue(_model_confirmed(
            "модель предложила другое написание, справочник не подтвердил"))

    def test_обрубок_признаком_не_защищён(self):
        from backend.word_confirm_digest import _model_confirmed
        self.assertFalse(_model_confirmed("модель: такого слова нет"))
        self.assertFalse(_model_confirmed("справочник молчал"))
        self.assertFalse(_model_confirmed(""))


class SecondReferenceIsExactTest(unittest.TestCase):
    """DWDS отвечает точным словом, а не похожим — иначе догадка стала бы источником."""

    def test_чужой_заголовок_не_принимается(self):
        import json
        from unittest import mock
        from backend import german_reference_dwds as D

        class _Response:
            def __init__(self, payload): self._p = json.dumps(payload).encode()
            def read(self): return self._p
            def __enter__(self): return self
            def __exit__(self, *a): return False

        # Источник ответил про ДРУГОЕ слово — засчитывать нельзя.
        with mock.patch.object(D.urllib.request, "urlopen",
                               lambda *a, **k: _Response([{"lemma": "Abschiebung",
                                                           "wortart": "Substantiv"}])):
            self.assertEqual(D.dwds_pos("Abschiebu"), "")
        # Тот же заголовок — ответ принимается вместе с частью речи.
        with mock.patch.object(D.urllib.request, "urlopen",
                               lambda *a, **k: _Response([{"lemma": "Abschiebung",
                                                           "wortart": "Substantiv"}])):
            self.assertEqual(D.dwds_pos("Abschiebung"), "Substantiv")

    def test_сеть_молчит_это_не_приговор(self):
        from unittest import mock
        from backend import german_reference_dwds as D

        def _boom(*a, **k):
            raise OSError("сеть недоступна")

        with mock.patch.object(D.urllib.request, "urlopen", _boom):
            self.assertIsNone(D.dwds_pos("Vergleichbarkeit"))
            self.assertIsNone(D.dwds_says_about_all(["Vergleichbarkeit"]))

if __name__ == "__main__":
    unittest.main()
