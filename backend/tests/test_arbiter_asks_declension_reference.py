# -*- coding: utf-8 -*-
"""Арбитр рода спрашивает справочник склонений — и делает это ОДНИМ читателем.

ЗАЧЕМ. 23.08.2026 в базу загружено 89 704 таблицы склонения вместо прежних 2 909.
Арбитр `authoritative_article` о них не знал и ходил в сеть впустую. Замер по всем 2881
нашим существительным: 249 слов справочник склонений знает, а арбитр — нет
(«Kurzbefehl», «Bugfahrwerk», «Wildcard», «Kostenhochlaufkurve»).

ПОЧЕМУ ЧИТАТЕЛЬ ОДИН. Я написал свой и тут же выбросил, когда сосед сделал общий
(`backend/noun_declension_reference.py`). Два читателя одной таблицы неизбежно
разъезжаются в правилах, а правил там три и все неочевидные:

    два рода в записи   «die Nebel» / «der Nebel» — молчим, выбирать не наше дело
    множественное       именительный единственного обязан совпасть с самим словом
    нет множественного  обычно имя собственное или субстантивация («das Athen»)

НА ЭТОМ Я УЖЕ ОШИБСЯ: брал из таблицы первый попавшийся род и насчитал 29 «расхождений»
с арбитром — все 29 оказались моей ошибкой отбора, а не расхождением данных. «Nebel»,
«Mund», «Zelt» хранят по два рода, и «die Mund» с «der Zelt» не существуют вовсе.
"""
from __future__ import annotations

import inspect
import unittest
from unittest import mock

from backend import article_authority as A


class АрбитрСпрашиваетСправочникTest(unittest.TestCase):
    def test_ответ_справочника_принимается(self):
        with mock.patch.object(A, "_article_from_declension",
                               lambda w: ("der", "справочник склонений")):
            артикль, откуда = A.authoritative_article("Kurzbefehl", allow_network=False)
        self.assertEqual(артикль, "der")
        self.assertEqual(откуда, "справочник склонений")

    def test_спрашивается_ДО_сети(self):
        """Сеть — дорогая и ненадёжная: её отказ по частоте неотличим от «слова нет».
        Справочник у нас на диске, и спрашивать его после сети было бы расточительством."""
        ходил_в_сеть = []
        with mock.patch.object(A, "_article_from_declension",
                               lambda w: ("die", "справочник склонений")), \
             mock.patch.object(A, "_wiktionary_live",
                               lambda w: ходил_в_сеть.append(w) or "der"):
            артикль, _ = A.authoritative_article("Wildcard", allow_network=True)
        self.assertEqual(артикль, "die")
        self.assertEqual(ходил_в_сеть, [], "пошли в сеть, хотя ответ лежал на диске")

    def test_молчание_справочника_не_мешает_остальным_ступеням(self):
        with mock.patch.object(A, "_article_from_declension", lambda w: (None, "молчит")), \
             mock.patch.object(A, "_wiktionary_live", lambda w: "das"):
            артикль, откуда = A.authoritative_article("Neuwort", allow_network=True)
        self.assertEqual((артикль, откуда), ("das", "wiktionary-live"))


class ЧитательОдинНаВсёПриложениеTest(unittest.TestCase):
    def test_арбитр_зовёт_общего_читателя_а_не_свой(self):
        src = inspect.getsource(A._article_from_declension)
        self.assertIn("article_from_declension_reference", src,
                      "заведён второй читатель таблицы склонений — правила разъедутся")

    def test_два_рода_в_записи_общий_читатель_отвергает(self):
        from backend.noun_declension_reference import article_from_declension_tables
        двойная = {"m": {"rows": [{"case": "nom", "singular": "der Nebel"}]},
                   "f": {"rows": [{"case": "nom", "singular": "die Nebel"}]}}
        артикль, почему = article_from_declension_tables("Nebel", двойная)
        self.assertIsNone(артикль, f"выбрали род по жребию: {почему}")


if __name__ == "__main__":
    unittest.main()
