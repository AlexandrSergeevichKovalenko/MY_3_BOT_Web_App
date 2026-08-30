# -*- coding: utf-8 -*-
"""Таблица окончаний прилагательного и склейка пропуска обратно в слово.

ПОЧЕМУ ЭТОТ ФАЙЛ ПОЯВИЛСЯ. Замер 21.08.2026 по 2370 заданиям, которые люди уже
получили: окончания сошлись с таблицей склонения во ВСЕХ 2340 заданиях основного
генератора, задействованы все 27 клеток. То есть таблица верна. Но теста на неё не
было ни одного — тронь её кто-нибудь, и ничто бы не упало, а неверное окончание
уехало бы прямо в голову ученику.

Здесь таблица набрана ЗАНОВО, вручную, а не импортом из проверяемого модуля: тест,
сверяющий код сам с собой, ничего не стережёт.

Источник: стандартная парадигма склонения немецкого прилагательного (слабое — после
определённого артикля, смешанное — после ein-/kein-/притяжательных, сильное — без
артикля). Генератор сознательно выдаёт только единственное число Nom/Akk/Dat, поэтому
и проверяется ровно это.
"""
from __future__ import annotations

import unittest
from unittest import mock

from backend import adjektiv_endings
from backend.adjektiv_endings import _TABLES, build_adjektiv_items
from backend.database import adjektiv_gap_rebuilds, derive_adjektiv_split

# Слабое склонение: артикль уже показал род и падеж.
ОЖИДАЕМО_СЛАБОЕ = {
    "Nom": {"m": "e",  "f": "e",  "n": "e"},
    "Akk": {"m": "en", "f": "e",  "n": "e"},
    "Dat": {"m": "en", "f": "en", "n": "en"},
}
# Смешанное: ein/kein/mein в Nom м.р. и Nom/Akk ср.р. рода не показывают — там
# прилагательное берёт окончание сильного склонения.
ОЖИДАЕМО_СМЕШАННОЕ = {
    "Nom": {"m": "er", "f": "e",  "n": "es"},
    "Akk": {"m": "en", "f": "e",  "n": "es"},
    "Dat": {"m": "en", "f": "en", "n": "en"},
}
# Сильное: артикля нет, прилагательное несёт признаки рода и падежа само.
ОЖИДАЕМО_СИЛЬНОЕ = {
    "Nom": {"m": "er", "f": "e",  "n": "es"},
    "Akk": {"m": "en", "f": "e",  "n": "es"},
    "Dat": {"m": "em", "f": "er", "n": "em"},
}
ОЖИДАЕМО = {"weak": ОЖИДАЕМО_СЛАБОЕ, "mixed": ОЖИДАЕМО_СМЕШАННОЕ, "strong": ОЖИДАЕМО_СИЛЬНОЕ}


class ТаблицаСклоненияTest(unittest.TestCase):
    def test_все_27_клеток_совпадают_с_парадигмой(self):
        for typ, падежи in ОЖИДАЕМО.items():
            for case, роды in падежи.items():
                for gender, ending in роды.items():
                    self.assertEqual(
                        _TABLES[typ][case][gender], ending,
                        f"{typ}/{case}/{gender}: в коде "
                        f"«{_TABLES[typ][case][gender]}», а должно быть «{ending}»")

    def test_ни_одной_лишней_клетки(self):
        """Genitiv и множественное число генератор не выдаёт — и не должен обещать."""
        self.assertEqual(set(_TABLES), set(ОЖИДАЕМО))
        for typ, падежи in _TABLES.items():
            self.assertEqual(set(падежи), set(ОЖИДАЕМО[typ]), typ)
            for case, роды in падежи.items():
                self.assertEqual(set(роды), {"m", "f", "n"}, f"{typ}/{case}")

    def test_окончание_всегда_одно_из_пяти(self):
        for typ, падежи in _TABLES.items():
            for case, роды in падежи.items():
                for gender, ending in роды.items():
                    self.assertIn(ending, ("e", "en", "er", "es", "em"),
                                  f"{typ}/{case}/{gender}")


class ПропускСклеиваетсяОбратноTest(unittest.TestCase):
    """Подставь ответ в пропуск — обязана выйти ровно объявленная фраза.

    Замер 21.08.2026: из 2370 выданных заданий 10 этого не проходили, и все десять
    пришли из резервного банка, который пишет модель. Пропуск стоял не на том слове:

        показано: ohne ein[___] gutes Argument   ответ «e»
        выйдет:   ohne eine gutes Argument       а верно: ohne ein gutes Argument

    Ученик получал «правильно» за неверную форму — и узнать об этом ему было неоткуда.
    """

    def test_живой_случай_из_банка_отбраковывается(self):
        сломано = {"full": "ohne ein gutes Argument", "before": "ohne ein",
                   "after": " gutes Argument", "correct": "e"}
        self.assertFalse(adjektiv_gap_rebuilds(сломано))

    def test_остальные_десять_случаев_чинятся_по_целой_фразе(self):
        """Все десять выданных поправимы: окончание стоит на нужном слове, кривой лишь
        раскрой на «до» и «после». Такое не выбрасывается, а пересобирается из `full`
        — выбросить исправимое задание было бы своей, отдельной потерей."""
        for full, before, after, correct in (
            ("ohne frische Luft", "ohne frische", " Luft", "e"),
            ("guter Kaffee", "guter", " Kaffee", "er"),
            ("einer jungen Frau", "einer jung", "en Frau", "en"),
            ("ein interessantes Buch", "ein interessant", "es Buch", "es"),
            ("mit frischem Obst", "mit frischem", " Obst", "em"),
            ("meines neuen Autos", "meines neu", "en Autos", "en"),
            ("trotz schönen Wetters", "trotz schön", "en Wetters", "en"),
            ("mit fröhlichen Kindern", "mit fröhlich", "en Kindern", "en"),
            ("ohne lange Wartezeit", "ohne lang", "e Wartezeit", "e"),
            ("keine wichtigen Informationen", "keine wichtig", "en Informationen", "en"),
        ):
            with self.subTest(full=full):
                починено = derive_adjektiv_split(full, correct)
                self.assertIsNotNone(починено, f"«{full}» не пересобрался")
                b, a = починено
                self.assertEqual(b + correct + a, full)
                self.assertTrue(adjektiv_gap_rebuilds(
                    {"full": full, "before": before, "after": after, "correct": correct}))

    def test_здоровое_задание_проходит(self):
        цел = {"full": "die muntere Ausrüstung", "before": "die munter",
               "after": " Ausrüstung", "correct": "e"}
        self.assertTrue(adjektiv_gap_rebuilds(цел))

    def test_пропуск_чинится_по_целой_фразе(self):
        """Кривой раскрой чинится по `full`, если окончание стоит на нужном слове."""
        self.assertEqual(derive_adjektiv_split("ein interessantes Buch", "es"),
                         ("ein interessant", " Buch"))
        self.assertTrue(adjektiv_gap_rebuilds(
            {"full": "ein interessantes Buch", "before": "ein interessant",
             "after": "es Buch", "correct": "es"}))


class ГенераторВыдаётТолькоЦелоеTest(unittest.TestCase):
    # ┌─ ПРОВЕРЕНО 30.08.2026. НЕ ПОДНИМАТЬ ЭТО КАК НОВУЮ НАХОДКУ. ────────────────────┐
    # │ Тест падал 2 прогона из 3 при НЕИЗМЕННОМ коде, и это не поломка генератора.    │
    # │ Признак в сводке pytest: 295 подтестов вместо 395 — ровно на 100 меньше,       │
    # │ то есть список заданий пришёл ПУСТОЙ и падение было на первой же строке.       │
    # │ Причина: _load_nouns ходил в БОЕВУЮ базу, а связь с неё скачет — замер с       │
    # │ машины владельца: 15 обращений, среднее 1,3 c, самое долгое 15,5 c.            │
    # │ Чиним не подгонкой ожидания, а корнем: слова тесту даёт фикстура, база больше  │
    # │ не участвует. Сам генератор (склейка + таблица окончаний) проверяется целиком. │
    # └────────────────────────────────────────────────────────────────────────────────┘
    # Слова взяты из боевого банка bt_3_article_sprint_nouns (все три рода) и здесь
    # зафиксированы: тест проверяет ПРАВИЛО склонения, а не наполненность банка.
    СЛОВА = [
        ("Tisch", "m", "стол"),
        ("Hund", "m", "собака"),
        ("Lampe", "f", "лампа"),
        ("Blume", "f", "цветок"),
        ("Buch", "n", "книга"),
        ("Fenster", "n", "окно"),
    ]

    def test_сто_заданий_подряд_склеиваются_и_сходятся_с_таблицей(self):
        with mock.patch.object(adjektiv_endings, "_load_nouns", return_value=list(self.СЛОВА)):
            items = build_adjektiv_items(100)
        self.assertTrue(items)
        for it in items:
            with self.subTest(full=it.get("full")):
                self.assertEqual(
                    it["before"] + it["correct"] + it["after"], it["full"])
                self.assertEqual(
                    it["correct"], ОЖИДАЕМО[it["typ"]][it["case"]][it["gender"]],
                    f"{it['typ']}/{it['case']}/{it['gender']} → «{it['correct']}»")


if __name__ == "__main__":
    unittest.main()
