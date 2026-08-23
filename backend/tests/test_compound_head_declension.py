# -*- coding: utf-8 -*-
"""Составное слово склоняется как его последняя часть — и только по ДОКАЗАННОМУ шву.

ЗАЧЕМ. Составных слов в немецком бесконечно много: «Umschaltsituation»,
«Geschwindigkeitsbegrenzung». Ни одна скачиваемая выгрузка их все не содержит —
проверено, «Umschaltsituation» нет ни в одной. Значит либо выводим склонение из головы
слова, либо у части слов таблицы не будет никогда.

ПРАВИЛО ДОКУМЕНТИРОВАНО: склоняется только основное слово (Duden, grammis, Righthand
Head Rule). Трудность не в правиле, а в том, ГДЕ РЕЗАТЬ.

ПОЧЕМУ НЕЛЬЗЯ РЕЗАТЬ НАИВНО. Правило «бери самый длинный хвост, который сам является
словом» я написал первым, и оно режет «Abart» как «A|bart» — выдавая мужской род от
«der Bart» вместо женского от «die Art». Прогон по 64 тысячам слов: 2597 ошибок по роду.

ЗАМЕР ДОКАЗАННОГО ШВА (обе части — известные однозначные слова), 6000 длинных слов,
верный ответ известен из справочника:

    шов доказан      2522    род верно 2474 · неверно 12   (99,5%)
    шов не доказан   3468    молчим — ничего не выдумываем
                             мн. верно 2262 · неверно 17   (99,3%)
                             ген. верно 2139 · неверно  9  (99,6%)
"""
from __future__ import annotations

import unittest
from unittest import mock

from backend import german_reference_forms as R


def _таблица(род: str, слово: str, источник: str) -> dict:
    return {род: {"rows": [{"case": "nom", "label": "Nominativ", "singular": f"die {слово}"}],
                  "has_singular": True, "has_plural": False},
            "source": источник}


class ВыводИзИсточникаПобеждаетДогадкуTest(unittest.TestCase):
    """Кэш отвечает первым, и догадка модели, попавшая туда раньше, побеждала бы всегда."""

    def test_догадка_заменяется_когда_шов_согласен(self):
        от_модели = _таблица("f", "Abzugshaube", "модель")
        от_шва = _таблица("f", "Abzugshaube", "правило композита")
        with mock.patch.object(R, "load_noun_declension", lambda w: от_модели), \
             mock.patch.object(R, "declension_from_compound", lambda w: от_шва):
            ответ = R.noun_declension_for("Abzugshaube")
        self.assertEqual(ответ["source"], "правило композита")

    def test_спорят_по_роду_догадку_НЕ_трогаем(self):
        """«apfelmark»: модель говорит мужской, шов — женский («die Mark» / «das Mark»).
        Слово двузначно, и молча выбрать сторону нельзя — это к человеку."""
        от_модели = _таблица("m", "Apfelmark", "модель")
        от_шва = _таблица("f", "Apfelmark", "правило композита")
        with mock.patch.object(R, "load_noun_declension", lambda w: от_модели), \
             mock.patch.object(R, "declension_from_compound", lambda w: от_шва):
            ответ = R.noun_declension_for("apfelmark")
        self.assertEqual(ответ["source"], "модель", "спорную догадку подменили выводом")

    def test_таблицу_справочника_не_подменяем_никогда(self):
        от_справочника = _таблица("f", "Ratte", "wiktionary-deklination")
        от_шва = _таблица("m", "Ratte", "правило композита")
        with mock.patch.object(R, "load_noun_declension", lambda w: от_справочника), \
             mock.patch.object(R, "declension_from_compound", lambda w: от_шва):
            ответ = R.noun_declension_for("Ratte")
        self.assertEqual(ответ["source"], "wiktionary-deklination")


class ШовДоказываетсяАНеУгадываетсяTest(unittest.TestCase):
    """Ловушки, на которых наивный разрез ошибается, а доказанный шов молчит."""

    def test_ложный_шов_не_принимается(self):
        from backend.article_authority import compound_heads
        # «Schwert» не «Schw|ert»: наивный разрез когда-то дал здесь «der» по «der Wert».
        self.assertEqual(compound_heads("schwert"), set())

    def test_короткое_слово_не_режется(self):
        from backend.article_authority import compound_heads
        for слово in ("haus", "ratte", "baum"):
            self.assertEqual(compound_heads(слово), set(), слово)


if __name__ == "__main__":
    unittest.main()
