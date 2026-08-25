# -*- coding: utf-8 -*-
"""Что справочник НЕ хранит — решено один раз и больше не обсуждается.

ПОВОД. Владелец 25.08.2026: «когда ты закончишь выкатывать то 12, то 150, то 278 слов?
решить один раз навсегда, а не мусолить каждый день».

Разбор показал: из 143 «неизвестных» слов настоящих 140. Справочник молчал не потому,
что слова нет, а потому что в офлайн-выгрузке лежат только существительные, глаголы и
прилагательные. Союзов, наречий, предлогов, артиклей и причастий там нет и не будет.
Пока это не было записано, они всплывали подозрительными в каждом прогоне.

Здесь заперты два решения владельца, чтобы следующий агент не открыл вопрос заново:
  • части речи вне охвата справочника не считаются подозрительными;
  • четыре английских слова оставлены сознательно («keep it, but mark»).
"""
import importlib.util
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
_spec = importlib.util.spec_from_file_location(
    "words_exist_offline_pass", ROOT / "scripts" / "words_exist_offline_pass.py")
проход = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(проход)


class ScopeIsWrittenDown(unittest.TestCase):
    def test_service_parts_of_speech_are_out_of_scope(self):
        """Союз и наречие — не брак. Их отсутствие в выгрузке существительных нормально."""
        for pos in ("conjunction", "preposition", "lokaladverb", "article", "participle"):
            self.assertIn(pos, проход.СПРАВОЧНИК_ИХ_НЕ_ХРАНИТ, f"{pos} снова станет подозрительным")

    def test_owner_kept_foreign_words(self):
        """Решение владельца 25.08.2026: оставить, но пометить."""
        for word in ("know-it-all", "sticky", "including", "stick-in-the-mud"):
            self.assertIn(word, проход.ЧУЖИЕ_ОСТАВЛЕНЫ)

    def test_nouns_and_verbs_stay_in_scope(self):
        """Существительное и глагол справочник ХРАНИТ — их молчание остаётся сигналом."""
        for pos in ("noun", "verb", "adjective"):
            self.assertNotIn(pos, проход.СПРАВОЧНИК_ИХ_НЕ_ХРАНИТ,
                             "существительное вне охвата — так мы перестанем видеть обрубки")


if __name__ == "__main__":
    unittest.main()
