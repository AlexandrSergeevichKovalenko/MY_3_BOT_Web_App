# -*- coding: utf-8 -*-
"""Сверка со справочником не должна врать ни в одну, ни в другую сторону.

Здесь заперты ТРИ ловушки, каждая из которых уже давала ложные числа на живой базе
23.08.2026 — и каждая выглядела как «нашли много ошибок»:

  • род хранится «der/die/das», справочник пишет «m/f/n» → сравнение в лоб дало
    «100% брака» на 2 779 словах;
  • «-» в справочнике значит «не знаю», а не ответ → 441 несуществующее расхождение;
  • у слова бывает два рода («das/der Verdienst» = «mn») → наш ответ, входящий в пару,
    верен, а не ошибочен. После починки расхождений стало 53 → 5.

И четвёртая, из глаголов: у отделяемых справочник печатает форму разделённой
(«fügte zu»), у нас она слитная («zufügte»), плюс у нас в карточке стоят местоимения.
Первая версия проверки объявила расхождением 84 глагола, почти все — верные.
"""
import importlib.util
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
_spec = importlib.util.spec_from_file_location(
    "dict_field_audit", ROOT / "scripts" / "dict_field_audit_by_reference.py")
audit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(audit)


def noun(display, gender, plural=None):
    return {"id": 1, "display": display, "pos": "noun", "gender": gender,
            "card": {"forms": {"plural": plural}} if plural is not None else {}}


def verb(display, praeteritum, perfekt):
    return {"id": 2, "display": display, "pos": "verb", "gender": None,
            "card": {"forms": {"praeteritum": praeteritum, "perfekt": perfekt}}}


class GenderTests(unittest.TestCase):
    def test_article_and_letter_are_the_same_answer(self):
        verdict, _, _ = audit.check_gender(noun("der Tisch", "der"), {"tisch": "m"})
        self.assertEqual(verdict, audit.CONFIRMED)

    def test_dash_means_the_source_does_not_know(self):
        verdict, _, _ = audit.check_gender(noun("die Rettungswache", "die"),
                                           {"rettungswache": "-"})
        self.assertEqual(verdict, audit.SILENT)

    def test_word_with_two_genders_is_not_a_conflict(self):
        verdict, _, _ = audit.check_gender(noun("das Verdienst", "das"), {"verdienst": "mn"})
        self.assertEqual(verdict, audit.CONFIRMED)

    def test_real_conflict_is_still_reported(self):
        verdict, _, _ = audit.check_gender(noun("der Tisch", "der"), {"tisch": "f"})
        self.assertEqual(verdict, audit.CONFLICT)

    def test_missing_gender_of_ours_is_an_open_cell(self):
        verdict, _, _ = audit.check_gender(noun("Tisch", None), {"tisch": "m"})
        self.assertEqual(verdict, audit.SILENT)


class PluralTests(unittest.TestCase):
    def test_article_on_our_side_is_not_a_difference(self):
        verdict, _, _ = audit.check_plural(noun("der Eimer", "der", "die Eimer"),
                                           {"eimer": "Eimer"})
        self.assertEqual(verdict, audit.CONFIRMED)

    def test_note_in_brackets_is_not_a_difference(self):
        verdict, _, _ = audit.check_plural(noun("die Gaze", "die", "die Gazen (редко)"),
                                           {"gaze": "Gazen"})
        self.assertEqual(verdict, audit.CONFIRMED)

    def test_wrong_plural_is_a_conflict(self):
        verdict, _, _ = audit.check_plural(noun("das Profilbild", "das", "die Profile"),
                                           {"profilbild": "Profilbilder"})
        self.assertEqual(verdict, audit.CONFLICT)

    def test_source_without_the_word_is_silent(self):
        verdict, _, _ = audit.check_plural(noun("das Drahtlosnetzwerk", "das", "die Netzwerke"), {})
        self.assertEqual(verdict, audit.SILENT)


class VerbFormTests(unittest.TestCase):
    TABLE = {"praeteritum": ["ich fügte zu", "er fügte zu"], "partizip": ["zugefügt"]}

    def test_separable_verb_written_apart_still_matches(self):
        verdict, _, _ = audit.check_verb_forms(verb("zufügen", "zufügte", "hat zugefügt"),
                                               {"zufügen": self.TABLE})
        self.assertEqual(verdict, audit.CONFIRMED)

    def test_pronouns_on_our_side_do_not_break_the_match(self):
        verdict, _, _ = audit.check_verb_forms(
            verb("zufügen", "er/sie/es fügte zu", "er hat zugefügt"), {"zufügen": self.TABLE})
        self.assertEqual(verdict, audit.CONFIRMED)

    def test_form_absent_from_the_table_is_a_conflict(self):
        verdict, _, _ = audit.check_verb_forms(verb("zufügen", "zufügete", "hat zugefügt"),
                                               {"zufügen": {"praeteritum": ["fügte zu"]}})
        self.assertEqual(verdict, audit.CONFLICT)

    def test_verb_the_source_never_saw_is_silent(self):
        verdict, _, _ = audit.check_verb_forms(verb("beispielen", "beispielte", "hat gebeispielt"), {})
        self.assertEqual(verdict, audit.SILENT)


class VerdictVocabularyTests(unittest.TestCase):
    def test_three_states_exist_and_differ(self):
        """«Источник молчит» — отдельное состояние, а не «всё хорошо» и не «ошибка»."""
        self.assertEqual(len({audit.CONFIRMED, audit.CONFLICT, audit.SILENT}), 3)


if __name__ == "__main__":
    unittest.main()
