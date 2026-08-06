"""Отказ базы — сигнал нам, а не сообщение человеку.

Человеку до наших правил дела нет: он отправил фразу и ждёт, что она сохранится. Если
запись не прошла проверку, разбираться должны мы — сама фраза у нас есть, остальное
наша работа. Поэтому сохранение читает, КАКОЕ правило отказало, чинит именно это и
повторяет попытку.

Чинить молча можно не всё. Если сохранять нечего (обе стороны пусты) или починка
неизвестна — ошибка идёт дальше и попадает в журнал: тихо потерять слово человека
хуже, чем громко упасть.
"""
import unittest
from unittest.mock import patch

from backend import backend_server as bs


class RuleNameTests(unittest.TestCase):
    def test_rule_name_is_read_out_of_the_database_error(self):
        exc = Exception('new row for relation "bt_3_webapp_dictionary_queries" violates '
                        'check constraint "chk_wdq_german_side_is_latin"')
        self.assertEqual(bs._db_rule_that_refused(exc), "chk_wdq_german_side_is_latin")

    def test_other_errors_are_not_mistaken_for_a_rule(self):
        self.assertEqual(bs._db_rule_that_refused(Exception("connection reset by peer")), "")
        self.assertEqual(bs._db_rule_that_refused(Exception("deadlock detected")), "")


class RepairTests(unittest.TestCase):
    def test_swapped_sides_are_turned_back(self):
        kwargs = {"word_de": "новорождённый", "word_ru": "das Neugeborene",
                  "translation_de": "новорождённый", "translation_ru": "das Neugeborene",
                  "source_lang": "ru", "target_lang": "de"}
        fixed = bs._repair_dictionary_kwargs_for_db_rule(kwargs, "chk_wdq_german_side_is_latin")
        self.assertEqual(fixed["word_de"], "das Neugeborene")
        self.assertEqual(fixed["word_ru"], "новорождённый")
        self.assertEqual((fixed["source_lang"], fixed["target_lang"]), ("de", "ru"))

    def test_card_without_a_german_side_is_saved_one_sided(self):
        """Латыни нет ниоткуда — немецкое слово не выдумываем, но слово человека спасаем."""
        kwargs = {"word_de": "дом", "word_ru": "дом", "source_lang": "ru", "target_lang": "de"}
        fixed = bs._repair_dictionary_kwargs_for_db_rule(kwargs, "chk_wdq_german_side_is_latin")
        self.assertIsNone(fixed["word_de"])
        self.assertEqual(fixed["word_ru"], "дом")

    def test_same_language_on_both_sides_is_read_off_the_alphabets(self):
        kwargs = {"word_de": "das Haus", "word_ru": "дом", "source_lang": "de", "target_lang": "de"}
        fixed = bs._repair_dictionary_kwargs_for_db_rule(kwargs, "chk_wdq_pair_is_two_languages")
        self.assertEqual((fixed["source_lang"], fixed["target_lang"]), ("de", "ru"))

    def test_broken_breakdown_is_dropped_not_the_word(self):
        kwargs = {"word_de": "das Haus", "word_ru": "дом", "response_json": "строка"}
        fixed = bs._repair_dictionary_kwargs_for_db_rule(kwargs, "chk_wdq_response_is_object")
        self.assertEqual(fixed["response_json"], {})
        self.assertEqual(fixed["word_de"], "das Haus")

    def test_nothing_to_save_is_not_quietly_swallowed(self):
        kwargs = {"word_de": "", "word_ru": ""}
        self.assertIsNone(bs._repair_dictionary_kwargs_for_db_rule(kwargs, "chk_wdq_has_a_side"))


class SaveRetriesAfterRepairTests(unittest.TestCase):
    def test_refused_save_is_repaired_and_goes_through(self):
        attempts = []

        def fake_save(**kwargs):
            attempts.append(dict(kwargs))
            if len(attempts) == 1:
                raise Exception('violates check constraint "chk_wdq_german_side_is_latin"')
            return 777

        with patch.object(bs, "save_webapp_dictionary_query_returning_id", side_effect=fake_save), \
             patch.object(bs, "_attach_saved_entry_to_lex_unit", return_value=None), \
             patch.object(bs, "_fix_swapped_sides_before_save", side_effect=lambda k: k), \
             patch.object(bs, "_fix_headword_case_before_save", side_effect=lambda k: k):
            entry_id = bs._save_dictionary_entry_with_schema_retry(
                word_de="новорождённый", word_ru="das Neugeborene",
                translation_de=None, translation_ru=None,
                source_lang="ru", target_lang="de",
            )

        self.assertEqual(entry_id, 777)
        self.assertEqual(len(attempts), 2, "починку не повторили")
        self.assertEqual(attempts[1]["word_de"], "das Neugeborene")

    def test_an_error_that_is_not_ours_is_not_swallowed(self):
        with patch.object(bs, "save_webapp_dictionary_query_returning_id",
                          side_effect=Exception("connection reset by peer")), \
             patch.object(bs, "_fix_swapped_sides_before_save", side_effect=lambda k: k), \
             patch.object(bs, "_fix_headword_case_before_save", side_effect=lambda k: k):
            with self.assertRaises(Exception) as ctx:
                bs._save_dictionary_entry_with_schema_retry(word_de="das Haus", word_ru="дом")
        self.assertIn("connection reset", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
