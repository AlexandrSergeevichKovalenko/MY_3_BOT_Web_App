import unittest

from backend.database import _apply_ru_de_dictionary_pair_alignment


class DictionaryEditPairAlignmentTests(unittest.TestCase):
    def test_ru_to_de_edit_keeps_russian_as_source_and_german_as_target(self):
        columns, source_text, target_text, payload = _apply_ru_de_dictionary_pair_alignment(
            source_lang="ru",
            target_lang="de",
            german_text="jemanden seiner Rechte berauben",
            russian_text="лишить кого-либо прав",
            current_word_ru="старый русский",
            current_translation_de="старый немецкий",
            current_word_de="старый немецкий",
            current_translation_ru="старый русский",
            response_json={"source_lang": "ru", "target_lang": "de"},
        )

        self.assertEqual(source_text, "лишить кого-либо прав")
        self.assertEqual(target_text, "jemanden seiner Rechte berauben")
        self.assertEqual(columns["word_ru"], "лишить кого-либо прав")
        self.assertEqual(columns["translation_ru"], "лишить кого-либо прав")
        self.assertEqual(columns["word_de"], "jemanden seiner Rechte berauben")
        self.assertEqual(columns["translation_de"], "jemanden seiner Rechte berauben")
        self.assertEqual(payload["source_text"], "лишить кого-либо прав")
        self.assertEqual(payload["target_text"], "jemanden seiner Rechte berauben")
        self.assertEqual(payload["word_de"], "jemanden seiner Rechte berauben")
        self.assertEqual(payload["translation_ru"], "лишить кого-либо прав")

    def test_de_to_ru_edit_keeps_german_as_source_and_russian_as_target(self):
        columns, source_text, target_text, payload = _apply_ru_de_dictionary_pair_alignment(
            source_lang="de",
            target_lang="ru",
            german_text="sich an etwas erinnern",
            russian_text="помнить о чём-либо",
            current_word_ru="старый русский",
            current_translation_de="старый немецкий",
            current_word_de="старый немецкий",
            current_translation_ru="старый русский",
            response_json={"source_lang": "de", "target_lang": "ru"},
        )

        self.assertEqual(source_text, "sich an etwas erinnern")
        self.assertEqual(target_text, "помнить о чём-либо")
        self.assertEqual(columns["word_de"], "sich an etwas erinnern")
        self.assertEqual(columns["translation_de"], "sich an etwas erinnern")
        self.assertEqual(columns["word_ru"], "помнить о чём-либо")
        self.assertEqual(columns["translation_ru"], "помнить о чём-либо")
        self.assertEqual(payload["source_text"], "sich an etwas erinnern")
        self.assertEqual(payload["target_text"], "помнить о чём-либо")
        self.assertEqual(payload["word_de"], "sich an etwas erinnern")
        self.assertEqual(payload["translation_ru"], "помнить о чём-либо")


if __name__ == "__main__":
    unittest.main()
