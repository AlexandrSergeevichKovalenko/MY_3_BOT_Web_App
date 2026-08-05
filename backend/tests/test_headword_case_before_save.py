"""Немецкий глагол и прилагательное не сохраняются с заглавной буквы.

Находка 04.08.2026. В личных карточках лежит «Gelingen» (часть речи — глагол),
«Schlank», «Vorhanden», «Hüpfen»: слово попало в словарь из начала предложения и так
и сохранилось. Само по себе это косметика, но по заголовку строится таблица форм —
и на карточке повторения человек читал «ich Gelinge / wir Gelingen».

Замер по проду: 1708 личных карточек и 36 строк общего пула, где заголовок из ОДНОГО
слова с частью речи verb/adjective/adverb написан с заглавной. Отдельно 499 личных
записей — целые предложения, там заглавная стоит по делу, их не трогаем.

Существующая нормализация (`_apply_german_headword_normalization`) лечит только путь
«разбор → сохранение» и только глаголы; сюда же сходятся ВСЕ пути сохранения, включая
быстрые сохранения из бота и игр.
"""

import unittest

import backend.backend_server as bs


class HeadwordCaseBeforeSaveTests(unittest.TestCase):
    def _fix(self, word_de, pos, **extra):
        kwargs = {
            "word_de": word_de,
            "word_ru": "проверка",
            "source_lang": "ru",
            "target_lang": "de",
            "response_json": {"part_of_speech": pos, "word_de": word_de},
            **extra,
        }
        return bs._fix_headword_case_before_save(kwargs)

    def test_verb_headword_becomes_lowercase(self):
        fixed = self._fix("Gelingen", "verb")
        self.assertEqual(fixed["word_de"], "gelingen")
        self.assertEqual(fixed["response_json"]["word_de"], "gelingen")

    def test_adjective_and_adverb_too(self):
        self.assertEqual(self._fix("Schlank", "adjective")["word_de"], "schlank")
        self.assertEqual(self._fix("Immerhin", "adverb")["word_de"], "immerhin")

    def test_noun_keeps_its_capital(self):
        self.assertEqual(self._fix("Nomen", "noun")["word_de"], "Nomen")

    def test_sentence_keeps_its_capital(self):
        """«Das Problem wurde behoben.» размечено глаголом, но это предложение."""
        fixed = self._fix("Das Problem wurde behoben.", "verb")
        self.assertEqual(fixed["word_de"], "Das Problem wurde behoben.")

    def test_unknown_part_of_speech_is_left_alone(self):
        self.assertEqual(self._fix("Gelingen", "")["word_de"], "Gelingen")

    def test_already_lowercase_is_untouched(self):
        kwargs = {
            "word_de": "gelingen",
            "source_lang": "ru",
            "target_lang": "de",
            "response_json": {"part_of_speech": "verb"},
        }
        self.assertIs(bs._fix_headword_case_before_save(kwargs), kwargs)

    def test_mirroring_columns_follow_the_headword(self):
        """Если то же слово продублировано в source_text/translation_de — правим и там,
        иначе карточка разъедется сама с собой."""
        fixed = self._fix("Hüpfen", "verb", source_text="Hüpfen", translation_de="Hüpfen")
        self.assertEqual(fixed["source_text"], "hüpfen")
        self.assertEqual(fixed["translation_de"], "hüpfen")

    def test_other_columns_are_not_touched(self):
        fixed = self._fix("Ehren", "verb", source_text="чтить", translation_de="Ehren ist wichtig")
        self.assertEqual(fixed["source_text"], "чтить")
        self.assertEqual(fixed["translation_de"], "Ehren ist wichtig")


if __name__ == "__main__":
    unittest.main()



class HeadwordCaseAllCapsTests(unittest.TestCase):
    """Слово целиком капсом. Замена одной первой буквы дала бы «eRGATTERN» — хуже,
    чем было. Живой случай из базы, найден при сплошной чистке 05.08.2026."""

    def test_all_caps_word_is_lowered_whole(self):
        fixed = bs._fix_headword_case_before_save({
            "word_de": "ERGATTERN",
            "response_json": {"part_of_speech": "verb", "word_de": "ERGATTERN"},
        })
        self.assertEqual(fixed["word_de"], "ergattern")
        self.assertEqual(fixed["response_json"]["word_de"], "ergattern")

    def test_single_capital_letter_is_not_broken(self):
        fixed = bs._fix_headword_case_before_save({
            "word_de": "A",
            "response_json": {"part_of_speech": "adverb"},
        })
        self.assertEqual(fixed["word_de"], "a")

    def test_broken_case_inside_the_word_is_lowered_whole(self):
        """«eRGATTERN» осталось от прежней версии этой же правки — она опускала только
        первую букву. Заглавная внутри слова у глагола не бывает никогда."""
        for broken in ("eRGATTERN", "GeLingen", "anPassen"):
            fixed = bs._fix_headword_case_before_save({
                "word_de": broken,
                "response_json": {"part_of_speech": "verb"},
            })
            self.assertEqual(fixed["word_de"], broken.lower(), broken)

    def test_broken_case_with_a_doubtful_part_of_speech_is_left_alone(self):
        """«zEITSCHRIFT» помечено прилагательным, хотя это существительное. Опустить
        его — испортить. Внутренней заглавной верим только у глагола."""
        for word in ("zEITSCHRIFT", "eROBERUNG"):
            kwargs = {"word_de": word, "response_json": {"part_of_speech": "adjective"}}
            self.assertIs(bs._fix_headword_case_before_save(kwargs), kwargs, word)
