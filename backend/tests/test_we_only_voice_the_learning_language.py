# -*- coding: utf-8 -*-
"""Вслух звучит только изучаемый язык. За русскую озвучку мы не платим.

Повод (замер 23.08.2026). Прогрев при сохранении слова озвучивал «целевую сторону»
словарного запроса. А запрос почти всегда идёт de->ru — человек смотрит немецкое слово,
чтобы узнать русский перевод: 3052 карточки против 240 обратных. Целевой стороной
оказывался РУССКИЙ текст, и мы его синтезировали и оплачивали.

Что накопилось: 1612 готовых русских озвучек, из них хоть раз запрошена 8. Для
сравнения немецких 1618, запрошено 839. То есть русское аудио почти никто не слушает,
а платили мы за него столько же.

Прогон нового правила на 2229 реальных сохранениях за 30 дней:
    было бы русским 2087, немецким 142
    стало  русским    0, немецким 2226, не озвучиваем 3

Владелец 23.08.2026: «we do not need russian to play and we do not need to spend money
for that». Русский вслух не нужен нигде — правило закрывает это на входе.
"""
import os
import sys
import unittest

os.environ.setdefault("BACKEND_RUNTIME_SIDE_EFFECTS_ENABLED", "0")
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")


class OnlyLearningLanguageIsSpokenTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from backend.backend_server import _pick_learning_language_utterance
        cls.pick = staticmethod(_pick_learning_language_utterance)

    def test_lookup_de_to_ru_voices_german_not_the_russian_translation(self):
        """Главный случай: 94% сохранений. Раньше отсюда уходил русский."""
        text, lang = self.pick(
            source_text="der Niederschlag", source_lang="de",
            target_text="осадки", target_lang="ru",
            learning_lang="de",
        )
        self.assertEqual(text, "der Niederschlag")
        self.assertEqual(lang, "de")

    def test_lookup_ru_to_de_still_voices_german(self):
        text, _lang = self.pick(
            source_text="осадки", source_lang="ru",
            target_text="der Niederschlag", target_lang="de",
            learning_lang="de",
        )
        self.assertEqual(text, "der Niederschlag")

    def test_swapped_columns_are_decided_by_the_script_not_the_column(self):
        """У части карточек немецкое слово лежит в русской графе. Верить графе нельзя."""
        text, _lang = self.pick(
            source_text="Hund", source_lang="ru",   # графа врёт
            target_text="собака", target_lang="de",  # и здесь тоже
            learning_lang="de",
        )
        self.assertEqual(text, "Hund")

    def test_both_sides_russian_is_not_voiced_at_all(self):
        """Нечего озвучивать — молчим и считаем это отдельным случаем.

        Подставлять сюда «хоть что-нибудь» нельзя: это была бы оплаченная озвучка
        русского, ради отказа от которой правило и написано.
        """
        text, _lang = self.pick(
            source_text="осадки", source_lang="ru",
            target_text="дождь", target_lang="de",
            learning_lang="de",
        )
        self.assertEqual(text, "")

    def test_a_russian_learner_would_get_russian(self):
        """Правило про ИЗУЧАЕМЫЙ язык, а не «немецкий всегда»: если человек учит
        русский, вслух ему нужен русский. Иначе мы зашили бы немецкий намертво."""
        text, lang = self.pick(
            source_text="der Hund", source_lang="de",
            target_text="собака", target_lang="ru",
            learning_lang="ru",
        )
        self.assertEqual(text, "собака")
        self.assertEqual(lang, "ru")


if __name__ == "__main__":
    unittest.main()
