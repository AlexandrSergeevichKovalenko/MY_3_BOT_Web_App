# -*- coding: utf-8 -*-
"""ЗАПАСНОЙ СУДЬЯ: Gemini молчит — спрашиваем GPT mini, но говорим об этом вслух.

Решение владельца 28.08.2026: «скорее всего Gemini я не буду пополнять… нужно проверить:
если нет ответа от Gemini, чтобы не падала опять вся работа, а мы шли и переспрашивали у
GPT через мини».

Тест держит ЧЕТЫРЕ обещания сразу — и последнее не менее важно, чем первое:
  1. основной голос молчит → спрашиваем запасной, работа не встаёт;
  2. основной ответил → к запасному не идём вовсе (лишних денег не тратим);
  3. молчат ОБА → разбор не записывается, как и прежде: непроверенное в базу не идёт;
  4. подмена судьи СЧИТАЕТСЯ и видна в ответе — иначе это тихая деградация, а она
     запрещена правилом ноль ровно так же, как выдуманный ответ.
"""
import os
import unittest
from unittest import mock

from backend import second_voice_check as sv

КАРТОЧКА = {
    "usage_examples": [{"de": "Das war ein Zufall.", "ru": "Это была случайность."}],
    "translation_ru": "случайность",
}


class SecondVoiceReserve(unittest.TestCase):
    def setUp(self):
        sv.reset_stats()
        # Проверка включена, оба ключа на месте: дальше каждый тест решает сам,
        # кто из судей «отвечает», подменяя _ask_gemini / _ask_openai.
        self.env = mock.patch.dict(
            os.environ,
            {"GEMINI_API_KEY": "тест", "OPENAI_API_KEY": "тест",
             "SECOND_VOICE_CHECK_DISABLED": ""},
        )
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_reserve_answers_when_the_main_voice_is_silent(self):
        with mock.patch.object(sv, "_ask_gemini", return_value=(None, "Gemini HTTP 429")):
            with mock.patch.object(sv, "_ask_openai", return_value=([], "")) as запасной:
                ответ = sv.review_new_card(headword="der Zufall", card=КАРТОЧКА)
        запасной.assert_called_once()
        self.assertTrue(ответ["checked"])
        self.assertTrue(ответ["ok"])
        self.assertEqual("openai", ответ["voice"])
        self.assertEqual(1, sv.stats()["openai"])

    def test_reserve_is_not_asked_when_the_main_voice_answered(self):
        with mock.patch.object(sv, "_ask_gemini", return_value=([], "")):
            with mock.patch.object(sv, "_ask_openai") as запасной:
                ответ = sv.review_new_card(headword="der Zufall", card=КАРТОЧКА)
        запасной.assert_not_called()
        self.assertEqual("gemini", ответ["voice"])
        self.assertEqual({"gemini": 1, "openai": 0, "unchecked": 0}, sv.stats())

    def test_reserve_verdict_still_blocks_a_bad_card(self):
        """Запасной судья слабее, но он СУДЬЯ: найденный им дефект закрывает запись."""
        дефект = [{"field": "examples", "what": "русское предложение в немецком поле"}]
        with mock.patch.object(sv, "_ask_gemini", return_value=(None, "Gemini HTTP 429")):
            with mock.patch.object(sv, "_ask_openai", return_value=(дефект, "")):
                ответ = sv.review_new_card(headword="der Zufall", card=КАРТОЧКА)
        self.assertTrue(ответ["checked"])
        self.assertFalse(ответ["ok"])
        self.assertEqual(["examples"], ответ["fields"])

    def test_both_silent_means_not_checked_and_both_reasons_named(self):
        with mock.patch.object(sv, "_ask_gemini", return_value=(None, "Gemini HTTP 429")):
            with mock.patch.object(sv, "_ask_openai", return_value=(None, "GPT Timeout")):
                ответ = sv.review_new_card(headword="der Zufall", card=КАРТОЧКА)
        self.assertFalse(ответ["checked"])
        self.assertFalse(ответ["ok"])
        self.assertIn("Gemini HTTP 429", ответ["why"])
        self.assertIn("GPT Timeout", ответ["why"])
        self.assertEqual(1, sv.stats()["unchecked"])

    def test_a_card_without_examples_is_not_counted_as_a_verdict(self):
        """Проверять нечего — это не работа судьи и не должно попасть в счётчик."""
        ответ = sv.review_new_card(headword="der Zufall", card={"usage_examples": []})
        self.assertTrue(ответ["ok"])
        self.assertEqual({"gemini": 0, "openai": 0, "unchecked": 0}, sv.stats())

    def test_the_reserve_is_asked_the_very_same_question(self):
        """Разница в вердиктах обязана идти от модели, а не от переформулировки:
        кривой вопрос опаснее отсутствия проверки (замер 23.08.2026, 4 295 ложных
        дефектов на своей же формулировке)."""
        отправлено = {}

        class _Ответ:
            choices = [type("c", (), {"message": type("m", (), {"content": '{"defects":[]}'})})]

        class _Клиент:
            class chat:
                class completions:
                    @staticmethod
                    def create(**kw):
                        отправлено.update(kw)
                        return _Ответ()

        with mock.patch.object(sv, "_ask_gemini", return_value=(None, "Gemini HTTP 429")):
            with mock.patch("backend.synthetic_load.build_sync_openai_client",
                            return_value=_Клиент()):
                sv.review_new_card(headword="der Zufall", card=КАРТОЧКА)
        роли = {m["role"]: m["content"] for m in отправлено["messages"]}
        self.assertEqual(sv.SYSTEM, роли["system"])
        self.assertEqual(sv.RESERVE_MODEL, отправлено["model"])


if __name__ == "__main__":
    unittest.main()
