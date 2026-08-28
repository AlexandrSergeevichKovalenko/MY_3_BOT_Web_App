# -*- coding: utf-8 -*-
"""Второй голос обязан работать ТЕМ ЖЕ кодом в проде, что и на моей машине.

ПОВОД, 26.08.2026. Второй голос звал Gemini через библиотеку `google-genai`, которой
НЕТ в requirements.txt. Локально она стояла — все мои прогоны проходили. На сервере
каждый вызов падал: «cannot import name 'genai' from 'google' (unknown location)»
(пакет `google` там namespace-пакет от google-cloud-*).

Цена. Второй голос стоит ДВЕРЬЮ: не ответил — не записываем. Значит в проде молча
отклонялась каждая запись разбора, собранного моделью. Замер по живой базе: ночное
обогащение писало 226–390 разборов в сутки до 23.08 и 50 / 43 / 2 после. При этом
обращения к модели продолжались — 1531 за 26.08. Мы платили и выбрасывали.

Стережём ДВЕ вещи: чтобы библиотека не вернулась в код и чтобы «не проверили» никогда
не означало «можно писать».
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend import second_voice_check  # noqa: E402


class БезСкрытойЗависимости(unittest.TestCase):
    def test_the_module_does_not_import_the_sdk(self):
        исходник = open(second_voice_check.__file__, encoding="utf-8").read()
        # Имя встречается только в объяснении, почему его здесь больше нет.
        строки_кода = [с for с in исходник.splitlines()
                       if "genai" in с and not с.strip().startswith("#")
                       and "│" not in с]
        self.assertEqual(строки_кода, [],
                         "SDK вернулся в код — в проде его нет, и дверь снова закроется")

    def test_it_talks_over_plain_http(self):
        исходник = open(second_voice_check.__file__, encoding="utf-8").read()
        self.assertIn("generativelanguage.googleapis.com", исходник)
        self.assertIn("import requests", исходник)


class НеПроверилиЗначитНеПишем(unittest.TestCase):
    """«Не смогли спросить» — это НЕ «всё хорошо». Иначе дверь становится картинкой.

    ОБНОВЛЕНО 28.08.2026: судей стало ДВА. Решение владельца — если Gemini молчит (у
    него кончились деньги), проверку делает запасной GPT mini, иначе встаёт вся ночная
    работа. Поэтому «молчит Gemini» больше НЕ равно «не проверили»: чтобы проверить
    прежнюю гарантию, здесь глушится и запасной (OPENAI_API_KEY=""). Сама гарантия не
    ослаблена ни на шаг — молчат оба, значит не пишем. Ниже это же и проверяется.
    """

    def _ответ(self, код=200, тело=None):
        ответ = mock.Mock()
        ответ.status_code = код
        ответ.text = "" if тело is None else "тело"
        ответ.json.return_value = тело or {}
        return ответ

    # Оба судьи глухи — ровно то состояние, при котором писать нельзя.
    ОБА_МОЛЧАТ = {"GEMINI_API_KEY": "ключ", "OPENAI_API_KEY": ""}

    def test_http_error_is_not_a_pass(self):
        with mock.patch.dict(os.environ, self.ОБА_МОЛЧАТ), \
             mock.patch("requests.post", return_value=self._ответ(код=503)):
            итог = second_voice_check.review_new_card(
                headword="Haus", card={"usage_examples": [{"source": "Das Haus."}]})
        self.assertFalse(итог["checked"])
        self.assertFalse(итог["ok"])

    def test_network_failure_is_not_a_pass(self):
        with mock.patch.dict(os.environ, self.ОБА_МОЛЧАТ), \
             mock.patch("requests.post", side_effect=RuntimeError("сети нет")):
            итог = second_voice_check.review_new_card(
                headword="Haus", card={"usage_examples": [{"source": "Das Haus."}]})
        self.assertFalse(итог["checked"])

    def test_missing_key_is_not_a_pass(self):
        with mock.patch.dict(os.environ, {"GEMINI_API_KEY": "", "OPENAI_API_KEY": ""}):
            итог = second_voice_check.review_new_card(
                headword="Haus", card={"usage_examples": [{"source": "Das Haus."}]})
        self.assertFalse(итог["checked"])
        self.assertIn("ключа", итог["why"])

    def test_the_reserve_saves_the_night_when_gemini_is_out_of_money(self):
        """Обратная сторона той же двери: Gemini отдал 429 «денег нет», а запасной
        судья на месте — работа обязана идти дальше, и видно, КТО судил."""
        with mock.patch.dict(os.environ, {"GEMINI_API_KEY": "ключ"}), \
             mock.patch("requests.post", return_value=self._ответ(код=429)), \
             mock.patch.object(second_voice_check, "_ask_openai", return_value=([], "")):
            итог = second_voice_check.review_new_card(
                headword="Haus", card={"usage_examples": [{"source": "Das Haus."}]})
        self.assertTrue(итог["checked"])
        self.assertTrue(итог["ok"])
        self.assertEqual("openai", итог["voice"])

    def test_a_clean_answer_passes(self):
        тело = {"candidates": [{"content": {"parts": [{"text": '{"defects": []}'}]}}]}
        with mock.patch.dict(os.environ, {"GEMINI_API_KEY": "ключ"}), \
             mock.patch("requests.post", return_value=self._ответ(тело=тело)):
            итог = second_voice_check.review_new_card(
                headword="Haus", card={"usage_examples": [{"source": "Das Haus."}]})
        self.assertTrue(итог["checked"])
        self.assertTrue(итог["ok"])

    def test_a_named_defect_blocks_the_write(self):
        тело = {"candidates": [{"content": {"parts": [{"text":
                '{"defects":[{"field":"examples","what":"перевод не тот"}]}'}]}}]}
        with mock.patch.dict(os.environ, {"GEMINI_API_KEY": "ключ"}), \
             mock.patch("requests.post", return_value=self._ответ(тело=тело)):
            итог = second_voice_check.review_new_card(
                headword="Haus", card={"usage_examples": [{"source": "Das Haus."}]})
        self.assertTrue(итог["checked"])
        self.assertFalse(итог["ok"])
        self.assertEqual(итог["fields"], ["examples"])


if __name__ == "__main__":
    unittest.main()
