# -*- coding: utf-8 -*-
"""Ночь не платит за то, что всё равно выбросит.

ПОВОД, ночь на 28.08.2026. Отчёт: «Наполнено за ночь: 0 из 400 взятых, Ошибок: 400».
Разбор ложится на слово только через второй голос (lex_units.save_unit_card): не
проверили — не записали, и это правильно. Но ключ GEMINI_API_KEY стоял только у сервиса
BACKEND_WEB, а ночной добор крутится в планировщике бота, где ключа не было. Ночь
спросила OpenAI про 400 слов — за деньги, успешно — и выбросила все 400 ответов,
уперевшись в закрытую дверь на каждом.

Тест держит ДВА обещания сразу:
  • прогон не начинается, когда писать всё равно будет некуда;
  • он падает ВСЛУХ, с названной причиной, а не возвращает пустой отчёт (иначе
    heartbeat запишет «completed», и утром это не отличить от «работать было нечего»).
"""
import os
import unittest
from unittest import mock


class NightEnrichmentStopsWithoutSecondVoice(unittest.TestCase):
    def _no_key_env(self):
        """Прод без ключа: выключатель для тестов снят, GEMINI_API_KEY пуст."""
        env = {k: v for k, v in os.environ.items()
               if k not in ("SECOND_VOICE_CHECK_DISABLED", "GEMINI_API_KEY")}
        return mock.patch.dict(os.environ, env, clear=True)

    def test_reason_is_named_when_the_key_is_missing(self):
        from backend.second_voice_check import unavailable_reason
        with self._no_key_env():
            self.assertIn("GEMINI_API_KEY", unavailable_reason())

    def test_no_reason_when_the_check_is_switched_off_on_purpose(self):
        from backend.second_voice_check import unavailable_reason
        with mock.patch.dict(os.environ, {"SECOND_VOICE_CHECK_DISABLED": "1"}):
            self.assertEqual("", unavailable_reason())

    def test_night_run_stops_before_spending_a_single_model_call(self):
        from backend import backend_server
        with self._no_key_env():
            with mock.patch.object(
                backend_server, "_rich_enrich_card_fields",
                side_effect=AssertionError("ночь пошла в модель, хотя записать некуда"),
            ):
                with self.assertRaises(RuntimeError) as поймано:
                    backend_server.run_pool_night_enrichment(limit=400)
        сказано = str(поймано.exception)
        self.assertIn("GEMINI_API_KEY", сказано)
        self.assertIn("Ни одного запроса к модели не сделано", сказано)

    def test_dry_run_still_works_without_the_second_voice(self):
        """Сухой прогон ничего не пишет, значит и дверь ему не нужна: он обязан
        остаться способом посмотреть очередь, когда ключа нет."""
        from backend import backend_server
        with self._no_key_env():
            with mock.patch.object(
                backend_server, "_rich_enrich_card_fields",
                side_effect=AssertionError("сухой прогон не имеет права ходить в модель"),
            ):
                with mock.patch.object(
                    backend_server.lex_units, "units_needing_card", return_value=[],
                ):
                    отчёт = backend_server.run_pool_night_enrichment(limit=5, dry_run=True)
        self.assertEqual(0, отчёт["enriched"])


if __name__ == "__main__":
    unittest.main()
