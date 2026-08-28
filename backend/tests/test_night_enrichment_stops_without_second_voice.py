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
        """Прод без ключей: выключатель для тестов снят, оба судьи недоступны.

        С 28.08.2026 судей ДВА (Gemini основной, GPT mini запасной), поэтому «спросить
        нечем» — это отсутствие ОБОИХ ключей, а не одного."""
        env = {k: v for k, v in os.environ.items()
               if k not in ("SECOND_VOICE_CHECK_DISABLED", "GEMINI_API_KEY",
                            "OPENAI_API_KEY")}
        return mock.patch.dict(os.environ, env, clear=True)

    def test_reason_is_named_when_both_keys_are_missing(self):
        from backend.second_voice_check import unavailable_reason
        with self._no_key_env():
            сказано = unavailable_reason()
        self.assertIn("GEMINI_API_KEY", сказано)
        self.assertIn("OPENAI_API_KEY", сказано)

    def test_reserve_key_alone_is_enough_to_start_the_night(self):
        """Gemini не пополнен, GPT есть — ночь обязана идти, а не вставать."""
        from backend.second_voice_check import unavailable_reason
        with self._no_key_env():
            with mock.patch.dict(os.environ, {"OPENAI_API_KEY": "тест"}):
                self.assertEqual("", unavailable_reason())

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

    def test_night_run_starts_when_only_the_reserve_voice_is_available(self):
        """Ровно случай владельца: Gemini не пополнен. Прогон обязан НАЧАТЬСЯ —
        значит дойти до модели, а не упасть на пороге."""
        from backend import backend_server
        дошли = []
        with self._no_key_env():
            with mock.patch.dict(os.environ, {"OPENAI_API_KEY": "тест"}):
                with mock.patch.object(
                    backend_server.lex_units, "units_needing_card",
                    return_value=[{"id": 1, "display": "der Zufall", "translation": "случай"}],
                ):
                    with mock.patch.object(
                        backend_server, "_rich_enrich_card_fields",
                        side_effect=lambda **kw: дошли.append(kw) or {},
                    ):
                        with mock.patch.object(
                            backend_server.lex_units, "count_units_needing_card",
                            return_value=0,
                        ):
                            backend_server.run_pool_night_enrichment(limit=1)
        self.assertEqual(1, len(дошли),
                         "ночь не дошла до модели, хотя запасной судья доступен")

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
