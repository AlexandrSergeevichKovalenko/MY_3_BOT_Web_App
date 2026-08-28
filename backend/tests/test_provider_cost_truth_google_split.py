# -*- coding: utf-8 -*-
"""Эталон Google = расход на API. Налог и пополнения счёта — НЕ расход и в сверку не идут.

ПОВОД, 28.08.2026. Владелец: «А как это эталон 31 евро?! Если в приложении показано 11,45».
Отчёт брал из billing-экспорта ВСЕ строки подряд и складывал их в «эталон»:

    Invoice / Billing Adjustment (Standalone)   16.66   cost_type=adjustment
    Invoice / Tax (Standalone)                   3.34   cost_type=tax
    Gemini API                                  11.45   cost_type=regular
    Cloud Text-to-Speech API                     0.17   cost_type=regular
                                              ------
                                                31.63  ← это и уходило владельцу

$20 из них — два пополнения по $10 (8.33 + 1.67 НДС), которые владелец сделал сам.
Наша ведомость считает только токены и символы API и про пополнения не знает ничего.
Сравнение выдавало Δ −98% ⚠️ — «слепой счётчик», хотя счётчик был ни при чём: врала
арифметика отчёта.

Второй корень того же места: в «наш» столбец складывались сервисы, которых ведомость не
меряет ВООБЩЕ (Gemini — второй голос ходит прямым HTTP и в bt_3_billing_events не пишет).
Ноль за неизмеряемый сервис неотличим от «спросили и вышло ноль» — а это разные вещи.

Тест сторожит три границы. Если он покраснел — не подгонять под новый вывод, а смотреть,
не вернулась ли в эталон сумма, которая расходом на API не является.
"""
import unittest

from backend.provider_cost_truth import (
    _GOOGLE_SERVICE_TO_OURS,
    _NONUSAGE_RU,
    build_provider_cost_truth_text,
)


class GoogleCostSplitTest(unittest.TestCase):
    def test_tax_and_adjustment_are_translated_for_the_owner(self):
        """Владелец читает отчёт глазами: cost_type наружу не выносится сырым."""
        for cost_type in ("tax", "adjustment", "rounding_error"):
            self.assertIn(cost_type, _NONUSAGE_RU)
            self.assertTrue(_NONUSAGE_RU[cost_type].strip())

    def test_tts_counts_both_our_tts_keys(self):
        """google_tts_standard существует в живой ведомости (4 207 строк, $1.28 на
        28.08.2026) и до этой правки не попадал в сверку вовсе — две трети нашего же
        счётчика TTS проходили мимо. Оба ключа обязаны считаться вместе."""
        keys = _GOOGLE_SERVICE_TO_OURS["Cloud Text-to-Speech API"]
        self.assertIn("google_tts", keys)
        self.assertIn("google_tts_standard", keys)

    def test_gemini_is_declared_unmetered_not_zero(self):
        """Gemini обязан оставаться ЯВНО неизмеряемым, пока расход второго голоса не
        пишется в ведомость. Прописать ему ключ, которого никто не пишет, — значит
        вернуть фальшивый ноль в столбец «наш»."""
        self.assertIn("Gemini API", _GOOGLE_SERVICE_TO_OURS)
        self.assertEqual((), _GOOGLE_SERVICE_TO_OURS["Gemini API"])


class GoogleReportTextTest(unittest.TestCase):
    """Сборка текста на подставном ответе BigQuery — ровно тот расклад августа 2026."""

    GOOGLE = {
        "configured": True,
        "mtd_usd": 11.62,          # ТОЛЬКО regular: Gemini 11.45 + TTS 0.17
        "yday_usd": 0.02,
        "by_service": {
            "Gemini API": {"mtd": 11.45, "yday": 0.02},
            "Cloud Text-to-Speech API": {"mtd": 0.17, "yday": 0.0},
        },
        "nonusage_mtd_usd": 20.0,
        "nonusage_yday_usd": 0.0,
        "nonusage_by_type": {
            "adjustment": {"mtd": 16.66, "yday": 0.0},
            "tax": {"mtd": 3.34, "yday": 0.0},
        },
    }
    OURS = {"google_tts": 0.67, "google_tts_standard": 1.25, "openai": 32.07}

    # Бесплатный лимит: оба бакета глубоко внутри него → настоящих денег ноль.
    # Подставляем ЗДЕСЬ, а не ходим в базу: тест обязан быть отвязан от боевой ведомости.
    FREE_TIER = {
        "google_tts": {"used": 42_284.0, "limit": 900_000.0,
                       "remaining": 857_716.0, "real_money_fraction": 0.0},
        "google_tts_standard": {"used": 312_413.0, "limit": 4_000_000.0,
                                "remaining": 3_687_587.0, "real_money_fraction": 0.0},
        "agent_tts": {"used": 0.0, "limit": 900_000.0,
                      "remaining": 900_000.0, "real_money_fraction": 0.0},
    }

    def _render(self):
        import backend.database as db
        import backend.provider_cost_truth as mod

        saved = {
            name: getattr(mod, name)
            for name in (
                "fetch_our_estimate", "fetch_openai_costs", "fetch_openai_usage_tokens",
                "fetch_google_costs", "fetch_google_budget", "fetch_deepl_usage",
                "fetch_cloudflare_r2", "fetch_railway_usage",
            )
        }
        saved_free = db.get_provider_free_tier_status
        try:
            mod.fetch_our_estimate = lambda **kw: dict(self.OURS)
            mod.fetch_openai_costs = lambda **kw: {"configured": False}
            mod.fetch_openai_usage_tokens = lambda **kw: {"configured": False}
            mod.fetch_google_costs = lambda **kw: dict(self.GOOGLE)
            mod.fetch_google_budget = lambda **kw: {"configured": False}
            mod.fetch_deepl_usage = lambda **kw: {"configured": False}
            mod.fetch_cloudflare_r2 = lambda **kw: {"configured": False}
            mod.fetch_railway_usage = lambda **kw: {"configured": False}
            db.get_provider_free_tier_status = lambda *, provider, **kw: dict(
                self.FREE_TIER[provider])
            return build_provider_cost_truth_text()
        finally:
            for name, fn in saved.items():
                setattr(mod, name, fn)
            db.get_provider_free_tier_status = saved_free

    def test_31_63_never_appears_as_the_google_benchmark(self):
        """Главное. Сумма «расход + налог + пополнение» эталоном быть не может."""
        text = self._render()
        self.assertIn("$11.62", text)          # расход на API
        self.assertNotIn("$31.63", text)       # прежний сырой итог
        self.assertNotIn("$31.62", text)

    def test_account_charges_are_shown_separately_and_named(self):
        text = self._render()
        self.assertIn("счёт аккаунта (не расход на API): $20.00", text)
        self.assertIn("корректировка/пополнение $16.66", text)
        self.assertIn("налог $3.34", text)

    def test_unmetered_service_says_so_instead_of_showing_zero(self):
        """Gemini: «мы не считаем», а не «наш $0.00» с фальшивым Δ −100%."""
        text = self._render()
        gemini_line = next(ln for ln in text.splitlines() if "Gemini API" in ln)
        self.assertIn("мы не считаем", gemini_line)
        self.assertNotIn("наш", gemini_line)
        self.assertNotIn("Δ", gemini_line)

    def test_ceiling_is_shown_but_never_compared_with_the_invoice(self):
        """Главное по TTS. Со счётом сравниваются НАСТОЯЩИЕ деньги (внутри бесплатного
        лимита — ноль), а потолок $1.92 стоит отдельной строкой и подписан словами.
        Если $1.92 снова окажется в столбце «наш» — вернулось «завышение в 11 раз»."""
        text = self._render()
        tts_lines = [ln for ln in text.splitlines() if "эталон $0.17" in ln]
        self.assertTrue(tts_lines, f"строки сверки TTS нет в отчёте:\n{text}")
        self.assertIn("наш $0.00", tts_lines[0])
        self.assertNotIn("наш $1.92", text)
        self.assertIn("без бесплатного лимита стоило бы $1.92", text)

    def test_free_tier_remaining_is_visible_for_both_buckets(self):
        """Владелец 28.08.2026 просил видеть остаток. По TTS он настоящий и берётся из
        счётчика символов, а не из денег: 0.67 + 1.25 = 1.92 потолка складываются из
        двух бакетов, и оба обязаны быть на экране."""
        text = self._render()
        self.assertIn("google_tts 42,284/900,000", text)
        self.assertIn("google_tts_standard 312,413/4,000,000", text)


if __name__ == "__main__":
    unittest.main()
