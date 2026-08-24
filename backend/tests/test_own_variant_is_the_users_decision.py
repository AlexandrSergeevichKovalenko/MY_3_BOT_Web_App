"""Свой вариант фразы: проверяем, показываем — но решает человек.

Карточка «Новости дня» и «Стендапа дня» показывает оборот так, как он прозвучал:
«einen hohen genetischen Anteil» стоит в винительном, потому что в предложении он был в
винительном. Кому-то нужно положить себе именительный, кому-то — свою формулировку.
Раньше выбора не было: кнопка одна, и она клала показанное.

Решение владельца 20.08.2026: дать вписать своё, прогнать это через проверку ВМЕСТЕ с
его переводом (падеж и предлог выбираются по смыслу) и ПОКАЗАТЬ мнение судьи, а не
подменить молча. Дословно: «пользователь может для каких-то словосочетаний
самостоятельно вписать сюда в именительном падеже фразу, мы её прогоним через проверку
правильности и сохраним».

Судья здесь — `run_phrase_grammar_verdict`, а не «быстрый корректор». Тот спрашивает
«исправь слово» и на обороте начинает править стиль; этот судит фразу КАК ФРАЗУ, видит
перевод как контекст и обязан не трогать разговорное: «Colloquial but attested German is
CORRECT. Do not standardise it». Для оборотов из стендапа это главное.
"""

import json
import unittest
from unittest.mock import patch

from backend import backend_server as bs


class OwnVariantCheckTests(unittest.TestCase):
    def setUp(self):
        bs.app.config["TESTING"] = True
        self.client = bs.app.test_client()

    def _post(self, *, de, ru, verdict):
        with patch.object(bs, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
             patch.object(bs, "_telegram_hash_is_valid", return_value=True), \
             patch.object(bs, "_parse_telegram_init_data",
                          return_value={"user": {"id": 7, "first_name": "U"}}), \
             patch.object(bs, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(bs, "_resolve_webapp_user_id", return_value=7), \
             patch.object(bs, "_dict_user_has_left_bot", return_value=False), \
             patch.object(bs, "_billing_log_openai_usage", return_value=None), \
             patch("backend.openai_manager.run_phrase_grammar_verdict", return_value=verdict):
            r = self.client.post("/api/webapp/dictionary/check-variant",
                                 json={"de": de, "ru": ru, "initData": "x"})
        return r.status_code, json.loads(r.data)

    def test_correct_variant_passes_without_a_question(self):
        code, data = self._post(de="hoher genetischer Anteil", ru="высокая доля",
                                verdict={"verdict": "ok", "corrected": "", "why": ""})
        self.assertEqual(code, 200)
        self.assertEqual(data["verdict"], "ok")
        self.assertEqual(data["suggestion_de"], "")

    def test_a_real_error_is_shown_not_applied(self):
        """Мнение судьи уходит наружу ПРЕДЛОЖЕНИЕМ. Подставить его вместо текста
        человека сервер не имеет права — выбор делают на экране."""
        code, data = self._post(
            de="einen hoher genetischer Anteil", ru="высокая доля",
            verdict={"verdict": "error", "corrected": "ein hoher genetischer Anteil",
                     "corrected_ru": "высокая доля", "why": "не согласован артикль"})
        self.assertEqual(code, 200)
        self.assertEqual(data["verdict"], "error")
        self.assertEqual(data["suggestion_de"], "ein hoher genetischer Anteil")
        self.assertIn("артикль", data["why"])

    def test_style_and_context_are_not_shown_as_a_complaint(self):
        """«style» и «context» — это отказ судьи трогать текст, а не претензия.
        Сеять сомнение на собственной формулировке человека мы не будем."""
        for status in ("style", "context"):
            with self.subTest(status):
                _code, data = self._post(de="null Bock haben", ru="нет желания",
                                         verdict={"verdict": status,
                                                  "corrected": "keine Lust haben",
                                                  "why": "разговорное"})
                self.assertEqual(data["verdict"], "ok")
                self.assertEqual(data["suggestion_de"], "")

    def test_a_silent_judge_never_blocks_the_person(self):
        """Судья не ответил — человек всё равно сохраняет своё. Он написал это
        осознанно, а мы просто не смогли проверить."""
        with patch.object(bs, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
             patch.object(bs, "_telegram_hash_is_valid", return_value=True), \
             patch.object(bs, "_parse_telegram_init_data",
                          return_value={"user": {"id": 7, "first_name": "U"}}), \
             patch.object(bs, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(bs, "_resolve_webapp_user_id", return_value=7), \
             patch.object(bs, "_dict_user_has_left_bot", return_value=False), \
             patch("backend.openai_manager.run_phrase_grammar_verdict",
                   side_effect=RuntimeError):
            r = self.client.post("/api/webapp/dictionary/check-variant",
                                 json={"de": "null Bock haben", "ru": "нет желания",
                                       "initData": "x"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(json.loads(r.data)["verdict"], "ok")

    def test_empty_input_is_refused_plainly(self):
        with patch.object(bs, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False), \
             patch.object(bs, "_telegram_hash_is_valid", return_value=True), \
             patch.object(bs, "_parse_telegram_init_data",
                          return_value={"user": {"id": 7, "first_name": "U"}}), \
             patch.object(bs, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(bs, "_resolve_webapp_user_id", return_value=7):
            r = self.client.post("/api/webapp/dictionary/check-variant",
                                 json={"de": "  ", "ru": "х", "initData": "x"})
        self.assertEqual(r.status_code, 400)

    def test_own_variant_is_not_proofread_again_when_saved(self):
        """Человек уже вписал своё осознанно и уже увидел мнение судьи. Прогонять его
        ещё раз при сохранении значило бы перерешать за него то, что он решил сам."""
        self.assertFalse(bs._save_source_was_typed_by_user("worldnews_phrase_save_own"))


if __name__ == "__main__":
    unittest.main()
