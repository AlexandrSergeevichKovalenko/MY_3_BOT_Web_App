"""Экран разбора спорных фраз: номер варианта на кнопке = номер, который применит сервер.

Почему это отдельный тест. Судьи расходятся постоянно — ровно поэтому фраза попадает
владельцу. На экране у каждого предложенного варианта своя кнопка с номером, и тот же
номер уходит обратно в решении. Если нумерация на сервере при сборке экрана и при
применении решения разойдётся хоть на единицу, владелец нажмёт «Принять 2», а в словарь
уедет вариант первого судьи — молча и без следа. Проверяем, что оба места считают
варианты одной и той же функцией и одинаково.
"""
import unittest
from unittest.mock import patch


REVIEWS = [
    {
        "id": 7, "unit_id": 42, "text": "Er hat hoch bekommen", "translation": "У него встал",
        "judges": [
            {"verdict": "error", "category": "wortstellung",
             "corrected": "Er hat hochbekommen", "proposal": "", "why": "Слитно."},
            {"verdict": "error", "category": "wortstellung",
             "corrected": "Er hat hoch bekommen", "proposal": "Er hat es hochbekommen",
             "why": "Порядок слов."},
        ],
    },
]


class VariantNumberingTests(unittest.TestCase):
    def _payload(self):
        from backend.backend_server import _phrase_review_payload
        with patch("backend.database.list_open_phrase_reviews", return_value=REVIEWS):
            return _phrase_review_payload()

    def test_every_distinct_judge_variant_becomes_a_button(self):
        item = self._payload()["items"][0]
        self.assertEqual([v["text"] for v in item["variants"]],
                         ["Er hat hochbekommen", "Er hat es hochbekommen"])

    def test_a_fix_that_changes_nothing_is_not_a_button(self):
        """Судья 2 объявил ошибку порядка слов и «исправил» фразу в саму себя. Кнопка на
        такой вариант ничего не меняет, но выглядит как решение — её быть не должно."""
        item = self._payload()["items"][0]
        self.assertNotIn(REVIEWS[0]["text"], [v["text"] for v in item["variants"]])

    def test_button_index_matches_what_the_server_would_apply(self):
        from backend.database import phrase_review_variants
        item = self._payload()["items"][0]
        applied = phrase_review_variants(REVIEWS[0]["judges"], REVIEWS[0]["text"])
        for v in item["variants"]:
            self.assertEqual(v["text"], applied[v["index"]]["text"],
                             "номер на кнопке не совпал с тем, что применит сервер")

    def test_judge_block_carries_the_same_number_as_the_button(self):
        """Рядом с вариантом в разборе стоит тот же номер, что на кнопке — иначе по
        кнопке нельзя понять, чей вариант принимаешь."""
        item = self._payload()["items"][0]
        by_text = {v["text"]: v["index"] for v in item["variants"]}
        for j in item["judges"]:
            if j["corrected"] and j["corrected"] in by_text:
                self.assertEqual(j["corrected_slot"], by_text[j["corrected"]])
            if j["proposal"] and j["proposal"] in by_text:
                self.assertEqual(j["proposal_slot"], by_text[j["proposal"]])

    def test_completion_is_marked_as_added_words(self):
        """Достройка неполной фразы и обычная правка — разные вещи, и владелец должен
        видеть, где судья ДОПИСАЛ слова, а где только поправил окончание."""
        item = self._payload()["items"][0]
        kinds = {v["text"]: v["kind"] for v in item["variants"]}
        self.assertEqual(kinds["Er hat hochbekommen"], "fix")
        self.assertEqual(kinds["Er hat es hochbekommen"], "complete")


if __name__ == "__main__":
    unittest.main()
