"""Банк кроссвордов считается ОДНИМ правилом — и отчётом, и пополнялкой.

Разбор 19.08.2026. Владелец четвёртое утро подряд читал одно и то же письмо:
«Кроссворды — в банке 60, дозаказать 2» и следом «Дозаказано ночью: заказано 2».
В живой базе оказалось два разных счётчика одного банка:

    отчёт     → image_status = 'ready' AND retired = FALSE   = 59
    пополнялка→ retired = FALSE (вместе с ненарисованными)   = 61

Отчёт ставил цель «наполни до 62», считая от своих 59; пополнялка сравнивала цель
со своими 61 и делала 1 вместо 3. Разницу не показывал никто. Здесь проверяется, что
такого разрыва больше нет: ждущие картинки сперва дорисовываются (это локальный PNG,
модели и денег не стоит), и только потом считается банк — тем же правилом, что у отчёта.
"""

import unittest
from unittest.mock import patch

from backend import crossword_generator


class OneCounterTests(unittest.TestCase):
    def _run(self, *, ready_after_render: int, target: int, made: int = 0):
        calls = []

        def _count(*, exclude_retired: bool = True, ready_only: bool = False) -> int:
            calls.append(("count", ready_only))
            return ready_after_render

        def _render(*, limit: int = 10) -> dict:
            calls.append(("render", limit))
            return {"attempted": 2, "succeeded": 2, "failed": 0}

        with patch("backend.database.count_crossword_bank_entries", new=_count), \
             patch("backend.crossword_renderer.prepare_crossword_images_batch",
                   new=_render), \
             patch("backend.database.retire_undersized_crossword_bank_entries",
                   return_value=0), \
             patch("backend.database.sweep_crossword_bank_shape",
                   return_value={"retired": 0, "re_render": 0}), \
             patch.object(crossword_generator, "generate_crossword_entry",
                          side_effect=[f"cid-{i}" for i in range(made)]), \
             patch.object(crossword_generator.time, "sleep", return_value=None):
            stats = crossword_generator.prepare_crossword_pool(
                target_ready=target, max_attempts=10)
        return stats, calls

    def test_bank_is_counted_by_the_same_rule_as_the_report(self):
        stats, calls = self._run(ready_after_render=61, target=62, made=1)
        self.assertIn(("count", True), calls,
                      "банк обязан считаться правилом отчёта — по готовым")
        self.assertEqual(stats["succeeded"], 1)

    def test_pending_images_are_rendered_before_counting_not_after(self):
        """Иначе сделанное ночью не успевает стать готовым, и заказ уходит впустую:
        замер отчёта идёт в 04:25, а дорисовка стояла отдельной работой в 08:30."""
        _, calls = self._run(ready_after_render=62, target=62)
        self.assertEqual(calls[0][0], "render",
                         "сперва дорисовать ждущие, потом считать банк")
        self.assertEqual(calls[1][0], "count")

    def test_order_is_not_silently_cut_by_unrendered_entries(self):
        """59 готовых, цель 62. После дорисовки готовых 61 — значит родить нужно 1,
        а не «уже хватает». Ноль генераций здесь и был дефектом."""
        stats, _ = self._run(ready_after_render=61, target=62, made=1)
        self.assertEqual(stats["attempted"], 1)
        self.assertEqual(stats["skipped"], 0)

    def test_failure_reason_reaches_the_report(self):
        with patch("backend.database.count_crossword_bank_entries",
                   return_value=59), \
             patch("backend.crossword_renderer.prepare_crossword_images_batch",
                   return_value={"attempted": 0, "succeeded": 0, "failed": 0}), \
             patch("backend.database.retire_undersized_crossword_bank_entries",
                   return_value=0), \
             patch("backend.database.sweep_crossword_bank_shape",
                   return_value={"retired": 0, "re_render": 0}), \
             patch.object(crossword_generator, "generate_crossword_entry",
                          side_effect=RuntimeError("загаданы неходовые слова (TASTEN)")), \
             patch.object(crossword_generator.time, "sleep", return_value=None):
            stats = crossword_generator.prepare_crossword_pool(
                target_ready=60, max_attempts=1)
        self.assertEqual(stats["failed"], 1)
        self.assertEqual(stats["reasons"], ["загаданы неходовые слова (TASTEN)"])


if __name__ == "__main__":
    unittest.main()
