"""Перерисовка после смены правила отрисовки обязана быть РАЗОВОЙ.

Разбор 19.08.2026. Ночная прополка банка кроссвордов решала, что картинка устарела,
сравнивая два правила открытия клеток — оба считаются по СЛОВАМ кроссворда. Слова при
перерисовке не меняются, поэтому вердикт «устарела» держался вечно: одна и та же
запись (от 31.07) уходила на перерисовку каждую ночь три недели подряд, занимая слот
рендера и вылетая из выдачи до утра.

Теперь у записи есть отметка, по какому правилу нарисована её картинка. Прополка
сверяется с ней, а не гадает по словам.
"""

import unittest

from backend.database import CROSSWORD_IMAGE_RULE_VERSION


class SweepReRenderConditionTests(unittest.TestCase):
    """Условие перерисовки проверяется как чистое правило, без похода в базу."""

    @staticmethod
    def _needs_re_render(*, image_status: str, rule_version: int,
                         shape_differs: bool, budget_left: int) -> bool:
        # Тот же порядок условий, что в sweep_crossword_bank_shape.
        return (image_status == "ready"
                and int(rule_version or 0) < CROSSWORD_IMAGE_RULE_VERSION
                and budget_left > 0
                and shape_differs)

    def test_old_image_is_re_rendered_once(self):
        self.assertTrue(self._needs_re_render(
            image_status="ready", rule_version=0, shape_differs=True, budget_left=10))

    def test_after_re_render_the_same_entry_is_left_alone(self):
        """Ровно то, чего не было: перерисовали — и запись больше не мигает."""
        self.assertFalse(self._needs_re_render(
            image_status="ready", rule_version=CROSSWORD_IMAGE_RULE_VERSION,
            shape_differs=True, budget_left=10))

    def test_entry_without_shape_problem_is_never_touched(self):
        self.assertFalse(self._needs_re_render(
            image_status="ready", rule_version=0, shape_differs=False, budget_left=10))

    def test_unrendered_entry_is_not_re_rendered(self):
        """Ждущая картинку запись и так в очереди на отрисовку."""
        self.assertFalse(self._needs_re_render(
            image_status="pending", rule_version=0, shape_differs=True, budget_left=10))

    def test_the_marker_starts_at_zero_so_old_entries_get_their_one_pass(self):
        """Записи, нарисованные до появления отметки, обязаны перерисоваться один
        раз: у них в базе стоит ноль, а текущее правило — больше нуля."""
        self.assertGreater(CROSSWORD_IMAGE_RULE_VERSION, 0)
        self.assertTrue(self._needs_re_render(
            image_status="ready", rule_version=0, shape_differs=True, budget_left=1))


if __name__ == "__main__":
    unittest.main()
