"""Личная выдача не теряет личную память, когда общий склад в отдыхе.

Разбор 19.08.2026. У заданий два разных ограничителя, и до этого дня их снимали разом:

  • общий отдых — «это задание недавно показывали КОМУ-ТО» (14 дней у кроссворда);
  • личная память — «ЭТОТ человек его уже решил» (лестница 90/120/никогда).

Запасной заход снимал оба сразу. Пока склад был просторный, это не мешало. Но замер
19.08 показал: из 61 готового кроссворда свободны СЕМЬ — то есть первый заход почти
всегда пуст, и на деле работал только запасной, возвращая человеку решённое.
Владелец: «зачем человек, который правильно ответил, увидит его снова через 14 дней?»

Решение владельца 20.08.2026: отпускается ТОЛЬКО общий отдых. Личная память не
отпускается никогда — ни ради «лишь бы не пусто», ни подменой на другую игру. Если
свежего нет, честно возвращаем «нечего», считаем это и говорим владельцу: значит
дозаказ не успел за расходом, и чинить надо банк, а не выдачу.
"""

import asyncio
import unittest

import bot_3


class PickOrderTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.calls = []

    def _picker(self, free_when):
        """Выдаёт задание только при названном послаблении, иначе ничего."""
        def picker(*, cooldown_days, exclude_ids=None, **kw):
            self.calls.append((cooldown_days, tuple(exclude_ids or ())))
            relaxed = ("отдых снят" if cooldown_days == 0 else "как есть")
            memory = ("память снята" if exclude_ids is None else "память учтена")
            return {"id": "x"} if (relaxed, memory) == free_when else None
        picker.__name__ = "picker"
        return picker

    async def test_free_task_is_taken_without_any_relaxation(self):
        entry = await bot_3._pick_for_person(
            self._picker(("как есть", "память учтена")), cooldown_days=14, blocked=["a"])
        self.assertIsNotNone(entry)
        self.assertEqual(len(self.calls), 1, "лишних заходов быть не должно")

    async def test_cooldown_is_released_before_personal_memory(self):
        """Главное правило: разным людям МОЖНО дать одно задание, а одному и тому же
        повторять решённое — нельзя. Значит первым отпускаем общий отдых."""
        entry = await bot_3._pick_for_person(
            self._picker(("отдых снят", "память учтена")), cooldown_days=14, blocked=["a"])
        self.assertIsNotNone(entry)
        self.assertEqual(self.calls[0], (14, ("a",)))
        self.assertEqual(self.calls[1], (0, ("a",)),
                         "второй заход обязан ещё помнить, что человек уже прошёл")
        self.assertEqual(len(self.calls), 2)

    async def test_personal_memory_is_never_dropped(self):
        """Решение владельца 20.08.2026: решённое не возвращается никогда. Раньше был
        третий заход «пустой экран хуже повтора» — он снимал личную память и отдавал
        человеку то, что тот уже прошёл. Подмена другой игрой тоже запрещена: банк
        обязан быть впереди расхода, а не выкручиваться."""
        entry = await bot_3._pick_for_person(
            self._picker(("отдых снят", "память снята")), cooldown_days=14, blocked=["a"])
        self.assertIsNone(entry, "лучше честное «нечего», чем решённое по второму разу")
        self.assertEqual(len(self.calls), 2, "заходов ровно два, третьего больше нет")
        self.assertTrue(all(c[1] == ("a",) for c in self.calls),
                        "личная память учитывается на каждом заходе")

    async def test_extra_arguments_reach_every_attempt(self):
        """У заданий пула есть формат — он обязан дойти до обоих заходов."""
        seen = []

        def picker(*, cooldown_days, exclude_ids=None, format=None):
            seen.append(format)
            return None
        picker.__name__ = "picker"
        await bot_3._pick_for_person(picker, cooldown_days=15, blocked=[], format="lueckentext")
        self.assertEqual(seen, ["lueckentext"] * 2)


if __name__ == "__main__":
    unittest.main()
