# -*- coding: utf-8 -*-
"""Человек со «своими часами» получает ВСЕ ступеньки рельса, а не две из трёх.

ЧТО СЛУЧИЛОСЬ 15.09.2026. Лестница слова: пн узнавание → вт узнавание → ср «Подставь
синоним» → чт спринт. Третью ступеньку сделали 13.09 и повесили только на слотовую
рассылку (синонимы 14:45, антонимы 17:00). А человека со «своими часами» слотовая
рассылка не берёт ВОВСЕ — его ведёт капля (_drip_bonus_pass). Спринт и повтор в каплю
добавлены, «Подставь синоним» — нет, и у такого человека ступеньки не было ни одной.

Замер на живой базе: 14.09 слот антонимов промолчал целиком. Слово «ehrlich» тренировал
ровно один человек, у него окно 06:00–09:00 и 18:00–22:30, а слот стоит в 17:00. За всё
время задание получил один человек — владелец. Обещание в отчёте выглядело как «4 дня
молчания», хотя молчал один слот: измеритель считал слова банка, а не слоты.

Здесь стережётся: капля несёт все три сверхплановые ступеньки, каждая уходит только
тому, кто тренировал ИМЕННО ЭТО слово, и два вида не затирают друг друга.
"""
import asyncio
import datetime
import unittest
from unittest import mock

import bot_3


ОКНО_ЧЕЛОВЕКА = 883565092   # тот самый человек со «своими часами» с прода
СЕГОДНЯ = datetime.datetime(2026, 9, 14, 18, 30)


def _слово(sprint_id: str, relation: str, wort: str) -> dict:
    return {"sprint_id": sprint_id, "relation": relation, "wort": wort,
            "accepted": [{"de": "aufrichtig", "ru": "искренний"}],
            "hint_ru": None, "trainer_json": {"correct_examples": [{"word": "aufrichtig"}]}}


class КапляНесётВсеТриСтупеньки(unittest.TestCase):
    def _прогон(self, *, тренировал: set, есть_заготовки: bool = True):
        """Один заход капли. Возвращает список (вид, час слота) отправленных «Подставь»."""
        ушло: list = []

        async def _gap_send(context, *, entry, relation, slot_date, slot_hour, chat_id,
                            target_user_id):
            ушло.append((relation, slot_hour, chat_id))
            return True

        with mock.patch.object(bot_3, "_trainer_enabled", return_value=True), \
             mock.patch.object(bot_3, "_sprint_enabled", return_value=False), \
             mock.patch.object(bot_3, "_relation_gap_enabled", return_value=True), \
             mock.patch.object(bot_3, "pick_repeat_trainer", return_value=None), \
             mock.patch.object(bot_3, "pick_gap_word",
                               side_effect=lambda *, relation, trained_on:
                                   _слово(f"sp_{relation}_x", relation, "ehrlich")), \
             mock.patch.object(bot_3, "get_trainer_recipient_ids", return_value=тренировал), \
             mock.patch("backend.relation_gap.build_gap_items",
                        return_value=([{"gap": "…"}] if есть_заготовки else [], {})), \
             mock.patch.object(bot_3, "send_gap_to_chat", side_effect=_gap_send):
            bot_3._drip_bonus_pass_done.clear()
            asyncio.run(bot_3._drip_bonus_pass(mock.MagicMock(), ОКНО_ЧЕЛОВЕКА, СЕГОДНЯ))
        return ушло

    def test_тренировавший_получает_оба_вида(self):
        ушло = self._прогон(тренировал={ОКНО_ЧЕЛОВЕКА})
        виды = sorted(v for v, _ч, _c in ушло)
        self.assertEqual(виды, ["antonym", "synonym"],
                         "человек со своими часами не получил третью ступеньку рельса")
        self.assertTrue(all(c == ОКНО_ЧЕЛОВЕКА for _v, _ч, c in ушло))

    def test_часы_у_двух_видов_разные(self):
        """Ключ защиты от повтора — (человек, дата, час). Один час на два вида означал бы,
        что второй вид дня тихо не записался и не ушёл."""
        часы = [ч for _v, ч, _c in self._прогон(тренировал={ОКНО_ЧЕЛОВЕКА})]
        self.assertEqual(len(set(часы)), 2, "два вида делят один час слота")

    def test_кто_не_тренировал_слово_не_получает_ничего(self):
        """Это ступенька конкретной лестницы, а не самостоятельная игра."""
        self.assertEqual(self._прогон(тренировал={111222333}), [])

    def test_без_заготовок_карточка_не_уходит(self):
        """Карточка с пустым экраном за ней хуже, чем не отправленная карточка."""
        self.assertEqual(self._прогон(тренировал={ОКНО_ЧЕЛОВЕКА}, есть_заготовки=False), [])

    def test_выключенная_рассылка_молчит_и_в_капле(self):
        with mock.patch.object(bot_3, "_trainer_enabled", return_value=True), \
             mock.patch.object(bot_3, "_sprint_enabled", return_value=False), \
             mock.patch.object(bot_3, "_relation_gap_enabled", return_value=False), \
             mock.patch.object(bot_3, "pick_repeat_trainer", return_value=None), \
             mock.patch.object(bot_3, "pick_gap_word") as выбор:
            bot_3._drip_bonus_pass_done.clear()
            asyncio.run(bot_3._drip_bonus_pass(mock.MagicMock(), ОКНО_ЧЕЛОВЕКА, СЕГОДНЯ))
        выбор.assert_not_called()


class ТекстКомандыНеПротиворечитСебе(unittest.TestCase):
    """15.09.2026 владелец прочитал в ответе /gap_test: «Рассылка людям закрыта:
    RELATION_GAP_ENABLED = включено» — и справедливо не понял, открыта она или нет."""

    def test_no_self_contradicting_line(self):
        import inspect
        текст = inspect.getsource(bot_3._admin_gap_test_command)
        self.assertNotIn("Рассылка людям закрыта: RELATION_GAP_ENABLED", текст)
        self.assertIn("Рассылка людям ОТКРЫТА", текст)
        self.assertIn("Рассылка людям ЗАКРЫТА", текст)


if __name__ == "__main__":
    unittest.main()
