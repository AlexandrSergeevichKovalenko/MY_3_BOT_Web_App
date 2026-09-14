"""Личное письмо новичку от владельца: уходит один раз, говорит правду, считает неудачи.

Владелец 14.09.2026 попросил письмо «от Александра» новому человеку. Здесь проверяется
то, что легко сломать незаметно:

  • письмо уходит РОВНО ОДИН РАЗ — учёт ведётся до следующего прогона, а не в памяти;
  • «не дошло» записывается причиной и считается, а не выглядит как «дошло»;
  • текст не обещает кнопку, которой в письме нет (у аккаунта владельца нет @username);
  • сроки в письме те же, что в замке доступа: 7 дней полного, дальше «Лайт» до 30-го.
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import bot_3


class WelcomeLetterTextTests(unittest.TestCase):
    def test_letter_names_the_real_trial_terms(self):
        """7 дней и 23 дня «Лайт» — то же, что стоит в замке доступа."""
        текст = bot_3._welcome_letter_text("Мария", contact="dm")
        self.assertIn("Мария", текст)
        self.assertIn("7 дней", текст)
        self.assertIn("23 дня", текст)
        self.assertIn("Лайт", текст)
        self.assertIn("онбординг", текст)

    def test_wording_matches_what_the_button_actually_does(self):
        """Текст не имеет права расходиться с кнопкой.

        14.09.2026 у аккаунта владельца не оказалось публичного @username, письмо ушло
        вовсе без кнопки — «свяжись со мной» без адреса. Теперь адрес есть всегда, и под
        каждый вид кнопки написан свой текст."""
        личка = bot_3._welcome_letter_text("Иван", contact="dm")
        self.assertIn("кнопка под этим письмом", личка)

        поддержка = bot_3._welcome_letter_text("Иван", contact="support")
        self.assertIn("кнопка под этим письмом откроет раздел «Поддержка»", поддержка)

        без_кнопки = bot_3._welcome_letter_text("Иван", contact="none")
        self.assertNotIn("кнопка под этим письмом", без_кнопки)
        self.assertIn("«Поддержка»", без_кнопки)

    def test_no_name_means_no_invented_greeting(self):
        """Имени нет — здороваемся без имени, а не подставляем «друг» или id."""
        текст = bot_3._welcome_letter_text("", contact="dm")
        self.assertTrue(текст.startswith("👋 Привет!"), текст[:40])

    def test_keyboard_drops_the_contact_button_when_there_is_no_address(self):
        без = bot_3._welcome_letter_keyboard("")
        self.assertEqual(1, len(без.inline_keyboard))
        с = bot_3._welcome_letter_keyboard("https://t.me/someone")
        self.assertEqual(2, len(с.inline_keyboard))


class WelcomeLetterJobTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.ctx = MagicMock()
        self.ctx.bot.send_message = AsyncMock()

    async def _run(self, candidates, *, send_effect=None, contact=("https://t.me/dev", "dm")):
        if send_effect is not None:
            self.ctx.bot.send_message.side_effect = send_effect
        with patch.object(bot_3, "list_welcome_letter_candidates", return_value=candidates), \
             patch.object(bot_3, "welcome_letter_first_name", return_value="Анна"), \
             patch.object(bot_3, "record_welcome_letter_sent") as ушло, \
             patch.object(bot_3, "record_welcome_letter_failure",
                          return_value="undeliverable") as не_ушло, \
             patch.object(bot_3, "_welcome_letter_contact", new=AsyncMock(return_value=contact)):
            итог = await bot_3._welcome_letter_job(self.ctx)
        return итог, ушло, не_ушло

    async def test_letter_goes_out_and_is_written_down(self):
        """Каждому кандидату — одно письмо, и каждое записано: иначе завтра уйдёт второе."""
        итог, ушло, не_ушло = await self._run([111111, 222222])
        self.assertEqual(2, итог["sent"])
        self.assertEqual(0, итог["failed"])
        self.assertEqual([111111, 222222], [c.args[0] for c in ушло.call_args_list])
        не_ушло.assert_not_called()
        self.assertEqual(2, self.ctx.bot.send_message.await_count)

    async def test_closed_dm_is_counted_not_swallowed(self):
        """Личка закрыта — это «не смогли» с причиной и счётчиком, а не тихий успех."""
        итог, ушло, не_ушло = await self._run(
            [333333], send_effect=RuntimeError("Forbidden: bot was blocked by the user"))
        self.assertEqual(0, итог["sent"])
        self.assertEqual(1, итог["failed"])
        self.assertEqual(1, итог["closed"])
        ушло.assert_not_called()
        причина = не_ушло.call_args.args[1]
        self.assertIn("Forbidden", причина)

    async def test_one_closed_dm_does_not_stop_the_rest(self):
        """Один недоступный человек не отменяет письма остальным."""
        отказ = RuntimeError("Forbidden")
        итог, ушло, _ = await self._run(
            [1, 2, 3], send_effect=[отказ, None, None])
        self.assertEqual(2, итог["sent"])
        self.assertEqual(1, итог["failed"])
        self.assertEqual([2, 3], [c.args[0] for c in ушло.call_args_list])

    async def test_nobody_to_write_means_no_messages(self):
        итог, ушло, _ = await self._run([])
        self.assertEqual({"candidates": 0, "sent": 0, "failed": 0, "closed": 0}, итог)
        self.ctx.bot.send_message.assert_not_awaited()

    async def test_missing_username_still_leaves_a_working_button(self):
        """Нет @username — кнопка ведёт в «Поддержку», и текст говорит именно про неё.

        Это и есть жалоба владельца 14.09.2026: «а где кнопка связаться со мной?»"""
        await self._run([444444], contact=("https://t.me/bot?startapp=support", "support"))
        отправка = self.ctx.bot.send_message.await_args.kwargs
        self.assertIn("откроет раздел «Поддержка»", отправка["text"])
        кнопки = [b.text for row in отправка["reply_markup"].inline_keyboard for b in row]
        self.assertEqual(2, len(кнопки), кнопки)
        self.assertTrue(any("Написать" in т for т in кнопки), кнопки)


if __name__ == "__main__":
    unittest.main()
