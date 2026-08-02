"""Расход «Загадочной истории» ложится на того, кто её играет.

Функция платная (`_paid_surface_gate_response(feature="story_mode")`), модель зовёт
конкретный человек, и по правилу владельца такой вызов пишется на него. Замерено на
боевой ведомости 02.08.2026: деньги были, а человека не было —

    generate_mystery_story          2 вызова, $0.0065, user_id=NULL
    check_translation_story_arena   3 вызова, $0.0041, user_id=NULL
    check_story_guess_semantic      3 вызова, $0.0019, user_id=NULL

рядом лежали пометки story_start_generation / story_submit_check / story_explain — они
человека знали, но денег не несли. Дневной потолок считает СУММУ cost_amount по user_id
(get_today_cost_eur), поэтому расход в него не попадал вообще.

Чинится тем же приёмом, что и воркер проверки переводов: обработчик ставит
set_llm_billing_user на время вызова и снимает в finally (потоки веб-сервера
переиспользуются, забытый id уехал бы на следующий запрос).
"""
import re
import unittest
from pathlib import Path

from backend import openai_manager as om


_SERVER = Path(__file__).resolve().parent.parent / "backend_server.py"

# Задачи, которые на самом деле зовут модель под этими тремя обработчиками.
STORY_LLM_TASKS = (
    "generate_mystery_story",
    "check_translation_story_arena",
    "check_story_guess_semantic",
    "check_story_explanation_core",
    "check_story_explanation_meta",
)

# Обработчики Flask и вызов, вокруг которого обязана стоять привязка.
STORY_HANDLERS = (
    ("start_story_session_webapp", "generate_mystery_story"),
    ("submit_story_translation_webapp", "check_translation_story_arena"),
    ("explain_story_translation_webapp", "check_story_explanation_core"),
)


class StoryBillingUserTests(unittest.TestCase):
    def setUp(self):
        self.source = _SERVER.read_text(encoding="utf-8")

    def test_story_tasks_are_personal_not_house(self):
        # Если задачу занесут в список «ничьих», привязка перестанет действовать молча:
        # _resolve_billing_user_for_task принудительно вернёт None.
        for task in STORY_LLM_TASKS:
            with self.subTest(task=task):
                self.assertEqual(
                    om._resolve_billing_user_for_task(task, 117649764),
                    117649764,
                    f"{task} помечена как общая — расход снова перестанет попадать в потолок игрока",
                )

    def test_each_story_handler_sets_and_clears_the_billing_user(self):
        for call_name, _task in STORY_HANDLERS:
            with self.subTest(handler=call_name):
                index = self.source.find(f"{call_name}(\n")
                self.assertNotEqual(index, -1, f"вызов {call_name} не найден — тест устарел")
                window = self.source[max(0, index - 1200):index + 1200]
                self.assertIn(
                    "set_llm_billing_user(int(user_id))", window,
                    f"перед {call_name} не выставлен плательщик — расход уйдёт в NULL",
                )
                self.assertIn(
                    "set_llm_billing_user(None)", window,
                    f"после {call_name} плательщик не снят — id утечёт на следующий запрос "
                    "в том же потоке веб-сервера",
                )

    def test_the_billing_user_is_cleared_in_finally_not_after_the_call(self):
        # Снятие обязано стоять в finally: при ошибке модели ранний return оставил бы
        # чужой id в потоке.
        for call_name, _task in STORY_HANDLERS:
            with self.subTest(handler=call_name):
                index = self.source.find(f"{call_name}(\n")
                window = self.source[index:index + 1500]
                match = re.search(r"\n    finally:\n        set_llm_billing_user\(None\)", window)
                self.assertIsNotNone(
                    match,
                    f"{call_name}: снятие плательщика не в finally — при ошибке id останется висеть",
                )

    def test_the_story_surface_is_paid_only(self):
        # Расход кладём на человека именно потому, что функция платная. Если гейт уберут,
        # бесплатный пользователь начнёт жечь чужой потолок — тест напомнит.
        self.assertIn('feature="story_mode"', self.source)


if __name__ == "__main__":
    unittest.main()
