"""Личный расход «Загадочной истории» ложится на игрока, генерация — на заведение.

Функция платная (`_paid_surface_gate_response(feature="story_mode")`). Правило владельца:
на человека — то, что никому больше не пригодится; на дом — всё, что переиспользуется.
Разбор ЕГО перевода, ЕГО догадки и ЕГО ошибок никому больше не нужен → на игрока.
Генерация истории уходит в ОБЩИЙ банк bt_3_story_bank и предлагается другим на арене
(`WHERE sc.user_id != exclude_user_id`), повторная игра модель не зовёт → на заведение,
иначе за общий банк платит тот, кто открыл историю первым.

Замерено на боевой ведомости 02.08.2026: деньги были, а человека не было —

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

# Личная работа: разбирают ИМЕННО его перевод и его догадку, никому больше не пригодится.
STORY_PERSONAL_TASKS = (
    "check_translation_story_arena",
    "check_story_guess_semantic",
    "check_story_explanation_core",
    "check_story_explanation_meta",
)

# За счёт заведения: генерация наполняет ОБЩИЙ банк bt_3_story_bank, и арена предлагает
# готовую историю другим игрокам (`WHERE sc.user_id != exclude_user_id`); при повторной
# игре модель не зовут вообще. Если это записать на человека, за общий банк заплатит тот,
# кто открыл историю первым, а остальные сыграют за его счёт.
STORY_HOUSE_TASKS = ("generate_mystery_story",)

# Обработчики Flask и вызов, вокруг которого обязана стоять привязка.
STORY_HANDLERS = (
    ("start_story_session_webapp", "generate_mystery_story"),
    ("submit_story_translation_webapp", "check_translation_story_arena"),
    ("explain_story_translation_webapp", "check_story_explanation_core"),
)


class StoryBillingUserTests(unittest.TestCase):
    def setUp(self):
        self.source = _SERVER.read_text(encoding="utf-8")

    def test_personal_story_work_lands_on_the_player(self):
        # Если такую задачу занесут в список «ничьих», привязка перестанет действовать
        # молча: _resolve_billing_user_for_task принудительно вернёт None.
        for task in STORY_PERSONAL_TASKS:
            with self.subTest(task=task):
                self.assertEqual(
                    om._resolve_billing_user_for_task(task, 117649764),
                    117649764,
                    f"{task} помечена как общая — разбор ЕГО работы перестанет попадать в его потолок",
                )

    def test_story_generation_is_paid_by_the_house(self):
        # Генерация наполняет общий банк, поэтому обязана оставаться ничьей ДАЖЕ когда
        # обработчик выставил плательщика.
        for task in STORY_HOUSE_TASKS:
            with self.subTest(task=task):
                self.assertIsNone(
                    om._resolve_billing_user_for_task(task, 117649764),
                    f"{task} записалась бы на игрока — но история уходит в общий банк "
                    "bt_3_story_bank и предлагается другим на арене, значит платит заведение",
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
