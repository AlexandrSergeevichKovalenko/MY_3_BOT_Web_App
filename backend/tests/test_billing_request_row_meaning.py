"""Строка `provider='openai' + units_type='requests'` значит РОВНО ОДНО: один поход
к провайдеру, у которого замерены токены.

Замер боевой ведомости 02.08.2026 за 30 дней показал, что она значила два разных
события сразу. Один поиск слова, ушедший в модель, писал ДВЕ такие строки: счётчик
«человек посмотрел слово» (он же срабатывает на попадании в кеш) и сам замеренный
вызов из `_billing_log_openai_usage`. Отчёты складывали обе:

    dictionary_lookup              446 «обращений к OpenAI» вместо 92
    dictionary_openai_explanation   95 вместо 11
    dictionary_collocations         46 вместо 25
    theory_package_prepare          16 лишних поверх theory_generation
                                       и theory_practice_sentences

Из-за того же раздутого знаменателя процент попаданий в кеш показывал экономию хуже
настоящей. Читателей у этих строк четверо (admin_economics ×3, database.py — там прямо
написано «реальные походы наружу»), поэтому чинили у пишущего: счётчик обращения
человека теперь `provider='app_internal'` и в расчёты OpenAI не попадает, но остаётся
считаемым.

Тест сторожит границу: если кто-то снова назовёт счётчик обращением к провайдеру,
отчёт снова начнёт двоить — молча.
"""
import re
import unittest
from pathlib import Path


# Пусто — и должно оставаться пустым. Здесь жили три счётчика «Загадочной истории»
# (story_start_generation, story_submit_check, story_explain): их вызовы никто не замерял,
# поэтому пометка была единственной записью о том, что модель звали. 02.08.2026 замер
# добавлен — истории зовут модель через llm_execute, а обработчики теперь ставят
# set_llm_billing_user, так что расход пишется на человека и попадает в его дневной
# потолок. Пополнять список нельзя: если у нового действия нет замера, добавляйте замер,
# а не исключение.
UNMEASURED_CALLS_ALLOWLIST: set[str] = set()

_SERVER = Path(__file__).resolve().parent.parent / "backend_server.py"


def _billing_event_calls(source: str) -> list[str]:
    """Тело каждого вызова `_billing_log_event_safe(...)` — со сбалансированными скобками."""
    bodies: list[str] = []
    for match in re.finditer(r"_billing_log_event_safe\(", source):
        index = match.end()
        depth = 1
        while depth:
            char = source[index]
            depth += (char == "(") - (char == ")")
            index += 1
        bodies.append(source[match.end():index])
    return bodies


class RequestRowMeaningTests(unittest.TestCase):
    def setUp(self):
        self.source = _SERVER.read_text(encoding="utf-8")
        self.assertIn("_billing_log_event_safe(", self.source)

    def test_only_unmeasured_calls_may_claim_an_openai_request(self):
        claimed = set()
        for body in _billing_event_calls(self.source):
            if 'provider="openai"' not in body or 'units_type="requests"' not in body:
                continue
            action = re.search(r'action_type="([^"]+)"', body)
            claimed.add(action.group(1) if action else "<неизвестное действие>")

        unexpected = claimed - UNMEASURED_CALLS_ALLOWLIST
        self.assertFalse(
            unexpected,
            "Счётчик обращения человека назван походом к OpenAI: "
            f"{sorted(unexpected)}. Если у действия рядом есть _billing_log_openai_usage, "
            "вызов уже замерен — счётчику полагается provider='app_internal', иначе один "
            "вызов попадёт в отчёт дважды. Если замера нет — сначала добавьте замер.",
        )

    def test_the_dictionary_counters_stay_internal(self):
        # Точечная проверка тех действий, на которых двойной счёт был измерен.
        for action in (
            "dictionary_lookup",
            "dictionary_collocations",
            "dictionary_openai_explanation",
            "ask_gpt_daily",
            "theory_check_feedback",
            "theory_package_prepare",
            "story_start_generation",
            "story_submit_check",
            "story_explain",
        ):
            with self.subTest(action=action):
                counters = [
                    body for body in _billing_event_calls(self.source)
                    if f'action_type="{action}"' in body and 'units_type="requests"' in body
                ]
                self.assertTrue(counters, f"счётчик {action} исчез — проверьте, не сломан ли учёт")
                for body in counters:
                    self.assertIn('provider="app_internal"', body)

    def test_the_measured_logger_still_writes_a_real_openai_request(self):
        # Обратная сторона: замеренный вызов ОБЯЗАН писать строку 'requests' с
        # provider='openai', иначе отчёт перестанет видеть обращения вообще.
        measured = self.source.split("def _billing_log_openai_usage(", 1)
        self.assertEqual(len(measured), 2, "функция замера расхода пропала")
        body = measured[1].split("\ndef ", 1)[0]
        self.assertIn('units_type="requests"', body)
        self.assertIn('provider="openai"', body)


if __name__ == "__main__":
    unittest.main()
