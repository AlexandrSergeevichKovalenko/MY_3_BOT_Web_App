"""Обработчик текста в группе с catch-all TypeHandler не срабатывает никогда.

Telegram-библиотека запускает в одной группе только ПЕРВЫЙ подошедший обработчик. В
group=-2 первым стоит TypeHandler(Update, enforce_user_access) — он подходит под любое
обновление, поэтому всё, что зарегистрировано в этой группе следом, мертво.

Так уже ломались платежи (комментарий в bot_3.py про successful_payment), так сломался
ответ владельца со списком слов для темы: он отвечал на просьбу, а слова уходили в
переводчик. Тест держит правило: у текстового обработчика, который должен срабатывать
по состоянию, — своя группа.
"""
import re
import unittest
from pathlib import Path

BOT = Path(__file__).resolve().parents[2] / "bot_3.py"


def _registrations() -> list[tuple[str, int]]:
    """[(имя обработчика, номер группы)] из add_handler(...) по всему файлу."""
    src = BOT.read_text(encoding="utf-8")
    out = []
    for m in re.finditer(r"add_handler\(\s*(.*?)\)\s*(?:,\s*group\s*=\s*(-?\d+))?\s*\)", src, re.S):
        body, group = m.group(1), m.group(2)
        name = re.search(r"(?:MessageHandler|TypeHandler)\([^,]+,\s*([A-Za-z_][\w.]*)", body)
        if name:
            out.append((name.group(1), int(group) if group else 0))
    return out


class HandlerGroupTests(unittest.TestCase):
    def test_state_driven_text_handlers_do_not_share_a_group(self):
        # Оба ждут своего состояния и обязаны получать каждое сообщение, чтобы решить,
        # их оно или нет. В общей группе выживет только первый.
        regs = dict(_registrations())
        words = regs.get("handle_manual_theme_words")
        describe = regs.get("handle_describe_custom_input")
        self.assertIsNotNone(words, "обработчик списка слов должен быть зарегистрирован")
        self.assertIsNotNone(describe, "обработчик «✏️ Своё» должен быть зарегистрирован")
        self.assertNotEqual(words, describe, "у каждого своя группа")

    def test_they_do_not_sit_with_the_access_gate(self):
        # enforce_user_access — TypeHandler(Update): подходит под любое обновление и
        # съедает свою группу целиком.
        regs = _registrations()
        gate = [g for n, g in regs if n == "enforce_user_access"]
        self.assertTrue(gate, "контроль доступа должен быть зарегистрирован")
        busy = set(gate)
        for name in ("handle_manual_theme_words", "handle_describe_custom_input"):
            group = dict(regs).get(name)
            self.assertNotIn(group, busy, f"{name} стоит в группе с catch-all — не сработает")

    def test_they_run_before_the_translator(self):
        # Переводчик ловит любой текст в group=1. Наши обработчики должны получить
        # сообщение раньше, иначе слова уйдут на перевод.
        regs = dict(_registrations())
        translator = regs.get("handle_user_message")
        self.assertIsNotNone(translator)
        for name in ("handle_manual_theme_words", "handle_describe_custom_input"):
            self.assertLess(regs[name], translator, f"{name} должен идти раньше переводчика")


if __name__ == "__main__":
    unittest.main()
