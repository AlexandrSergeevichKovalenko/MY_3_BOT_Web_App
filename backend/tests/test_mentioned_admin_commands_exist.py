# -*- coding: utf-8 -*-
"""Команда, названная в тексте бота, ОБЯЗАНА иметь обработчик.

ПОВОД, 28.08.2026. Утренний отчёт о словаре звал «Разбор: /admin_dict_integrity»
(backend/dictionary_integrity.py:358) с 25.08.2026. Обработчика не существовало ни
одного: владелец нажимал на ссылку, и не происходило НИЧЕГО — ни ответа, ни ошибки.
Готовый текст всё это время лежал в dictionary_integrity.report_lines(), и его никто
не звал.

Это класс, а не случай: ссылку на команду пишут в отчёте, а обработчик добавляют
отдельной правкой в bot_3.py, и второй шаг легко забыть — язык об этом не напомнит.
Тест ловит именно разрыв между «позвали» и «есть кому ответить».

┌─ ПРОВЕРЕНО 28.08.2026. НЕ ПОДНИМАТЬ КАК НОВУЮ НАХОДКУ. ─────────────────────────┐
│ Сначала я сканировал ВСЕ токены вида /слово: 1 327 «упоминаний», 1 126 «без      │
│ обработчика». Это мусор, а не находка — в выборку попали ключи JSON, пути и      │
│ куски URL («/actions», «/api», «/added_by»). Сырое число сюда не годится.        │
│ Класс сузили до /admin_* — их 78, однозначные и все до одного команды бота.      │
│ Единственное исключение — путь к файлу backend/admin_command_catalog.py, он      │
│ отсекается по хвосту «.py». После починки без обработчика осталось 0.            │
│ Перемерить: тот же прогон, число в MISSING обязано быть пустым.                  │
└─────────────────────────────────────────────────────────────────────────────────┘
"""
import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]

# Имя команды рядом с «/», но не хвост файлового пути вроде «backend/admin_x.py».
MENTION = re.compile(r"/(admin_[a-z0-9_]+)\b(?!\.py)")
REGISTERED = re.compile(r'CommandHandler\(\s*["\']([a-zA-Z0-9_]+)["\']')


class MentionedAdminCommandsExist(unittest.TestCase):
    def test_every_mentioned_admin_command_has_a_handler(self):
        bot_source = (ROOT / "bot_3.py").read_text(encoding="utf-8")
        registered = set(REGISTERED.findall(bot_source))
        self.assertGreater(len(registered), 100,
                           "обработчики перестали находиться — сломался разбор bot_3.py, "
                           "а не продукт; чинить регулярку REGISTERED")

        mentions: dict[str, set[str]] = {}
        sources = [ROOT / "bot_3.py"] + sorted((ROOT / "backend").glob("*.py"))
        for path in sources:
            text = path.read_text(encoding="utf-8", errors="replace")
            for name in MENTION.findall(text):
                mentions.setdefault(name, set()).add(path.name)
        self.assertGreater(len(mentions), 50,
                           "упоминания команд перестали находиться — сломался разбор, "
                           "а не продукт; чинить регулярку MENTION")

        missing = {n: files for n, files in mentions.items() if n not in registered}
        self.assertEqual(
            {}, missing,
            "эти команды названы в текстах, но обработчика у них нет — нажатие не "
            "сделает НИЧЕГО: "
            + "; ".join(f"/{n} (в {', '.join(sorted(f))})" for n, f in sorted(missing.items())),
        )


if __name__ == "__main__":
    unittest.main()
