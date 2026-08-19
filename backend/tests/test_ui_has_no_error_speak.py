"""Страж: в интерфейсе не должно быть «ошибок» и красных технических плашек.

Владелец, 19.08.2026, дословно:

    «Это пользовательское приложение, оно не должно содержать ошибок.
     Пользователь вообще не должен понимать, что такое ошибка! У него их быть не должно!»

Повод: кнопка «Проверить перевод» при пустых полях выдавала красную плашку
«Bitte fülle mindestens eine Übersetzung aus.» — тем же каналом, которым падают сбои
сервера. При этом на ЭТОМ ЖЕ экране две соседние подсказки уже показывались нашей
модалкой `NoticeModal`. Класс починили наполовину и бросили — этот тест не даёт
второй половине отвалиться обратно.

Три правила, каждое — про свой способ вернуть дефект:

  1. Подсказка-валидация («ещё не заполнено», «ещё не выбрано») не уходит в канал
     ошибок `set*Error`. Ей место в `showNoticeModal`.
  2. Слово «Ошибка» / «Fehler» не встречается в тексте, который читает человек.
     В учебном смысле («работа над ошибками») — можно, это про немецкий, не про софт.
  3. Экран краха не печатает пользователю текст исключения и стек.

Тест читает исходники фронта — так же, как это делает `test_paid_surface_gates.py`.
"""
import re
import unittest
from pathlib import Path

FRONTEND_SRC = Path(__file__).resolve().parents[2] / "frontend" / "src"

# Фразы, которыми говорят С ЧЕЛОВЕКОМ, а не сообщают о сбое. Если такая фраза стоит
# первым аргументом у set*Error — значит, подсказку опять положили в канал ошибок.
VALIDATION_MARKERS = re.compile(
    r"(Сначала|Заполни|Заполните|Выбери|Выберите|Введи|Введите|Напиши|Напишите"
    r"|Укажи|Укажите|Добавь|Добавьте|Отметь|Отметьте|Вставь|Вставьте"
    r"|хотя бы один|минимум один|не должно быть пустым"
    r"|zuerst|Bitte gib|Bitte fülle|Bitte schreib|Bitte wähl|Wähle |mindestens"
    r"|darf nicht leer)"
)

# «Ошибка» в учебном смысле — законная и нужная часть продукта: работа над ошибками,
# разбор ошибок ученика, счётчик «Fehler: 3» в игре. Поэтому ловим не слово, а ФОРМУ,
# в которой о сбое говорит программа, а не учитель:
#   • русская строка НАЧИНАЕТСЯ с «Ошибка» («Ошибка сохранения», «Ошибка загрузки»);
#   • «Произошла ошибка» / «Неизвестная ошибка»;
#   • немецкое сложное слово-ярлык в начале строки: Ladefehler, Startfehler, Speicherfehler…
#     («Fehleranalyse», «Fehlerdatenbank», «Fehler pro Satz» так НЕ ловятся — и правильно);
#   • канцелярское «Fehler bei / beim …» и «Fehler.» как самостоятельное сообщение.
SYSTEM_ERROR_SPEAK = re.compile(
    r"^(Ошибка\b"
    r"|Произошла ошибка|Неизвестная ошибка"
    r"|[A-ZÄÖÜ][\wÄÖÜäöüß-]*fehler\b"
    r"|Fehler (bei|beim)\b"
    r"|(Unbekannter|Unerwarteter|Interner) Fehler\b"
    r"|Fehler\.)"
)

ERROR_SETTER_CALL = re.compile(r"set[A-Za-z]*Error\(")


def _jsx_sources():
    for path in sorted(FRONTEND_SRC.rglob("*.jsx")):
        yield path, path.read_text(encoding="utf-8")


def _call_arguments(source: str, start: int) -> str:
    """Текст аргументов вызова, начиная сразу после открывающей скобки."""
    depth = 1
    out = []
    for char in source[start:start + 600]:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                break
        out.append(char)
    return "".join(out)


class UiHasNoErrorSpeakTests(unittest.TestCase):
    def test_validation_hints_do_not_go_into_the_error_channel(self):
        offenders = []
        for path, source in _jsx_sources():
            for match in ERROR_SETTER_CALL.finditer(source):
                args = _call_arguments(source, match.end())
                literals = re.findall(r"'((?:[^'\\]|\\.)*)'", args)
                text = " | ".join(item for item in literals if item.strip())
                if not text or not VALIDATION_MARKERS.search(text):
                    continue
                line = source[: match.start()].count("\n") + 1
                offenders.append(f"{path.relative_to(FRONTEND_SRC)}:{line} → {text[:90]}")

        self.assertEqual(
            offenders,
            [],
            "Подсказка человеку уехала в канал ошибок и нарисуется плашкой. "
            "Показывайте её через showNoticeModal({emoji, title, message}):\n"
            + "\n".join(offenders),
        )

    def test_user_facing_copy_never_says_error(self):
        offenders = []
        for path, source in _jsx_sources():
            for line_number, line in enumerate(source.split("\n"), start=1):
                for literal in re.findall(r"'((?:[^'\\]|\\.)*)'", line):
                    if not SYSTEM_ERROR_SPEAK.match(literal.strip()):
                        continue
                    offenders.append(
                        f"{path.relative_to(FRONTEND_SRC)}:{line_number} → {literal[:90]}"
                    )

        self.assertEqual(
            offenders,
            [],
            "Слово «ошибка» ушло в текст для человека. Скажите, ЧТО не получилось и "
            "ЧТО сделать, а техническое — в console.warn:\n" + "\n".join(offenders),
        )

    def test_crash_screen_shows_no_stack_trace(self):
        source = (FRONTEND_SRC / "App.jsx").read_text(encoding="utf-8")
        boundary_start = source.index("class ErrorBoundary")
        boundary = source[boundary_start : source.index("\n}\n", boundary_start)]
        self.assertNotIn("error.stack", boundary)
        self.assertNotIn("webapp-error-stack", boundary)
        self.assertNotIn("this.state.error?.message", boundary)


if __name__ == "__main__":
    unittest.main()
