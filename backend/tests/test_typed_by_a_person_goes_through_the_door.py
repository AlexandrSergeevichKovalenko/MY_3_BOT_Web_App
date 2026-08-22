"""Там, где текст печатает ЧЕЛОВЕК, вход идёт через ту же дверь, что и всё остальное.

╔══════════════════════════════════════════════════════════════════════════════════╗
║  ЗАКРЫТО 22.08.2026. Три места из карты «немецкий текст мимо всех проверок».      ║
║                                                                                  ║
║  Карту собрал соседний агент: четырнадцать мест пишут немецкий текст в три        ║
║  хранилища, а чистка стоит на четырёх. Владелец распорядился закрывать; места,    ║
║  где печатает человек, взял я. Остальные поделены по номерам между агентами.      ║
╚══════════════════════════════════════════════════════════════════════════════════╝

ЧТО ЗДЕСЬ ПРОВЕРЯЕТСЯ И ПОЧЕМУ ИМЕННО ЭТО

Во всех трёх местах стоял голый `.strip()`, и в базу ложилось ровно то, что набрано:

    правка карточки в мини-аппе        `edit_vocabulary_entry`
    «разбить карточку на значения»     `split_vocabulary_entry_senses`
    «пересобрать неверно написанное»   `reset_dictionary_card_for_rebuild`

Грязь при этом не выдумана: невидимые символы приезжают из буфера обмена, двойные
пробелы — из набора на телефоне, а «ä» существует в двух начертаниях (одной буквой и
буквой со значком сверху). После такого одно и то же слово перестаёт быть равно самому
себе: не находится поиском, задваивается в словаре, не сходится с общей записью.

Третье место опаснее двух первых: оттуда написание уезжает в ПЯТЬ мест сразу и по нему
же ночь заново ПОКУПАЕТ разбор у модели. Грязь там стоит денег, а не только вида.

`clean_text` смысл не меняет никогда — только убирает грязь — и идемпотентна, поэтому
чистый ввод проходит через неё без изменений.
"""
import pathlib
import re
import unittest

from backend.dictionary_intake import clean_text

BACKEND = pathlib.Path(__file__).resolve().parents[1]

# Место в коде → как называется для человека.
DOORS = {
    "edit_vocabulary_entry": "правка карточки человеком",
    "split_vocabulary_entry_senses": "«разбить карточку на значения»",
    "reset_dictionary_card_for_rebuild": "«пересобрать неверно написанное слово»",
}


def _body_of(name: str) -> str:
    text = (BACKEND / "database.py").read_text(encoding="utf-8")
    start = text.index(f"def {name}(")
    rest = text[start:]
    nxt = re.search(r"\ndef ", rest[1:])
    return rest[: nxt.start()] if nxt else rest


class TypedTextGoesThroughTheDoorTests(unittest.TestCase):
    def test_every_human_entry_point_calls_the_cleaner(self):
        for name, human in DOORS.items():
            with self.subTest(name):
                body = _body_of(name)
                self.assertIn(
                    "clean_text", body,
                    f"{human}: текст человека кладётся мимо чистки")

    def test_no_bare_strip_is_left_on_the_typed_value(self):
        """Голый `.strip()` на том, что набрал человек, — это и есть обойдённая дверь."""
        for name, human in DOORS.items():
            with self.subTest(name):
                body = _body_of(name)
                bare = re.findall(
                    r'str\((?:corrected_word|word_de|translation_ru|item|sense\.get\("value"\))'
                    r'[^)]*\)\s*\.strip\(\)', body)
                self.assertEqual(
                    bare, [],
                    f"{human}: осталась голая обрезка пробелов вместо чистки: {bare}")


class TheCleanerDoesNotChangeMeaningTests(unittest.TestCase):
    """Дверь обязана быть безопасной, иначе её начнут обходить снова.

    Если чистка портит нормальный ввод, следующий разработчик просто уберёт её у себя —
    и место откроется заново. Поэтому проверяем не только «чистит», но и «не мешает».
    """

    def test_normal_german_passes_untouched(self):
        for text in ("die Schifffahrt", "Es ist mir latte", "der Bombenanschlag",
                     "Er erlag der Versuchung.", "sich mit etw. wappnen",
                     "Wie geht's?", "Sie sollen sich haben scheiden lassen"):
            self.assertEqual(clean_text(text), text, text)

    def test_normal_russian_passes_untouched(self):
        for text in ("Положи кошелек в карман.", "быть занятым чем-либо; заниматься чем-то",
                     "Пролить-пролил- пролил", "мне всё равно"):
            self.assertEqual(clean_text(text), text, text)

    def test_the_dirt_that_actually_arrives_is_removed(self):
        # Невидимый разделитель из буфера обмена.
        self.assertEqual(clean_text("die​Gaze"), "dieGaze")
        # Двойные пробелы с телефона.
        self.assertEqual(clean_text("sterile  Gaze"), "sterile Gaze")
        # Пробелы по краям.
        self.assertEqual(clean_text("  Gaze  "), "Gaze")
        # «ä» одной буквой и «a» со значком сверху — на вид одно, для базы разное.
        self.assertEqual(clean_text("Mädchen"), clean_text("Mädchen"))

    def test_cleaning_twice_changes_nothing(self):
        """Идемпотентность — то, что позволяет ставить чистку и у вызывающего, и внутри."""
        for text in ("  sterile  Gaze ", "die​Gaze", "Mädchen", ""):
            once = clean_text(text)
            self.assertEqual(clean_text(once), once, text)


if __name__ == "__main__":
    unittest.main()
