"""Правило «немецкий текст пишется мимо двери» — и один источник списка на всё.

╔══════════════════════════════════════════════════════════════════════════════════╗
║  ЗАЧЕМ ЭТО ПРАВИЛО В ПРОВЕРКЕ ЦЕЛОСТНОСТИ                                        ║
║                                                                                  ║
║  Оно единственное смотрит не в данные, а В КОД, — и это ровно то, ради чего вся   ║
║  проверка заведена: она сторожит СТРАЖЕЙ. Данные сегодня чистые именно потому,    ║
║  что на каждом из четырнадцати путей записи стоит дверь. Уберут дверь или заведут ║
║  новый путь в обход — грязь начнёт копиться заново, и владелец увидит её не       ║
║  раньше, чем на экране у человека. С этим правилом он видит её тем же утром.      ║
║                                                                                  ║
║  Владелец, 22.08.2026: «А как мне понять, кто конкретно их делает, сделали ли     ║
║  они их или нет?» — ответ не список, который каждый помечает сам, а число,        ║
║  прочитанное из кода и приходящее утренним отчётом.                              ║
╚══════════════════════════════════════════════════════════════════════════════════╝
"""
import pathlib
import unittest
from unittest.mock import patch

from backend.dictionary_integrity import RULES
from backend.write_doors import DOORS, PLACES, SAFE_MARK, body_of, inspect

ROOT = pathlib.Path(__file__).resolve().parents[2]
RULE_TITLE = "немецкий текст пишется мимо двери"


class TheRuleIsInTheMorningReportTests(unittest.TestCase):
    def test_the_rule_is_part_of_the_integrity_check(self):
        self.assertIn(RULE_TITLE, [title for title, _rule in RULES])

    def test_the_rule_counts_places_without_a_door(self):
        """Считает открытые, а не все подряд — иначе число в отчёте бессмысленно."""
        rule = next(rule for title, rule in RULES if title == RULE_TITLE)
        fake = [
            {"number": 1, "human": "дверь есть", "state": "ok", "doors": ["чистка"]},
            {"number": 2, "human": "проверено", "state": "safe", "doors": []},
            {"number": 3, "human": "двери нет", "state": "open", "doors": []},
            {"number": 4, "human": "пропало из кода", "state": "missing", "doors": []},
        ]
        with patch("backend.write_doors.inspect", return_value=fake):
            count, sample = rule(None)
        self.assertEqual(count, 2, "открытое и пропавшее — оба требуют работы")
        self.assertEqual([item[0] for item in sample], [3, 4])

    def test_the_rule_does_not_touch_the_database(self):
        """Курсора у него нет и быть не должно: правило читает код, а не данные."""
        rule = next(rule for title, rule in RULES if title == RULE_TITLE)
        count, _sample = rule(None)          # None вместо курсора — намеренно
        self.assertIsInstance(count, int)


class TheListLivesInOnePlaceTests(unittest.TestCase):
    def test_the_report_and_the_script_share_one_source(self):
        """Два списка разойдутся, и отчёт начнёт врать. Поэтому показ импортирует.

        Ровно так тема «размноженного текста» открывалась четыре раза подряд: каждый
        заход заводил себе свой признак и находил «что-то новое».
        """
        script = (ROOT / "scripts/dict_write_doors_audit.py").read_text(encoding="utf-8")
        self.assertIn("from backend.write_doors import", script)
        self.assertNotIn("PLACES = (", script, "у показа завёлся свой список")

    def test_every_place_names_a_real_file(self):
        for number, path, _name, human in PLACES:
            with self.subTest(number=number):
                self.assertTrue((ROOT / path).exists(), f"{human}: файла {path} нет")


class TheDoorIsRecognisedHonestlyTests(unittest.TestCase):
    def test_a_place_with_a_cleaner_is_closed(self):
        with patch("backend.write_doors.body_of", return_value="x = clean_text(y)"):
            self.assertTrue(all(item["state"] == "ok" for item in inspect()))

    def test_a_place_with_a_refusal_is_closed_too(self):
        """Две двери — разные. Место может иметь одну и не иметь другой."""
        with patch("backend.write_doors.body_of",
                   return_value="if mangled_strings_inside(card): return"):
            states = {item["state"] for item in inspect()}
        self.assertEqual(states, {"ok"})

    def test_a_place_with_neither_is_open(self):
        with patch("backend.write_doors.body_of", return_value="cursor.execute(sql)"):
            self.assertTrue(all(item["state"] == "open" for item in inspect()))

    def test_a_place_marked_safe_is_not_counted_as_work(self):
        """Проверенное и неопасное помечается В КОДЕ, рядом с объяснением почему."""
        with patch("backend.write_doors.body_of",
                   return_value=f"# {SAFE_MARK}\ncursor.execute(sql)"):
            self.assertTrue(all(item["state"] == "safe" for item in inspect()))

    def test_a_vanished_place_is_work_too(self):
        """Функцию переименовали или удалили — это тоже повод посмотреть, а не тишина."""
        with patch("backend.write_doors.body_of", return_value=""):
            self.assertTrue(all(item["state"] == "missing" for item in inspect()))

    def test_function_body_stops_at_the_next_definition(self):
        """Тело берётся точно: иначе место «закрывается» дверью из соседней функции.

        На первом прогоне разбор захватывал перевод строки перед `def`, тело выходило
        длиной в один символ, и проверка объявила открытыми все места подряд —
        включая только что закрытые.
        """
        body = body_of("backend/write_doors.py", "places_without_a_door")
        self.assertIn("places_without_a_door", body)
        self.assertNotIn("def inspect(", body)
        self.assertGreater(len(body), 50)


class TheDoorTokensStayMeaningfulTests(unittest.TestCase):
    def test_both_kinds_of_door_are_known(self):
        self.assertEqual(set(DOORS), {"чистка", "отказ"})

    def test_no_token_is_so_short_it_matches_anything(self):
        """Короткий признак («add», «set») находился бы в любом файле и врал бы «закрыто»."""
        for kind, tokens in DOORS.items():
            for token in tokens:
                with self.subTest(kind=kind, token=token):
                    self.assertGreater(len(token), 8)


if __name__ == "__main__":
    unittest.main()
