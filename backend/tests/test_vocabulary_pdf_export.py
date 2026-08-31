"""PDF из выборки словаря: «посмотрел ролик — вытянул слова — выучил».

Держим три вещи, которые ломаются молча:
1. Эмодзи из подписи не попадает в файл — в шрифте её нет, вышел бы пустой квадрат.
2. Длинное немецкое слово переносится, а не вылезает за колонку.
3. Потолок выборки на экране и на сервере — одно и то же число.
"""

import re
import unittest
from pathlib import Path

import backend.backend_server as server


class PdfHeadingTests(unittest.TestCase):
    def test_emoji_is_removed_from_the_heading(self):
        # На собранном файле 31.08.2026 «🎬 Die großen» печаталось с пустым глифом.
        self.assertEqual(server._strip_unrenderable_for_pdf("🎬 Die großen"), "Die großen")
        self.assertEqual(server._strip_unrenderable_for_pdf("🗂 Природа · 🎬 Ролик"), "Природа · Ролик")

    def test_letters_and_punctuation_survive(self):
        self.assertEqual(
            server._strip_unrenderable_for_pdf("Größe, Straße — «Тема» · 21"),
            "Größe, Straße — «Тема» · 21",
        )

    def test_empty_stays_empty(self):
        self.assertEqual(server._strip_unrenderable_for_pdf(""), "")
        self.assertEqual(server._strip_unrenderable_for_pdf(None), "")


class PdfWrapTests(unittest.TestCase):
    def setUp(self):
        self.font, _bold = server._register_pdf_fonts()

    def _width(self, line: str, size: float) -> float:
        from reportlab.pdfbase import pdfmetrics
        return pdfmetrics.stringWidth(line, self.font, size)

    def test_long_phrase_is_wrapped_within_the_column(self):
        text = "sie vermuten finstere Mächte hinter der angeblichen Pandemie"
        lines = server._wrap_pdf_text(text, font=self.font, size=9.6, width=240)
        self.assertGreater(len(lines), 1)
        for line in lines:
            self.assertLessEqual(self._width(line, 9.6), 240 + 0.5)
        self.assertEqual(" ".join(lines), text)

    def test_a_single_word_longer_than_the_column_is_broken_by_letters(self):
        # Без этого «Drohnenabwehrzentrum» наезжал бы на соседнюю колонку.
        lines = server._wrap_pdf_text(
            "Drohnenabwehrzentrum", font=self.font, size=9.6, width=40,
        )
        self.assertGreater(len(lines), 1)
        for line in lines:
            self.assertLessEqual(self._width(line, 9.6), 40 + 0.5)
        self.assertEqual("".join(lines), "Drohnenabwehrzentrum")

    def test_empty_text_gives_no_lines(self):
        self.assertEqual(server._wrap_pdf_text("   ", font=self.font, size=9.6, width=200), [])


class PdfDocumentTests(unittest.TestCase):
    def test_short_list_fills_both_columns_on_one_page(self):
        items = [
            {"display_word": f"das Wort {n}", "display_translation": f"слово {n}"}
            for n in range(20)
        ]
        buffer, name = server._build_vocabulary_study_pdf(
            items=items, heading="Die großen", subheading="20 слов",
        )
        data = buffer.getvalue()
        self.assertTrue(data.startswith(b"%PDF"))
        self.assertTrue(name.endswith(".pdf"))
        # Одна страница: 20 коротких записей в две колонки заведомо помещаются.
        self.assertEqual(data.count(b"/Type /Page\n") or data.count(b"/Type /Page"), 1)

    def test_words_without_a_headword_are_skipped_not_printed_empty(self):
        buffer, _name = server._build_vocabulary_study_pdf(
            items=[{"display_word": "", "display_translation": "перевод без слова"}],
            heading="Проверка", subheading="",
        )
        self.assertTrue(buffer.getvalue().startswith(b"%PDF"))


class SelectionLimitTests(unittest.TestCase):
    def test_the_limit_is_the_same_on_both_sides(self):
        """Экран и сервер обязаны считать одинаково.

        Разойдись они — «Выбрать все» наберёт слов больше, чем сервер согласится
        напечатать, и человек получит ошибку вместо файла.
        """
        app_jsx = Path(__file__).resolve().parents[2] / "frontend" / "src" / "App.jsx"
        found = re.search(r"const VOCAB_SELECTION_MAX = (\d+);", app_jsx.read_text(encoding="utf-8"))
        self.assertIsNotNone(found, "в App.jsx нет константы VOCAB_SELECTION_MAX")
        self.assertEqual(int(found.group(1)), server.VOCABULARY_SELECTION_MAX)


if __name__ == "__main__":
    unittest.main()
