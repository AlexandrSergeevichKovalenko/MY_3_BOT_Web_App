import re
import unittest

import backend.backend_server as server


class ReaderPdfNormalizationTests(unittest.TestCase):
    def test_normalize_reader_pages_for_response_repairs_old_pdf_pages(self):
        pages = [
            {"page_number": 9, "text": "Geleitwort von Frank Hoepfel . . . . . . .\n. . . . . . . . . . . . 1"},
            {"page_number": 10, "text": "Definition und Aufgabenspektrum der\nforensischen Medizin 1.1 Definition . . .\n. . . . . . . . 7 1.2"},
        ]

        normalized = server._normalize_reader_pages_for_response("pdf", pages)

        self.assertEqual(len(normalized), 2)
        self.assertIn("Geleitwort von Frank Hoepfel", normalized[0]["text"])
        self.assertIn("........", normalized[0]["text"])
        self.assertIn("1.1 Definition", normalized[1]["text"])
        self.assertNotIn("\n\n\n", normalized[1]["text"])

    def test_pdf_toc_lines_are_compacted_into_entries(self):
        raw = """
Geleitwort von Frank Hoepfel. . . . . . . . . . . . .
. . . . . . . . . . . . . . . . . . . . . . . . . 1
Vorwort der Autoren. . . . . . . . . . . . . . . . .

3 Vorwort der Autoren zur 2. Auflage. . . . . . . . .
. . . . . . . . . . . . . . . . . . . . . . . . . 5 1
Definition und Aufgabenspektrum der
forensischen Medizin 1.1 Definition . . . . . . . . .
. . . . . . . . . . . . . . . . . . . . . . . . . 7 1.2
Aufgabenspektrum . . . . . . . . . . . . . . . . . .
. . . . . . . . . . . . . . . . . . . . . . . . . 7
        """.strip()

        normalized = server._normalize_pdf_extracted_page_text(raw)
        lines = [line for line in normalized.splitlines() if line.strip()]

        self.assertGreaterEqual(len(lines), 4)
        self.assertTrue(any("Geleitwort von Frank Hoepfel" in line for line in lines))
        self.assertTrue(any("Vorwort der Autoren" in line for line in lines))
        self.assertTrue(any("Definition und Aufgabenspektrum der forensischen Medizin 1.1 Definition" in line for line in lines))
        self.assertTrue(any(re.search(r"\.{4,}\s+1$", line) for line in lines))
        self.assertTrue(any(re.search(r"\.{4,}\s+5 1$", line) for line in lines))
        self.assertTrue(any(re.search(r"\.{4,}\s+7 1\.2$", line) for line in lines))
        self.assertFalse(any(re.fullmatch(r"[.\-–—_·• ]{4,}", line) for line in lines))

    def test_pdf_body_lines_are_reflowed(self):
        raw = """
Dies ist ein laengerer Absatz mit einem Zeilenumbruch
und er sollte wieder als Fliesstext erscheinen.

Das gilt auch fuer eine zweite Zeile,
wenn sie nur durch das PDF-Layout getrennt wurde.
        """.strip()

        normalized = server._normalize_pdf_extracted_page_text(raw)

        self.assertIn(
            "Dies ist ein laengerer Absatz mit einem Zeilenumbruch und er sollte wieder als Fliesstext erscheinen.",
            normalized,
        )
        self.assertIn(
            "Das gilt auch fuer eine zweite Zeile, wenn sie nur durch das PDF-Layout getrennt wurde.",
            normalized,
        )


if __name__ == "__main__":
    unittest.main()
