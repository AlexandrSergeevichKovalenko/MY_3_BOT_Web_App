# -*- coding: utf-8 -*-
"""Чужой формат — честный отказ, а не «книга» из двоичного мусора.

Повод (22.08.2026). Всё, что не PDF и не EPUB, декодировалось как UTF-8 с
errors="ignore" и записывалось в библиотеку как текст книги. Загруженный .docx
лёг в базу «книгой» на 45 083 знака, начинающейся с «PK\\x03\\x04…[Content_Types].xml».
Человек видел не отказ, а открывшуюся книгу с абракадаброй внутри.

Формат опознаётся по СОДЕРЖИМОМУ (сигнатуры форматов), потому что расширение и
MIME теряются при пересылке через мессенджер (там всё приходит octet-stream).
"""

import base64
import io
import unittest
import zipfile

import backend.backend_server as server


def _docx_bytes() -> bytes:
    """Минимальный настоящий .docx — это ZIP с [Content_Types].xml внутри."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "[Content_Types].xml",
            '<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/'
            'package/2006/content-types"/>',
        )
        archive.writestr("word/document.xml", "<w:document><w:body/></w:document>")
    return buffer.getvalue()


def _fb2_bytes() -> bytes:
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<FictionBook xmlns="http://www.gribuser.ru/xml/fictionbook/2.0">'
        "<body><section><p>Das Leben ist schwer.</p></section></body></FictionBook>"
    ).encode("utf-8")


def _mobi_bytes() -> bytes:
    return b"\x00" * 60 + b"BOOKMOBI" + b"\x00" * 200


class ReaderUploadFormatTests(unittest.TestCase):
    def _ingest(self, raw: bytes, *, file_name: str, file_mime: str = ""):
        return server._resolve_reader_ingest_content(
            input_text="",
            input_url="",
            file_name=file_name,
            file_mime=file_mime,
            file_content_b64=base64.b64encode(raw).decode(),
        )

    def test_docx_is_refused_with_a_human_message(self):
        with self.assertRaises(ValueError) as caught:
            self._ingest(_docx_bytes(), file_name="Diplomarbeit.docx")
        message = str(caught.exception)
        self.assertIn("DOCX", message)
        self.assertIn("EPUB", message)
        self.assertNotIn("Traceback", message)

    def test_fb2_is_refused_even_though_it_is_valid_text(self):
        """FB2 — это XML: раньше он проходил как «текст» и читался разметкой наружу."""
        with self.assertRaises(ValueError) as caught:
            self._ingest(_fb2_bytes(), file_name="buch.fb2")
        self.assertIn("FB2", str(caught.exception))

    def test_mobi_is_refused(self):
        with self.assertRaises(ValueError) as caught:
            self._ingest(_mobi_bytes(), file_name="buch.mobi")
        self.assertIn("MOBI", str(caught.exception))

    def test_binary_file_without_extension_is_refused(self):
        """Мессенджер потерял имя и MIME — решает содержимое, а не догадка."""
        with self.assertRaises(ValueError):
            self._ingest(_docx_bytes(), file_name="", file_mime="application/octet-stream")

    def test_plain_text_still_works(self):
        raw = "Das Leben ist schwer.\n\nAber wir lesen weiter.".encode("utf-8")
        text, _pages, source_type, _url, _meta = self._ingest(raw, file_name="buch.txt")
        self.assertEqual(source_type, "file")
        self.assertIn("Das Leben ist schwer.", text)

    def test_epub_without_extension_is_still_recognised(self):
        """EPUB объявляет себя внутри архива — открываем даже без расширения."""
        from backend.tests.test_epub_book_arrives_whole import _build_epub, _chapter_html

        data = _build_epub([("kapitel1.xhtml", _chapter_html(paragraphs=60))])
        # Настоящий EPUB несёт «application/epub+zip» первым файлом архива.
        rebuilt = io.BytesIO()
        with zipfile.ZipFile(io.BytesIO(data)) as source:
            with zipfile.ZipFile(rebuilt, "w") as target:
                target.writestr("mimetype", "application/epub+zip")
                for name in source.namelist():
                    if name != "mimetype":
                        target.writestr(name, source.read(name))

        text, pages, source_type, _url, _meta = self._ingest(
            rebuilt.getvalue(), file_name="ohne_endung", file_mime="application/octet-stream"
        )
        self.assertEqual(source_type, "epub")
        self.assertGreater(len(pages), 1)
        self.assertIn("zu schwer für uns", text)

    def test_non_utf8_text_is_read_by_detecting_its_encoding(self):
        """Немецкий текст в cp1252 не должен терять умляуты по дороге."""
        raw = "Die Bücher öffnen sich für alle.".encode("cp1252")
        text, _pages, source_type, _url, _meta = self._ingest(raw, file_name="buch.txt")
        self.assertEqual(source_type, "file")
        self.assertIn("Bücher", text)
        self.assertIn("öffnen", text)


if __name__ == "__main__":
    unittest.main()
