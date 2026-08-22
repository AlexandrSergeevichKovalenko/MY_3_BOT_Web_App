# -*- coding: utf-8 -*-
"""Книга открывается целиком — или человек получает честный отказ. Куска не бывает.

Повод (22.08.2026, владелец): «зачем мы берём какую-то из потолка цифру и применяем
её к какой-то книге? но пользователь хочет прочитать книгу, зачем ему обрезать её??
ну никто ж не хочет получить кусок книги».

Так и было: потолок 3 000 000 знаков стоял с первого коммита читалки, ни из чего не
выведенный, и книга сверх него просто кончалась молча — «Innere Medizin 2023» лежала
в базе ровно на 3 000 000 знаков.

Что измерено 22.08.2026 и почему потолок теперь именно такой:
  • хранение не ограничивает — 3 млн знаков это 1.7 МБ в базе при пределе 1 ГБ;
  • выдача не ограничивает — страницы режутся окном в самой базе;
  • ограничивает разбор при загрузке: ~28 МБ пика памяти на миллион знаков
    (3 млн → 205 МБ, 24 млн → 785 МБ) при 8 ГБ контейнера.
Отсюда 20 млн знаков — и ОТКАЗ вместо обрезка, если книга ещё больше.
"""

import io
import unittest
import zipfile

import backend.backend_server as server


def _epub_with_chars(nchars: int) -> bytes:
    paragraph = "<p>" + ("Das Leben, wie es uns auferlegt ist, ist zu schwer für uns. " * 5) + "</p>"
    body = paragraph * (nchars // 300 + 1)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("mimetype", "application/epub+zip")
        archive.writestr(
            "META-INF/container.xml",
            '<?xml version="1.0"?><container version="1.0" xmlns="urn:oasis:names:tc:'
            'opendocument:xmlns:container"><rootfiles><rootfile full-path="OEBPS/c.opf" '
            'media-type="application/oebps-package+xml"/></rootfiles></container>',
        )
        archive.writestr(
            "OEBPS/c.opf",
            '<?xml version="1.0"?><package xmlns="http://www.idpf.org/2007/opf" version="3.0" '
            'unique-identifier="i"><metadata xmlns:dc="http://purl.org/dc/elements/1.1/">'
            '<dc:identifier id="i">x</dc:identifier><dc:title>T</dc:title>'
            '<dc:language>de</dc:language></metadata><manifest><item id="c" href="b.xhtml" '
            'media-type="application/xhtml+xml"/></manifest><spine><itemref idref="c"/>'
            "</spine></package>",
        )
        archive.writestr("OEBPS/b.xhtml", f"<html><body>{body}</body></html>")
    return buffer.getvalue()


class ReaderNeverServesHalfABookTests(unittest.TestCase):
    def test_ceiling_is_derived_and_far_above_real_books(self):
        """Потолок один на все форматы и с большим запасом над живыми книгами."""
        self.assertEqual(
            server._EPUB_MAX_TOTAL_TEXT_CHARS, server._READER_BOOK_MAX_TOTAL_CHARS,
            "у EPUB и PDF должен быть ОДИН потолок, иначе они разъедутся снова",
        )
        # Самая толстая живая книга на 22.08.2026 — «Innere Medizin 2023», 3 млн знаков.
        self.assertGreaterEqual(server._READER_BOOK_MAX_TOTAL_CHARS, 20_000_000)

    def test_a_book_over_the_ceiling_is_refused_not_trimmed(self):
        """Сверх потолка — отказ с числами, а не обрезанная книга."""
        original = server._EPUB_MAX_TOTAL_TEXT_CHARS
        server._EPUB_MAX_TOTAL_TEXT_CHARS = 40_000
        try:
            with self.assertRaises(ValueError) as caught:
                server._extract_epub_content_from_bytes(_epub_with_chars(200_000))
        finally:
            server._EPUB_MAX_TOTAL_TEXT_CHARS = original
        message = str(caught.exception)
        self.assertIn("слишком большая", message)
        self.assertIn("млн знаков", message)

    def test_a_book_under_the_ceiling_arrives_whole(self):
        """Под потолком книга доезжает целиком, до последнего абзаца."""
        text, pages = server._extract_epub_content_from_bytes(_epub_with_chars(400_000))
        self.assertGreater(len(text), 380_000)
        self.assertGreater(len(pages), 300)

    def test_page_window_helper_exists_so_serving_does_not_scale_with_size(self):
        """Страницы обязаны резаться в базе — иначе потолок вернётся через тормоза."""
        from backend.database import get_reader_document_pages_window
        self.assertTrue(callable(get_reader_document_pages_window))


if __name__ == "__main__":
    unittest.main()
