# -*- coding: utf-8 -*-
"""Страница читалки — экран телефона, и в EPUB тоже, а не «сколько вышло».

Повод (22.08.2026). У Фрейда «Das Unbehagen in der Kultur» вышло 117 страниц при
176 448 знаках — 1506 знаков на страницу вместо заявленных 1100, 48% страниц длиннее
1500 знаков, самая длинная 3733. У книг Гутенберга среднее было в норме (935 и 984).

Причина: правило «страница ≈ 1100 знаков» написано в ДВУХ местах, и одно неполное.
Путь PDF/текста (`_repaginate_reader_text_fixed`) режет слишком длинный абзац по
предложениям; путь EPUB (`_split_chapter_into_pages_structured`) не резал вообще —
длинный абзац становился страницей любой длины. У авторов с длинными абзацами счётчик
врал, а озвучка получала куски вдвое длиннее экрана. Одна и та же книга в PDF и в EPUB
давала разное число страниц — ровно та болезнь, ради которой единицу и вводили
(сверено с Apple Books: 960 000 знаков ≈ 755 страниц, см. комментарий у
_EPUB_PAGE_SPLIT_CHARS).

Замер после правки на живых книгах: Фрейд 117 → 191 страница (среднее 922, максимум
1755), Kissinger «Weltordnung» максимум 2943 → 1758, «Effi Briest» 7085 → 1752.
"""

import io
import unittest
import zipfile

import backend.backend_server as server


def _epub_with_one_huge_paragraph() -> bytes:
    """Одна глава из одного очень длинного абзаца — как пишет Фрейд."""
    sentence = "Das Leben, wie es uns auferlegt ist, ist zu schwer für uns. "
    huge_paragraph = sentence * 120          # ~7000 знаков, точки на месте
    body = f"<p>{huge_paragraph}</p>"
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


class ReaderPageIsOneScreenTests(unittest.TestCase):
    def test_a_wall_of_text_does_not_become_one_giant_page(self):
        _text, pages = server._extract_epub_content_from_bytes(_epub_with_one_huge_paragraph())
        longest = max(len(p["text"]) for p in pages)
        self.assertGreater(len(pages), 4, "абзац-стена так и остался одной страницей")
        self.assertLess(
            longest, server._EPUB_PAGE_SPLIT_CHARS * 1.7,
            f"страница длиной {longest} знаков — это не экран телефона",
        )

    def test_page_formatting_ranges_stay_inside_their_page(self):
        """Разрез абзаца обязан сохранять смещения — иначе оформление съедет."""
        _text, pages = server._extract_epub_content_from_bytes(_epub_with_one_huge_paragraph())
        for page in pages:
            page_length = len(page["text"])
            for block in page.get("blocks") or []:
                self.assertGreaterEqual(block["start"], 0)
                self.assertLessEqual(block["end"], page_length)
                self.assertLessEqual(block["start"], block["end"])

    def test_both_formats_use_the_same_page_unit(self):
        self.assertEqual(server._EPUB_PAGE_SPLIT_CHARS, server._READER_FIXED_PAGE_CHARS)

    def test_a_paragraph_without_a_single_full_stop_is_not_chopped_by_letters(self):
        """Сплошную строку без точек резать по буквам нельзя — отдаём как есть."""
        chunks = server._split_long_paragraph_with_offsets("abc " * 800, 1100)
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0][0], 0)

    def test_chunk_offsets_point_at_the_real_place_in_the_paragraph(self):
        paragraph = "Erster Satz. Zweiter Satz! Dritter Satz? Vierter Satz."
        for offset, chunk in server._split_long_paragraph_with_offsets(paragraph, 20):
            self.assertEqual(paragraph[offset:offset + len(chunk)], chunk)


if __name__ == "__main__":
    unittest.main()
