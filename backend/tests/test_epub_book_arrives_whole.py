# -*- coding: utf-8 -*-
"""EPUB доезжает до читалки ЦЕЛИКОМ — и книга целиком, и все её главы.

Повод (22.08.2026). Владелец загрузил Фрейда «Das Unbehagen in der Kultur»
(e-artnow, ~150 000 знаков) — читалка показала ОДНУ страницу: титульный лист,
110 знаков. В Apple Books та же книга открывалась целиком.

Нашлись две дыры, обе в `_extract_epub_content_from_bytes`:

1. Страж «это оглавление, а не глава» решал по приметам: имя файла со словом
   index/toc/nav, слово «Inhaltsverzeichnis» где угодно в тексте, тег <nav>
   где угодно в разметке. Каждая примета выбрасывала ЦЕЛУЮ главу. Книга,
   которую конвертер положил одним файлом index.html, теряла весь текст.
2. Каждый документ книги молча обрезался на 50 000 знаках. Книга одним файлом
   кончалась на 56-й странице; у «Weltordnung» Киссинджера так пропало
   ~350 000 знаков из 1 119 000 (замер 22.08.2026).

Теперь оглавление берётся из ИСТОЧНИКА — из того, что книга сама о себе
объявила (EPUB3: properties="nav" в манифесте; EPUB2: <guide type="toc">), —
а при отсутствии объявления по ИЗМЕРЕНИЮ доли текста, лежащего в ссылках.
Обрезки главы нет вовсе.
"""

import io
import unittest
import zipfile

import backend.backend_server as server


def _build_epub(documents: list[tuple[str, str]], *, nav_href: str = "", guide_toc_href: str = "") -> bytes:
    """Собрать настоящий EPUB из пар (имя файла, html). Ничего не мокаем."""
    manifest_items = []
    spine_items = []
    for index, (file_name, _html) in enumerate(documents):
        item_id = f"doc{index}"
        properties = ' properties="nav"' if file_name == nav_href else ""
        manifest_items.append(
            f'<item id="{item_id}" href="{file_name}" '
            f'media-type="application/xhtml+xml"{properties}/>'
        )
        spine_items.append(f'<itemref idref="{item_id}"/>')

    guide_xml = ""
    if guide_toc_href:
        guide_xml = f'<guide><reference type="toc" title="Inhalt" href="{guide_toc_href}"/></guide>'

    opf = (
        '<?xml version="1.0"?>'
        '<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="pid">'
        '<metadata xmlns:dc="http://purl.org/dc/elements/1.1/">'
        '<dc:identifier id="pid">urn:test</dc:identifier>'
        '<dc:title>Testbuch</dc:title><dc:language>de</dc:language>'
        '</metadata>'
        f'<manifest>{"".join(manifest_items)}</manifest>'
        f'<spine>{"".join(spine_items)}</spine>'
        f'{guide_xml}'
        '</package>'
    )

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("mimetype", "application/epub+zip")
        archive.writestr(
            "META-INF/container.xml",
            '<?xml version="1.0"?>'
            '<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">'
            '<rootfiles><rootfile full-path="OEBPS/content.opf" '
            'media-type="application/oebps-package+xml"/></rootfiles></container>',
        )
        archive.writestr("OEBPS/content.opf", opf)
        for file_name, html in documents:
            archive.writestr(f"OEBPS/{file_name}", html)
    return buffer.getvalue()


def _chapter_html(paragraphs: int = 400, extra: str = "") -> str:
    body = "".join(
        f"<p>Das Leben, wie es uns auferlegt ist, ist zu schwer für uns — Satz {i}.</p>"
        for i in range(paragraphs)
    )
    return f"<html><body>{body}{extra}</body></html>"


TITLE_PAGE = (
    "<html><body><p><b>Sigmund Freud</b></p>"
    "<h1>Das Unbehagen in der Kultur</h1>"
    "<p>e-artnow, 2017</p></body></html>"
)


class EpubBookArrivesWholeTests(unittest.TestCase):
    def _pages(self, data: bytes) -> tuple[str, list]:
        return server._extract_epub_content_from_bytes(data)

    def test_book_in_a_file_named_index_html_is_not_thrown_away(self):
        """Конвертер назвал файл index.html — это НЕ повод считать его оглавлением."""
        data = _build_epub([("title.xhtml", TITLE_PAGE), ("index.html", _chapter_html())])
        text, pages = self._pages(data)
        self.assertGreater(len(pages), 20, "книга в index.html потерялась целиком")
        self.assertIn("zu schwer für uns", text)

    def test_word_inhaltsverzeichnis_inside_a_chapter_does_not_drop_it(self):
        """Слово «Inhaltsverzeichnis» внутри главы не делает главу оглавлением."""
        data = _build_epub([
            ("title.xhtml", TITLE_PAGE),
            ("kapitel1.xhtml", "<html><body><h2>Inhaltsverzeichnis</h2>"
                               + _chapter_html()[len("<html><body>"):]),
        ])
        text, pages = self._pages(data)
        self.assertGreater(len(pages), 20)
        self.assertIn("zu schwer für uns", text)

    def test_nav_tag_for_footnotes_does_not_drop_the_chapter(self):
        """Тег <nav> у сносок EPUB3 не делает главу навигацией."""
        chapter = _chapter_html(extra='<nav epub:type="footnotes"><a href="#f1">1</a></nav>')
        data = _build_epub([("title.xhtml", TITLE_PAGE), ("kapitel1.xhtml", chapter)])
        text, pages = self._pages(data)
        self.assertGreater(len(pages), 20)
        self.assertIn("zu schwer für uns", text)

    def test_declared_navigation_document_is_still_skipped(self):
        """А объявленное самой книгой оглавление в текст чтения по-прежнему не идёт."""
        # Настоящий EPUB3-навигатор — это <nav epub:type="toc">, так его и собираем.
        toc = (
            '<html xmlns:epub="http://www.idpf.org/2007/ops"><body>'
            '<nav epub:type="toc"><h1>Inhalt</h1><ol>'
            + "".join(f'<li><a href="kapitel1.xhtml#k{i}">Kapitel {i} — Der Anfang</a></li>'
                      for i in range(12))
            + "</ol></nav></body></html>"
        )
        data = _build_epub(
            [("nav.xhtml", toc), ("kapitel1.xhtml", _chapter_html())],
            nav_href="nav.xhtml",
        )
        text, pages = self._pages(data)
        self.assertGreater(len(pages), 20)
        self.assertNotIn("Kapitel 7 — Der Anfang", text)

    def test_epub2_guide_toc_is_still_skipped(self):
        """EPUB2 объявляет оглавление в <guide type="toc"> — тоже слушаемся книги."""
        toc = (
            "<html><body><h1>Inhalt</h1><ol>"
            + "".join(f'<li><a href="kapitel1.xhtml#k{i}">Kapitel {i} — Der Anfang</a></li>'
                      for i in range(12))
            + "</ol></body></html>"
        )
        data = _build_epub(
            [("inhalt.xhtml", toc), ("kapitel1.xhtml", _chapter_html())],
            guide_toc_href="inhalt.xhtml",
        )
        text, pages = self._pages(data)
        self.assertGreater(len(pages), 20)
        self.assertNotIn("Kapitel 7 — Der Anfang", text)

    def test_undeclared_toc_page_is_recognised_by_measurement(self):
        """Книга ничего не объявила — оглавление узнаём по доле текста в ссылках."""
        toc = (
            "<html><body><h1>Inhalt</h1><ol>"
            + "".join(f'<li><a href="kapitel1.xhtml#k{i}">Kapitel {i} — Der Anfang</a></li>'
                      for i in range(12))
            + "</ol></body></html>"
        )
        data = _build_epub([("seite1.xhtml", toc), ("kapitel1.xhtml", _chapter_html())])
        text, pages = self._pages(data)
        self.assertGreater(len(pages), 20)
        self.assertNotIn("Kapitel 7 — Der Anfang", text)

    def test_single_file_book_is_not_cut_at_50000_chars(self):
        """Книга одним файлом читается целиком, а не до 50 000 знаков."""
        data = _build_epub([("buch.xhtml", _chapter_html(paragraphs=2000))])
        text, pages = self._pages(data)
        self.assertGreater(len(text), 100_000, "книгу снова обрезали на середине")
        self.assertGreater(len(pages), 90)
        # Последний абзац книги обязан доехать до читалки.
        self.assertIn("Satz 1999.", text)

    def test_total_book_ceiling_is_the_only_limit_left(self):
        """Единственный оставшийся потолок — общий на книгу, и он честно соблюдается."""
        self.assertEqual(server._EPUB_MAX_TOTAL_TEXT_CHARS, 3_000_000)


if __name__ == "__main__":
    unittest.main()
