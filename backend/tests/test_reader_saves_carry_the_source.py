# -*- coding: utf-8 -*-
"""Слово из читалки несёт свой источник: ролик у книги «Текст видео», книга, статья.

Владелец 07.09.2026: «Я хочу, чтобы сохранение слов из этого текста сохранялось с меткой
"название видео", из которого был этот текст сгенерирован — это же всё равно относится
к тому видео». И то же для книг и статей.

До 07.09.2026 читалка слала слово без источника: в списке «Откуда» оно падало в общую
группу «Читалка», а не под название ролика, хотя плеер тот же ролик подписывал.

Тест держит три вещи:
  • сервер называет источник по книге: видео → тот же ролик, что и плеер (kind youtube,
    key = идентификатор из нашей же ссылки), url/html → статья, остальное → книга;
    заглушка «Текст видео» названием не считается; чужая ссылка — не источник;
  • оба пути сохранения из читалки в App.jsx прикладывают источник (selectionSource);
  • обещание reader_saves_without_source стоит в реестре с ожиданием 0.
"""
import os
import re
import unittest
from pathlib import Path

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

ROOT = Path(__file__).resolve().parents[2]
APP_JSX = ROOT / "frontend" / "src" / "App.jsx"


class СерверНазываетИсточникПоКниге(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from backend import backend_server as server
        cls.server = server

    def test_video_book_points_at_the_same_video_as_the_player(self):
        source = self.server._reader_document_dictionary_source({
            "id": 41, "source_type": "video",
            "source_url": "https://youtu.be/nLiOMhqDvC8", "title": "Die großen Mythen",
        })
        self.assertEqual(source["kind"], "youtube")
        self.assertEqual(source["key"], "nLiOMhqDvC8", "ключ — идентификатор ролика, как у плеера")
        self.assertEqual(source["title"], "Die großen Mythen")
        self.assertEqual(source["document_id"], 41)

    def test_video_book_url_roundtrips_through_our_own_writer(self):
        url = self.server._video_text_source_url("abc_DEF-123")
        self.assertEqual(self.server._video_text_video_id_from_source_url(url), "abc_DEF-123")

    def test_placeholder_title_is_not_a_title(self):
        source = self.server._reader_document_dictionary_source({
            "id": 7, "source_type": "video",
            "source_url": "https://youtu.be/nLiOMhqDvC8",
            "title": self.server._VIDEO_TEXT_TITLE_PLACEHOLDER,
        })
        self.assertEqual(source["title"], "", "«Текст видео» — заглушка, а не название ролика")
        self.assertEqual(source["title_source"], "")

    def test_foreign_url_on_a_video_book_is_not_a_source(self):
        with self.assertLogs(level="WARNING"):
            source = self.server._reader_document_dictionary_source({
                "id": 7, "source_type": "video",
                "source_url": "https://example.com/watch?v=x", "title": "x",
            })
        self.assertIsNone(source)

    def test_articles_and_books_get_their_own_kind(self):
        article = self.server._reader_document_dictionary_source({
            "id": 5, "source_type": "url", "source_url": "https://www.spiegel.de/x", "title": "Spiegel",
        })
        self.assertEqual((article["kind"], article["key"], article["title"]), ("article", "5", "Spiegel"))
        html = self.server._reader_document_dictionary_source({"id": 6, "source_type": "html", "title": "H"})
        self.assertEqual(html["kind"], "article")
        for source_type in ("epub", "pdf", "text", "txt", "file"):
            book = self.server._reader_document_dictionary_source(
                {"id": 9, "source_type": source_type, "title": "Der Prozess"})
            self.assertEqual((book["kind"], book["key"], book["title"]), ("book", "9", "Der Prozess"), source_type)

    def test_every_kind_the_server_names_is_accepted_by_the_database_layer(self):
        from backend.database import _normalize_dictionary_source_kind
        for kind in set(self.server._DICTIONARY_SOURCE_KIND_BY_READER_SOURCE_TYPE.values()):
            self.assertEqual(_normalize_dictionary_source_kind(kind), kind)

    def test_unknown_book_or_type_is_no_source(self):
        self.assertIsNone(self.server._reader_document_dictionary_source(None))
        self.assertIsNone(self.server._reader_document_dictionary_source({"source_type": "epub", "title": "x"}))
        with self.assertLogs(level="WARNING"):
            self.assertIsNone(self.server._reader_document_dictionary_source(
                {"id": 3, "source_type": "hologram", "title": "x"}))


class ЧиталкаПрикладываетИсточник(unittest.TestCase):
    def setUp(self):
        self.source = APP_JSX.read_text(encoding="utf-8")

    def test_every_reader_save_sends_the_source(self):
        """Каждый запрос к /api/webapp/dictionary/save, чей origin_process бывает
        'reader', обязан нести source и номер книги. Окно — от вызова fetch до конца
        его тела: тела не сливаются, как при поиске по отступам."""
        windows = []
        for m in re.finditer(r"fetch\('/api/webapp/dictionary/save'", self.source):
            end = self.source.index("\n      });", m.start()) if "\n      });" in self.source[m.start():m.start() + 6000] else m.start() + 6000
            end = min(end, self.source.index("\n          });", m.start()) if "\n          });" in self.source[m.start():m.start() + 6000] else end)
            windows.append(self.source[m.start():end])
        reader_windows = [w for w in windows if re.search(
            r"origin_process:\s*(?:saveOriginProcess|isYoutubeSelectionContext\(\) \? 'youtube' : 'reader')", w)]
        self.assertEqual(len(reader_windows), 2, "путей сохранения из читалки два: быстрое и из шита разбора")
        for w in reader_windows:
            self.assertIn("source: selectionSource", w,
                          "сохранение из читалки не шлёт источник — слово упадёт в общую «Читалку»")
        # Номер книги едет в origin_meta: у быстрого сохранения пометка собирается выше
        # вызова (saveOriginMeta), у шита разбора — прямо в теле запроса.
        quick_meta = re.search(r"const saveOriginMeta = \{(.*?)\n      \};", self.source, re.S)
        self.assertIsNotNone(quick_meta, "saveOriginMeta не найден")
        self.assertIn("isReaderInline && readerDocumentId ? { document_id: Number(readerDocumentId) }", quick_meta.group(1))
        sheet = [w for w in reader_windows if "isYoutubeSelectionContext() ? 'youtube' : 'reader'" in w]
        self.assertEqual(len(sheet), 1)
        self.assertIn("readerDocumentId ? { document_id: Number(readerDocumentId) }", sheet[0])

    def test_reader_source_comes_from_the_server_and_only_for_the_open_book(self):
        builder = re.search(r"const buildReaderSourcePayload = \(\) => \{(.*?)\n  \};", self.source, re.S)
        self.assertIsNotNone(builder, "buildReaderSourcePayload не найден")
        body = builder.group(1)
        self.assertIn("readerDictionarySource", body)
        self.assertIn("Number(source.document_id || 0) !== Number(readerDocumentId || 0)", body,
                      "карточка предыдущей книги не должна подписывать слова из следующей")
        self.assertNotIn("readerSourceUrl", body, "фронт не разбирает ссылку сам — источник называет сервер")
        self.assertEqual(self.source.count("setReaderDictionarySource(data?.dictionary_source"), 2,
                         "оба пути открытия готовой книги (открытие и мгновенный ingest) берут источник от сервера")

    def test_server_attaches_the_source_to_the_reader_responses(self):
        server_src = (ROOT / "backend" / "backend_server.py").read_text(encoding="utf-8")
        self.assertEqual(server_src.count('"dictionary_source": _reader_document_dictionary_source('), 2)


class ОбещаниеСтоитВРеестре(unittest.TestCase):
    def test_promise_registered(self):
        from backend.fix_promises import by_key
        promise = by_key("reader_saves_without_source")
        self.assertIsNotNone(promise)
        self.assertEqual(promise.expected, 0)


if __name__ == "__main__":
    unittest.main()
