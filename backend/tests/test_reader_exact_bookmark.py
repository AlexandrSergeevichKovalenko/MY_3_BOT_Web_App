"""Точная закладка читалки (27.08.2026).

Владелец 27.08.2026: «Закладка почему-то на 2-х страницах книги находится» и
«когда я открываю книгу, она не открывается на закладке».

Корень был один: закладка знала только СТРАНИЦУ. Читалка режет книгу на серверные
страницы по ~1100 символов, а на экран влезает меньше, поэтому одна страница = 2-3
экрана — и флажок висел на всех. Здесь проверяется серверная половина починки:
точный якорь «страница + символ ВНУТРИ страницы» доходит до базы и возвращается
при открытии книги. Экранная половина (один флажок на один экран) живёт во фронте,
в ReaderSection.readerBookmarkOnThisScreen.
"""

import unittest
from unittest.mock import patch

import backend.backend_server as server


class ReaderExactBookmarkTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = server.app.test_client()

    def _auth_patches(self):
        return [
            patch.object(server, "_telegram_hash_is_valid", return_value=True),
            patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 55}}),
            patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")),
            patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {})),
        ]

    def test_state_endpoint_stores_the_exact_anchor(self):
        """Кнопка закладки шлёт страницу И символ внутри неё — оба доходят до базы."""
        captured = {}

        def fake_update(**kwargs):
            captured.update(kwargs)
            return {"id": 17, "bookmark_percent": 42.5}

        patches = self._auth_patches() + [
            patch.object(server, "update_reader_library_state", side_effect=fake_update),
        ]
        for p in patches:
            p.start()
        try:
            response = self.client.post(
                "/api/webapp/reader/library/state",
                json={
                    "initData": "valid",
                    "document_id": 17,
                    "bookmark_percent": 42.5,
                    "bookmark_page": 96,
                    "bookmark_char": 743,
                },
            )
        finally:
            for p in patches:
                p.stop()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured.get("bookmark_page"), 96)
        self.assertEqual(captured.get("bookmark_char"), 743)

    def test_progress_only_save_never_touches_the_anchor(self):
        """Обычное перелистывание шлёт только прогресс — закладка обязана уцелеть.

        Если бы сюда приходили нули, каждый перелистанный экран стирал бы точную
        закладку и она снова стала бы «страничной», то есть на два экрана.
        """
        captured = {}

        def fake_update(**kwargs):
            captured.update(kwargs)
            return {"id": 17}

        patches = self._auth_patches() + [
            patch.object(server, "update_reader_library_state", side_effect=fake_update),
        ]
        for p in patches:
            p.start()
        try:
            response = self.client.post(
                "/api/webapp/reader/library/state",
                json={"initData": "valid", "document_id": 17, "progress_percent": 12.0},
            )
        finally:
            for p in patches:
                p.stop()

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(captured.get("bookmark_page"))
        self.assertIsNone(captured.get("bookmark_char"))

    def test_public_book_anchor_goes_to_the_per_user_side_table(self):
        """Классика лежит одной общей строкой, поэтому её якорь — в личной таблице."""
        captured = {}

        def fake_upsert(**kwargs):
            captured.update(kwargs)
            return {
                "progress_percent": 0.0,
                "bookmark_percent": 42.5,
                "reading_mode": "vertical",
                "bookmark_page": 96,
                "bookmark_char": 743,
            }

        patches = self._auth_patches() + [
            # Личной строки нет → путь публичной книги.
            patch.object(server, "update_reader_library_state", return_value=None),
            patch.object(server, "get_public_library_document", return_value={"id": 17}),
            patch.object(server, "upsert_public_reader_progress", side_effect=fake_upsert),
        ]
        for p in patches:
            p.start()
        try:
            response = self.client.post(
                "/api/webapp/reader/library/state",
                json={
                    "initData": "valid",
                    "document_id": 17,
                    "bookmark_percent": 42.5,
                    "bookmark_page": 96,
                    "bookmark_char": 743,
                },
            )
        finally:
            for p in patches:
                p.stop()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured.get("bookmark_page"), 96)
        self.assertEqual(captured.get("bookmark_char"), 743)
        payload = response.get_json()
        self.assertEqual(payload["document"]["bookmark_page"], 96)
        self.assertEqual(payload["document"]["bookmark_char"], 743)

    def test_anchor_is_not_fetched_for_a_still_processing_book(self):
        """У книги, которую ещё разбирают, позиции чтения нет — и запроса за ней тоже."""
        pending_doc = {
            "id": 17,
            "title": "Queued book",
            "source_type": "epub",
            "source_url": None,
            "processing_status": "processing",
            "processing_error": None,
        }
        patches = self._auth_patches() + [
            patch.object(server, "get_reader_library_document", return_value=pending_doc),
            patch.object(server, "get_reader_library_bookmark_anchor"),
        ]
        started = [p.start() for p in patches]
        anchor_mock = started[-1]
        try:
            response = self.client.post(
                "/api/webapp/reader/library/open",
                json={"initData": "valid", "document_id": 17},
            )
        finally:
            for p in patches:
                p.stop()

        self.assertEqual(response.status_code, 202)
        anchor_mock.assert_not_called()


class ReaderBookmarkFrontendGuardTests(unittest.TestCase):
    """Сторожа на фронт: компонентных тестов в проекте нет, а вернуться этим двум
    строкам легче всего — обе выглядят безобидно и обе ломают закладку целиком."""

    @staticmethod
    def _read(rel_path: str) -> str:
        import pathlib
        root = pathlib.Path(__file__).resolve().parents[2]
        return (root / rel_path).read_text(encoding="utf-8")

    def test_open_does_not_force_page_one_by_layout_mode(self):
        """Книга открывается на закладке, а не на первой странице.

        getReaderPreferredLayoutMode() с 10.07.2026 возвращает 'custom' для ЛЮБОГО
        формата, поэтому условие `preferredLayoutMode === 'custom' ? 1 : …` означало
        «всегда страница 1» — ровно то, на что пожаловался владелец 27.08.2026.
        """
        source = self._read("frontend/src/App.jsx")
        self.assertNotIn(
            "const initialPage = preferredLayoutMode === 'custom'",
            source,
            "Открытие книги снова сброшено на страницу 1 по режиму раскладки — "
            "закладка перестала работать (разбор 27.08.2026).",
        )
        self.assertIn("const initialPage = usesOriginalEpub", source)

    def test_ribbon_is_bound_to_the_screen_not_to_the_server_page(self):
        """Флажок закладки живёт на ОДНОМ экране.

        Серверная страница ~1100 символов растягивается на 2-3 экрана, а флажок
        лежит в неподвижной рамке просмотра, а не в едущем тексте: привязка к
        странице снова показала бы его дважды.
        """
        source = self._read("frontend/src/components/ReaderSection.jsx")
        self.assertIn(
            "{readerBookmarkOnThisScreen && (\n                      <span className=\"reader-page-bookmark-indicator\"",
            source,
            "Флажок в колоночном движке снова привязан не к экрану — он покажется "
            "на всех экранах одной серверной страницы (жалоба 27.08.2026).",
        )


if __name__ == "__main__":
    unittest.main()
