# -*- coding: utf-8 -*-
"""Перезаливка книги не отнимает у читателя место, на котором он остановился.

Повод (22.08.2026). Починив разборщик, я перезалил 13 обрезанных книг классики — и
закладок стало 2 вместо 9. Причина: при смене текста `upsert_public_library_document`
УДАЛЯЛА строку книги и вставляла новую, с новым номером. А закладки читателей
(`bt_3_reader_public_progress`) и прогретое аудио ссылаются на этот номер через
ON DELETE CASCADE — и уходили вместе со старой строкой.

Сегодня это было 7 человек. На тысячах читателей это закладка КАЖДОГО, каждый раз,
когда мы улучшаем разбор книги. Поэтому строка книги теперь обновляется НА МЕСТЕ.

Тест структурный: он читает исходник функции. Проверять это прогоном по базе нельзя —
тесты не ходят в боевую базу, а именно боевая база и пострадала.
"""

import inspect
import re
import unittest

import backend.database as database


class ReingestKeepsBookmarkTests(unittest.TestCase):
    def setUp(self):
        self.source = inspect.getsource(database.upsert_public_library_document)

    def test_changed_text_updates_the_row_instead_of_deleting_it(self):
        deletes_the_book = re.search(
            r"DELETE\s+FROM\s+bt_3_reader_library", self.source, re.IGNORECASE
        )
        self.assertIsNone(
            deletes_the_book,
            "перезаливка снова удаляет строку книги — закладки читателей уйдут каскадом",
        )

    def test_the_new_text_is_written_into_the_same_row(self):
        self.assertRegex(
            self.source,
            r"UPDATE\s+bt_3_reader_library[\s\S]{0,600}?content_pages\s*=",
            "новый текст обязан попадать в ТУ ЖЕ строку книги",
        )

    def test_stale_audio_of_the_old_pagination_is_dropped_explicitly(self):
        """Старая озвучка привязана к прежним границам страниц и совпасть не может —
        её убираем явно, а не оставляем мёртвыми строками."""
        self.assertRegex(
            self.source, r"DELETE\s+FROM\s+bt_3_reader_audio_pages",
        )

    def test_public_progress_still_hangs_on_the_document_id(self):
        """Если это когда-нибудь перестанет быть правдой — тест выше потеряет смысл."""
        schema = inspect.getsource(database)
        self.assertIn("bt_3_reader_public_progress", schema)
        self.assertRegex(
            schema,
            r"document_id\s+BIGINT\s+NOT NULL\s+REFERENCES\s+bt_3_reader_library\(id\)\s+ON DELETE CASCADE",
        )


if __name__ == "__main__":
    unittest.main()
