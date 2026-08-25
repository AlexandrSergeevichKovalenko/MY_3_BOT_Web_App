# -*- coding: utf-8 -*-
"""Перезаливка книги обязана убрать и САМИ mp3, а не только строки о них.

Повод (25.08.2026). Перезаливка классики 22.08 пересобрала страницы и удалила строки
`bt_3_reader_audio_pages`, но объекты в R2 остались лежать: у «Märchen» — 278 mp3 на
325.6 МБ. Недельная уборка R2 их не видит по устройству: она удаляет файлы УДАЛЁННЫХ
книг, а книга жива. Ключ объекта содержит отпечаток текста страницы, поэтому к новой
нарезке они не подойдут никогда — это мусор навсегда и повторный расход на TTS.

Удалять безопасно, потому что страницу читалка ищет ТОЛЬКО через строку в базе
(`get_cached_reader_audio_page`): нет строки — объект недостижим.

Порядок обязателен: сперва фиксируем удаление строк, потом трогаем файлы. Наоборот —
откат транзакции оставит живую строку, у которой mp3 уже удалён.

Проверяется структурно: тест не ходит в боевую базу, а пострадала именно она.
"""

import inspect
import re
import unittest

import backend.database as database


class ReingestRemovesOrphanedAudioFilesTests(unittest.TestCase):
    def setUp(self):
        self.source = inspect.getsource(database.upsert_public_library_document)
        self.helper = inspect.getsource(database._purge_book_audio_objects_after_repagination)

    def test_every_audio_row_wipe_also_wipes_the_files(self):
        """Оба места, где снимается озвучка (страницы пересобраны / текст изменился)."""
        chunks = self.source.split("DELETE FROM bt_3_reader_audio_pages")
        self.assertGreaterEqual(len(chunks) - 1, 2, "мест снятия озвучки стало меньше двух — проверь функцию")
        for i, tail in enumerate(chunks[1:], start=1):
            head = tail[:1200]
            self.assertIn(
                "_purge_book_audio_objects_after_repagination", head,
                f"место #{i}: строки озвучки удалены, а mp3 в R2 остаются мусором навсегда",
            )

    def test_files_are_touched_only_after_the_rows_are_committed(self):
        for tail in self.source.split("DELETE FROM bt_3_reader_audio_pages")[1:]:
            head = tail[:1200]
            commit_at = head.find("conn.commit()")
            purge_at = head.find("_purge_book_audio_objects_after_repagination")
            self.assertNotEqual(commit_at, -1, "нет фиксации перед удалением файлов — откат оставит строку без mp3")
            self.assertLess(commit_at, purge_at, "файлы удаляются ДО фиксации строк — при откате строка останется без mp3")

    def test_purge_targets_exactly_one_book(self):
        self.assertIn("reader-audio-pages/", self.helper)
        self.assertRegex(self.helper, r"owner_id.*document_id|\{int\(owner_id\)\}/\{int\(document_id\)\}")
        self.assertIn("r2_delete_prefix", self.helper)

    def test_storage_failure_is_reported_not_swallowed(self):
        """Ошибка R2 не должна уходить в тишину: строк уже нет, остаток надо видеть."""
        self.assertIn("logging.warning", self.helper)
        self.assertIsNone(
            re.search(r"except Exception:\s*\n\s*(pass|return \[\])", self.helper),
            "ошибка хранилища глушится молча",
        )


if __name__ == "__main__":
    unittest.main()
