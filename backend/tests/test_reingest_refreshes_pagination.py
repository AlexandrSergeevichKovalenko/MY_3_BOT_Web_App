# -*- coding: utf-8 -*-
"""Перезаливка книги обязана пересобрать СТРАНИЦЫ, даже если текст тот же.

Повод (22.08.2026). Починили разбиение длинных абзацев, перезалили всю классику — и
почти ничего не изменилось: у 14 книг из 18 страницы остались прежними. Причина:
`upsert_public_library_document` при совпадении хеша текста обновляла только название,
автора и обложку. А нарезка на страницы — НАША, она считается кодом и меняется вместе
с кодом. Значит совпадение текста не означает совпадения страниц.

Проверяется структурно: тесты не ходят в боевую базу, а пострадала именно она.
"""

import inspect
import re
import unittest

import backend.database as database


class ReingestRefreshesPaginationTests(unittest.TestCase):
    def setUp(self):
        self.source = inspect.getsource(database.upsert_public_library_document)

    def test_same_text_branch_can_still_rewrite_pages(self):
        same_text_branch = self.source.split("if existing and str(existing[1]", 1)[1]
        same_text_branch = same_text_branch.split("if existing:", 1)[0]
        self.assertIn(
            "content_pages", same_text_branch,
            "при совпавшем тексте страницы не пересобираются — починка нарезки не доедет до книг",
        )

    def test_pages_are_compared_before_rewriting(self):
        """Совпали страницы — не переписываем, иначе зря сожжём прогретую озвучку."""
        self.assertIn("pagination_changed", self.source)

    def test_audio_is_dropped_only_when_pagination_really_changed(self):
        after_flag = self.source.split("if pagination_changed:", 1)
        self.assertEqual(len(after_flag), 2, "снятие озвучки не привязано к смене нарезки")
        self.assertRegex(after_flag[1], r"DELETE\s+FROM\s+bt_3_reader_audio_pages")

    def test_the_book_row_is_never_deleted(self):
        self.assertIsNone(
            re.search(r"DELETE\s+FROM\s+bt_3_reader_library", self.source, re.IGNORECASE),
            "строка книги снова удаляется — закладки читателей уйдут каскадом",
        )


if __name__ == "__main__":
    unittest.main()
