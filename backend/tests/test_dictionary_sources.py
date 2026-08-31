"""Тема — дом слова, источник — его свойство (решение владельца 31.08.2026).

Тесты держат ровно те три правила, из-за нарушения которых «папки пустели»:
1. Тема не перебивает папку, выбранную человеком.
2. Источник берётся только из настоящих данных; нет id — нет источника.
3. Тема карточки доезжает от модели до сохранения (регрессия 31.08.2026).
"""

import unittest
from unittest.mock import patch

import backend.backend_server as server
import backend.database as database


class DictionaryFolderChoiceTests(unittest.TestCase):
    """Правило «не решаем за пользователя» на самом больном месте."""

    def test_theme_fills_the_folder_when_nobody_chose_one(self):
        self.assertEqual(
            server._folder_for_dictionary_save(
                folder_id=7, folder_chosen_by_user=False, semantic_folder_id=42,
            ),
            42,
        )

    def test_theme_never_overrides_a_folder_chosen_by_the_user(self):
        # Ровно этот случай и ломал папки роликов: присланную папку выбрасывали.
        self.assertEqual(
            server._folder_for_dictionary_save(
                folder_id=263, folder_chosen_by_user=True, semantic_folder_id=42,
            ),
            263,
        )

    def test_without_a_theme_the_folder_stays_as_it_came(self):
        self.assertEqual(
            server._folder_for_dictionary_save(
                folder_id=7, folder_chosen_by_user=False, semantic_folder_id=None,
            ),
            7,
        )


class DictionarySourceResolveTests(unittest.TestCase):
    def test_source_is_created_from_kind_key_and_title(self):
        with patch.object(
            server, "get_or_create_dictionary_source", return_value={"id": 12},
        ) as source_mock:
            source_id = server._resolve_dictionary_source_for_save(
                {"source": {"kind": "youtube", "key": "nLiOMhqDvC8", "title": "Die großen Mythen"}},
                user_id=117649764,
            )
        self.assertEqual(source_id, 12)
        source_mock.assert_called_once_with(
            user_id=117649764,
            kind="youtube",
            external_key="nLiOMhqDvC8",
            title="Die großen Mythen",
            title_source="player",
        )

    def test_missing_title_is_not_invented(self):
        # Плеер не отдал заголовок — источник всё равно заводится по id ролика, но имя
        # остаётся пустым. Подставлять сюда «YouTube nLiOMh» запрещено.
        with patch.object(
            server, "get_or_create_dictionary_source", return_value={"id": 13},
        ) as source_mock:
            server._resolve_dictionary_source_for_save(
                {"source": {"kind": "youtube", "key": "nLiOMhqDvC8", "title": "  "}},
                user_id=1,
            )
        self.assertIsNone(source_mock.call_args.kwargs["title"])
        self.assertIsNone(source_mock.call_args.kwargs["title_source"])

    def test_no_key_means_no_source(self):
        with patch.object(server, "get_or_create_dictionary_source") as source_mock:
            self.assertIsNone(
                server._resolve_dictionary_source_for_save(
                    {"source": {"kind": "youtube", "title": "Die großen Mythen"}}, user_id=1,
                )
            )
            self.assertIsNone(server._resolve_dictionary_source_for_save({}, user_id=1))
        source_mock.assert_not_called()

    def test_unknown_kind_is_rejected_by_the_database_layer(self):
        self.assertEqual(database._normalize_dictionary_source_kind("youtube"), "youtube")
        self.assertEqual(database._normalize_dictionary_source_kind("подкаст"), "")


class DictionarySemanticCategorySurvivesTests(unittest.TestCase):
    """Регрессия 31.08.2026: тема терялась при пересборке карточки.

    Модель возвращает semantic_category вместе с секцией значений, но общий сборщик
    ответа её не переносил, и сохранение клало слово мимо тематических папок: за сутки
    44 слова из 68 ушли без темы (26–30.08 таких было 0).
    """

    def test_semantic_category_reaches_the_built_item(self):
        result, _detected, _source, _target = server._build_multilang_dictionary_result(
            raw={"word_source": "Moschee", "word_target": "мечеть", "semantic_category": "Культура"},
            query_word="Moschee",
            source_lang="de",
            target_lang="ru",
        )
        self.assertEqual(result.get("semantic_category"), "Культура")

    def test_the_save_path_reads_that_very_field(self):
        with patch.object(
            server, "get_or_create_dictionary_semantic_folder", return_value={"id": 50, "name": "Культура"},
        ):
            semantic_tag, folder_id = server._resolve_dictionary_semantic_folder_for_save(
                117649764, {"semantic_category": "Культура"},
            )
        self.assertEqual((semantic_tag, folder_id), ("Культура", 50))


class DictionaryOriginGroupsTests(unittest.TestCase):
    def test_every_group_lists_at_least_one_origin_process(self):
        for key, (name, icon, processes) in database.DICTIONARY_ORIGIN_GROUPS.items():
            with self.subTest(group=key):
                self.assertTrue(name.strip(), "у группы должно быть человеческое имя")
                self.assertTrue(icon.strip())
                self.assertTrue(processes, "группа без origin_process не найдёт ни одного слова")

    def test_origin_process_belongs_to_a_single_group(self):
        seen: dict[str, str] = {}
        for key, (_name, _icon, processes) in database.DICTIONARY_ORIGIN_GROUPS.items():
            for process in processes:
                self.assertNotIn(
                    process, seen,
                    f"origin_process {process!r} попал и в {seen.get(process)!r}, и в {key!r} — "
                    "слова посчитались бы дважды",
                )
                seen[process] = key


if __name__ == "__main__":
    unittest.main()
