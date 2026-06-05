import unittest
from unittest.mock import patch

import backend.backend_server as server
import backend.database as database


class DictionarySemanticFolderTests(unittest.TestCase):
    def test_normalize_dictionary_semantic_tag_accepts_canonical_and_synonym(self):
        self.assertEqual(database.normalize_dictionary_semantic_tag("Еда"), "Еда")
        self.assertEqual(database.normalize_dictionary_semantic_tag("essen"), "Еда")
        self.assertEqual(database.normalize_dictionary_semantic_tag("wohnung"), "Жильё")
        self.assertEqual(database.normalize_dictionary_semantic_tag("unknown-folder"), "")

    def test_backend_semantic_folder_resolver_uses_existing_lookup_category(self):
        with patch.object(
            server,
            "get_or_create_dictionary_semantic_folder",
            return_value={"id": 55, "name": "Еда"},
        ) as folder_mock:
            semantic_tag, folder_id = server._resolve_dictionary_semantic_folder_for_save(
                117649764,
                {"semantic_category": "Еда"},
            )

        self.assertEqual(semantic_tag, "Еда")
        self.assertEqual(folder_id, 55)
        folder_mock.assert_called_once_with(117649764, "Еда")

    def test_backend_semantic_folder_resolver_ignores_unknown_category(self):
        with patch.object(server, "get_or_create_dictionary_semantic_folder") as folder_mock:
            semantic_tag, folder_id = server._resolve_dictionary_semantic_folder_for_save(
                117649764,
                {"semantic_category": "not-a-real-category"},
            )

        self.assertEqual(semantic_tag, "")
        self.assertIsNone(folder_id)
        folder_mock.assert_not_called()

    def test_backend_semantic_folder_resolver_uses_origin_meta_category(self):
        with patch.object(
            server,
            "get_or_create_dictionary_semantic_folder",
            return_value={"id": 77, "name": "Общение"},
        ) as folder_mock:
            semantic_tag, folder_id = server._resolve_dictionary_semantic_folder_for_save(
                117649764,
                {"origin_meta": {"semantic_category": "Общение"}},
            )

        self.assertEqual(semantic_tag, "Общение")
        self.assertEqual(folder_id, 77)
        folder_mock.assert_called_once_with(117649764, "Общение")


if __name__ == "__main__":
    unittest.main()
