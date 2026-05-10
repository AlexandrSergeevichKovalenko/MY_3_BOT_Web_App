from contextlib import contextmanager
import unittest
from unittest.mock import patch

import backend.backend_server as server


class ReaderAsyncIngestTests(unittest.TestCase):
    @staticmethod
    @contextmanager
    def _fake_db_scope(*_args, **_kwargs):
        yield []

    def setUp(self) -> None:
        self.client = server.app.test_client()

    def test_reader_ingest_async_returns_placeholder_immediately(self):
        placeholder_doc = {
            "id": 91,
            "title": "book.epub",
            "source_type": "epub",
            "source_url": None,
            "processing_status": "pending",
        }

        with patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 55}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "get_or_create_user_subscription"), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {})), \
             patch.object(server, "_resolve_user_entitlement", return_value=({"effective_mode": "pro"}, None)), \
             patch.object(server, "list_reader_library_documents", return_value=[]), \
             patch.object(server, "create_reader_library_document_placeholder", return_value=placeholder_doc), \
             patch.object(server, "r2_put_bytes"), \
             patch.object(server, "set_reader_library_document_ingest_payload"), \
             patch.object(server, "_enqueue_reader_library_ingest_job", return_value={"queued": True}) as enqueue_mock:
            response = self.client.post(
                "/api/webapp/reader/ingest",
                json={
                    "initData": "valid",
                    "file_name": "book.epub",
                    "file_mime": "application/epub+zip",
                    "file_content_base64": "Zm9v",
                },
            )

        self.assertEqual(response.status_code, 202)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["async"])
        self.assertEqual(payload["status"], "pending")
        self.assertEqual(payload["document"]["id"], 91)
        enqueue_mock.assert_called_once()

    def test_reader_open_pending_document_returns_processing_payload(self):
        pending_doc = {
            "id": 17,
            "title": "Queued book",
            "source_type": "epub",
            "source_url": None,
            "processing_status": "processing",
            "processing_error": None,
        }

        with patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "_telegram_hash_is_valid", return_value=True), \
             patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": 55}}), \
             patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {})), \
             patch.object(server, "get_reader_library_document", return_value=pending_doc):
            response = self.client.post(
                "/api/webapp/reader/library/open",
                json={"initData": "valid", "document_id": 17},
            )

        self.assertEqual(response.status_code, 202)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["status"], "processing")
        self.assertEqual(payload["document"]["id"], 17)


if __name__ == "__main__":
    unittest.main()
