import unittest
from unittest.mock import patch

from backend.job_queue import (
    get_reader_audio_page_job_status,
    set_reader_audio_page_job_status,
)


class _FakeRedis:
    def __init__(self):
        self.values = {}

    def get(self, key):
        return self.values.get(key)

    def setex(self, key, ttl, value):
        self.values[key] = value


class ReaderAudioPageJobQueueTests(unittest.TestCase):
    def test_reader_audio_page_job_status_round_trip(self):
        fake_redis = _FakeRedis()
        with patch("backend.job_queue.get_redis_client", return_value=fake_redis):
            set_reader_audio_page_job_status(
                "reader-job-1",
                status="pending",
                job_id="msg-1",
                source="unit-test",
            )
            payload = get_reader_audio_page_job_status("reader-job-1")

        self.assertIsInstance(payload, dict)
        self.assertEqual("pending", payload["status"])
        self.assertEqual("msg-1", payload["job_id"])
        self.assertEqual("unit-test", payload["source"])
        self.assertIsInstance(payload.get("updated_at_ms"), int)


if __name__ == "__main__":
    unittest.main()
