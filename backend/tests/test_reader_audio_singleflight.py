import unittest

from backend.reader_audio_singleflight import (
    acquire_reader_audio_singleflight_slot,
    release_reader_audio_singleflight_slot,
)


class ReaderAudioSingleflightTests(unittest.TestCase):
    def test_reuses_inflight_event_until_release(self):
        cache_key = "reader-audio:test"

        is_owner_1, event_1 = acquire_reader_audio_singleflight_slot(cache_key)
        is_owner_2, event_2 = acquire_reader_audio_singleflight_slot(cache_key)

        self.assertTrue(is_owner_1)
        self.assertFalse(is_owner_2)
        self.assertIsNotNone(event_1)
        self.assertIs(event_1, event_2)
        self.assertFalse(event_1.is_set())

        release_reader_audio_singleflight_slot(cache_key, event_1)

        self.assertTrue(event_1.is_set())

        is_owner_3, event_3 = acquire_reader_audio_singleflight_slot(cache_key)
        self.assertTrue(is_owner_3)
        self.assertIsNot(event_1, event_3)

        release_reader_audio_singleflight_slot(cache_key, event_3)


if __name__ == "__main__":
    unittest.main()
