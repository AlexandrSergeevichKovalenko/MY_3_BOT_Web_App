import unittest
from types import SimpleNamespace
from unittest.mock import patch

import backend.tts_ssml as tts_ssml
from backend.tts_generation import _estimate_reader_page_tts_budget_chars


class ReaderTtsSsmlTests(unittest.TestCase):
    def test_reader_page_tts_budget_counts_full_normalized_text(self):
        text = "Hallo, Welt!\n\nNoch ein Satz."
        self.assertEqual(len(text), _estimate_reader_page_tts_budget_chars(text))

    def test_chunk_text_preserves_trailing_punctuation(self):
        text = "Eins zwei drei vier. Fuenf sechs sieben acht."
        words = tts_ssml.segment_page_words(text)

        with patch.object(tts_ssml, "_SSML_CHUNK_MAX_CHARS", 1020):
            chunks = tts_ssml.chunk_text_with_words(text, words)

        self.assertGreater(len(chunks), 1)
        self.assertEqual("".join(chunk_text for chunk_text, _chunk_words in chunks), text)
        self.assertTrue(chunks[0][0].endswith(". "))
        self.assertFalse(chunks[1][0].startswith("."))

    def test_build_ssml_from_spans_marks_sentences_not_each_word(self):
        text = "Hallo Welt. Noch ein Satz!"
        words = tts_ssml.segment_page_words(text)
        spans = tts_ssml.segment_timing_spans(text, words, text_char_offset=0)

        ssml_text, mark_index = tts_ssml._build_ssml_from_spans(
            text,
            spans,
            text_char_offset=0,
            mark_offset=0,
        )

        self.assertEqual(2, len(spans))
        self.assertEqual([2, 3], [len(span["words"]) for span in spans])
        self.assertEqual(2, len(mark_index))
        self.assertEqual(2, ssml_text.count("<mark"))
        self.assertIn("Hallo Welt.", ssml_text)
        self.assertIn("Noch ein Satz!", ssml_text)

    def test_parse_timepoints_for_spans_interpolates_monotonic_word_timings(self):
        text = "Hallo Welt. Noch ein Satz!"
        words = tts_ssml.segment_page_words(text)
        spans = tts_ssml.segment_timing_spans(text, words, text_char_offset=0)
        _ssml_text, mark_index = tts_ssml._build_ssml_from_spans(
            text,
            spans,
            text_char_offset=0,
            mark_offset=0,
        )
        timepoints = [
            SimpleNamespace(mark_name="s1", time_seconds=0.10),
            SimpleNamespace(mark_name="s2", time_seconds=0.70),
        ]

        timings = tts_ssml.parse_timepoints_for_spans(
            timepoints,
            mark_index,
            chunk_duration_ms=1400,
            time_offset_ms=0,
        )

        self.assertEqual(len(words), len(timings))
        self.assertEqual(
            [word["char_start"] for word in words],
            [timing["char_start"] for timing in timings],
        )
        self.assertGreaterEqual(timings[2]["start_ms"], 700)

        for index, timing in enumerate(timings):
            self.assertLess(timing["start_ms"], timing["end_ms"])
            if index > 0:
                self.assertGreaterEqual(timing["start_ms"], timings[index - 1]["end_ms"])


if __name__ == "__main__":
    unittest.main()
