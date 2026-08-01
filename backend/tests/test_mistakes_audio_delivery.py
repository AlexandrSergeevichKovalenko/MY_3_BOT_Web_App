"""Morning mistakes-audio delivery. Guards the 2026-07-31 incident: a 32-разбор batch
was rendered, the hero card was sent, and the audio itself died on Telegram's 413
(bot uploads cap at 50 MB) because the montage was re-encoded at 192 kbps.

Three things must hold:
  • the drill closes on the WHOLE sentence, voiced as a sentence (not glued chunks);
  • a batch is split into Telegram-sized files on разбор boundaries, never mid-разбор;
  • only characters that actually reach Google are billed — cache hits are free.
"""
import io
import unittest
from unittest.mock import patch

from pydub import AudioSegment

from backend import backend_server as bs


def _fake_clip(lang, text, speed=None):
    """Stand-in for Google TTS: silence proportional to the text, 24 kHz mono like the
    real voice. Records the synthesis so the accounting ledger sees it."""
    bs._note_tts_synthesis(text)
    return AudioSegment.silent(duration=max(1, int(len(text) / 14.0 * 1000)), frame_rate=24000)


class TargetScriptTests(unittest.TestCase):
    def test_drill_closes_on_the_whole_sentence_as_one_utterance(self):
        chunks = ["Weil es regnete,", "blieb ich zu Hause", "und las ein Buch"]
        whole = "Weil es regnete, blieb ich zu Hause und las ein Buch"
        script = bs.build_target_script(chunks, "de", full_sentence=whole)

        # Closing step, twice, as a real sentence — a chain would splice three clips that
        # each end on a falling intonation.
        self.assertEqual([s["kind"] for s in script[-2:]], ["utterance", "utterance"])
        self.assertEqual([s.get("text") for s in script[-2:]], [whole, whole])
        # The snowball itself survives: intermediate cumulative steps are still chains.
        self.assertTrue(any(s["kind"] == "chain" for s in script))

    def test_single_chunk_sentence_has_no_chain_and_no_duplicate_whole(self):
        script = bs.build_target_script(["Ich komme."], "de", full_sentence="Ich komme.")
        self.assertEqual([s["kind"] for s in script], ["utterance", "utterance"])

    def test_without_full_sentence_the_old_chain_ending_is_kept(self):
        chunks = ["Weil es regnete,", "blieb ich zu Hause"]
        script = bs.build_target_script(chunks, "de")
        self.assertEqual(script[-1]["kind"], "chain")
        self.assertEqual(script[-1]["chunks"], chunks)


class PackMistakeSegmentsTests(unittest.TestCase):
    def _segments(self, minutes):
        return [AudioSegment.silent(duration=int(m * 60_000), frame_rate=24000) for m in minutes]

    def test_small_batch_stays_one_file(self):
        parts = bs.pack_mistake_segments(self._segments([1.5, 2.0, 1.0]))
        self.assertEqual(len(parts), 1)

    def test_oversized_batch_splits_and_every_part_fits_the_budget(self):
        budget = 2 * 1024 * 1024
        segments = self._segments([4] * 8)  # 32 min ≈ 15 MB at 64 kbps
        parts = bs.pack_mistake_segments(segments, max_part_bytes=budget)

        self.assertGreater(len(parts), 1)
        for payload in parts:
            self.assertLessEqual(len(payload), budget * 1.05)

        # Nothing is lost and nothing is cut: total playtime survives the split, and every
        # part is a whole number of 4-minute разборы.
        total_ms = sum(len(AudioSegment.from_file(io.BytesIO(p), format="mp3")) for p in parts)
        self.assertAlmostEqual(total_ms / 1000.0, 8 * 4 * 60, delta=5.0)
        for payload in parts:
            part_ms = len(AudioSegment.from_file(io.BytesIO(payload), format="mp3"))
            self.assertAlmostEqual((part_ms / 1000.0) % (4 * 60), 0, delta=5.0)

    def test_single_oversized_mistake_is_sent_alone_not_dropped(self):
        parts = bs.pack_mistake_segments(self._segments([30, 1]), max_part_bytes=1024)
        self.assertEqual(len(parts), 2)

    def test_export_bitrate_matches_the_source_voice(self):
        # 64 kbps is what Google hands us; anything higher only inflates the upload.
        self.assertEqual(bs._AUDIO_EXPORT_BITRATE, "64k")
        payload = bs.export_audio_segment(AudioSegment.silent(duration=10_000, frame_rate=24000))
        self.assertLess(len(payload), 100 * 1024)


class TtsSynthesisAccountingTests(unittest.TestCase):
    def test_only_real_synthesis_is_counted(self):
        item = {"source_text": "Я вчера написал письмо.", "target_text": "Ich habe gestern einen Brief geschrieben."}
        cache: dict = {}

        def cached_clip(lang, text, speed=None):
            key = (lang, text)
            if key not in cache:
                cache[key] = _fake_clip(lang, text, speed)
            return cache[key]

        with patch.object(bs, "get_or_create_tts_clip", side_effect=cached_clip), \
                patch.object(bs, "chunk_sentence_for_language", side_effect=lambda s, l: [s]):
            script = bs.build_mistake_script(item, "ru", "de")
            with bs.tts_synthesis_accounting() as first:
                bs.render_script_to_segment(script)
            with bs.tts_synthesis_accounting() as second:
                bs.render_script_to_segment(script)

        self.assertGreater(first["chars"], 0)
        # Repeats inside one разбор and the whole second render come from cache: free.
        self.assertEqual(second["chars"], 0)

    def test_ledger_is_inert_outside_an_accounting_block(self):
        bs._note_tts_synthesis("kein Ledger")  # must not raise


if __name__ == "__main__":
    unittest.main()
