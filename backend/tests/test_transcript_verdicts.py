"""Приговор ролику выносится по ОТВЕТУ YouTube, а не по нашему секундомеру.

Владелец, 29.08.2026: «мы возьмём просто за 25 секунд навсегда решим что этот ролик
плохой и его в чёрный список добавим?? Но это неправильно… чтобы мы точно знали, что мы
хороший ролик не выбросили».

Отсюда два закона, и оба проверяются здесь:
  1. «Не дождались» / «не пустили» / «проверка неполная» — НИКОГДА не приговор.
  2. Сомнение всегда в пользу ролика: если среди причин есть хоть один признак
     блокировки, весь ответ временный, даже если другие ступени успели сказать
     «субтитров нет» — заблокированная ступень означает неполную проверку.
"""
import unittest

from backend.transcript_failure import (
    VERDICT_BLOCKED, VERDICT_INCOMPLETE, VERDICT_NO_CAPTIONS, VERDICT_TIMEOUT,
    VERDICT_UNKNOWN, VERDICT_UNUSABLE, classify_transcript_failure, is_permanent,
)


class ClassificationTests(unittest.TestCase):
    def test_full_ladder_says_no_captions(self):
        # Живой ответ по sPJLkkwyLYs (29.08.2026), без мёртвой четвёртой ступени.
        text = ("direct[de]: NoTranscriptFound; direct[en]: NoTranscriptFound; "
                "yt-dlp: yt-dlp fallback failed to obtain VTT subtitles; "
                "webshare[de]: NoTranscriptFound; webshare[en]: NoTranscriptFound")
        self.assertEqual(classify_transcript_failure(text), VERDICT_NO_CAPTIONS)
        self.assertTrue(is_permanent(VERDICT_NO_CAPTIONS))

    def test_one_blocked_rung_cancels_the_whole_verdict(self):
        text = "direct[de]: RequestBlocked; webshare[de]: NoTranscriptFound"
        self.assertEqual(classify_transcript_failure(text), VERDICT_BLOCKED)
        self.assertFalse(is_permanent(VERDICT_BLOCKED))

    def test_dead_generic_proxy_does_not_block_the_verdict(self):
        # «generic rejected country None» — четвёртая ступень не запускалась, потому что
        # владелец за эти прокси не платит. Ответ YouTube уже подтверждён напрямую и
        # трижды через немецкие адреса webshare, поэтому приговор выносится.
        text = ("direct[de]: NoTranscriptFound; webshare[de]: NoTranscriptFound; "
                "generic rejected country None")
        self.assertEqual(classify_transcript_failure(text), VERDICT_NO_CAPTIONS)

    def test_nothing_ran_at_all_is_not_a_sentence(self):
        self.assertEqual(classify_transcript_failure("ни одна ступень не запускалась"),
                         VERDICT_INCOMPLETE)
        self.assertFalse(is_permanent(VERDICT_INCOMPLETE))

    def test_video_gone_is_permanent(self):
        self.assertEqual(classify_transcript_failure("direct[de]: VideoUnavailable"),
                         VERDICT_UNUSABLE)
        self.assertTrue(is_permanent(VERDICT_UNUSABLE))

    def test_unrecognised_reason_is_never_permanent(self):
        self.assertEqual(classify_transcript_failure("что-то пошло не так"), VERDICT_UNKNOWN)
        self.assertFalse(is_permanent(VERDICT_UNKNOWN))
        self.assertEqual(classify_transcript_failure(""), VERDICT_UNKNOWN)

    def test_our_stopwatch_never_condemns(self):
        self.assertFalse(is_permanent(VERDICT_TIMEOUT))
        self.assertFalse(is_permanent(None))


class TimeoutIsNotAVerdictTests(unittest.TestCase):
    def test_slow_fetch_returns_timeout_and_does_not_judge_the_video(self):
        import backend.world_news_generator as wng

        def _never_returns(*_a, **_kw):
            import time
            time.sleep(5)
            raise AssertionError("не должно дойти сюда")

        original = wng.__dict__.get("_fetch_youtube_transcript")
        import backend.backend_server as bs
        bs._fetch_youtube_transcript = _never_returns
        try:
            data, verdict, reason = wng.fetch_transcript_or_verdict("XXX", timeout_sec=1)
        finally:
            if original is not None:
                bs._fetch_youtube_transcript = original
        self.assertIsNone(data)
        self.assertEqual(verdict, VERDICT_TIMEOUT)
        self.assertIn("не дождались", reason)
        self.assertFalse(is_permanent(verdict))


if __name__ == "__main__":
    unittest.main()
