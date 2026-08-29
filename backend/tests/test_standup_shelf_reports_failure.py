"""Ночное пополнение полки стендапов обязано говорить, когда оно НЕ смогло.

Повод (29.08.2026). Полка стояла на 4 роликах из 30 с 21.08: каждую ночь работа
обходила каналы, упиралась в бюджет на первом же ролике без субтитров и добавляла
ноль. Сообщение владельцу уходило только при added > 0, поэтому семь ночей подряд
он не знал ничего. Рубрика выходит через день — запас кончился бы молча.

Молчим ТОЛЬКО когда полка полна. «Пробовали и не смогли» — всегда письмо.
"""
import unittest

from backend.standup_shelf import format_shelf_refill_report, refill_fell_short


class RefillFellShortTests(unittest.TestCase):
    def test_full_shelf_stays_silent(self):
        report = {"had_unused": 30, "target": 30, "added": 0,
                  "reason": "полка полна — в YouTube не ходили"}
        self.assertFalse(refill_fell_short(report))

    def test_nothing_added_on_a_half_empty_shelf_speaks_up(self):
        report = {"had_unused": 4, "target": 30, "added": 0, "now_unused": 4,
                  "swept": 3764, "attempted": 2, "no_transcript": 2, "budget_spent": True,
                  "budget_sec": 150}
        self.assertTrue(refill_fell_short(report))

    def test_no_candidates_at_all_speaks_up(self):
        # «кандидатов не получено (квота или сеть)» — ранний выход без пересчёта полки.
        report = {"had_unused": 4, "target": 30, "added": 0,
                  "reason": "кандидатов не получено (квота или сеть)"}
        self.assertTrue(refill_fell_short(report))

    def test_successful_refill_is_not_a_failure(self):
        report = {"had_unused": 4, "target": 30, "added": 7, "now_unused": 11}
        self.assertFalse(refill_fell_short(report))


class FailureTextTests(unittest.TestCase):
    def test_text_names_the_numbers_the_owner_needs(self):
        report = {"had_unused": 4, "target": 30, "added": 0, "now_unused": 4,
                  "swept": 3764, "attempted": 2, "no_transcript": 2, "short_transcript": 0,
                  "dur_skipped": 3116, "budget_spent": True, "budget_sec": 150}
        text = format_shelf_refill_report(report)
        self.assertIn("НЕ пополнилась", text)
        self.assertIn("4", text)          # сколько осталось
        self.assertIn("30", text)         # сколько нужно
        self.assertIn("3764", text)       # что обошли
        self.assertIn("без субтитров 2", text)
        self.assertIn("150", text)        # бюджет, в который упёрлись
        self.assertIn("/standup_shelf", text)

    def test_full_shelf_text_is_short_and_says_no_network(self):
        report = {"had_unused": 30, "target": 30, "added": 0,
                  "reason": "полка полна — в YouTube не ходили"}
        text = format_shelf_refill_report(report)
        self.assertIn("полка полна", text)
        self.assertNotIn("НЕ пополнилась", text)


if __name__ == "__main__":
    unittest.main()


class QueueHeadIsClearedTests(unittest.TestCase):
    """Голова очереди должна расчищаться, иначе ночь снова упрётся в тех же роликов.

    Порядок отбора детерминированный (ручные субтитры → просмотры), поэтому без реестра
    вердиктов работа семь ночей подряд бралась за одни и те же два ролика и добавляла
    ноль (замер 29.08.2026). Здесь проверяется весь круг: осуждённого не трогаем,
    новому отказу выносим вердикт, годного кладём.
    """

    def _run_refill(self, *, skip_ids, fetch_results):
        import backend.database as db
        import backend.world_news_generator as wng
        from backend.standup_shelf import refill_standup_shelf

        cands = [{"video_id": v} for v in ("SKIPME", "NOCAPS", "GOOD")]
        details = {
            "SKIPME": {"title": "уже осуждён", "duration_seconds": 400,
                       "has_manual_captions": True, "view_count": 900},
            "NOCAPS": {"title": "без субтитров", "duration_seconds": 400,
                       "has_manual_captions": True, "view_count": 800},
            "GOOD": {"title": "годный", "duration_seconds": 400,
                     "has_manual_captions": True, "view_count": 700},
        }
        judged, put = [], []
        saved = {}

        def _stub(module, name, value):
            saved[(module, name)] = getattr(module, name)
            setattr(module, name, value)

        _stub(wng, "_gather_candidates", lambda profile=None: list(cands))
        _stub(wng, "_yt_api_video_details", lambda ids: details)
        _stub(wng, "fetch_transcript_or_verdict",
              lambda vid, timeout_sec=150, proxy_first=False: fetch_results[vid])
        _stub(db, "standup_shelf_counts", lambda: {"total": 0, "unused": 0, "unused_manual": 0})
        _stub(db, "standup_shelf_video_ids", lambda: set())
        _stub(db, "get_shown_daily_video_ids", lambda rubric: set())
        _stub(db, "transcript_video_ids_to_skip", lambda: set(skip_ids))
        _stub(db, "transcript_verdict_counts",
              lambda: {"permanent": 1, "waiting": 0, "due": 0, "needs_review": 0})
        _stub(db, "upsert_daily_video_pool_snapshot", lambda **kw: None)
        _stub(db, "record_transcript_verdict",
              lambda **kw: judged.append((kw["video_id"], kw["verdict"])))
        _stub(db, "put_on_standup_shelf", lambda **kw: put.append(kw["video_id"]) or True)
        try:
            report = refill_standup_shelf(target=5, max_add=5, budget_sec=30)
        finally:
            for (module, name), value in saved.items():
                setattr(module, name, value)
        return report, judged, put

    def test_condemned_is_not_touched_new_refusal_is_judged_good_is_kept(self):
        report, judged, put = self._run_refill(
            skip_ids={"SKIPME"},
            fetch_results={
                "NOCAPS": (None, "no_captions", "webshare[de]: NoTranscriptFound"),
                "GOOD": ({"items": [{"text": "ха " * 400}], "language": "de",
                          "is_generated": False}, None, None),
            },
        )
        self.assertEqual(report["skipped_known"], 1)          # осуждённого не трогали
        self.assertEqual([v for v, _ in judged], ["NOCAPS"])  # новому — вердикт
        self.assertEqual(put, ["GOOD"])                       # годный лёг на полку
        self.assertEqual(report["added"], 1)
        self.assertEqual(report["verdicts"], {"no_captions": 1})

    def test_timeout_is_recorded_but_never_as_a_sentence(self):
        from backend.transcript_failure import is_permanent
        report, judged, _ = self._run_refill(
            skip_ids=set(),
            fetch_results={
                "SKIPME": (None, "timeout", "не дождались за 90 c"),
                "NOCAPS": (None, "timeout", "не дождались за 90 c"),
                "GOOD": (None, "timeout", "не дождались за 90 c"),
            },
        )
        self.assertEqual(report["added"], 0)
        self.assertEqual(report["verdicts"], {"timeout": 3})
        for _vid, verdict in judged:
            self.assertFalse(is_permanent(verdict))
