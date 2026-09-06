# -*- coding: utf-8 -*-
"""Недельный рейтинг: место не даётся за ноль, равный балл — одно место.

Решение владельца 06.09.2026. Повод: «@salesdoc» неделя за неделей видел «#8 из 14» с
подписью «same» при балле 0.0 — место среди нулей решал алфавит имени, а на третьей
ступени пьедестала стоял человек с 0.0. Стратегия — docs/tasks/weekly_ranking_zero_place_strategy.md.
"""
import os
import unittest
from datetime import date
from unittest.mock import patch

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

import backend.backend_server as server  # noqa: E402
from backend import fix_promises  # noqa: E402


def _row(user_id, username, *, translations=0, avg_score=0.0, srs=0, voice=0.0, reader=0.0,
         active_days=0, prev_units=0.0, prev_rank=None):
    units = translations + srs * 0.35 + (voice + reader) * 0.12
    return {
        "user_id": user_id, "username": username,
        "translations_count": translations, "avg_score": avg_score, "srs_reviews": srs,
        "voice_minutes": voice, "reader_minutes": reader, "active_days": active_days,
        "current_units": units, "previous_units": prev_units, "previous_rank": prev_rank,
    }


class МестоНеДаётсяЗаНоль(unittest.TestCase):

    def test_нули_без_места_и_не_входят_в_из_N(self):
        rows = server._score_and_rank_weekly_global_ranking_rows([
            _row(1, "Iryna", translations=10, avg_score=80, active_days=5),
            _row(2, "owner_admin", translations=3, avg_score=60, active_days=2),
            _row(3, "@daria_romanska"),
            _row(4, "@salesdoc"),
            _row(5, "zzz"),
        ])
        by_name = {r["username"]: r for r in rows}
        self.assertEqual(by_name["Iryna"]["rank"], 1)
        self.assertEqual(by_name["owner_admin"]["rank"], 2)
        for имя in ("@daria_romanska", "@salesdoc", "zzz"):
            self.assertIsNone(by_name[имя]["rank"], имя)
            self.assertEqual(by_name[имя]["final_score"], 0.0, имя)
            self.assertIsNone(by_name[имя]["rank_delta"], имя)
        # «из N» — только занимавшиеся, у всех строк одинаково
        self.assertEqual({r["total_users"] for r in rows}, {2})
        # занимавшиеся идут первыми — из них берётся «Топ недели»
        self.assertEqual([r["username"] for r in rows[:2]], ["Iryna", "owner_admin"])

    def test_один_активный_день_без_единиц_это_уже_место(self):
        rows = server._score_and_rank_weekly_global_ranking_rows([
            _row(1, "a", active_days=1),
            _row(2, "b"),
        ])
        self.assertEqual(rows[0]["rank"], 1)
        self.assertGreater(rows[0]["final_score"], 0)
        self.assertIsNone(rows[1]["rank"])
        self.assertEqual(rows[0]["total_users"], 1)

    def test_равный_балл_одно_место_как_в_спорте(self):
        rows = server._score_and_rank_weekly_global_ranking_rows([
            _row(1, "a", translations=10, avg_score=80, active_days=5),
            _row(2, "b", translations=4, avg_score=50, active_days=3),
            _row(3, "c", translations=4, avg_score=50, active_days=3),
            _row(4, "d", translations=1, avg_score=20, active_days=1),
        ])
        self.assertEqual([r["rank"] for r in rows], [1, 2, 2, 4])

    def test_стрелка_считается_от_настоящего_прошлого_места(self):
        rows = server._score_and_rank_weekly_global_ranking_rows([
            _row(1, "a", translations=5, avg_score=70, active_days=3, prev_rank=3),
            _row(2, "b", translations=1, avg_score=50, active_days=1, prev_rank=None),
        ])
        self.assertEqual(rows[0]["rank_delta"], 2)   # был 3-м, стал 1-м
        self.assertIsNone(rows[1]["rank_delta"])     # прошлого места не было → «new»

    def test_прошлое_место_у_нуля_в_запросе_не_засчитывается(self):
        # Старые снимки хранят алфавитные места у нулей; стрелка от них — ложь.
        import inspect
        src = inspect.getsource(server._collect_weekly_global_ranking_rows)
        self.assertIn("CASE WHEN prevsnap.final_score > 0 THEN prevsnap.rank ELSE NULL END", src)
        self.assertNotIn("COALESCE(prevsnap.rank, NULL)", src)


class РассылкаБезЗанятийПрисылаетТекст(unittest.TestCase):

    def test_нулю_текст_занимавшемуся_карточка_топ_без_нулей(self):
        rows = server._score_and_rank_weekly_global_ranking_rows([
            _row(11, "Iryna", translations=10, avg_score=80, active_days=5),
            _row(12, "@salesdoc"),
        ])
        photos, texts, rendered_tops = [], [], []

        def _render(row, *, start_date, end_date, top_rows):
            rendered_tops.append([t["username"] for t in top_rows])
            return b"png"

        with patch.object(server, "claim_scheduler_run_guard", return_value=True), \
             patch.object(server, "finish_scheduler_run_guard"), \
             patch.object(server, "_ensure_weekly_global_ranking_schema"), \
             patch.object(server, "_repair_weekly_global_ranking_snapshots", return_value={"zero_rank_removed": 0, "ranked_rows_recomputed": 0}), \
             patch.object(server, "_collect_weekly_global_ranking_rows", return_value=rows), \
             patch.object(server, "_persist_weekly_global_ranking_snapshot"), \
             patch.object(server, "_weekly_global_ranking_delivered_user_ids", return_value=set()), \
             patch.object(server, "_mark_weekly_global_ranking_delivery"), \
             patch.object(server, "is_telegram_user_allowed", return_value=True), \
             patch.object(server, "_render_weekly_global_ranking_card_png", side_effect=_render), \
             patch.object(server, "_send_private_photo", side_effect=lambda **kw: photos.append(kw)), \
             patch.object(server, "_send_private_message", side_effect=lambda **kw: texts.append(kw)), \
             patch.object(server, "_weekly_global_ranking_bounds", return_value=(date(2026, 8, 30), date(2026, 9, 5))):
            result = server._dispatch_weekly_global_ranking_report(tz_name="Europe/Berlin")

        self.assertEqual(result["sent"], 1)
        self.assertEqual(result["sent_idle_notice"], 1)
        self.assertEqual(result["ranked_users"], 1)
        self.assertEqual([p["user_id"] for p in photos], [11])
        self.assertEqual(photos[0]["caption"], "🏆 Твой weekly ranking: #1 из 1")
        self.assertEqual([t["user_id"] for t in texts], [12])
        self.assertEqual(texts[0]["text"], server.WEEKLY_GLOBAL_RANKING_IDLE_TEXT)
        self.assertIn("занятий не было", texts[0]["text"])
        self.assertEqual(rendered_tops, [["Iryna"]])


class Обещание(unittest.TestCase):

    def test_обещание_зарегистрировано(self):
        keys = {p.key for p in fix_promises.PROMISES}
        self.assertIn("weekly_ranking_no_place_for_zero", keys)
        promise = next(p for p in fix_promises.PROMISES if p.key == "weekly_ranking_no_place_for_zero")
        self.assertEqual(promise.expected, 0)


class НочнаяЧисткаСнимков(unittest.TestCase):

    def test_сначала_схема_потом_чистка_и_сердцебиение(self):
        import bot_3
        calls = []
        with patch.object(server, "_ensure_weekly_global_ranking_schema", side_effect=lambda: calls.append("schema")), \
             patch.object(server, "_repair_weekly_global_ranking_snapshots",
                          side_effect=lambda: (calls.append("repair") or {"zero_rank_removed": 12, "ranked_rows_recomputed": 2})), \
             patch.object(bot_3, "_record_sched_heartbeat", side_effect=lambda *a: calls.append(a)):
            bot_3._run_weekly_ranking_snapshot_repair_safe()
        self.assertEqual(calls[:2], ["schema", "repair"])
        self.assertEqual(calls[2], ("weekly_ranking_snapshot_repair", "completed",
                                    {"zero_rank_removed": 12, "ranked_rows_recomputed": 2}))

    def test_падение_оставляет_след_failed(self):
        import bot_3
        beats = []
        with patch.object(server, "_ensure_weekly_global_ranking_schema", side_effect=RuntimeError("база не ответила")), \
             patch.object(bot_3, "_record_sched_heartbeat", side_effect=lambda *a: beats.append(a)):
            bot_3._run_weekly_ranking_snapshot_repair_safe()
        self.assertEqual(beats[0][:2], ("weekly_ranking_snapshot_repair", "failed"))
        self.assertIn("база не ответила", beats[0][2]["error"])

    def test_задание_стоит_в_расписании_бота(self):
        import inspect
        import bot_3
        src = inspect.getsource(bot_3)
        self.assertIn("_run_weekly_ranking_snapshot_repair_safe,\n            \"cron\",\n            hour=3,\n            minute=15,", src)


if __name__ == "__main__":
    unittest.main()
