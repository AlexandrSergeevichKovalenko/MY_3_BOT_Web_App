import unittest
from contextlib import contextmanager
from datetime import date
from unittest.mock import patch

import backend.backend_server as server


class PaidSurfaceGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = server.app.test_client()

    @staticmethod
    @contextmanager
    def _fake_db_scope(*_args, **_kwargs):
        yield []

    @staticmethod
    def _entitlement(mode: str) -> tuple[dict, dict]:
        return (
            {
                "effective_mode": mode,
                "reset_at": "2026-06-04T00:00:00+02:00",
            },
            {},
        )

    def _assert_paid_required(self, response, *, feature: str, feature_title: str) -> None:
        self.assertEqual(response.status_code, 402)
        payload = response.get_json()
        self.assertEqual(payload["ok"], False)
        self.assertEqual(payload["error"], "paid_feature_required")
        self.assertEqual(payload["feature"], feature)
        self.assertEqual(payload["feature_title"], feature_title)
        self.assertEqual(payload["effective_mode"], "free")

    def test_free_user_blocked_on_today(self):
        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(77, "Iryna", None)), \
             patch.object(server, "_resolve_user_entitlement", return_value=self._entitlement("free")), \
             patch.object(server, "_load_today_card_projection_with_source") as load_mock:
            response = self.client.get("/api/today")

        self._assert_paid_required(response, feature="today", feature_title="Задачи на день")
        load_mock.assert_not_called()

    def test_free_user_blocked_on_skills(self):
        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(77, "Iryna", None)), \
             patch.object(server, "_resolve_user_entitlement", return_value=self._entitlement("free")), \
             patch.object(server, "_load_skills_card_projection_with_source") as load_mock:
            response = self.client.get("/api/progress/skills")

        self._assert_paid_required(response, feature="skills", feature_title="Карта навыков")
        load_mock.assert_not_called()

    def test_free_user_blocked_on_weekly_plan(self):
        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(77, "Iryna", None)), \
             patch.object(server, "_resolve_user_entitlement", return_value=self._entitlement("free")), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "_get_weekly_plan_response_cached") as cached_mock:
            response = self.client.get("/api/progress/weekly-plan")

        self._assert_paid_required(response, feature="weekly_plan", feature_title="План на неделю")
        cached_mock.assert_not_called()

    def test_free_user_blocked_on_today_sync_before_refresh(self):
        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(77, "Iryna", None)), \
             patch.object(server, "_resolve_user_entitlement", return_value=self._entitlement("free")), \
             patch.object(server, "_refresh_today_plan_snapshot_now") as refresh_mock:
            response = self.client.post("/api/today/sync", json={"initData": "valid"})

        self._assert_paid_required(response, feature="today", feature_title="Задачи на день")
        refresh_mock.assert_not_called()

    def test_free_user_blocked_on_skills_sync_before_refresh(self):
        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(77, "Iryna", None)), \
             patch.object(server, "_resolve_user_entitlement", return_value=self._entitlement("free")), \
             patch.object(server, "_refresh_skill_progress_snapshot_now") as refresh_mock:
            response = self.client.post("/api/progress/skills/sync", json={"initData": "valid"})

        self._assert_paid_required(response, feature="skills", feature_title="Карта навыков")
        refresh_mock.assert_not_called()

    def test_free_user_blocked_on_weekly_plan_sync_before_aggregation(self):
        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(77, "Iryna", None)), \
             patch.object(server, "_resolve_user_entitlement", return_value=self._entitlement("free")), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "_build_weekly_plan_response_payload") as build_mock:
            response = self.client.post("/api/progress/weekly-plan/sync", json={"initData": "valid"})

        self._assert_paid_required(response, feature="weekly_plan", feature_title="План на неделю")
        build_mock.assert_not_called()

    def test_free_user_blocked_on_weekly_plan_goals_before_save(self):
        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(77, "Iryna", None)), \
             patch.object(server, "_resolve_user_entitlement", return_value=self._entitlement("free")), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "upsert_weekly_goals") as save_mock:
            response = self.client.post(
                "/api/progress/weekly-plan/goals",
                json={
                    "initData": "valid",
                    "translations_goal": 1,
                    "learned_words_goal": 2,
                    "agent_minutes_goal": 3,
                    "reading_minutes_goal": 4,
                },
            )

        self._assert_paid_required(response, feature="weekly_plan", feature_title="План на неделю")
        save_mock.assert_not_called()

    def test_trial_user_allowed_on_today(self):
        response_payload = {
            "date": "2026-06-03",
            "items": [],
            "total_minutes": 0,
            "language_pair": {"source_lang": "ru", "target_lang": "de"},
        }

        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(77, "Iryna", None)), \
             patch.object(server, "_resolve_user_entitlement", return_value=self._entitlement("trial")), \
             patch.object(server, "_safe_plan_date", return_value=date(2026, 6, 3)), \
             patch.object(server, "_load_today_card_projection_with_source", return_value=({"projection_status": "ready"}, "test")) as load_mock, \
             patch.object(server, "_build_today_card_response_payload", return_value=response_payload), \
             patch.object(server, "_estimate_json_payload_size_bytes", return_value=0), \
             patch.object(server, "_log_flow_observation"):
            response = self.client.get("/api/today")

        self.assertEqual(response.status_code, 200)
        load_mock.assert_called_once()

    def test_trial_user_allowed_on_paid_surface_sync_endpoints(self):
        today_response = {
            "date": "2026-06-03",
            "items": [],
            "total_minutes": 0,
            "language_pair": {"source_lang": "ru", "target_lang": "de"},
        }
        skills_response = {
            "groups_count": 0,
            "total_skills": 0,
            "skills_with_data": 0,
            "language_pair": {"source_lang": "ru", "target_lang": "de"},
        }
        weekly_response = {
            "ok": True,
            "week": {"start_date": "2026-06-01"},
            "plan": {},
            "metrics": {},
            "language_pair": {"source_lang": "ru", "target_lang": "de"},
        }
        weekly_meta = {
            "snapshot_week_start": "2026-06-01",
            "source_lang": "ru",
            "target_lang": "de",
        }

        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(77, "Iryna", None)), \
             patch.object(server, "_resolve_user_entitlement", return_value=self._entitlement("trial")), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "_safe_plan_date", return_value=date(2026, 6, 3)), \
             patch.object(server, "_refresh_today_plan_snapshot_now", return_value={}) as today_refresh_mock, \
             patch.object(server, "_write_today_card_projection_from_today_snapshot", return_value={"payload": {}}), \
             patch.object(server, "_build_today_card_response_payload", return_value=today_response), \
             patch.object(server, "_refresh_skill_progress_snapshot_now", return_value={}) as skills_refresh_mock, \
             patch.object(server, "_write_skills_card_projection_from_skill_snapshot", return_value={"payload": {}}), \
             patch.object(server, "_build_skills_card_response_payload", return_value=skills_response), \
             patch.object(server, "_build_weekly_plan_response_payload", return_value=(weekly_response, weekly_meta)) as weekly_build_mock, \
             patch.object(server, "upsert_user_api_snapshot", return_value={}), \
             patch.object(server, "_store_snapshot_in_front_cache"), \
             patch.object(server, "_estimate_json_payload_size_bytes", return_value=0), \
             patch.object(server, "_log_flow_observation"):
            today = self.client.post("/api/today/sync", json={"initData": "valid"})
            skills = self.client.post("/api/progress/skills/sync", json={"initData": "valid"})
            weekly = self.client.post("/api/progress/weekly-plan/sync", json={"initData": "valid"})

        self.assertEqual(today.status_code, 200)
        self.assertEqual(skills.status_code, 200)
        self.assertEqual(weekly.status_code, 200)
        today_refresh_mock.assert_called_once()
        skills_refresh_mock.assert_called_once()
        weekly_build_mock.assert_called_once()

    def test_pro_user_allowed_on_skills(self):
        projection_payload = {
            "projection_status": "ready",
            "groups": [],
            "aggregate_summary": {
                "groups_count": 0,
                "skills_with_data": 0,
            },
        }
        response_payload = {
            "groups_count": 0,
            "total_skills": 0,
            "skills_with_data": 0,
            "language_pair": {"source_lang": "ru", "target_lang": "de"},
        }

        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(77, "Iryna", None)), \
             patch.object(server, "_resolve_user_entitlement", return_value=self._entitlement("pro")), \
             patch.object(server, "_load_skills_card_projection_with_source", return_value=(projection_payload, "test")) as load_mock, \
             patch.object(server, "_build_skills_card_response_payload", return_value=response_payload), \
             patch.object(server, "_estimate_json_payload_size_bytes", return_value=0), \
             patch.object(server, "_log_flow_observation"):
            response = self.client.get("/api/progress/skills")

        self.assertEqual(response.status_code, 200)
        load_mock.assert_called_once()

    def test_pro_user_allowed_on_weekly_plan_goals(self):
        with patch.object(server, "_get_authenticated_user_from_request_init_data", return_value=(77, "Iryna", None)), \
             patch.object(server, "_resolve_user_entitlement", return_value=self._entitlement("pro")), \
             patch.object(server, "db_acquire_scope", self._fake_db_scope), \
             patch.object(server, "_get_user_language_pair", return_value=("ru", "de", {})), \
             patch.object(server, "upsert_weekly_goals") as save_mock, \
             patch.object(server, "_mark_weekly_plan_snapshot_stale"), \
             patch.object(server, "_mark_plan_analytics_snapshot_stale"), \
             patch.object(server, "_get_local_today_date", return_value=date(2026, 6, 3)), \
             patch.object(server, "_estimate_json_payload_size_bytes", return_value=0), \
             patch.object(server, "_log_flow_observation"):
            response = self.client.post(
                "/api/progress/weekly-plan/goals",
                json={
                    "initData": "valid",
                    "translations_goal": 1,
                    "learned_words_goal": 2,
                    "agent_minutes_goal": 3,
                    "reading_minutes_goal": 4,
                },
            )

        self.assertEqual(response.status_code, 200)
        save_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
