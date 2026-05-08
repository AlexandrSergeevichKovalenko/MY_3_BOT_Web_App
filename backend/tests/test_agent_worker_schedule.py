import os
import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from backend.agent_worker_schedule import (
    get_agent_worker_schedule_state,
    get_agent_worker_schedule_windows,
    run_agent_worker_schedule_control,
)


class AgentWorkerScheduleTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env = patch.dict(
            os.environ,
            {
                "AGENT_WORKER_SCHEDULE_ENABLED": "1",
                "AGENT_WORKER_SCHEDULE_DRY_RUN": "1",
                "AGENT_WORKER_START_TIMES": "06:55,15:55",
                "AGENT_WORKER_STOP_TIMES": "12:00,19:00",
                "AGENT_WORKER_TIMEZONE": "Europe/Vienna",
                "AGENT_WORKER_RAILWAY_ENVIRONMENT_ID": "env",
                "AGENT_WORKER_RAILWAY_SERVICE_ID": "svc",
            },
            clear=False,
        )
        self._env.start()
        self.addCleanup(self._env.stop)

    def _service_state(
        self,
        *,
        active_deployments: list[dict] | None = None,
        latest_deployment: dict | None = None,
    ) -> dict:
        return {
            "environment_id": "env",
            "service_id": "svc",
            "service_name": "AGENT_WORKER",
            "active_deployments": list(active_deployments or []),
            "latest_deployment": latest_deployment,
        }

    def test_schedule_windows_and_vienna_state(self) -> None:
        windows = get_agent_worker_schedule_windows()
        self.assertEqual([window.label() for window in windows], ["06:55-12:00", "15:55-19:00"])

        morning = datetime(2026, 5, 5, 7, 15, tzinfo=ZoneInfo("Europe/Vienna"))
        state = get_agent_worker_schedule_state(now=morning)
        self.assertTrue(state["inside_window"])
        self.assertEqual(state["active_window"], "06:55-12:00")

        midday = datetime(2026, 5, 5, 12, 0, tzinfo=ZoneInfo("Europe/Vienna"))
        state = get_agent_worker_schedule_state(now=midday)
        self.assertFalse(state["inside_window"])
        self.assertIsNone(state["active_window"])

    def test_start_dry_run_uses_redeploy_for_stopped_latest_deployment(self) -> None:
        now = datetime(2026, 5, 7, 6, 55, tzinfo=ZoneInfo("Europe/Vienna"))
        service_state = self._service_state(
            active_deployments=[
                {
                    "id": "dep_stopped",
                    "status": "SUCCESS",
                    "created_at": "2026-05-07T04:17:59Z",
                    "deployment_stopped": True,
                    "can_redeploy": True,
                }
            ],
            latest_deployment={
                "id": "dep_stopped",
                "status": "SUCCESS",
                "created_at": "2026-05-07T04:17:59Z",
                "deployment_stopped": True,
                "can_redeploy": True,
            },
        )
        with patch("backend.agent_worker_schedule._claim_transition_lock", return_value="tok"), patch(
            "backend.agent_worker_schedule._release_transition_lock",
        ), patch(
            "backend.agent_worker_schedule.count_active_agent_voice_sessions",
            return_value={"active_sessions": 0, "oldest_started_at": None, "newest_started_at": None},
        ), patch(
            "backend.agent_worker_schedule.fetch_agent_worker_service_instance_state",
            return_value=service_state,
        ), patch(
            "backend.agent_worker_schedule.get_agent_worker_schedule_state",
            return_value=get_agent_worker_schedule_state(now=now),
        ):
            with self.assertLogs(level="INFO") as captured:
                result = run_agent_worker_schedule_control("start", source="test")
        self.assertTrue(result["ok"])
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["method"], "serviceInstanceRedeploy")
        joined = "\n".join(captured.output)
        self.assertIn("agent_worker_schedule_start_requested", joined)
        self.assertIn("start_method=serviceInstanceRedeploy", joined)

    def test_reconcile_inside_window_ensures_start(self) -> None:
        now = datetime(2026, 5, 7, 7, 0, tzinfo=ZoneInfo("Europe/Vienna"))
        service_state = self._service_state(
            active_deployments=[],
            latest_deployment={
                "id": "dep_old",
                "status": "SUCCESS",
                "created_at": "2026-05-07T04:17:59Z",
                "deployment_stopped": True,
                "can_redeploy": True,
            },
        )
        with patch.dict(os.environ, {"AGENT_WORKER_SCHEDULE_DRY_RUN": "0"}, clear=False), patch(
            "backend.agent_worker_schedule._claim_transition_lock", return_value="tok"
        ), patch(
            "backend.agent_worker_schedule._release_transition_lock",
        ), patch(
            "backend.agent_worker_schedule.count_active_agent_voice_sessions",
            return_value={"active_sessions": 0, "oldest_started_at": None, "newest_started_at": None},
        ), patch(
            "backend.agent_worker_schedule.fetch_agent_worker_service_instance_state",
            side_effect=[service_state, service_state],
        ), patch(
            "backend.agent_worker_schedule.get_agent_worker_schedule_state",
            return_value=get_agent_worker_schedule_state(now=now),
        ), patch(
            "backend.agent_worker_schedule._railway_service_instance_redeploy",
            return_value=True,
        ) as redeploy:
            result = run_agent_worker_schedule_control("reconcile_stop", source="test")
        self.assertTrue(result["ok"])
        self.assertEqual(result["method"], "serviceInstanceRedeploy")
        redeploy.assert_called_once()

    def test_reconcile_outside_window_never_starts(self) -> None:
        now = datetime(2026, 5, 7, 13, 0, tzinfo=ZoneInfo("Europe/Vienna"))
        service_state = self._service_state(
            active_deployments=[],
            latest_deployment={
                "id": "dep_old",
                "status": "SUCCESS",
                "created_at": "2026-05-07T04:17:59Z",
                "deployment_stopped": True,
                "can_redeploy": True,
            },
        )
        with patch("backend.agent_worker_schedule._claim_transition_lock", return_value="tok"), patch(
            "backend.agent_worker_schedule._release_transition_lock",
        ), patch(
            "backend.agent_worker_schedule.count_active_agent_voice_sessions",
            return_value={"active_sessions": 0, "oldest_started_at": None, "newest_started_at": None},
        ), patch(
            "backend.agent_worker_schedule.fetch_agent_worker_service_instance_state",
            side_effect=[service_state, service_state],
        ), patch(
            "backend.agent_worker_schedule.get_agent_worker_schedule_state",
            return_value=get_agent_worker_schedule_state(now=now),
        ), patch(
            "backend.agent_worker_schedule._railway_service_instance_redeploy",
        ) as redeploy:
            result = run_agent_worker_schedule_control("reconcile_stop", source="test")
        self.assertTrue(result["ok"])
        self.assertEqual(result["reason"], "already_stopped")
        redeploy.assert_not_called()

    def test_stop_skips_when_active_session_exists(self) -> None:
        now = datetime(2026, 5, 7, 20, 0, tzinfo=ZoneInfo("Europe/Vienna"))
        service_state = self._service_state(
            active_deployments=[
                {
                    "id": "dep_active",
                    "status": "SUCCESS",
                    "created_at": "2026-05-07T15:54:00Z",
                    "deployment_stopped": False,
                    "can_redeploy": True,
                }
            ],
            latest_deployment={
                "id": "dep_active",
                "status": "SUCCESS",
                "created_at": "2026-05-07T15:54:00Z",
                "deployment_stopped": False,
                "can_redeploy": True,
            },
        )
        with patch("backend.agent_worker_schedule._claim_transition_lock", return_value="tok"), patch(
            "backend.agent_worker_schedule._release_transition_lock",
        ), patch(
            "backend.agent_worker_schedule.count_active_agent_voice_sessions",
            return_value={
                "active_sessions": 1,
                "oldest_started_at": "2026-05-07T18:59:00+02:00",
                "newest_started_at": "2026-05-07T18:59:00+02:00",
            },
        ), patch(
            "backend.agent_worker_schedule.fetch_agent_worker_service_instance_state",
            return_value=service_state,
        ), patch(
            "backend.agent_worker_schedule.get_agent_worker_schedule_state",
            return_value=get_agent_worker_schedule_state(now=now),
        ):
            with self.assertLogs(level="INFO") as captured:
                result = run_agent_worker_schedule_control("stop", source="test")
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "active_session")
        joined = "\n".join(captured.output)
        self.assertIn("agent_worker_schedule_stop_requested", joined)
        self.assertIn("agent_worker_stop_skipped_active_session", joined)

    def test_stop_attempts_all_stoppable_active_deployments(self) -> None:
        now = datetime(2026, 5, 7, 20, 0, tzinfo=ZoneInfo("Europe/Vienna"))
        active = [
            {
                "id": "dep_a",
                "status": "SUCCESS",
                "created_at": "2026-05-07T15:54:00Z",
                "deployment_stopped": False,
                "can_redeploy": True,
            },
            {
                "id": "dep_b",
                "status": "INITIALIZING",
                "created_at": "2026-05-07T15:55:00Z",
                "deployment_stopped": False,
                "can_redeploy": False,
            },
        ]
        service_state = self._service_state(active_deployments=active, latest_deployment=active[-1])
        with patch.dict(os.environ, {"AGENT_WORKER_SCHEDULE_DRY_RUN": "0"}, clear=False), patch(
            "backend.agent_worker_schedule._claim_transition_lock", return_value="tok"
        ), patch(
            "backend.agent_worker_schedule._release_transition_lock",
        ), patch(
            "backend.agent_worker_schedule.count_active_agent_voice_sessions",
            return_value={"active_sessions": 0, "oldest_started_at": None, "newest_started_at": None},
        ), patch(
            "backend.agent_worker_schedule.fetch_agent_worker_service_instance_state",
            side_effect=[service_state, self._service_state(active_deployments=[], latest_deployment=active[-1])],
        ), patch(
            "backend.agent_worker_schedule.get_agent_worker_schedule_state",
            return_value=get_agent_worker_schedule_state(now=now),
        ), patch(
            "backend.agent_worker_schedule._railway_deployment_stop",
            side_effect=[True, True],
        ) as stop_mock:
            result = run_agent_worker_schedule_control("stop", source="test")
        self.assertTrue(result["ok"])
        self.assertEqual(result["stopped_deployment_ids"], ["dep_a", "dep_b"])
        self.assertEqual(stop_mock.call_count, 2)

    def test_non_stoppable_deployment_is_logged_and_skipped_without_crash(self) -> None:
        now = datetime(2026, 5, 7, 20, 30, tzinfo=ZoneInfo("Europe/Vienna"))
        active = [
            {
                "id": "dep_a",
                "status": "SUCCESS",
                "created_at": "2026-05-07T15:54:00Z",
                "deployment_stopped": False,
                "can_redeploy": True,
            },
            {
                "id": "dep_b",
                "status": "SUCCESS",
                "created_at": "2026-05-07T15:55:00Z",
                "deployment_stopped": False,
                "can_redeploy": True,
            },
        ]
        service_state = self._service_state(active_deployments=active, latest_deployment=active[-1])
        with patch.dict(os.environ, {"AGENT_WORKER_SCHEDULE_DRY_RUN": "0"}, clear=False), patch(
            "backend.agent_worker_schedule._claim_transition_lock", return_value="tok"
        ), patch(
            "backend.agent_worker_schedule._release_transition_lock",
        ), patch(
            "backend.agent_worker_schedule.count_active_agent_voice_sessions",
            return_value={"active_sessions": 0, "oldest_started_at": None, "newest_started_at": None},
        ), patch(
            "backend.agent_worker_schedule.fetch_agent_worker_service_instance_state",
            side_effect=[service_state, self._service_state(active_deployments=[active[1]], latest_deployment=active[-1])],
        ), patch(
            "backend.agent_worker_schedule.get_agent_worker_schedule_state",
            return_value=get_agent_worker_schedule_state(now=now),
        ), patch(
            "backend.agent_worker_schedule._railway_deployment_stop",
            side_effect=[True, RuntimeError("Deployment is not stoppable")],
        ):
            with self.assertLogs(level="WARNING") as captured:
                result = run_agent_worker_schedule_control("stop", source="test")
        self.assertTrue(result["ok"])
        self.assertEqual(result["stopped_deployment_ids"], ["dep_a"])
        self.assertEqual(result["skipped_non_stoppable_ids"], ["dep_b"])
        self.assertEqual(result["stop_errors"], [])
        self.assertIn("agent_worker_non_stoppable_deployment_skipped", "\n".join(captured.output))


if __name__ == "__main__":
    unittest.main()
