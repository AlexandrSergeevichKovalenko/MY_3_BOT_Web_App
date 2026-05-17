import os
import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from backend.translation_check_worker_schedule import (
    get_translation_check_worker_schedule_state,
    get_translation_check_worker_schedule_windows,
    run_translation_check_worker_schedule_control,
)


class TranslationCheckWorkerScheduleTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env = patch.dict(
            os.environ,
            {
                "TRANSLATION_CHECK_WORKER_SCHEDULE_ENABLED": "1",
                "TRANSLATION_CHECK_WORKER_SCHEDULE_DRY_RUN": "1",
                "TRANSLATION_CHECK_WORKER_START_TIMES": "06:30",
                "TRANSLATION_CHECK_WORKER_STOP_TIMES": "00:30",
                "TRANSLATION_CHECK_WORKER_TIMEZONE": "Europe/Vienna",
                "TRANSLATION_CHECK_WORKER_RAILWAY_ENVIRONMENT_ID": "env",
                "TRANSLATION_CHECK_WORKER_RAILWAY_SERVICE_ID": "svc",
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
            "service_name": "TRANSLATION_CHECK_WORKER",
            "active_deployments": list(active_deployments or []),
            "latest_deployment": latest_deployment,
        }

    def _deployment(
        self,
        deployment_id: str,
        *,
        status: str = "SUCCESS",
        deployment_stopped: bool = False,
        can_redeploy: bool = True,
        created_at: str = "2026-05-07T04:17:59Z",
    ) -> dict:
        return {
            "id": deployment_id,
            "status": status,
            "created_at": created_at,
            "deployment_stopped": deployment_stopped,
            "can_redeploy": can_redeploy,
        }

    def test_schedule_windows_and_vienna_state(self) -> None:
        windows = get_translation_check_worker_schedule_windows()
        self.assertEqual([window.label() for window in windows], ["06:30-00:30"])

        evening = datetime(2026, 5, 5, 23, 45, tzinfo=ZoneInfo("Europe/Vienna"))
        state = get_translation_check_worker_schedule_state(now=evening)
        self.assertTrue(state["inside_window"])
        self.assertEqual(state["active_window"], "06:30-00:30")

        night = datetime(2026, 5, 5, 3, 15, tzinfo=ZoneInfo("Europe/Vienna"))
        state = get_translation_check_worker_schedule_state(now=night)
        self.assertFalse(state["inside_window"])
        self.assertIsNone(state["active_window"])

    def test_start_dry_run_uses_redeploy_for_stopped_latest_deployment(self) -> None:
        now = datetime(2026, 5, 7, 6, 30, tzinfo=ZoneInfo("Europe/Vienna"))
        service_state = self._service_state(
            active_deployments=[self._deployment("dep_stopped", deployment_stopped=True)],
            latest_deployment=self._deployment("dep_stopped", deployment_stopped=True),
        )
        with patch(
            "backend.translation_check_worker_schedule._claim_transition_lock",
            return_value="tok",
        ), patch(
            "backend.translation_check_worker_schedule._release_transition_lock",
        ), patch(
            "backend.translation_check_worker_schedule.count_active_translation_check_sessions",
            return_value={
                "pending_sessions": 0,
                "queued_sessions": 0,
                "running_sessions": 0,
                "oldest_activity_at": None,
                "newest_activity_at": None,
            },
        ), patch(
            "backend.translation_check_worker_schedule.fetch_translation_check_worker_service_instance_state",
            return_value=service_state,
        ), patch(
            "backend.translation_check_worker_schedule.get_translation_check_worker_schedule_state",
            return_value=get_translation_check_worker_schedule_state(now=now),
        ):
            result = run_translation_check_worker_schedule_control("start", source="test")
        self.assertTrue(result["ok"])
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["method"], "serviceInstanceRedeploy")

    def test_reconcile_inside_window_ensures_start(self) -> None:
        now = datetime(2026, 5, 7, 7, 0, tzinfo=ZoneInfo("Europe/Vienna"))
        service_state = self._service_state(
            active_deployments=[],
            latest_deployment=self._deployment("dep_old", deployment_stopped=True),
        )
        with patch.dict(os.environ, {"TRANSLATION_CHECK_WORKER_SCHEDULE_DRY_RUN": "0"}, clear=False), patch(
            "backend.translation_check_worker_schedule._claim_transition_lock",
            return_value="tok",
        ), patch(
            "backend.translation_check_worker_schedule._release_transition_lock",
        ), patch(
            "backend.translation_check_worker_schedule.count_active_translation_check_sessions",
            return_value={
                "pending_sessions": 0,
                "queued_sessions": 0,
                "running_sessions": 0,
                "oldest_activity_at": None,
                "newest_activity_at": None,
            },
        ), patch(
            "backend.translation_check_worker_schedule.fetch_translation_check_worker_service_instance_state",
            side_effect=[service_state, service_state],
        ), patch(
            "backend.translation_check_worker_schedule.get_translation_check_worker_schedule_state",
            return_value=get_translation_check_worker_schedule_state(now=now),
        ), patch(
            "backend.translation_check_worker_schedule._railway_service_instance_redeploy",
            return_value=True,
        ) as redeploy:
            result = run_translation_check_worker_schedule_control("reconcile_stop", source="test")
        self.assertTrue(result["ok"])
        self.assertEqual(result["method"], "serviceInstanceRedeploy")
        redeploy.assert_called_once()

    def test_stop_skips_when_pending_translation_checks_exist(self) -> None:
        now = datetime(2026, 5, 7, 3, 0, tzinfo=ZoneInfo("Europe/Vienna"))
        service_state = self._service_state(
            active_deployments=[self._deployment("dep_active")],
            latest_deployment=self._deployment("dep_active"),
        )
        with patch(
            "backend.translation_check_worker_schedule._claim_transition_lock",
            return_value="tok",
        ), patch(
            "backend.translation_check_worker_schedule._release_transition_lock",
        ), patch(
            "backend.translation_check_worker_schedule.count_active_translation_check_sessions",
            return_value={
                "pending_sessions": 2,
                "queued_sessions": 1,
                "running_sessions": 1,
                "oldest_activity_at": "2026-05-07T02:54:00+02:00",
                "newest_activity_at": "2026-05-07T02:59:00+02:00",
            },
        ), patch(
            "backend.translation_check_worker_schedule.fetch_translation_check_worker_service_instance_state",
            return_value=service_state,
        ), patch(
            "backend.translation_check_worker_schedule.get_translation_check_worker_schedule_state",
            return_value=get_translation_check_worker_schedule_state(now=now),
        ):
            result = run_translation_check_worker_schedule_control("stop", source="test")
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "pending_translation_checks")

    def test_stop_attempts_all_stoppable_active_deployments(self) -> None:
        now = datetime(2026, 5, 7, 3, 0, tzinfo=ZoneInfo("Europe/Vienna"))
        active = [
            self._deployment("dep_a"),
            self._deployment("dep_b", status="INITIALIZING", can_redeploy=False),
        ]
        service_state = self._service_state(active_deployments=active, latest_deployment=active[-1])
        with patch.dict(os.environ, {"TRANSLATION_CHECK_WORKER_SCHEDULE_DRY_RUN": "0"}, clear=False), patch(
            "backend.translation_check_worker_schedule._claim_transition_lock",
            return_value="tok",
        ), patch(
            "backend.translation_check_worker_schedule._release_transition_lock",
        ), patch(
            "backend.translation_check_worker_schedule.count_active_translation_check_sessions",
            return_value={
                "pending_sessions": 0,
                "queued_sessions": 0,
                "running_sessions": 0,
                "oldest_activity_at": None,
                "newest_activity_at": None,
            },
        ), patch(
            "backend.translation_check_worker_schedule.fetch_translation_check_worker_service_instance_state",
            side_effect=[service_state, self._service_state(active_deployments=[], latest_deployment=active[-1])],
        ), patch(
            "backend.translation_check_worker_schedule.get_translation_check_worker_schedule_state",
            return_value=get_translation_check_worker_schedule_state(now=now),
        ), patch(
            "backend.translation_check_worker_schedule._railway_deployment_stop",
            side_effect=[True, True],
        ) as stop_mock:
            result = run_translation_check_worker_schedule_control("stop", source="test")
        self.assertTrue(result["ok"])
        self.assertEqual(result["stopped_deployment_ids"], ["dep_a", "dep_b"])
        self.assertEqual(stop_mock.call_count, 2)


if __name__ == "__main__":
    unittest.main()
