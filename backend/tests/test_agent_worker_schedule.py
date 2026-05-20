import os
import unittest
from datetime import datetime
from unittest.mock import Mock
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

    def _response(self, *, payload: dict, status_code: int = 200) -> Mock:
        response = Mock()
        response.content = b"1"
        response.json.return_value = payload
        response.raise_for_status.side_effect = None
        response.status_code = status_code
        return response

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
                self._deployment("dep_stopped", deployment_stopped=True)
            ],
            latest_deployment=self._deployment("dep_stopped", deployment_stopped=True),
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

    def test_graphql_retries_with_project_token_header_when_bearer_is_not_authorized(self) -> None:
        unauthorized = self._response(
            payload={"errors": [{"message": "Not Authorized"}]},
        )
        authorized = self._response(
            payload={"data": {"serviceInstance": {"id": "svc"}}},
        )
        with patch.dict(
            os.environ,
            {
                "AGENT_WORKER_RAILWAY_PROJECT_TOKEN": "",
                "AGENT_WORKER_RAILWAY_API_TOKEN": "shared-token",
            },
            clear=False,
        ), patch("backend.agent_worker_schedule.requests.post", side_effect=[unauthorized, authorized]) as post_mock:
            payload = __import__("backend.agent_worker_schedule", fromlist=["_railway_graphql"])._railway_graphql(
                "query Test { serviceInstance { id } }",
                {},
            )
        self.assertEqual(payload, {"serviceInstance": {"id": "svc"}})
        self.assertEqual(post_mock.call_count, 2)
        first_headers = post_mock.call_args_list[0].kwargs["headers"]
        second_headers = post_mock.call_args_list[1].kwargs["headers"]
        self.assertIn("Authorization", first_headers)
        self.assertEqual(second_headers.get("Project-Access-Token"), "shared-token")

    def test_headers_accept_generic_railway_token_as_project_token(self) -> None:
        with patch.dict(
            os.environ,
            {
                "AGENT_WORKER_RAILWAY_PROJECT_TOKEN": "",
                "AGENT_WORKER_RAILWAY_TOKEN": "",
                "RAILWAY_TOKEN": "project-token",
                "AGENT_WORKER_RAILWAY_API_TOKEN": "",
                "RAILWAY_API_TOKEN": "",
            },
            clear=False,
        ):
            headers = __import__("backend.agent_worker_schedule", fromlist=["_railway_headers"])._railway_headers()
        self.assertEqual(headers.get("Project-Access-Token"), "project-token")
        self.assertNotIn("Authorization", headers)

    def test_reconcile_inside_window_ensures_start(self) -> None:
        now = datetime(2026, 5, 7, 7, 0, tzinfo=ZoneInfo("Europe/Vienna"))
        service_state = self._service_state(
            active_deployments=[],
            latest_deployment=self._deployment("dep_old", deployment_stopped=True),
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
            latest_deployment=self._deployment("dep_old", deployment_stopped=True),
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
            active_deployments=[self._deployment("dep_active", created_at="2026-05-07T15:54:00Z")],
            latest_deployment=self._deployment("dep_active", created_at="2026-05-07T15:54:00Z"),
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
            self._deployment("dep_a", created_at="2026-05-07T15:54:00Z"),
            self._deployment("dep_b", status="INITIALIZING", can_redeploy=False, created_at="2026-05-07T15:55:00Z"),
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
            self._deployment("dep_a", created_at="2026-05-07T15:54:00Z"),
            self._deployment("dep_b", created_at="2026-05-07T15:55:00Z"),
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

    def test_starting_deployment_blocks_second_redeploy(self) -> None:
        now = datetime(2026, 5, 7, 15, 55, 22, tzinfo=ZoneInfo("Europe/Vienna"))
        starting = self._deployment("dep_starting", status="QUEUED", deployment_stopped=False)
        service_state = self._service_state(
            active_deployments=[starting, self._deployment("dep_stopped", deployment_stopped=True)],
            latest_deployment=starting,
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
        ) as redeploy:
            result = run_agent_worker_schedule_control("start", source="test")
        self.assertTrue(result["ok"])
        self.assertEqual(result["reason"], "start_in_progress")
        self.assertEqual(result["actual_state"], "starting")
        self.assertEqual(result["starting_deployment_ids"], ["dep_starting"])
        redeploy.assert_not_called()

    def test_reconcile_inside_window_returns_start_in_progress_for_starting_deployment(self) -> None:
        now = datetime(2026, 5, 7, 15, 56, tzinfo=ZoneInfo("Europe/Vienna"))
        starting = self._deployment("dep_starting", status="BUILDING")
        service_state = self._service_state(
            active_deployments=[starting],
            latest_deployment=starting,
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
        self.assertEqual(result["reason"], "start_in_progress")
        self.assertEqual(result["actual_state"], "starting")
        redeploy.assert_not_called()

    def test_starting_deployment_that_becomes_running_returns_already_running(self) -> None:
        now = datetime(2026, 5, 7, 16, 5, tzinfo=ZoneInfo("Europe/Vienna"))
        running = self._deployment("dep_running", status="SUCCESS")
        service_state = self._service_state(active_deployments=[running], latest_deployment=running)
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
        ):
            result = run_agent_worker_schedule_control("reconcile_stop", source="test")
        self.assertTrue(result["ok"])
        self.assertEqual(result["reason"], "already_running")
        self.assertEqual(result["running_deployment_ids"], ["dep_running"])

    def test_outside_window_with_starting_non_stoppable_deployment_skips_without_crash(self) -> None:
        now = datetime(2026, 5, 7, 20, 5, tzinfo=ZoneInfo("Europe/Vienna"))
        starting = self._deployment("dep_starting", status="DEPLOYING")
        service_state = self._service_state(
            active_deployments=[starting],
            latest_deployment=starting,
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
            "backend.agent_worker_schedule._railway_deployment_stop",
            side_effect=RuntimeError("Deployment is not stoppable"),
        ):
            with self.assertLogs(level="WARNING") as captured:
                result = run_agent_worker_schedule_control("stop", source="test")
        self.assertTrue(result["ok"])
        self.assertEqual(result["stopped_deployment_ids"], [])
        self.assertEqual(result["skipped_non_stoppable_ids"], ["dep_starting"])
        self.assertEqual(result["stop_errors"], [])
        self.assertIn("agent_worker_non_stoppable_deployment_skipped", "\n".join(captured.output))

    def test_no_running_no_starting_still_starts(self) -> None:
        now = datetime(2026, 5, 7, 15, 55, tzinfo=ZoneInfo("Europe/Vienna"))
        stopped = self._deployment("dep_stopped", deployment_stopped=True)
        service_state = self._service_state(active_deployments=[stopped], latest_deployment=stopped)
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
            result = run_agent_worker_schedule_control("start", source="test")
        self.assertTrue(result["ok"])
        self.assertEqual(result["method"], "serviceInstanceRedeploy")
        redeploy.assert_called_once()

    def test_duplicate_start_then_immediate_reconcile_only_one_start_mutation(self) -> None:
        now_start = datetime(2026, 5, 7, 15, 55, tzinfo=ZoneInfo("Europe/Vienna"))
        now_reconcile = datetime(2026, 5, 7, 15, 55, 22, tzinfo=ZoneInfo("Europe/Vienna"))
        stopped = self._deployment("dep_stopped", deployment_stopped=True)
        starting = self._deployment("dep_starting", status="QUEUED")
        pre_start_state = self._service_state(active_deployments=[stopped], latest_deployment=stopped)
        starting_state = self._service_state(active_deployments=[starting, stopped], latest_deployment=starting)

        state_iter = iter(
            [
                get_agent_worker_schedule_state(now=now_start),
                get_agent_worker_schedule_state(now=now_reconcile),
            ]
        )
        service_iter = iter(
            [
                pre_start_state,
                starting_state,
                starting_state,
                starting_state,
            ]
        )

        with patch.dict(os.environ, {"AGENT_WORKER_SCHEDULE_DRY_RUN": "0"}, clear=False), patch(
            "backend.agent_worker_schedule._claim_transition_lock", side_effect=["tok1", "tok2"]
        ), patch(
            "backend.agent_worker_schedule._release_transition_lock",
        ), patch(
            "backend.agent_worker_schedule.count_active_agent_voice_sessions",
            return_value={"active_sessions": 0, "oldest_started_at": None, "newest_started_at": None},
        ), patch(
            "backend.agent_worker_schedule.fetch_agent_worker_service_instance_state",
            side_effect=lambda: next(service_iter),
        ), patch(
            "backend.agent_worker_schedule.get_agent_worker_schedule_state",
            side_effect=lambda now=None: next(state_iter),
        ), patch(
            "backend.agent_worker_schedule._railway_service_instance_redeploy",
            return_value=True,
        ) as redeploy:
            first = run_agent_worker_schedule_control("start", source="test")
            second = run_agent_worker_schedule_control("reconcile_stop", source="test")
        self.assertTrue(first["ok"])
        self.assertTrue(second["ok"])
        self.assertEqual(second["reason"], "start_in_progress")
        self.assertEqual(redeploy.call_count, 1)


if __name__ == "__main__":
    unittest.main()
