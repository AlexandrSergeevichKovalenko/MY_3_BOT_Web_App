import os
import unittest
from datetime import datetime
from unittest.mock import Mock
from unittest.mock import patch
from zoneinfo import ZoneInfo

from backend.service_resource_schedule import (
    get_service_resource_schedule_state,
    get_service_resource_schedule_windows,
    run_service_resource_schedule_control,
)


class ServiceResourceScheduleTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env = patch.dict(
            os.environ,
            {
                "RAILWAY_PROJECT_ID": "proj",
                "RAILWAY_ENVIRONMENT_ID": "env",
                "RAILWAY_SERVICE_ID": "svc-self",
                "RAILWAY_SERVICE_NAME": "BACKGROUND_JOBS",
                "BACKGROUND_JOBS_RESOURCE_SCHEDULE_ENABLED": "1",
                "BACKGROUND_JOBS_RESOURCE_SCHEDULE_DRY_RUN": "1",
                "BACKGROUND_JOBS_RESOURCE_SCHEDULE_START_TIMES": "06:30",
                "BACKGROUND_JOBS_RESOURCE_SCHEDULE_STOP_TIMES": "00:30",
                "BACKGROUND_JOBS_RESOURCE_SCHEDULE_TIMEZONE": "Europe/Vienna",
                "BACKGROUND_JOBS_RESOURCE_SCHEDULE_DAY_PROFILE": '{"BACKGROUND_JOBS_THREADS":"8","DRAMATIQ_WORKER_THREADS":"8","DB_POOL_MAXCONN":"10","TTS_GENERATION_WORKERS":"4"}',
                "BACKGROUND_JOBS_RESOURCE_SCHEDULE_NIGHT_PROFILE": '{"BACKGROUND_JOBS_THREADS":"4","DRAMATIQ_WORKER_THREADS":"4","DB_POOL_MAXCONN":"6","TTS_GENERATION_WORKERS":"2"}',
            },
            clear=False,
        )
        self._env.start()
        self.addCleanup(self._env.stop)

    def _service_state(
        self,
        *,
        service_id: str = "svc-self",
        service_name: str = "BACKGROUND_JOBS",
        active_deployments: list[dict] | None = None,
        latest_deployment: dict | None = None,
    ) -> dict:
        return {
            "project_id": "proj",
            "environment_id": "env",
            "service_id": service_id,
            "service_name": service_name,
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
        created_at: str = "2026-05-20T20:30:00Z",
    ) -> dict:
        return {
            "id": deployment_id,
            "status": status,
            "createdAt": created_at,
            "created_at": created_at,
            "deploymentStopped": deployment_stopped,
            "deployment_stopped": deployment_stopped,
            "canRedeploy": can_redeploy,
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
        windows = get_service_resource_schedule_windows("BACKGROUND_JOBS")
        self.assertEqual([window.label() for window in windows], ["06:30-00:30"])

        evening = datetime(2026, 5, 25, 23, 45, tzinfo=ZoneInfo("Europe/Vienna"))
        state = get_service_resource_schedule_state("BACKGROUND_JOBS", now=evening)
        self.assertTrue(state["inside_window"])
        self.assertEqual(state["active_window"], "06:30-00:30")

        night = datetime(2026, 5, 25, 3, 15, tzinfo=ZoneInfo("Europe/Vienna"))
        state = get_service_resource_schedule_state("BACKGROUND_JOBS", now=night)
        self.assertFalse(state["inside_window"])
        self.assertIsNone(state["active_window"])

    def test_dry_run_reconcile_selects_day_profile_and_reports_diff(self) -> None:
        now = datetime(2026, 5, 25, 7, 0, tzinfo=ZoneInfo("Europe/Vienna"))
        service_state = self._service_state(
            active_deployments=[self._deployment("dep_a")],
            latest_deployment=self._deployment("dep_a"),
        )
        current_vars = {
            "BACKGROUND_JOBS_THREADS": "4",
            "DRAMATIQ_WORKER_THREADS": "4",
            "DB_POOL_MAXCONN": "6",
            "TTS_GENERATION_WORKERS": "2",
        }
        with patch(
            "backend.service_resource_schedule._claim_transition_lock",
            return_value="tok",
        ), patch(
            "backend.service_resource_schedule._release_transition_lock",
        ), patch(
            "backend.service_resource_schedule.get_service_resource_schedule_state",
            return_value=get_service_resource_schedule_state("BACKGROUND_JOBS", now=now),
        ), patch(
            "backend.service_resource_schedule.fetch_service_resource_service_instance_state",
            return_value=service_state,
        ), patch(
            "backend.service_resource_schedule.fetch_service_resource_variables",
            return_value=current_vars,
        ):
            result = run_service_resource_schedule_control("reconcile", service_name="BACKGROUND_JOBS", source="test")
        self.assertTrue(result["ok"])
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["desired_profile"], "day")
        self.assertEqual(result["current_profile"], "night")
        self.assertEqual(
            result["diff_keys"],
            ["BACKGROUND_JOBS_THREADS", "DB_POOL_MAXCONN", "DRAMATIQ_WORKER_THREADS", "TTS_GENERATION_WORKERS"],
        )

    def test_reconcile_skips_when_already_in_night_profile(self) -> None:
        now = datetime(2026, 5, 25, 2, 30, tzinfo=ZoneInfo("Europe/Vienna"))
        service_state = self._service_state(
            active_deployments=[self._deployment("dep_a")],
            latest_deployment=self._deployment("dep_a"),
        )
        current_vars = {
            "BACKGROUND_JOBS_THREADS": "4",
            "DRAMATIQ_WORKER_THREADS": "4",
            "DB_POOL_MAXCONN": "6",
            "TTS_GENERATION_WORKERS": "2",
        }
        with patch(
            "backend.service_resource_schedule._claim_transition_lock",
            return_value="tok",
        ), patch(
            "backend.service_resource_schedule._release_transition_lock",
        ), patch(
            "backend.service_resource_schedule.get_service_resource_schedule_state",
            return_value=get_service_resource_schedule_state("BACKGROUND_JOBS", now=now),
        ), patch(
            "backend.service_resource_schedule.fetch_service_resource_service_instance_state",
            return_value=service_state,
        ), patch(
            "backend.service_resource_schedule.fetch_service_resource_variables",
            return_value=current_vars,
        ), patch(
            "backend.service_resource_schedule._railway_variable_collection_upsert",
        ) as upsert, patch(
            "backend.service_resource_schedule._railway_service_instance_redeploy",
        ) as redeploy:
            result = run_service_resource_schedule_control("reconcile", service_name="BACKGROUND_JOBS", source="test")
        self.assertTrue(result["ok"])
        self.assertEqual(result["reason"], "already_desired_profile")
        self.assertEqual(result["desired_profile"], "night")
        upsert.assert_not_called()
        redeploy.assert_not_called()

    def test_apply_profile_updates_variables_and_redeploys(self) -> None:
        now = datetime(2026, 5, 25, 7, 0, tzinfo=ZoneInfo("Europe/Vienna"))
        service_state = self._service_state(
            active_deployments=[self._deployment("dep_a")],
            latest_deployment=self._deployment("dep_a"),
        )
        current_vars = {
            "BACKGROUND_JOBS_THREADS": "4",
            "DRAMATIQ_WORKER_THREADS": "4",
            "DB_POOL_MAXCONN": "6",
            "TTS_GENERATION_WORKERS": "2",
        }
        with patch.dict(os.environ, {"BACKGROUND_JOBS_RESOURCE_SCHEDULE_DRY_RUN": "0"}, clear=False), patch(
            "backend.service_resource_schedule._claim_transition_lock",
            return_value="tok",
        ), patch(
            "backend.service_resource_schedule._release_transition_lock",
        ), patch(
            "backend.service_resource_schedule.get_service_resource_schedule_state",
            return_value=get_service_resource_schedule_state("BACKGROUND_JOBS", now=now),
        ), patch(
            "backend.service_resource_schedule.fetch_service_resource_service_instance_state",
            return_value=service_state,
        ), patch(
            "backend.service_resource_schedule.fetch_service_resource_variables",
            return_value=current_vars,
        ), patch(
            "backend.service_resource_schedule._railway_variable_collection_upsert",
            return_value=True,
        ) as upsert, patch(
            "backend.service_resource_schedule._railway_service_instance_redeploy",
            return_value=True,
        ) as redeploy:
            result = run_service_resource_schedule_control("day", service_name="BACKGROUND_JOBS", source="test")
        self.assertTrue(result["ok"])
        self.assertEqual(result["method"], "serviceInstanceRedeploy")
        upsert.assert_called_once()
        redeploy.assert_called_once_with(
            service_name="BACKGROUND_JOBS",
            environment_id="env",
            service_id="svc-self",
        )

    def test_config_error_when_profile_keys_mismatch(self) -> None:
        with patch.dict(
            os.environ,
            {
                "BACKGROUND_JOBS_RESOURCE_SCHEDULE_DAY_PROFILE": "BACKGROUND_JOBS_THREADS=8\nDRAMATIQ_WORKER_THREADS=8",
                "BACKGROUND_JOBS_RESOURCE_SCHEDULE_NIGHT_PROFILE": "BACKGROUND_JOBS_THREADS=4",
            },
            clear=False,
        ), patch(
            "backend.service_resource_schedule._claim_transition_lock",
            return_value="tok",
        ), patch(
            "backend.service_resource_schedule._release_transition_lock",
        ):
            result = run_service_resource_schedule_control("day", service_name="BACKGROUND_JOBS", source="test")
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "config_error")

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
                "BACKGROUND_JOBS_RESOURCE_SCHEDULE_RAILWAY_PROJECT_TOKEN": "",
                "BACKGROUND_JOBS_RESOURCE_SCHEDULE_RAILWAY_API_TOKEN": "shared-token",
                "RAILWAY_TOKEN": "",
                "RAILWAY_API_TOKEN": "",
            },
            clear=False,
        ), patch("backend.service_resource_schedule.requests.post", side_effect=[unauthorized, authorized]) as post_mock:
            payload = __import__("backend.service_resource_schedule", fromlist=["_railway_graphql"])._railway_graphql(
                "BACKGROUND_JOBS",
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
                "BACKGROUND_JOBS_RESOURCE_SCHEDULE_RAILWAY_PROJECT_TOKEN": "",
                "BACKGROUND_JOBS_RESOURCE_SCHEDULE_RAILWAY_API_TOKEN": "",
                "RAILWAY_TOKEN": "project-token",
                "RAILWAY_API_TOKEN": "",
            },
            clear=False,
        ):
            headers = __import__(
                "backend.service_resource_schedule",
                fromlist=["_railway_header_candidates"],
            )._railway_header_candidates("BACKGROUND_JOBS")[0][1]
        self.assertEqual(headers.get("Project-Access-Token"), "project-token")
        self.assertNotIn("Authorization", headers)
