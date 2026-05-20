from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, time as dt_time
from typing import Any
from zoneinfo import ZoneInfo

import requests


RAILWAY_GRAPHQL_URL = "https://backboard.railway.com/graphql/v2"
DEFAULT_AGENT_WORKER_TIMEZONE = "Europe/Vienna"
AGENT_WORKER_TRANSITION_LOCK_KEY = "agent_worker_schedule:transition"
AGENT_WORKER_TRANSITION_LOCK_TTL_SEC = 15 * 60
DEPLOYMENT_RUNNING_STATUSES = {"SUCCESS"}
DEPLOYMENT_STARTING_STATUSES = {"INITIALIZING", "BUILDING", "DEPLOYING", "QUEUED", "WAITING", "REMOVING"}
DEPLOYMENT_STOPPED_STATUSES = {"REMOVED"}
DEPLOYMENT_FAILED_STATUSES = {"FAILED", "CRASHED", "CANCELED", "CANCELLED"}


@dataclass(frozen=True)
class AgentWorkerWindow:
    start: dt_time
    stop: dt_time

    def contains(self, value: dt_time) -> bool:
        if self.start <= self.stop:
            return self.start <= value < self.stop
        return value >= self.start or value < self.stop

    def label(self) -> str:
        return f"{self.start.strftime('%H:%M')}-{self.stop.strftime('%H:%M')}"


def _enabled(key: str, default: str = "1") -> bool:
    return str(os.getenv(key) or default).strip().lower() in {"1", "true", "yes", "on"}


def _int_env(key: str, default: int) -> int:
    try:
        return int((os.getenv(key) or str(default)).strip())
    except Exception:
        return default


def get_agent_worker_schedule_timezone_name() -> str:
    return (os.getenv("AGENT_WORKER_TIMEZONE") or DEFAULT_AGENT_WORKER_TIMEZONE).strip() or DEFAULT_AGENT_WORKER_TIMEZONE


def get_agent_worker_schedule_timezone() -> ZoneInfo:
    tz_name = get_agent_worker_schedule_timezone_name()
    try:
        return ZoneInfo(tz_name)
    except Exception:
        logging.warning(
            "agent_worker_schedule: invalid timezone %r, falling back to %s",
            tz_name,
            DEFAULT_AGENT_WORKER_TIMEZONE,
        )
        return ZoneInfo(DEFAULT_AGENT_WORKER_TIMEZONE)


def get_agent_worker_idle_stop_grace_minutes() -> int:
    return max(1, _int_env("AGENT_WORKER_IDLE_STOP_GRACE_MINUTES", 10))


def is_agent_worker_schedule_enabled() -> bool:
    return _enabled("AGENT_WORKER_SCHEDULE_ENABLED", "0")


def is_agent_worker_schedule_dry_run() -> bool:
    return _enabled("AGENT_WORKER_SCHEDULE_DRY_RUN", "1")


def _parse_hhmm(value: str) -> dt_time:
    parts = value.split(":", 1)
    if len(parts) != 2:
        raise ValueError(f"invalid time {value!r}")
    hour = int(parts[0])
    minute = int(parts[1])
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"invalid time {value!r}")
    return dt_time(hour=hour, minute=minute)


def parse_agent_worker_schedule_times(raw: str | None) -> list[dt_time]:
    values: list[dt_time] = []
    for item in str(raw or "").split(","):
        candidate = item.strip()
        if not candidate:
            continue
        values.append(_parse_hhmm(candidate))
    return values


def get_agent_worker_schedule_windows() -> list[AgentWorkerWindow]:
    start_times = parse_agent_worker_schedule_times(os.getenv("AGENT_WORKER_START_TIMES") or "06:55,15:55")
    stop_times = parse_agent_worker_schedule_times(os.getenv("AGENT_WORKER_STOP_TIMES") or "10:00,19:00")
    if len(start_times) != len(stop_times):
        raise ValueError(
            f"AGENT_WORKER_START_TIMES count {len(start_times)} does not match "
            f"AGENT_WORKER_STOP_TIMES count {len(stop_times)}"
        )
    return [AgentWorkerWindow(start=start_time, stop=stop_time) for start_time, stop_time in zip(start_times, stop_times)]


def get_agent_worker_schedule_state(now: datetime | None = None) -> dict:
    tzinfo = get_agent_worker_schedule_timezone()
    current = now.astimezone(tzinfo) if now is not None else datetime.now(tzinfo)
    windows = get_agent_worker_schedule_windows()
    local_time = current.timetz().replace(tzinfo=None)
    active_window = next((window for window in windows if window.contains(local_time)), None)
    return {
        "timezone": getattr(tzinfo, "key", str(tzinfo)),
        "now_local": current.isoformat(),
        "local_clock": local_time.strftime("%H:%M"),
        "windows": [window.label() for window in windows],
        "inside_window": active_window is not None,
        "active_window": active_window.label() if active_window else None,
    }


def count_active_agent_voice_sessions() -> dict:
    from backend.database import get_db_connection_context

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*)::INT AS active_sessions,
                    MIN(started_at) AS oldest_started_at,
                    MAX(started_at) AS newest_started_at
                FROM bt_3_agent_voice_sessions
                WHERE ended_at IS NULL;
                """
            )
            row = cursor.fetchone() or (0, None, None)
    return {
        "active_sessions": int(row[0] or 0),
        "oldest_started_at": row[1].isoformat() if row[1] else None,
        "newest_started_at": row[2].isoformat() if row[2] else None,
    }


def _claim_transition_lock() -> str | None:
    from backend.job_queue import claim_shared_idempotency

    return claim_shared_idempotency(
        AGENT_WORKER_TRANSITION_LOCK_KEY,
        ttl_sec=AGENT_WORKER_TRANSITION_LOCK_TTL_SEC,
    )


def _release_transition_lock(token: str | None) -> None:
    from backend.job_queue import release_shared_idempotency

    release_shared_idempotency(AGENT_WORKER_TRANSITION_LOCK_KEY, token)


def _railway_headers() -> dict[str, str]:
    project_token = str(
        os.getenv("AGENT_WORKER_RAILWAY_PROJECT_TOKEN")
        or os.getenv("AGENT_WORKER_RAILWAY_TOKEN")
        or os.getenv("RAILWAY_TOKEN")
        or ""
    ).strip()
    workspace_token = str(
        os.getenv("AGENT_WORKER_RAILWAY_API_TOKEN")
        or os.getenv("RAILWAY_API_TOKEN")
        or ""
    ).strip()
    headers = {"Content-Type": "application/json"}
    if project_token:
        headers["Project-Access-Token"] = project_token
        return headers
    if workspace_token:
        headers["Authorization"] = f"Bearer {workspace_token}"
        return headers
    raise RuntimeError("Missing Railway API token for AGENT_WORKER schedule control")


def _railway_header_candidates() -> list[tuple[str, dict[str, str]]]:
    project_token = str(
        os.getenv("AGENT_WORKER_RAILWAY_PROJECT_TOKEN")
        or os.getenv("AGENT_WORKER_RAILWAY_TOKEN")
        or os.getenv("RAILWAY_TOKEN")
        or ""
    ).strip()
    workspace_token = str(
        os.getenv("AGENT_WORKER_RAILWAY_API_TOKEN")
        or os.getenv("RAILWAY_API_TOKEN")
        or ""
    ).strip()
    candidates: list[tuple[str, dict[str, str]]] = []
    seen: set[tuple[str, str]] = set()

    def _append(label: str, header_key: str, token: str) -> None:
        if not token:
            return
        signature = (header_key, token)
        if signature in seen:
            return
        seen.add(signature)
        candidates.append((label, {"Content-Type": "application/json", header_key: token}))

    _append("project_token", "Project-Access-Token", project_token)
    _append("bearer_token", "Authorization", f"Bearer {workspace_token}" if workspace_token else "")
    # Some Railway tokens are stored under *_API_TOKEN but only authorize GraphQL when
    # sent as Project-Access-Token. Retrying with both modes avoids a config-only outage.
    _append("project_token_fallback", "Project-Access-Token", workspace_token)

    if not candidates:
        raise RuntimeError("Missing Railway API token for AGENT_WORKER schedule control")
    return candidates


def _railway_payload_is_not_authorized(payload: dict[str, Any] | None) -> bool:
    for item in list((payload or {}).get("errors") or []):
        message = str((item or {}).get("message") or "").strip().lower()
        if "not authorized" in message or "unauthorized" in message:
            return True
    return False


def _railway_graphql(query: str, variables: dict) -> dict:
    graphql_url = os.getenv("AGENT_WORKER_RAILWAY_GRAPHQL_URL") or RAILWAY_GRAPHQL_URL
    header_candidates = _railway_header_candidates()
    last_exception: Exception | None = None
    for index, (auth_mode, headers) in enumerate(header_candidates):
        is_last = index == len(header_candidates) - 1
        try:
            response = requests.post(
                graphql_url,
                headers=headers,
                json={"query": query, "variables": variables},
                timeout=25,
            )
            response.raise_for_status()
            payload = response.json() if response.content else {}
            if payload.get("errors"):
                if _railway_payload_is_not_authorized(payload) and not is_last:
                    logging.warning(
                        "agent_worker_schedule: Railway GraphQL auth rejected auth_mode=%s, retrying alternate auth mode",
                        auth_mode,
                    )
                    continue
                raise RuntimeError(str(payload["errors"]))
            return payload.get("data") or {}
        except requests.HTTPError as exc:
            last_exception = exc
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if status_code in {401, 403} and not is_last:
                logging.warning(
                    "agent_worker_schedule: Railway GraphQL HTTP auth rejected auth_mode=%s status=%s, retrying alternate auth mode",
                    auth_mode,
                    status_code,
                )
                continue
            raise
        except RuntimeError as exc:
            last_exception = exc
            if not is_last and "Not Authorized" in str(exc):
                logging.warning(
                    "agent_worker_schedule: Railway GraphQL payload auth rejected auth_mode=%s, retrying alternate auth mode",
                    auth_mode,
                )
                continue
            raise
    if last_exception is not None:
        raise last_exception
    return {}


def fetch_agent_worker_service_instance_state() -> dict:
    environment_id = str(os.getenv("AGENT_WORKER_RAILWAY_ENVIRONMENT_ID") or "").strip()
    service_id = str(os.getenv("AGENT_WORKER_RAILWAY_SERVICE_ID") or "").strip()
    if not environment_id or not service_id:
        raise RuntimeError("Missing AGENT_WORKER_RAILWAY_ENVIRONMENT_ID or AGENT_WORKER_RAILWAY_SERVICE_ID")
    data = _railway_graphql(
        """
        query AgentWorkerServiceInstance($environmentId: String!, $serviceId: String!) {
          serviceInstance(environmentId: $environmentId, serviceId: $serviceId) {
            id
            serviceId
            serviceName
            environmentId
            sleepApplication
            latestDeployment {
              id
              status
              createdAt
              deploymentStopped
              canRedeploy
            }
            activeDeployments {
              id
              status
              createdAt
              deploymentStopped
              canRedeploy
            }
          }
        }
        """,
        {"environmentId": environment_id, "serviceId": service_id},
    )
    service_instance = dict((data.get("serviceInstance") or {}))
    active_deployments = list(service_instance.get("activeDeployments") or [])
    latest_deployment = service_instance.get("latestDeployment") or None
    return {
        "environment_id": service_instance.get("environmentId") or environment_id,
        "service_id": service_instance.get("serviceId") or service_id,
        "service_name": service_instance.get("serviceName") or "AGENT_WORKER",
        "sleep_application": bool(service_instance.get("sleepApplication")),
        "active_deployments": [_normalize_deployment(item) for item in active_deployments if str(item.get("id") or "").strip()],
        "latest_deployment": _normalize_deployment(latest_deployment) if isinstance(latest_deployment, dict) and str(latest_deployment.get("id") or "").strip() else None,
    }


def _normalize_deployment(item: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(item or {})
    return {
        "id": str(payload.get("id") or "").strip(),
        "status": str(payload.get("status") or "").strip().upper(),
        "created_at": payload.get("createdAt"),
        "deployment_stopped": bool(payload.get("deploymentStopped")),
        "can_redeploy": bool(payload.get("canRedeploy")),
    }


def _is_deployment_running(deployment: dict[str, Any]) -> bool:
    return _deployment_lifecycle_state(deployment) == "running"


def _is_deployment_stoppable(deployment: dict[str, Any]) -> bool:
    state = _deployment_lifecycle_state(deployment)
    return state not in {"stopped", "failed"}


def _deployment_lifecycle_state(deployment: dict[str, Any]) -> str:
    if bool(deployment.get("deployment_stopped")):
        return "stopped"
    status = str(deployment.get("status") or "").upper()
    if status in DEPLOYMENT_STOPPED_STATUSES:
        return "stopped"
    if status in DEPLOYMENT_RUNNING_STATUSES:
        return "running"
    if status in DEPLOYMENT_STARTING_STATUSES:
        return "starting"
    if status in DEPLOYMENT_FAILED_STATUSES:
        return "failed"
    if not status:
        return "unknown"
    return "unknown"


def _classify_deployments(deployments: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    buckets: dict[str, list[dict[str, Any]]] = {
        "running": [],
        "starting": [],
        "stopped": [],
        "failed": [],
        "unknown": [],
    }
    for deployment in deployments:
        buckets[_deployment_lifecycle_state(deployment)].append(deployment)
    return buckets


def _derive_actual_state(classified: dict[str, list[dict[str, Any]]]) -> str:
    if classified["running"]:
        return "running"
    if classified["starting"]:
        return "starting"
    if classified["unknown"]:
        return "unknown"
    if classified["failed"]:
        return "failed"
    return "stopped"


def _select_start_method(
    service_state: dict[str, Any],
    running_deployments: list[dict[str, Any]],
    starting_deployments: list[dict[str, Any]],
) -> tuple[str, str | None]:
    latest_deployment = service_state.get("latest_deployment") or {}
    if running_deployments:
        return "already_running", None
    if starting_deployments:
        return "start_in_progress", None
    if latest_deployment.get("id"):
        return "serviceInstanceRedeploy", str(latest_deployment.get("id") or "").strip()
    return "serviceInstanceDeployV2", None


def _apply_start(
    *,
    service_state: dict[str, Any],
    classified: dict[str, list[dict[str, Any]]],
    dry_run: bool,
    source: str,
    requested_action: str,
    selected_action: str,
    schedule_state: dict[str, Any],
) -> dict[str, Any]:
    running_deployments = classified["running"]
    starting_deployments = classified["starting"]
    stopped_deployments = classified["stopped"]
    failed_deployments = classified["failed"]
    method, target_deployment_id = _select_start_method(service_state, running_deployments, starting_deployments)
    logging.info(
        "agent_worker_schedule_start_requested source=%s requested_action=%s dry_run=%s now_local=%s windows=%s running_deployments=%s starting_deployments=%s stopped_deployments=%s failed_deployments=%s latest_deployment_id=%s start_method=%s selected_action=%s target_deployment_id=%s",
        source,
        requested_action,
        dry_run,
        schedule_state.get("now_local"),
        schedule_state.get("windows"),
        [item.get("id") for item in running_deployments],
        [item.get("id") for item in starting_deployments],
        [item.get("id") for item in stopped_deployments],
        [item.get("id") for item in failed_deployments],
        (service_state.get("latest_deployment") or {}).get("id"),
        method,
        selected_action,
        target_deployment_id,
    )
    if method == "already_running":
        return {
            "ok": True,
            "action": requested_action,
            "desired_state": "running",
            "actual_state": "running",
            "reason": "already_running",
            "running_deployment_ids": [item.get("id") for item in running_deployments],
        }
    if method == "start_in_progress":
        return {
            "ok": True,
            "action": requested_action,
            "desired_state": "running",
            "actual_state": "starting",
            "reason": "start_in_progress",
            "starting_deployment_ids": [item.get("id") for item in starting_deployments],
        }
    if dry_run:
        logging.info(
            "agent_worker_schedule_dry_run action=start source=%s requested_action=%s method=%s service_id=%s environment_id=%s",
            source,
            requested_action,
            method,
            service_state.get("service_id"),
            service_state.get("environment_id"),
        )
        return {"ok": True, "action": requested_action, "dry_run": True, "method": method, "desired_state": "running"}

    try:
        if method == "serviceInstanceRedeploy":
            ok = _railway_service_instance_redeploy(
                environment_id=str(service_state.get("environment_id") or ""),
                service_id=str(service_state.get("service_id") or ""),
            )
            result = {
                "ok": ok,
                "action": requested_action,
                "method": method,
                "requested_deployment_id": target_deployment_id,
                "desired_state": "running",
            }
        else:
            deployment_id = _railway_service_instance_deploy_v2(
                environment_id=str(service_state.get("environment_id") or ""),
                service_id=str(service_state.get("service_id") or ""),
            )
            result = {
                "ok": bool(deployment_id),
                "action": requested_action,
                "method": method,
                "deployment_id": deployment_id,
                "desired_state": "running",
            }
        logging.info(
            "agent_worker_schedule_start_result source=%s requested_action=%s method=%s ok=%s requested_deployment_id=%s deployment_id=%s",
            source,
            requested_action,
            result.get("method"),
            result.get("ok"),
            result.get("requested_deployment_id"),
            result.get("deployment_id"),
        )
        return result
    except Exception:
        logging.exception(
            "agent_worker_schedule_api_error action=%s source=%s stage=start_mutation",
            requested_action,
            source,
        )
        return {"ok": False, "reason": "start_mutation_error", "action": requested_action, "desired_state": "running"}


def _apply_stop(
    *,
    service_state: dict[str, Any],
    active_deployments: list[dict[str, Any]],
    classified: dict[str, list[dict[str, Any]]],
    dry_run: bool,
    source: str,
    requested_action: str,
    selected_action: str,
    schedule_state: dict[str, Any],
    active_sessions: dict[str, Any],
) -> dict[str, Any]:
    logging.info(
        "agent_worker_schedule_stop_requested source=%s action=%s dry_run=%s now_local=%s inside_window=%s active_sessions=%s running_deployments=%s starting_deployments=%s stopped_deployments=%s failed_deployments=%s active_deployments=%s selected_action=%s",
        source,
        requested_action,
        dry_run,
        schedule_state.get("now_local"),
        schedule_state.get("inside_window"),
        active_sessions.get("active_sessions"),
        [item.get("id") for item in classified["running"]],
        [item.get("id") for item in classified["starting"]],
        [item.get("id") for item in classified["stopped"]],
        [item.get("id") for item in classified["failed"]],
        [item.get("id") for item in active_deployments],
        selected_action,
    )
    if int(active_sessions.get("active_sessions") or 0) > 0:
        logging.info(
            "agent_worker_stop_skipped_active_session source=%s action=%s active_sessions=%s oldest_started_at=%s newest_started_at=%s",
            source,
            requested_action,
            active_sessions.get("active_sessions"),
            active_sessions.get("oldest_started_at"),
            active_sessions.get("newest_started_at"),
        )
        return {"ok": False, "reason": "active_session", "action": requested_action, "desired_state": "running"}
    stoppable_deployments = [item for item in active_deployments if _is_deployment_stoppable(item)]
    if not stoppable_deployments:
        return {
            "ok": True,
            "action": requested_action,
            "reason": "already_stopped",
            "desired_state": "stopped",
            "actual_state": "stopped",
            "skipped_deployment_ids": [item.get("id") for item in active_deployments],
        }
    if dry_run:
        logging.info(
            "agent_worker_schedule_dry_run action=stop source=%s requested_action=%s deployment_ids=%s",
            source,
            requested_action,
            [item.get("id") for item in stoppable_deployments],
        )
        return {
            "ok": True,
            "action": requested_action,
            "dry_run": True,
            "desired_state": "stopped",
            "candidate_deployment_ids": [item.get("id") for item in stoppable_deployments],
        }

    stopped_ids: list[str] = []
    skipped_non_stoppable_ids: list[str] = []
    stop_errors: list[dict[str, str]] = []
    for item in active_deployments:
        deployment_id = str(item.get("id") or "").strip()
        if not deployment_id:
            continue
        if not _is_deployment_stoppable(item):
            skipped_non_stoppable_ids.append(deployment_id)
            logging.warning(
                "agent_worker_non_stoppable_deployment_skipped source=%s action=%s deployment_id=%s status=%s deployment_stopped=%s",
                source,
                requested_action,
                deployment_id,
                item.get("status"),
                item.get("deployment_stopped"),
            )
            continue
        try:
            if _railway_deployment_stop(deployment_id=deployment_id):
                stopped_ids.append(deployment_id)
        except Exception as exc:
            message = str(exc)
            if "Deployment is not stoppable" in message:
                skipped_non_stoppable_ids.append(deployment_id)
                logging.warning(
                    "agent_worker_non_stoppable_deployment_skipped source=%s action=%s deployment_id=%s status=%s message=%s",
                    source,
                    requested_action,
                    deployment_id,
                    item.get("status"),
                    message,
                )
                continue
            stop_errors.append({"deployment_id": deployment_id, "message": message})
            logging.exception(
                "agent_worker_schedule_api_error action=%s source=%s stage=stop_mutation deployment_id=%s",
                requested_action,
                source,
                deployment_id,
            )
    return {
        "ok": not stop_errors,
        "action": requested_action,
        "desired_state": "stopped",
        "stopped_deployment_ids": stopped_ids,
        "skipped_non_stoppable_ids": skipped_non_stoppable_ids,
        "stop_errors": stop_errors,
    }


def _log_schedule_result(
    *,
    source: str,
    requested_action: str,
    selected_action: str,
    result: dict[str, Any],
    service_state: dict[str, Any] | None,
) -> None:
    active_deployments = list((service_state or {}).get("active_deployments") or [])
    classified = _classify_deployments(active_deployments)
    actual_state = _derive_actual_state(classified)
    logging.info(
        "agent_worker_schedule_result source=%s requested_action=%s ok=%s selected_action=%s desired_state=%s actual_state=%s running_deployments=%s starting_deployments=%s stopped_deployments=%s failed_deployments=%s active_deployments=%s method=%s reason=%s stopped_deployment_ids=%s skipped_non_stoppable_ids=%s stop_errors=%s",
        source,
        requested_action,
        result.get("ok"),
        selected_action,
        result.get("desired_state"),
        actual_state,
        [item.get("id") for item in classified["running"]],
        [item.get("id") for item in classified["starting"]],
        [item.get("id") for item in classified["stopped"]],
        [item.get("id") for item in classified["failed"]],
        [item.get("id") for item in active_deployments],
        result.get("method"),
        result.get("reason"),
        result.get("stopped_deployment_ids"),
        result.get("skipped_non_stoppable_ids"),
        result.get("stop_errors"),
    )


def _railway_service_instance_redeploy(*, environment_id: str, service_id: str) -> bool:
    data = _railway_graphql(
        """
        mutation RedeployAgentWorker($environmentId: String!, $serviceId: String!) {
          serviceInstanceRedeploy(environmentId: $environmentId, serviceId: $serviceId)
        }
        """,
        {"environmentId": environment_id, "serviceId": service_id},
    )
    return bool(data.get("serviceInstanceRedeploy"))


def _railway_service_instance_deploy_v2(*, environment_id: str, service_id: str) -> str:
    data = _railway_graphql(
        """
        mutation DeployAgentWorker($environmentId: String!, $serviceId: String!) {
          serviceInstanceDeployV2(environmentId: $environmentId, serviceId: $serviceId)
        }
        """,
        {"environmentId": environment_id, "serviceId": service_id},
    )
    return str(data.get("serviceInstanceDeployV2") or "").strip()


def _railway_deployment_stop(*, deployment_id: str) -> bool:
    data = _railway_graphql(
        """
        mutation StopAgentWorkerDeployment($id: String!) {
          deploymentStop(id: $id)
        }
        """,
        {"id": deployment_id},
    )
    return bool(data.get("deploymentStop"))


def run_agent_worker_schedule_control(action: str, *, source: str = "scheduler") -> dict:
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"start", "stop", "reconcile_stop"}:
        raise ValueError(f"Unsupported action {action!r}")
    if not is_agent_worker_schedule_enabled():
        logging.info("agent_worker_schedule disabled: action=%s source=%s", normalized_action, source)
        return {"ok": False, "reason": "disabled", "action": normalized_action}

    lock_token = _claim_transition_lock()
    if lock_token is None:
        logging.info(
            "agent_worker_schedule_lock_busy action=%s source=%s lock_key=%s",
            normalized_action,
            source,
            AGENT_WORKER_TRANSITION_LOCK_KEY,
        )
        return {"ok": True, "reason": "lock_busy", "action": normalized_action}

    try:
        dry_run = is_agent_worker_schedule_dry_run()
        schedule_state = get_agent_worker_schedule_state()
        active_sessions = count_active_agent_voice_sessions()

        try:
            service_state = fetch_agent_worker_service_instance_state()
        except Exception:
            logging.exception(
                "agent_worker_schedule_api_error action=%s source=%s stage=fetch_service_instance",
                normalized_action,
                source,
            )
            return {"ok": False, "reason": "service_state_error", "action": normalized_action}

        active_deployments = list(service_state.get("active_deployments") or [])
        classified = _classify_deployments(active_deployments)
        requested_action = normalized_action
        if normalized_action == "reconcile_stop":
            requested_action = "start" if bool(schedule_state.get("inside_window")) else "stop"

        if requested_action == "start":
            result = _apply_start(
                service_state=service_state,
                classified=classified,
                dry_run=dry_run,
                source=source,
                requested_action=normalized_action,
                selected_action=requested_action,
                schedule_state=schedule_state,
            )
        else:
            result = _apply_stop(
                service_state=service_state,
                active_deployments=active_deployments,
                classified=classified,
                dry_run=dry_run,
                source=source,
                requested_action=normalized_action,
                selected_action=requested_action,
                schedule_state=schedule_state,
                active_sessions=active_sessions,
            )

        refreshed_service_state = service_state
        if not dry_run:
            try:
                refreshed_service_state = fetch_agent_worker_service_instance_state()
            except Exception:
                logging.exception(
                    "agent_worker_schedule_api_error action=%s source=%s stage=refresh_service_instance",
                    normalized_action,
                    source,
                )
        _log_schedule_result(
            source=source,
            requested_action=normalized_action,
            selected_action=requested_action,
            result=result,
            service_state=refreshed_service_state,
        )
        return result
    finally:
        _release_transition_lock(lock_token)
