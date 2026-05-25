from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, time as dt_time
from typing import Any
from zoneinfo import ZoneInfo

import requests


RAILWAY_GRAPHQL_URL = "https://backboard.railway.com/graphql/v2"
DEPLOYMENT_STARTING_STATUSES = {"INITIALIZING", "BUILDING", "DEPLOYING", "QUEUED", "WAITING", "REMOVING"}
SERVICE_RESOURCE_SCHEDULE_RECONCILE_ACTION = "reconcile"


@dataclass(frozen=True)
class ServiceResourceWindow:
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


def _normalize_service_name(service_name: str) -> str:
    normalized = re.sub(r"[^A-Z0-9_]", "_", str(service_name or "").strip().upper())
    if not normalized:
        raise ValueError(f"Unsupported service name {service_name!r}")
    return normalized


def _service_schedule_prefix(service_name: str) -> str:
    return f"{_normalize_service_name(service_name)}_RESOURCE_SCHEDULE"


def _parse_hhmm(value: str) -> dt_time:
    parts = value.split(":", 1)
    if len(parts) != 2:
        raise ValueError(f"invalid time {value!r}")
    hour = int(parts[0])
    minute = int(parts[1])
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"invalid time {value!r}")
    return dt_time(hour=hour, minute=minute)


def _parse_hhmm_list(raw: str | None) -> list[dt_time]:
    values: list[dt_time] = []
    for item in str(raw or "").split(","):
        candidate = item.strip()
        if not candidate:
            continue
        values.append(_parse_hhmm(candidate))
    return values


def _profile_timezone_name(service_name: str) -> str:
    prefix = _service_schedule_prefix(service_name)
    return (os.getenv(f"{prefix}_TIMEZONE") or "Europe/Vienna").strip() or "Europe/Vienna"


def _profile_timezone(service_name: str) -> ZoneInfo:
    tz_name = _profile_timezone_name(service_name)
    try:
        return ZoneInfo(tz_name)
    except Exception:
        logging.warning(
            "service_resource_schedule: invalid timezone service=%s value=%r fallback=%s",
            _normalize_service_name(service_name),
            tz_name,
            "Europe/Vienna",
        )
        return ZoneInfo("Europe/Vienna")


def is_service_resource_schedule_enabled(service_name: str) -> bool:
    prefix = _service_schedule_prefix(service_name)
    return _enabled(f"{prefix}_ENABLED", "0")


def is_service_resource_schedule_dry_run(service_name: str) -> bool:
    prefix = _service_schedule_prefix(service_name)
    return _enabled(f"{prefix}_DRY_RUN", "1")


def get_service_resource_schedule_reconcile_minutes(service_name: str) -> int:
    prefix = _service_schedule_prefix(service_name)
    return max(1, _int_env(f"{prefix}_RECONCILE_MINUTES", 10))


def get_service_resource_schedule_windows(service_name: str) -> list[ServiceResourceWindow]:
    prefix = _service_schedule_prefix(service_name)
    start_times = _parse_hhmm_list(os.getenv(f"{prefix}_START_TIMES") or "")
    stop_times = _parse_hhmm_list(os.getenv(f"{prefix}_STOP_TIMES") or "")
    if not start_times or not stop_times:
        raise ValueError(f"{prefix}_START_TIMES/{prefix}_STOP_TIMES must not be empty")
    if len(start_times) != len(stop_times):
        raise ValueError(
            f"{prefix}_START_TIMES count {len(start_times)} does not match "
            f"{prefix}_STOP_TIMES count {len(stop_times)}"
        )
    return [ServiceResourceWindow(start=start_time, stop=stop_time) for start_time, stop_time in zip(start_times, stop_times)]


def get_service_resource_schedule_state(service_name: str, now: datetime | None = None) -> dict[str, Any]:
    tzinfo = _profile_timezone(service_name)
    current = now.astimezone(tzinfo) if now is not None else datetime.now(tzinfo)
    windows = get_service_resource_schedule_windows(service_name)
    local_time = current.timetz().replace(tzinfo=None)
    active_window = next((window for window in windows if window.contains(local_time)), None)
    return {
        "service_name": _normalize_service_name(service_name),
        "timezone": getattr(tzinfo, "key", str(tzinfo)),
        "now_local": current.isoformat(),
        "local_clock": local_time.strftime("%H:%M"),
        "windows": [window.label() for window in windows],
        "inside_window": active_window is not None,
        "active_window": active_window.label() if active_window else None,
    }


def _parse_profile_variables(raw_value: str | None) -> dict[str, str]:
    raw = str(raw_value or "").strip()
    if not raw:
        return {}
    if raw.startswith("{"):
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError("profile JSON must be an object")
        result: dict[str, str] = {}
        for key, value in parsed.items():
            normalized_key = str(key or "").strip()
            if not normalized_key:
                continue
            result[normalized_key] = "" if value is None else str(value)
        return result

    if "\n" in raw:
        items = raw.splitlines()
    elif raw.count("=") > 1 and "," in raw:
        items = raw.split(",")
    else:
        items = [raw]

    result: dict[str, str] = {}
    for item in items:
        candidate = str(item or "").strip()
        if not candidate:
            continue
        if "=" not in candidate:
            raise ValueError(f"invalid profile entry {candidate!r}")
        key, value = candidate.split("=", 1)
        normalized_key = key.strip()
        if not normalized_key:
            raise ValueError(f"invalid profile entry {candidate!r}")
        result[normalized_key] = value.strip()
    return result


def get_service_resource_profiles(service_name: str) -> dict[str, dict[str, str]]:
    prefix = _service_schedule_prefix(service_name)
    day = _parse_profile_variables(os.getenv(f"{prefix}_DAY_PROFILE") or "")
    night = _parse_profile_variables(os.getenv(f"{prefix}_NIGHT_PROFILE") or "")
    if not day:
        raise ValueError(f"{prefix}_DAY_PROFILE must not be empty")
    if not night:
        raise ValueError(f"{prefix}_NIGHT_PROFILE must not be empty")
    if set(day) != set(night):
        raise ValueError(
            f"{prefix}_DAY_PROFILE keys {sorted(day)} do not match "
            f"{prefix}_NIGHT_PROFILE keys {sorted(night)}"
        )
    return {"day": day, "night": night}


def _profile_name_for_variables(
    current_vars: dict[str, str],
    *,
    day_profile: dict[str, str],
    night_profile: dict[str, str],
) -> str:
    managed_keys = set(day_profile) | set(night_profile)
    current_subset = {key: str(current_vars.get(key) or "") for key in managed_keys}
    if current_subset == day_profile:
        return "day"
    if current_subset == night_profile:
        return "night"
    return "custom"


def _profile_diff(current_vars: dict[str, str], desired_profile: dict[str, str]) -> dict[str, dict[str, str]]:
    diff: dict[str, dict[str, str]] = {}
    for key, desired_value in desired_profile.items():
        current_value = str(current_vars.get(key) or "")
        if current_value == desired_value:
            continue
        diff[key] = {"current": current_value, "desired": desired_value}
    return diff


def _claim_transition_lock(service_name: str) -> str | None:
    from backend.job_queue import claim_shared_idempotency

    return claim_shared_idempotency(
        f"service_resource_schedule:{_normalize_service_name(service_name).lower()}:transition",
        ttl_sec=15 * 60,
    )


def _release_transition_lock(service_name: str, token: str | None) -> None:
    from backend.job_queue import release_shared_idempotency

    release_shared_idempotency(
        f"service_resource_schedule:{_normalize_service_name(service_name).lower()}:transition",
        token,
    )


def _railway_header_candidates(service_name: str) -> list[tuple[str, dict[str, str]]]:
    prefix = _service_schedule_prefix(service_name)
    project_token = str(
        os.getenv(f"{prefix}_RAILWAY_PROJECT_TOKEN")
        or os.getenv(f"{prefix}_RAILWAY_TOKEN")
        or os.getenv("RAILWAY_PROJECT_TOKEN")
        or os.getenv("RAILWAY_TOKEN")
        or ""
    ).strip()
    workspace_token = str(
        os.getenv(f"{prefix}_RAILWAY_API_TOKEN")
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
    _append("project_token_fallback", "Project-Access-Token", workspace_token)

    if not candidates:
        raise RuntimeError(
            f"Missing Railway API token for service resource schedule control: {_normalize_service_name(service_name)}"
        )
    return candidates


def _railway_payload_is_not_authorized(payload: dict[str, Any] | None) -> bool:
    for item in list((payload or {}).get("errors") or []):
        message = str((item or {}).get("message") or "").strip().lower()
        if "not authorized" in message or "unauthorized" in message:
            return True
    return False


def _railway_graphql(service_name: str, query: str, variables: dict[str, Any]) -> dict[str, Any]:
    graphql_url = os.getenv(f"{_service_schedule_prefix(service_name)}_RAILWAY_GRAPHQL_URL") or RAILWAY_GRAPHQL_URL
    header_candidates = _railway_header_candidates(service_name)
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
                        "service_resource_schedule: Railway GraphQL auth rejected service=%s auth_mode=%s retrying",
                        _normalize_service_name(service_name),
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
                    "service_resource_schedule: Railway GraphQL HTTP auth rejected service=%s auth_mode=%s status=%s retrying",
                    _normalize_service_name(service_name),
                    auth_mode,
                    status_code,
                )
                continue
            raise
        except RuntimeError as exc:
            last_exception = exc
            if not is_last and "Not Authorized" in str(exc):
                logging.warning(
                    "service_resource_schedule: Railway GraphQL payload auth rejected service=%s auth_mode=%s retrying",
                    _normalize_service_name(service_name),
                    auth_mode,
                )
                continue
            raise
    if last_exception is not None:
        raise last_exception
    return {}


def _target_service_context(service_name: str) -> dict[str, str]:
    normalized_name = _normalize_service_name(service_name)
    prefix = _service_schedule_prefix(normalized_name)
    current_service_name = _normalize_service_name(os.getenv("RAILWAY_SERVICE_NAME") or "")
    target_service_id = str(
        os.getenv(f"{prefix}_SERVICE_ID")
        or os.getenv(f"{normalized_name}_RAILWAY_SERVICE_ID")
        or (os.getenv("RAILWAY_SERVICE_ID") if normalized_name == current_service_name else "")
        or ""
    ).strip()
    environment_id = str(
        os.getenv(f"{prefix}_ENVIRONMENT_ID")
        or os.getenv(f"{normalized_name}_RAILWAY_ENVIRONMENT_ID")
        or os.getenv("RAILWAY_ENVIRONMENT_ID")
        or ""
    ).strip()
    project_id = str(
        os.getenv(f"{prefix}_PROJECT_ID")
        or os.getenv("RAILWAY_PROJECT_ID")
        or ""
    ).strip()
    if not target_service_id or not environment_id or not project_id:
        raise RuntimeError(
            f"Missing Railway target context for service resource schedule: service={normalized_name} "
            f"project_id={bool(project_id)} environment_id={bool(environment_id)} service_id={bool(target_service_id)}"
        )
    return {
        "project_id": project_id,
        "environment_id": environment_id,
        "service_id": target_service_id,
        "service_name": normalized_name,
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


def _deployment_is_starting(deployment: dict[str, Any]) -> bool:
    if bool(deployment.get("deployment_stopped")):
        return False
    return str(deployment.get("status") or "").upper() in DEPLOYMENT_STARTING_STATUSES


def fetch_service_resource_service_instance_state(service_name: str) -> dict[str, Any]:
    target = _target_service_context(service_name)
    data = _railway_graphql(
        service_name,
        """
        query ServiceResourceServiceInstance($environmentId: String!, $serviceId: String!) {
          serviceInstance(environmentId: $environmentId, serviceId: $serviceId) {
            id
            serviceId
            serviceName
            environmentId
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
        {"environmentId": target["environment_id"], "serviceId": target["service_id"]},
    )
    service_instance = dict((data.get("serviceInstance") or {}))
    active_deployments = list(service_instance.get("activeDeployments") or [])
    latest_deployment = service_instance.get("latestDeployment") or None
    return {
        "project_id": target["project_id"],
        "environment_id": service_instance.get("environmentId") or target["environment_id"],
        "service_id": service_instance.get("serviceId") or target["service_id"],
        "service_name": service_instance.get("serviceName") or target["service_name"],
        "active_deployments": [_normalize_deployment(item) for item in active_deployments if str(item.get("id") or "").strip()],
        "latest_deployment": _normalize_deployment(latest_deployment) if isinstance(latest_deployment, dict) and str(latest_deployment.get("id") or "").strip() else None,
    }


def fetch_service_resource_variables(service_name: str) -> dict[str, str]:
    target = _target_service_context(service_name)
    data = _railway_graphql(
        service_name,
        """
        query VariablesForServiceDeployment($projectId: String!, $environmentId: String!, $serviceId: String!) {
          variablesForServiceDeployment(
            projectId: $projectId
            environmentId: $environmentId
            serviceId: $serviceId
          )
        }
        """,
        {
            "projectId": target["project_id"],
            "environmentId": target["environment_id"],
            "serviceId": target["service_id"],
        },
    )
    variables = data.get("variablesForServiceDeployment") or {}
    if not isinstance(variables, dict):
        raise RuntimeError(f"Unexpected Railway variables payload type: {type(variables).__name__}")
    return {str(key): "" if value is None else str(value) for key, value in variables.items()}


def _railway_variable_collection_upsert(
    service_name: str,
    *,
    project_id: str,
    environment_id: str,
    service_id: str,
    variables: dict[str, str],
    skip_deploys: bool,
) -> bool:
    data = _railway_graphql(
        service_name,
        """
        mutation VariableCollectionUpsert(
          $projectId: String!
          $serviceId: String!
          $environmentId: String!
          $variables: EnvironmentVariables!
          $skipDeploys: Boolean
        ) {
          variableCollectionUpsert(
            input: {
              projectId: $projectId
              environmentId: $environmentId
              serviceId: $serviceId
              variables: $variables
              skipDeploys: $skipDeploys
            }
          )
        }
        """,
        {
            "projectId": project_id,
            "serviceId": service_id,
            "environmentId": environment_id,
            "variables": variables,
            "skipDeploys": skip_deploys,
        },
    )
    payload = data.get("variableCollectionUpsert")
    return True if payload is None else bool(payload)


def _railway_service_instance_redeploy(*, service_name: str, environment_id: str, service_id: str) -> bool:
    data = _railway_graphql(
        service_name,
        """
        mutation RedeployServiceResourceTarget($environmentId: String!, $serviceId: String!) {
          serviceInstanceRedeploy(environmentId: $environmentId, serviceId: $serviceId)
        }
        """,
        {"environmentId": environment_id, "serviceId": service_id},
    )
    return bool(data.get("serviceInstanceRedeploy"))


def _railway_service_instance_deploy_v2(*, service_name: str, environment_id: str, service_id: str) -> str:
    data = _railway_graphql(
        service_name,
        """
        mutation DeployServiceResourceTarget($environmentId: String!, $serviceId: String!) {
          serviceInstanceDeployV2(environmentId: $environmentId, serviceId: $serviceId)
        }
        """,
        {"environmentId": environment_id, "serviceId": service_id},
    )
    return str(data.get("serviceInstanceDeployV2") or "").strip()


def run_service_resource_schedule_control(
    action: str,
    *,
    service_name: str,
    source: str = "scheduler",
) -> dict[str, Any]:
    normalized_service_name = _normalize_service_name(service_name)
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"day", "night", SERVICE_RESOURCE_SCHEDULE_RECONCILE_ACTION}:
        raise ValueError(f"Unsupported action {action!r}")
    if not is_service_resource_schedule_enabled(normalized_service_name):
        logging.info(
            "service_resource_schedule disabled service=%s action=%s source=%s",
            normalized_service_name,
            normalized_action,
            source,
        )
        return {"ok": False, "reason": "disabled", "action": normalized_action, "service_name": normalized_service_name}

    lock_token = _claim_transition_lock(normalized_service_name)
    if lock_token is None:
        logging.info(
            "service_resource_schedule_lock_busy service=%s action=%s source=%s",
            normalized_service_name,
            normalized_action,
            source,
        )
        return {"ok": True, "reason": "lock_busy", "action": normalized_action, "service_name": normalized_service_name}

    try:
        dry_run = is_service_resource_schedule_dry_run(normalized_service_name)
        try:
            schedule_state = get_service_resource_schedule_state(normalized_service_name)
            profiles = get_service_resource_profiles(normalized_service_name)
        except Exception as exc:
            logging.exception(
                "service_resource_schedule_config_error service=%s action=%s source=%s",
                normalized_service_name,
                normalized_action,
                source,
            )
            return {
                "ok": False,
                "reason": "config_error",
                "error": str(exc),
                "action": normalized_action,
                "service_name": normalized_service_name,
            }

        desired_profile_name = "day"
        if normalized_action == "night":
            desired_profile_name = "night"
        elif normalized_action == SERVICE_RESOURCE_SCHEDULE_RECONCILE_ACTION:
            desired_profile_name = "day" if bool(schedule_state.get("inside_window")) else "night"
        desired_profile = profiles[desired_profile_name]

        try:
            service_state = fetch_service_resource_service_instance_state(normalized_service_name)
            current_vars = fetch_service_resource_variables(normalized_service_name)
        except Exception:
            logging.exception(
                "service_resource_schedule_api_error service=%s action=%s source=%s stage=fetch_state",
                normalized_service_name,
                normalized_action,
                source,
            )
            return {
                "ok": False,
                "reason": "service_state_error",
                "action": normalized_action,
                "service_name": normalized_service_name,
            }

        current_profile_name = _profile_name_for_variables(
            current_vars,
            day_profile=profiles["day"],
            night_profile=profiles["night"],
        )
        diff = _profile_diff(current_vars, desired_profile)
        active_deployments = list(service_state.get("active_deployments") or [])
        if any(_deployment_is_starting(item) for item in active_deployments):
            logging.info(
                "service_resource_schedule_skip_start_in_progress service=%s action=%s source=%s desired_profile=%s diff_keys=%s",
                normalized_service_name,
                normalized_action,
                source,
                desired_profile_name,
                sorted(diff),
            )
            return {
                "ok": False,
                "reason": "start_in_progress",
                "action": normalized_action,
                "desired_profile": desired_profile_name,
                "current_profile": current_profile_name,
                "diff_keys": sorted(diff),
                "service_name": normalized_service_name,
            }

        logging.info(
            "service_resource_schedule_requested service=%s action=%s source=%s dry_run=%s desired_profile=%s current_profile=%s now_local=%s windows=%s diff_keys=%s",
            normalized_service_name,
            normalized_action,
            source,
            dry_run,
            desired_profile_name,
            current_profile_name,
            schedule_state.get("now_local"),
            schedule_state.get("windows"),
            sorted(diff),
        )

        if not diff:
            return {
                "ok": True,
                "reason": "already_desired_profile",
                "action": normalized_action,
                "desired_profile": desired_profile_name,
                "current_profile": current_profile_name,
                "diff_keys": [],
                "service_name": normalized_service_name,
            }

        if dry_run:
            logging.info(
                "service_resource_schedule_dry_run service=%s action=%s source=%s desired_profile=%s diff_keys=%s",
                normalized_service_name,
                normalized_action,
                source,
                desired_profile_name,
                sorted(diff),
            )
            return {
                "ok": True,
                "dry_run": True,
                "action": normalized_action,
                "desired_profile": desired_profile_name,
                "current_profile": current_profile_name,
                "diff_keys": sorted(diff),
                "service_name": normalized_service_name,
            }

        project_id = str(service_state.get("project_id") or "")
        environment_id = str(service_state.get("environment_id") or "")
        service_id = str(service_state.get("service_id") or "")
        try:
            _railway_variable_collection_upsert(
                normalized_service_name,
                project_id=project_id,
                environment_id=environment_id,
                service_id=service_id,
                variables=desired_profile,
                skip_deploys=True,
            )
        except Exception:
            logging.exception(
                "service_resource_schedule_api_error service=%s action=%s source=%s stage=variable_upsert",
                normalized_service_name,
                normalized_action,
                source,
            )
            return {
                "ok": False,
                "reason": "variable_upsert_error",
                "action": normalized_action,
                "desired_profile": desired_profile_name,
                "diff_keys": sorted(diff),
                "service_name": normalized_service_name,
            }

        latest_deployment = service_state.get("latest_deployment") or {}
        method = "serviceInstanceRedeploy" if latest_deployment.get("id") else "serviceInstanceDeployV2"
        try:
            if method == "serviceInstanceRedeploy":
                ok = _railway_service_instance_redeploy(
                    service_name=normalized_service_name,
                    environment_id=environment_id,
                    service_id=service_id,
                )
                result: dict[str, Any] = {
                    "ok": ok,
                    "method": method,
                }
            else:
                deployment_id = _railway_service_instance_deploy_v2(
                    service_name=normalized_service_name,
                    environment_id=environment_id,
                    service_id=service_id,
                )
                result = {
                    "ok": bool(deployment_id),
                    "method": method,
                    "deployment_id": deployment_id,
                }
        except Exception:
            logging.exception(
                "service_resource_schedule_api_error service=%s action=%s source=%s stage=redeploy",
                normalized_service_name,
                normalized_action,
                source,
            )
            return {
                "ok": False,
                "reason": "redeploy_error",
                "action": normalized_action,
                "desired_profile": desired_profile_name,
                "diff_keys": sorted(diff),
                "service_name": normalized_service_name,
            }

        result.update(
            {
                "action": normalized_action,
                "desired_profile": desired_profile_name,
                "current_profile": current_profile_name,
                "diff_keys": sorted(diff),
                "service_name": normalized_service_name,
            }
        )
        logging.info(
            "service_resource_schedule_result service=%s action=%s source=%s ok=%s method=%s desired_profile=%s diff_keys=%s deployment_id=%s",
            normalized_service_name,
            normalized_action,
            source,
            result.get("ok"),
            result.get("method"),
            desired_profile_name,
            sorted(diff),
            result.get("deployment_id"),
        )
        return result
    finally:
        _release_transition_lock(normalized_service_name, lock_token)
