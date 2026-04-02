#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import math
import os
import random
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
import psycopg2


DEFAULT_BASE_URL = "https://backendwebbackendserverpy-production.up.railway.app"
DEFAULT_SYNTHETIC_START_ID = 9100000001
DEFAULT_USER_NOTE = "load_test_translation_check_2026_03_23"
THINK_TIME_MIN_SEC = 12.0
THINK_TIME_MAX_SEC = 25.0
REVIEW_PAUSE_MIN_SEC = 3.0
REVIEW_PAUSE_MAX_SEC = 5.0
PROGRESSIVE_FILL_MAX_POLLS = 72
CHECK_STATUS_MAX_WAIT_SEC = 420.0
SESSION_TIMEOUT_SEC = 600.0
DEFAULT_LANGUAGE_PAIR = {"source_lang": "ru", "target_lang": "de"}
DEFAULT_TOPIC = "Random sentences"
DEFAULT_CUSTOM_FOCUS = ""
DEFAULT_LEVEL = "b1"
DEFAULT_PREWARM_MIN_READY = 12
DEFAULT_PREWARM_TARGET_READY = 24
DEFAULT_PREWARM_MAX_GENERATE = 12
HOT_BUCKET_PREWARM_OVERRIDES: dict[tuple[str, str], dict[str, int]] = {
    ("🔗 Порядок слов в придаточном", "b1"): {
        "min_ready": 24,
        "target_ready": 48,
        "max_generate": 24,
    },
}


@dataclass
class LoadUser:
    user_id: int
    username: str
    init_data: str
    synthetic: bool = True


@dataclass
class RequestRecord:
    stage: str
    cohort: str
    user_id: int
    flow: str
    start_ts: float
    latency_ms: float
    status_code: int | None
    ok: bool
    error: str | None = None


@dataclass
class SessionResult:
    stage: str
    cohort: str
    user_id: int
    success: bool
    failed: bool
    timeout: bool
    session_time_ms: float | None
    error: str | None = None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_users(path: str) -> list[LoadUser]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    users: list[LoadUser] = []
    for item in payload:
        users.append(
            LoadUser(
                user_id=int(item["user_id"]),
                username=str(item["username"]),
                init_data=str(item["init_data"]),
                synthetic=bool(item.get("synthetic", True)),
            )
        )
    return users


def select_users(users: list[LoadUser], *, offset: int = 0, count: int | None = None) -> list[LoadUser]:
    normalized_offset = max(0, int(offset))
    if count is None:
        return users[normalized_offset:]
    normalized_count = max(0, int(count))
    return users[normalized_offset : normalized_offset + normalized_count]


def write_json(path: str, payload: Any) -> None:
    Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    sorted_values = sorted(values)
    rank = (len(sorted_values) - 1) * (pct / 100.0)
    low = int(math.floor(rank))
    high = int(math.ceil(rank))
    if low == high:
        return sorted_values[low]
    weight = rank - low
    return sorted_values[low] * (1.0 - weight) + sorted_values[high] * weight


def summarize_latencies(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "avg": None, "p50": None, "p95": None, "p99": None}
    return {
        "count": len(values),
        "avg": sum(values) / len(values),
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
    }


def connect_db() -> psycopg2.extensions.connection:
    database_url = str(os.getenv("DATABASE_URL_RAILWAY") or "").strip()
    if not database_url:
        raise RuntimeError("DATABASE_URL_RAILWAY is required")
    return psycopg2.connect(database_url)


def build_signed_init_data(user_data: dict[str, Any], bot_token: str, auth_date: int | None = None) -> str:
    compact_user = {
        "id": int(user_data["id"]),
        "first_name": str(user_data.get("first_name") or "").strip(),
        "last_name": str(user_data.get("last_name") or "").strip(),
        "username": str(user_data.get("username") or "").strip(),
    }
    payload = {
        "auth_date": str(int(auth_date or time.time())),
        "user": json.dumps(compact_user, ensure_ascii=False, separators=(",", ":")),
    }
    data_check_string = "\n".join(f"{key}={payload[key]}" for key in sorted(payload.keys()))
    secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    payload["hash"] = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    return urlencode(payload)


def ensure_synthetic_users(args: argparse.Namespace) -> None:
    bot_token = str(os.getenv("TELEGRAM_Deutsch_BOT_TOKEN") or "").strip()
    if not bot_token:
        raise RuntimeError("TELEGRAM_Deutsch_BOT_TOKEN is required")
    count = int(args.count)
    start_id = int(args.start_id)
    note = str(args.note or DEFAULT_USER_NOTE).strip() or DEFAULT_USER_NOTE
    users: list[dict[str, Any]] = []
    with connect_db() as conn:
        with conn.cursor() as cursor:
            for idx in range(count):
                user_id = start_id + idx
                username = f"load_test_{user_id}"
                cursor.execute(
                    """
                    INSERT INTO bt_3_allowed_users (user_id, username, note)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id)
                    DO UPDATE SET
                        username = EXCLUDED.username,
                        note = EXCLUDED.note,
                        updated_at = CURRENT_TIMESTAMP;
                    """,
                    (user_id, username, note),
                )
                init_data = build_signed_init_data(
                    {
                        "id": user_id,
                        "first_name": "Load",
                        "last_name": "Test",
                        "username": username,
                    },
                    bot_token=bot_token,
                )
                users.append(
                    {
                        "user_id": user_id,
                        "username": username,
                        "init_data": init_data,
                        "synthetic": True,
                    }
                )
    write_json(args.out, users)
    print(json.dumps({"ok": True, "count": count, "out": args.out, "start_id": start_id}))


def cleanup_synthetic_users(args: argparse.Namespace) -> None:
    users = load_users(args.users_file)
    ids = [user.user_id for user in users if user.synthetic]
    removed = 0
    if not ids:
        print(json.dumps({"ok": True, "removed": 0}))
        return
    with connect_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM bt_3_allowed_users WHERE user_id = ANY(%s);",
                (ids,),
            )
            removed = int(cursor.rowcount or 0)
    print(json.dumps({"ok": True, "removed": removed}))


def db_snapshot(args: argparse.Namespace) -> None:
    users = load_users(args.users_file)
    selected = select_users(users, offset=int(getattr(args, "offset", 0)), count=getattr(args, "users", None))
    ids = [user.user_id for user in selected]
    payload = {
        "label": args.label,
        "captured_at": now_iso(),
        "user_count": len(ids),
        "user_ids": ids,
    }
    with connect_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM bt_3_translation_check_sessions WHERE user_id = ANY(%s);",
                (ids,),
            )
            payload["translation_check_sessions"] = int(cursor.fetchone()[0] or 0)
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM bt_3_translation_check_items i
                JOIN bt_3_translation_check_sessions s ON s.id = i.check_session_id
                WHERE s.user_id = ANY(%s);
                """,
                (ids,),
            )
            payload["translation_check_items"] = int(cursor.fetchone()[0] or 0)
            cursor.execute(
                "SELECT COUNT(*) FROM bt_3_skill_events_v2 WHERE user_id = ANY(%s);",
                (ids,),
            )
            payload["skill_events_v2"] = int(cursor.fetchone()[0] or 0)
            cursor.execute(
                "SELECT COUNT(*) FROM bt_3_skill_state_v2_dirty WHERE user_id = ANY(%s);",
                (ids,),
            )
            payload["skill_state_v2_dirty"] = int(cursor.fetchone()[0] or 0)
    write_json(args.out, payload)
    print(json.dumps(payload))


def skill_status_snapshot(args: argparse.Namespace) -> None:
    required_token = str(os.getenv("AUDIO_DISPATCH_TOKEN") or os.getenv("ADMIN_TOKEN") or "").strip()
    if not required_token:
        raise RuntimeError("AUDIO_DISPATCH_TOKEN or ADMIN_TOKEN is required")
    base_url = str(args.base_url or DEFAULT_BASE_URL).rstrip("/")
    with httpx.Client(http2=False, timeout=httpx.Timeout(60.0, connect=20.0)) as client:
        response = client.get(
            f"{base_url}/api/admin/skill-state-v2/status",
            params={"token": required_token},
        )
        response.raise_for_status()
        payload = response.json()
    output = {
        "label": args.label,
        "captured_at": now_iso(),
        "base_url": base_url,
        "status": payload,
    }
    write_json(args.out, output)
    print(json.dumps({"ok": True, "label": args.label, "out": args.out}))


def build_request_headers(instance_id: str, session_id: str | None = None) -> dict[str, str]:
    headers = {
        "Content-Type": "application/json",
        "X-Webapp-Instance-Id": instance_id,
        "X-Webapp-App-Context": "load-test",
    }
    if session_id:
        headers["X-Webapp-Session-Id"] = str(session_id)
    return headers


def extract_language_pair(payload: dict[str, Any] | None) -> dict[str, str] | None:
    pair = (payload or {}).get("language_pair")
    if not isinstance(pair, dict):
        return None
    source_lang = str(pair.get("source_lang") or "").strip().lower()
    target_lang = str(pair.get("target_lang") or "").strip().lower()
    if not source_lang or not target_lang:
        return None
    return {"source_lang": source_lang, "target_lang": target_lang}


def normalize_language_pair(pair: dict[str, Any] | None) -> dict[str, str]:
    extracted = extract_language_pair({"language_pair": dict(pair or {})})
    return extracted or dict(DEFAULT_LANGUAGE_PAIR)


def build_user_translations(sentences: list[dict[str, Any]], user_id: int) -> list[dict[str, Any]]:
    templates_ok = [
        "Ich denke, dass das heute wirklich wichtig ist.",
        "Das ist eine gute Idee, aber ich bin noch nicht sicher.",
        "Wir muessen das Problem so schnell wie moeglich loesen.",
        "Er hat gesagt, dass er spaeter zurueckkommt.",
        "Wenn ich mehr Zeit haette, wuerde ich es anders machen.",
        "Sie arbeitet jeden Tag sehr ruhig und konzentriert.",
        "Am Ende war alles einfacher als erwartet.",
    ]
    templates_wrong = [
        "Ich habe gehen gestern nach Hause sehr spaet.",
        "Weil ich hatte keine Zeit, ich komme nicht.",
    ]
    items: list[dict[str, Any]] = []
    for idx, sentence in enumerate(sentences):
        source_id = int(sentence.get("id_for_mistake_table") or 0)
        if (user_id + idx) % 5 == 0:
            translation = templates_wrong[(user_id + idx) % len(templates_wrong)]
        else:
            translation = templates_ok[(user_id + idx) % len(templates_ok)]
        items.append(
            {
                "id_for_mistake_table": source_id,
                "translation": translation,
            }
        )
    return items


def build_schedule_delays(user_count: int) -> list[float]:
    if user_count <= 0:
        return []
    normal_count = max(1, int(math.floor(user_count * 0.7)))
    peak_count = max(0, user_count - normal_count)
    delays: list[float] = [idx * 5.0 for idx in range(normal_count)]
    if peak_count <= 0:
        return delays
    peak_windows = {10: 10.0, 50: 30.0, 100: 45.0}
    peak_window = peak_windows.get(user_count, max(10.0, user_count * 0.6))
    peak_start = (normal_count * 5.0)
    if peak_count == 1:
        delays.append(peak_start)
        return delays
    spacing = peak_window / float(peak_count - 1)
    for idx in range(peak_count):
        delays.append(peak_start + (idx * spacing))
    return delays


def progressive_fill_poll_delay_sec(attempt: int) -> float:
    if attempt <= 1:
        return 0.0
    backoff_base = 0.85 if attempt <= 4 else min(2.5, 0.85 * (1.18 ** (attempt - 4)))
    jitter = random.uniform(0.0, 0.179)
    return backoff_base + jitter


def check_status_poll_delay_sec(attempt: int, *, suggested_delay_ms: float | None = None) -> float:
    if attempt <= 1:
        return 0.0
    backoff_base = 1.60 if attempt <= 3 else min(8.0, 1.60 * (1.22 ** (attempt - 3)))
    suggested_sec = max(0.0, float(suggested_delay_ms or 0.0) / 1000.0)
    jitter = random.uniform(0.0, 0.420)
    return max(backoff_base, suggested_sec) + jitter


def _resolve_exact_bucket_prewarm_targets(topic: str, level: str) -> tuple[int, int, int]:
    normalized_topic = str(topic or DEFAULT_TOPIC).strip() or DEFAULT_TOPIC
    normalized_level = str(level or DEFAULT_LEVEL).strip().lower() or DEFAULT_LEVEL
    override = HOT_BUCKET_PREWARM_OVERRIDES.get((normalized_topic, normalized_level)) or {}
    min_ready = int(override.get("min_ready") or DEFAULT_PREWARM_MIN_READY)
    target_ready = int(override.get("target_ready") or DEFAULT_PREWARM_TARGET_READY)
    max_generate = int(override.get("max_generate") or DEFAULT_PREWARM_MAX_GENERATE)
    return min_ready, target_ready, max_generate


def force_prewarm_exact_bucket(args: argparse.Namespace) -> dict[str, Any]:
    topic = str(getattr(args, "topic", DEFAULT_TOPIC) or DEFAULT_TOPIC).strip()
    custom_focus = str(getattr(args, "custom_focus", DEFAULT_CUSTOM_FOCUS) or "").strip()
    level = str(getattr(args, "level", DEFAULT_LEVEL) or DEFAULT_LEVEL).strip().lower() or DEFAULT_LEVEL
    source_lang = str(getattr(args, "source_lang", DEFAULT_LANGUAGE_PAIR["source_lang"]) or DEFAULT_LANGUAGE_PAIR["source_lang"]).strip().lower()
    target_lang = str(getattr(args, "target_lang", DEFAULT_LANGUAGE_PAIR["target_lang"]) or DEFAULT_LANGUAGE_PAIR["target_lang"]).strip().lower()
    min_ready, target_ready, max_generate = _resolve_exact_bucket_prewarm_targets(topic, level)
    cli_min_ready = getattr(args, "prewarm_min_ready", None)
    cli_target_ready = getattr(args, "prewarm_target_ready", None)
    cli_max_generate = getattr(args, "prewarm_max_generate", None)
    if cli_min_ready is not None:
        min_ready = max(min_ready, int(cli_min_ready))
    if cli_target_ready is not None:
        target_ready = max(target_ready, int(cli_target_ready))
    if cli_max_generate is not None:
        max_generate = max(max_generate, int(cli_max_generate))
    admin_token = str(os.getenv("AUDIO_DISPATCH_TOKEN") or os.getenv("ADMIN_TOKEN") or "").strip()
    if not admin_token:
        raise RuntimeError("AUDIO_DISPATCH_TOKEN or ADMIN_TOKEN is required for exact bucket prewarm")
    base_url = str(getattr(args, "base_url", DEFAULT_BASE_URL) or DEFAULT_BASE_URL).rstrip("/")
    url = f"{base_url}/api/admin/prewarm-translation-bucket"
    payload = {
        "token": admin_token,
        "topic": topic,
        "custom_focus": custom_focus,
        "level": level,
        "source_lang": source_lang,
        "target_lang": target_lang,
        "min_ready": int(min_ready),
        "target_ready": int(target_ready),
        "max_generate": int(max_generate),
    }
    started = time.perf_counter()
    with httpx.Client(timeout=180.0, follow_redirects=True) as client:
        response = client.post(url, json=payload)
    latency_ms = (time.perf_counter() - started) * 1000.0
    if response.status_code >= 400:
        raise RuntimeError(f"Exact bucket prewarm failed status={response.status_code} body={response.text[:400]}")
    result = response.json()
    return {
        "topic": topic,
        "custom_focus": custom_focus,
        "level": level,
        "source_lang": source_lang,
        "target_lang": target_lang,
        "min_ready": int(min_ready),
        "target_ready": int(target_ready),
        "max_generate": int(max_generate),
        "http_status": int(response.status_code),
        "latency_ms": latency_ms,
        "result": result,
    }


class StageRunner:
    def __init__(self, args: argparse.Namespace, users: list[LoadUser]) -> None:
        self.args = args
        self.users = users
        self.base_url = str(args.base_url or DEFAULT_BASE_URL).rstrip("/")
        self.stage = str(args.stage)
        self.cold_count = max(0, int(args.cold_count))
        self.topic = str(getattr(args, "topic", DEFAULT_TOPIC) or DEFAULT_TOPIC).strip() or DEFAULT_TOPIC
        self.custom_focus = str(getattr(args, "custom_focus", DEFAULT_CUSTOM_FOCUS) or "").strip()
        self.level = str(getattr(args, "level", DEFAULT_LEVEL) or DEFAULT_LEVEL).strip().lower() or DEFAULT_LEVEL
        self.language_pair = normalize_language_pair(
            {
                "source_lang": getattr(args, "source_lang", DEFAULT_LANGUAGE_PAIR["source_lang"]),
                "target_lang": getattr(args, "target_lang", DEFAULT_LANGUAGE_PAIR["target_lang"]),
            }
        )
        self.post_finish_session_read = bool(getattr(args, "post_finish_session_read", False))
        self.post_finish_home_reads = bool(getattr(args, "post_finish_home_reads", False))
        self.request_records: list[RequestRecord] = []
        self.session_results: list[SessionResult] = []

    async def _request(
        self,
        client: httpx.AsyncClient,
        *,
        method: str,
        path: str,
        flow: str,
        cohort: str,
        user_id: int,
        headers: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        timeout_sec: float = 180.0,
    ) -> httpx.Response:
        started = time.perf_counter()
        started_wall_ts = time.time()
        status_code: int | None = None
        try:
            response = await client.request(
                method,
                f"{self.base_url}{path}",
                headers=headers,
                json=json_body,
                params=params,
                timeout=httpx.Timeout(timeout_sec, connect=20.0),
            )
            status_code = int(response.status_code)
            self.request_records.append(
                RequestRecord(
                    stage=self.stage,
                    cohort=cohort,
                    user_id=user_id,
                    flow=flow,
                    start_ts=started_wall_ts,
                    latency_ms=(time.perf_counter() - started) * 1000.0,
                    status_code=status_code,
                    ok=response.is_success,
                    error=None if response.is_success else response.text[:400],
                )
            )
            return response
        except Exception as exc:
            self.request_records.append(
                RequestRecord(
                    stage=self.stage,
                    cohort=cohort,
                    user_id=user_id,
                    flow=flow,
                    start_ts=started_wall_ts,
                    latency_ms=(time.perf_counter() - started) * 1000.0,
                    status_code=status_code,
                    ok=False,
                    error=str(exc),
                )
            )
            raise

    async def _run_user(self, client: httpx.AsyncClient, user: LoadUser, delay_sec: float, index: int) -> None:
        if delay_sec > 0:
            await asyncio.sleep(delay_sec)
        stage_started = time.perf_counter()
        cohort = "cold" if index < self.cold_count else "warm"
        instance_id = f"lt_{self.stage}_{user.user_id}"
        session_id: str | None = None
        language_pair: dict[str, str] | None = dict(self.language_pair)
        acked = False
        try:
            start_response = await self._request(
                client,
                method="POST",
                path="/api/webapp/start",
                flow="start_session",
                cohort=cohort,
                user_id=user.user_id,
                headers=build_request_headers(instance_id),
                json_body={
                    "initData": user.init_data,
                    "topic": self.topic,
                    "custom_focus": self.custom_focus,
                    "level": self.level,
                    "language_pair": language_pair,
                },
            )
            start_payload = start_response.json()
            if not start_response.is_success:
                raise RuntimeError(start_payload.get("error") or start_response.text)
            session_id = str(start_payload.get("session_id") or "").strip() or None
            language_pair = extract_language_pair(start_payload) or language_pair
            sentences = list(start_payload.get("items") or [])
            expected_total = max(1, int(start_payload.get("expected_total") or 7))
            ready_count = int(start_payload.get("ready_count") or len(sentences) or 0)
            generation_status = str(
                start_payload.get("generation_status")
                or (
                    "running"
                    if start_payload.get("generation_in_progress") and ready_count < expected_total
                    else "ready"
                    if ready_count >= expected_total
                    else "idle"
                )
            ).strip().lower()
            progressive_attempts = 0
            while ready_count < expected_total and generation_status in {"pending", "running"} and progressive_attempts < PROGRESSIVE_FILL_MAX_POLLS:
                if progressive_attempts > 0:
                    await asyncio.sleep(progressive_fill_poll_delay_sec(progressive_attempts))
                progressive_attempts += 1
                fill_response = await self._request(
                    client,
                    method="POST",
                    path="/api/webapp/sentences",
                    flow="progressive_fill",
                    cohort=cohort,
                    user_id=user.user_id,
                    headers=build_request_headers(instance_id, session_id=session_id),
                    json_body={
                        "initData": user.init_data,
                        "limit": 7,
                        "session_id": session_id,
                        "language_pair": language_pair,
                    },
                )
                fill_payload = fill_response.json()
                if not fill_response.is_success:
                    raise RuntimeError(fill_payload.get("error") or fill_response.text)
                language_pair = extract_language_pair(fill_payload) or language_pair
                sentences = list(fill_payload.get("items") or [])
                ready_count = int(fill_payload.get("ready_count") or len(sentences) or 0)
                generation_status = str(
                    fill_payload.get("generation_status")
                    or (
                        "running"
                        if fill_payload.get("generation_in_progress") and ready_count < expected_total
                        else "ready"
                        if ready_count >= expected_total
                        else "idle"
                    )
                ).strip().lower()
                if generation_status == "failed":
                    raise RuntimeError(fill_payload.get("generation_error") or "Sentence generation failed")
            if not sentences:
                raise RuntimeError("No sentences available after start/progressive fill")
            if not acked:
                acked = True
            await asyncio.sleep(random.uniform(THINK_TIME_MIN_SEC, THINK_TIME_MAX_SEC))
            translations = build_user_translations(sentences, user.user_id)
            check_start_response = await self._request(
                client,
                method="POST",
                path="/api/webapp/check/start",
                flow="check_start",
                cohort=cohort,
                user_id=user.user_id,
                headers=build_request_headers(instance_id, session_id=session_id),
                json_body={
                    "initData": user.init_data,
                    "session_id": session_id,
                    "translations": translations,
                    "send_private_grammar_text": False,
                    "language_pair": language_pair,
                },
                timeout_sec=180.0,
            )
            check_start_payload = check_start_response.json()
            if not check_start_response.is_success:
                raise RuntimeError(check_start_payload.get("error") or check_start_response.text)
            check_session = check_start_payload.get("check_session") or {}
            check_session_id = int(check_session.get("id") or 0)
            if check_session_id <= 0:
                raise RuntimeError("Missing check session id")

            poll_started = time.perf_counter()
            terminal_status = ""
            status_attempt = 0
            suggested_status_delay_ms = 0.0
            while True:
                if (time.perf_counter() - poll_started) > CHECK_STATUS_MAX_WAIT_SEC:
                    raise TimeoutError("check_status timeout")
                if status_attempt > 0:
                    await asyncio.sleep(
                        check_status_poll_delay_sec(
                            status_attempt,
                            suggested_delay_ms=suggested_status_delay_ms,
                        )
                    )
                status_attempt += 1
                status_response = await self._request(
                    client,
                    method="POST",
                    path="/api/webapp/check/status",
                    flow="check_status",
                    cohort=cohort,
                    user_id=user.user_id,
                    headers=build_request_headers(instance_id, session_id=session_id),
                    json_body={
                        "initData": user.init_data,
                        "check_session_id": check_session_id,
                        "poll_count": status_attempt,
                        "language_pair": language_pair,
                    },
                    timeout_sec=120.0,
                )
                status_payload = status_response.json()
                if not status_response.is_success:
                    raise RuntimeError(status_payload.get("error") or status_response.text)
                current_session = status_payload.get("check_session") or {}
                suggested_status_delay_ms = float(
                    ((status_payload.get("polling") or {}).get("suggested_delay_ms") or 0.0)
                )
                terminal_status = str(current_session.get("status") or "").strip().lower()
                if terminal_status in {"done", "failed", "canceled"}:
                    break

            await asyncio.sleep(random.uniform(REVIEW_PAUSE_MIN_SEC, REVIEW_PAUSE_MAX_SEC))
            finish_response = await self._request(
                client,
                method="POST",
                path="/api/webapp/finish",
                flow="session_completion",
                cohort=cohort,
                user_id=user.user_id,
                headers=build_request_headers(instance_id, session_id=session_id),
                json_body={"initData": user.init_data},
                timeout_sec=120.0,
            )
            finish_payload = finish_response.json()
            if not finish_response.is_success:
                raise RuntimeError(finish_payload.get("error") or finish_response.text)

            post_finish_requests = []
            if self.post_finish_session_read:
                post_finish_requests.append(
                    self._request(
                        client,
                        method="POST",
                        path="/api/webapp/session",
                        flow="post_finish_session",
                        cohort=cohort,
                        user_id=user.user_id,
                        headers=build_request_headers(instance_id),
                        json_body={"initData": user.init_data},
                        timeout_sec=60.0,
                    )
                )
            if self.post_finish_home_reads:
                post_finish_requests.extend(
                    [
                        self._request(
                            client,
                            method="GET",
                            path="/api/today",
                            flow="post_finish_today",
                            cohort=cohort,
                            user_id=user.user_id,
                            headers={"X-Webapp-Instance-Id": instance_id},
                            params={"initData": user.init_data},
                            timeout_sec=60.0,
                        ),
                        self._request(
                            client,
                            method="GET",
                            path="/api/progress/skills",
                            flow="post_finish_skills",
                            cohort=cohort,
                            user_id=user.user_id,
                            headers={"X-Webapp-Instance-Id": instance_id},
                            params={"period": "7d", "initData": user.init_data},
                            timeout_sec=60.0,
                        ),
                    ]
                )
            if post_finish_requests:
                await asyncio.gather(*post_finish_requests)
            self.session_results.append(
                SessionResult(
                    stage=self.stage,
                    cohort=cohort,
                    user_id=user.user_id,
                    success=(terminal_status == "done"),
                    failed=(terminal_status in {"failed", "canceled"}),
                    timeout=False,
                    session_time_ms=(time.perf_counter() - stage_started) * 1000.0,
                    error=None if terminal_status == "done" else terminal_status,
                )
            )
        except TimeoutError as exc:
            self.session_results.append(
                SessionResult(
                    stage=self.stage,
                    cohort=cohort,
                    user_id=user.user_id,
                    success=False,
                    failed=False,
                    timeout=True,
                    session_time_ms=(time.perf_counter() - stage_started) * 1000.0,
                    error=str(exc),
                )
            )
        except Exception as exc:
            self.session_results.append(
                SessionResult(
                    stage=self.stage,
                    cohort=cohort,
                    user_id=user.user_id,
                    success=False,
                    failed=True,
                    timeout=False,
                    session_time_ms=(time.perf_counter() - stage_started) * 1000.0,
                    error=str(exc),
                )
            )

    async def run(self) -> dict[str, Any]:
        delays = build_schedule_delays(len(self.users))
        stage_begin_iso = now_iso()
        async with httpx.AsyncClient(http2=False) as client:
            tasks = [
                asyncio.create_task(self._run_user(client, user, delays[idx], idx))
                for idx, user in enumerate(self.users)
            ]
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=SESSION_TIMEOUT_SEC + (max(delays) if delays else 0) + 120.0)
        stage_end_iso = now_iso()
        return {
            "stage": self.stage,
            "started_at": stage_begin_iso,
            "finished_at": stage_end_iso,
            "user_count": len(self.users),
            "cold_count": min(self.cold_count, len(self.users)),
            "request_records": [asdict(item) for item in self.request_records],
            "session_results": [asdict(item) for item in self.session_results],
            "request_summary": summarize_requests(self.request_records),
            "session_summary": summarize_sessions(self.session_results),
            "flow_summary": summarize_flows(self.request_records),
        }


def summarize_requests(records: list[RequestRecord]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for cohort in ("cold", "warm", "all"):
        subset = records if cohort == "all" else [item for item in records if item.cohort == cohort]
        latencies = [item.latency_ms for item in subset]
        ok_count = sum(1 for item in subset if item.ok)
        payload[cohort] = {
            "total_requests": len(subset),
            "ok_requests": ok_count,
            "error_requests": len(subset) - ok_count,
            "success_rate": (ok_count / len(subset)) if subset else None,
            "error_rate": ((len(subset) - ok_count) / len(subset)) if subset else None,
            **summarize_latencies(latencies),
        }
    return payload


def summarize_sessions(records: list[SessionResult]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for cohort in ("cold", "warm", "all"):
        subset = records if cohort == "all" else [item for item in records if item.cohort == cohort]
        session_times = [item.session_time_ms for item in subset if item.session_time_ms is not None]
        success_count = sum(1 for item in subset if item.success)
        payload[cohort] = {
            "total_sessions": len(subset),
            "successful_sessions": success_count,
            "failed_sessions": sum(1 for item in subset if item.failed),
            "timed_out_sessions": sum(1 for item in subset if item.timeout),
            "success_rate": (success_count / len(subset)) if subset else None,
            "error_rate": ((len(subset) - success_count) / len(subset)) if subset else None,
            "avg_session_time_ms": (sum(session_times) / len(session_times)) if session_times else None,
            "p95_session_time_ms": percentile(session_times, 95),
            "p99_session_time_ms": percentile(session_times, 99),
        }
    return payload


def summarize_flows(records: list[RequestRecord]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    flows = sorted({item.flow for item in records})
    for cohort in ("cold", "warm", "all"):
        subset = records if cohort == "all" else [item for item in records if item.cohort == cohort]
        cohort_summary: dict[str, Any] = {}
        for flow in flows:
            items = [item for item in subset if item.flow == flow]
            latencies = [item.latency_ms for item in items]
            errors = sum(1 for item in items if not item.ok)
            cohort_summary[flow] = {
                "count": len(items),
                "error_rate": (errors / len(items)) if items else None,
                **summarize_latencies(latencies),
            }
        result[cohort] = cohort_summary
    return result


def run_stage(args: argparse.Namespace) -> None:
    users = load_users(args.users_file)
    offset = max(0, int(args.offset))
    count = int(args.users)
    selected = users[offset : offset + count]
    if len(selected) < count:
        raise RuntimeError(f"Need {count} users from offset {offset}, found {len(selected)}")
    random.seed(int(args.seed))
    prewarm_payload = None
    if bool(getattr(args, "force_prewarm_exact_bucket", False)):
        prewarm_payload = force_prewarm_exact_bucket(args)
    payload = asyncio.run(StageRunner(args, selected).run())
    if prewarm_payload is not None:
        payload["prewarm"] = prewarm_payload
    write_json(args.out, payload)
    print(json.dumps({
        "ok": True,
        "stage": args.stage,
        "users": count,
        "offset": offset,
        "out": args.out,
        "prewarm": prewarm_payload,
        "session_summary": payload["session_summary"],
    }))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run staged translation-check load tests against production.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare-users")
    prepare.add_argument("--count", type=int, required=True)
    prepare.add_argument("--start-id", type=int, default=DEFAULT_SYNTHETIC_START_ID)
    prepare.add_argument("--note", default=DEFAULT_USER_NOTE)
    prepare.add_argument("--out", required=True)
    prepare.set_defaults(func=ensure_synthetic_users)

    cleanup = subparsers.add_parser("cleanup-users")
    cleanup.add_argument("--users-file", required=True)
    cleanup.set_defaults(func=cleanup_synthetic_users)

    snapshot = subparsers.add_parser("db-snapshot")
    snapshot.add_argument("--users-file", required=True)
    snapshot.add_argument("--label", required=True)
    snapshot.add_argument("--out", required=True)
    snapshot.add_argument("--offset", type=int, default=0)
    snapshot.add_argument("--users", type=int)
    snapshot.set_defaults(func=db_snapshot)

    skill = subparsers.add_parser("skill-status")
    skill.add_argument("--label", required=True)
    skill.add_argument("--out", required=True)
    skill.add_argument("--base-url", default=DEFAULT_BASE_URL)
    skill.set_defaults(func=skill_status_snapshot)

    run = subparsers.add_parser("run-stage")
    run.add_argument("--users-file", required=True)
    run.add_argument("--offset", type=int, default=0)
    run.add_argument("--users", type=int, required=True)
    run.add_argument("--stage", required=True)
    run.add_argument("--out", required=True)
    run.add_argument("--base-url", default=DEFAULT_BASE_URL)
    run.add_argument("--cold-count", type=int, default=3)
    run.add_argument("--seed", type=int, default=20260323)
    run.add_argument("--topic", default=DEFAULT_TOPIC)
    run.add_argument("--custom-focus", default=DEFAULT_CUSTOM_FOCUS)
    run.add_argument("--level", default=DEFAULT_LEVEL)
    run.add_argument("--source-lang", default=DEFAULT_LANGUAGE_PAIR["source_lang"])
    run.add_argument("--target-lang", default=DEFAULT_LANGUAGE_PAIR["target_lang"])
    run.add_argument("--force-prewarm-exact-bucket", action="store_true")
    run.add_argument("--prewarm-min-ready", type=int)
    run.add_argument("--prewarm-target-ready", type=int)
    run.add_argument("--prewarm-max-generate", type=int)
    run.add_argument("--post-finish-session-read", action="store_true")
    run.add_argument("--post-finish-home-reads", action="store_true")
    run.set_defaults(func=run_stage)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
