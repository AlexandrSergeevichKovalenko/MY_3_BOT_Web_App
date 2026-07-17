"""Provider cost TRUTH report.

A daily admin DM that puts the numbers WE estimate (from ``bt_3_billing_events``)
next to the GROUND TRUTH each provider reports through its own billing API — the
exact figures they invoice us on. Purpose: catch the case where our own counter
is blind (as happened once when background TTS synthesis silently crossed the
free tier) by comparing against the provider's authoritative number.

Sources of truth, per provider:
  * OpenAI  -> Costs API  GET /v1/organization/costs   (needs an Admin key)
  * Google  -> Cloud Billing export in BigQuery         (needs export enabled)
  * DeepL   -> Usage API  GET /v2/usage                 (chars used vs free limit)
  * Perplexity -> NO programmatic usage API exists (dashboard only) -> our
    estimate is shown without a truth column.

Every fetcher is defensive: a missing key/config yields "не настроено" and any
network/parse error is captured into the report instead of crashing the job.
"""
from __future__ import annotations

import calendar
import logging
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests

from backend.database import (
    claim_scheduler_run_guard,
    finish_scheduler_run_guard,
    get_admin_telegram_ids,
    get_db_connection_context,
)

DEFAULT_TZ = "Europe/Vienna"
JOB_KEY = "provider_cost_truth_daily"
_HTTP_TIMEOUT = 25

# Map an external provider -> the ``bt_3_billing_events.provider`` keys that roll
# up into it, so OUR estimate can sit next to the provider's ground truth.
# (agent_tts = Chirp3-HD premium voice, also billed by Google Cloud TTS.)
_OUR_PROVIDER_GROUPS: dict[str, tuple[str, ...]] = {
    "openai": ("openai",),
    "google": (
        "google_tts",
        "agent_tts",
        "google_translate",
        "youtube_api",
        "youtube_manual_search",
        "youtube_proxy",
    ),
    "deepl": ("deepl_free",),
    "perplexity": ("perplexity",),
}

# Cloudflare R2 — monthly free allowances + list prices (2026) for an overage estimate.
_CF_GRAPHQL = "https://api.cloudflare.com/client/v4/graphql"
_R2_FREE_STORAGE_GB = 10.0
_R2_FREE_CLASS_A = 1_000_000
_R2_FREE_CLASS_B = 10_000_000
_R2_PRICE_STORAGE_GB = 0.015
_R2_PRICE_CLASS_A_PER_M = 4.50
_R2_PRICE_CLASS_B_PER_M = 0.36
# actionType → billing class. Free ops are skipped; everything not Class A counts as B.
_R2_CLASS_A_ACTIONS = {
    "ListBuckets", "PutBucket", "ListObjects", "PutObject", "CopyObject",
    "CompleteMultipartUpload", "CreateMultipartUpload", "ListMultipartUploads",
    "LifecycleStorageTierTransition", "UploadPart", "UploadPartCopy",
    "PutBucketEncryption", "PutBucketCors", "PutBucketLifecycleConfiguration",
}
_R2_FREE_ACTIONS = {"DeleteObject", "DeleteBucket", "AbortMultipartUpload"}

_RAILWAY_GRAPHQL = "https://backboard.railway.com/graphql/v2"


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _tz_name() -> str:
    return (os.getenv("PROVIDER_COST_TRUTH_TZ") or DEFAULT_TZ).strip() or DEFAULT_TZ


def _fmt_usd(value: float | None) -> str:
    try:
        return f"${float(value or 0.0):,.2f}"
    except Exception:
        return "$0.00"


def _delta_line(*, label: str, truth: float | None, ours: float | None) -> str:
    """One comparison line: truth vs our estimate + signed delta and %.
    Flags ⚠️ when the two diverge by more than 10% (our counter likely blind)."""
    t = float(truth or 0.0)
    o = float(ours or 0.0)
    diff = o - t
    if t > 0.005:
        pct = diff / t * 100.0
        warn = " ⚠️" if abs(pct) > 10.0 else ""
        pct_txt = f"{pct:+.0f}%"
    else:
        warn = " ⚠️" if o > 0.01 else ""
        pct_txt = "—"
    return (
        f"  {label}: эталон {_fmt_usd(t)} | наш {_fmt_usd(o)} | "
        f"Δ {diff:+.2f} ({pct_txt}){warn}"
    )


# --------------------------------------------------------------------------- #
# OUR estimate (from the billing ledger)
# --------------------------------------------------------------------------- #
def fetch_our_estimate(*, start_day: date, end_day: date, tz_name: str) -> dict[str, float]:
    """Sum ``cost_amount`` per provider over [start_day, end_day] (inclusive),
    in the given timezone. Returns {billing_provider: usd}."""
    out: dict[str, float] = {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COALESCE(provider, '') AS provider,
                       COALESCE(SUM(cost_amount), 0) AS cost
                FROM bt_3_billing_events
                WHERE (event_time AT TIME ZONE %s)::date BETWEEN %s AND %s
                GROUP BY COALESCE(provider, '');
                """,
                (tz_name, start_day, end_day),
            )
            for provider, cost in cursor.fetchall() or []:
                out[str(provider or "")] = float(cost or 0.0)
    return out


def _our_group_total(estimate: dict[str, float], external: str) -> float:
    keys = _OUR_PROVIDER_GROUPS.get(external, ())
    return sum(float(estimate.get(k, 0.0)) for k in keys)


# --------------------------------------------------------------------------- #
# OpenAI Costs API  (ground truth in USD)
# --------------------------------------------------------------------------- #
def fetch_openai_costs(*, start_day: date, yesterday: date) -> dict[str, Any]:
    """Daily cost buckets from OpenAI's Costs API for [start_day, today].
    Returns {'configured', 'mtd_usd', 'yday_usd', 'by_line_item', 'error'}.
    Buckets are UTC days (OpenAI-side)."""
    key = (os.getenv("OPENAI_ADMIN_KEY") or "").strip()
    if not key:
        return {"configured": False}
    start_ts = int(datetime(start_day.year, start_day.month, start_day.day, tzinfo=timezone.utc).timestamp())
    try:
        mtd = 0.0
        yday = 0.0
        by_line: dict[str, float] = {}
        params = {
            "start_time": start_ts,
            "bucket_width": "1d",
            "limit": 62,
            "group_by": "line_item",
        }
        page = None
        for _ in range(10):  # pagination guard
            if page:
                params["page"] = page
            resp = requests.get(
                "https://api.openai.com/v1/organization/costs",
                headers={"Authorization": f"Bearer {key}"},
                params=params,
                timeout=_HTTP_TIMEOUT,
            )
            if resp.status_code >= 400:
                return {"configured": True, "error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
            body = resp.json() if resp.content else {}
            for bucket in body.get("data") or []:
                b_start = bucket.get("start_time")
                b_date = (
                    datetime.fromtimestamp(int(b_start), tz=timezone.utc).date()
                    if b_start is not None
                    else None
                )
                for item in bucket.get("results") or []:
                    amount = float((item.get("amount") or {}).get("value") or 0.0)
                    mtd += amount
                    if b_date == yesterday:
                        yday += amount
                    line = str(item.get("line_item") or "—")
                    by_line[line] = by_line.get(line, 0.0) + amount
            if body.get("has_more") and body.get("next_page"):
                page = body.get("next_page")
                continue
            break
        return {
            "configured": True,
            "mtd_usd": mtd,
            "yday_usd": yday,
            "by_line_item": dict(sorted(by_line.items(), key=lambda kv: kv[1], reverse=True)),
        }
    except Exception as exc:  # noqa: BLE001
        logging.warning("OpenAI costs fetch failed: %s", exc, exc_info=True)
        return {"configured": True, "error": str(exc)}


# --------------------------------------------------------------------------- #
# Google Cloud Billing export -> BigQuery  (ground truth in USD)
# --------------------------------------------------------------------------- #
def fetch_google_costs(*, start_day: date, yesterday: date, tz_name: str) -> dict[str, Any]:
    """Net cost (cost + credits) per Google service for [start_day, today] and for
    yesterday, from the Cloud Billing export table in BigQuery.
    Requires env GOOGLE_BILLING_BQ_TABLE = `project.dataset.gcp_billing_export_*`."""
    table = (os.getenv("GOOGLE_BILLING_BQ_TABLE") or "").strip()
    if not table:
        return {"configured": False}
    try:
        # Lazy import so the module loads even before the dep is deployed.
        from google.cloud import bigquery  # type: ignore

        from backend.utils import prepare_google_creds_for_tts

        key_path = prepare_google_creds_for_tts()
        if key_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
        project = (os.getenv("GOOGLE_CLOUD_PROJECT") or "").strip() or None
        client = bigquery.Client(project=project) if project else bigquery.Client()

        query = f"""
            SELECT
              service.description AS service,
              SUM(IF(DATE(usage_start_time, @tz) = @yday,
                     cost + IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0), 0)) AS net_yday,
              SUM(cost + IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) AS net_mtd
            FROM `{table}`
            WHERE DATE(usage_start_time, @tz) >= @month_start
            GROUP BY service
            ORDER BY net_mtd DESC
        """
        job = client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("tz", "STRING", tz_name),
                    bigquery.ScalarQueryParameter("yday", "DATE", yesterday),
                    bigquery.ScalarQueryParameter("month_start", "DATE", start_day),
                ]
            ),
        )
        by_service: dict[str, dict[str, float]] = {}
        mtd_total = yday_total = 0.0
        for row in job.result():
            svc = str(row["service"] or "—")
            net_mtd = float(row["net_mtd"] or 0.0)
            net_yday = float(row["net_yday"] or 0.0)
            by_service[svc] = {"mtd": net_mtd, "yday": net_yday}
            mtd_total += net_mtd
            yday_total += net_yday
        return {
            "configured": True,
            "mtd_usd": mtd_total,
            "yday_usd": yday_total,
            "by_service": by_service,
        }
    except Exception as exc:  # noqa: BLE001
        logging.warning("Google BigQuery costs fetch failed: %s", exc, exc_info=True)
        return {"configured": True, "error": str(exc)}


# --------------------------------------------------------------------------- #
# DeepL usage  (chars used vs monthly free allowance)
# --------------------------------------------------------------------------- #
def fetch_deepl_usage() -> dict[str, Any]:
    key = (os.getenv("DEEPL_AUTH_KEY") or "").strip()
    if not key:
        return {"configured": False}
    base = "https://api.deepl.com" if not key.endswith(":fx") else "https://api-free.deepl.com"
    try:
        resp = requests.get(
            f"{base}/v2/usage",
            headers={"Authorization": f"DeepL-Auth-Key {key}"},
            timeout=_HTTP_TIMEOUT,
        )
        if resp.status_code >= 400:
            return {"configured": True, "error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
        body = resp.json() if resp.content else {}
        used = int(body.get("character_count") or 0)
        limit = int(body.get("character_limit") or 0)
        return {"configured": True, "used": used, "limit": limit}
    except Exception as exc:  # noqa: BLE001
        logging.warning("DeepL usage fetch failed: %s", exc, exc_info=True)
        return {"configured": True, "error": str(exc)}


# --------------------------------------------------------------------------- #
# Cloudflare R2  (storage GB + Class A/B ops vs free tier)
# --------------------------------------------------------------------------- #
def fetch_cloudflare_r2(*, start_day: date) -> dict[str, Any]:
    """Account-level R2 usage from Cloudflare's GraphQL Analytics API: latest stored
    bytes + object count, and month-to-date Class A/B operations, with an overage
    estimate vs the free tier. Needs CLOUDFLARE_API_TOKEN (Account Analytics: Read)
    + R2_ACCOUNT_ID. Metrics are retained ~31 days, so MTD is reliable early-month."""
    token = (os.getenv("CLOUDFLARE_API_TOKEN") or "").strip()
    account = (os.getenv("R2_ACCOUNT_ID") or "").strip()
    if not token or not account:
        return {"configured": False}
    start_iso = f"{start_day.isoformat()}T00:00:00Z"
    end_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    storage_q = (
        "query($tag:String!,$start:Time,$end:Time){viewer{accounts(filter:{accountTag:$tag}){"
        "r2StorageAdaptiveGroups(limit:1,filter:{datetime_geq:$start,datetime_leq:$end},orderBy:[datetime_DESC]){"
        "max{objectCount payloadSize metadataSize} dimensions{datetime}}}}}"
    )
    ops_q = (
        "query($tag:String!,$start:Time,$end:Time){viewer{accounts(filter:{accountTag:$tag}){"
        "r2OperationsAdaptiveGroups(limit:10000,filter:{datetime_geq:$start,datetime_leq:$end}){"
        "sum{requests} dimensions{actionType}}}}}"
    )
    variables = {"tag": account, "start": start_iso, "end": end_iso}
    try:
        s_resp = requests.post(_CF_GRAPHQL, headers=headers,
                               json={"query": storage_q, "variables": variables}, timeout=_HTTP_TIMEOUT)
        o_resp = requests.post(_CF_GRAPHQL, headers=headers,
                               json={"query": ops_q, "variables": variables}, timeout=_HTTP_TIMEOUT)
        for r in (s_resp, o_resp):
            if r.status_code >= 400:
                return {"configured": True, "error": f"HTTP {r.status_code}: {r.text[:200]}"}
        s_body = s_resp.json()
        o_body = o_resp.json()
        if s_body.get("errors"):
            return {"configured": True, "error": str(s_body["errors"])[:200]}
        if o_body.get("errors"):
            return {"configured": True, "error": str(o_body["errors"])[:200]}

        def _accounts(body: dict) -> list:
            return (((body.get("data") or {}).get("viewer") or {}).get("accounts") or [{}])

        s_groups = _accounts(s_body)[0].get("r2StorageAdaptiveGroups") or []
        stored_bytes = 0.0
        object_count = 0
        if s_groups:
            mx = s_groups[0].get("max") or {}
            stored_bytes = float(mx.get("payloadSize") or 0) + float(mx.get("metadataSize") or 0)
            object_count = int(mx.get("objectCount") or 0)

        o_groups = _accounts(o_body)[0].get("r2OperationsAdaptiveGroups") or []
        class_a = class_b = 0
        for g in o_groups:
            action = str((g.get("dimensions") or {}).get("actionType") or "")
            reqs = int((g.get("sum") or {}).get("requests") or 0)
            if action in _R2_FREE_ACTIONS:
                continue
            if action in _R2_CLASS_A_ACTIONS:
                class_a += reqs
            else:
                class_b += reqs

        gb = stored_bytes / 1e9
        est_cost = (
            max(0.0, gb - _R2_FREE_STORAGE_GB) * _R2_PRICE_STORAGE_GB
            + max(0, class_a - _R2_FREE_CLASS_A) / 1e6 * _R2_PRICE_CLASS_A_PER_M
            + max(0, class_b - _R2_FREE_CLASS_B) / 1e6 * _R2_PRICE_CLASS_B_PER_M
        )
        return {
            "configured": True,
            "gb": gb,
            "object_count": object_count,
            "class_a": class_a,
            "class_b": class_b,
            "est_cost": est_cost,
        }
    except Exception as exc:  # noqa: BLE001
        logging.warning("Cloudflare R2 fetch failed: %s", exc, exc_info=True)
        return {"configured": True, "error": str(exc)}


# --------------------------------------------------------------------------- #
# Railway  (infra usage estimate for the current month)
# --------------------------------------------------------------------------- #
def fetch_railway_usage(*, start_day: date) -> dict[str, Any]:
    """Month-to-date estimated usage ($) from Railway's GraphQL API. Railway does NOT
    document this billing query, so it is self-diagnosing: a wrong field name comes
    back in ``error`` (GraphQL echoes a "did you mean …" hint) and we patch from there.
    Needs RAILWAY_API_TOKEN (account/workspace token); RAILWAY_WORKSPACE_ID optional."""
    token = (os.getenv("RAILWAY_API_TOKEN") or "").strip()
    if not token:
        return {"configured": False}
    workspace = (os.getenv("RAILWAY_WORKSPACE_ID") or "").strip() or None
    # estimatedUsage rejects date args (returns the current billing cycle) and is scoped
    # by the token; workspaceId is only sent when explicitly provided, to keep the arg
    # list minimal (fewer chances for an "unknown argument" validation error).
    var_defs = ["$measurements:[MetricMeasurement!]!"]
    field_args = ["measurements:$measurements"]
    variables: dict[str, Any] = {"measurements": ["CPU_USAGE", "MEMORY_USAGE_GB", "NETWORK_TX_GB"]}
    if workspace:
        var_defs.append("$workspaceId:String")
        field_args.append("workspaceId:$workspaceId")
        variables["workspaceId"] = workspace
    query = (
        f"query usage({','.join(var_defs)}){{"
        f"estimatedUsage({','.join(field_args)}){{measurement estimatedValue}}}}"
    )
    try:
        # Railway caps concurrent usage queries (16/client) and answers with a transient
        # INTERNAL_SERVER_ERROR when busy; back off and retry a couple of times.
        last_msg = ""
        for attempt in range(3):
            resp = requests.post(
                _RAILWAY_GRAPHQL,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"query": query, "variables": variables},
                timeout=_HTTP_TIMEOUT,
            )
            if resp.status_code >= 400:
                return {"configured": True, "error": f"HTTP {resp.status_code}: {resp.text[:600]}"}
            body = resp.json()
            errs = body.get("errors")
            if errs:
                last_msg = str(errs)
                if ("usage queries" in last_msg or "retry in" in last_msg
                        or "INTERNAL_SERVER_ERROR" in last_msg) and attempt < 2:
                    time.sleep(6 * (attempt + 1))
                    continue
                if "usage queries" in last_msg or "retry in" in last_msg:
                    return {"configured": True, "error": "Railway ограничил одновременные usage-запросы (лимит 16). Данные подтянутся в следующем отчёте."}
                return {"configured": True, "error": last_msg[:600]}
            rows = ((body.get("data") or {}).get("estimatedUsage")) or []
            # estimatedValue is the RAW metered quantity (GB-minutes of RAM, vCPU-minutes,
            # GB egress), NOT dollars — price it with Railway's list rates ($10/GB-month RAM,
            # $20/vCPU-month CPU, $0.05/GB egress, $0.25/GB-month volume). Calibrated within
            # ~2-4% of the dashboard's "Estimated Usage" panel (which this query powers).
            now = datetime.now(timezone.utc)
            minutes_in_month = calendar.monthrange(now.year, now.month)[1] * 24 * 60
            unit_price = {
                "MEMORY_USAGE_GB": 10.0 / minutes_in_month,
                "CPU_USAGE": 20.0 / minutes_in_month,
                "NETWORK_TX_GB": 0.05,
                "DISK_USAGE_GB": 0.25 / minutes_in_month,
            }
            by_measure: dict[str, float] = {}
            total = 0.0
            for r in rows:
                m = str(r.get("measurement") or "—")
                cost = float(r.get("estimatedValue") or 0.0) * unit_price.get(m, 0.0)
                by_measure[m] = by_measure.get(m, 0.0) + cost
                total += cost
            return {"configured": True, "total_usd": total, "by_measure": by_measure}
        return {"configured": True, "error": (last_msg or "Railway usage unavailable")[:600]}
    except Exception as exc:  # noqa: BLE001
        logging.warning("Railway usage fetch failed: %s", exc, exc_info=True)
        return {"configured": True, "error": str(exc)}


# --------------------------------------------------------------------------- #
# report text
# --------------------------------------------------------------------------- #
def build_provider_cost_truth_text(*, target_day: date | None = None, tz_name: str | None = None) -> str:
    tz_name = tz_name or _tz_name()
    yesterday = target_day or (datetime.now(timezone.utc).date() - timedelta(days=1))
    month_start = yesterday.replace(day=1)

    our_mtd = fetch_our_estimate(start_day=month_start, end_day=yesterday, tz_name=tz_name)
    our_yday = fetch_our_estimate(start_day=yesterday, end_day=yesterday, tz_name=tz_name)

    openai = fetch_openai_costs(start_day=month_start, yesterday=yesterday)
    openai_usage = fetch_openai_usage_tokens(start_day=month_start)
    google = fetch_google_costs(start_day=month_start, yesterday=yesterday, tz_name=tz_name)
    deepl = fetch_deepl_usage()
    r2 = fetch_cloudflare_r2(start_day=month_start)
    railway = fetch_railway_usage(start_day=month_start)

    lines: list[str] = []
    lines.append("📊 Эталон провайдеров (факт vs наш расчёт)")
    lines.append(f"Вчера: {yesterday.isoformat()} · с начала месяца · TZ {tz_name}")
    lines.append("")

    # ---- OpenAI ----
    # Three honest numbers, since our list-price ledger can't equal the real invoice while
    # OpenAI's free daily token grants are active (see /openai_audit): the real bill (Costs
    # API), the list-price ceiling if grants stopped (gross Usage × list price), and our
    # internal ledger estimate (which also under-logs part of the calls).
    lines.append("▪️ OpenAI")
    if not openai.get("configured"):
        lines.append("  не настроено (нет OPENAI_ADMIN_KEY)")
    elif openai.get("error"):
        lines.append(f"  ошибка API: {openai['error']}")
    else:
        lines.append(f"  реальный счёт (что платишь): месяц {_fmt_usd(openai.get('mtd_usd'))} · вчера {_fmt_usd(openai.get('yday_usd'))}")
        ceiling = openai_list_price_ceiling(openai_usage)
        if ceiling is not None:
            lines.append(f"  потолок по прайсу (если гранты OpenAI кончатся): ~{_fmt_usd(ceiling)}/мес")
        lines.append(f"  наш ledger (внутр. оценка, логирует часть вызовов): {_fmt_usd(_our_group_total(our_mtd, 'openai'))}")
        top = list((openai.get("by_line_item") or {}).items())[:5]
        if top:
            lines.append("  по статьям (месяц): " + ", ".join(f"{k} {_fmt_usd(v)}" for k, v in top))
    lines.append("")

    # ---- Google ----
    lines.append("▪️ Google Cloud (TTS + Translate + YouTube)")
    if not google.get("configured"):
        lines.append("  не настроено (нет GOOGLE_BILLING_BQ_TABLE / BigQuery export)")
    elif google.get("error"):
        lines.append(f"  ошибка BigQuery: {google['error']}")
    else:
        lines.append(_delta_line(label="вчера", truth=google.get("yday_usd"),
                                 ours=_our_group_total(our_yday, "google")))
        lines.append(_delta_line(label="месяц", truth=google.get("mtd_usd"),
                                 ours=_our_group_total(our_mtd, "google")))
        for svc, vals in list((google.get("by_service") or {}).items())[:6]:
            lines.append(f"    · {svc}: месяц {_fmt_usd(vals.get('mtd'))} (вчера {_fmt_usd(vals.get('yday'))})")
    lines.append("")

    # ---- DeepL ----
    lines.append("▪️ DeepL (остаток бесплатного лимита)")
    if not deepl.get("configured"):
        lines.append("  не настроено (нет DEEPL_AUTH_KEY)")
    elif deepl.get("error"):
        lines.append(f"  ошибка API: {deepl['error']}")
    else:
        used = int(deepl.get("used") or 0)
        limit = int(deepl.get("limit") or 0)
        remaining = max(0, limit - used)
        pct = (used / limit * 100.0) if limit else 0.0
        lines.append(f"  использовано {used:,} / {limit:,} символов ({pct:.0f}%), осталось {remaining:,}")
    lines.append("")

    # ---- Cloudflare R2 ----
    lines.append("▪️ Cloudflare R2 (хранилище + операции)")
    if not r2.get("configured"):
        lines.append("  не настроено (нет CLOUDFLARE_API_TOKEN / R2_ACCOUNT_ID)")
    elif r2.get("error"):
        lines.append(f"  ошибка API: {r2['error']}")
    else:
        gb = float(r2.get("gb") or 0.0)
        a = int(r2.get("class_a") or 0)
        b = int(r2.get("class_b") or 0)
        lines.append(f"  хранилище {gb:.2f} / 10 GB · объектов {int(r2.get('object_count') or 0):,}")
        lines.append(f"  Class A {a:,} / 1,000,000 · Class B {b:,} / 10,000,000 (месяц)")
        lines.append(f"  оценка сверх free: {_fmt_usd(r2.get('est_cost'))}")
    lines.append("")

    # ---- Railway ----
    lines.append("▪️ Railway (инфраструктура, прогноз на конец месяца ≈)")
    if not railway.get("configured"):
        lines.append("  не настроено (нет RAILWAY_API_TOKEN)")
    elif railway.get("error"):
        lines.append(f"  ошибка API: {railway['error']}")
    else:
        lines.append(f"  ≈ {_fmt_usd(railway.get('total_usd'))} (по прайсу Railway; точное — в Railway → Usage)")
        bm = railway.get("by_measure") or {}
        if bm:
            lines.append("  по ресурсам: " + ", ".join(f"{k} {_fmt_usd(v)}" for k, v in bm.items()))
    lines.append("")

    # ---- Perplexity (no truth API) ----
    ppx_yday = _our_group_total(our_yday, "perplexity")
    ppx_mtd = _our_group_total(our_mtd, "perplexity")
    if ppx_mtd > 0.005 or ppx_yday > 0.005:
        lines.append("▪️ Perplexity (только наш расчёт — у них нет usage API)")
        lines.append(f"  вчера {_fmt_usd(ppx_yday)} · месяц {_fmt_usd(ppx_mtd)}")
        lines.append("")

    lines.append("⚠️ = расхождение факт/наш > 10% — наш счётчик мог что-то не учесть.")
    return "\n".join(lines).rstrip()


# --------------------------------------------------------------------------- #
# OpenAI overcount AUDIT (read-only): tokens first, then prices, then $
# --------------------------------------------------------------------------- #
# Real OpenAI list prices per 1M tokens (mid-2026), reference for the ⚠️ flag.
_OPENAI_REAL_PRICE_PER_1M: dict[str, dict[str, float]] = {
    "gpt-4.1": {"input": 2.00, "output": 8.00, "cached": 0.50},
    "gpt-4.1-mini": {"input": 0.40, "output": 1.60, "cached": 0.10},
    "gpt-4.1-nano": {"input": 0.10, "output": 0.40, "cached": 0.025},
    "gpt-4o": {"input": 2.50, "output": 10.00, "cached": 1.25},
    "gpt-4o-mini": {"input": 0.15, "output": 0.60, "cached": 0.075},
}
_MODEL_DATE_RE = re.compile(r"-\d{4}-\d{2}-\d{2}$")


def _ref_price_for_sku(sku: str) -> float | None:
    """Map a ledger price SKU (e.g. 'gpt-4.1-2025-04-14_output') to the real
    OpenAI per-1M price, so we can flag an inflated snapshot."""
    s = str(sku or "")
    for suffix, side in (("_input", "input"), ("_output", "output"), ("_cached", "cached")):
        if s.endswith(suffix):
            model = _MODEL_DATE_RE.sub("", s[: -len(suffix)])
            return (_OPENAI_REAL_PRICE_PER_1M.get(model) or {}).get(side)
    return None


def _model_family(model: str) -> str:
    """Strip the -YYYY-MM-DD date suffix so 'gpt-4.1-2025-04-14' == 'gpt-4.1'."""
    return _MODEL_DATE_RE.sub("", str(model or "").strip())


def _sku_to_model_family(sku: str) -> str:
    s = str(sku or "")
    for suffix in ("_input", "_output", "_cached"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
            break
    return _model_family(s)


def openai_list_price_ceiling(usage: dict[str, Any]) -> float | None:
    """What OpenAI's GROSS usage (from the Usage API) would cost at full list price —
    i.e. the bill if the free daily token grants / caching went away. Cached input is
    still priced at the cached rate (that discount is not a grant). Returns None if
    usage is unavailable. Models without a known list price are skipped."""
    if not usage.get("configured") or usage.get("error"):
        return None
    total = 0.0
    for model, v in (usage.get("by_model") or {}).items():
        price = _OPENAI_REAL_PRICE_PER_1M.get(_model_family(model))
        if not price:
            continue
        inp = int(v.get("input") or 0)
        cached = int(v.get("cached") or 0)
        out = int(v.get("output") or 0)
        uncached = max(0, inp - cached)
        total += (
            uncached / 1_000_000.0 * price["input"]
            + cached / 1_000_000.0 * price.get("cached", price["input"])
            + out / 1_000_000.0 * price["output"]
        )
    return total


def fetch_openai_usage_tokens(*, start_day: date) -> dict[str, Any]:
    """Token QUANTITIES (input/cached/output) from OpenAI's Usage API, grouped by
    model — the ground truth to check our token counting against. Read-only."""
    key = (os.getenv("OPENAI_ADMIN_KEY") or "").strip()
    if not key:
        return {"configured": False}
    start_ts = int(datetime(start_day.year, start_day.month, start_day.day, tzinfo=timezone.utc).timestamp())
    try:
        input_t = cached_t = output_t = 0
        requests_n = 0
        by_model: dict[str, dict[str, int]] = {}
        # Usage API caps limit at 31 for bucket_width=1d — one bucket per day covers a month.
        params: dict[str, Any] = {"start_time": start_ts, "bucket_width": "1d", "limit": 31, "group_by": "model"}
        page = None
        for _ in range(12):
            if page:
                params["page"] = page
            resp = requests.get(
                "https://api.openai.com/v1/organization/usage/completions",
                headers={"Authorization": f"Bearer {key}"},
                params=params,
                timeout=_HTTP_TIMEOUT,
            )
            if resp.status_code >= 400:
                return {"configured": True, "error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
            body = resp.json() if resp.content else {}
            for bucket in body.get("data") or []:
                for item in bucket.get("results") or []:
                    it = int(item.get("input_tokens") or 0)
                    ct = int(item.get("input_cached_tokens") or 0)
                    ot = int(item.get("output_tokens") or 0)
                    rq = int(item.get("num_model_requests") or 0)
                    input_t += it
                    cached_t += ct
                    output_t += ot
                    requests_n += rq
                    model = str(item.get("model") or "—")
                    slot = by_model.setdefault(model, {"input": 0, "cached": 0, "output": 0, "requests": 0})
                    slot["input"] += it
                    slot["cached"] += ct
                    slot["output"] += ot
                    slot["requests"] += rq
            if body.get("has_more") and body.get("next_page"):
                page = body.get("next_page")
                continue
            break
        return {
            "configured": True,
            "input": input_t,
            "cached": cached_t,
            "output": output_t,
            "requests": requests_n,
            "by_model": dict(sorted(by_model.items(), key=lambda kv: kv[1]["input"] + kv[1]["output"], reverse=True)),
        }
    except Exception as exc:  # noqa: BLE001
        logging.warning("OpenAI usage tokens fetch failed: %s", exc, exc_info=True)
        return {"configured": True, "error": str(exc)}


def fetch_our_openai_ledger(*, start_day: date, end_day: date, tz_name: str) -> dict[str, Any]:
    """Our recorded OpenAI usage from bt_3_billing_events, joined to the price
    snapshot to recover the model/SKU. Read-only: totals per units_type + per SKU."""
    by_units: dict[str, dict[str, float]] = {}
    by_sku: dict[str, dict[str, Any]] = {}
    by_model: dict[str, dict[str, float]] = {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT e.units_type,
                       COALESCE(s.sku, '(no price snapshot)') AS sku,
                       SUM(e.units_value) AS units,
                       SUM(e.cost_amount) AS cost
                FROM bt_3_billing_events e
                LEFT JOIN bt_3_billing_price_snapshots s ON s.id = e.price_snapshot_id
                WHERE e.provider = 'openai'
                  AND (e.event_time AT TIME ZONE %s)::date BETWEEN %s AND %s
                GROUP BY e.units_type, COALESCE(s.sku, '(no price snapshot)');
                """,
                (tz_name, start_day, end_day),
            )
            for units_type, sku, units, cost in cursor.fetchall() or []:
                ut = str(units_type or "")
                u = float(units or 0.0)
                c = float(cost or 0.0)
                slot = by_units.setdefault(ut, {"units": 0.0, "cost": 0.0})
                slot["units"] += u
                slot["cost"] += c
                by_sku[f"{sku} · {ut}"] = {"units": u, "cost": c}
                fam = _sku_to_model_family(sku)
                if fam and ut in ("tokens_in", "tokens_out"):
                    fm = by_model.setdefault(fam, {"tokens_in": 0.0, "tokens_out": 0.0})
                    fm[ut] += u
    return {"by_units": by_units, "by_sku": by_sku, "by_model": by_model}


def fetch_openai_price_snapshots() -> dict[str, float]:
    """Current effective per-unit OpenAI prices from the snapshot table (latest
    valid_from per sku+unit). Key = 'sku|unit', value = price_per_unit. Read-only."""
    prices: dict[str, float] = {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (sku, unit) sku, unit, price_per_unit
                FROM bt_3_billing_price_snapshots
                WHERE provider = 'openai'
                ORDER BY sku, unit, valid_from DESC;
                """
            )
            for sku, unit, ppu in cursor.fetchall() or []:
                prices[f"{sku}|{unit}"] = float(ppu or 0.0)
    return prices


def _token_ratio_note(ours: int, ref: int) -> str:
    if ref <= 0:
        return "эталон 0" if ours <= 0 else "эталон 0 / у нас >0 ⚠️"
    d = (ours - ref) / ref * 100.0
    return f"Δ {d:+.0f}%{' ⚠️' if abs(d) > 10 else ' ✅'}"


def build_openai_audit_text(*, target_day: date | None = None, tz_name: str | None = None) -> str:
    tz_name = tz_name or _tz_name()
    yesterday = target_day or (datetime.now(timezone.utc).date() - timedelta(days=1))
    month_start = yesterday.replace(day=1)

    usage = fetch_openai_usage_tokens(start_day=month_start)
    ledger = fetch_our_openai_ledger(start_day=month_start, end_day=yesterday, tz_name=tz_name)
    snaps = fetch_openai_price_snapshots()

    lines: list[str] = []
    lines.append("🔎 OpenAI аудит: сначала ТОКЕНЫ, потом цены, потом $")
    lines.append(f"Месяц {month_start.isoformat()} → {yesterday.isoformat()} · TZ {tz_name}")
    lines.append("")

    # 1) TOKENS — do our counts match OpenAI's? (isolates counting vs pricing)
    lines.append("1️⃣ ТОКЕНЫ (эталон OpenAI Usage vs наш ledger)")
    if not usage.get("configured"):
        lines.append("  нет OPENAI_ADMIN_KEY")
    elif usage.get("error"):
        lines.append(f"  ошибка Usage API: {usage['error']}")
    else:
        oa_in = int(usage.get("input") or 0)
        oa_cached = int(usage.get("cached") or 0)
        oa_out = int(usage.get("output") or 0)
        oa_req = int(usage.get("requests") or 0)
        our_in = int(ledger["by_units"].get("tokens_in", {}).get("units", 0))
        our_out = int(ledger["by_units"].get("tokens_out", {}).get("units", 0))
        our_req = int(ledger["by_units"].get("requests", {}).get("units", 0))
        cached_pct = (oa_cached / oa_in * 100.0) if oa_in else 0.0
        lines.append(f"  вход:    OpenAI {oa_in:,} (кэш {oa_cached:,} = {cached_pct:.0f}%) | наш {our_in:,} | {_token_ratio_note(our_in, oa_in)}")
        lines.append(f"  выход:   OpenAI {oa_out:,} | наш {our_out:,} | {_token_ratio_note(our_out, oa_out)}")
        lines.append(f"  запросы: OpenAI {oa_req:,} | наш {our_req:,} | {_token_ratio_note(our_req, oa_req)}")
        lines.append("")
        # Per-model: does OpenAI's GROSS gpt-4.1 usage match what WE attribute to gpt-4.1?
        # This settles it — the aggregate mixes cheap models we barely log.
        lines.append("  по моделям (OpenAI gross vs наш ledger), вход/выход:")
        oa_fam: dict[str, dict[str, int]] = {}
        for model, v in (usage.get("by_model") or {}).items():
            f = _model_family(model)
            acc = oa_fam.setdefault(f, {"input": 0, "output": 0})
            acc["input"] += int(v.get("input") or 0)
            acc["output"] += int(v.get("output") or 0)
        our_fam = ledger.get("by_model") or {}
        fams = sorted(set(oa_fam) | set(our_fam), key=lambda f: oa_fam.get(f, {}).get("input", 0), reverse=True)
        for f in fams[:8]:
            oi = int(oa_fam.get(f, {}).get("input", 0))
            oo = int(oa_fam.get(f, {}).get("output", 0))
            ui = int(our_fam.get(f, {}).get("tokens_in", 0))
            uo = int(our_fam.get(f, {}).get("tokens_out", 0))
            lines.append(f"    {f}: вход OA {oi:,}/наш {ui:,} {_token_ratio_note(ui, oi)} · выход OA {oo:,}/наш {uo:,} {_token_ratio_note(uo, oo)}")
    lines.append("")

    # 2) PRICES — our snapshot vs the real OpenAI list price
    lines.append("2️⃣ ЦЕНЫ снапшота в БД vs реальные OpenAI")
    if not snaps:
        lines.append("  снапшотов OpenAI нет (цена=0 → это был бы недосчёт, а не пере-)")
    else:
        for key in sorted(snaps):
            sku, unit = key.split("|", 1)
            ppu = snaps[key]
            if unit in ("tokens_in", "tokens_out"):
                per_1m = ppu * 1_000_000.0
                ref = _ref_price_for_sku(sku)
                if ref and ref > 0:
                    r = per_1m / ref
                    flag = f"⚠️ ×{r:.1f}" if (r >= 1.2 or r <= 0.8) else "✅"
                    lines.append(f"  {sku}: ${per_1m:,.2f}/1M vs реально ${ref:.2f} {flag}")
                else:
                    lines.append(f"  {sku}: ${per_1m:,.2f}/1M (нет эталона)")
            else:
                lines.append(f"  {sku} [{unit}]: ${ppu:,.4f}/ед")
    lines.append("")

    # 3) MONEY — our ledger $ by SKU (compare to /provider_costs OpenAI truth $2.09)
    lines.append("3️⃣ ДЕНЬГИ (наш ledger по SKU)")
    total = sum(v["cost"] for v in ledger["by_units"].values())
    for k, v in sorted(ledger["by_sku"].items(), key=lambda it: it[1]["cost"], reverse=True)[:10]:
        lines.append(f"  {k}: {int(v['units']):,} ед → {_fmt_usd(v['cost'])}")
    lines.append(f"  ИТОГО наш OpenAI: {_fmt_usd(total)} (эталон OpenAI Costs ≈ см. /provider_costs)")
    return "\n".join(lines).rstrip()


# --------------------------------------------------------------------------- #
# delivery
# --------------------------------------------------------------------------- #
def _split_telegram_text(text: str, limit: int = 3800) -> list[str]:
    parts: list[str] = []
    buf = ""
    for line in str(text or "").splitlines():
        candidate = f"{buf}\n{line}" if buf else line
        if len(candidate) > limit and buf:
            parts.append(buf)
            buf = line
        else:
            buf = candidate
    if buf:
        parts.append(buf)
    return parts or [""]


def send_provider_cost_truth_report(*, target_day: date | None = None, force: bool = False) -> dict[str, Any]:
    """Build and DM the provider-truth report to all admins. Guarded so a restart
    doesn't double-send the same day (unless force=True)."""
    tz_name = _tz_name()
    day = target_day or (datetime.now(timezone.utc).date() - timedelta(days=1))
    run_period = day.isoformat()
    if not force and not claim_scheduler_run_guard(
        job_key=JOB_KEY,
        run_period=run_period,
        target_scope="global",
        metadata={"tz": tz_name},
    ):
        return {"ok": True, "skipped": True, "reason": "already_claimed", "day": run_period}
    try:
        token = os.getenv("TELEGRAM_Deutsch_BOT_TOKEN")
        admin_ids = sorted(int(a) for a in (get_admin_telegram_ids() or []) if int(a) > 0)
        if not token or not admin_ids:
            if not force:
                finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                           target_scope="global", status="failed",
                                           metadata={"error": "no_token_or_admins"})
            return {"ok": False, "sent": 0, "error": "no_token_or_admins"}
        text = build_provider_cost_truth_text(target_day=day, tz_name=tz_name)
        sent = 0
        for uid in admin_ids:
            for part in _split_telegram_text(text):
                resp = requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": uid, "text": part, "disable_web_page_preview": True},
                    timeout=_HTTP_TIMEOUT,
                )
                if resp.status_code >= 400:
                    logging.warning("provider cost truth DM failed uid=%s: %s", uid, resp.text[:200])
            sent += 1
        if not force:
            finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                       target_scope="global", status="completed",
                                       metadata={"sent": sent})
        return {"ok": True, "sent": sent, "day": run_period}
    except Exception as exc:
        if not force:
            finish_scheduler_run_guard(job_key=JOB_KEY, run_period=run_period,
                                       target_scope="global", status="failed",
                                       metadata={"error": str(exc)})
        logging.exception("provider cost truth report failed")
        raise
