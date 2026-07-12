"""Phase-1 cost control: the app-wide WEEKLY spend ceiling (€) evaluator + gate.

Self-contained on purpose (kept out of the hot database.py / backend_server.py files):
- The weekly spend is NOT counted per billing_event. Instead it is summed from
  bt_3_billing_events into EUR and cached in Redis with a short TTL, refreshed at most
  once per `_SPEND_CACHE_TTL_SEC`. One cheap indexed SUM every couple of minutes — no
  per-request DB load (a single user cannot burn €10 inside one refresh window).
- Enforcement reads a per-tier Redis boolean flag (sub-ms GET). "heavy" is paused first;
  cheap "core" keeps running. Fail-OPEN: if Redis is unreachable we do NOT block (the
  per-provider monthly budgets in provider_budget_controls remain the hard backstop).
- `evaluate_ceiling()` is pure-ish: it computes status, dedupes soft alerts via the DB
  row's notified_thresholds, and records the hard-state timestamps. It does NOT send
  Telegram DMs and does NOT flip the block flag itself — that wiring lives in the bot
  layer / scheduler tick and is added in a later increment (shadow-first rollout).

Nothing here runs until the scheduler tick + DM dispatch are wired. Importing this module
has no side effects.
"""
from __future__ import annotations

import json
import logging
import os
import time as _time
from datetime import datetime, timedelta, timezone, time as dt_time

from backend.database import (
    TRIAL_POLICY_TZ,
    _resolve_timezone,
    _iso_week_key,
    convert_cost_to_eur,
    get_db_connection_context,
    get_or_create_app_spend_ceiling,
    mark_app_spend_ceiling_threshold_notified,
    set_app_spend_ceiling_hard_state,
    set_app_spend_ceiling_blocked_tiers,
    add_app_spend_ceiling_extra,
    append_app_spend_ceiling_decision,
)

logger = logging.getLogger(__name__)

# Cost tiers. "heavy" = expensive/generative (reader audio, image gen, multi-call LLM,
# Perplexity) — paused first at the ceiling. "core" = cheap/one-shot (quick translate,
# dictionary, single GPT, short TTS) — kept running so basic UX survives.
TIER_HEAVY = "heavy"
TIER_CORE = "core"
KNOWN_TIERS = (TIER_HEAVY, TIER_CORE)

# Soft reminders (alert only) then the hard stop. Tunable via env.
def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return default


SOFT_THRESHOLDS = (80, 95)
HARD_THRESHOLD = 100
GRACE_HOURS = _env_float("APP_SPEND_CEILING_GRACE_HOURS", 2.0)
# Night window (Europe/Vienna): a hard breach here auto-stops immediately (admin asleep).
NIGHT_START = dt_time(0, 0)
NIGHT_END = dt_time(6, 30)

# The weekly € ceiling must reflect ACTUAL out-of-pocket money, not list price.
# billing_events records the notional list-price cost of every unit (e.g. Google TTS at
# ~$16/1M chars) even while we are inside a provider's FREE tier. Providers whose monthly
# per-provider budget keeps them within their free allowance (google_tts 1M, google_translate
# 500k, deepl 500k, azure 2M, R2 10GB) cost us ~€0 in reality, and Stripe fees are
# revenue-linked (not a controllable API spend) — so they are EXCLUDED from the ceiling sum.
# Only genuinely pay-from-first-unit providers (openai, perplexity, …) count.
_DEFAULT_EXCLUDED_PROVIDERS = {
    "google_tts", "google_translate", "deepl_free", "deepl",
    "azure_translator", "cloudflare_r2_class_a", "cloudflare_r2_class_b",
    "cloudflare_r2_storage", "youtube_api", "stripe",
}


def _excluded_providers() -> set[str]:
    raw = (os.getenv("APP_SPEND_CEILING_EXCLUDE_PROVIDERS", "") or "").strip()
    if raw:
        return {p.strip().lower() for p in raw.split(",") if p.strip()}
    return set(_DEFAULT_EXCLUDED_PROVIDERS)


_SPEND_CACHE_TTL_SEC = int(_env_float("APP_SPEND_CACHE_TTL_SEC", 90))
_REDIS_SPEND_KEY = "spend:app:week:{week}"          # JSON {"eur": float, "ts": epoch}
_REDIS_BLOCK_KEY = "spend:app:blocked:{tier}"        # "1" when that tier is paused


# ── Redis (lazy, fail-safe) ──────────────────────────────────────────────────
def _redis():
    try:
        from backend.job_queue import get_redis_client
        return get_redis_client()
    except Exception:
        return None


# ── Week boundaries (ISO week, Europe/Vienna → UTC range) ─────────────────────
def _week_bounds_utc(now: datetime | None = None, tz: str = TRIAL_POLICY_TZ) -> tuple[datetime, datetime, str]:
    """Return (start_utc, end_utc, week_key) for the ISO week (Mon–Sun) containing `now`."""
    tzinfo = _resolve_timezone(tz)
    local_now = (now.astimezone(tzinfo) if isinstance(now, datetime) else datetime.now(timezone.utc).astimezone(tzinfo))
    monday_date = local_now.date() - timedelta(days=local_now.isoweekday() - 1)
    start_local = datetime.combine(monday_date, dt_time.min, tzinfo=tzinfo)
    end_local = start_local + timedelta(days=7)
    week_key = _iso_week_key(local_now, tz=tz)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc), week_key


def _is_night(now: datetime | None = None, tz: str = TRIAL_POLICY_TZ) -> bool:
    tzinfo = _resolve_timezone(tz)
    local_now = (now.astimezone(tzinfo) if isinstance(now, datetime) else datetime.now(timezone.utc).astimezone(tzinfo))
    t = local_now.timetz().replace(tzinfo=None)
    return NIGHT_START <= t < NIGHT_END


# ── Weekly spend (Postgres SUM → EUR, Redis-cached) ──────────────────────────
def _rebuild_week_spend_from_db(start_utc: datetime, end_utc: datetime) -> tuple[float, float]:
    """One indexed SUM over bt_3_billing_events for the week → (real_eur, free_eur).
    real = pay-from-first-unit providers (drives the ceiling / the stop);
    free = notional list-price value of usage still inside providers' FREE tiers
    (shown for visibility, NEVER enforced)."""
    excluded = _excluded_providers()
    real_eur = 0.0
    free_eur = 0.0
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT provider, currency, COALESCE(SUM(cost_amount), 0)
                FROM bt_3_billing_events
                WHERE event_time >= %s AND event_time < %s
                GROUP BY provider, currency;
                """,
                (start_utc, end_utc),
            )
            rows = cursor.fetchall() or []
    for provider, currency, amount in rows:
        try:
            eur = float(convert_cost_to_eur(float(amount or 0.0), currency) or 0.0)
        except Exception:
            continue
        if str(provider or "").strip().lower() in excluded:
            free_eur += eur
        else:
            real_eur += eur
    return round(real_eur, 6), round(free_eur, 6)


def get_week_spend_breakdown(now: datetime | None = None, *, max_age_sec: int | None = None, force: bool = False) -> tuple[float, float]:
    """(real_eur, free_eur) for the current week — Redis-cached ~TTL, DB fallback."""
    start_utc, end_utc, week_key = _week_bounds_utc(now)
    ttl = _SPEND_CACHE_TTL_SEC if max_age_sec is None else int(max_age_sec)
    client = _redis()
    key = _REDIS_SPEND_KEY.format(week=week_key)
    if client is not None and not force:
        try:
            raw = client.get(key)
            if raw:
                payload = json.loads(raw)
                if (_time.time() - float(payload.get("ts", 0))) < ttl:
                    return float(payload.get("eur", 0.0)), float(payload.get("free", 0.0))
        except Exception:
            pass
    real, free = _rebuild_week_spend_from_db(start_utc, end_utc)
    if client is not None:
        try:
            client.set(key, json.dumps({"eur": real, "free": free, "ts": _time.time()}), ex=max(ttl * 4, 3600))
        except Exception:
            pass
    return real, free


def get_week_spend_eur(now: datetime | None = None, *, max_age_sec: int | None = None, force: bool = False) -> float:
    """Weekly REAL out-of-pocket spend in EUR (this is what drives the ceiling / stop)."""
    real, _free = get_week_spend_breakdown(now, max_age_sec=max_age_sec, force=force)
    return real


# ── Tier gate (sub-ms Redis flag; fail-open) ─────────────────────────────────
def is_tier_blocked(tier: str) -> bool:
    client = _redis()
    if client is None:
        return False  # fail-open: never break UX on a Redis blip
    try:
        return bool(client.get(_REDIS_BLOCK_KEY.format(tier=str(tier).strip().lower())))
    except Exception:
        return False


def set_tier_blocked(tiers, *, reason: str | None = None, week: str | None = None) -> list[str]:
    """Pause the given tiers (e.g. ['heavy']) — Redis flag + persisted on the week row.
    Pass [] to resume everything."""
    wanted = sorted({str(t).strip().lower() for t in (tiers or []) if str(t).strip()})
    client = _redis()
    if client is not None:
        for tier in KNOWN_TIERS:
            k = _REDIS_BLOCK_KEY.format(tier=tier)
            try:
                if tier in wanted:
                    client.set(k, "1")
                else:
                    client.delete(k)
            except Exception:
                pass
    try:
        set_app_spend_ceiling_blocked_tiers(tiers=wanted, week=week)
    except Exception:
        logger.debug("set_app_spend_ceiling_blocked_tiers failed", exc_info=True)
    return wanted


# ── Evaluation (compute status + record; does NOT DM or block) ───────────────
def evaluate_ceiling(now: datetime | None = None) -> dict:
    """Compute weekly spend vs the ceiling and return a decision dict for the caller
    (bot layer) to act on. Side effects are limited to recording soft-alert dedup and
    the hard-state timestamps in bt_3_app_spend_ceiling — never DMs, never the block flag.

    Decision keys:
      week, spent_eur, limit_eur, pct, new_soft (list[int]), hard (bool),
      should_block_now (bool: hard breach during the night window), grace_deadline (iso|None).
    """
    ceiling = get_or_create_app_spend_ceiling()
    if not ceiling:
        return {"error": "no_ceiling_row"}
    week_key = str(ceiling.get("period_week") or "")
    limit_eur = float(ceiling.get("effective_limit_eur") or 0.0)
    spent_eur, free_eur = get_week_spend_breakdown(now if isinstance(now, datetime) else None)
    pct = (spent_eur / limit_eur * 100.0) if limit_eur > 0 else 0.0

    notified = ceiling.get("notified_thresholds") or {}
    new_soft: list[int] = []
    for th in SOFT_THRESHOLDS:
        if pct >= th and str(th) not in notified:
            new_soft.append(th)

    hard = pct >= HARD_THRESHOLD
    should_block_now = False
    grace_deadline_iso = None
    now_dt = now if isinstance(now, datetime) else datetime.now(timezone.utc)

    if hard:
        already_hard = bool(ceiling.get("hard_reached_at"))
        if not already_hard:
            if _is_night(now_dt):
                should_block_now = True  # night → immediate stop (caller flips the flag)
                try:
                    set_app_spend_ceiling_hard_state(hard_reached_at=now_dt, auto_stop_at=now_dt, week=week_key)
                except Exception:
                    logger.debug("set hard-state (night) failed", exc_info=True)
            else:
                deadline = now_dt + timedelta(hours=GRACE_HOURS)
                grace_deadline_iso = deadline.isoformat()
                try:
                    set_app_spend_ceiling_hard_state(hard_reached_at=now_dt, auto_stop_at=deadline, week=week_key)
                except Exception:
                    logger.debug("set hard-state (grace) failed", exc_info=True)
        else:
            existing = ceiling.get("auto_stop_at")
            grace_deadline_iso = existing

    for th in new_soft:
        try:
            mark_app_spend_ceiling_threshold_notified(threshold_percent=th, week=week_key)
        except Exception:
            logger.debug("mark threshold notified failed th=%s", th, exc_info=True)
    if hard and str(HARD_THRESHOLD) not in notified:
        try:
            mark_app_spend_ceiling_threshold_notified(threshold_percent=HARD_THRESHOLD, week=week_key)
        except Exception:
            logger.debug("mark hard threshold notified failed", exc_info=True)

    return {
        "week": week_key,
        "spent_eur": round(spent_eur, 4),
        "free_eur": round(free_eur, 4),
        "limit_eur": round(limit_eur, 4),
        "pct": round(pct, 1),
        "new_soft": new_soft,
        "hard": hard,
        "hard_newly": hard and not bool(ceiling.get("hard_reached_at")),
        "should_block_now": should_block_now,
        "grace_deadline": grace_deadline_iso,
        "blocked_tiers": ceiling.get("blocked_tiers") or [],
    }


# ── Presentation + admin actions (pure; the bot layer calls these) ───────────
TOP_UP_OPTIONS = (2, 5, 10)


def _fmt_eur(x) -> str:
    try:
        return f"€{float(x or 0):.2f}"
    except Exception:
        return "€0.00"


def build_appcap_keyboard():
    """Inline keyboard for the ceiling DM: +€2/€5/€10 · ⛔ stop · 📊 why."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    rows = [[InlineKeyboardButton(f"➕ €{n}", callback_data=f"appcap:add:{n}") for n in TOP_UP_OPTIONS]]
    rows.append([InlineKeyboardButton("⛔ Остановить сейчас", callback_data="appcap:stop")])
    rows.append([InlineKeyboardButton("📊 Разобраться в причине", callback_data="appcap:why")])
    return InlineKeyboardMarkup(rows)


def _free_line(decision: dict) -> str:
    free = float(decision.get("free_eur", 0.0) or 0.0)
    if free <= 0:
        return ""
    return f"\n🆓 Бесплатные ресурсы (Google/DeepL/Azure/R2): ~{_fmt_eur(free)} — <i>в потолок не идут</i>."


def format_soft_alert(decision: dict) -> str:
    spent = decision.get("spent_eur", 0.0)
    limit = decision.get("limit_eur", 0.0)
    pct = decision.get("pct", 0.0)
    remaining = max(0.0, float(limit) - float(spent))
    return (
        f"💸 <b>Реальные затраты: {_fmt_eur(spent)} из {_fmt_eur(limit)}</b> "
        f"({pct:.0f}%) за неделю {decision.get('week')}.\n"
        f"Осталось {_fmt_eur(remaining)}.{_free_line(decision)}\n"
        f"Можно добавить бюджет на эту неделю или посмотреть, куда ушло."
    )


def format_hard_alert(decision: dict) -> str:
    spent = decision.get("spent_eur", 0.0)
    limit = decision.get("limit_eur", 0.0)
    if decision.get("should_block_now"):
        when = "🌙 Ночь — тяжёлые функции остановлены сразу (аудио-книга, картинки, тяжёлый LLM). Дешёвое ядро работает."
    else:
        when = (
            f"⏳ Если не отвечу, тяжёлые функции авто-остановятся в {decision.get('grace_deadline') or '~2 ч'} "
            "(дешёвое ядро продолжит работать)."
        )
    return (
        f"⛔ <b>Достигнут недельный потолок РЕАЛЬНЫХ затрат: {_fmt_eur(spent)} / {_fmt_eur(limit)}</b> "
        f"(неделя {decision.get('week')}).{_free_line(decision)}\n{when}\n\n"
        "Добавить бюджет на эту неделю, остановить сейчас или разобраться в причине:"
    )


def analyze_week_cause(now: datetime | None = None) -> str:
    """Read-only weekly breakdown for the 📊 button: € by provider + top activities."""
    start_utc, end_utc, week_key = _week_bounds_utc(now if isinstance(now, datetime) else None)
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT provider, action_type, currency, COALESCE(SUM(cost_amount), 0)
                    FROM bt_3_billing_events
                    WHERE event_time >= %s AND event_time < %s
                    GROUP BY provider, action_type, currency;
                    """,
                    (start_utc, end_utc),
                )
                rows = cursor.fetchall() or []
    except Exception:
        logger.debug("analyze_week_cause query failed", exc_info=True)
        return "Не удалось собрать разбор затрат."
    excluded = _excluded_providers()
    counted_total = 0.0
    free_total = 0.0
    counted_prov: dict[str, float] = {}
    free_prov: dict[str, float] = {}
    action_eur: dict[str, float] = {}  # counted (real-money) activities only
    for provider, action_type, currency, amount in rows:
        try:
            eur = float(convert_cost_to_eur(float(amount or 0.0), currency) or 0.0)
        except Exception:
            eur = 0.0
        if eur <= 0:
            continue
        pname = str(provider or "—")
        if pname.strip().lower() in excluded:
            free_total += eur
            free_prov[pname] = free_prov.get(pname, 0.0) + eur
        else:
            counted_total += eur
            counted_prov[pname] = counted_prov.get(pname, 0.0) + eur
            aname = str(action_type or "—")
            action_eur[aname] = action_eur.get(aname, 0.0) + eur
    if counted_total <= 0 and free_total <= 0:
        return f"📊 Неделя {week_key}: затрат пока не зафиксировано."
    lines = [
        f"📊 <b>Разбор недели {week_key}</b>",
        f"💶 В потолок (реальные деньги): <b>{_fmt_eur(counted_total)}</b>",
    ]
    if counted_prov:
        lines += ["", "<b>Платные провайдеры:</b>"]
        lines += [f"• {n} — {_fmt_eur(v)}" for n, v in sorted(counted_prov.items(), key=lambda kv: kv[1], reverse=True)[:6]]
    if action_eur:
        lines += ["", "<b>Топ платных активностей:</b>"]
        lines += [f"• {n} — {_fmt_eur(v)}" for n, v in sorted(action_eur.items(), key=lambda kv: kv[1], reverse=True)[:5]]
    if free_prov:
        lines += ["", f"🆓 <b>Бесплатный тариф (НЕ в потолок): {_fmt_eur(free_total)}</b>"]
        lines += [f"• {n} — {_fmt_eur(v)}" for n, v in sorted(free_prov.items(), key=lambda kv: kv[1], reverse=True)[:6]]
    return "\n".join(lines)


def _reset_week_alerts(week_key: str) -> None:
    """Re-arm soft/hard alerts + clear the pending auto-stop after a top-up, so the next
    approach to the RAISED ceiling alerts again. Own-module UPDATE (avoids editing database.py)."""
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE bt_3_app_spend_ceiling "
                    "SET notified_thresholds = '{}'::jsonb, hard_reached_at = NULL, "
                    "    auto_stop_at = NULL, updated_at = NOW() "
                    "WHERE period_week = %s;",
                    (str(week_key),),
                )
            conn.commit()
    except Exception:
        logger.debug("_reset_week_alerts failed week=%s", week_key, exc_info=True)


def apply_appcap_action(action: str, *, amount=None, admin_id=None) -> dict:
    """Apply an admin button press. Returns {ok, text}. The bot handler is a thin wrapper."""
    act = str(action or "").strip().lower()
    _, _, week_key = _week_bounds_utc()
    if act == "add":
        try:
            add_eur = float(amount or 0)
        except Exception:
            add_eur = 0.0
        if add_eur <= 0:
            return {"ok": False, "text": "Некорректная сумма добора."}
        row = add_app_spend_ceiling_extra(add_eur=add_eur, week=week_key, metadata={"by": admin_id})
        _reset_week_alerts(week_key)
        set_tier_blocked([], week=week_key)  # resume everything
        try:
            append_app_spend_ceiling_decision(decision={"action": "add", "eur": add_eur, "by": admin_id}, week=week_key)
        except Exception:
            pass
        new_limit = row.get("effective_limit_eur") if row else None
        return {"ok": True, "text": f"✅ Бюджет недели поднят до {_fmt_eur(new_limit)}. Функции возобновлены."}
    if act == "stop":
        set_tier_blocked([TIER_HEAVY], reason="admin_manual_stop", week=week_key)
        try:
            append_app_spend_ceiling_decision(decision={"action": "stop", "by": admin_id}, week=week_key)
        except Exception:
            pass
        return {"ok": True, "text": "⛔ Тяжёлые функции остановлены. Дешёвое ядро (перевод, словарь) продолжает работать."}
    if act == "why":
        return {"ok": True, "text": analyze_week_cause()}
    return {"ok": False, "text": "Неизвестное действие."}
