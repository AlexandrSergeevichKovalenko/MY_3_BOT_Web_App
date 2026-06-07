"""Synthetic load dispatcher (Phase 2) — local, disposable, production-safe.

Feeds synthetic Telegram updates into a PTB Application backed by a STUB bot
(no Telegram API) and a self-contained handler that performs real DB work, so a
local run exercises the actual infrastructure pressure (DB pool, PgBouncer hold
time, event loop, to_thread executor) WITHOUT touching production or any paid
external API.

Hard safety contract (see assert_safe_to_dispatch):
  * SYNTHETIC_LOAD_MODE must be 1
  * the active Postgres host must be local/synthetic (never a known prod host)
  * the Redis host must be local/synthetic
  * only a stub bot is used — never a real Telegram token / network call
  * OpenAI / YouTube / TTS are faked by the synthetic-provider layer

Run only against the local docker-compose.synthetic.yml stack. The CLI refuses
to dispatch unless --execute is passed AND all guards pass.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Callable, Optional
from urllib.parse import urlparse

from backend import load_metrics
from backend.synthetic_load import is_synthetic_load_mode
from scripts.synthetic_load_runner import generate_updates


# --------------------------------------------------------------------------- #
# Safety guards
# --------------------------------------------------------------------------- #

# Known production hosts (explicit) + production-like suffixes.
PRODUCTION_HOSTS = {"centerbeam.proxy.rlwy.net", "kodama.proxy.rlwy.net"}
PRODUCTION_HOST_SUFFIXES = (".proxy.rlwy.net", ".railway.internal", ".up.railway.app", ".rlwy.net")
# Hosts considered safe local/synthetic targets.
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0", "postgres", "pgbouncer", "redis"}


class SyntheticSafetyError(RuntimeError):
    """Raised when the environment is not safe for synthetic load dispatch."""


def _host_from_url(url: str | None) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        return (urlparse(raw).hostname or "").strip().lower()
    except Exception:
        return ""


def _is_production_host(host: str) -> bool:
    h = (host or "").strip().lower()
    if not h:
        return False
    if h in PRODUCTION_HOSTS:
        return True
    return any(h.endswith(suffix) for suffix in PRODUCTION_HOST_SUFFIXES)


def _is_local_or_synthetic_host(host: str) -> bool:
    h = (host or "").strip().lower()
    if not h:
        return False
    if h in LOCAL_HOSTS:
        return True
    if h.endswith(".local") or "synthetic" in h:
        return True
    allowed = (os.getenv("SYNTHETIC_ALLOWED_HOST") or "").strip().lower()
    return bool(allowed) and h == allowed


def _active_db_host() -> str:
    # Prefer the host the app actually resolved (reflects DATABASE_URL selection).
    try:
        from backend.database import pgbouncer_rollout_status
        host = str(pgbouncer_rollout_status().get("active_endpoint_host") or "").strip().lower()
        if host:
            return host
    except Exception:
        pass
    for var in ("DATABASE_URL_PGBOUNCER_RAILWAY", "DATABASE_URL_PGBOUNCER",
                "DATABASE_URL_RAILWAY", "DATABASE_URL_DIRECT_RAILWAY", "DATABASE_URL"):
        host = _host_from_url(os.getenv(var))
        if host:
            return host
    return ""


def _active_redis_host() -> str:
    for var in ("REDIS_URL", "RAILWAY_REDIS_URL", "UPSTASH_REDIS_URL"):
        host = _host_from_url(os.getenv(var))
        if host:
            return host
    return ""


def assert_safe_to_dispatch() -> dict[str, Any]:
    """Raise SyntheticSafetyError unless it is safe to dispatch synthetic load."""
    if not is_synthetic_load_mode():
        raise SyntheticSafetyError("Refusing to dispatch: SYNTHETIC_LOAD_MODE is not enabled (set =1).")

    db_host = _active_db_host()
    if _is_production_host(db_host):
        raise SyntheticSafetyError(f"Refusing to dispatch: DB host '{db_host}' looks like PRODUCTION.")
    if not _is_local_or_synthetic_host(db_host):
        raise SyntheticSafetyError(
            f"Refusing to dispatch: DB host '{db_host or '<none>'}' is not local/synthetic. "
            "Point DATABASE_URL at the local stack or set SYNTHETIC_ALLOWED_HOST.")

    redis_host = _active_redis_host()
    if redis_host:  # redis optional for DB-only flows, but if set it must be safe
        if _is_production_host(redis_host):
            raise SyntheticSafetyError(f"Refusing to dispatch: Redis host '{redis_host}' looks like PRODUCTION.")
        if not _is_local_or_synthetic_host(redis_host):
            raise SyntheticSafetyError(
                f"Refusing to dispatch: Redis host '{redis_host}' is not local/synthetic.")

    return {"db_host": db_host, "redis_host": redis_host or "<unset>", "synthetic_mode": True}


# --------------------------------------------------------------------------- #
# Stub bot (no Telegram network)
# --------------------------------------------------------------------------- #

def build_stub_bot():
    """Return a PTB ExtBot subclass whose senders are local no-ops + metric
    counters. Never performs any Telegram network call."""
    from telegram import User
    from telegram.ext import ExtBot

    class _StubExtBot(ExtBot):  # pragma: no cover - exercised only in a real run
        def __init__(self):
            super().__init__(token="123456:SYNTHETIC-DO-NOT-USE")
            self._synthetic_me = User(id=1, is_bot=True, first_name="synthetic_bot", username="synthetic_bot")

        async def initialize(self):
            # Skip the real get_me() network call.
            self._initialized = True

        async def shutdown(self):
            self._initialized = False

        async def get_me(self, *args, **kwargs):
            return self._synthetic_me

        async def send_message(self, *args, **kwargs):
            load_metrics.incr_telegram("send")
            return SimpleNamespace(message_id=0)

        async def edit_message_text(self, *args, **kwargs):
            load_metrics.incr_telegram("edit")
            return SimpleNamespace(message_id=0)

        async def answer_callback_query(self, *args, **kwargs):
            load_metrics.incr_telegram("callback_answer")
            return True

        async def send_chat_action(self, *args, **kwargs):
            return True

    return _StubExtBot()


# --------------------------------------------------------------------------- #
# Self-contained synthetic handler (real DB work, no bot_3 dependency)
# --------------------------------------------------------------------------- #

async def _synthetic_db_handler(update, context):
    """A safe, representative handler: access-check (cache) + a cheap real DB
    round-trip (exercises the pool / PgBouncer / to_thread executor) + a stub
    bot reply. No OpenAI/TTS/YouTube, no writes to user data."""
    user = getattr(update, "effective_user", None)
    uid = int(getattr(user, "id", 0) or 0)
    with load_metrics.handler_latency("synthetic_db_flow"):
        try:
            from backend.database import is_telegram_user_allowed, get_db_connection_context

            def _db_probe() -> int:
                # access check (cache hit after seeding) + one cheap pooled query
                allowed = is_telegram_user_allowed(uid)
                with get_db_connection_context() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1;")
                        cur.fetchone()
                return 1 if allowed else 0

            await asyncio.to_thread(_db_probe)
            bot = getattr(context, "bot", None)
            if bot is not None:
                if getattr(update, "callback_query", None) is not None:
                    await bot.answer_callback_query(callback_query_id="synthetic")
                else:
                    await bot.send_message(chat_id=uid, text="synthetic-ack")
        except Exception:
            load_metrics.record_handler_latency("synthetic_db_flow_error", 0.0)
            raise


# --------------------------------------------------------------------------- #
# Application + update construction
# --------------------------------------------------------------------------- #

def build_synthetic_application(extra_handlers: Optional[list] = None):
    """Build a real PTB Application backed by the stub bot, registering the
    self-contained synthetic handler (and any injected extra (handler, group))."""
    from telegram.ext import Application, MessageHandler, CallbackQueryHandler, filters

    application = Application.builder().bot(build_stub_bot()).build()
    application.add_handler(MessageHandler(filters.ALL, _synthetic_db_handler), group=0)
    application.add_handler(CallbackQueryHandler(_synthetic_db_handler), group=0)
    for handler, group in (extra_handlers or []):
        application.add_handler(handler, group=group)
    return application


def build_update(payload: dict, bot) -> Any:
    """Construct a real telegram.Update from a synthetic payload dict."""
    from telegram import Update, Message, CallbackQuery, User, Chat

    now = datetime.now(timezone.utc)
    raw_user = payload.get("message", {}).get("from") or payload.get("callback_query", {}).get("from") or {}
    user = User(id=int(raw_user.get("id", 0)), is_bot=False, first_name=raw_user.get("first_name", "synthetic"))
    chat = Chat(id=int(raw_user.get("id", 0)), type="private")

    if "callback_query" in payload:
        cq = payload["callback_query"]
        msg = Message(message_id=int(cq.get("message", {}).get("message_id", 1)), date=now, chat=chat)
        callback = CallbackQuery(id=str(cq.get("id", "cb")), from_user=user,
                                 chat_instance="synthetic", data=str(cq.get("data", "")), message=msg)
        update = Update(update_id=int(payload["update_id"]), callback_query=callback)
    else:
        m = payload["message"]
        msg = Message(message_id=int(m.get("message_id", 1)), date=now, chat=chat,
                      from_user=user, text=str(m.get("text", "")))
        update = Update(update_id=int(payload["update_id"]), message=msg)
    update._bot = bot  # so update.effective_* and replies resolve
    return update


def seed_allowed_users(user_ids: list[int]) -> int:
    """Mark synthetic users allowed via the in-memory allow-list cache only
    (no DB writes), so enforce_user_access-style checks pass."""
    from backend.database import _allowed_user_cache_put
    n = 0
    for uid in user_ids:
        _allowed_user_cache_put(int(uid), True)
        n += 1
    return n


# --------------------------------------------------------------------------- #
# Dispatch loop
# --------------------------------------------------------------------------- #

async def run_dispatch(
    *,
    users: int,
    rate: float,
    duration: float,
    application: Any | None = None,
    enforce_guards: bool = True,
) -> dict[str, Any]:
    """Dispatch synthetic updates at `rate`/s for `duration`s into `application`.

    `application` must expose `async process_update(update)`. If None, a real
    synthetic Application is built. Tests inject a recorder. Guards run first
    unless enforce_guards=False (tests)."""
    safety = assert_safe_to_dispatch() if enforce_guards else {"guards": "skipped"}
    load_metrics.reset()

    payloads = list(generate_updates(users, rate, duration))
    user_ids = sorted({int(p.get("message", {}).get("from", {}).get("id")
                            or p.get("callback_query", {}).get("from", {}).get("id") or 0)
                       for p in payloads})
    seeded = seed_allowed_users(user_ids)

    own_app = application is None
    if own_app:
        application = build_synthetic_application()
        await application.initialize()
    bot = getattr(application, "bot", None)

    lag_task = asyncio.create_task(load_metrics.event_loop_lag_sampler(interval_sec=0.2))
    interval = 1.0 / rate if rate > 0 else 0.0
    generated = dispatched = errors = 0
    started = time.perf_counter()
    try:
        for payload in payloads:
            generated += 1
            try:
                update = build_update(payload, bot) if bot is not None else payload
                await application.process_update(update)
                dispatched += 1
            except Exception:
                errors += 1
            if interval:
                await asyncio.sleep(interval)
    finally:
        lag_task.cancel()
        if own_app:
            try:
                await application.shutdown()
            except Exception:
                pass

    try:
        from backend.database import pgbouncer_rollout_status
        pool = {k: pgbouncer_rollout_status().get(k)
                for k in ("db_pool_minconn", "db_pool_maxconn", "db_connection_target")}
    except Exception:
        pool = {}
    queues = load_metrics.sample_queue_depths(["scheduler_jobs", "tts_generation", "translation_check_dedicated"])

    return {
        "safety": safety,
        "users": users, "rate": rate, "duration": duration,
        "seeded_allowed_users": seeded,
        "generated": generated, "dispatched": dispatched, "errors": errors,
        "elapsed_sec": round(time.perf_counter() - started, 3),
        "metrics": load_metrics.snapshot(),
        "db_pool": pool,
        "queue_depths": queues,
    }


# --------------------------------------------------------------------------- #
# Scenario + CLI
# --------------------------------------------------------------------------- #

async def scenario_safe_db_flows() -> dict[str, Any]:
    """First scenario: 100 users, 60s, 5 upd/s, DB-only/callback flows."""
    return await run_dispatch(users=100, rate=5.0, duration=60.0)


def _print_report(result: dict[str, Any]) -> None:
    import json
    print(json.dumps(result, indent=2, default=str))


def main() -> int:
    parser = argparse.ArgumentParser(description="Synthetic load dispatcher (local, safe)")
    parser.add_argument("--users", type=int, default=100)
    parser.add_argument("--rate", type=float, default=5.0)
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--execute", action="store_true",
                        help="actually dispatch load (otherwise safety check only)")
    args = parser.parse_args()

    if not args.execute:
        try:
            safety = assert_safe_to_dispatch()
            print("Safety check PASSED (dry-run). Re-run with --execute to dispatch.")
            print(safety)
            return 0
        except SyntheticSafetyError as exc:
            print(f"Safety check FAILED: {exc}")
            return 1

    try:
        assert_safe_to_dispatch()
    except SyntheticSafetyError as exc:
        print(f"ABORT: {exc}")
        return 1
    result = asyncio.run(run_dispatch(users=args.users, rate=args.rate, duration=args.duration))
    _print_report(result)
    return 0 if result["errors"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
