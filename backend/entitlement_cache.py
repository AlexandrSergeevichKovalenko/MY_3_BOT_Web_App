"""Short-lived, Redis-backed cache of resolved user entitlements (plan/mode + daily cap).

`resolve_entitlement()` costs ~2 uncached Postgres reads (subscription + earned-Pro grant)
and runs on many requests. A user's plan doesn't change second-to-second, so we cache the
resolved answer per user for `ENTITLEMENT_CACHE_TTL_SEC` and BUST the key on the events that
actually change a plan (Stripe webhook write, DAU Pro grant, Stripe-customer bind). Stored in
Redis so an invalidation from the web service is seen by the bot + workers instantly.

Fail-safe: if Redis is unavailable, `get()` returns None and `set()`/`invalidate()` are
no-ops, so callers transparently fall back to the live DB path. Importing has no side effects.
"""
from __future__ import annotations

import json
import logging
import os

logger = logging.getLogger(__name__)

_KEY = "ent:{uid}"


def _ttl_seconds() -> int:
    try:
        return max(30, int(os.getenv("ENTITLEMENT_CACHE_TTL_SEC", "600") or 600))
    except Exception:
        return 600


def _redis():
    try:
        from backend.job_queue import get_redis_client
        return get_redis_client()
    except Exception:
        return None


def get(user_id: int) -> dict | None:
    """Cached entitlement dict for the user, or None on miss / Redis down."""
    client = _redis()
    if client is None:
        return None
    try:
        raw = client.get(_KEY.format(uid=int(user_id)))
        if raw:
            data = json.loads(raw)
            if isinstance(data, dict):
                return data
    except Exception:
        logger.debug("entitlement cache get failed uid=%s", user_id, exc_info=True)
    return None


def set(user_id: int, entitlement: dict) -> None:
    """Cache the resolved entitlement with the configured TTL (best-effort)."""
    if not isinstance(entitlement, dict):
        return
    client = _redis()
    if client is None:
        return
    try:
        client.set(_KEY.format(uid=int(user_id)), json.dumps(entitlement), ex=_ttl_seconds())
    except Exception:
        logger.debug("entitlement cache set failed uid=%s", user_id, exc_info=True)


def invalidate(user_id: int) -> None:
    """Drop the user's cached entitlement — call on any plan-change event so the next
    request re-resolves against the DB immediately."""
    client = _redis()
    if client is None:
        return
    try:
        client.delete(_KEY.format(uid=int(user_id)))
    except Exception:
        logger.debug("entitlement cache invalidate failed uid=%s", user_id, exc_info=True)
