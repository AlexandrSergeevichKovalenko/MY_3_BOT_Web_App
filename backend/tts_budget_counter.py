"""O(1) per-(provider, month) character counter for the TTS budget guard.

Why this exists
---------------
The budget guard used to compute "chars used this month" by SUM-ing the whole
``bt_3_billing_events`` table on every single synth. Two problems:

1. **Slow at scale** — an aggregate over a growing table on each cache-miss synth
   (the user's concern: "нельзя перед каждой операцией считать по всей таблице").
2. **Blind** — many synth paths (background prewarm, game pools, numdict/artikel,
   world-news, morning-mistakes audio) never wrote a billing row, so the SUM saw
   only a fraction of real usage. The WaveNet 1M/mo free-tier alert therefore
   never fired even as Google started billing — the reported incident.

This counter is INCR'd **once per synth** at the single enforce gate and read with
a single GET, so the guard finally reflects real Google usage and its threshold
alerts fire honestly. On a cold Redis miss it *seeds* from the caller-supplied
ledger SUM so a restart / Redis flush never drops the guard below what history
already knows. Everything is fail-open: if Redis is unavailable we fall back to the
SUM the caller passes in, i.e. exactly the old behavior — never worse.

The durable ledger (``bt_3_billing_events``) is untouched and still drives the
economics report; this is purely the fast enforcement counter.
"""

from __future__ import annotations

import logging
from datetime import date, datetime

# Redis key namespace. One key per provider bucket per calendar month.
_KEY = "tts_budget:used:{provider}:{month}"
# Keep a month's key comfortably (never expire mid-month); a little slack for tz.
_TTL_SECONDS = 45 * 24 * 3600


def _redis():
    try:
        from backend.job_queue import get_redis_client

        return get_redis_client()
    except Exception:
        return None


def _month_token(period_month) -> str:
    """Stable 'YYYY-MM' token from the guard's month-start value.

    ``period_month`` is the month-start the budget layer already computed (in its
    own tz), so the counter's month boundary matches the DB control row exactly.
    """
    if isinstance(period_month, datetime):
        period_month = period_month.date()
    if isinstance(period_month, date):
        return period_month.strftime("%Y-%m")
    token = str(period_month or "").strip()
    return token[:7] if token else "unknown"


def get_used(provider: str, period_month, *, fallback: float | int | None = None) -> float:
    """Chars used this month for ``provider``.

    On a cold key, seed from ``fallback`` (the ledger SUM) so we never under-read
    right after a restart. Fail-open: on any Redis problem, return ``fallback``
    (the previous SUM-based behavior).
    """
    provider_value = str(provider or "").strip().lower()
    fallback_value = float(fallback or 0.0)
    client = _redis()
    if client is None:
        return fallback_value
    key = _KEY.format(provider=provider_value, month=_month_token(period_month))
    try:
        raw = client.get(key)
        if raw is not None:
            return float(int(raw))
        # Cold key → seed from the ledger SUM so the guard starts where history is.
        # SET NX so a racing synth's INCR isn't clobbered by a late seed.
        seed = int(max(0.0, fallback_value))
        client.set(key, seed, nx=True, ex=_TTL_SECONDS)
        raw = client.get(key)
        return float(int(raw)) if raw is not None else float(seed)
    except Exception:
        logging.warning("tts_budget_counter.get_used failed provider=%s", provider_value, exc_info=True)
        return fallback_value


def add(provider: str, period_month, chars: int) -> None:
    """Add ``chars`` synthesized characters to the month counter (one INCRBY).

    Fail-open: a Redis hiccup just means this synth isn't counted (the ledger SUM
    fallback still bounds us on the next cold read).
    """
    added = max(0, int(chars or 0))
    if added <= 0:
        return
    provider_value = str(provider or "").strip().lower()
    client = _redis()
    if client is None:
        return
    key = _KEY.format(provider=provider_value, month=_month_token(period_month))
    try:
        client.incrby(key, added)
        client.expire(key, _TTL_SECONDS)
    except Exception:
        logging.warning("tts_budget_counter.add failed provider=%s", provider_value, exc_info=True)
