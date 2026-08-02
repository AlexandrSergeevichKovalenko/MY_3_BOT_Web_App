"""Fire-and-forget billing log for OpenAI calls that bypass the SDK-wrapper loggers.

Several call sites hit OpenAI via raw ``requests.post`` to api.openai.com (or via the
SDK) without going through ``_billing_log_openai_usage`` (web tier) or
``_log_openai_usage_event`` (bot tier), so their tokens never reached
``bt_3_billing_events`` — the ledger saw only ~half of real usage. This helper closes
that gap for any such site with a single call.

Design (mirrors the existing bot-tier logger exactly, so behaviour and scaling stay
consistent):
  * records a ``requests`` row plus ``tokens_in`` / ``tokens_out`` rows;
  * prices via the ``{model}_input`` / ``{model}_output`` snapshot SKUs (cost is
    computed by ``log_billing_event`` when a snapshot exists, else tokens are still
    recorded with no cost — attribution never lost);
  * runs in a daemon thread so it NEVER blocks or slows the caller's hot path;
  * swallows every error, so instrumenting a path can't break functionality.

It adds accounting only — it does not change any business logic.
"""
from __future__ import annotations

import logging
import threading
import uuid
from typing import Any


def _usage_field(usage: Any, *names: str) -> int:
    """Read the first present token field from a usage dict OR an SDK usage object.
    Accepts both chat-completions (prompt_tokens/completion_tokens) and responses
    (input_tokens/output_tokens) naming."""
    for name in names:
        value = usage.get(name) if isinstance(usage, dict) else getattr(usage, name, None)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
    return 0


def cached_input_tokens(usage: Any) -> int:
    """How many of the input tokens OpenAI served from its prompt cache.

    Any prompt whose prefix is at least ~1024 tokens is cached automatically, and the
    cached part is billed at a quarter of the input price. Our nightly dictionary job
    sends the SAME ~2900-token instruction 200 times in a row, so 92% of its input is
    cached (measured 02.08.2026) — pricing all of it at the full rate overstated that
    job by half. The number is reported by the provider in three different shapes
    depending on the API, so read all of them.

    Returns 0 when the provider said nothing — never guesses."""
    if usage is None:
        return 0
    direct = _usage_field(usage, "cached_prompt_tokens", "cached_tokens")
    if direct > 0:
        return direct
    for holder in ("prompt_tokens_details", "input_tokens_details"):
        details = usage.get(holder) if isinstance(usage, dict) else getattr(usage, holder, None)
        if details is None:
            continue
        value = _usage_field(details, "cached_tokens")
        if value > 0:
            return value
    return 0


def split_input_tokens(usage: Any, tokens_in: int) -> tuple[int, int]:
    """Разложить входные токены на «из кеша» и «свежие» — платим за них по РАЗНОЙ цене.
    Кеш никогда не может быть больше самого входа: провайдер иногда округляет."""
    cached = max(0, min(int(tokens_in), cached_input_tokens(usage)))
    return cached, max(0, int(tokens_in) - cached)


def log_openai_raw_usage(
    *,
    action_type: str,
    model: str,
    usage: Any,
    user_id: int | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    metadata: dict | None = None,
) -> None:
    """Record one OpenAI call (requests + tokens_in/out) in the billing ledger.
    Non-blocking and never raises. ``usage`` may be the raw HTTP response's ``usage``
    dict or an SDK usage object; pass ``None`` safely (nothing is logged)."""
    try:
        tn = str(action_type or "llm").strip() or "llm"
        mdl = str(model or "unknown").strip() or "unknown"
        uid = int(user_id) if user_id is not None else None
        tokens_in = max(0, _usage_field(usage, "input_tokens", "prompt_tokens"))
        tokens_out = max(0, _usage_field(usage, "output_tokens", "completion_tokens"))
        meta = {"model": mdl, "tier": "raw"}
        if isinstance(metadata, dict):
            meta.update(metadata)

        def _worker() -> None:
            try:
                from backend.database import log_billing_event
                seed = uuid.uuid4().hex
                log_billing_event(
                    idempotency_key=f"rawreq:{seed}", user_id=uid, action_type=tn,
                    provider="openai", units_type="requests", units_value=1.0,
                    source_lang=source_lang, target_lang=target_lang,
                    status="estimated", metadata=meta,
                )
                # Кешированный вход стоит вчетверо дешевле обычного, поэтому идёт
                # ОТДЕЛЬНОЙ строкой со своим SKU. Тип единиц у обеих строк один
                # (`tokens_in`), чтобы отчёты, считающие количество токенов, не изменились.
                cached_in, fresh_in = split_input_tokens(usage, tokens_in)
                if fresh_in > 0:
                    log_billing_event(
                        idempotency_key=f"rawtin:{seed}", user_id=uid, action_type=tn,
                        provider="openai", units_type="tokens_in", units_value=float(fresh_in),
                        price_provider="openai", price_sku=f"{mdl}_input", price_unit="tokens_in",
                        source_lang=source_lang, target_lang=target_lang,
                        status="estimated", metadata={**meta, "input_kind": "fresh"},
                    )
                if cached_in > 0:
                    log_billing_event(
                        idempotency_key=f"rawtinc:{seed}", user_id=uid, action_type=tn,
                        provider="openai", units_type="tokens_in", units_value=float(cached_in),
                        price_provider="openai", price_sku=f"{mdl}_cached", price_unit="tokens_in",
                        source_lang=source_lang, target_lang=target_lang,
                        status="estimated", metadata={**meta, "input_kind": "cached"},
                    )
                if tokens_out > 0:
                    log_billing_event(
                        idempotency_key=f"rawtout:{seed}", user_id=uid, action_type=tn,
                        provider="openai", units_type="tokens_out", units_value=float(tokens_out),
                        price_provider="openai", price_sku=f"{mdl}_output", price_unit="tokens_out",
                        source_lang=source_lang, target_lang=target_lang,
                        status="estimated", metadata=meta,
                    )
            except Exception:
                logging.debug("raw openai usage logging failed action=%s", tn, exc_info=True)

        threading.Thread(target=_worker, daemon=True).start()
    except Exception:
        pass
