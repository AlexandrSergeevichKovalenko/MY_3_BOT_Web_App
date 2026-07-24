"""External dead-man's-switch liveness ping (Healthchecks.io or any cron-monitor).

Gated behind HEALTHCHECK_PING_URL — fully inert until that env var is set, so shipping this
changes nothing until you opt in. While THIS process is alive it GETs the ping URL on an
interval; if the process crashes, hangs, or fails to deploy, the pings stop and the external
monitor alerts you (email / push / Telegram) after its grace period.

Why external: this is the ONE failure our own bot-based alerts cannot report — a dead bot
cannot DM you. The DB-pool saturation DM covers "running hot"; this covers "not running".

Reusable across services: pass a distinct service_name and (optionally) a per-service URL via
HEALTHCHECK_PING_URL_<SERVICE> so bot / backend_web can each have their own check.
"""
import logging
import os
import threading
import time

import requests

_STARTED: set[str] = set()
_LOCK = threading.Lock()


def _resolve_ping_url(service_name: str) -> str:
    # Per-service override wins (HEALTHCHECK_PING_URL_BOT), else the shared URL.
    specific = (os.getenv(f"HEALTHCHECK_PING_URL_{service_name.upper()}") or "").strip()
    if specific:
        return specific
    return (os.getenv("HEALTHCHECK_PING_URL") or "").strip()


def start_healthcheck_pinger(service_name: str = "bot") -> bool:
    """Start the background liveness pinger for this process. No-op (returns False) when no
    ping URL is configured or when already started for this service_name. Best-effort: ping
    failures are swallowed (a transient network blip must not look like the service is down —
    that is the monitor's own grace period's job)."""
    url = _resolve_ping_url(service_name)
    if not url:
        return False
    with _LOCK:
        if service_name in _STARTED:
            return False
        _STARTED.add(service_name)
    interval = max(15, int(os.getenv("HEALTHCHECK_PING_INTERVAL_SEC", "60") or "60"))

    def _loop() -> None:
        while True:
            try:
                requests.get(url, timeout=10)
            except Exception:
                logging.debug("healthcheck ping failed service=%s", service_name, exc_info=True)
            time.sleep(interval)

    threading.Thread(target=_loop, daemon=True, name=f"healthcheck-ping-{service_name}").start()
    logging.info("Healthcheck pinger started service=%s interval=%ss", service_name, interval)
    return True
