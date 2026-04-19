import logging
import os
import signal
import sys
import threading
import time

import dramatiq
from dramatiq import Worker

from backend.job_queue import (
    claim_translation_check_watchdog_requeue_cooldown,
    enqueue_translation_check_job,
    get_translation_check_dispatch_state,
    get_dramatiq_broker,
)

# Import actors so their queues are declared on the broker before the worker starts.
import backend.background_jobs  # noqa: F401


_TRANSLATION_CHECK_QUEUE_NAME = str(
    os.getenv("TRANSLATION_CHECK_QUEUE_NAME") or "translation_check"
).strip() or "translation_check"
_TRANSLATION_CHECK_WATCHDOG_ENABLED = str(
    os.getenv("TRANSLATION_CHECK_WATCHDOG_ENABLED") or "1"
).strip().lower() in {"1", "true", "yes", "on"}
_TRANSLATION_CHECK_WATCHDOG_INTERVAL_SEC = max(
    2,
    int((os.getenv("TRANSLATION_CHECK_WATCHDOG_INTERVAL_SEC") or "5").strip() or "5"),
)
_TRANSLATION_CHECK_WATCHDOG_QUEUED_STALE_MS = max(
    30000,
    int((os.getenv("TRANSLATION_CHECK_WATCHDOG_QUEUED_STALE_MS") or "60000").strip() or "60000"),
)
_TRANSLATION_CHECK_WATCHDOG_BATCH_LIMIT = max(
    1,
    int((os.getenv("TRANSLATION_CHECK_WATCHDOG_BATCH_LIMIT") or "20").strip() or "20"),
)


def _parse_queue_set(raw_value: str | None) -> set[str] | None:
    raw = str(raw_value or "").strip()
    if not raw:
        return None
    values = {part.strip() for part in raw.split(",") if part.strip()}
    return values or None


def _translation_check_queue_is_subscribed(queue_names: set[str] | None) -> bool:
    if not queue_names:
        return True
    return _TRANSLATION_CHECK_QUEUE_NAME in queue_names


def _run_translation_check_watchdog(*, should_stop: dict[str, bool], worker_threads: int) -> None:
    from backend.database import (
        count_active_translation_check_running_sessions,
        list_stale_translation_check_queued_sessions,
    )

    logging.info(
        "Starting translation-check watchdog: queue=%s interval_sec=%s queued_stale_ms=%s batch_limit=%s worker_threads=%s",
        _TRANSLATION_CHECK_QUEUE_NAME,
        _TRANSLATION_CHECK_WATCHDOG_INTERVAL_SEC,
        _TRANSLATION_CHECK_WATCHDOG_QUEUED_STALE_MS,
        _TRANSLATION_CHECK_WATCHDOG_BATCH_LIMIT,
        worker_threads,
    )
    while not should_stop["value"]:
        try:
            running_sessions = count_active_translation_check_running_sessions()
            if running_sessions >= worker_threads:
                logging.info(
                    "translation_check_dispatch watchdog_skip_backlog queue=%s running_sessions=%s worker_threads=%s queued_stale_ms=%s",
                    _TRANSLATION_CHECK_QUEUE_NAME,
                    running_sessions,
                    worker_threads,
                    _TRANSLATION_CHECK_WATCHDOG_QUEUED_STALE_MS,
                )
                stale_sessions = []
            else:
                stale_sessions = list_stale_translation_check_queued_sessions(
                    stale_ms=_TRANSLATION_CHECK_WATCHDOG_QUEUED_STALE_MS,
                    limit=_TRANSLATION_CHECK_WATCHDOG_BATCH_LIMIT,
                )
            for session in stale_sessions:
                try:
                    session_id = int(session.get("id") or 0)
                except Exception:
                    continue
                if session_id <= 0:
                    continue
                dispatch_state = get_translation_check_dispatch_state(session_id) or {}
                if dispatch_state.get("worker_received_at_ms"):
                    logging.info(
                        "translation_check_dispatch watchdog_skip_received queue=%s session_id=%s dispatch_generation=%s redispatch_count=%s",
                        _TRANSLATION_CHECK_QUEUE_NAME,
                        session_id,
                        dispatch_state.get("dispatch_generation"),
                        dispatch_state.get("redispatch_count"),
                    )
                    continue
                if not claim_translation_check_watchdog_requeue_cooldown(session_id):
                    continue
                dispatched_job_id = str(session.get("dispatched_job_id") or "").strip() or None
                logging.warning(
                    "translation_check_dispatch watchdog_reenqueue queue=%s session_id=%s dispatched_job_id=%s stale_ms=%s running_sessions=%s worker_threads=%s dispatch_generation=%s redispatch_count=%s",
                    _TRANSLATION_CHECK_QUEUE_NAME,
                    session_id,
                    dispatched_job_id,
                    _TRANSLATION_CHECK_WATCHDOG_QUEUED_STALE_MS,
                    running_sessions,
                    worker_threads,
                    dispatch_state.get("dispatch_generation"),
                    dispatch_state.get("redispatch_count"),
                )
                enqueue_translation_check_job(
                    session_id,
                    correlation_id=f"translation_check_watchdog_{session_id}",
                    request_id=f"watchdog_{session_id}",
                    force_dispatch=True,
                )
        except Exception:
            logging.exception("translation-check watchdog scan failed")
        for _ in range(_TRANSLATION_CHECK_WATCHDOG_INTERVAL_SEC):
            if should_stop["value"]:
                break
            time.sleep(1.0)


def main() -> int:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
    broker = get_dramatiq_broker()
    dramatiq.set_broker(broker)

    queue_names = _parse_queue_set(os.getenv("DRAMATIQ_QUEUES"))
    worker_threads = max(
        1,
        int((os.getenv("DRAMATIQ_WORKER_THREADS") or os.getenv("BACKGROUND_JOBS_THREADS") or "8").strip() or "8"),
    )
    worker_timeout = max(
        100,
        int((os.getenv("DRAMATIQ_WORKER_TIMEOUT_MS") or "1000").strip() or "1000"),
    )
    translation_check_queue_subscribed = _translation_check_queue_is_subscribed(queue_names)
    logging.info(
        "Dedicated dramatiq worker queue configuration: configured_queues=%s translation_check_queue=%s subscribed=%s",
        sorted(queue_names) if queue_names else "ALL",
        _TRANSLATION_CHECK_QUEUE_NAME,
        translation_check_queue_subscribed,
    )
    if not translation_check_queue_subscribed:
        logging.error(
            "Dedicated dramatiq worker is not subscribed to required translation-check queue: configured_queues=%s required_queue=%s",
            sorted(queue_names) if queue_names else [],
            _TRANSLATION_CHECK_QUEUE_NAME,
        )
        return 2

    worker = Worker(
        broker,
        queues=queue_names,
        worker_threads=worker_threads,
        worker_timeout=worker_timeout,
    )

    should_stop = {"value": False}

    def _handle_stop(signum, _frame) -> None:
        logging.info("Dramatiq dedicated worker stopping on signal=%s", signum)
        should_stop["value"] = True

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    logging.info(
        "Starting dedicated dramatiq worker: queues=%s worker_threads=%s worker_timeout_ms=%s",
        sorted(queue_names) if queue_names else "ALL",
        worker_threads,
        worker_timeout,
    )
    if _TRANSLATION_CHECK_WATCHDOG_ENABLED and translation_check_queue_subscribed:
        threading.Thread(
            target=_run_translation_check_watchdog,
            kwargs={"should_stop": should_stop, "worker_threads": worker_threads},
            daemon=True,
        ).start()
    worker.start()
    try:
        while not should_stop["value"]:
            time.sleep(1.0)
    finally:
        worker.stop(timeout=600000)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
