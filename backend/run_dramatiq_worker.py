import logging
import os
import signal
import sys
import time

import dramatiq
from dramatiq import Worker

from backend.job_queue import get_dramatiq_broker

# Import actors so their queues are declared on the broker before the worker starts.
import backend.background_jobs  # noqa: F401


def _parse_queue_set(raw_value: str | None) -> set[str] | None:
    raw = str(raw_value or "").strip()
    if not raw:
        return None
    values = {part.strip() for part in raw.split(",") if part.strip()}
    return values or None


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
    worker.start()
    try:
        while not should_stop["value"]:
            time.sleep(1.0)
    finally:
        worker.stop(timeout=600000)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
