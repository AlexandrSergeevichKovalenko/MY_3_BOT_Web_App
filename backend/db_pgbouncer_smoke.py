"""Read-only PgBouncer transaction-pool smoke test.

Verifies that the database path the app relies on behaves correctly through the
active connection target (direct or PgBouncer transaction pooling) WITHOUT
touching any user data. Every check uses only ``SELECT``, transaction control
(``COMMIT`` / ``ROLLBACK``), transaction-scoped ``SET LOCAL``, ``SAVEPOINT``,
and ``pg_advisory_xact_lock`` on a dedicated namespaced key.

Hard guarantees:
  * No ``INSERT`` / ``UPDATE`` / ``DELETE`` / ``CREATE`` / ``DROP`` / ``TRUNCATE``.
  * No references to application (``bt_3_*``) tables.
  * No credentials or full DSNs in the result (only sanitized rollout status).

The logic is parametrized on a ``connection_context_factory`` so it can be
exercised in tests against a recording fake connection, and run for real via
``scripts/pgbouncer_smoke_test.py`` (or any admin-only caller).
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any, Callable


# Dedicated, namespaced advisory-lock key so the smoke test never collides with
# a real per-session / per-user / per-schema advisory lock used elsewhere.
_SMOKE_ADVISORY_LOCK_MATERIAL = "pgbouncer_smoke_test:advisory_lock_probe"
SMOKE_ADVISORY_LOCK_KEY = int.from_bytes(
    hashlib.sha256(_SMOKE_ADVISORY_LOCK_MATERIAL.encode("utf-8")).digest()[:8],
    "big",
    signed=True,
)


class _SmokeRollback(Exception):
    """Sentinel raised to force a transaction rollback inside a check."""


def _default_connection_context_factory() -> Callable[[], Any]:
    # Imported lazily so this module stays importable (and testable) without a
    # live database / full backend import at module load time.
    from backend.database import get_db_connection_context

    return get_db_connection_context


def _run_check(name: str, fn: Callable[[], Any]) -> dict[str, Any]:
    try:
        detail = fn()
        return {"name": name, "ok": True, "detail": detail}
    except Exception as exc:  # pragma: no cover - exercised via failing fakes
        logging.warning("pgbouncer_smoke check failed name=%s err=%s", name, exc, exc_info=True)
        return {"name": name, "ok": False, "error": f"{type(exc).__name__}: {exc}"}


def run_pgbouncer_smoke_test(
    *,
    connection_context_factory: Callable[[], Any] | None = None,
    rollout_status_provider: Callable[[], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run the read-only smoke checks and return a structured result.

    ``connection_context_factory`` must yield an object usable as
    ``with factory() as conn:`` that commits on clean exit and rolls back on
    exception (the production ``get_db_connection_context`` contract).
    """
    factory = connection_context_factory or _default_connection_context_factory()
    if rollout_status_provider is None:
        from backend.database import pgbouncer_rollout_status as rollout_status_provider  # type: ignore

    def _select_1() -> dict[str, Any]:
        with factory() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                row = cur.fetchone()
        value = (row or [None])[0]
        if int(value) != 1:
            raise AssertionError(f"SELECT 1 returned {value!r}")
        return {"value": int(value)}

    def _transaction_commit() -> dict[str, Any]:
        # Clean exit -> the context manager issues COMMIT.
        with factory() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return {"committed": True}

    def _transaction_rollback() -> dict[str, Any]:
        # Raising inside the context must trigger ROLLBACK and re-raise.
        rolled_back = False
        try:
            with factory() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                    cur.fetchone()
                raise _SmokeRollback()
        except _SmokeRollback:
            rolled_back = True
        if not rolled_back:
            raise AssertionError("rollback sentinel did not propagate")
        return {"rolled_back": True}

    def _advisory_xact_lock() -> dict[str, Any]:
        with factory() as conn:
            with conn.cursor() as cur:
                # Transaction-scoped: auto-released on COMMIT at context exit.
                cur.execute("SELECT pg_advisory_xact_lock(%s::bigint);", (SMOKE_ADVISORY_LOCK_KEY,))
                cur.fetchone()
        return {"lock_key": SMOKE_ADVISORY_LOCK_KEY, "acquired": True}

    def _set_local_statement_timeout() -> dict[str, Any]:
        with factory() as conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = %s;", ("5000ms",))
                cur.execute("SHOW statement_timeout;")
                row = cur.fetchone()
        shown = str((row or [None])[0] or "").strip()
        return {"statement_timeout": shown}

    def _savepoint() -> dict[str, Any]:
        with factory() as conn:
            with conn.cursor() as cur:
                cur.execute("SAVEPOINT pgbouncer_smoke_sp;")
                cur.execute("SELECT 1;")
                cur.fetchone()
                cur.execute("ROLLBACK TO SAVEPOINT pgbouncer_smoke_sp;")
                cur.execute("RELEASE SAVEPOINT pgbouncer_smoke_sp;")
        return {"savepoint": True}

    def _target_report() -> dict[str, Any]:
        return dict(rollout_status_provider())

    checks = [
        _run_check("select_1", _select_1),
        _run_check("transaction_commit", _transaction_commit),
        _run_check("transaction_rollback", _transaction_rollback),
        _run_check("advisory_xact_lock", _advisory_xact_lock),
        _run_check("set_local_statement_timeout", _set_local_statement_timeout),
        _run_check("savepoint", _savepoint),
        _run_check("target_report", _target_report),
    ]
    overall_ok = all(c["ok"] for c in checks)
    return {
        "ok": overall_ok,
        "checks": checks,
        "rollout_status": dict(rollout_status_provider()),
    }
