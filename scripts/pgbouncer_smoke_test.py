"""Admin/operator CLI: read-only PgBouncer transaction-pool smoke test.

Run on a service (locally or via `railway run`) to verify the active DB
connection target behaves correctly under transaction pooling, without touching
any user data. Prints a sanitized report (no secrets) and exits non-zero if any
check fails — suitable for gating a staged rollout.

Usage:
    python -m scripts.pgbouncer_smoke_test
    python -m scripts.pgbouncer_smoke_test --json
"""

import argparse
import json
import sys

from backend.db_pgbouncer_smoke import run_pgbouncer_smoke_test


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only PgBouncer smoke test")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON only")
    args = parser.parse_args()

    result = run_pgbouncer_smoke_test()

    if args.json:
        print(json.dumps(result, indent=2, default=str))
        return 0 if result["ok"] else 1

    status = result["rollout_status"]
    print("PgBouncer rollout smoke test")
    print(f"  target              : {status.get('db_connection_target')}")
    print(f"  source              : {status.get('database_url_source')}")
    print(f"  pgbouncer configured: {status.get('pgbouncer_url_configured')}")
    print(f"  endpoint_kind       : {status.get('active_endpoint_kind')}")
    print(f"  host:port           : {status.get('active_endpoint_host')}:{status.get('active_endpoint_port')}")
    print(f"  sslmode             : {status.get('active_sslmode')}")
    print(f"  pool min/max        : {status.get('db_pool_minconn')}/{status.get('db_pool_maxconn')}")
    print(f"  selection_error     : {status.get('selection_error') or 'none'}")
    print("  checks:")
    for check in result["checks"]:
        mark = "PASS" if check["ok"] else "FAIL"
        extra = check.get("detail") if check["ok"] else check.get("error")
        print(f"    [{mark}] {check['name']}: {extra}")
    print(f"  overall             : {'PASS' if result['ok'] else 'FAIL'}")
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
