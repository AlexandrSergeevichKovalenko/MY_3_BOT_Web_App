"""Test-run environment.

Importing backend.backend_server runs schema DDL against whatever DATABASE_URL is
configured — and a developer machine carries PRODUCTION credentials. So a plain `pytest`
connected to the live database and issued DDL there, which is both wrong and flaky: when
that connection was slow, unrelated tests failed with a psycopg2 timeout mid-suite.

Set here, before any test module imports the app, so no test can opt out by accident.
Production does not set this variable and its startup is unchanged.
"""

import os

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
