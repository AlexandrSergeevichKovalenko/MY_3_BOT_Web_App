"""Test-run environment.

Importing backend.backend_server runs schema DDL against whatever DATABASE_URL is
configured — and a developer machine carries PRODUCTION credentials. So a plain `pytest`
connected to the live database and issued DDL there, which is both wrong and flaky: when
that connection was slow, unrelated tests failed with a psycopg2 timeout mid-suite.

The same production credentials also let tests WRITE. Measured 02.08.2026: one week of
local `pytest` runs left 1010 phantom "OpenAI request" rows in the live billing ledger
under the fixture user ids 123 and 456 (test_shortcut_lookup_split feeds a fake OpenAI
client with no usage, so the rows carry no tokens) plus 108 more from
pool_crossword_judge. The daily economics report then showed 1037 shortcut calls where
27 were real. Patching the writer inside a test does not help: the ledger write happens
in a daemon thread spawned by the billing helpers.

Both variables are set here, before any test module imports the app, so no test can opt
out by accident. Production sets neither and its behaviour is unchanged.
"""

import os

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
# Личная ротация тоже пишет в базу — в прогоне тестов это боевая база.
os.environ.setdefault("SKIP_TASK_ROTATION_WRITES", "1")
