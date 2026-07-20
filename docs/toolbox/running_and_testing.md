# Toolbox — running the app and using the tests

The learning docs say "check your hypotheses by running things" and "we have tests". This file makes
that concrete: the exact commands to run the frontend, the backend, and the test suite, and — most
useful for learning — **how to read one test as a worked example of how a function is called**.

All commands are typed in the terminal (see [cli_commands.md §0](cli_commands.md)) from the repo
root unless noted:

```zsh
cd /Users/alexandr/Desktop/TELEGRAM_BOT_DEUTSCHESPRACHE
```

## 1. Running the frontend (React + Vite)

The frontend is a **Vite** project (Vite = the dev server + build tool; see
[stack_explained.md](stack_explained.md)). Its commands are defined in `frontend/package.json`
under `"scripts"` (lines 6–12). You run them with `npm run <scriptname>`.

```zsh
cd frontend
npm install        # ONE time (and after dependencies change): downloads packages into node_modules/
npm run dev        # start the Vite dev server → prints a http://localhost:5173 URL, live-reloads on save
npm run build      # produce the optimized production bundle into frontend/dist/
npm run lint       # run eslint — static checks for JS/JSX mistakes (unused vars, bad hooks, ...)
```

- `npm` is Node's package manager; `npm run dev` looks up `"dev": "vite"` in `package.json` and runs
  it. `npm run build` runs `"build": "vite build"`, etc.
- `npm run dev` is for development: it serves the app and hot-reloads the browser when you edit a
  `.jsx` file. Our real app runs *inside Telegram*, so the localhost page will complain about missing
  `initData` (foundations 00b) — that's expected; it's still enough to see UI and console logs.

## 2. Running the backend (Flask via gunicorn)

The backend is Python 3.11. In production it's not "one program" — the `Procfile` (repo root)
defines four separate processes:

```
web:                      gunicorn ... backend.web_service:app     # the Flask HTTP server
worker:                   python -m backend.background_jobs        # heavy background jobs
translation_check_worker: python -m backend.run_dramatiq_worker    # the translation grading worker
scheduler:                python -m backend.scheduler_service       # periodic "cron" ticks
```

- **`gunicorn ... backend.web_service:app`** — gunicorn is a production WSGI server (see
  [stack_explained.md](stack_explained.md)); `backend.web_service:app` means "import the object
  `app` from `backend/web_service.py`" — that `app` is the Flask application every `@app.route`
  attaches to.
- `python -m backend.background_jobs` — `python -m <module>` runs a module as a program. This is the
  worker that does the slow OpenAI/TTS work off the request path.

You rarely run the whole backend locally (it needs Postgres, Redis, and secret env vars). For
learning, **the tests are the practical way to run backend code** — see §3.

## 3. The tests: how to run them and why they're the best learning tool

Our tests use **pytest**. They live in `backend/tests/` (there are ~100 files, one per feature, e.g.
`test_dictionary_lookup_free_limit.py`, `test_fsrs_scheduler.py`). A test is a small Python function
that calls our real code with fixed inputs and asserts the output — so **it's a runnable, correct
example of how a function is meant to be used**.

### Running them

```zsh
# from the repo root:
python -m pytest backend/tests/ -q                       # run the whole suite, quiet output
python -m pytest backend/tests/test_fsrs_scheduler.py -v  # run ONE file, -v = verbose (list each test)
python -m pytest backend/tests/ -k "free_limit"          # -k = only tests whose name matches "free_limit"
python -m pytest backend/tests/test_x.py::test_specific  # run ONE test function (:: selects it)
python -m pytest backend/tests/ -x                        # -x = stop at the FIRST failure
```

- `python -m pytest` — runs the pytest tool. `-q` quiet, `-v` verbose, `-x` stop-on-first-fail,
  `-k "expr"` filter by name substring.
- Output: a row of `.` (pass) and `F` (fail). A green `passed` line at the bottom means the code
  behaves as the test expects. A failure prints the exact assertion that broke and the values.
- If `pytest` says "command not found", use `python -m pytest` (as above) or install:
  `pip install pytest`.

### Reading a test as documentation

This is the payoff. Say you want to understand `save_webapp_translation` (block 01). Find a test
that touches translations:

```zsh
grep -rln "webapp_translation\|translation_check" backend/tests/    # -l = just filenames, -n off
```

Open one and you'll see the pattern every test follows — this is the skeleton of a pytest test:

```python
# a test is just a function whose name starts with test_
def test_something_specific():           # pytest auto-discovers and runs every test_* function
    # 1. ARRANGE — set up inputs (and fake/mocked dependencies)
    user_id = 123
    payload = {"initData": "...", "translations": [{"id_for_mistake_table": 1, "translation": "Ich"}]}

    # 2. ACT — call the real function under test
    result = some_function(payload)

    # 3. ASSERT — state what the output MUST be; pytest fails the test if it isn't
    assert result["status"] == "processing"      # `assert X` raises if X is false
    assert len(result["items"]) == 1
```

`assert <condition>` is Python's check: if the condition is false, the test fails and pytest shows
you the actual vs expected values. So reading the ARRANGE block tells you **what inputs the function
takes and their exact shape**, and the ASSERT block tells you **what it returns**. That's often
clearer than reading the function itself.

### Fixtures (the `backend/tests/fixtures/` folder)

A **fixture** is reusable setup shared across tests (a fake database, a sample user, canned API
responses). You'll see tests take an argument like `def test_x(db):` — pytest fills `db` from a
fixture. You don't need to master this to *read* tests; just know that an argument to a test function
is injected setup, not something you pass by hand.

## 4. Self-check

1. Write the command to run only the tests whose name contains `"billing"`, verbosely.
2. In a pytest test, what do the ARRANGE and ASSERT sections each tell you about the function under
   test?
3. `Procfile` lists `web`, `worker`, `translation_check_worker`, `scheduler`. Which one serves the
   `@app.route` HTTP endpoints, and which one grades translations in the background?
4. What does `assert result == 5` do if `result` is actually `4`?
