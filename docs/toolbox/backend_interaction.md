# Toolbox — how I talk to the backend from the terminal (curl, deploy, logs)

You've watched me deploy, fire `curl` at our endpoints, tail logs, and query the database from the
command line. This file documents exactly those commands so you can do them yourself. Some of these
**change the live app** (deploy) — those are marked ⚠️.

Terminal basics and `grep` are in [cli_commands.md](cli_commands.md); read that first.

## 1. `curl` — calling an HTTP endpoint by hand

`curl` is a command-line tool that sends an HTTP request (exactly the request from
[00b §1](../security_deep_dives/00b_http_and_backend_foundations.md)) and prints the response. It's
`fetch`, but from the terminal instead of the browser. This is how I test "does this endpoint work?"
without opening the app.

Syntax and the flags that matter:

```
curl  [options]  URL
```

| Flag | Meaning |
| --- | --- |
| `-X POST` | set the HTTP **method** (default is GET) |
| `-H "Name: Value"` | add a **header** (e.g. `Content-Type: application/json`) |
| `-d '...'` | the request **body** data (using `-d` also auto-implies POST) |
| `-i` | include the **response headers + status line** in the output |
| `-s` | **silent** — hide the progress meter (nice when piping) |
| `| jq` | pipe the JSON response into `jq` to pretty-print/filter it |

A real call against one of our endpoints (block 01's "load sentences"):

```zsh
curl -i -X POST https://<our-backend-host>/api/webapp/sentences \
  -H "Content-Type: application/json" \
  -d '{"initData":"user=...&hash=...","limit":7}'
```

- The `\` at line end just means "the command continues on the next line" (readability).
- `-i` prints the status line first, e.g. `HTTP/1.1 401 UNAUTHORIZED`, then the JSON body.
- **This specific call will return `401`** unless the `initData` is a real, fresh, correctly-signed
  string from Telegram (block 01 §3). That's the auth gate working. So for protected endpoints I
  either (a) copy a real `initData` out of the app's Network tab, or (b) hit endpoints that don't
  require it. That 401 is itself useful — it *proves* the guard is on.

Pretty-printing JSON with `jq` (a JSON processor):

```zsh
curl -s https://<host>/api/some/public/endpoint | jq              # pretty-print the whole response
curl -s https://<host>/api/some/endpoint | jq '.plans[0].price'   # extract ONE field from the JSON
```

`jq '.field'` walks into the JSON: `.plans[0].price` = "the `price` of the first element of the
`plans` array". If `jq` is missing: `brew install jq`.

## 2. Deploying ⚠️ (git push → Railway auto-deploys)

We host on **Railway** (see [stack_explained.md](stack_explained.md)). Railway watches our GitHub
repo: **pushing to the `refactor/interface` branch of the `bot3_webapp` remote auto-triggers a
deploy.** So "deploy" = "commit + push", there's no separate deploy button.

⚠️ These change the live app. The safe sequence (and our house rules):

```zsh
git status                       # see what changed
git add -p                       # stage changes HUNK BY HUNK (review each) — NOT `git add wholefile`
git commit -m "Explain: ..."     # record the staged changes with a message
git push bot3_webapp refactor/interface   # push → Railway sees it → builds & redeploys
```

House rules that matter here (from how this repo is run):
- **Always work on `refactor/interface`, never commit to `main`.**
- **Stage only your own hunks** (`git add -p`, answer `y`/`n` per hunk) — the repo is shared and
  someone else may be editing concurrently, so never blindly `git add` a whole file.
- The remote is named `bot3_webapp` → `https://github.com/.../MY_3_BOT_Web_App.git`.

After pushing, watch the deploy/logs (next section) to confirm it came up.

**Watch Paths (selective redeploy per service).** By default a push redeploys *every* service. Each
Railway service can set **Watch Paths** — a list of file-path glob patterns; that service only
rebuilds when a push changes a matching file (`/backend/**` = any file under `backend/`, `**` =
nested folders included). This stops, e.g., a pure-frontend push from restarting a background worker
(and orphaning its in-flight jobs). Trade-off: a service with Watch Paths won't redeploy for changes
*outside* those paths — so if a service looks stale, check its Watch Paths first. Worked example (why
this mattered for translation grading):
[security_deep_dives/01 §1.7](../security_deep_dives/01_sentence_translation.md).

## 3. Railway CLI — logs, running commands against prod, querying the DB

The **Railway CLI** is a command-line tool to talk to our hosting. Common uses:

```zsh
railway status                 # which project/environment/service you're linked to
railway logs                   # stream the live logs of the current service
railway logs --service BACKEND_WEB   # logs of a specific service (web / worker / scheduler)
railway run <command>          # run <command> LOCALLY but with prod env vars injected
railway ssh -s <service> ...    # run a command INSIDE the running prod container (see §4)
```

`railway run` is one key one: it gives your **local** command the production environment variables
(database URL, secrets), so you can, for example, open a psql shell against the production database:

```zsh
railway run psql "$DATABASE_URL"    # open an interactive SQL prompt on the prod DB
```

⚠️ That's the **live** database — `SELECT` freely to inspect, but be extremely careful with anything
that writes (`UPDATE`/`DELETE`/`INSERT`). Note our infra has had a "split-brain" DB quirk in the
past (two database hosts), so always confirm which host `DATABASE_URL` actually points at before
trusting a query.

**`railway run` vs `railway ssh` — the crucial difference.** `railway run <cmd>` runs `<cmd>` on
**your laptop**, only borrowing prod's env vars. `railway ssh <cmd>` runs `<cmd>` **inside the actual
production container** — same machine, same filesystem, same live process environment as the running
app. When I "probed prod" during our sessions, that was `railway ssh` — dissected next.

## 4. Running a local script ON the production container (`railway ssh` + the base64 trick)

This is the most powerful — and most dangerous — command in this file. You've watched me use it to
run a diagnostic Python script directly inside our live bot process. Here is the exact command,
taken apart character by character.

```zsh
B64=$(base64 < /tmp/probe2.py | tr -d '\n'); railway ssh -s MY_3_BOT --project c0e448eb-db8c-41c0-8b17-dd3ae1ef53a6 --environment production "python -c \"import base64;exec(base64.b64decode('$B64').decode())\""
```

**What it does in one sentence:** take a Python file sitting on my laptop (`/tmp/probe2.py`), smuggle
it — unchanged — into the running production container, and execute it there inside a fresh Python
interpreter.

**Why it's shaped so weirdly:** you can't just paste a multi-line Python script with its own quotes,
`$`, and newlines into a quoted remote command — the nested quoting would break. Base64 solves that
(explained in 4.4). Let's build it up.

### 4.1 It's actually TWO commands joined by `;`

The `;` is a **command separator**: run the left command fully, then run the right one, regardless of
whether the left succeeded. So this is:

```zsh
# Command 1: build a variable
B64=$(base64 < /tmp/probe2.py | tr -d '\n')
# Command 2: use it
railway ssh -s MY_3_BOT --project <id> --environment production "python -c \"...$B64...\""
```

### 4.2 Command 1 — encode the local script into one safe line

```zsh
B64=$(base64 < /tmp/probe2.py | tr -d '\n')
```

Token by token:
- `B64=...` — a **shell variable assignment**. `B64` is a name we invent; after this line, `$B64`
  holds the value. (No spaces allowed around `=` in shell — `B64 = x` would be read as a command.)
- `$( ... )` — **command substitution**: run the command inside the parentheses, capture whatever it
  prints (its stdout), and substitute that text in place. So `B64` gets set to the output of the
  pipeline inside.
- `base64` — a standard command that **encodes bytes into base64 text**. Base64 rewrites any data
  using only the 64 "safe" characters `A–Z a–z 0–9 + /` (plus `=` padding). No spaces, no quotes, no
  newlines-with-meaning, nothing the shell treats specially.
- `< /tmp/probe2.py` — **input redirection**. The `<` feeds the file's contents into `base64` as its
  standard input. So `base64` encodes the whole script file.
- `| tr -d '\n'` — a **pipe** (`|`) sends `base64`'s output into `tr`. `tr` = "translate/delete
  characters"; `-d '\n'` **deletes all newline characters**. Why: `base64` wraps its output into
  multiple lines (~76 chars each) by default, but we need it as **one unbroken line** so it can sit
  inside a single-line remote command without the newlines breaking the quoting. After `tr -d '\n'`,
  `$B64` is one long string like `aW1wb3J0IG9zCnByaW50KC4uLik=`.

**Result of Command 1:** `$B64` = the entire `probe2.py`, encoded as a single safe token.

### 4.3 Command 2 — run it inside the prod container

```zsh
railway ssh -s MY_3_BOT --project c0e448eb-db8c-41c0-8b17-dd3ae1ef53a6 --environment production \
  "python -c \"import base64;exec(base64.b64decode('$B64').decode())\""
```

- `railway ssh` — the Railway CLI subcommand that **executes a command inside a running service's
  container** (like SSHing into that box). It needs you to be logged in (`railway login`) and to have
  access to the project.
- `-s MY_3_BOT` — the `--service` flag (short form `-s`): **which** service's container to run in.
  `MY_3_BOT` is our Telegram bot process. (Other services: `BACKEND_WEB`, `TRANSLATION_CHECK_WORKER`,
  `SCHEDULER_SERVICE`.)
- `--project c0e448eb-db8c-41c0-8b17-dd3ae1ef53a6` — the project's **UUID** (a globally-unique id).
  Pins the command to exactly this Railway project so it can't accidentally hit another.
- `--environment production` — which environment inside that project (e.g. `production` vs a staging
  one). Together, `service + project + environment` uniquely identify one running container.
- The final **quoted string** is the command to run *on the remote*:
  `python -c "import base64;exec(base64.b64decode('$B64').decode())"`.

The remote Python one-liner, unwound:
- `python -c "<code>"` — the `-c` flag means "run this **c**ode string" instead of a file. Whatever's
  in the string is executed as a Python program.
- `import base64` — load Python's base64 module.
- `base64.b64decode('...')` — **decode** the base64 token back into the original **bytes** of our
  script. This is the exact inverse of the `base64` command from Command 1.
- `.decode()` — turn those **bytes into a string** (the actual Python source text). (Bytes vs string
  is explained in [security_deep_dives/02 §1.3](../security_deep_dives/02_telegram_auth_initdata.md).)
- `exec(<string>)` — **execute a string as Python code**. `exec` compiles and runs whatever source it
  is given. So the remote interpreter now runs our original `probe2.py`, line for line, inside prod.

### 4.4 The quoting/escaping, and why base64 is used at all

This is the subtle part. There are **two** interpreters reading this line: your **local shell** first,
then the **remote Python** second. The quoting keeps each happy.

- The remote command is wrapped in **outer double quotes** `"..."`. Inside double quotes the shell
  still does `$`-expansion — that's deliberate: we WANT `$B64` replaced with the real base64 text
  **locally, before sending**, so the container receives the literal data (the container doesn't have
  a `B64` variable).
- The `python -c` argument itself needs to be quoted on the remote side, so those inner double quotes
  are **escaped** as `\"` — the `\` tells the local shell "this `"` is a literal character, pass it
  through," so the remote receives `python -c "..."` intact.
- Inside the Python, the base64 token is wrapped in **single quotes**: `b64decode('$B64')`. Those are
  Python's string quotes. The local shell doesn't treat these single quotes as its own (they're deep
  inside the outer double-quoted string), so it leaves them alone **but still expands `$B64`**,
  because the outermost quoting the shell sees is the double quote (which permits expansion). Net:
  Python receives `b64decode('aW1wb3J0...=')` — a valid string literal.

**Why base64 instead of just sending the script text?** Because a real script contains characters
that would collide with all this quoting:

| If the script contains… | …sent raw it would… | base64 fixes it because… |
| --- | --- | --- |
| `"` double quotes | close the remote quote early → broken command | base64 has no `"` |
| `'` single quotes | close Python's string early | base64 has no `'` |
| newlines (multi-line script) | can't fit in a one-line remote command | `tr -d '\n'` → one line |
| `$name`, backticks | shell would expand/execute them | base64 has no `$` or backtick |
| spaces, `;`, `|`, `&` | shell would split/redirect | base64 has none of these |

Base64 flattens *any* script into the safe alphabet `[A-Za-z0-9+/=]`, so it drops into the nested
quotes as a single inert token. Encode locally, decode+`exec` remotely. That's the whole trick.

### 4.5 ⚠️ Security — this is remote code execution on production

Be clear-eyed about what this is: **arbitrary code execution inside the live production process,**
with all its secrets and database access. It's incredibly useful for diagnosing "what does prod
actually see right now?", and incredibly dangerous:
- A read-only probe (print an env var's presence, count rows, check a flag) is safe.
- The **same command shape** can mutate the live DB, leak secrets, or take the service down. There is
  no "undo".

Rules of thumb: prefer `railway logs` and `railway run psql` (read-only `SELECT`) first; only reach
for `railway ssh ... exec(...)` when you must run logic inside the container; keep probe scripts
read-only; never leave one that writes. And note the security angle for the whole app: this power
exists **only** because you hold Railway credentials — protecting those credentials (and the bot
token, and DB URL) is exactly the kind of secret-hygiene the [auth block](../security_deep_dives/02_telegram_auth_initdata.md)
and the env-vars section of [stack_explained.md](stack_explained.md) keep stressing. If an attacker
got your Railway login, they'd get this command too.

### 4.6 A minimal, safe version to try

```zsh
# a one-line read-only probe (no local file, no exec needed):
railway ssh -s MY_3_BOT --environment production "python -c \"import os; print('OPENAI key set:', bool(os.getenv('OPENAI_API_KEY')))\""
```

That prints whether the key env var exists in prod (True/False) — without ever printing the secret
itself. Build up from that before doing anything with `exec` or writes.

## 5. Reading logs that are already saved as files

You've seen `railway_logs_*.jsonl` files in the repo root — those are captured log dumps, one JSON
object per line (`.jsonl` = "JSON Lines"). You read them with the same tools as any text, plus `jq`:

```zsh
wc -l railway_logs_last24h.jsonl                     # how many log lines
grep "translation_check" railway_logs_last24h.jsonl | head    # lines mentioning a subsystem
jq 'select(.level=="ERROR")' railway_logs_last24h.jsonl | head   # only ERROR entries (jq filter)
jq -r '.message' railway_logs_last24h.jsonl | head   # -r = raw: print just the message field, unquoted
```

`jq 'select(.<field>==<value>)'` keeps only objects matching a condition — the log-analysis
equivalent of a `WHERE` clause.

## 6. The mental model: which tool talks to what

```
curl / browser fetch ──HTTP──▶ BACKEND_WEB (Flask)            ← test endpoints, see responses
git push ────────────────────▶ GitHub ──▶ Railway auto-build  ← deploy
railway logs ◀──stream──────── the running service            ← see what prod is doing right now
railway run psql ────────────▶ Postgres (prod), runs LOCAL    ← inspect real data (local cmd, prod env)
railway ssh <cmd> ───────────▶ INSIDE the prod container      ← run code on the live box (⚠️ powerful)
jq / grep / wc ──────────────▶ saved *.jsonl log files        ← analyze captured logs offline
```

## 7. Self-check

1. Write a `curl` that POSTs `{"limit":7}` as JSON to `/api/webapp/sentences` and shows the response
   status line. Why will it likely return `401`?
2. What single action triggers a production deploy in our setup?
3. Why do we use `git add -p` instead of `git add <file>` on this repo?
4. What does `railway run psql "$DATABASE_URL"` give you that plain `psql` on your laptop would not?
5. In `B64=$(base64 < /tmp/probe2.py | tr -d '\n')`, what does each of `$( )`, `<`, `|`, and
   `tr -d '\n'` do, and what is finally stored in `$B64`?
6. Why encode the script as base64 instead of pasting it straight into the `python -c "..."` string?
   Name two specific characters in a normal Python script that would break the raw version.
7. In the remote part, `$B64` sits inside Python's single quotes `'$B64'`. Does the **local shell**
   still replace `$B64` with the real value? Why (think about which quote is outermost)?
8. What's the difference between `railway run` and `railway ssh`, and why is `railway ssh ... exec()`
   the most dangerous command in this file?
