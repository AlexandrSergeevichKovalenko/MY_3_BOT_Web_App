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

## 3. Railway CLI — logs, running commands against prod, querying the DB

The **Railway CLI** is a command-line tool to talk to our hosting. Common uses:

```zsh
railway status                 # which project/environment/service you're linked to
railway logs                   # stream the live logs of the current service
railway logs --service BACKEND_WEB   # logs of a specific service (web / worker / scheduler)
railway run <command>          # run <command> LOCALLY but with prod env vars injected
```

`railway run` is the key one: it gives your local command the production environment variables
(database URL, secrets), so you can, for example, open a psql shell against the production database:

```zsh
railway run psql "$DATABASE_URL"    # open an interactive SQL prompt on the prod DB
```

⚠️ That's the **live** database — `SELECT` freely to inspect, but be extremely careful with anything
that writes (`UPDATE`/`DELETE`/`INSERT`). Note our infra has had a "split-brain" DB quirk in the
past (two database hosts), so always confirm which host `DATABASE_URL` actually points at before
trusting a query.

## 4. Reading logs that are already saved as files

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

## 5. The mental model: which tool talks to what

```
curl / browser fetch ──HTTP──▶ BACKEND_WEB (Flask)            ← test endpoints, see responses
git push ────────────────────▶ GitHub ──▶ Railway auto-build  ← deploy
railway logs ◀──stream──────── the running service            ← see what prod is doing right now
railway run psql ────────────▶ Postgres (prod)                ← inspect real data
jq / grep / wc ──────────────▶ saved *.jsonl log files        ← analyze captured logs offline
```

## 6. Self-check

1. Write a `curl` that POSTs `{"limit":7}` as JSON to `/api/webapp/sentences` and shows the response
   status line. Why will it likely return `401`?
2. What single action triggers a production deploy in our setup?
3. Why do we use `git add -p` instead of `git add <file>` on this repo?
4. What does `railway run psql "$DATABASE_URL"` give you that plain `psql` on your laptop would not?
