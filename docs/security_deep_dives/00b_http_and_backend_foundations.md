# 00b — HTTP requests and the backend: what a request is, GET vs POST, how it travels, who receives it

Read this once. Every block sends HTTP requests from the frontend to the backend; this file
explains what an HTTP request physically is, its exact structure, the difference between GET and
POST, the status codes we return, and the full path from `fetch` in the browser to a Python
function on our server. Later blocks assume you know all of this.

## 1. What "an HTTP request" actually is

When the frontend calls `fetch('/api/webapp/sentences', {...})`, the browser opens a network
connection to our server and sends a block of **text** in a fixed format. That text is the HTTP
request. It has four parts, in this exact order:

```
POST /api/webapp/sentences HTTP/1.1        ← 1. request line: METHOD  PATH  PROTOCOL-VERSION
Host: bot-backend.up.railway.app           ← 2. headers: "Key: Value" lines, metadata about the request
Content-Type: application/json
Content-Length: 91
                                           ← 3. one BLANK line — separates headers from body
{"initData":"user=...&hash=...","limit":7} ← 4. body: the actual payload (only some methods have one)
```

Line by line:
- **Request line** — three tokens separated by spaces. `POST` is the **method** (the verb, "what
  do you want to do"). `/api/webapp/sentences` is the **path** (which resource). `HTTP/1.1` is the
  protocol version (you never touch this).
- **Headers** — a list of `Name: Value` lines. They are metadata, *not* the data itself. The one we
  set by hand is `Content-Type: application/json`, which tells the server "the body below is JSON
  text, parse it as JSON." The browser adds others automatically (`Host`, `Content-Length`, cookies
  if any).
- **Blank line** — a required empty line marking "headers are done, body starts next."
- **Body** — the payload. For us it's always a JSON string (foundations 00a §4). GET requests have
  **no** body; POST requests carry the data here.

The response the server sends back has the same shape, but the first line is a **status line**
instead of a request line:

```
HTTP/1.1 200 OK                            ← status line: PROTOCOL  STATUS-CODE  REASON
Content-Type: application/json
                                           ← blank line
{"sentences":[...]}                        ← response body (JSON)
```

## 2. The methods: GET, POST, and the others

The **method** declares intent. The two we use, and the ones you should recognize:

| Method | Meaning | Has a body? | We use it for |
| --- | --- | --- | --- |
| `GET` | read data, change nothing | no | (some static reads elsewhere in the app) |
| `POST` | send data / cause an action | yes | **all four translation endpoints** |
| `PUT` / `PATCH` | replace / partially update a resource | yes | (not in this block) |
| `DELETE` | remove a resource | usually no | (not in this block) |

Two properties matter for security and correctness:
- **Safe** — a method that only reads and changes nothing (GET). It should be repeatable with no
  side effects.
- **Idempotent** — running it twice has the same effect as once (GET, PUT, DELETE are; POST is
  **not** — two POSTs may create two things). This is why our `/check/start` needs an idempotency
  key (block 01 §3): POST is not idempotent by nature, so we add that guarantee ourselves.

**Why we POST even to "read" sentences (endpoints ① and ③ in block 01).** Reading is normally GET.
But GET has no body, and our authentication proof `initData` is a long string we don't want in the
URL. Putting secrets/large data in a URL is bad: URLs get logged by servers and proxies (a **proxy** =
an intermediary server your traffic passes through on the way to its destination), saved in browser
history, and shown in the address bar. A body is not logged that way. So we choose POST to
keep `initData` in the body. That is a deliberate trade-off, not a mistake — but note it means these
"reads" are POSTs, which is why they also need rate-limiting like writes (block 01 §4).

## 3. The status codes we return

The **status code** is a 3-digit number on the response's first line telling the frontend how it
went. The ranges:
- `2xx` success. We use **`200 OK`**.
- `3xx` redirect (not used here).
- `4xx` the **client's** fault (bad/forbidden request). We use:
  - **`400 Bad Request`** — you sent malformed or missing data (e.g. no `initData`, `translations`
    not a list).
  - **`401 Unauthorized`** — authentication failed (`initData` hash didn't validate).
  - **`429 Too Many Requests`** — you hit a rate/quota limit (the free daily explain cap).
- `5xx` the **server's** fault (`500 Internal Server Error` — our code crashed).

The frontend branches on these. In 00a §7 you saw `if (!response.ok)` — `response.ok` is `true`
only for `2xx`. Everything `4xx`/`5xx` is an error path. Distinct codes let the UI react differently:
`401` → "reopen the app", `429` → "you've hit today's limit", `400` → "fix your input".

## 4. The full round trip: from `fetch` to a Python function and back

Here is the entire journey of one request, end to end. Memorize this pipeline — it's the same for
every block.

```
[1] FRONTEND (React, in the Telegram webview)
      fetch('/api/webapp/sentences', { method:'POST', headers:{...}, body: JSON.stringify(obj) })
      │   JSON.stringify turns the JS object into a JSON string (the body text)
      ▼
[2] BROWSER builds the raw HTTP request text (§1) and sends it over TLS (https, encrypted)
      │   the path '/api/...' is relative, so it goes to the same host that served the app
      ▼
[3] RAILWAY routing → the request reaches our BACKEND_WEB service (the Flask process)
      │   (Railway is the hosting platform; BACKEND_WEB is the one service that serves HTTP)
      ▼
[4] FLASK matches the path+method to a handler via the @app.route decorator
      │   @app.route("/api/webapp/sentences", methods=["POST"]) → def get_webapp_sentences()
      ▼
[5] THE HANDLER runs:
      payload = request.get_json(silent=True) or {}     # parse body JSON → Python dict
      init_data = payload.get("initData")               # read fields
      ... validate, authenticate, query DB / call OpenAI ...
      return jsonify({...}), 200                         # build response body + status
      ▼
[6] FLASK serializes the dict to a JSON response (§1 response shape) and sends it back
      ▼
[7] BROWSER resolves the fetch Promise → response.ok / await response.json()
      ▼
[8] FRONTEND updates React state → the screen redraws with the data

LEGEND
  TLS/https  encrypted transport, so nobody on the network can read initData or the body
  handler    the Python function bound to one (path, method) pair
  BACKEND_WEB the single service (process) that receives HTTP; other services (bot, workers) don't
```

## 5. How Flask *receives* a request (the server side, once)

Three Flask constructs appear in every handler. Learn them here so blocks don't re-explain them.

```python
@app.route("/api/webapp/sentences", methods=["POST"])   # (A) routing decorator
def get_webapp_sentences():                              # (B) the handler function (no params)
    payload = request.get_json(silent=True) or {}        # (C) read the request
    init_data = payload.get("initData")
    ...
    return jsonify({"error": "initData обязателен"}), 400 # (D) build the response + status
```

- **(A) `@app.route(path, methods=[...])`** — a decorator (the `@` line attaches behavior to the
  function below it). It registers the function in Flask's routing table: "when an HTTP request
  arrives with this exact path and one of these methods, call this function." If a `GET` hits a
  route declared `methods=["POST"]`, Flask auto-responds `405 Method Not Allowed` — the handler
  never runs.
- **(B) `def handler():`** — the function takes no arguments. Flask does not pass the request as a
  parameter; instead it exposes it through a global-like object called `request` (imported from
  Flask). This is a Flask convention.
- **(C) `request.get_json(silent=True)`** — reads the raw body text and parses it from JSON into a
  Python **dict** (Python's equivalent of a JS object). `silent=True` = "return `None` instead of
  raising if the body isn't valid JSON"; `... or {}` then guarantees `payload` is a dict. Related:
  `request.args` reads URL query parameters (for GET), `request.headers` reads headers,
  `request.form` reads form-encoded bodies. We use `get_json` because our bodies are JSON.
- **(D) `return jsonify(dict), status`** — `jsonify` converts a Python dict into a proper JSON HTTP
  response (sets `Content-Type: application/json`). Returning a tuple `(body, 401)` sets the status
  code; returning just `body` defaults to `200`.

"Who receives it" in one sentence: the request travels over https to Railway, which routes it to the
**BACKEND_WEB** Flask process, where the `@app.route`-registered handler function runs and returns
JSON. The Telegram bot process and the background workers are **different** services and never see
these HTTP requests.

## 6. Self-check

1. What are the four parts of an HTTP request, in order, and which part carries `initData` in our
   POST calls?
2. Endpoint ① only *reads* sentences but uses POST, not GET. Give the concrete reason, and name one
   downside of putting `initData` in a URL instead.
3. The server returns `401` vs `400` vs `429`. Match each to: "your input was malformed", "your
   auth failed", "you exceeded a limit".
4. In `@app.route("/x", methods=["POST"])`, what does Flask do if a `GET` arrives at `/x`?
