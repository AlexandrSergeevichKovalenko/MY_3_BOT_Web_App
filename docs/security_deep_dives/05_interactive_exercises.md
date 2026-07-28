# 05 — Interactive exercises: from tap to result (deep dive on image hit-testing)

Prerequisites: [00a](00a_frontend_foundations.md), [00b](00b_http_and_backend_foundations.md),
[01](01_sentence_translation.md) (grading, `%s` SQL), [02](02_telegram_auth_initdata.md) (auth),
[04](04_shared_word_cache.md) (`ON CONFLICT`). This block reuses all of it.

The app has **many** interactive mini-games (rebus, crossword, anagram, multiple-choice, artikel
der/die/das, wo-frage, number-dictation, "find the error", satzbau, and the star of this file:
**tap-the-object-in-a-picture**). They all run through **one** pipeline. This file explains that
pipeline from the user's tap to the result on screen, then goes deep on the image-tap mechanic,
which has a genuinely clever multi-layer validation you'll enjoy.

Standard frame: `⚙️1 Under the hood → 🥷2 Threats → 🛡️3 Defenses → 📈4 Recommendations`.

# ⚙️ 1. Under the hood

## 1.1 One overlay for every game, driven over HTTP

First, a correction to a natural assumption. In a Telegram bot, buttons usually send **`callback_data`**
(a short string the bot receives in a `CallbackQueryHandler`). The interactives do **not** work that
way. The bot sends a button whose URL is a **Direct-Link Mini App deeplink**:

```
https://t.me/<bot>?startapp=ans_<kind>_<id>
```

built by `get_webapp_deeplink(...)` (`bot_3.py:5340`). Tapping it **opens the Mini App** (the React
web app) with `ans_<kind>_<id>` handed to the frontend as the `start_param`. There is **no bot-side
handler** for `ans_` — the entire answer loop is plain HTTP from the Mini App to `/api/answer/*`.
(**`startapp`** = a launch parameter Telegram passes into a Mini App when it's opened via a link;
**`start_param`** is where the frontend reads it.)

The frontend routes any `?startapp=ans_*` to a single component,
`frontend/src/answer/AnswerOverlay.jsx` (mounted via `main.jsx:613`), which renders **every** game
kind. It parses the deeplink and whitelists the kind with a regex (`AnswerOverlay.jsx:47`).

## 1.2 The two endpoints and the `api()` helper

Everything is two POSTs: **load a task**, then **submit an answer**. Both go through one helper
(`AnswerOverlay.jsx:105`):

```js
async function api(path, body) {
  const response = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Telegram-InitData': getInitData() },
    body: JSON.stringify({ initData: getInitData(), ...body }),   // initData in BOTH header and body
  });
  ...
}
```

- **Load:** `api('/api/answer/task', { kind, id })` (`AnswerOverlay.jsx:667`) → returns the task to
  render (question text, options, image URL, etc.).
- **Submit:** `api('/api/answer/submit', { kind, id, answer, time_ms })` (`AnswerOverlay.jsx:693`) →
  returns the verdict.

Note `{ initData: getInitData(), ...body }` — the spread `...body` merges the caller's fields into
the same object (see [00a](00a_frontend_foundations.md)). And `initData` rides in **both** the header
and the JSON body (this flow predates the header-only convention; block 02 §1.7).

The Flask side:
- `@app.route("/api/answer/task")` → `get_answer_task()` (`backend_server.py:26491`)
- `@app.route("/api/answer/submit")` → `submit_answer()` (`backend_server.py:26554`)
- Both authenticate first via `_answer_auth_user_id()` (`backend_server.py:26447`) — the block-02
  `initData` gate. No user → error, before any work.

## 1.3 The catalog of "kinds"

Dispatch is by a short **`kind`** code, and for the `au` ("Aufgabe") kind, a secondary **`format`**.
There is no registry object; it's an explicit `if/elif` mirrored in **three** places that must stay
in sync — frontend render (`AnswerOverlay.jsx:803`), backend load (`backend_server.py:26519`), backend
grade (`backend_server.py:26580`). The kinds (whitelist regex at `AnswerOverlay.jsx:48`):

| kind | game | grader (`answer_eval.py`) |
| --- | --- | --- |
| `rb` | rebus | `evaluate_rebus:237` |
| `cw` | crossword | `evaluate_crossword:411` |
| `ag` | anagram | `evaluate_anagram:489` |
| `mc` | multiple-choice over an image | `evaluate_mc:612` |
| `qf`/`qfp` | freeform (poll-scoped) | `evaluate_freeform:1198` |
| `ls` | listening (async LLM-graded) | `start_listening_evaluation:792` |
| `nd`/`np` | number-dictation | `grade_numdict_item:957` |
| `au` | Aufgabe (multi-format, below) | `evaluate_aufgabe:2361` |
| `sp`,`as*`,`ad*`,`wf*`,`rv`,`bh` | sprints, Artikel/Adjektiv/Wo-Frage battles, review | self-loading game components |

The `au` kind fans out again by `format`: `cloze`, `wortbildung`, `wortgruppe`, `transform`,
`error` ("Найди ошибку"), `hoerluecke`, `satzbau`, `adjektiv`, `artikel`, `wofrage`, `synonym`,
`antonym`, `video`, and **`pin`** (tap-the-object). Backend format dispatch: `_grade_aufgabe`
(`answer_eval.py:2075`) → `_check_aufgabe`/`_grade_pin`.

## 1.4 The generic pipeline (tap → result)

```
[1] bot sends a button: url = t.me/<bot>?startapp=ans_<kind>_<id>     (bot_3.py:5340)
      user taps → Telegram opens the Mini App with start_param
        ▼
[2] AnswerOverlay parses "ans_<kind>_<id>", POST /api/answer/task {kind,id}   (AnswerOverlay.jsx:667)
        ▼
[3] backend get_answer_task(): auth → load_<kind>_task → returns the task
      (question, image_url, options…) — WITHOUT the answer                    (backend_server.py:26491)
        ▼
[4] overlay renders the right game component; user interacts (types / taps / drags)
        ▼
[5] POST /api/answer/submit {kind,id,answer,time_ms}                          (AnswerOverlay.jsx:693)
        ▼
[6] backend submit_answer(): auth → per-kind evaluator (answer_eval.py) → verdict
      → record attempt (anti-replay) → add ranking → jsonify(ok, **result)    (backend_server.py:26554)
        ▼
[7] overlay shows the result card (correct/incorrect, correct answer, explanation, ranking)

  LEGEND
    kind   short code selecting the game (rb, cw, au, …); au has a secondary `format`
    id     the dispatch_id: which specific task instance this is (created when the bot sent it)
    the answer never travels to the client in step 3 — grading is server-side only
```

## 1.5 Grading, anti-replay, storage, ranking

Each kind has an evaluator in `backend/answer_eval.py` returning a dict like
`{is_correct, correct_word, explanation, hint_ru, …}`. `submit_answer` merges it, adds a **ranking**
(`compute_challenge_ranking`, `database.py:43820`), and returns `jsonify({"ok": True, **result})`
(`backend_server.py:26730`).

Two storage/anti-abuse mechanisms worth knowing:

- **Anti-replay via the database.** An Aufgabe answer is stored with
  `record_aufgabe_answer(...)` (`database.py:46923`):
  ```sql
  INSERT INTO bt_3_aufgabe_answers (dispatch_id, user_id, answer, is_correct)
  VALUES (%s, %s, %s, %s)
  ON CONFLICT (dispatch_id, user_id) DO NOTHING     -- one answer per (task, user), ever
  ```
  `ON CONFLICT (dispatch_id, user_id) DO NOTHING` means: if this user already answered this task, the
  insert is silently ignored. And the evaluator early-returns the **stored** verdict on a second
  submit (`evaluate_aufgabe` checks `get_aufgabe_answer` first, `answer_eval.py:~2368`). So you can't
  re-submit to fish for the right answer — your first answer is final.
- **Mistakes feed spaced repetition.** For review-eligible formats, `record_aufgabe_mistake(...)`
  (`database.py:1659`) queues the miss into the "работа над ошибками" (SRS = spaced-repetition
  system) so it resurfaces later.

## 1.6 DEEP DIVE — the `pin` mechanic (tap the object in a picture)

This is format `au`/`pin` ("🖼 Finde im Bild"): you see a scene, tap the named object, and type its
article. The "right answer" is a **region of a picture**, so the interesting question is *where does
that region come from, and how do we decide a tap hit it?*

> **Evolution note (worth understanding — the design was reversed).** An earlier version of this
> mechanic was **fully automated**: DALL·E rendered a scene with a mandatory bigger *decoy*, then a
> vision model was asked to *locate* the object **twice**, the two boxes were cross-checked by
> **IoU** and stored as their **union**, and at grade time a **second vision "is the mark on the
> target?" call** plus a background **self-heal** re-located wrong boxes. It was clever but fragile:
> vision models ground coordinates poorly for small objects, so items were sometimes unwinnable, and
> every dispute cost an OpenAI vision call. That whole pipeline has been **removed** and replaced
> with a **human-in-the-loop "scene studio"**: an admin draws the answer box **by hand**, once, and
> that box is the single source of truth. Grading is now **purely deterministic — no AI at answer
> time.** This is a nice real-world lesson: sometimes the robust design is *less* clever. Below is
> the current system; the old one is gone from the code.

### 1.6.1 Concepts you need

- **Normalized coordinates.** Instead of pixels (which differ per screen), a point is stored as two
  fractions in `0..1`: `x=0.5, y=0.5` is the image's center, regardless of displayed size. This makes
  a tap comparable to a stored region without knowing anyone's screen size.
- **Bounding box (bbox).** A rectangle around the target, stored as `[x, y, w, h]` — top-left corner
  `(x,y)` plus width and height, all normalized `0..1`. So `[0.2, 0.3, 0.1, 0.15]` is a box starting
  at 20%/30% across, 10% wide, 15% tall.
- **Point-in-box hit test.** "Is a tap inside the box?" → check `x` is between the box's left and
  right edges and `y` between top and bottom (with a small tolerance margin).

### 1.6.2 Authoring time — the admin "scene studio"

Pin items are created **by an admin**, not by the AI. The endpoints (all admin-only via
`_pin_review_admin_id`, in `backend/backend_server.py`) form a small studio:

| Endpoint | Handler | Does |
| --- | --- | --- |
| `POST /api/answer/pinreview/scenes` | `answer_pin_scenes_create` (`:28372`) | queue scene descriptions → background DALL·E render |
| `POST /api/answer/pinreview/upload` | `answer_pin_scene_upload` (`:28391`) | admin uploads own photo (base64, ≤12 MB) → ready scene |
| `GET  …/scenes/ready` | `answer_pin_scenes_ready` (`:28425`) | list scenes awaiting a target |
| `POST …/addtarget` | `answer_pin_add_target` (`:28436`) | **the core: the hand-drawn box + typed word** |
| `POST …/deltarget` | `answer_pin_del_target` (`:28478`) | retire a mis-added target |
| `POST …/scenedone` | `answer_pin_scene_done` (`:28493`) | mark a scene done/skipped |

The heart is `answer_pin_add_target` (`backend_server.py:28436`). The admin has drawn a rectangle on
the image and typed `der/die/das <word>`; the server validates both and stores them:

```python
word = str(payload.get("word") or "").strip()
m = re.match(r"^(der|die|das)\s+(.+)$", word, flags=re.IGNORECASE)   # must start with an article
if not m: return jsonify({"error": "Слово должно начинаться с артикля …"}), 400
article = m.group(1).lower()
target_label = f"{article} {m.group(2).strip()}"
bbox = payload.get("bbox")
if not (isinstance(bbox, list) and len(bbox) == 4): return jsonify({"error": "нет рамки"}), 400
x, y, w, h = (float(v) for v in bbox)
# box sanity: inside the image and not a pin-prick
if not (0 <= x <= 1 and 0 <= y <= 1 and w > 0.005 and h > 0.005
        and x + w <= 1.001 and y + h <= 1.001):
    return jsonify({"error": "рамка вне картинки или слишком мелкая"}), 400
aufgabe_id = create_pin_target_from_scene(                       # database.py:48841
    scene_id=scene_id, image_object_key=scene["image_object_key"],
    bbox=[round(x, 4), round(y, 4), round(w, 4), round(h, 4)],   # stored, rounded to 4 decimals
    target_label=target_label, article=article)
```

So the **only** source of the answer box is a human's drawn rectangle, sanity-checked to be inside
the image (`0..1`) and not degenerate (`w,h > 0.005`), and stored normalized. No vision, no IoU, no
union. A `_PIN_TRIVIAL_NOUNS` difficulty gate still exists (`bot_3.py`, ~`:32724`) but is **bypassed
for admin-chosen words** (`if not admin_chosen and noun in _PIN_TRIVIAL_NOUNS: …`) — the admin is
trusted to pick a fair target. For review, `draw_pin_bbox_preview` (`openai_manager.py:7908`) renders
the image with the stored box framed (green) so the admin can eyeball it on an acceptance screen
(used by `admin_pin_check_command` in `bot_3.py`).

> **Dead/stale code to know about (honest findings):** `run_vision_object_coverage`
> (`openai_manager.py:7856`) — a "how big is the object" vision check — exists but is **called from
> nowhere** (only its own error-log line references it). And a comment in `_check_aufgabe`
> (`answer_eval.py:2231`) still says grading "adds the vision re-check on a bbox miss" — that is now
> **false**; there is no vision at grade time. Both are cleanup candidates (see recommendations).

### 1.6.3 Runtime — capturing the tap (frontend)

`AufgabePin` (`frontend/src/answer/AufgabeGame.jsx:277`) turns a click into normalized coords:

```js
const onImgClick = (e) => {
  const r = e.currentTarget.getBoundingClientRect();       // the displayed <img>'s position+size on screen
  const x = Math.min(1, Math.max(0, (e.clientX - r.left) / r.width));   // pixel → fraction 0..1, clamped
  const y = Math.min(1, Math.max(0, (e.clientY - r.top)  / r.height));
  setTap({ x, y });
};
```

- `getBoundingClientRect()` returns the `<img>`'s on-screen rectangle (`left`, `top`, `width`,
  `height` in pixels).
- `(e.clientX - r.left) / r.width` — the tap's pixel offset from the image's left edge, divided by the
  image's displayed width → a **fraction 0..1**. Same for `y`.
- `Math.max(0, Math.min(1, …))` — clamps into `[0,1]` in case of an edge tap.

Because the coordinate is normalized against the **displayed** image, the server needs **no**
knowledge of screen size or original resolution — `0.5,0.5` means "center" for everyone. The submit
serializes to a compact string `"x.xxxx,y.yyyy"` or `"x,y|article"` (`AufgabeGame.jsx:292`).

### 1.6.4 Grading time — purely deterministic (backend)

Because the box was drawn by a human and is trusted, grading is now a simple, AI-free check.
`_grade_pin` (`answer_eval.py:2158`) — read the docstring, it states the philosophy:

```python
def _grade_pin(payload, raw_input):
    """... The region is drawn BY HAND on the acceptance screen, so it is the source of
    truth — no vision second-guessing. A miss is a miss; the learner is told which half
    (tap or article) failed."""
    tap, article = _parse_pin_answer(raw_input)        # "x,y|article" → ((x,y), "der")   (:2134)
    if not tap: return False, ""
    req_article = str(payload.get("article") or "").strip().lower()
    ok_article = (not req_article) or check_quiz_freeform_deterministic(article, req_article)
    ok_tap = _pin_bbox_hit(payload, tap)               # the ONE check: point-in-box (below)
    if ok_tap and ok_article:
        return True, ""
    # else: an HONEST per-half reason — which of {object, article} failed:
    #   "предмет верно, но артикль другой" / "артикль верный, но не тот предмет" / "оба мимо"
    return False, reason
```

The single geometric check is `_pin_bbox_hit` (`answer_eval.py:2145`), unchanged from before:

```python
bx, by, bw, bh = (float(v) for v in bbox)
x, y = tap
m = 0.06   # forgiving margin so a near-miss on a clear object still counts
return (bx - m) <= x <= (bx + bw + m) and (by - m) <= y <= (by + bh + m)
```

This is a point-in-box test on normalized coords with a **0.06 margin** added on all four sides (a
**tolerance**: a tap up to 6% of the image outside the box still counts). `(bx - m) <= x <= (bx + bw
+ m)` is Python's chained comparison — "x is between the left edge minus margin and the right edge
plus margin", same for `y`. That's the whole grader now: no vision call, no background repair, no
per-answer OpenAI spend. **Honest failure reasons** still tell the learner which half (object vs
article) failed (`answer_eval.py:2172`).

### 1.6.5 The layers, at a glance (current)

| When | Layer | What it guards against |
| --- | --- | --- |
| authoring | **a human draws the box** (`answer_pin_add_target`) | an AI mis-locating small objects → unwinnable items |
| authoring | article regex `^(der\|die\|das)\s+…` | a target stored without/with a wrong article |
| authoring | box sanity `0..1`, `w,h > 0.005`, `x+w,y+h ≤ 1.001` | a box outside the image or a pin-prick |
| authoring | admin acceptance preview (`draw_pin_bbox_preview`) | a wrong box shipping unreviewed |
| authoring | trivial-noun gate (bypassed for admin) | too-easy auto-picked targets |
| grade | bbox hit-test + 0.06 margin | precise-but-harsh rejection of near-misses |

Compared to the old design, every *generation-time vision layer* and every *grade-time vision layer*
is replaced by one thing: a trusted human box. Fewer moving parts, zero answer-time AI cost,
deterministic and repeatable grading.

# 🥷 2. Threats

- **T1 — Answer forgery / reading the answer.** Read the `/api/answer/task` response hoping it
  contains the correct answer (the bbox, the right option, the solution word), then submit that.
- **T2 — Coordinate forgery.** For `pin`, skip the UI and POST `answer:"0.5,0.5|der"` directly, or
  submit the exact box center — a script instead of a real tap.
- **T3 — Replay / brute force.** Submit the same task many times with different answers until one is
  marked correct.
- **T4 — IDOR.** Submit an answer for a `dispatch_id` that belongs to another user/chat, or load
  another user's task, to grade content you weren't given or corrupt their attempt.
- **T5 — Authoring abuse (non-admin).** Call the `pinreview` endpoints directly to create or poison
  targets — a box nobody can hit, an offensive uploaded image, or a wrong article — which would then
  be served to real learners.
- **T6 — Injection via scene text.** Put instructions in a scene description or `target_label` so the
  **background** DALL·E render or the language-enrichment step is subverted. (There is no longer any
  vision at answer time to attack — that surface is gone.)

# 🛡️ 3. Defenses

- **T1 fails because the answer never leaves the server.** `/api/answer/task` returns only what's
  needed to *play* — for `pin`, `question_de`, `needs_article`, `image_url`, `hint_ru` (task meta
  builder, `answer_eval.py:~1673`) — and **not** the `bbox`. The target region is stored server-side
  and only echoed back in the **result** payload *after* you answer (`_aufgabe_result_payload`,
  `answer_eval.py:~1540`, for the learning replay). So there's nothing to read ahead of time. (This is
  the reason the bbox-not-in-task-payload detail matters — verify it stays that way when editing.)
- **T2 is neutralized by server-side grading.** Whether the tap came from a finger or a `curl`, the
  server runs the same layered check. Submitting the exact center is fine — that's a correct answer;
  there's no bypass because *there is no client-trusted verdict*. The client only sends the raw tap;
  the truth is computed on the server.
- **T3 fails on anti-replay.** `record_aufgabe_answer`'s `ON CONFLICT (dispatch_id, user_id) DO
  NOTHING` plus the evaluator's early-return of the stored verdict (`answer_eval.py:~2368`) make the
  **first** answer final; later submits return the same verdict, not a new grading.
- **T4 fails on auth + ownership.** Every `/api/answer/*` route resolves the acting user from
  `initData` via `_answer_auth_user_id` (`backend_server.py:26447`, block 02); attempts are keyed by
  `(dispatch_id, user_id)`, so you can only affect **your own** row.
- **T5 fails on admin-only auth + input validation.** Every `pinreview` endpoint checks
  `_pin_review_admin_id` first; a non-admin is rejected. The admin's own inputs are still validated —
  article regex `^(der|die|das)…`, box sanity (`0..1`, `w,h > 0.005`, `x+w,y+h ≤ 1.001`), upload size
  ≤ 12 MB — so even a fat-fingered target can't store a broken box.
- **T6 is much smaller than before.** There is **no answer-time model call** to inject into (the
  vision `locate`/`point-check` are gone). The only remaining surface is the background scene
  render / language enrichment, which runs off the request path on admin-provided text; strict-JSON
  parsing and `temperature=0` apply where a model is still used.

# 📈 4. Recommendations

1. **Delete the dead code / fix the stale comments** left by the rewrite: `run_vision_object_coverage`
   (`openai_manager.py:7856`) has **no callers**, and the comment in `_check_aufgabe`
   (`answer_eval.py:2231`) still claims a vision re-check that no longer exists. Both mislead a reader
   (and you — that's why this doc flags them).
2. **Add a server-side sanity check on submitted coords** (reject values outside `[0,1]` or malformed
   `"x,y"` before grading) — cheap, and it hardens T2's malformed-input variants.
3. **Assert in a test that `/api/answer/task` never includes `bbox`/`article`/`correct_*`** for any
   kind — a single parametrized pytest so a future refactor can't accidentally leak the answer (T1).
4. **Log admin authoring actions** (who added / retired which target, on which scene). The human is
   now the source of truth for pin, so an audit trail makes an offensive or broken target traceable.
5. **Cap `time_ms` / detect impossible speeds.** An answer submitted a few milliseconds after the task
   loads is a script, not a human — useful signal for T3.

# Self-check

1. Trace `_grade_pin` when the tap lands **in** the box but the typed **article is wrong**. What does
   it return, and what message does the learner see? (No AI is involved — name every step.)
2. The pin answer box now comes from a **human**, not the AI. Name two validations
   `answer_pin_add_target` runs on the admin's box + word before storing them.
3. The mechanic used to run a vision "is the mark on target?" recheck and even self-heal wrong boxes,
   then that whole pipeline was **removed**. Give the engineering reason the simpler human-in-the-loop
   design is more robust here.
4. `/api/answer/task` deliberately omits one field that would let a cheater win instantly. Which
   field, and where is the answer actually revealed to the client?
5. `record_aufgabe_answer` uses `ON CONFLICT (dispatch_id, user_id) DO NOTHING`. What attack does that
   defeat, and what does the evaluator return on a second submit?

Last checked against the code: 2026-07-28 (pin mechanic re-mapped after the human-in-the-loop rewrite;
repo is edited concurrently — if a line is off by a few, grep the function name).
