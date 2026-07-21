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

This is format `au`/`pin` ("🖼 Finde im Bild"): you see a generated scene, tap the named object, and
type its article. It's the most interesting validation in the app because the "right answer" is a
**region of a picture**, and vision models are unreliable at coordinates — so the design uses
**several independent layers**, some at generation time and some at grading time.

### 1.6.1 New concepts you need first

- **Normalized coordinates.** Instead of pixels (which differ per screen), a point is stored as two
  fractions in `0..1`: `x=0.5, y=0.5` is the image's center, regardless of displayed size. This makes
  a tap comparable to a stored region without knowing anyone's screen size.
- **Bounding box (bbox).** A rectangle around the target, stored as `[x, y, w, h]` — top-left corner
  `(x,y)` plus width and height, all normalized `0..1`. So `[0.2, 0.3, 0.1, 0.15]` is a box starting
  at 20%/30% across, 10% wide, 15% tall.
- **Point-in-box hit test.** "Is a tap inside the box?" → check `x` is between the box's left and
  right edges and `y` between top and bottom.
- **IoU (Intersection over Union).** A number `0..1` measuring how much two boxes overlap:
  `overlap_area / combined_area`. `1.0` = identical boxes; `0.0` = no overlap. Used to check whether
  two independent guesses agree.
- **Vision model, two modes.** We use an OpenAI vision model two different ways: `locate` (give me the
  bbox of object X) and `point-check` (does this drawn mark sit on object X?). The key insight in our
  code: **vision models ground raw coordinates poorly but read a drawn marker reliably** — so grading
  never asks the model for numbers, it draws the tap and asks yes/no.

### 1.6.2 Generation time (pool) — building a trustworthy target region

Pin items are built **off the critical path**, when the pool is topped up (`bot_3.py`, the
`fmt == "pin"` branch at `32167`+ and the vision step at `32253`+). The steps:

1. **Render a scene with a deliberate decoy.** DALL·E (OpenAI's image generator) draws the scene from
   a blueprint prompt (`aufgabe_pin_blueprint`, `openai_manager.py:4097`) that **requires** the target
   to *not* be the biggest/central object and **mandates a bigger decoy of the same category**
   (`openai_manager.py:4108`). That "подвох" (trick) is what makes the game non-trivial — you can't
   just tap the obvious big thing.
2. **Locate the target with vision — twice, independently** (`bot_3.py:32271` and `32275`):
   ```python
   loc  = await asyncio.to_thread(run_vision_locate, img, payload["target_label"], mime=mime)
   loc2 = await asyncio.to_thread(run_vision_locate, img, payload["target_label"], mime=mime)
   ```
   `run_vision_locate` (`openai_manager.py:7083`) asks the model for strict JSON
   `{"present": bool, "bbox": [x,y,w,h]}` with `temperature=0` and `response_format={"type":
   "json_object"}` (**JSON mode** = the model must return valid JSON; **temperature=0** = most
   deterministic output). It then **sanity-clamps**: rejects anything not inside `[0,1]` or degenerate
   (`openai_manager.py:7134`). If the object isn't clearly present → the item is **rejected**, no
   silent fallback.
3. **Require the two locates to agree (IoU gate)** (`bot_3.py:32280`):
   ```python
   from backend.answer_eval import PIN_BBOX_MIN_IOU, pin_bbox_iou, pin_bbox_union
   iou = pin_bbox_iou(loc["bbox"], loc2["bbox"])
   if iou < PIN_BBOX_MIN_IOU:          # 0.35 — the two guesses point at different things → drop item
       ... reject ...
   payload["bbox"] = pin_bbox_union(loc["bbox"], loc2["bbox"])   # store the UNION of the two
   ```
   The IoU formula (`pin_bbox_iou`, `answer_eval.py:2158`):
   ```python
   ix = max(0.0, min(ax + aw, bx + bw) - max(ax, bx))   # width of the overlap rectangle
   iy = max(0.0, min(ay + ah, by + bh) - max(ay, by))   # height of the overlap rectangle
   inter = ix * iy                                       # overlap area
   union = aw * ah + bw * bh - inter                     # combined area (don't double-count overlap)
   return (inter / union) if union > 0 else 0.0
   ```
   If the two independent locates overlap by at least 35%, we trust them and store their **union**
   (`pin_bbox_union`, `answer_eval.py:2172`) — the smallest box covering both, so a slightly-off locate
   still accepts an honest tap. If they disagree, the item is thrown away (a target nobody can
   reliably find would mis-grade everyone).
4. A **difficulty gate** also drops trivially-easy nouns (`_PIN_TRIVIAL_NOUNS`, `bot_3.py:31898`,
   checked at `32176`).

So before an item is ever shown, it survived: object-present check ×2, an agreement check, a
non-degenerate clamp, and a triviality filter. The stored answer is a **union of two agreeing vision
guesses**, not a single guess.

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

### 1.6.4 Grading time — the layered verdict (backend)

`_grade_pin` (`answer_eval.py:2244`) runs the layers in a deliberate order:

```python
def _grade_pin(payload, raw_input):
    tap, article = _parse_pin_answer(raw_input)        # "x,y|article" → ((x,y), "der")   (:2134)
    if not tap: return False, ""
    req_article = str(payload.get("article") or "").strip().lower()
    ok_article = (not req_article) or check_quiz_freeform_deterministic(article, req_article)
    ok_tap = _pin_bbox_hit(payload, tap)               # LAYER 1: geometry (below)
    if not ok_tap and ok_article:                      # only worth a paid recheck if it'd FLIP the verdict
        ok_tap = _pin_vision_hit(payload, tap)         # LAYER 2: vision on a drawn mark
        if ok_tap:
            _spawn_pin_bbox_repair(payload)            # LAYER 3: self-heal a wrong box in the background
    if ok_tap and ok_article:
        return True, ""
    # ... else an HONEST per-half reason: which of {object, article} failed ...
```

**Layer 1 — the geometric hit-test** (`_pin_bbox_hit`, `answer_eval.py:2145`):

```python
bx, by, bw, bh = (float(v) for v in bbox)
x, y = tap
m = 0.06   # forgiving margin so a near-miss on a clear object still counts
return (bx - m) <= x <= (bx + bw + m) and (by - m) <= y <= (by + bh + m)
```

This is a point-in-box test on normalized coords, but with a **0.06 margin** added on all four sides
(a **tolerance**: a tap 6% of the image outside the box still counts, so a near-miss on an obvious
object isn't punished). `(bx - m) <= x <= (bx + bw + m)` is Python's chained comparison — "x is
between the left edge minus margin and the right edge plus margin", and the same for `y`.

**Layer 2 — the vision re-check, but only when it matters.** The stored bbox is still just two vision
guesses; for small objects it can be off. So on a **bbox miss where the article is correct** (i.e.
the tap is the *only* reason for a ❌), we spend one vision call to double-check. Crucially it does
**not** ask the model about numbers. `_pin_vision_hit` (`answer_eval.py:2224`) → `run_vision_point_check`
(`openai_manager.py:7162`): first `_mark_tap_on_image` (`openai_manager.py:7139`) draws a
white-haloed **red ring + crosshair** at `(x·w, y·h)` using PIL (Python Imaging Library), then asks
strict JSON `{"on_target": true|false}` — *"be strict about which object, forgiving about precision"*.
The rationale is in the code comment (`openai_manager.py:7139`): vision models read a **drawn marker**
far more reliably than raw coordinates. This layer runs **only on disputes**, so the OpenAI spend is
bounded.

**Layer 3 — self-healing (background).** If Layer 2 credits a tap that Layer 1 rejected, the stored
box was on the wrong object — so `_spawn_pin_bbox_repair` (`answer_eval.py:2214`) fires a background
thread (`threading.Thread(..., daemon=True)`) that **re-locates the object twice** and either stores
the new agreeing union or **retires** the item if the two locates disagree (`_pin_bbox_repair`,
`answer_eval.py:2187`). So a bad box fixes itself for the next learner instead of mis-grading everyone
forever.

**Honest failure reasons.** Instead of a bare ❌, it tells the learner *which half* failed — object
vs article (`answer_eval.py:2267`) — because with a possibly-imperfect box, "you tapped wrong" must be
truthful.

### 1.6.5 The layers, at a glance

| When | Layer | What it guards against |
| --- | --- | --- |
| pool | object present ×2 (`run_vision_locate`) | showing a picture that doesn't contain the target |
| pool | non-degenerate `[0,1]` clamp | a garbage/out-of-frame box |
| pool | IoU ≥ 0.35 agreement | two guesses pointing at different things |
| pool | store the **union** | a slightly-off single guess rejecting honest taps |
| pool | triviality gate | too-easy items |
| grade | bbox hit-test + 0.06 margin | precise-but-harsh rejection of near-misses |
| grade | vision point-check (on dispute only) | a wrong stored box making an item unwinnable |
| grade | background bbox repair / retire | a bad box mis-grading future learners |

# 🥷 2. Threats

- **T1 — Answer forgery / reading the answer.** Read the `/api/answer/task` response hoping it
  contains the correct answer (the bbox, the right option, the solution word), then submit that.
- **T2 — Coordinate forgery.** For `pin`, skip the UI and POST `answer:"0.5,0.5|der"` directly, or
  submit the exact box center — a script instead of a real tap.
- **T3 — Replay / brute force.** Submit the same task many times with different answers until one is
  marked correct.
- **T4 — IDOR.** Submit an answer for a `dispatch_id` that belongs to another user/chat, or load
  another user's task, to grade content you weren't given or corrupt their attempt.
- **T5 — Cost abuse via the vision re-check.** Deliberately near-miss on many `pin` tasks to force the
  Layer-2 OpenAI vision call repeatedly and run up our bill.
- **T6 — Prompt/vision injection.** Put text in the `target_label` or craft an image so the vision
  `locate`/`point-check` prompt is subverted into always saying "on_target".

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
- **T5 is bounded by design.** The vision re-check runs **only** on a bbox miss *and* a correct
  article (`answer_eval.py:2258`) — i.e. only on genuine disputes, one call each, and only for `pin`.
  A random spammer who also gets the article wrong never triggers it. (Still worth a per-user rate
  limit — see recommendations.)
- **T6 is contained by strict-JSON + narrow questions + temperature 0.** Both vision prompts demand
  a tiny fixed JSON shape (`{"present":…}` / `{"on_target":…}`), parse only that boolean, run at
  `temperature=0`, and default to the safe answer on any parse failure (`present=False` / `on_target`
  false → bbox verdict stands). The label is our own generated word, not free user input, which
  shrinks the injection surface further.

# 📈 4. Recommendations

1. **Rate-limit `pin` submissions per user** (a Redis token bucket) so T5 can't farm vision calls even
   with occasional correct articles.
2. **Add a server-side sanity check on submitted coords** (reject values outside `[0,1]` or malformed
   `"x,y"` before grading) — cheap, and it hardens T2's malformed-input variants.
3. **Assert in a test that `/api/answer/task` never includes `bbox`/`article`/`correct_*`** for any
   kind — a single parametrized pytest so a future refactor can't accidentally leak the answer (T1).
4. **Log disputes and repairs.** The Layer-2 recheck and Layer-3 repair already log; feed those into
   monitoring — a spike in "box was wrong" repairs flags a bad generation batch (quality) *or* an
   abuse pattern (cost).
5. **Cap `time_ms` / detect impossible speeds.** An answer submitted in a few milliseconds after the
   task loads is a script, not a human — useful signal for T3/T5.

# Self-check

1. Trace what happens when you tap an object, get a bbox **miss**, but your **article is right**.
   Which three functions run, in order, and what does each decide?
2. Why are two `run_vision_locate` calls made at pool time instead of one, and what does IoU ≥ 0.35
   guarantee about the stored box? What is stored — one box or a combination?
3. The vision re-check draws a red ring and asks `{"on_target": …}` instead of asking the model for
   the object's coordinates. Why is that more reliable (quote the code's reasoning)?
4. `/api/answer/task` deliberately omits one field that would let a cheater win instantly. Which
   field, and where is the answer actually revealed to the client?
5. `record_aufgabe_answer` uses `ON CONFLICT (dispatch_id, user_id) DO NOTHING`. What attack does that
   defeat, and what does the evaluator return on a second submit?

Last checked against the code: 2026-07-21 (line numbers re-verified against current source; repo is
edited concurrently — if a line is off by a few, grep the function name).
