# Toolbox — the test suite and the pre-push hook (tests run before every push)

Two things live here: **(1)** the git **pre-push hook** that runs our tests on every `git push` and
blocks the push if anything is red, and **(2)** a guided tour of the ~82 test files — how they work,
what libraries/methods they use, and what they guard. This is the deep companion to
[running_and_testing.md](running_and_testing.md) (which is the quick "how do I run a test" primer).

Status when this was written: `python3 -m pytest backend/tests -q` → **658 passed, 96 subtests
passed** (~137 s). It is green.

## 1. The pre-push hook — what it is and why it exists

A **git hook** is a script git runs automatically at a certain moment. A **pre-push** hook runs
*right before* `git push` sends your commits, and if it exits non-zero, **the push is aborted**. Ours
runs the whole test suite: red tests → the push doesn't leave your machine.

**Why we added it (the real story, from the hook's own comment):** the tests were never run
automatically anywhere, so over months **~40 tests went red and nobody noticed** — and a *genuine*
production bug (the FSRS repetition counters, §4.2) was drowning among those 40. The hook makes red
impossible to ignore: you literally can't push past it (without a conscious override). "40 tests" =
those 40 accumulated failures being driven back to zero; the hook runs the *entire* suite (658
tests), not just 40.

### 1.1 It lives only on your machine (important)

**Git does not store hooks in the repository.** Files under `.git/hooks/` are per-clone and never
committed or pulled. So the hook is present on the owner's machine (you saw its output on the last
push), but:
- a **fresh clone** won't have it,
- a **new worktree** made by `./agent-worktree.sh <name>` (see `CLAUDE.md` §2) won't have it.

To install it there, run **once**:

```zsh
./scripts/git-hooks/install.sh
```

`install.sh` (`scripts/git-hooks/install.sh`) simply copies the versioned source
`scripts/git-hooks/pre-push` into `.git/hooks/pre-push` and marks it executable:

```sh
HOOK_DIR=$(git rev-parse --git-path hooks)      # the .git/hooks dir for THIS clone/worktree
for hook in pre-push; do
    cp "$REPO_ROOT/scripts/git-hooks/$hook" "$HOOK_DIR/$hook"
    chmod +x "$HOOK_DIR/$hook"
done
```

So the **source of truth** (`scripts/git-hooks/pre-push`) *is* in the repo; the **active copy**
(`.git/hooks/pre-push`) is a per-machine install. That's the pattern for versioning a git hook. This
is documented in `CLAUDE.md` §3.1.

### 1.2 The hook script, line by line

`scripts/git-hooks/pre-push` (a POSIX `sh` script):

```sh
REPO_ROOT=$(git rev-parse --show-toplevel) || exit 0    # find repo root; if not a repo, do nothing
cd "$REPO_ROOT" || exit 0

# Tests must NOT touch the live DB: importing the app runs schema DDL, and a dev machine
# carries PRODUCTION credentials. This env var turns that startup DDL off.
export SKIP_STARTUP_SCHEMA_BOOTSTRAP=1

command -v python3  >/dev/null 2>&1 || { echo "python3 не найден — тесты пропущены"; exit 0; }
python3 -c "import pytest" 2>/dev/null || { echo "pytest не установлен — пропущены"; exit 0; }

echo "▶️  Прогоняю тесты перед пушем (около минуты)…"
OUTPUT=$(python3 -m pytest backend/tests -q 2>&1)       # ← run the WHOLE suite, capture output+status
STATUS=$?

if [ $STATUS -eq 0 ]; then
    echo "✅ Тесты зелёные — пушу."; exit 0             # all green → allow the push
fi

FAILURES=$(echo "$OUTPUT" | grep -E "^FAILED|^ERROR")   # the actual red test lines

# Smart bit: if there are NO real FAILED/ERROR lines but the output mentions a DB/network
# outage, the run didn't actually happen — don't block a push for a non-bug.
if [ -z "$FAILURES" ] && echo "$OUTPUT" | grep -qiE "OperationalError|could not connect|Connection refused"; then
    echo "⚠️  прогон не состоялся — внешний сервис недоступен. Пуш продолжается."; exit 0
fi

echo "$FAILURES" | head -n 20                            # show what's red
echo "⛔️ Пуш остановлен: тесты красные."
echo "   Запушить всё равно:   git push --no-verify"     # the escape hatch
exit 1                                                   # non-zero → git aborts the push
```

Four design decisions worth internalizing:
- **`SKIP_STARTUP_SCHEMA_BOOTSTRAP=1`** — merely *importing* `backend.backend_server` runs
  `ensure_*_schema()` DDL against whatever `DATABASE_URL` is set, and on a dev machine that's
  **production**. Without this flag, running tests would issue DDL on the live DB (wrong and flaky).
  Production never sets this var, so its startup is unchanged. (`conftest.py` sets the same var, §3.)
- **Graceful skip** if `python3`/`pytest` are missing — the hook never *blocks* a push over a missing
  toolchain, it just warns and lets it through.
- **Infra-flake vs real red** — a suite that couldn't connect to a DB/service is *not* a code bug;
  blocking on it would send you hunting a nonexistent breakage, so those runs are waved through.
- **`git push --no-verify`** — the conscious override. It skips *all* pre-push hooks. Legitimate for a
  **docs-only** change (like these learning files — they can't affect tests), or an emergency. Not for
  routine code pushes; that's how red accumulates again.

> This is exactly what happened while writing these docs: a docs-only push was blocked because the
> suite was momentarily red (unrelated to the `.md` edits), so it went out with `--no-verify`. The
> right call for docs; the wrong call for code.

## 2. How to run the tests yourself

Same command the hook runs (from the repo root):

```zsh
SKIP_STARTUP_SCHEMA_BOOTSTRAP=1 python3 -m pytest backend/tests -q          # the whole suite (~2 min)
python3 -m pytest backend/tests/test_fsrs_scheduler.py -v                    # one file, verbose
python3 -m pytest backend/tests -k "free_limit" -v                          # only tests matching a name
python3 -m pytest backend/tests/test_billing_economics.py::FooTests -x      # one class, stop on first fail
```

Flags recap (full list in [running_and_testing.md §3](running_and_testing.md)): `-q` quiet, `-v`
verbose, `-k` filter by name, `-x` stop at first failure, `::` select one class/test.

## 3. How our tests are built — methods & libraries

- **pytest** is the runner; it **discovers** tests automatically: any file `test_*.py`, any class
  `Test*`/`*Tests`, any function/method `test_*`. No registration needed.
- Most of our files use **`unittest.TestCase`** (Python's built-in test class) *run by pytest*. That
  gives you `setUp`/`tearDown` (run before/after each test) and `self.assertTrue/assertEqual/
  assertGreaterEqual/…` assertions. A failing assertion fails the test with a diff of actual vs
  expected.
- **`conftest.py`** (`backend/tests/conftest.py`) is pytest's special per-directory setup file, run
  **before any test imports the app**. Ours is tiny but critical — it sets the "don't touch prod DB"
  flag so no test can forget:
  ```python
  import os
  os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
  ```
- **`backend/tests/fixtures/`** holds reusable canned data/helpers shared across tests.
- **The golden rule: a test must be hermetic** — it must not hit the real database, OpenAI, Google,
  Telegram, or the network. Tests achieve this by (a) calling **pure functions** directly (the FSRS
  scheduler, the initData HMAC, normalization — no I/O), or (b) **monkeypatching** (temporarily
  swapping) an I/O function for a fake during the test. That's why the suite runs in ~2 minutes and is
  safe to run on a laptop with prod credentials.
- **subtests** (the "96 subtests passed" in the summary) are multiple assertions grouped under one
  test using `with self.subTest(...)`, so one test can check many cases and report each separately.
- The shape of almost every test is **AAA**: *Arrange* inputs → *Act* (call the function) → *Assert*
  the result. Reading the Arrange block tells you the function's inputs and shapes; the Assert block
  tells you its contract. Often clearer than reading the function itself.

## 4. Two tests dissected in full

### 4.1 `test_telegram_init_data_ttl.py` — a pure-function auth test (ties to [block 02](../security_deep_dives/02_telegram_auth_initdata.md))

This tests the exact functions block 02 documents — no DB, no mocks, just the HMAC + freshness logic.

```python
import time, unittest
import backend.backend_server as server            # import the module under test

class TelegramInitDataTtlTests(unittest.TestCase):
    def setUp(self):                                # runs before EACH test method
        self.original_token = server.TELEGRAM_Deutsch_BOT_TOKEN
        self.original_ttl = server.TELEGRAM_WEBAPP_INIT_TTL_SECONDS
        server.TELEGRAM_Deutsch_BOT_TOKEN = "test-bot-token"   # ← control the secret so we can sign test data
        server.TELEGRAM_WEBAPP_INIT_TTL_SECONDS = 60           # ← shrink the TTL to 60s for the test

    def tearDown(self):                             # runs after EACH test — restore the real values
        server.TELEGRAM_Deutsch_BOT_TOKEN = self.original_token
        server.TELEGRAM_WEBAPP_INIT_TTL_SECONDS = self.original_ttl

    def test_signed_init_data_is_valid_while_fresh(self):
        init_data = server._build_signed_init_data_for_user(       # the server MINTS a signed initData
            {"id": 123456, "first_name": "Test"}, auth_date=int(time.time()) - 30)  # dated 30s ago
        self.assertTrue(server._telegram_hash_is_valid(init_data)) # 30s < 60s TTL → valid

    def test_signed_init_data_is_rejected_after_ttl(self):
        init_data = server._build_signed_init_data_for_user(
            {"id": 123456, "first_name": "Test"}, auth_date=int(time.time()) - 120)  # dated 120s ago
        self.assertFalse(server._telegram_init_data_auth_date_is_fresh(init_data, max_age_seconds=60))
        self.assertFalse(server._telegram_hash_is_valid(init_data)) # 120s > 60s TTL → rejected
```

What it teaches: `setUp`/`tearDown` let a test **temporarily override module globals** (the bot token,
the TTL) and restore them, so tests are isolated. Because it uses the server's *own* mint function
`_build_signed_init_data_for_user`, it produces a genuinely-signed `initData` (block 02 §1.8), then
asserts the verifier accepts it while fresh and rejects it once past the TTL. This is the executable
proof of block 02's replay defense.

### 4.2 `test_fsrs_scheduler.py` — a regression guard for a real production bug

This is the flagship. FSRS is the spaced-repetition scheduler that decides when a flashcard is due.
The test file's `CounterOwnershipTests` class exists to guard one specific, expensive bug — read its
docstring:

```python
class CounterOwnershipTests(unittest.TestCase):
    """reps/lapses are OUR columns, counted from OUR stored state.
    They used to be read back off the fsrs Card, which quietly stopped carrying them —
    884 reviewed cards in production sat at reps=0, lapses=0 for months. These tests fail
    the moment the counters start depending on the library again."""

    def _chain(self, ratings):                       # helper: replay a sequence of answers
        state, result = None, None
        for rating in ratings:
            result, _ = schedule_review(current_state=state, rating=rating, reviewed_at=now)
            state = { ...carry reps/lapses/stability/... }   # feed each result back in as the next state
        return result

    def test_reps_count_every_answer(self):
        self.assertEqual(self._chain(["GOOD"]).reps, 1)
        self.assertEqual(self._chain(["GOOD","AGAIN","HARD","EASY"]).reps, 4)  # 4 answers → reps == 4

    def test_lapses_count_only_again(self):
        self.assertEqual(self._chain(["GOOD","HARD","EASY"]).lapses, 0)        # no AGAIN → 0 lapses
        self.assertEqual(self._chain(["GOOD","AGAIN","GOOD","AGAIN"]).lapses, 2)  # two AGAINs → 2

    def test_counters_continue_from_stored_state(self):
        """A card loaded from the DB between sessions — counting must RESUME, not restart."""
        result, _ = schedule_review(current_state={..., "reps": 17, "lapses": 4}, rating="AGAIN", ...)
        self.assertEqual(result.reps, 18)            # 17 → 18
        self.assertEqual(result.lapses, 5)           # 4 → 5
```

What it teaches:
- **A test can encode a bug's post-mortem.** The docstring records *what broke, how badly (884 cards,
  months), and why*. The test then pins the correct behavior so the bug can't return silently.
- **Testing an algorithm by chaining state.** `schedule_review` is a pure function `(current_state,
  rating, reviewed_at) → new_state`. The `_chain` helper feeds each output back as the next input, so
  a sequence of ratings can be checked (`["GOOD","AGAIN","HARD","EASY"] → reps == 4`). No DB needed —
  the "stored state" is just a dict, exactly as it comes from/goes to the DB.
- **Pinning product decisions.** `LearningStepsTests` asserts the learning steps are `[1 minute, 1
  day]` — not because 1 minute is "correct" in the abstract, but so nobody changes the review rhythm
  by accident; a deliberate change must also change the test.

## 5. The whole suite, mapped (82 files, by area)

You don't need every file dissected — with §3–4 you can open any one and read it. Here's what each
guards, grouped so you can find the tests for whatever you're studying. (Cross-references point at the
security deep-dive that covers the same feature.)

**Auth · entitlement · billing · access** (→ [02](../security_deep_dives/02_telegram_auth_initdata.md), [03](../security_deep_dives/03_payments.md))
- `test_telegram_init_data_ttl` — initData HMAC signing + TTL freshness (§4.1).
- `test_entitlement_bootstrap_paths`, `test_enforce_user_access_cache`, `test_self_serve_access` — how "is this user Pro/allowed" resolves and caches.
- `test_paid_surface_gates`, `test_settings_dictionary_tier`, `test_webapp_start_requires_level` — paid/level features are gated.
- `test_billing_economics`, `test_billing_subscription_sync`, `test_billing_attribution_guard`, `test_subscription_single_queue`, `test_admin_economics` — billing math, subscription sync, cost attribution.

**Free-tier limits** (the freemium caps, → [01 §3](../security_deep_dives/01_sentence_translation.md), [04](../security_deep_dives/04_shared_word_cache.md))
- `test_ask_gpt_free_limit`, `test_dictionary_lookup_free_limit`, `test_dictionary_save_free_limit`, `test_bot_dictionary_save_free_limit`, `test_flashcards_free_limit_ux`, `test_shortcut_forwarded_message_free_limit` — each verifies a daily cap blocks free users and Pro bypasses.

**Dictionary · words · units** (→ [04](../security_deep_dives/04_shared_word_cache.md), [04b](../security_deep_dives/04b_word_units_layer.md))
- `test_dictionary_edit_pair_alignment`, `test_dictionary_semantic_folders`, `test_cache_invalidation_feed`, `test_starter_dictionary_disconnect`, `test_vocabulary_bulk_delete`, `test_private_dictionary_batch_fast_button` — dictionary editing, folders, cache busting, bulk ops.
- `test_german_form_index`, `test_german_surface`, `test_headword_not_lemmatized` — word-form/surface handling in the units layer.

**FSRS · SRS · today · progress**
- `test_fsrs_scheduler` — the spaced-repetition scheduler + the counter-ownership regression guard (§4.2).
- `test_srs_manual_queue`, `test_today_cards_progress_sync`, `test_today_translation_level_override`, `test_plan_progress_metrics`, `test_daily_interactive_activity`, `test_global_rotation` — the daily-plan/progress machinery.

**Interactive games / quizzes** (→ [05](../security_deep_dives/05_interactive_exercises.md))
- `test_image_quiz_prompting`, `test_image_quiz_utils`, `test_visual_riddle_validation` — image-quiz generation/validation.
- `test_article_game_plural_gate`, `test_plural_article_gates`, `test_artikel_homographs_and_wofrage_animacy`, `test_aufgabe_hoerluecke_difficulty`, `test_aufgabe_wortgruppe`, `test_wofrage_generator_quality`, `test_wofrage_item_telemetry`, `test_quiz_freeform_navigation`, `test_quiz_poll_stats`, `test_group_champion_untimed`, `test_rebus_schema_migrations`, `test_sentence_gpt_seed_generation` — per-game content quality + grading.

**Sentence translation-check** (→ [01](../security_deep_dives/01_sentence_translation.md))
- `test_translation_check_queue_first`, `test_translation_check_stale_cleanup`, `test_translation_check_worker_schedule`, `test_translation_session_accounting`, `test_translation_sentence_selection`, `test_translation_focus_pool_refill`, `test_translation_focus_pool_admin_report` — the async grading pipeline, the "6 of 7" durability, sentence selection.

**Reader · audio · TTS**
- `test_reader_async_ingest`, `test_reader_pdf_normalization`, `test_reader_tts_ssml` — ingest + text/SSML shaping.
- `test_reader_audio_page_job_queue`, `test_reader_audio_prefetch`, `test_reader_audio_premium_gate`, `test_reader_audio_singleflight`, `test_tts_voice_defaults` — audio jobs, gating, dedupe (single-flight), voices.

**Shortcut · autosave** (→ `autosave_scaling_explained.md`)
- `test_shortcut_autosave_staging`, `test_shortcut_batch_and_chunking`, `test_shortcut_lookup_split`, `test_shortcut_install_token_cache`, `test_shortcut_rate_limits_cleanup`, `test_shortcut_onboarding_copy` — the iPhone-Shortcut import pipeline.
- `test_bot_autosave_digest`, `test_bot_inplace_save`, `test_bot_pending_stale_cleanup`, `test_bot_rubezh2_garbage_drop` — bot-side save + the anti-garbage filters.

**Numdict · listening · voice**
- `test_numdict_practice`, `test_listening_evaluator`, `test_voice_assessment_quality`, `test_voice_skill_bridge_service` — number-dictation, listening grading, LiveKit voice.

**Infra · scheduler · load**
- `test_agent_worker_schedule`, `test_service_resource_schedule`, `test_translation_check_worker_schedule` — periodic job schedules.
- `test_pgbouncer_rollout`, `test_db_pool_alert`, `test_phase1_snapshot_lazy_ensure` — DB pool/PgBouncer + lazy schema.
- `test_synthetic_load_dispatch`, `test_synthetic_load_infra` — the synthetic load-test harness.

**Prompting · analytics · misc**
- `test_openai_manager_prompting` — prompt construction/shaping.
- `test_analytics_scoring`, `test_plan_progress_metrics`, `test_flashcard_feel_keyboard`, `test_user_return_notice`, `test_weekly_youtube_recommendation_gate`, `test_youtube_transcript_admin_library` — scoring, UI keyboard, re-engagement, YouTube gating.

## 6. Self-check

1. Where does the active pre-push hook live, why isn't it in the repo, and what one command installs
   it in a fresh worktree?
2. Why does the hook (and `conftest.py`) set `SKIP_STARTUP_SCHEMA_BOOTSTRAP=1`? What bad thing happens
   on a dev machine without it?
3. The hook treats "5 tests FAILED" and "couldn't connect to the DB" differently. What does it do in
   each case, and why is that fair?
4. In `test_fsrs_scheduler.py`, `_chain(["GOOD","AGAIN","HARD","EASY"]).reps == 4`. Explain how the
   helper produces that, and what real production bug the whole `CounterOwnershipTests` class exists
   to catch.
5. When is `git push --no-verify` the right call, and when is it how "40 red tests" come back?

Last checked against the code: 2026-07-30 (hook + conftest + two tests read verbatim; suite green at
658 passed. Repo edited concurrently — grep a test name if a path drifts).
