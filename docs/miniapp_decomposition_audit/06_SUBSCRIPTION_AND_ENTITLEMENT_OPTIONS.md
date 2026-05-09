# Subscription and Entitlement Options

## Current Model

### What exists today

Observed in `backend/database.py` and `backend/backend_server.py`:

- `plans`
- `user_subscriptions`
- `plan_limits`
- `resolve_entitlement(...)`
- `enforce_feature_limit(...)`
- route-level billing guard rules

### Current reality

The system is primarily:

- plan-based for commercial state (`free`, `trial`, `pro`)
- feature-limit-based for some usage enforcement

Examples of feature codes already present:

- `translation_daily_sets`
- `feel_word_daily`
- `skill_training_daily`
- `youtube_fetch_daily`
- `tts_chars_daily`

### Important implication

The codebase already supports differentiated feature limits, but not a true module catalog or module-native entitlement model.

## Is Modular Subscription Feasible?

Yes, with preconditions.

## Conceptual Future Model

### Option A: Module entitlements layered on top of plans

Keep:

- global user subscription record

Add conceptually:

- module entitlement catalog
- plan-to-module grants
- explicit all-access mapping

Example:

- Free plan -> shell + limited translations + limited words
- Words plan -> dictionary + flashcards
- Translations plan -> translation sessions/checks/history
- All access -> all modules

### Option B: Bundle-first catalog

Plans become product bundles:

- Starter
- Translations
- Words
- Content
- All access

This is viable, but only if the shell and billing UI can explain bundle scope clearly.

## What Would Need to Change Conceptually

### 1. Entitlement model

Need a concept beyond `feature_code` daily limits:

- module access grant
- module-specific visibility in UI
- module-specific checkout/upsell

### 2. Billing guard semantics

Current guards answer:

- may this route consume this feature today?

Future guards must also answer:

- does this user own this module at all?

### 3. UX and navigation

Need explicit behavior for:

- user opens module not included in plan
- user opens shell but only owns one module
- upsell to all-access from inside a module

### 4. Trial logic

Current trial is global-ish in entitlement behavior.

Need product decision:

- one global trial?
- per-module trial?
- shell-wide limited trial plus module upsell?

### 5. Analytics

Need module-aware analytics segmentation:

- module acquisition
- module retention
- module upgrade funnels
- all-access conversion

### 6. Daily quotas

Current feature limits already exist.

Need rule clarity:

- are quotas shared across modules?
- is TTS a global quota or module-local quota?
- is skill training global if it appears inside Today and Skills?

## Global/Shared Features That Complicate Modular Subscription

These features cut across multiple modules:

- Today plan
- weekly plan
- analytics
- TTS
- support
- Telegram reminders/summaries

These should not be treated as naive “extra modules” without explicit product rules.

## Recommended Conceptual Direction

Use a hybrid model:

- keep one shared account/subscription identity
- add module grants as first-class entitlement concepts
- keep all-access as a top-level bundle
- keep shell visibility and upsell centralized

## Risks

- inconsistent access rules between shell and modules
- duplicated upsell logic
- confusion when one module depends on shared features like TTS or analytics
- trial behavior becoming unintelligible if done per module too early

## Migration Implication

Do not implement module subscriptions before:

1. defining Mini App/module boundaries
2. defining shared-core vs module-owned features
3. deciding what Today/home should display for partial-access users
