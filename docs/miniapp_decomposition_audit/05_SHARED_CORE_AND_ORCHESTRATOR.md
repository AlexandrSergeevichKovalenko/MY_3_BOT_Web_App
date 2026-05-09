# Shared Core and Orchestrator

## Recommendation

The current main Mini App should remain as the temporary orchestrator shell during migration.

It already provides:

- authenticated bootstrapping
- global section navigation
- home/dashboard aggregation
- billing visibility
- support access
- transitions between learning domains

Removing that role too early would force duplication before shared contracts exist.

## What Should Remain Shared

### 1. Auth / Identity / Session Bootstrapping

Must remain shared:

- Telegram initData validation
- allowlist/access checks
- token exchange and web/mobile auth bootstrap
- webapp bootstrap / instance claim-release semantics

Reason:

- every future Mini App depends on the same Telegram user identity and trust model

### 2. Billing / Entitlement

Must remain shared:

- Stripe checkout / portal / webhook processing
- `plans`, `user_subscriptions`, `plan_limits`
- entitlement resolution
- route-level billing guard rules
- feature usage counters

Reason:

- duplicated billing logic across Mini Apps would create entitlement drift

### 3. Shared User Profile / Learning Context

Must remain shared:

- user language profile
- source/target language preferences
- cross-domain user context

Reason:

- dictionary, translations, reader, YouTube, TTS, and voice all depend on it

### 4. Analytics / Event Taxonomy

Must remain shared:

- analytics event semantics
- economics usage tracking
- group/private summary generation

Reason:

- cross-module comparisons require consistent event meaning

### 5. TTS Platform

Must remain shared:

- TTS generation service
- runtime state / prewarm / recovery / cleanup / quota control
- route semantics for URL/generate

Reason:

- TTS is a shared service, not a self-contained user module

### 6. Job Queue / Redis / Projection Infrastructure

Must remain shared:

- `backend/job_queue.py`
- async orchestration
- projection materialization
- hot-path/snapshot transient state

Reason:

- multiple domains already depend on the same async/control plane

### 7. Telegram Bot Integration

Must remain shared:

- reminders
- group summaries
- support messaging
- quiz callbacks
- access request flows

Reason:

- Telegram is a shared outer interface, not a single-module add-on

### 8. Shared Design / Navigation Contracts

Must remain shared:

- common UI language and tokens
- return/deep-link semantics between shell and module Mini Apps
- error/loading conventions

Reason:

- otherwise the product will fragment immediately

## What the Current Main App Should Do During Migration

### Remain responsible for:

- app bootstrap
- home shell
- today/skills/weekly-plan/analytics entry view
- billing and support entry view
- cross-module launcher/navigation
- fallback stable surface while other Mini Apps are introduced

### Avoid becoming:

- a duplicate copy of every extracted module forever

The shell should eventually become thinner, but only after extracted modules are stable and shared contracts are enforced.

## What Must Not Be Duplicated Across Mini Apps

- initData validation
- Stripe billing lifecycle
- entitlement resolution
- feature limit enforcement primitives
- user language profile resolution
- analytics taxonomy
- TTS platform implementation
- job queue definitions
- Telegram support/reminder orchestration

## Orchestrator Contract Needed Before Implementation

Before any split, define:

1. how a Mini App is launched from the shell
2. how a Mini App returns to the shell
3. how a module reports entitlement failure
4. how analytics events are namespaced
5. how shared user context is loaded once and reused safely
