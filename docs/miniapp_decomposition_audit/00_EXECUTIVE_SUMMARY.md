# Mini App Decomposition Audit: Executive Summary

## Scope

This audit covers the current Telegram bot + Telegram Mini App + backend + jobs ecosystem as implemented in the repository today. It is analysis only. No runtime behavior was changed as part of this step.

The current product is not a single-purpose Mini App. It is already a multi-domain learning shell that combines:

- translation sessions and translation checking
- vocabulary and dictionary workflows
- FSRS/cards/flashcards
- reader/document ingest/audio
- YouTube transcript and translation workflows
- today plan, skills, weekly plan, analytics
- TTS user flows and prewarm/recovery infrastructure
- live voice assistant
- image quiz and Telegram-native quiz flows
- Telegram chat reminders, summaries, and support
- billing, access control, subscriptions, and feature limits

## Top-Level Recommendation

The current main Mini App should remain active during migration and should act as the temporary orchestrator shell. It already owns cross-domain navigation, shared auth/initData bootstrapping, billing visibility, snapshot-backed home surfaces, and feature transitions between domains.

The safest decomposition direction is not to split the app by frontend pages first, but to split along domains that already have:

- clear user purpose
- reasonably bounded backend endpoints
- limited synchronous dependence on other screens
- manageable billing and analytics consequences

The best first extraction candidate is the **Translations Mini App**:

- it already has a distinct user journey
- it has a strong backend domain in `backend/translation_workflow.py`
- it already has async workers, session state, result side effects, and sentence-pool infrastructure
- it is commercially legible as a future module-specific subscription

The current home shell should continue to aggregate Today, Skills, Weekly Plan, billing, and cross-module entry points until shared platform contracts are stable.

## Main Findings

### 1. The current Mini App is already an orchestrator shell

`frontend/src/App.jsx` contains a broad multi-section shell, not a narrow domain app. The visible and guided sections include:

- subscription
- today
- translations
- YouTube
- movies/catalog
- dictionary
- reader
- flashcards
- assistant/voice practice
- support
- analytics
- skill training

This means the migration problem is architectural, not cosmetic. Splitting too early at the UI layer would duplicate shared app concerns unless platform boundaries are defined first.

### 2. Shared core concerns are substantial

The current system has several platform-level concerns that must not be duplicated across future Mini Apps:

- Telegram initData validation and authenticated bootstrapping
- allowlist/access control
- Stripe billing and entitlement resolution
- feature usage limits and guard rules
- shared user language profile
- analytics and economics tracking
- projection/snapshot infrastructure for home and progress surfaces
- async job queue and Redis-backed transient state
- TTS generation pipeline
- Telegram bot messaging, reminders, group summaries, private analytics, and support

### 3. Not every functional area is a good early Mini App candidate

Some domains are good product candidates but poor first extraction candidates because they are currently too cross-cutting:

- Today
- Skills
- Weekly plan
- Analytics
- Voice assistant

These areas depend heavily on shared state, plan generation, reminders, projections, aggregate metrics, and cross-domain data. Extracting them first would increase operational complexity before the shared platform is stabilized.

### 4. Billing is already partly feature-based, but not module-native

The billing model is still primarily plan/subscription-centric (`plans`, `user_subscriptions`), but it already uses `feature_code`-based limits through `plan_limits` and billing guard rules.

That makes modular subscriptions feasible, but only with explicit product and entitlement model changes. The current model is not yet a clean fit for module-specific catalog items such as:

- Words only
- Translations only
- All access

### 5. Telegram bot remains first-class during migration

The bot is not only a notification layer. It owns real user-facing product flows:

- access request flow
- translation commands
- dictionary interactions
- image quizzes
- flashcard/feel feedback
- reminders
- analytics digests
- group flows
- admin/budget operations affecting user experience

The bot must stay compatible with any future Mini App split.

## Top Risks

- Splitting the home shell too early would duplicate Today/Skills/Weekly Plan aggregation logic.
- Splitting billing or entitlement late would create inconsistent feature access across Mini Apps.
- Splitting voice/assistant too early would create high operational overhead with little immediate product isolation benefit.
- Treating Telegram flows as peripheral would break reminders, support, summaries, and quiz continuity.
- Creating separate Mini Apps before defining shared auth/navigation/entitlement contracts would produce drift and regressions.

## Top Opportunities

- A Translations Mini App can become an independently scalable product surface.
- Vocabulary + Flashcards can become a distinct retention-oriented learning product.
- Reader + YouTube can evolve into a content-consumption module if shared dictionary/TTS hooks remain shared.
- Feature-based billing groundwork already exists and can be evolved toward modular subscriptions.
- Projection-backed home surfaces can let the main shell remain thin while domains move out gradually.

## Proposed Next Step After This Analysis

Do not start by moving frontend files. First define and review:

1. shared platform contracts for auth, entitlement, analytics, navigation, and user context
2. domain ownership boundaries for Translations vs Words/Cards vs Content/Reader/YouTube
3. an entitlement model extension for module-level access without breaking current plans
4. a migration phase plan that keeps the current app as orchestrator shell until shared contracts are stable

## Strict Output Section

- **Should the current app remain during migration?**  
  Yes. It should remain the working orchestrator shell because it already owns cross-domain navigation, shared bootstrapping, billing visibility, snapshot-backed home surfaces, and handoffs between domains.

- **Best first Mini App candidate**  
  Translations.

- **Most dangerous premature split**  
  Today / home productivity shell.

- **Is modular subscription feasible?**  
  Yes with preconditions. The current billing system already has feature-limit primitives, but module-native entitlements, catalog mapping, and UX rules are not yet defined.

- **What must be audited before any code changes?**  
  - shared auth/initData/session contract  
  - shared entitlement and billing guard contract  
  - shared analytics event taxonomy  
  - cross-module navigation and return-path contract  
  - Telegram bot interactions that depend on each domain  
  - projection/snapshot ownership for Today, Skills, Weekly Plan, and session presence
