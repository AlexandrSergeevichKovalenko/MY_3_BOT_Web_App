# Mini App Candidates

## Candidate 1: Translations Mini App

### Purpose

Own the full translation-learning loop:

- session start
- sentence delivery
- draft input
- async checking
- results/explanations/history
- story mode

### What goes inside

- translation session UI
- story mode
- translation history for that domain
- explain/check/fill progress handling
- translation recommendations within that domain

### What must stay shared

- auth/initData validation
- entitlement/billing guard
- shared language profile
- analytics event contract
- dictionary jump-out contract
- TTS service
- shared user identity and return navigation

### Why this is a good split

- strongest standalone user loop
- backend logic already clustered in `backend/translation_workflow.py`
- async worker model already explicit
- easiest future subscription story

### Why this is also risky

- active session continuity and story mode must remain exact
- the shell still needs to reflect translation-derived progress

### Scaling impact

- positive for backend scaling
- positive for deployment isolation
- positive for queue/worker observability

### UX impact

- good if launched from shell with clean return/deep-link behavior
- bad if dictionary, TTS, or history handoffs are duplicated incorrectly

### Migration risk

- medium

## Candidate 2: Words + Flashcards Mini App

### Purpose

Own vocabulary accumulation and repetition:

- dictionary
- saved words/folders
- flashcards/FSRS/quiz modes

### What goes inside

- lookup/save/folder management
- personal vocabulary library
- flashcard setup and training
- card enrichment

### What must stay shared

- auth/entitlement
- analytics
- pronunciation/TTS service
- bot reminder and quiz integration
- shared language profile

### Why this is a good split

- productically coherent
- retention-oriented
- closer to a module subscription than many other domains

### Why this is also risky

- current dictionary is used as a jump-out from translations, YouTube, and reader
- if split too aggressively, lookup friction increases across the entire product

### Scaling impact

- moderate positive for frontend modularity
- small-to-moderate positive for backend isolation

### Migration risk

- medium

## Candidate 3: Reader Mini App

### Purpose

Deep reading and document study:

- ingest
- library
- reading session
- audio

### Good split?

Eventually yes, but not first.

### Why not first

- strongly depends on shared dictionary/TTS/navigation
- likely benefits from a content-study suite with YouTube, not a solo first extraction

### Scaling impact

- moderate for file/media workflows

### Migration risk

- medium-high if extracted before shared content contracts exist

## Candidate 4: YouTube Study Mini App

### Purpose

Video-based study:

- transcript fetch
- manual transcript
- translation overlay
- catalog/search

### Good split?

Eventually yes, but not first.

### Why not first

- transcript jobs, quotas, catalog, dictionary, and today-plan recommendations are tightly entangled

### Scaling impact

- positive for media/transcript workload isolation

### Migration risk

- medium-high

## Candidate 5: Content Study Mini App (Reader + YouTube)

### Purpose

A unified content-consumption surface for:

- reading
- video transcript learning
- dictionary-assisted study
- audio-assisted study

### Good split?

Possibly better than splitting Reader and YouTube separately, but only after shared contracts are mature.

### Scaling impact

- good modular product fit
- moderate backend isolation gain

### Migration risk

- high if attempted early

## Candidate 6: Voice Practice Mini App

### Purpose

Speaking practice, scenarios, prep packs, assessments.

### Good split?

Late-stage candidate only.

### Why not early

- specialized runtime
- more operational complexity
- still tied to shared context, skills, and analytics

### Scaling impact

- may help runtime isolation later

### Migration risk

- high

## Candidate 7: Today / Skills / Planning Mini App

### Purpose

Home productivity and planning shell.

### Good split?

No as an early split.

### Why this is a bad first split

- aggregates data from almost every other domain
- depends on reminders, skills, weekly planning, and projections
- is already the best temporary orchestrator surface

### Scaling impact

- negative if split too early because it duplicates aggregation and navigation logic

### Migration risk

- very high

## Candidate 8: Analytics Mini App

### Good split?

Not early.

### Why

- analytics is cross-domain by definition
- it should consume domain outputs, not define decomposition boundaries

## Candidate 9: Billing Mini App

### Good split?

No.

Billing must remain shared core, even if a separate billing screen exists.

## Recommendation Summary

### Best candidates

1. Translations Mini App
2. Words + Flashcards Mini App

### Late candidates

- Reader / YouTube or a combined Content Study Mini App
- Voice Practice Mini App

### Poor early candidates

- Today / shell
- Skills / Weekly Plan
- Analytics
- Billing
