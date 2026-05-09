# Migration Safety and Sequence

## Safest Rollout Order

## Phase 1: Frontend-Only Home Composition Change

### Goal

Add a tile-based dashboard on the current home state while reusing existing section navigation.

### Safe because

- existing screens remain untouched
- existing endpoints remain untouched
- existing `selectedSections` navigation remains the source of truth

### Must not change in this phase

- section detail internals
- backend endpoints
- task/skills/weekly-plan business logic
- TTS logic
- bot flows
- entitlement logic

## Phase 2: Demote Existing Summary Blocks

### Goal

Keep weekly/today/skills on home, but in compact form.

### Safe because

- preserves current data contracts
- preserves snapshot loading
- changes mostly layout and visibility priority

### Risk

- accidentally breaking action buttons embedded in current summaries

## Phase 3: Reduce Hamburger to Secondary Navigation

### Goal

Move core destinations to tiles and leave hamburger for utilities.

### Safe because

- existing destinations still exist
- menu can remain as backup during rollout

### Risk

- hiding an item that product still expects users to find quickly

## What Must Not Be Touched in the First Pass

- `selectedSections` semantics
- back-swipe / section route history
- flashcards setup side effects on open
- YouTube back-section behavior
- local snapshot read/refresh sequencing
- language-profile/starter-dictionary gates

## Frontend-Only vs Backend-Touching

### Frontend-only in first pass

- header layout
- tile dashboard composition
- visibility ordering of summaries
- hamburger simplification

### Backend dependencies that must be respected

- today/skills/weekly-plan/plan-analytics payloads
- support unread polling
- bootstrap/language profile gating
- billing status/plans loading when subscription is opened

## Rollback Considerations

The safest rollout should preserve:

- the old section screens
- the old menu destinations
- the old data contracts

So rollback can be:

- reverting home/dashboard composition only
- without touching backend or section implementations

## Highest-Risk Failure Modes

- tile clicks that do not mirror current menu semantics
- broken section focus/back navigation
- support/billing becoming hard to reach
- compact summaries losing current action affordances
