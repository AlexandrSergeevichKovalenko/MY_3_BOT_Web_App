# Migration Sequence

## Principle

Zero-downtime and low-risk order means:

- keep the current app working
- keep the current app visible
- do not remove existing feature paths before replacements are fully live
- avoid extracting aggregate/shared surfaces first

## Phase 0: Architecture Contracts

### Goal

Define shared contracts before moving product surfaces.

### Why this order

Without shared contracts, every extracted Mini App will duplicate auth, billing, analytics, and navigation logic.

### Dependencies

- shared auth contract
- entitlement contract
- analytics contract
- cross-module navigation contract
- shell launcher contract

### Risks

- under-specifying the contracts

### Rollback

- no runtime changes required yet

## Phase 1: Keep the Current App as Orchestrator Shell

### Goal

Stabilize the current shell as the migration control plane.

### Why this order

The shell already owns cross-domain access and home aggregation.

### Dependencies

- projection/home-surface stability
- clean module launch points

### Risks

- turning the shell into permanent technical debt if it is never thinned later

### Rollback

- no functional removal because shell remains primary

## Phase 2: Extract Translations as the First Mini App

### Goal

Create the first true domain Mini App around translations.

### Why this order

- strongest product boundary
- strongest backend boundary
- clear async infrastructure already exists
- high monetization clarity

### Dependencies

- shared auth/initData contract
- shared entitlement guard contract
- shared dictionary/TTS handoff contract
- active-session resume semantics

### Risks

- breaking session continuity
- breaking history or story mode

### Rollback

- shell keeps existing translation entry until module path is fully validated

## Phase 3: Extract Words + Flashcards

### Goal

Create a vocabulary retention module.

### Why this order

- good product coherence
- good subscription fit after translations

### Dependencies

- shared lookup/save contract
- card generation/review continuity
- bot reminder integration

### Risks

- degrading dictionary jump-outs from translations, reader, and YouTube

### Rollback

- shell keeps original lookup/training entry points until parity is proven

## Phase 4: Extract Content Study Surface

### Goal

Move Reader and YouTube into a content-study family, either together or in a carefully staged sequence.

### Why this order

Both depend on shared content-study behaviors and benefit from shared dictionary/TTS hooks.

### Dependencies

- transcript/library contracts
- content state persistence
- video/doc deep-link behavior

### Risks

- content-state fragmentation

### Rollback

- shell keeps content launchers and legacy flows

## Phase 5: Reassess Voice Practice as a Separate Runtime Surface

### Goal

Only after platform contracts are stable, decide whether voice becomes its own Mini App.

### Why this order

Voice adds specialized runtime complexity but is not the cleanest first commercial split.

### Dependencies

- stable shared auth
- stable shared analytics and skill-bridge semantics

### Risks

- high operational overhead

### Rollback

- keep voice in shell

## What Should Not Be Extracted First

### 1. Today / home shell

Why not:

- central orchestrator
- aggregate surface
- reminder-linked

### 2. Billing

Why not:

- must stay shared

### 3. Analytics

Why not:

- cross-domain by definition

### 4. Skills / Weekly Plan

Why not:

- aggregate and recommendation-heavy
- tightly connected to Today

## Safest First Extraction Candidate

- Translations

## Most Dangerous Premature Extraction

- Today / home shell
