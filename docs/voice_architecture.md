# Voice Domain Architecture

## Goal

Add a minimal voice domain foundation for scenario-based speaking practice without changing the current LiveKit runtime, endpoint contracts, or billing flow.

The v1 target is narrow:

- attach a prepared learning context to a voice session;
- persist transcript segments in the database;
- persist a structured post-session assessment;
- leave skill updates as a later bridge step.

## Responsibility Boundaries

The voice domain should own:

- scenario selection inputs and scenario metadata;
- preparation pack references for a session;
- transcript persistence;
- post-session assessment persistence;
- later conversion of assessment findings into skill updates.

The voice domain should not own:

- LiveKit room transport;
- WebRTC runtime behavior;
- telephony or PSTN calling;
- scheduler automation;
- frontend planning UX;
- replacement of existing translation skill pipelines.

## Why LiveKit Agent Stays Thin

The current LiveKit agent already works as a real-time execution layer. That is still the right place for:

- joining the room;
- handling audio interaction;
- calling low-level teaching tools;
- reacting during the live session.

It is not the right place to become the source of truth for pedagogy state. Scenario planning, transcript storage, assessment, and later skill bridging should live outside the runtime worker so they can be persisted, audited, and reused by future channels.

## V1 Entities

The minimal voice domain for v1 needs:

- `voice_scenarios`: reusable learning scenarios and prompts.
- `voice_prep_packs`: prepared context linked to a learner and optionally to a scenario.
- extended existing voice sessions: link a session to scenario/prep context and status.
- `voice_session_transcript_segments`: structured transcript storage.
- `voice_session_assessments`: structured post-call assessment storage.

## Happy Path

`prep pack exists -> user joins LiveKit session -> transcript stored -> assessment stored -> skill bridge later`

Practical flow:

1. A prep pack already exists for the learner.
2. The user joins a LiveKit session using the current runtime flow.
3. The session record is created in the existing voice session envelope.
4. Transcript segments are stored during or immediately after the session.
5. A structured assessment is stored after the session ends.
6. Skill bridge is deferred to a later phase and can read the stored assessment.

## What Is Deferred On Purpose

The following are intentionally out of scope for Phase 0 and v1 foundation work:

- telephony;
- outbound calling;
- scheduler automation;
- full analytics;
- UI planning screens.

## Fit With Current Project

This design keeps the current project stable:

- `backend/agent.py` remains the thin runtime worker.
- `backend/backend_server.py` keeps the current token/session endpoints.
- `backend/database.py` remains the place where future schema integration will happen.
- current translation and reminder flows stay untouched.
