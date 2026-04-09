# Voice Migration Plan

This is a migration draft for the new voice domain foundation. It is not executable SQL.

## Likely Additions

### New tables

- `voice_scenarios`
- `voice_prep_packs`
- `voice_session_transcript_segments`
- `voice_session_assessments`

### Existing table changes

Extend the existing voice session table with nullable fields for:

- `scenario_id`
- `prep_pack_id`
- `status`
- `ended_reason`
- `topic_mode`
- `custom_topic_text`

## Recommended Migration Order

1. Add `voice_scenarios`.
2. Add `voice_prep_packs`.
3. Extend the existing voice session table with nullable columns only.
4. Add `voice_session_transcript_segments`.
5. Add `voice_session_assessments`.
6. Only after that, wire service-layer reads and writes.

## Existing Tables That Must Not Be Broken

- current voice session envelope and billing usage paths;
- translation and skill tables in `backend/database.py`;
- existing Telegram/reminder tables;
- current authentication and assistant session tracking tables.

## Risky Changes

- changing existing voice session semantics instead of extending them;
- adding non-null fields too early;
- coupling transcript persistence to runtime assumptions before retention/privacy rules are settled;
- mixing voice skill evidence into the current translation mastery flow too early;
- attempting to redesign existing billing/session accounting during schema rollout.

## What To Do First

- create additive tables only;
- keep new columns nullable;
- keep foreign keys simple and optional where needed for early rollout;
- introduce service-layer wrappers before integrating runtime writes.

## What To Do Later

- stricter constraints after the runtime flow is proven;
- skill-bridge persistence tables, if still needed after v1;
- planner/scheduler-specific linkage;
- retention, privacy, and redaction policies;
- richer scenario versioning and state progression.
