# Voice Schema Draft

This draft covers only the minimal v1 persistence layer for scenario-based voice learning.

It intentionally excludes:

- telephony tables;
- outbound call scheduling tables;
- full scenario state machines.

## `voice_scenarios`

### Purpose

Store reusable speaking scenarios that can be attached to prep packs and sessions.

### MUST fields for v1

- `id`
- `slug`
- `title`
- `description`
- `level`
- `topic`
- `target_skills_json`
- `system_prompt`
- `opening_brief`
- `version`
- `is_active`
- `created_at`
- `updated_at`

### NICE TO HAVE later

- separate scenario step storage;
- teacher policy JSON;
- localized display fields;
- archival metadata;
- scenario family / difficulty progression fields.

## `voice_prep_packs`

### Purpose

Store prepared voice-learning context for a learner before the session starts.

### MUST fields for v1

- `id`
- `user_id`
- `scenario_id` nullable
- `custom_topic_text` nullable
- `target_vocab_json`
- `target_expressions_json`
- `source_skill_ids_json`
- `status`
- `prepared_for_date` nullable
- `created_at`
- `updated_at`

### NICE TO HAVE later

- delivery status fields;
- Telegram message linkage;
- expiry / activation windows;
- richer CEFR targeting;
- prep source audit metadata.

## Existing voice sessions extension

### Purpose

Extend the current voice session envelope so each session can point to scenario/prep context and lifecycle status.

### MUST fields for v1

Existing table should gain nullable fields such as:

- `scenario_id`
- `prep_pack_id`
- `status`
- `ended_reason`
- `topic_mode`
- `custom_topic_text`

### NICE TO HAVE later

- session source classification;
- reminder linkage;
- planner linkage;
- teacher policy snapshot;
- client version snapshot.

## `voice_session_transcript_segments`

### Purpose

Persist structured transcript segments instead of relying on local debug files.

### MUST fields for v1

- `id`
- `session_id`
- `seq_no`
- `speaker`
- `text`
- `start_ms` nullable
- `end_ms` nullable
- `metadata_json` nullable
- `created_at`

### NICE TO HAVE later

- token / confidence fields;
- ASR source metadata;
- correction markers;
- segment-level rubric anchors;
- retention / redaction flags.

## `voice_session_assessments`

### Purpose

Persist a structured post-session assessment for later review and future skill bridging.

### MUST fields for v1

- `id`
- `session_id`
- `rubric_version`
- `summary`
- `strict_feedback`
- `scenario_completion_score`
- `target_vocab_used_json`
- `target_vocab_missed_json`
- `grammar_notes`
- `fluency_notes`
- `created_at`

### NICE TO HAVE later

- self-correction counts;
- teacher reveal counts;
- confidence scores;
- evidence pointers to transcript segments;
- speaking-dimension subscores.
