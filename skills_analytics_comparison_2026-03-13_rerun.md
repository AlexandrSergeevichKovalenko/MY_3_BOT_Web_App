# Skills Analytics Comparison - 2026-03-13 rerun

## Artifacts

- Logs: `railway_logs_skills_rerun_150m.jsonl`
- Observability: `observability_summary_skills_rerun_150m.json`
- Previous baseline summary: `skills_analytics_summary_2026-03-13.md`

## Before vs after

### Previous problematic pair

- Fail-like session: `341592401048`
  - `7` sentences
  - `44` skill events
  - `avg_shadow_delta = -0.412`
- Recovery session: `250326362614`
  - `7` daily sentences, but only `5` sentences in `bt_3_skill_events_v2`
  - `21` skill events
  - Missing coverage for repeated remediation sentences:
    - `id_for_mistake_table = 146`
    - `id_for_mistake_table = 220`

### New rerun pair

- Fail-like session: `421868506490`
  - `7` sentences in `bt_3_skill_events_v2`
  - `50` skill events
  - `28` tested events
  - `30` errored-now events
  - `avg_shadow_delta = -0.417`
  - Outcomes:
    - `untargeted_error_fail`: 22
    - `clean_neutral`: 20
    - `fail_new`: 8

- Recovery session: `580408720043`
  - `7` sentences in `bt_3_skill_events_v2`
  - `31` skill events
  - `28` tested events
  - `3` errored-now events
  - `avg_shadow_delta = 0.553`
  - Outcomes:
    - `recovered_final`: 25
    - `clean_success`: 3
    - `untargeted_error_fail`: 3

## Key result

The previous symptom is not present in the rerun:

- old recovery session `250326362614`: `5 / 7` sentences reached `bt_3_skill_events_v2`
- new recovery session `580408720043`: `7 / 7` sentences reached `bt_3_skill_events_v2`

## Per-sentence coverage in rerun

### Session `421868506490`

All 7 daily sentences have:

- `targets_count = 4`
- `skill_event_rows > 0`
- `shadow_exists = true`

Notable repeated remediation sentences:

- `sentence_id = 474`, `id_for_mistake_table = 146`
  - `targets_count = 4`
  - `skill_event_rows = 6`
- `sentence_id = 475`, `id_for_mistake_table = 220`
  - `targets_count = 4`
  - `skill_event_rows = 7`

### Session `580408720043`

All 7 daily sentences have:

- `targets_count = 4`
- `skill_event_rows > 0`
- `shadow_exists = true`

The two previously missing remediation IDs now have full coverage:

- `sentence_id = 481`, `id_for_mistake_table = 146`
  - `targets_count = 4`
  - `skill_event_rows = 4`
  - `overall_score = 99`
  - Outcomes: `clean_success, recovered_final`
- `sentence_id = 486`, `id_for_mistake_table = 220`
  - `targets_count = 4`
  - `skill_event_rows = 4`
  - `overall_score = 95`
  - Outcomes: `clean_success, recovered_final`

## Current platform health after rerun

- `bt_3_skill_state_v2_dirty = 0`
- No worker errors
- Latest worker run: `2026-03-13T15:15:59.539848+00:00`
- Latest worker success: `2026-03-13T15:08:51.793323+00:00`

Current global counts:

- `bt_3_sentence_skill_targets = 283`
- `bt_3_sentence_skill_shadow_state_v2 = 70`
- `bt_3_skill_events_v2 = 414`
- `bt_3_user_skill_state_v2 = 82`

## Observability window

From the last `150m` Railway window:

- `translation_check` sessions observed: `2`
- `item_processed` events: `14`
- terminal outcomes: `2 success`, `0 partial`, `0 error`
- avg `check_start_completed.duration_ms = 4522.50`
- avg `per_item_duration_ms = 72388.14`
- avg `db_update_duration_ms = 4133.25`

## Conclusion

The concrete production symptom that motivated the fix is gone in the latest rerun:

- repeated remediation sentences now receive `sentence_skill_targets`
- all 14 new sentences produced `shadow_state`
- all 14 new sentences produced `skill_events_v2`
- the aggregator drained the dirty queue back to zero without errors
