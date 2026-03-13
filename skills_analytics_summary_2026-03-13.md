# Skills Analytics Summary - 2026-03-13

## Artifacts

- Railway logs: `railway_logs_skills_last90m.jsonl`
- Observability summary: `observability_summary_skills_last90m.json`

## Global skills-system status

- `bt_3_sentence_skill_targets`: 227 rows
- `bt_3_sentence_skill_shadow_state_v2`: 56 rows
- `bt_3_skill_events_v2`: 333 rows
- `bt_3_user_skill_state_v2`: 79 rows
- `bt_3_skill_state_v2_dirty`: 0 rows
- `bt_3_skill_state_v2_worker_stats`: 23 rows

## Recent 24h activity

- Events: 248
- Distinct users: 1
- Distinct sentences: 40
- Distinct skills: 60
- Tested events: 159
- Errored-now events: 131
- Avg `shadow_delta_signal`: -0.270
- All 24h events had `tested_profile_available = true`

### 24h distributions

- `profile_source`
  - `remediation_history`: 194
  - `authored_generation`: 54
- `per_skill_outcome`
  - `untargeted_error_fail`: 89
  - `clean_neutral`: 69
  - `fail_new`: 42
  - `recovered_final`: 26
  - `clean_success`: 22
- `sentence_progress_kind`
  - `first_attempt`: 205
  - `final_recovery`: 43
- `retention_state`
  - `repeat_fail_keep`: 142
  - `new_fail_store`: 53
  - `repeat_success_remove`: 43
  - `new_pass`: 10

## Recent 3h activity

- Events: 65
- Distinct users: 1
- Distinct sentences: 12
- Distinct skills: 25
- Tested events: 48
- Errored-now events: 28
- Avg `shadow_delta_signal`: -0.078

## Aggregator / dirty queue

- Dirty queue is empty: `dirty_count = 0`
- No retries, no leased rows
- Worker errors total: `0`
- Latest worker run: `2026-03-13T14:29:55.397651+00:00`
- Latest worker success: `2026-03-13T14:25:21.713675+00:00`
- Latest duration: `1484 ms`

## Latest Railway observability window (last 90m)

### Translation check

- One session observed
- 7 items processed
- `check_start_completed.duration_ms = 40120`
- `runner_start_delay_ms = 2530`
- Avg `per_item_duration_ms = 66671.14`
- Avg `db_update_duration_ms = 3566.20`
- `session_completion_duration_ms = 163075`
- Avg polling count: `13.49`
- Terminal outcome: success

### TTS

- 40 TTS samples
- 28 endpoint completions
- Avg endpoint duration: `1846.96 ms`
- P95 endpoint duration: `5983.25 ms`

## Latest user sessions for user `117649764`

### Session `341592401048`

- Time: `2026-03-13T14:05:05Z` to `2026-03-13T14:06:36Z`
- 44 skill events
- 7 sentences
- 24 distinct skills
- 28 tested events
- 27 errored-now events
- Avg `shadow_delta_signal = -0.412`
- Outcomes:
  - `clean_neutral`: 17
  - `untargeted_error_fail`: 16
  - `fail_new`: 11

### Session `250326362614`

- Time: `2026-03-13T14:22:57Z` to `2026-03-13T14:24:54Z`
- 21 skill events
- 5 sentences with skill events
- 13 distinct skills
- 20 tested events
- 1 errored-now event
- Avg `shadow_delta_signal = 0.622`
- Outcomes:
  - `recovered_final`: 18
  - `clean_success`: 2
  - `untargeted_error_fail`: 1

## Coverage anomaly in latest 7-sentence set

Latest session `250326362614` had 7 daily sentences, but only 5 produced `bt_3_skill_events_v2` rows.

Missing from skill-events coverage:

- `daily_sentence_id = 467`
  - `id_for_mistake_table = 146`
  - Sentence: "Несмотря на нестабильную экономическую ситуацию..."
- `daily_sentence_id = 472`
  - `id_for_mistake_table = 220`
  - Sentence: "В условиях жёсткой конкуренции на глобальном рынке..."

For both missing sentences:

- `bt_3_sentence_skill_shadow_state_v2` row exists
- `shadow_last_score` is high (`100` and `95`)
- `bt_3_sentence_skill_targets` count is `0`
- `bt_3_skill_events_v2` count is `0`

Previous-set counterparts did have remediation targets:

- Sentence `460` had 4 remediation targets
- Sentence `462` had 4 remediation targets

This isolates the current issue to target seeding / target carry-forward for repeated sentences, not to:

- dirty queue lag
- aggregation worker errors
- changed `id_for_mistake_table`
- failed shadow-state write

## Current weakest skills for user `117649764`

1. `verbs_placement_subordinate` - mastery `14.03`
2. `de_articles_determiners_article_omission_redundancy` - mastery `20.13`
3. `de_cases_case_after_preposition` - mastery `20.47`
4. `prepositions_usage` - mastery `20.72`
5. `de_infinitive_participles_zu_infinitive` - mastery `25.44`
6. `word_order_v2_rule` - mastery `30.11`
7. `verbs_modals` - mastery `31.96`
8. `de_punctuation_comma_in_subordinate_clause` - mastery `32.54`
9. `adjectives_case_agreement` - mastery `33.10`
10. `de_cases_case_agreement_in_noun_phrase` - mastery `33.36`
