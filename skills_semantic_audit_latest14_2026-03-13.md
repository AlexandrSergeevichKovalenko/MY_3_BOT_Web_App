# Skills Semantic Audit - latest 14-sentence rerun - 2026-03-13

## Scope

This audit uses the canonical production outputs of the full skills pipeline for the two newest 7-sentence sessions:

- fail-like session: `421868506490`
- recovery session: `580408720043`

Source of truth:

- `bt_3_sentence_skill_targets`
- `bt_3_skill_events_v2`
- `bt_3_skill_mastery_group_members`

## Important evaluation note

The benchmark outcome expects `clean_success` for all 7 cases, but the production rerun is a remediation loop:

- first pass is a fail-like pass
- second pass is a recovery pass from prior mistakes

So strict benchmark outcome comparison undercounts correctness. In remediation context, `recovered_final` is semantically correct for many recovery cases.

## Global metrics

Using the latest recovery profile for each of the 7 benchmark sentences:

- `primary_skill_accuracy` (strict semantic match to benchmark): `2/7 = 28.6%`
- `secondary_skill_overlap` (strict benchmark overlap on expected secondary slots): `1/14 = 7.1%`
- `outcome_classification_accuracy` vs benchmark `clean_success`:
  - strict: `0/7 = 0%`
  - context-adjusted for remediation semantics: `7/7 = 100%`

Interpretation:

- outcome semantics are mostly fine in remediation context
- tested-profile semantics are the main weak point

## Per-case audit

### Case 1

Sentence:

`Несмотря на нестабильную экономическую ситуацию...`

Expected:

- primary: `prepositions_governing_cases`
- secondary: `cases_dative`, `noun_phrases_complex`
- outcome: `clean_success`

Actual fail session:

- primary: `prepositions_usage`
- secondary: `de_cases_case_after_preposition`, `cases_accusative`
- errored: `prepositions_usage`, `de_cases_case_after_preposition`, plus untargeted article/case diagnostics
- outcome set: `clean_neutral`, `fail_new`, `untargeted_error_fail`

Actual recovery session:

- primary: `prepositions_usage`
- secondary: `de_cases_case_after_preposition`, `cases_accusative`
- errored: none
- outcome set: `clean_success`, `recovered_final`

Assessment:

- primary: match
- secondary: reasonable but benchmark overlap is weak
- error attribution: mostly plausible
- outcome semantics: correct in remediation context

Verdict: clearly correct

### Case 2

Sentence:

`Несмотря на отсутствие официальных заявлений, кажется, что...`

Expected:

- primary: `subordinate_clause_word_order`
- secondary: `impersonal_constructions`, `present_perfect_vs_present`
- outcome: `clean_success`

Actual fail session:

- primary: `de_articles_determiners_definite_articles_der_die_das`
- secondary: `adjectives_endings_general`, `word_order_subordinate_clause`
- errored: article definite, plus untargeted genitive, TMP word order, conjugation, verb placement
- outcome set: `clean_neutral`, `fail_new`, `untargeted_error_fail`

Actual recovery session:

- primary: `de_articles_determiners_definite_articles_der_die_das`
- secondary: `verbs_conjugation`, `verbs_auxiliaries`
- errored: none
- outcome set: `recovered_final`

Assessment:

- primary: mismatch
- secondary: mismatch
- false negatives: `dass`-subordinate structure and impersonal frame are not targeted as the core test
- false positives: article choice is over-promoted as primary

Verdict: likely incorrect

### Case 3

Sentence:

`Честно говоря, многие люди считают, что новости должны быть представлены так, чтобы...`

Expected:

- primary: `subordinate_clause_word_order`
- secondary: `modal_verbs_usage`, `relative_clause_structure`
- outcome: `clean_success`

Actual fail session:

- primary: `word_order_modal_structure`
- secondary: `word_order_subordinate_clause`, `verbs_auxiliaries`
- errored: subordinate placement diagnostic, modal-related signals
- outcome set: `clean_neutral`, `fail_new`, `untargeted_error_fail`

Actual recovery session:

- primary: `word_order_subordinate_clause`
- secondary: `verbs_modals`, `word_order_modal_structure`
- errored: none
- outcome set: `clean_success`, `recovered_final`

Assessment:

- primary: match on recovery
- secondary: one strong overlap (`modal_verbs_usage`)
- missing benchmark secondary `relative_clause_structure` is not a serious issue; the sentence is more nested-subordinate than relative-clause-driven
- outcome semantics: correct

Verdict: clearly correct

### Case 4

Sentence:

`Честно говоря, мне сложно понять, почему... работы ... остаются недооценёнными критиками.`

Expected:

- primary: `subordinate_clause_word_order`
- secondary: `passive_voice`, `participial_adjectives`
- outcome: `clean_success`

Actual fail session:

- primary: `de_articles_determiners_definite_articles_der_die_das`
- secondary: `adjectives_case_agreement`, `word_order_subordinate_clause`
- errored: article definite plus untargeted genitive, article omission, case agreement, capitalization
- outcome set: `clean_neutral`, `fail_new`, `untargeted_error_fail`

Actual recovery session:

- primary: `de_articles_determiners_definite_articles_der_die_das`
- secondary: `nouns_plural`, `cases_accusative`
- errored: untargeted noun capitalization only
- outcome set: `recovered_final`, `untargeted_error_fail`

Assessment:

- primary: mismatch
- secondary: mismatch
- false negatives: passive/result-state construction is not represented
- false positive risk: article-definiteness dominates a sentence whose main difficulty is clausal/passive structure

Verdict: likely incorrect

### Case 5

Sentence:

`Хотя казалось бы, что ... автор наконец-то достиг своей цели...`

Expected:

- primary: `concessive_clauses_word_order`
- secondary: `perfect_tense_usage`, `genitive_possession`
- outcome: `clean_success`

Actual fail session:

- primary: `de_articles_determiners_definite_articles_der_die_das`
- secondary: `adjectives_case_agreement`, `de_articles_determiners_indefinite_articles_ein_eine`
- errored: adjective agreement plus untargeted accusative, genitive, possessive pronouns, comma
- outcome set: `clean_neutral`, `fail_new`, `untargeted_error_fail`

Actual recovery session:

- primary: `adjectives_case_agreement`
- secondary: `de_articles_determiners_definite_articles_der_die_das`, `cases_accusative`
- errored: untargeted noun capitalization only
- outcome set: `recovered_final`, `untargeted_error_fail`

Assessment:

- primary: mismatch
- secondary: mismatch
- false negatives: concessive-clause structure is absent; genitive survives only as supporting/untargeted evidence
- tested profile is dominated by nominal/article residue rather than sentence-level syntax

Verdict: likely incorrect

### Case 6

Sentence:

`В условиях жёсткой конкуренции... предприятия вынуждены повышать ... и внедрять...`

Expected:

- primary: `infinitive_structures`
- secondary: `noun_compounds`, `modal_necessity_structures`
- outcome: `clean_success`

Actual fail session:

- primary: `adjectives_case_agreement`
- secondary: `prepositions_usage`, `de_articles_determiners_definite_articles_der_die_das`
- errored: adjective agreement, prepositions, plus untargeted noun compounds and main-clause order diagnostics
- outcome set: `clean_neutral`, `fail_new`, `untargeted_error_fail`

Actual recovery session:

- primary: `adjectives_case_agreement`
- secondary: `prepositions_usage`, `de_articles_determiners_definite_articles_der_die_das`
- errored: none
- outcome set: `clean_success`, `recovered_final`

Assessment:

- primary: mismatch
- secondary: mismatch
- important false negative: infinitive/necessity structure is not targeted
- useful signal exists in untargeted `nouns_compounds`, but it is not promoted into the tested profile

Verdict: likely incorrect

### Case 7

Sentence:

`Честно говоря, если бы не моя встреча с этим художником, я бы...`

Expected:

- primary: `conditional_konjunktiv_ii`
- secondary: `verb_prefix_usage`, `reflexive_verbs`
- outcome: `clean_success`

Actual fail session:

- primary: `de_articles_determiners_definite_articles_der_die_das`
- secondary: `de_clauses_sentence_types_conditionals_wenn_falls`, `nouns_declension`
- errored: untargeted dative and subordinate-clause order
- outcome set: `clean_neutral`, `untargeted_error_fail`

Actual recovery session:

- primary: `word_order_subordinate_clause`
- secondary: `de_articles_determiners_definite_articles_der_die_das`, `de_clauses_sentence_types_conditionals_wenn_falls`
- errored: untargeted infinitive-form diagnostic
- outcome set: `recovered_final`, `untargeted_error_fail`

Assessment:

- primary: mismatch
- secondary: partially reasonable because conditional structure appears, but Konjunktiv II still does not dominate
- false negative: counterfactual conditional should outrank generic subordinate-clause order

Verdict: questionable

## Clearly correct classifications

- Case 1 primary `prepositions_usage` is a good mastery-facing proxy for the `Несмотря на` / `trotz` construction.
- Case 1 recovery outcome is semantically correct.
- Case 3 recovery primary `word_order_subordinate_clause` is correct.
- Case 3 modal-related secondary signal is correct.
- Recovery outcomes across all 7 cases are semantically correct as `recovered_final` in a remediation setting.

## Questionable classifications

- Case 1 secondaries do not reflect the benchmark noun-phrase complexity well.
- Case 4 recovery still emits an untargeted capitalization diagnostic at score `96`; this may be too noisy.
- Case 7 surfaces the conditional structure only as secondary, not as primary.
- Several fail sessions mix genuine mastery leaves with untargeted diagnostics, making sentence-level interpretation less clean than it should be.

## Likely incorrect classifications

- Case 2 primary anchored on definite articles instead of `dass`-clause word order.
- Case 4 primary anchored on definite articles instead of clause/passive structure.
- Case 5 tested profile misses concessive syntax almost entirely.
- Case 6 tested profile misses infinitive/necessity structure and over-focuses on adjective/article residue.
- Case 7 misses Konjunktiv II as the main target.

## Overall verdict

`semantics needs another correction pass`

Reason:

- the mechanics are now healthy
- outcome semantics are mostly healthy
- but tested-profile selection is still too dominated by remediation-history residue
- sentence-construction semantics are underrepresented in at least 5 of the 7 benchmark cases

## Recommended next correction

Bias remediation profile generation toward sentence-construction anchors when the authored sentence clearly contains:

- concessive clause markers
- `dass` / `weil` / `obwohl` subordinate structures
- counterfactual `wenn ... hätte / wäre / würde`
- infinitive-governed necessity structures
- passive/result-state constructions

Keep article/adjective residue as secondary/supporting unless the sentence itself is primarily nominal/article-focused.
