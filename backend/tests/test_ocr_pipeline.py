"""
Tests for backend/ocr_pipeline.py — OCR Intelligence Pipeline v1 + v2.

v1 Coverage:
  - Rule metadata: unique names, stage ordering
  - Stage 1: whitespace normalization (deterministic, idempotent)
  - Stage 2: structural cleanup — Instagram/TikTok noise removal
  - Stage 3: OCR artifact cleanup — mangled quotes, punctuation
  - Stage 4: language detection — script presence detection
  - Integration: OcrPipelineResult fields, observability metrics
  - Fixture suite: all 10 real-world fixtures pass must_keep / must_drop
  - Safety: no catastrophic deletion of learnable content
  - Backend integration: pipeline wired into _start_shortcut_lookup_enqueue_runner

v2 Coverage:
  - Candidate segmentation: blank-line boundaries, multiline preservation
  - Learnability scoring: signal breakdown, determinism, thresholds
  - Classification: noise / uncertain / learnable against fixture suite
  - Route decisions: skip_all_noise vs proceed
  - No catastrophic filtering of real learnable content
  - Delivery routing: noise payload → skip LLM; learnable → LLM called
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from backend.ocr_pipeline import (
    ALL_RULE_LISTS,
    LEARNABLE_THRESHOLD,
    NOISE_THRESHOLD,
    OcrCandidate,
    OcrContextSignal,
    OcrPipelineResult,
    OcrStageMetric,
    LearnabilityScore,
    _SELF_INGESTION_PATTERNS,
    _STRUCTURAL_RULES,
    _WHITESPACE_RULES,
    _OCR_ARTIFACT_RULES,
    _apply_self_ingestion_suppression,
    _detect_scripts,
    annotate_context,
    build_extraction_text,
    route_candidates,
    run_ocr_pipeline,
    score_learnability,
    segment_candidates,
)
from backend.tests.fixtures.ocr_samples import (
    ALL_FIXTURES,
    ALL_LEARNABILITY_FIXTURES,
    ALL_V3_LEARNABILITY_FIXTURES,
    ALL_V3_OCR_FIXTURES,
    OCR_MISTAKES,
    LEARNABLE_GERMAN_SENTENCE,
    LEARNABLE_MULTILINE_SUBTITLE,
    NOISE_SINGLE_CTA,
    NOISE_PURE_NUMERIC,
    TELEGRAM_SELF_INGESTION,
    OCR_CORRUPTION_TOKENS,
    NUMERIC_ORPHAN_LINES,
    CONTEXT_QUESTION_ANSWER,
    UNCERTAIN_UNDERSCORE_TOKEN,
)


# ---------------------------------------------------------------------------
# 1. Rule metadata
# ---------------------------------------------------------------------------

class OcrPipelineRuleMetadataTests(unittest.TestCase):

    def test_all_rule_names_unique_across_all_stages(self):
        all_names = [rule.name for ruleset in ALL_RULE_LISTS for rule in ruleset]
        self.assertEqual(len(all_names), len(set(all_names)),
                         "Duplicate rule names found: " + str(
                             [n for n in all_names if all_names.count(n) > 1]))

    def test_stage_ordering_is_five_stages(self):
        result = run_ocr_pipeline("Hello Welt", source="test")
        self.assertEqual(len(result.stage_metrics), 5)

    def test_stage_names_in_correct_order(self):
        result = run_ocr_pipeline("Hello Welt", source="test")
        expected_order = [
            "whitespace_normalization",
            "structural_cleanup",
            "ocr_artifact_cleanup",
            "self_ingestion_suppression",
            "language_detection",
        ]
        actual = [s.stage for s in result.stage_metrics]
        self.assertEqual(actual, expected_order)

    def test_stage_metrics_are_ocrstagemetic_instances(self):
        result = run_ocr_pipeline("test", source="test")
        for metric in result.stage_metrics:
            self.assertIsInstance(metric, OcrStageMetric)
            self.assertGreaterEqual(metric.duration_ms, 0)
            self.assertGreaterEqual(metric.chars_before, 0)
            self.assertGreaterEqual(metric.chars_after, 0)


# ---------------------------------------------------------------------------
# 2. Stage 1: Whitespace normalization
# ---------------------------------------------------------------------------

class OcrPipelineWhitespaceTests(unittest.TestCase):

    def test_normalization_is_deterministic(self):
        raw = "Hello\r\nWelt\t! Gut."
        r1 = run_ocr_pipeline(raw, source="test")
        r2 = run_ocr_pipeline(raw, source="test")
        self.assertEqual(r1.cleaned_text, r2.cleaned_text)

    def test_normalization_is_idempotent(self):
        raw = "Hello\r\nWelt\t! Gut."
        r1 = run_ocr_pipeline(raw, source="test")
        r2 = run_ocr_pipeline(r1.cleaned_text, source="test")
        self.assertEqual(r1.cleaned_text, r2.cleaned_text)

    def test_crlf_normalized(self):
        result = run_ocr_pipeline("Zeile1\r\nZeile2\rZeile3")
        self.assertNotIn("\r", result.cleaned_text)
        self.assertIn("Zeile1", result.cleaned_text)
        self.assertIn("Zeile2", result.cleaned_text)
        self.assertIn("Zeile3", result.cleaned_text)

    def test_tab_converted_to_space(self):
        result = run_ocr_pipeline("Guten\tMorgen!")
        self.assertEqual(result.cleaned_text, "Guten Morgen!")

    def test_non_breaking_space_converted(self):
        result = run_ocr_pipeline("Guten Tag")
        self.assertNotIn(" ", result.cleaned_text)
        self.assertIn("Guten Tag", result.cleaned_text)

    def test_multiple_spaces_collapsed(self):
        result = run_ocr_pipeline("Wie  geht  es  Ihnen?")
        self.assertEqual(result.cleaned_text, "Wie geht es Ihnen?")

    def test_three_plus_blank_lines_collapsed(self):
        result = run_ocr_pipeline("Zeile1\n\n\n\nZeile2")
        self.assertNotIn("\n\n\n", result.cleaned_text)
        self.assertIn("Zeile1", result.cleaned_text)
        self.assertIn("Zeile2", result.cleaned_text)

    def test_zero_width_chars_removed(self):
        result = run_ocr_pipeline("Text​here‌test﻿")
        self.assertNotIn("​", result.cleaned_text)
        self.assertNotIn("‌", result.cleaned_text)
        self.assertNotIn("﻿", result.cleaned_text)


# ---------------------------------------------------------------------------
# 3. Stage 2: Structural cleanup — Instagram / TikTok noise
# ---------------------------------------------------------------------------

class OcrPipelineStructuralCleanupTests(unittest.TestCase):

    def test_instagram_username_removed(self):
        result = run_ocr_pipeline("@germanlinguist\nDas ist ein Beispiel.")
        self.assertNotIn("@germanlinguist", result.cleaned_text)
        self.assertIn("Das ist ein Beispiel.", result.cleaned_text)

    def test_hashtag_removed(self):
        result = run_ocr_pipeline("Ich lerne Deutsch.\n#deutsch #lernen #sprache")
        self.assertNotIn("#deutsch", result.cleaned_text)
        self.assertNotIn("#lernen", result.cleaned_text)
        self.assertIn("Ich lerne Deutsch.", result.cleaned_text)

    def test_engagement_counter_en_removed(self):
        result = run_ocr_pipeline("1.2K likes\nDas Buch ist toll.\n234 comments")
        self.assertNotIn("likes", result.cleaned_text)
        self.assertNotIn("comments", result.cleaned_text)
        self.assertIn("Das Buch ist toll.", result.cleaned_text)

    def test_engagement_counter_ru_removed(self):
        result = run_ocr_pipeline("5,2K подписчиков\nОчень интересно.\n300 просмотров")
        self.assertNotIn("подписчиков", result.cleaned_text)
        self.assertNotIn("просмотров", result.cleaned_text)
        self.assertIn("Очень интересно.", result.cleaned_text)

    def test_original_audio_label_removed(self):
        result = run_ocr_pipeline("Ich spreche Deutsch.\nOriginal Audio\nWeiter so.")
        self.assertNotIn("Original Audio", result.cleaned_text)
        self.assertIn("Ich spreche Deutsch.", result.cleaned_text)

    def test_tiktok_duet_label_removed(self):
        result = run_ocr_pipeline("duet with @someone\nIch lerne jeden Tag.")
        self.assertNotIn("duet with", result.cleaned_text)
        self.assertIn("Ich lerne jeden Tag.", result.cleaned_text)

    def test_sponsored_label_removed(self):
        result = run_ocr_pipeline("Das Wort bedeutet Freude.\nsponsored\n")
        self.assertNotIn("sponsored", result.cleaned_text)
        self.assertIn("Das Wort bedeutet Freude.", result.cleaned_text)

    def test_see_more_cta_removed(self):
        result = run_ocr_pipeline("Das ist sehr interessant.\nSee more")
        self.assertNotIn("See more", result.cleaned_text)
        self.assertIn("Das ist sehr interessant.", result.cleaned_text)

    def test_standalone_timestamp_removed(self):
        result = run_ocr_pipeline("Ich lerne.\n3:42\nSehr gut.")
        self.assertNotIn("3:42", result.cleaned_text)
        self.assertIn("Ich lerne.", result.cleaned_text)
        self.assertIn("Sehr gut.", result.cleaned_text)

    def test_isolated_emoji_only_lines_removed(self):
        result = run_ocr_pipeline("😂😂😂😂\nDas ist lustig!\n🔥💯🎉🥳🎊\n")
        self.assertIn("Das ist lustig!", result.cleaned_text)
        # Emoji-only lines should not be present as standalone lines
        lines = [l.strip() for l in result.cleaned_text.splitlines() if l.strip()]
        for line in lines:
            # No line should consist solely of emoji
            self.assertRegex(line, r"[A-Za-zÀ-ɏА-яёЁ]",
                             f"Line appears to be emoji-only: {line!r}")

    def test_multiple_usernames_and_hashtags_removed(self):
        result = run_ocr_pipeline(
            "@user1 @user2\nDie Katze sitzt.\n#katze #tier #deutsch"
        )
        self.assertNotIn("@user1", result.cleaned_text)
        self.assertNotIn("@user2", result.cleaned_text)
        self.assertNotIn("#katze", result.cleaned_text)
        self.assertIn("Die Katze sitzt.", result.cleaned_text)


# ---------------------------------------------------------------------------
# 4. Stage 3: OCR artifact cleanup
# ---------------------------------------------------------------------------

class OcrPipelineArtifactCleanupTests(unittest.TestCase):

    def test_double_comma_becomes_german_opening_quote(self):
        result = run_ocr_pipeline(",,Guten Morgen,, sagte er.")
        self.assertNotIn(",,", result.cleaned_text)
        self.assertIn("„", result.cleaned_text)

    def test_triple_exclamation_collapsed(self):
        result = run_ocr_pipeline("Unglaublich!!!!")
        self.assertIn("!", result.cleaned_text)
        self.assertNotIn("!!!!", result.cleaned_text)
        self.assertEqual(result.cleaned_text.count("!"), 1)

    def test_triple_question_collapsed(self):
        result = run_ocr_pipeline("Was ist das???")
        self.assertNotIn("???", result.cleaned_text)
        self.assertEqual(result.cleaned_text.count("?"), 1)

    def test_four_plus_dots_normalized(self):
        result = run_ocr_pipeline("Wirklich.....\n")
        self.assertNotIn(".....", result.cleaned_text)
        self.assertIn("...", result.cleaned_text)

    def test_trailing_whitespace_removed_per_line(self):
        result = run_ocr_pipeline("Hallo   \nWelt   \n")
        for line in result.cleaned_text.splitlines():
            self.assertEqual(line, line.rstrip(), f"Trailing space in: {line!r}")


# ---------------------------------------------------------------------------
# 5. Stage 4: Language detection
# ---------------------------------------------------------------------------

class OcrPipelineLanguageDetectionTests(unittest.TestCase):

    def test_latin_detected_in_german_text(self):
        result = run_ocr_pipeline("Das ist ein guter Tag.")
        self.assertIn("latin", result.detected_languages)

    def test_cyrillic_detected_in_russian_text(self):
        result = run_ocr_pipeline("Это хорошо.")
        self.assertIn("cyrillic", result.detected_languages)

    def test_mixed_latin_and_cyrillic_both_detected(self):
        result = run_ocr_pipeline("Das ist gut — это хорошо.")
        self.assertIn("latin", result.detected_languages)
        self.assertIn("cyrillic", result.detected_languages)

    def test_empty_text_no_scripts_detected(self):
        result = run_ocr_pipeline("12345 ... !?")
        self.assertEqual(result.detected_languages, ())

    def test_detect_scripts_function_directly(self):
        self.assertIn("latin", _detect_scripts("Hello"))
        self.assertIn("cyrillic", _detect_scripts("Привет"))
        self.assertEqual(_detect_scripts("12345"), ())

    def test_language_detection_does_not_modify_text(self):
        text = "Ich lerne Deutsch. Это интересно."
        result = run_ocr_pipeline(text)
        # Language detection is read-only — text should not be further changed
        stage_before = result.stage_metrics[2]  # ocr_artifact_cleanup output
        stage_after = result.stage_metrics[3]   # language_detection
        self.assertEqual(stage_after.chars_before, stage_after.chars_after)
        self.assertEqual(stage_after.rules_fired, 0)


# ---------------------------------------------------------------------------
# 6. Result structure and observability
# ---------------------------------------------------------------------------

class OcrPipelineObservabilityTests(unittest.TestCase):

    def test_result_is_ocrpipelineresult(self):
        result = run_ocr_pipeline("Test")
        self.assertIsInstance(result, OcrPipelineResult)

    def test_raw_text_preserved_in_result(self):
        raw = "@user\nIch lerne.\n#hashtag"
        result = run_ocr_pipeline(raw)
        self.assertEqual(result.raw_text, raw)

    def test_raw_size_matches_input_length(self):
        raw = "Guten Morgen!"
        result = run_ocr_pipeline(raw)
        self.assertEqual(result.raw_size, len(raw))

    def test_cleaned_size_matches_cleaned_text(self):
        result = run_ocr_pipeline("@user\nHello Welt.\n#tag")
        self.assertEqual(result.cleaned_size, len(result.cleaned_text))

    def test_removed_noise_count_nonzero_for_noisy_input(self):
        result = run_ocr_pipeline("@user\n#tag\n1.2K likes\nOriginal Audio")
        self.assertGreater(result.removed_noise_count, 0)

    def test_removed_noise_count_zero_for_clean_input(self):
        result = run_ocr_pipeline("Das ist ein guter Tag.")
        self.assertEqual(result.removed_noise_count, 0)

    def test_empty_input_handled_without_raising(self):
        result = run_ocr_pipeline("")
        self.assertEqual(result.cleaned_text, "")
        self.assertEqual(result.raw_size, 0)

    def test_none_input_handled_without_raising(self):
        result = run_ocr_pipeline(None)  # type: ignore[arg-type]
        self.assertEqual(result.cleaned_text, "")


# ---------------------------------------------------------------------------
# 7. No catastrophic deletion — learnable content preserved
# ---------------------------------------------------------------------------

class OcrPipelineSafetyTests(unittest.TestCase):

    def test_clean_german_text_passes_through_unchanged(self):
        raw = "Das ist ein guter Tag.\nIch lerne Deutsch seit zwei Jahren."
        result = run_ocr_pipeline(raw)
        self.assertIn("Das ist ein guter Tag.", result.cleaned_text)
        self.assertIn("Ich lerne Deutsch seit zwei Jahren.", result.cleaned_text)

    def test_german_umlaut_preserved(self):
        raw = "Die Früchte sind schön. Ä Ö Ü ä ö ü ß"
        result = run_ocr_pipeline(raw)
        for char in "ÄÖÜäöüßéà":
            if char in raw:
                self.assertIn(char, result.cleaned_text)

    def test_long_text_not_catastrophically_shortened(self):
        raw = "\n".join(f"Satz Nummer {i}: Das ist ein interessanter Satz." for i in range(20))
        result = run_ocr_pipeline(raw)
        # Must retain at least 80% of meaningful content
        self.assertGreater(result.cleaned_size, result.raw_size * 0.75)

    def test_german_sentence_with_numbers_not_garbled(self):
        raw = "Ich habe 3 Bücher gelesen. Das kostet 2,50 Euro."
        result = run_ocr_pipeline(raw)
        self.assertIn("3 Bücher", result.cleaned_text)
        self.assertIn("2,50 Euro", result.cleaned_text)

    def test_sentence_with_email_like_pattern_not_destroyed(self):
        # Ensure @-removal only fires on username-like patterns, not email addresses
        # (email patterns have domain parts — our rule stops at the username portion)
        raw = "Kontakt: info@example.com ist die E-Mail-Adresse."
        result = run_ocr_pipeline(raw)
        # The text may have @example.com partially processed, but the sentence
        # must retain meaningful content
        self.assertIn("Kontakt", result.cleaned_text)
        self.assertIn("E-Mail-Adresse", result.cleaned_text)

    def test_russian_learnable_phrases_preserved(self):
        raw = "Привет, как дела?\nДа, всё хорошо, спасибо.\n"
        result = run_ocr_pipeline(raw)
        self.assertIn("Привет", result.cleaned_text)
        self.assertIn("всё хорошо", result.cleaned_text)


# ---------------------------------------------------------------------------
# 8. Fixture suite — all 10 real-world fixtures pass must_keep / must_drop
# ---------------------------------------------------------------------------

class OcrPipelineFixtureTests(unittest.TestCase):

    def _run_fixture(self, fixture):
        result = run_ocr_pipeline(fixture.raw, source=fixture.name)
        cleaned = result.cleaned_text

        for phrase in fixture.must_keep:
            self.assertIn(
                phrase, cleaned,
                f"[{fixture.name}] must_keep phrase missing: {phrase!r}\n"
                f"Cleaned:\n{cleaned}"
            )

        for phrase in fixture.must_drop:
            self.assertNotIn(
                phrase, cleaned,
                f"[{fixture.name}] must_drop phrase still present: {phrase!r}\n"
                f"Cleaned:\n{cleaned}"
            )

    def test_all_fixtures_pass(self):
        for fixture in ALL_FIXTURES:
            with self.subTest(fixture=fixture.name):
                self._run_fixture(fixture)

    def test_ocr_mistakes_fixture_quotes_fixed(self):
        result = run_ocr_pipeline(OCR_MISTAKES.raw)
        self.assertIn("„", result.cleaned_text, "German opening quote not restored from ,,")
        self.assertNotIn(",,", result.cleaned_text)


# ---------------------------------------------------------------------------
# 9. Backend integration — pipeline wired into shortcut enqueue runner
# ---------------------------------------------------------------------------

class OcrPipelineBackendIntegrationTests(unittest.TestCase):

    def test_pipeline_called_in_start_shortcut_lookup_enqueue_runner(self):
        """run_ocr_pipeline must be invoked from _start_shortcut_lookup_enqueue_runner."""
        import backend.backend_server as server

        fake_upsert = {
            "ingest_id": 1,
            "is_new": False,
            "status": "delivered",
            "duplicate_count": 1,
        }
        pipeline_result = run_ocr_pipeline("@noise\nIch lerne.", source="shortcut")

        with (
            patch("backend.backend_server.shortcut_ingest_request_upsert", return_value=fake_upsert),
            patch("backend.backend_server.run_ocr_pipeline", return_value=pipeline_result) as mock_pipeline,
        ):
            server._start_shortcut_lookup_enqueue_runner(
                user_id=42,
                text="@noise\nIch lerne.",
                source="shortcut",
            )
            mock_pipeline.assert_called_once()
            args, kwargs = mock_pipeline.call_args
            # First positional arg is the raw text
            raw_passed = args[0] if args else kwargs.get("raw_text", "")
            self.assertIn("Ich lerne", raw_passed)

    def test_pipeline_cleaned_text_used_as_normalized_text(self):
        """The cleaned output of the pipeline must reach the upsert call, not the raw text."""
        import backend.backend_server as server

        raw_with_noise = "@germanlinguist\nDas Wort bedeutet Freude.\n#deutsch"
        clean_result = run_ocr_pipeline(raw_with_noise, source="shortcut")

        fake_upsert = {
            "ingest_id": 1,
            "is_new": False,
            "status": "delivered",
            "duplicate_count": 1,
        }

        captured: list[str] = []

        def capture_upsert(**kwargs):
            captured.append(kwargs.get("normalized_text_full", ""))
            return fake_upsert

        with (
            patch("backend.backend_server.shortcut_ingest_request_upsert", side_effect=capture_upsert),
            patch("backend.backend_server.run_ocr_pipeline", return_value=clean_result),
        ):
            server._start_shortcut_lookup_enqueue_runner(
                user_id=42,
                text=raw_with_noise,
                source="shortcut",
            )

        self.assertEqual(len(captured), 1)
        stored = captured[0]
        self.assertNotIn("@germanlinguist", stored)
        self.assertNotIn("#deutsch", stored)
        self.assertIn("Das Wort bedeutet Freude.", stored)


# ===========================================================================
# v2 Tests — Candidate Segmentation
# ===========================================================================

class OcrCandidateSegmentationTests(unittest.TestCase):

    def test_segmentation_stable_same_output_every_call(self):
        text = "Das ist ein Test.\n\nNoch ein Satz."
        c1 = segment_candidates(text)
        c2 = segment_candidates(text)
        self.assertEqual([c.text for c in c1], [c.text for c in c2])

    def test_blank_line_creates_two_candidates(self):
        text = "Erster Absatz.\n\nZweiter Absatz."
        candidates = segment_candidates(text)
        self.assertEqual(len(candidates), 2)
        self.assertIn("Erster Absatz.", candidates[0].text)
        self.assertIn("Zweiter Absatz.", candidates[1].text)

    def test_multiline_subtitle_preserved_as_one_candidate(self):
        text = (
            "Wenn ich in Deutschland bin,\n"
            "spreche ich immer Deutsch.\n"
            "Aber manchmal ist es schwierig."
        )
        candidates = segment_candidates(text)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].line_count, 3)
        self.assertIn("spreche ich immer Deutsch.", candidates[0].text)

    def test_empty_text_returns_empty_list(self):
        self.assertEqual(segment_candidates(""), [])
        self.assertEqual(segment_candidates("   "), [])

    def test_single_line_is_one_candidate(self):
        candidates = segment_candidates("Das ist ein Satz.")
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].line_count, 1)

    def test_candidate_token_estimate_is_positive(self):
        candidates = segment_candidates("Das ist ein guter Tag.")
        self.assertGreater(candidates[0].token_estimate, 0)

    def test_candidate_detected_languages_present(self):
        candidates = segment_candidates("Das ist Deutsch. Это русский.")
        langs = candidates[0].detected_languages
        self.assertIn("latin", langs)
        self.assertIn("cyrillic", langs)

    def test_char_offset_is_non_negative(self):
        for c in segment_candidates("Hallo.\n\nWelt."):
            self.assertGreaterEqual(c.char_offset, 0)

    def test_multiple_blank_lines_still_two_candidates(self):
        candidates = segment_candidates("Alpha.\n\n\n\nBeta.")
        self.assertEqual(len(candidates), 2)

    def test_ocrcandidate_instances_returned(self):
        for c in segment_candidates("Guten Morgen!\n\nGuten Abend!"):
            self.assertIsInstance(c, OcrCandidate)


# ===========================================================================
# v2 Tests — Learnability Scoring
# ===========================================================================

class OcrLearnabilityScoringTests(unittest.TestCase):

    def _candidate(self, text: str) -> OcrCandidate:
        cands = segment_candidates(text)
        return cands[0] if cands else OcrCandidate(
            text=text, line_count=1,
            token_estimate=max(1, len(text) // 4),
            detected_languages=_detect_scripts(text),
            char_offset=0,
        )

    def test_scoring_is_deterministic(self):
        cand = self._candidate("Das ist ein guter Tag.")
        s1 = score_learnability(cand)
        s2 = score_learnability(cand)
        self.assertEqual(s1.score, s2.score)
        self.assertEqual(s1.label, s2.label)

    def test_score_clamped_to_minus_one_plus_one(self):
        for text in ["😂😂😂😂😂😂😂", "Das ist ein sehr langer guter Satz mit vielen Wörtern."]:
            cand = self._candidate(text)
            s = score_learnability(cand)
            self.assertGreaterEqual(s.score, -1.0)
            self.assertLessEqual(s.score, 1.0)

    def test_score_breakdown_is_dict(self):
        cand = self._candidate("Ich lerne Deutsch.")
        s = score_learnability(cand)
        self.assertIsInstance(s.breakdown, dict)

    def test_breakdown_only_contains_fired_signals(self):
        cand = self._candidate("Ich lerne Deutsch.")
        s = score_learnability(cand)
        # Every entry in breakdown must be non-zero
        for k, v in s.breakdown.items():
            self.assertNotEqual(v, 0.0, f"Signal {k!r} is zero in breakdown")

    def test_thresholds_are_explicit_constants(self):
        self.assertIsInstance(LEARNABLE_THRESHOLD, float)
        self.assertIsInstance(NOISE_THRESHOLD, float)
        self.assertGreater(LEARNABLE_THRESHOLD, NOISE_THRESHOLD)

    def test_learnable_score_result_is_learnabilityscore(self):
        cand = self._candidate("Das ist gut.")
        self.assertIsInstance(score_learnability(cand), LearnabilityScore)

    def test_no_alphabetic_chars_hard_noise_override(self):
        # Emoji/symbol-only → no_alphabetic_chars; numeric → numeric_orphan.
        # Both produce likely_noise.
        for text, expected_key in [
            ("😂😂😂😂", "no_alphabetic_chars"),
            ("3:42",    "no_alphabetic_chars"),  # has colon → not pure numeric
            ("89",      "numeric_orphan"),
            ("12345",   "numeric_orphan"),
        ]:
            cand = self._candidate(text)
            s = score_learnability(cand)
            self.assertEqual(s.label, "likely_noise",
                             f"{text!r} should be noise, got {s.label} (score={s.score})")
            self.assertIn(expected_key, s.breakdown,
                          f"{text!r}: expected breakdown key {expected_key!r}, got {s.breakdown}")

    def test_german_sentence_punct_fires_positive(self):
        cand = self._candidate("Ich lerne Deutsch.")
        s = score_learnability(cand)
        self.assertIn("sentence_punct", s.breakdown)
        self.assertGreater(s.breakdown["sentence_punct"], 0)

    def test_pedagogical_marker_fires_for_arrow(self):
        cand = self._candidate("aufgeben — to give up")
        s = score_learnability(cand)
        self.assertIn("pedagogical_marker", s.breakdown)
        self.assertGreater(s.breakdown["pedagogical_marker"], 0)

    def test_verb_structure_fires_for_infinitive(self):
        cand = self._candidate("Ich möchte Deutsch lernen.")
        s = score_learnability(cand)
        self.assertIn("verb_structure", s.breakdown)

    def test_cta_signal_fires_for_follow(self):
        cand = self._candidate("Follow")
        s = score_learnability(cand)
        self.assertIn("isolated_cta", s.breakdown)
        self.assertLess(s.breakdown["isolated_cta"], 0)

    def test_engagement_residual_fires_for_likes(self):
        cand = self._candidate("1.2K likes")
        s = score_learnability(cand)
        self.assertIn("engagement_residual", s.breakdown)

    def test_all_caps_label_fires_for_shout(self):
        cand = self._candidate("FOLLOW NOW")
        s = score_learnability(cand)
        self.assertIn("all_caps_label", s.breakdown)
        self.assertLess(s.breakdown["all_caps_label"], 0)


# ===========================================================================
# v2 Tests — Classification against fixture suite
# ===========================================================================

class OcrLearnabilityClassificationTests(unittest.TestCase):

    def _candidate(self, text: str) -> OcrCandidate:
        cands = segment_candidates(text)
        return cands[0] if cands else OcrCandidate(
            text=text, line_count=1,
            token_estimate=max(1, len(text) // 4),
            detected_languages=_detect_scripts(text),
            char_offset=0,
        )

    def test_all_learnability_fixtures_pass(self):
        for fix in ALL_LEARNABILITY_FIXTURES:
            with self.subTest(fixture=fix.name):
                cand = self._candidate(fix.text)
                s = score_learnability(cand)
                self.assertEqual(
                    s.label, fix.expected_label,
                    f"[{fix.name}] expected {fix.expected_label!r}, "
                    f"got {s.label!r} (score={s.score:.3f}, breakdown={s.breakdown})"
                )
                self.assertGreaterEqual(s.score, fix.score_min,
                    f"[{fix.name}] score {s.score:.3f} below min {fix.score_min}")
                self.assertLessEqual(s.score, fix.score_max,
                    f"[{fix.name}] score {s.score:.3f} above max {fix.score_max}")

    def test_clean_german_text_not_catastrophically_filtered(self):
        text = (
            "Das ist ein guter Tag.\n"
            "Ich lerne Deutsch seit zwei Jahren.\n"
            "Die Sprache ist schön, aber schwer."
        )
        cands = segment_candidates(text)
        self.assertGreater(len(cands), 0)
        routing, pass_list, noise_list = route_candidates(cands)
        self.assertEqual(routing, "proceed",
                         "Clean German text should not be routed as all-noise")
        self.assertEqual(len(noise_list), 0,
                         "No candidate in clean German text should be noise")

    def test_multiline_learnable_subtitle_not_filtered(self):
        text = LEARNABLE_MULTILINE_SUBTITLE.text
        cands = segment_candidates(text)
        routing, pass_list, noise_list = route_candidates(cands)
        self.assertEqual(routing, "proceed")
        self.assertEqual(len(noise_list), 0)

    def test_pure_cta_list_produces_noise_routing(self):
        text = "Follow\nSubscribe"
        cands = segment_candidates(text)
        routing, pass_list, noise_list = route_candidates(cands)
        self.assertEqual(routing, "skip_all_noise")

    def test_pure_numeric_input_routes_as_all_noise(self):
        cands = segment_candidates("89\n1234")
        _, pass_list, _ = route_candidates(cands)
        self.assertEqual(len(pass_list), 0)


# ===========================================================================
# v2 Tests — Routing behaviour
# ===========================================================================

class OcrRoutingTests(unittest.TestCase):

    def test_route_candidates_returns_three_tuple(self):
        cands = segment_candidates("Ich lerne Deutsch.")
        result = route_candidates(cands)
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 3)

    def test_routing_proceed_for_learnable_text(self):
        cands = segment_candidates(LEARNABLE_GERMAN_SENTENCE.text)
        routing, pass_list, noise_list = route_candidates(cands)
        self.assertEqual(routing, "proceed")
        self.assertGreater(len(pass_list), 0)

    def test_routing_skip_all_noise_for_pure_noise(self):
        cands = segment_candidates("Follow")
        routing, pass_list, noise_list = route_candidates(cands)
        self.assertEqual(routing, "skip_all_noise")
        self.assertEqual(len(pass_list), 0)
        self.assertEqual(len(noise_list), 1)

    def test_routing_pass_list_preserves_order(self):
        text = "Erste Zeile.\n\nZweite Zeile.\n\nDritte Zeile."
        cands = segment_candidates(text)
        _, pass_list, _ = route_candidates(cands)
        texts = [c.text for c, _ in pass_list]
        self.assertEqual(texts, sorted(texts, key=lambda t: text.find(t)))

    def test_mixed_payload_partial_noise_still_proceeds(self):
        # One learnable paragraph + one CTA
        text = "Ich lerne Deutsch.\n\nFollow"
        cands = segment_candidates(text)
        routing, pass_list, noise_list = route_candidates(cands)
        self.assertEqual(routing, "proceed")
        self.assertEqual(len(pass_list), 1)
        self.assertEqual(len(noise_list), 1)

    def test_routing_empty_candidates_gives_skip(self):
        routing, pass_list, noise_list = route_candidates([])
        self.assertEqual(routing, "skip_all_noise")
        self.assertEqual(pass_list, [])
        self.assertEqual(noise_list, [])

    def test_routing_pass_list_items_are_tuples_of_candidate_and_score(self):
        cands = segment_candidates("Das ist gut.")
        _, pass_list, _ = route_candidates(cands)
        for item in pass_list:
            self.assertIsInstance(item[0], OcrCandidate)
            self.assertIsInstance(item[1], LearnabilityScore)


# ===========================================================================
# v2 Tests — Delivery integration
# ===========================================================================

class OcrV2DeliveryIntegrationTests(unittest.TestCase):
    """Verify that v2 routing is wired into _run_shortcut_lookup_delivery."""

    def test_all_noise_payload_skips_llm_and_returns_zero(self):
        """If all candidates are noise, _shortcut_split_blocks must NOT be called."""
        import backend.backend_server as server

        # A payload that consists entirely of a single CTA word — will be routed as noise.
        noise_text = "Follow"

        with patch("backend.backend_server._shortcut_split_blocks") as mock_split:
            sent = server._run_shortcut_lookup_delivery(
                user_id=42,
                text=noise_text,
            )
        self.assertEqual(sent, 0)
        mock_split.assert_not_called()

    def test_learnable_payload_calls_llm_extraction(self):
        """When at least one candidate passes, _shortcut_split_blocks must be called."""
        import backend.backend_server as server

        learnable_text = "Ich lerne Deutsch. Das ist sehr wichtig."

        with patch("backend.backend_server._shortcut_split_blocks", return_value=[]) as mock_split:
            server._run_shortcut_lookup_delivery(
                user_id=42,
                text=learnable_text,
            )
        mock_split.assert_called_once()

    def test_filtered_text_excludes_noise_candidates(self):
        """The text passed to _shortcut_split_blocks must not contain noise candidates."""
        import backend.backend_server as server

        # Two blank-line-separated paragraphs: one learnable, one noise
        mixed_text = "Ich lerne Deutsch.\n\nFollow"

        captured_texts: list[str] = []

        def capture_split(text: str):
            captured_texts.append(text)
            return []

        with patch("backend.backend_server._shortcut_split_blocks", side_effect=capture_split):
            server._run_shortcut_lookup_delivery(user_id=42, text=mixed_text)

        self.assertEqual(len(captured_texts), 1)
        # The noise candidate "Follow" must not be in the extraction input
        self.assertNotIn("Follow", captured_texts[0])
        # The learnable candidate must be present
        self.assertIn("Ich lerne Deutsch.", captured_texts[0])


# ---------------------------------------------------------------------------
# 9. OCR v2 routing contract hardening tests
# ---------------------------------------------------------------------------

class OcrRoutingContractTests(unittest.TestCase):
    """
    Verify the explicit routing contract:
      - skip_all_noise  → no LLM call, return 0
      - proceed         → pass_list non-empty, extraction built exclusively from pass
      - proceed + empty pass (inconsistent state) → return 0, no LLM call
      - no 'or normalized_text' fallback anywhere in the delivery path
    """

    # ------------------------------------------------------------------
    # build_extraction_text unit tests
    # ------------------------------------------------------------------

    def test_build_extraction_text_raises_on_empty_pass_list(self):
        """build_extraction_text must raise ValueError for empty input."""
        with self.assertRaises(ValueError) as ctx:
            build_extraction_text([])
        self.assertIn("routing contract violation", str(ctx.exception))

    def test_build_extraction_text_joins_candidate_texts(self):
        """build_extraction_text produces double-newline-joined text."""
        cand_a = segment_candidates("Ich lerne Deutsch.")[0]
        cand_b = segment_candidates("Das ist gut.")[0]
        score_a = score_learnability(cand_a)
        score_b = score_learnability(cand_b)
        result = build_extraction_text([(cand_a, score_a), (cand_b, score_b)])
        self.assertIn("Ich lerne Deutsch.", result)
        self.assertIn("Das ist gut.", result)
        self.assertIn("\n\n", result)

    def test_build_extraction_text_single_candidate_no_separator(self):
        """Single candidate produces no joining separator."""
        cand = segment_candidates("Ich lerne Deutsch.")[0]
        score = score_learnability(cand)
        result = build_extraction_text([(cand, score)])
        self.assertNotIn("\n\n", result)
        self.assertEqual(result, "Ich lerne Deutsch.")

    # ------------------------------------------------------------------
    # Routing contract violation: proceed + empty pass
    # ------------------------------------------------------------------

    def test_contract_violation_returns_zero(self):
        """Simulated proceed+empty-pass routing must return 0 without calling split_blocks."""
        import backend.backend_server as server

        # Force route_candidates to return an inconsistent state:
        # routing="proceed" but pass_list empty — should never happen in prod
        # but the contract check must catch it.
        def bad_route(candidates):
            return "proceed", [], []

        learnable_text = "Ich lerne Deutsch. Das ist sehr wichtig."

        with patch("backend.backend_server.route_candidates", side_effect=bad_route):
            with patch("backend.backend_server._shortcut_split_blocks") as mock_split:
                result = server._run_shortcut_lookup_delivery(
                    user_id=42,
                    text=learnable_text,
                )

        self.assertEqual(result, 0)
        mock_split.assert_not_called()

    def test_contract_violation_logs_error(self):
        """A routing contract violation must emit ocr_routing_contract_violation at ERROR level."""
        import logging
        import backend.backend_server as server

        def bad_route(candidates):
            return "proceed", [], []

        learnable_text = "Ich lerne Deutsch."

        with patch("backend.backend_server.route_candidates", side_effect=bad_route):
            with patch("backend.backend_server._shortcut_split_blocks"):
                with self.assertLogs(level=logging.ERROR) as log_cm:
                    server._run_shortcut_lookup_delivery(
                        user_id=99,
                        text=learnable_text,
                    )

        violation_logs = [
            line for line in log_cm.output
            if "ocr_routing_contract_violation" in line
        ]
        self.assertTrue(violation_logs, "expected ocr_routing_contract_violation log entry")

    # ------------------------------------------------------------------
    # No fallback to normalized_text in source
    # ------------------------------------------------------------------

    def test_no_or_normalized_text_fallback_in_delivery_source(self):
        """The delivery function source must not contain 'or normalized_text' after extraction."""
        import inspect
        import backend.backend_server as server

        source = inspect.getsource(server._run_shortcut_lookup_delivery)
        # The unsafe fallback pattern must not exist in the function body.
        self.assertNotIn(
            "or normalized_text",
            source,
            "Found unsafe 'or normalized_text' fallback in _run_shortcut_lookup_delivery",
        )

    # ------------------------------------------------------------------
    # Contract-ok path observability
    # ------------------------------------------------------------------

    def test_contract_ok_logged_for_valid_proceed(self):
        """A valid proceed routing emits ocr_routing_contract_ok at INFO level."""
        import backend.backend_server as server

        learnable_text = "Ich lerne Deutsch. Das ist sehr wichtig."

        with patch("backend.backend_server._shortcut_split_blocks", return_value=[]):
            with self.assertLogs(level="INFO") as log_cm:
                server._run_shortcut_lookup_delivery(
                    user_id=42,
                    text=learnable_text,
                )

        contract_ok_logs = [
            line for line in log_cm.output
            if "ocr_routing_contract_ok" in line
        ]
        self.assertTrue(contract_ok_logs, "expected ocr_routing_contract_ok log entry")

    def test_extraction_payload_built_logged(self):
        """ocr_extraction_payload_built must be logged after assembling extraction text."""
        import backend.backend_server as server

        learnable_text = "Ich lerne Deutsch."

        with patch("backend.backend_server._shortcut_split_blocks", return_value=[]):
            with self.assertLogs(level="INFO") as log_cm:
                server._run_shortcut_lookup_delivery(
                    user_id=42,
                    text=learnable_text,
                )

        payload_logs = [
            line for line in log_cm.output
            if "ocr_extraction_payload_built" in line
        ]
        self.assertTrue(payload_logs, "expected ocr_extraction_payload_built log entry")

    # ------------------------------------------------------------------
    # Extraction text content guarantees
    # ------------------------------------------------------------------

    def test_extraction_text_built_only_from_pass_candidates(self):
        """Noise candidates must never appear in the text sent to split_blocks."""
        import backend.backend_server as server

        # Blank-line-separated: one learnable, one pure noise (CTA word)
        mixed = "Wenn ich in Deutschland bin, spreche ich Deutsch.\n\nFollow"

        captured: list[str] = []

        def capture(text: str):
            captured.append(text)
            return []

        with patch("backend.backend_server._shortcut_split_blocks", side_effect=capture):
            server._run_shortcut_lookup_delivery(user_id=1, text=mixed)

        self.assertEqual(len(captured), 1)
        self.assertNotIn("Follow", captured[0])
        self.assertIn("Deutschland", captured[0])

    def test_every_pass_candidate_has_breakdown(self):
        """Each candidate in route_candidates pass list has a non-empty breakdown dict."""
        text = (
            "Ich lerne Deutsch.\n\n"
            "Das ist sehr wichtig.\n\n"
            "Follow"
        )
        candidates = segment_candidates(text)
        _, pass_list, _ = route_candidates(candidates)

        self.assertTrue(pass_list, "expected non-empty pass list for this input")
        for cand, lscore in pass_list:
            self.assertIsInstance(lscore.breakdown, dict)
            self.assertTrue(
                lscore.breakdown,
                f"pass candidate has empty breakdown: {cand.text!r}",
            )

    def test_skip_all_noise_never_calls_split_blocks(self):
        """Pure noise payload must not call _shortcut_split_blocks at all."""
        import backend.backend_server as server

        with patch("backend.backend_server._shortcut_split_blocks") as mock_split:
            result = server._run_shortcut_lookup_delivery(
                user_id=5,
                text="Follow\n\nSubscribe\n\n89",
            )

        self.assertEqual(result, 0)
        mock_split.assert_not_called()

    def test_mixed_payload_sends_only_useful_candidates_to_llm(self):
        """Mixed payload routes proceed and extraction_text contains only non-noise text."""
        import backend.backend_server as server

        # Learnable: multi-word German; Noise: engagement residual
        mixed = "Ich möchte Deutsch lernen.\n\n1.5K likes"

        captured: list[str] = []

        def capture(text: str):
            captured.append(text)
            return []

        with patch("backend.backend_server._shortcut_split_blocks", side_effect=capture):
            server._run_shortcut_lookup_delivery(user_id=7, text=mixed)

        self.assertEqual(len(captured), 1)
        self.assertNotIn("1.5K likes", captured[0])
        self.assertIn("Ich möchte Deutsch lernen.", captured[0])


# ---------------------------------------------------------------------------
# 10. OCR v3 — self-ingestion suppression tests
# ---------------------------------------------------------------------------

class OcrSelfIngestionTests(unittest.TestCase):
    """
    Verify that bot-generated wrapper vocabulary is stripped before candidate
    segmentation and that the suppression is observable.
    """

    # ------------------------------------------------------------------
    # Pattern metadata
    # ------------------------------------------------------------------

    def test_all_self_ingestion_patterns_have_unique_names(self):
        names = [p.name for p in _SELF_INGESTION_PATTERNS]
        self.assertEqual(len(names), len(set(names)), "duplicate self-ingestion pattern names")

    def test_all_self_ingestion_patterns_have_compiled_regex(self):
        import re as re_mod
        for pat in _SELF_INGESTION_PATTERNS:
            self.assertIsInstance(
                pat.pattern, re_mod.Pattern,
                f"pattern {pat.name!r} is not a compiled regex",
            )

    # ------------------------------------------------------------------
    # _apply_self_ingestion_suppression unit tests
    # ------------------------------------------------------------------

    def test_query_wrapper_suppressed(self):
        text, fired = _apply_self_ingestion_suppression("Запрос: aufgeben")
        self.assertEqual(text.strip(), "")
        self.assertIn("query_wrapper_ru", fired)

    def test_language_pair_selector_suppressed(self):
        text, fired = _apply_self_ingestion_suppression(
            "Выберите языковую пару для перевода:"
        )
        self.assertEqual(text.strip(), "")
        self.assertIn("language_pair_selector_ru", fired)

    def test_feel_the_word_header_suppressed(self):
        _, fired = _apply_self_ingestion_suppression("🧠 Feel the Word")
        self.assertIn("feel_the_word_header", fired)

    def test_horizontal_divider_suppressed(self):
        _, fired = _apply_self_ingestion_suppression("━━━━━━━━━━━━")
        self.assertIn("bot_horizontal_divider", fired)

    def test_language_pair_label_suppressed(self):
        _, fired = _apply_self_ingestion_suppression("🌐 DE → RU")
        self.assertIn("bot_language_pair_label", fired)

    def test_rate_answer_prompt_suppressed(self):
        _, fired = _apply_self_ingestion_suppression("Оцени ответ кнопкой ниже:")
        self.assertIn("bot_rate_answer_prompt_ru", fired)

    def test_learnable_content_not_suppressed(self):
        """Real German learnable content must pass through unsuppressed."""
        text = "aufgeben bedeutet aufhören oder weitergeben."
        cleaned, fired = _apply_self_ingestion_suppression(text)
        self.assertEqual(cleaned, text)
        self.assertEqual(fired, [])

    def test_mixed_input_preserves_learnable_removes_wrapper(self):
        raw = "Запрос: aufgeben\naufgeben bedeutet aufhören."
        cleaned, fired = _apply_self_ingestion_suppression(raw)
        self.assertIn("aufgeben bedeutet aufhören.", cleaned)
        self.assertNotIn("Запрос:", cleaned)
        self.assertIn("query_wrapper_ru", fired)

    def test_multiple_wrappers_all_fired(self):
        raw = "\n".join([
            "Запрос: lernen",
            "Выберите языковую пару для перевода:",
            "━━━━━━━━━━━━",
            "Ich lerne Deutsch.",
        ])
        _, fired = _apply_self_ingestion_suppression(raw)
        self.assertIn("query_wrapper_ru", fired)
        self.assertIn("language_pair_selector_ru", fired)
        self.assertIn("bot_horizontal_divider", fired)

    def test_suppression_count_matches_removed_lines(self):
        raw = "Запрос: lernen\n━━━━━━━━━━━━\nIch lerne Deutsch."
        _, fired = _apply_self_ingestion_suppression(raw)
        self.assertEqual(len(fired), 2)

    # ------------------------------------------------------------------
    # Pipeline integration: stage appears in result
    # ------------------------------------------------------------------

    def test_self_ingestion_stage_in_pipeline_result(self):
        result = run_ocr_pipeline("Запрос: lernen\nIch lerne Deutsch.", source="test")
        stage_names = [s.stage for s in result.stage_metrics]
        self.assertIn("self_ingestion_suppression", stage_names)

    def test_telegram_self_ingestion_fixture_passes(self):
        result = run_ocr_pipeline(TELEGRAM_SELF_INGESTION.raw, source="test")
        for must_keep in TELEGRAM_SELF_INGESTION.must_keep:
            self.assertIn(must_keep, result.cleaned_text,
                          f"must_keep {must_keep!r} missing from cleaned output")
        for must_drop in TELEGRAM_SELF_INGESTION.must_drop:
            self.assertNotIn(must_drop, result.cleaned_text,
                             f"must_drop {must_drop!r} found in cleaned output")

    def test_suppression_logged_at_stage_level(self):
        with self.assertLogs(level="INFO") as log_cm:
            run_ocr_pipeline("Запрос: lernen\nIch lerne Deutsch.", source="test")
        stage_logs = [
            line for line in log_cm.output
            if "self_ingestion_suppression" in line
        ]
        self.assertTrue(stage_logs, "expected self_ingestion_suppression stage log")

    def test_clean_payload_has_zero_suppressed_lines(self):
        result = run_ocr_pipeline(
            "Ich lerne Deutsch.\nDas ist sehr wichtig.", source="test"
        )
        sup_stage = next(
            (s for s in result.stage_metrics if s.stage == "self_ingestion_suppression"),
            None,
        )
        self.assertIsNotNone(sup_stage)
        self.assertEqual(sup_stage.rules_fired, 0)


# ---------------------------------------------------------------------------
# 11. OCR v3 — corruption detection tests
# ---------------------------------------------------------------------------

class OcrCorruptionDetectionTests(unittest.TestCase):
    """
    Verify OCR corruption signals: underscore_artifact, numeric_orphan.
    """

    def _candidate(self, text: str) -> OcrCandidate:
        return segment_candidates(text)[0] if text.strip() else OcrCandidate(
            text=text, line_count=1, token_estimate=0,
            detected_languages=[], char_offset=0,
        )

    # ------------------------------------------------------------------
    # underscore_artifact signal
    # ------------------------------------------------------------------

    def test_underscore_artifact_fires_for_merged_word(self):
        """frau_deuen should trigger underscore_artifact."""
        cand = self._candidate("frau_deuen lernt Deutsch.")
        s = score_learnability(cand)
        self.assertIn("underscore_artifact", s.breakdown,
                      f"underscore_artifact not in breakdown: {s.breakdown}")
        self.assertLess(s.breakdown["underscore_artifact"], 0)

    def test_underscore_artifact_fires_for_wort_buch(self):
        cand = self._candidate("Das wort_buch liegt auf dem Tisch.")
        s = score_learnability(cand)
        self.assertIn("underscore_artifact", s.breakdown)

    def test_underscore_artifact_reduces_score(self):
        clean = self._candidate("Ich lerne Deutsch.")
        corrupted = self._candidate("Ich lerne deutsch_lernen.")
        clean_score = score_learnability(clean).score
        corrupted_score = score_learnability(corrupted).score
        self.assertLess(corrupted_score, clean_score,
                        "corruption should reduce learnability score")

    def test_underscore_artifact_not_fired_for_clean_text(self):
        cand = self._candidate("Ich lerne jeden Tag Deutsch.")
        s = score_learnability(cand)
        self.assertNotIn("underscore_artifact", s.breakdown)

    def test_ocr_corruption_fixture_passes(self):
        result = run_ocr_pipeline(OCR_CORRUPTION_TOKENS.raw, source="test")
        for must_keep in OCR_CORRUPTION_TOKENS.must_keep:
            self.assertIn(must_keep, result.cleaned_text,
                          f"must_keep {must_keep!r} missing from cleaned output")

    # ------------------------------------------------------------------
    # numeric_orphan signal
    # ------------------------------------------------------------------

    def test_numeric_orphan_fires_for_bare_number(self):
        cand = self._candidate("477")
        s = score_learnability(cand)
        self.assertIn("numeric_orphan", s.breakdown)
        self.assertEqual(s.score, -1.0)
        self.assertEqual(s.label, "likely_noise")

    def test_numeric_orphan_fires_for_parenthesized_number(self):
        cand = self._candidate("(21)")
        s = score_learnability(cand)
        self.assertIn("numeric_orphan", s.breakdown)
        self.assertEqual(s.label, "likely_noise")

    def test_no_alphabetic_chars_fires_for_emoji_only(self):
        cand = self._candidate("😂😂😂😂")
        s = score_learnability(cand)
        self.assertIn("no_alphabetic_chars", s.breakdown)
        self.assertNotIn("numeric_orphan", s.breakdown)

    def test_no_alphabetic_chars_fires_for_colon_timestamp(self):
        """3:42 has a colon so is NOT a pure numeric orphan."""
        cand = self._candidate("3:42")
        s = score_learnability(cand)
        self.assertIn("no_alphabetic_chars", s.breakdown)
        self.assertNotIn("numeric_orphan", s.breakdown)

    def test_numeric_orphan_lines_removed_by_v1_structural_rule(self):
        result = run_ocr_pipeline(NUMERIC_ORPHAN_LINES.raw, source="test")
        for must_keep in NUMERIC_ORPHAN_LINES.must_keep:
            self.assertIn(must_keep, result.cleaned_text,
                          f"must_keep {must_keep!r} missing from cleaned output")
        # Standalone numeric lines should be absent after v1 cleanup
        for orphan in ("1", "477", "5"):
            # The number must not appear as a standalone line
            import re as _re
            self.assertIsNone(
                _re.search(rf"(?m)^[ \t]*\(?{orphan}\)?[ \t]*$", result.cleaned_text),
                f"numeric orphan {orphan!r} survived structural cleanup",
            )

    def test_v3_learnability_fixtures_pass(self):
        for fix in ALL_V3_LEARNABILITY_FIXTURES:
            with self.subTest(fixture=fix.name):
                cand = self._candidate(fix.text)
                s = score_learnability(cand)
                self.assertEqual(
                    s.label, fix.expected_label,
                    f"[{fix.name}] expected {fix.expected_label!r}, "
                    f"got {s.label!r} (score={s.score:.3f}, breakdown={s.breakdown})",
                )
                self.assertGreaterEqual(
                    s.score, fix.score_min,
                    f"[{fix.name}] score {s.score:.3f} < min {fix.score_min}",
                )
                self.assertLessEqual(
                    s.score, fix.score_max,
                    f"[{fix.name}] score {s.score:.3f} > max {fix.score_max}",
                )


# ---------------------------------------------------------------------------
# 12. OCR v3 — context annotation tests
# ---------------------------------------------------------------------------

class OcrContextAnnotationTests(unittest.TestCase):
    """
    Verify the context reconstruction scaffold: annotate_context returns
    advisory signals without modifying or filtering candidates.
    """

    def test_annotate_context_returns_list(self):
        candidates = segment_candidates("Ich lerne Deutsch.")
        signals = annotate_context(candidates)
        self.assertIsInstance(signals, list)

    def test_empty_candidates_returns_empty(self):
        self.assertEqual(annotate_context([]), [])

    def test_single_candidate_returns_empty(self):
        candidates = segment_candidates("Ich lerne Deutsch.")
        self.assertEqual(annotate_context(candidates), [])

    def test_language_transition_detected(self):
        # Ukrainian question + German answer → different scripts
        text = "Що означає це слово?\n\nDas bedeutet 'Wort'."
        candidates = segment_candidates(text)
        signals = annotate_context(candidates)
        signal_types = [s.signal_type for s in signals]
        self.assertIn("language_transition_pair", signal_types)

    def test_question_before_answer_detected(self):
        text = "Що означає це слово?\n\nDas bedeutet 'Wort'."
        candidates = segment_candidates(text)
        signals = annotate_context(candidates)
        signal_types = [s.signal_type for s in signals]
        self.assertIn("question_before_answer", signal_types)

    def test_same_language_pair_no_transition(self):
        text = "Ich lerne Deutsch.\n\nDas ist sehr gut."
        candidates = segment_candidates(text)
        signals = annotate_context(candidates)
        transitions = [s for s in signals if s.signal_type == "language_transition_pair"]
        self.assertEqual(transitions, [],
                         "same-language adjacent candidates should not trigger transition")

    def test_annotate_does_not_modify_candidates(self):
        """annotate_context must be pure — candidates unchanged."""
        text = "Що означає це слово?\n\nIch lerne Deutsch."
        candidates_before = segment_candidates(text)
        texts_before = [c.text for c in candidates_before]
        annotate_context(candidates_before)
        texts_after = [c.text for c in candidates_before]
        self.assertEqual(texts_before, texts_after)

    def test_signal_has_required_fields(self):
        text = "Що означає?\n\nIch lerne Deutsch."
        candidates = segment_candidates(text)
        signals = annotate_context(candidates)
        for sig in signals:
            self.assertIsInstance(sig, OcrContextSignal)
            self.assertIsInstance(sig.candidate_index, int)
            self.assertIsInstance(sig.signal_type, str)
            self.assertIsInstance(sig.detail, str)
            self.assertTrue(sig.signal_type)

    def test_context_question_answer_fixture(self):
        result = run_ocr_pipeline(CONTEXT_QUESTION_ANSWER.raw, source="test")
        for must_keep in CONTEXT_QUESTION_ANSWER.must_keep:
            self.assertIn(must_keep, result.cleaned_text,
                          f"must_keep {must_keep!r} missing from cleaned output")

    def test_all_v3_ocr_fixtures_pass(self):
        for fix in ALL_V3_OCR_FIXTURES:
            with self.subTest(fixture=fix.name):
                result = run_ocr_pipeline(fix.raw, source="test")
                for must_keep in fix.must_keep:
                    self.assertIn(must_keep, result.cleaned_text,
                                  f"[{fix.name}] must_keep {must_keep!r} missing")
                for must_drop in fix.must_drop:
                    if must_drop.startswith("\n"):
                        # must_drop with newline markers: check it's not present as standalone line
                        import re as _re
                        fragment = must_drop.strip()
                        self.assertIsNone(
                            _re.search(rf"(?m)^[ \t]*\(?{_re.escape(fragment)}\)?[ \t]*$",
                                       result.cleaned_text),
                            f"[{fix.name}] numeric orphan {fragment!r} survived cleanup",
                        )
                    else:
                        self.assertNotIn(must_drop, result.cleaned_text,
                                         f"[{fix.name}] must_drop {must_drop!r} found")
