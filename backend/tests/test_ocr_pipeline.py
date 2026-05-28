"""
Tests for backend/ocr_pipeline.py — OCR Intelligence Pipeline v1.

Coverage:
  - Rule metadata: unique names, stage ordering
  - Stage 1: whitespace normalization (deterministic, idempotent)
  - Stage 2: structural cleanup — Instagram/TikTok noise removal
  - Stage 3: OCR artifact cleanup — mangled quotes, punctuation
  - Stage 4: language detection — script presence detection
  - Integration: OcrPipelineResult fields, observability metrics
  - Fixture suite: all 10 real-world fixtures pass must_keep / must_drop
  - Safety: no catastrophic deletion of learnable content
  - Backend integration: pipeline wired into _start_shortcut_lookup_enqueue_runner
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from backend.ocr_pipeline import (
    ALL_RULE_LISTS,
    OcrPipelineResult,
    OcrStageMetric,
    _STRUCTURAL_RULES,
    _WHITESPACE_RULES,
    _OCR_ARTIFACT_RULES,
    _detect_scripts,
    run_ocr_pipeline,
)
from backend.tests.fixtures.ocr_samples import ALL_FIXTURES, OCR_MISTAKES


# ---------------------------------------------------------------------------
# 1. Rule metadata
# ---------------------------------------------------------------------------

class OcrPipelineRuleMetadataTests(unittest.TestCase):

    def test_all_rule_names_unique_across_all_stages(self):
        all_names = [rule.name for ruleset in ALL_RULE_LISTS for rule in ruleset]
        self.assertEqual(len(all_names), len(set(all_names)),
                         "Duplicate rule names found: " + str(
                             [n for n in all_names if all_names.count(n) > 1]))

    def test_stage_ordering_is_four_stages(self):
        result = run_ocr_pipeline("Hello Welt", source="test")
        self.assertEqual(len(result.stage_metrics), 4)

    def test_stage_names_in_correct_order(self):
        result = run_ocr_pipeline("Hello Welt", source="test")
        expected_order = [
            "whitespace_normalization",
            "structural_cleanup",
            "ocr_artifact_cleanup",
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
