import unittest
from unittest.mock import AsyncMock, patch

from backend.voice_assessment_service import build_and_store_voice_assessment, build_voice_assessment


QUALITY_FIXTURES = {
    "short_transcript": {
        "segments": [
            {"speaker": "assistant", "text": "Hallo."},
            {"speaker": "user", "text": "Hallo."},
        ],
        "context": {
            "session": {"session_id": 101},
            "scenario": None,
            "prep_pack": None,
        },
    },
    "weak_grammar_direct_feedback": {
        "segments": [
            {"speaker": "assistant", "text": "Erzählen Sie kurz von Ihrem Morgen."},
            {"speaker": "user", "text": "Ich habe heute früh Kaffee trinken und dann ich gehe Arbeit ohne Frühstück, weil ich war sehr müde und die Zeit war schlecht."},
            {"speaker": "assistant", "text": "Und danach? Versuchen Sie es genauer zu beschreiben."},
            {"speaker": "user", "text": "Danach ich warte lange auf Bus, dann ich komme zu spät und mein Chef ist nicht zufrieden mit mir heute."},
        ],
        "context": {
            "session": {"session_id": 102},
            "scenario": None,
            "prep_pack": None,
        },
        "llm_payload": """
        {
          "summary": "Good job overall. The learner described the morning routine.",
          "strict_feedback": "Well done. The main issue was broken word order and missing inflection in several clauses.",
          "lexical_range_note": "Vocabulary stayed basic and repetitive.",
          "grammar_control_note": "Word order was unstable and verb forms were often wrong.",
          "fluency_note": "Speech moved forward, but sentence building looked effortful.",
          "coherence_relevance_note": "The answer stayed on topic.",
          "self_correction_note": "No self-correction appeared.",
          "target_vocab_used": [],
          "target_vocab_missed": [],
          "recommended_next_focus": "Keep practicing."
        }
        """,
    },
    "weak_coherence_but_vocab_ok": {
        "segments": [
            {"speaker": "assistant", "text": "Bestellen Sie bitte im Café."},
            {"speaker": "user", "text": "Ich hätte gern einen Kaffee mit Hafermilch."},
            {"speaker": "assistant", "text": "Sonst noch etwas?"},
            {"speaker": "user", "text": "Ja, und gestern war mein Büro stressig und der Bus war auch spät."},
            {"speaker": "assistant", "text": "Bleiben wir bei der Bestellung."},
            {"speaker": "user", "text": "Dann noch ein Stück Kuchen zum Mitnehmen."},
        ],
        "context": {
            "session": {"session_id": 103},
            "scenario": {"topic": "ordering in a cafe"},
            "prep_pack": {
                "target_vocab": ["Kaffee", "Hafermilch", "Kuchen"],
                "target_expressions": ["Ich hätte gern", "zum Mitnehmen"],
            },
        },
        "llm_payload": """
        {
          "summary": "The learner completed the order but drifted off topic in the middle.",
          "strict_feedback": "The main problem was coherence: one answer jumped out of the café task and wasted the turn.",
          "lexical_range_note": "Target vocabulary was used accurately, but range stayed narrow.",
          "grammar_control_note": "Grammar was mostly serviceable in short request sentences.",
          "fluency_note": "Delivery was functional, but turn efficiency dropped when the response drifted.",
          "coherence_relevance_note": "One answer broke the task frame and needed redirection.",
          "self_correction_note": "No self-repair appeared after the off-topic turn.",
          "target_vocab_used": ["Kaffee", "Hafermilch", "Kuchen", "Ich hätte gern", "zum Mitnehmen"],
          "target_vocab_missed": [],
          "recommended_next_focus": "Practice answering the exact prompt before adding extra information."
        }
        """,
    },
    "duplicate_note_regression": {
        "segments": [
            {"speaker": "assistant", "text": "Bitte beschreiben Sie Ihr Wochenende."},
            {"speaker": "user", "text": "Am Samstag ich gehen Park und Sonntag ich bleiben zuhause."},
            {"speaker": "assistant", "text": "Was war schwierig?"},
            {"speaker": "user", "text": "Ich weiß nicht viele Wörter."},
        ],
        "context": {
            "session": {"session_id": 104},
            "scenario": None,
            "prep_pack": None,
        },
        "llm_payload": """
        {
          "summary": "The learner described the weekend briefly.",
          "strict_feedback": "Grammar control was weak and the response stayed very short.",
          "lexical_range_note": "Very limited evidence.",
          "grammar_control_note": "Very limited evidence.",
          "fluency_note": "Very limited evidence.",
          "coherence_relevance_note": "Very limited evidence.",
          "self_correction_note": "Very limited evidence.",
          "target_vocab_used": [],
          "target_vocab_missed": [],
          "recommended_next_focus": "Force short past-tense sentences with correct verb placement."
        }
        """,
    },
}


class VoiceAssessmentQualityTests(unittest.IsolatedAsyncioTestCase):
    async def test_short_transcript_fallback_is_honest(self):
        fixture = QUALITY_FIXTURES["short_transcript"]
        with patch("backend.voice_assessment_service.fetch_agent_voice_transcript_segments", return_value=fixture["segments"]), patch(
            "backend.voice_assessment_service.get_agent_voice_session_context",
            return_value=fixture["context"],
        ):
            assessment = await build_voice_assessment(session_id=101)

        self.assertIsNotNone(assessment)
        self.assertIn("too short", assessment.summary.lower())
        self.assertIn("not enough", assessment.strict_feedback.lower())
        self.assertTrue(assessment.recommended_next_focus)
        self.assertNotIn("good job", assessment.strict_feedback.lower())

    async def test_strict_feedback_sanitization_removes_praise_and_vague_focus(self):
        fixture = QUALITY_FIXTURES["weak_grammar_direct_feedback"]
        with patch("backend.voice_assessment_service.fetch_agent_voice_transcript_segments", return_value=fixture["segments"]), patch(
            "backend.voice_assessment_service.get_agent_voice_session_context",
            return_value=fixture["context"],
        ), patch(
            "backend.voice_assessment_service.llm_execute",
            return_value=fixture["llm_payload"],
        ):
            assessment = await build_voice_assessment(session_id=102)

        self.assertIsNotNone(assessment)
        self.assertLessEqual(len(assessment.summary), 220)
        self.assertNotIn("good job", assessment.summary.lower())
        self.assertNotIn("well done", assessment.strict_feedback.lower())
        self.assertNotEqual(assessment.recommended_next_focus.lower(), "keep practicing.")
        self.assertIn("word order", assessment.strict_feedback.lower())

    async def test_prep_vocab_case_returns_concrete_non_empty_fields(self):
        fixture = QUALITY_FIXTURES["weak_coherence_but_vocab_ok"]
        with patch("backend.voice_assessment_service.fetch_agent_voice_transcript_segments", return_value=fixture["segments"]), patch(
            "backend.voice_assessment_service.get_agent_voice_session_context",
            return_value=fixture["context"],
        ), patch(
            "backend.voice_assessment_service.llm_execute",
            return_value=fixture["llm_payload"],
        ):
            assessment = await build_voice_assessment(session_id=103)

        self.assertIsNotNone(assessment)
        self.assertTrue(assessment.summary)
        self.assertTrue(assessment.strict_feedback)
        self.assertTrue(assessment.lexical_range_note)
        self.assertTrue(assessment.grammar_control_note)
        self.assertTrue(assessment.fluency_note)
        self.assertTrue(assessment.coherence_relevance_note)
        self.assertTrue(assessment.self_correction_note)
        self.assertIn("coherence", assessment.strict_feedback.lower())
        self.assertEqual(assessment.target_vocab_missed, [])
        self.assertIn("Kaffee", assessment.target_vocab_used)
        self.assertIn("zum Mitnehmen", assessment.target_vocab_used)

    async def test_duplicate_notes_do_not_survive_as_identical_block(self):
        fixture = QUALITY_FIXTURES["duplicate_note_regression"]
        with patch("backend.voice_assessment_service.fetch_agent_voice_transcript_segments", return_value=fixture["segments"]), patch(
            "backend.voice_assessment_service.get_agent_voice_session_context",
            return_value=fixture["context"],
        ), patch(
            "backend.voice_assessment_service.llm_execute",
            return_value=fixture["llm_payload"],
        ):
            assessment = await build_voice_assessment(session_id=104)

        self.assertIsNotNone(assessment)
        notes = [
            assessment.lexical_range_note,
            assessment.grammar_control_note,
            assessment.fluency_note,
            assessment.coherence_relevance_note,
            assessment.self_correction_note,
        ]
        self.assertGreater(len(set(notes)), 1)

    async def test_build_and_store_waits_for_late_transcript_material_before_fallback(self):
        fixture = QUALITY_FIXTURES["weak_coherence_but_vocab_ok"]
        late_segments = [
            [],
            [{"speaker": "assistant", "text": "Hallo Aleksandr!"}],
            fixture["segments"],
        ]
        with patch(
            "backend.voice_assessment_service.fetch_agent_voice_transcript_segments",
            side_effect=late_segments,
        ) as fetch_mock, patch(
            "backend.voice_assessment_service.get_agent_voice_session_context",
            return_value=fixture["context"],
        ), patch(
            "backend.voice_assessment_service.llm_execute",
            return_value=fixture["llm_payload"],
        ), patch(
            "backend.voice_assessment_service.asyncio.sleep",
            new=AsyncMock(return_value=None),
        ) as sleep_mock, patch(
            "backend.voice_assessment_service.store_voice_assessment",
            side_effect=lambda assessment: assessment,
        ):
            assessment = await build_and_store_voice_assessment(session_id=105)

        self.assertIsNotNone(assessment)
        self.assertNotIn("too short", assessment.summary.lower())
        self.assertTrue(fetch_mock.call_count >= 3)
        self.assertEqual(sleep_mock.await_count, 2)

    async def test_build_and_store_keeps_fallback_when_transcript_stays_premature(self):
        assistant_only_segments = [{"speaker": "assistant", "text": "Hallo Aleksandr!"}]
        with patch(
            "backend.voice_assessment_service.fetch_agent_voice_transcript_segments",
            side_effect=[assistant_only_segments, assistant_only_segments, assistant_only_segments],
        ) as fetch_mock, patch(
            "backend.voice_assessment_service.get_agent_voice_session_context",
            return_value={"session": {"session_id": 106}, "scenario": None, "prep_pack": None},
        ), patch(
            "backend.voice_assessment_service.asyncio.sleep",
            new=AsyncMock(return_value=None),
        ) as sleep_mock, patch(
            "backend.voice_assessment_service.store_voice_assessment",
            side_effect=lambda assessment: assessment,
        ):
            assessment = await build_and_store_voice_assessment(session_id=106)

        self.assertIsNotNone(assessment)
        self.assertIn("too short", assessment.summary.lower())
        self.assertEqual(fetch_mock.call_count, 3)
        self.assertEqual(sleep_mock.await_count, 2)


if __name__ == "__main__":
    unittest.main()
