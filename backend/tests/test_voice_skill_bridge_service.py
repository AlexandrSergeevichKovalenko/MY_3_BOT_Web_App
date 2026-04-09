import unittest
from unittest.mock import patch

from backend.voice_skill_bridge_service import apply_voice_skill_bridge, build_voice_skill_bridge_payload


class VoiceSkillBridgeServiceTests(unittest.TestCase):
    def test_build_payload_maps_explicit_subordinate_clause_signal(self):
        with patch(
            "backend.voice_skill_bridge_service.get_agent_voice_session",
            return_value={
                "session_id": 501,
                "user_id": 42,
                "source_lang": "ru",
                "target_lang": "de",
                "ended_at": "2026-04-09T10:00:00+00:00",
            },
        ), patch(
            "backend.voice_skill_bridge_service.get_voice_session_assessment",
            return_value={
                "session_id": 501,
                "grammar_control_note": "Subordinate clause order was unstable and the verb-final position broke several times.",
                "strict_feedback": "Clause structure stayed weak under pressure.",
                "recommended_next_focus": "Force short subordinate clauses with the verb at the end.",
            },
        ), patch(
            "backend.voice_skill_bridge_service.get_skill_by_id",
            return_value={"skill_id": "word_order_subordinate_clause", "is_active": True},
        ):
            payload = build_voice_skill_bridge_payload(session_id=501)

        self.assertEqual(payload["user_id"], 42)
        self.assertEqual(len(payload["events"]), 1)
        self.assertEqual(payload["events"][0]["skill_id"], "word_order_subordinate_clause")
        self.assertEqual(payload["events"][0]["event_type"], "fail")
        self.assertLess(payload["events"][0]["base_delta"], 0)

    def test_apply_bridge_skips_when_no_low_risk_mapping_exists(self):
        with patch(
            "backend.voice_skill_bridge_service.get_voice_session_assessment",
            return_value={"session_id": 601, "skill_bridge_status": "pending"},
        ), patch(
            "backend.voice_skill_bridge_service.claim_voice_session_assessment_for_skill_bridge",
            return_value={"session_id": 601, "skill_bridge_status": "in_progress"},
        ), patch(
            "backend.voice_skill_bridge_service.build_voice_skill_bridge_payload",
            return_value={"session_id": 601, "events": [], "notes": "No explicit low-risk voice-to-skill mapping was found.", "signals": []},
        ), patch(
            "backend.voice_skill_bridge_service.set_voice_session_assessment_skill_bridge_status",
        ) as mark_status:
            result = apply_voice_skill_bridge(session_id=601)

        self.assertFalse(result.applied)
        self.assertIn("No explicit low-risk", result.notes)
        mark_status.assert_called_once()
        self.assertEqual(mark_status.call_args.kwargs["status"], "skipped")

    def test_apply_bridge_does_not_reapply_when_status_is_already_applied(self):
        with patch(
            "backend.voice_skill_bridge_service.get_voice_session_assessment",
            return_value={"session_id": 701, "skill_bridge_status": "applied"},
        ), patch(
            "backend.voice_skill_bridge_service.claim_voice_session_assessment_for_skill_bridge",
            return_value=None,
        ), patch(
            "backend.voice_skill_bridge_service.apply_user_skill_event",
        ) as apply_event:
            result = apply_voice_skill_bridge(session_id=701)

        self.assertFalse(result.applied)
        self.assertEqual(result.notes, "bridge_not_claimed:applied")
        apply_event.assert_not_called()


if __name__ == "__main__":
    unittest.main()
