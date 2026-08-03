import unittest
from unittest.mock import patch

from backend import rebus_generator


class RebusComponentReviewTests(unittest.TestCase):
    """A freshly drawn half waits for the owner; a card is built only from accepted
    halves; a rejection is not thrown away but folded into the next prompt."""

    def _draw(self, existing, *, capture):
        """Run generate_component_image against fakes and return the upsert calls."""
        calls = []

        def _upsert(word, **kw):
            calls.append({"word": word, **kw})

        with patch("backend.database.get_rebus_component_image", return_value=existing), \
             patch("backend.database.upsert_rebus_component_image", _upsert), \
             patch("backend.image_generation_provider.generate_image_bytes",
                   side_effect=lambda **kw: capture.update(kw) or {"data": b"img", "mime_type": "image/png"}), \
             patch("backend.openai_manager.run_image_depicts", return_value={"ok": True}), \
             patch("backend.r2_storage.r2_put_bytes", return_value=None):
            rebus_generator.generate_component_image("Kaffee", "A heap of coffee beans")
        return calls

    def test_new_picture_waits_for_the_owner(self) -> None:
        calls = self._draw(None, capture={})
        ready = [c for c in calls if c.get("generation_status") == "ready"]
        self.assertEqual(len(ready), 1)
        self.assertEqual(ready[0].get("review_status"), "pending")

    def test_rejection_reason_goes_into_the_next_prompt(self) -> None:
        capture: dict = {}
        self._draw(
            {"generation_status": "failed", "image_object_key": None, "review_status": "pending",
             "review_reason": "the picture contains the object of the other half", "redraw_count": 1},
            capture=capture,
        )
        self.assertIn("previous attempt was rejected", capture.get("prompt", ""))
        self.assertIn("other half", capture.get("prompt", ""))

    def test_blocked_word_is_never_drawn_again(self) -> None:
        with patch("backend.database.get_rebus_component_image",
                   return_value={"review_status": "blocked", "generation_status": "failed"}), \
             patch("backend.image_generation_provider.generate_image_bytes") as gen:
            with self.assertRaises(RuntimeError):
                rebus_generator.generate_component_image("Kaffee", "A heap of coffee beans")
            gen.assert_not_called()

    def test_card_is_not_composed_from_an_unaccepted_half(self) -> None:
        entry = {
            "compound": "Kaffeetasse",
            "composed_status": "pending",
            "parts": [{"word": "Kaffee", "meaning_ru": "кофе"}, {"word": "Tasse", "meaning_ru": "чашка"}],
        }
        status = {"Kaffee": "approved", "Tasse": "pending"}
        with patch("backend.database.get_rebus_bank_entry", return_value=entry), \
             patch("backend.database.mark_rebus_composed") as composed, \
             patch("backend.database.mark_rebus_compose_failed") as failed, \
             patch("backend.database.get_rebus_component_image",
                   side_effect=lambda w: {"review_status": status[w], "dalle_prompt": ""}), \
             patch.object(rebus_generator, "generate_component_image", side_effect=lambda w, *a, **k: f"{w}.png"), \
             patch.object(rebus_generator, "compose_rebus_card") as compose:
            result = rebus_generator.prepare_rebus_entry("kaffeetasse_001")
        self.assertEqual(result["status"], "awaiting_review")
        self.assertEqual(result["waiting"], ["Tasse"])
        compose.assert_not_called()
        composed.assert_not_called()
        failed.assert_not_called()


if __name__ == "__main__":
    unittest.main()
