import unittest

from backend.rebus_bank import (
    COMPONENT_IMAGE_PROMPTS,
    REBUS_COMPOUND_BANK,
    is_rebus_compound_blocked,
    rebus_prompt_pair_collision,
)


class RebusPairCollisionTests(unittest.TestCase):
    """The learner must ADD the two pictures up. If one of them already contains the
    other's object, there is nothing to add and the answer is on the card."""

    def test_content_inside_its_container_is_caught(self) -> None:
        coffee_in_a_cup = "A cup of black coffee with steam, children's book illustration style"
        a_cup = "A white ceramic tea cup with saucer, children's book illustration style"
        self.assertEqual(rebus_prompt_pair_collision(coffee_in_a_cup, a_cup), "cup")

    def test_loose_content_and_empty_container_pass(self) -> None:
        beans = ("A small heap of roasted brown coffee beans lying loose on a plain surface, "
                 "no cup, no mug, no jar, no container, children's book illustration style")
        empty_cup = ("A single empty white ceramic cup with a handle, no liquid, no coffee, "
                     "no tea, no saucer, children's book illustration style")
        self.assertEqual(rebus_prompt_pair_collision(beans, empty_cup), "")

    def test_shared_adjectives_are_not_a_collision(self) -> None:
        moon = "A bright full moon, glowing, children's book illustration style"
        light = "A bright glowing light beam, children's book illustration style"
        self.assertEqual(rebus_prompt_pair_collision(moon, light), "")

    def test_whole_static_bank_is_clean(self) -> None:
        offenders = []
        for entry in REBUS_COMPOUND_BANK:
            parts = entry.get("parts") or []
            if len(parts) < 2:
                continue
            compound = str(entry.get("compound") or "")
            if is_rebus_compound_blocked(compound, compound_id=str(entry.get("id") or "")):
                continue
            prompts = entry.get("dalle_prompts") or {}
            resolved = []
            for part in parts[:2]:
                word = str(part.get("word") or "")
                resolved.append(str(COMPONENT_IMAGE_PROMPTS.get(word) or prompts.get(word) or ""))
            if not all(resolved):
                continue
            shared = rebus_prompt_pair_collision(*resolved)
            if shared:
                offenders.append(f"{compound}: both show '{shared}'")
        self.assertEqual(offenders, [])

    def test_self_revealing_compounds_are_blocked(self) -> None:
        for compound in ("Tannenbaum", "Elefantenrüssel", "Löwenmähne"):
            with self.subTest(compound):
                self.assertTrue(is_rebus_compound_blocked(compound))


if __name__ == "__main__":
    unittest.main()


class RebusFailureBudgetTests(unittest.TestCase):
    """Неудачная отрисовка стоит столько же, сколько удачная. Значит у неё должен
    быть счёт, а у невозможной пары — конец, а не вечные попытки."""

    def test_blocked_compounds_include_the_self_revealing_ones(self) -> None:
        for compound in ("Adlerflügel", "Weinglas", "Tannenbaum", "Elefantenrüssel", "Löwenmähne"):
            with self.subTest(compound):
                self.assertTrue(is_rebus_compound_blocked(compound))

    def test_band_is_a_strip_not_a_bow(self) -> None:
        # Проверка предмета браковала «Band» как бант — описание было про бант.
        prompt = COMPONENT_IMAGE_PROMPTS["Band"].lower()
        self.assertIn("strip", prompt)
        self.assertIn("no bow", prompt)

    def test_word_is_not_redrawn_forever(self) -> None:
        from unittest.mock import patch
        from backend import rebus_generator
        from backend.database import MAX_REBUS_COMPONENT_REDRAWS
        spent = {"redraw_count": MAX_REBUS_COMPONENT_REDRAWS + 1,
                 "generation_status": "failed", "review_status": "pending",
                 "review_reason": "main object is a bow"}
        with patch("backend.database.get_rebus_component_image", return_value=spent), \
             patch("backend.image_generation_provider.generate_image_bytes") as gen:
            with self.assertRaises(RuntimeError):
                rebus_generator.generate_component_image("Band", "A flat ribbon strip")
            gen.assert_not_called()
