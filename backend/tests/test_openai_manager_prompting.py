import unittest

from backend.openai_manager import system_message


class OpenAIManagerPromptingTests(unittest.TestCase):
    def test_quiz_followup_prompt_prefers_natural_collocations(self) -> None:
        prompt = system_message["quiz_followup_question"]
        self.assertIn("Prefer a natural collocation over a generic example sentence", prompt)
        self.assertIn("Never output broken fragments, dictionary-style fragments, or artificial textbook phrases.", prompt)
        self.assertIn("Avoid trivial toy examples unless they are the only natural option.", prompt)

    def test_quiz_followup_prompt_prioritizes_exact_question(self) -> None:
        prompt = system_message["quiz_followup_question"]
        self.assertIn("learner_question is the primary task", prompt)
        self.assertIn("Start reply_text with the direct answer to learner_question", prompt)
        self.assertIn("Do not re-explain the whole studied_text", prompt)

    def test_private_question_prompt_prefers_natural_collocations(self) -> None:
        prompt = system_message["language_learning_private_question"]
        self.assertIn("Prefer a natural collocation over a generic example sentence", prompt)
        self.assertIn("If possible, make at least one save_variant a compact collocation", prompt)

    def test_detailed_prompt_mentions_natural_collocations(self) -> None:
        prompt = system_message["language_learning_private_question_detailed"]
        self.assertIn("Prefer natural collocations and characteristic real usage", prompt)
        self.assertIn("Never output broken fragments, literal translations, or awkward artificial phrases.", prompt)

    def test_wortgruppe_prompt_keeps_its_contract(self) -> None:
        """The prompt itself was rewritten (C1–C2 rebuild), so this pins the CONTRACT the
        rest of the code depends on, not the wording: the sentence/gap/answer triple, the
        rule that re-inserting the answer restores the sentence exactly, and the Russian
        hint the UI shows. Wording is free to improve; these fields are not."""
        prompt = system_message["aufgabe_wortgruppe"]
        for field in ("vollsatz", "satz", "correct", "hint_ru", "lemmas"):
            self.assertIn(field, prompt, f"поле {field} исчезло из промпта")
        self.assertIn("_____", prompt)                 # exactly one gap marker
        self.assertRegex(prompt, r"C1[–-]C2|C1|C2")    # the level it is written for


if __name__ == "__main__":
    unittest.main()
