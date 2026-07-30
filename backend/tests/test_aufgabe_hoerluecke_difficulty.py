"""Hörlücke must hide WORD GROUPS, not single words.

A blank swallowing one unstressed function word ("_____ das Wochenende" → "auf") is
guessed from the printed rest without listening. The rule lives in ONE place
(`hoerluecke_gaps_are_hard`) and is used by the build gate, the ingest guard and
`is_degenerate_aufgabe` — these tests pin that they agree, otherwise the pool either
drains (generate → instantly purge) or serves one-word blanks again.
"""

import unittest

import bot_3
from backend.answer_eval import _aufgabe_result_payload, _check_aufgabe, aufgabe_client_meta
from backend.database import (
    hoerluecke_gap_is_hard, hoerluecke_gaps_are_hard, hoerluecke_repair_item,
    hoerluecke_transcript_matches_audio, is_degenerate_aufgabe,
)


def _gaps(*corrects):
    return [{"correct": c, "aliases": []} for c in corrects]


HARD_GAPS = ("auf das lange Wochenende", "mit den neuen Kollegen", "darüber gesprochen",
             "um das Essen für alle")
SATZ_VOLL = ("Ich freue mich schon lange auf das lange Wochenende. Am Samstag treffe ich "
             "mich mit den neuen Kollegen im Park. Wir haben gestern Abend ausführlich "
             "darüber gesprochen, wohin wir danach gehen. Am Abend kümmert sich Jan um das "
             "Essen für alle.")
TRANSCRIPT = ("Ich freue mich schon lange _____. Am Samstag treffe ich mich _____ im Park. "
              "Wir haben gestern Abend ausführlich _____, wohin wir danach gehen. Am Abend "
              "kümmert sich Jan _____.")


class HoerlueckeGapRuleTests(unittest.TestCase):
    def test_single_word_and_function_only_chunks_rejected(self) -> None:
        for easy in ["auf", "im", "darüber", "Trotzdem", "auf das", "mit dem",
                     "sich damit", "hat sich", "in dem", "es sich", ""]:
            self.assertFalse(hoerluecke_gap_is_hard(easy), msg=easy)

    def test_word_groups_with_a_content_word_accepted(self) -> None:
        for hard in ["auf das Wochenende", "kümmert sich um", "freue mich auf",
                     "mit den neuen Kollegen", "darüber gesprochen", "des Projekts",
                     "abgesagt worden", "obwohl wir warten"]:
            self.assertTrue(hoerluecke_gap_is_hard(hard), msg=hard)

    def test_overlong_chunk_rejected(self) -> None:
        # 6+ words is a whole clause — not reconstructable from two listens.
        self.assertFalse(hoerluecke_gap_is_hard("freuen sich alle auf die gemeinsame Feier"))

    def test_item_needs_three_hard_gaps(self) -> None:
        self.assertTrue(hoerluecke_gaps_are_hard(_gaps(*HARD_GAPS)))
        self.assertFalse(hoerluecke_gaps_are_hard(_gaps(*HARD_GAPS[:2])))  # too few blanks
        self.assertFalse(hoerluecke_gaps_are_hard(  # one easy gap poisons the item
            _gaps("auf das Wochenende", "darüber gesprochen", "auf")))
        self.assertFalse(hoerluecke_gaps_are_hard([]))
        self.assertFalse(hoerluecke_gaps_are_hard(None))

    def test_three_minimal_chunks_are_not_enough(self) -> None:
        # 3 gaps × 2 words = 6 hidden words: every blank is just article+noun, no real
        # construction to hear. One of them has to be a bigger group.
        self.assertFalse(hoerluecke_gaps_are_hard(
            _gaps("die Gespräche", "mit Freunden", "leise Musik")))
        self.assertTrue(hoerluecke_gaps_are_hard(
            _gaps("die Gespräche", "mit Freunden", "auf das lange Wochenende")))


class HoerlueckeAudioMatchTests(unittest.TestCase):
    def test_printed_text_must_reproduce_the_audio(self) -> None:
        self.assertTrue(hoerluecke_transcript_matches_audio(
            TRANSCRIPT, _gaps(*HARD_GAPS), SATZ_VOLL))
        # the model rewrote a word between audio text and printed text → broken item
        drifted = SATZ_VOLL.replace("im Park", "im Garten")
        self.assertFalse(hoerluecke_transcript_matches_audio(
            TRANSCRIPT, _gaps(*HARD_GAPS), drifted))
        self.assertTrue(hoerluecke_transcript_matches_audio(  # commas don't get spoken
            TRANSCRIPT, _gaps(*HARD_GAPS), SATZ_VOLL.replace(",", "")))


class HoerlueckeRepairTests(unittest.TestCase):
    def test_weak_gap_is_given_back_to_the_printed_text(self) -> None:
        transcript = ("Ich freue mich schon lange _____. Am Samstag treffe ich mich _____ "
                      "im Park. Wir _____ gestern Abend ausführlich _____, wohin wir gehen.")
        gaps = _gaps("auf das lange Wochenende", "mit den neuen Kollegen", "haben wir",
                     "darüber gesprochen")
        fixed_t, kept = hoerluecke_repair_item(transcript, gaps)
        self.assertEqual([g["correct"] for g in kept],
                         ["auf das lange Wochenende", "mit den neuen Kollegen",
                          "darüber gesprochen"])
        self.assertEqual(fixed_t.count("_____"), 3)
        self.assertIn("Wir haben wir gestern", fixed_t)  # the weak words are printed now
        self.assertTrue(hoerluecke_gaps_are_hard(kept))

    def test_structurally_broken_item_is_left_untouched(self) -> None:
        # transcript slots ≠ gaps → nothing to splice; the gate drops it.
        transcript = "Ich freue mich _____ und _____ und _____."
        gaps = _gaps("auf das Wochenende", "auf")
        self.assertEqual(hoerluecke_repair_item(transcript, gaps), (transcript, gaps))


class HoerlueckeSelfHealTests(unittest.TestCase):
    def test_legacy_single_word_item_is_degenerate(self) -> None:
        # Pre-hardening rows (one blank, one word) must be retired at serve time.
        self.assertTrue(is_degenerate_aufgabe("hoerluecke", {
            "satz_luecke": "Ich freue mich sehr _____ das Wochenende.",
            "satz_voll": "Ich freue mich sehr auf das Wochenende.",
            "correct": "auf",
        }))
        # …and so are the multi-gap items that hid one preposition per blank.
        self.assertTrue(is_degenerate_aufgabe("hoerluecke", {
            "satz_voll": SATZ_VOLL,
            "transcript": ("Ich freue mich schon lange _____ das lange Wochenende. Am "
                           "Samstag treffe ich mich _____ den neuen Kollegen im Park. Wir "
                           "haben gestern Abend ausführlich _____ gesprochen, wohin wir "
                           "danach gehen. Am Abend kümmert sich Jan um das Essen für alle."),
            "gaps": _gaps("auf", "mit", "darüber"),
        }))

    def test_word_group_item_survives(self) -> None:
        self.assertFalse(is_degenerate_aufgabe("hoerluecke", {
            "satz_voll": SATZ_VOLL, "transcript": TRANSCRIPT, "gaps": _gaps(*HARD_GAPS)}))

    def test_item_whose_text_drifted_from_the_audio_is_degenerate(self) -> None:
        self.assertTrue(is_degenerate_aufgabe("hoerluecke", {
            "satz_voll": SATZ_VOLL.replace("im Park", "im Garten"),
            "transcript": TRANSCRIPT, "gaps": _gaps(*HARD_GAPS)}))


class HoerlueckeBuildGateTests(unittest.TestCase):
    def _item(self, transcript, *corrects, satz_voll=SATZ_VOLL):
        return {
            "satz_voll": satz_voll,
            "transcript": transcript,
            "gaps": _gaps(*corrects),
            "erklaerung": "…", "tip": "…", "hint_ru": "…",
        }

    def test_easy_item_is_not_built(self) -> None:
        transcript = ("Ich freue mich schon lange _____ das lange Wochenende. Am Samstag "
                      "treffe ich mich _____ den neuen Kollegen im Park. Wir haben gestern "
                      "Abend ausführlich _____ gesprochen, wohin wir danach gehen. Am Abend "
                      "kümmert sich Jan um das Essen für alle.")
        item = self._item(transcript, "auf", "mit", "darüber")
        self.assertIsNone(bot_3._aufgabe_payload_from_item("hoerluecke", item))

    def test_word_group_item_is_built(self) -> None:
        payload = bot_3._aufgabe_payload_from_item(
            "hoerluecke", self._item(TRANSCRIPT, *HARD_GAPS))
        self.assertIsNotNone(payload)
        self.assertEqual([g["correct"] for g in payload["gaps"]], list(HARD_GAPS))
        # Build gate and serve-time self-heal must agree on the very same payload.
        self.assertFalse(is_degenerate_aufgabe("hoerluecke", payload))

    def test_item_with_one_weak_gap_is_repaired_not_dropped(self) -> None:
        transcript = ("Ich freue mich schon lange _____. Am Samstag treffe ich mich _____ "
                      "im Park. Wir _____ gestern Abend ausführlich _____, wohin wir danach "
                      "gehen. Am Abend kümmert sich Jan _____.")
        item = self._item(transcript, "auf das lange Wochenende", "mit den neuen Kollegen",
                          "haben wir", "darüber gesprochen", "um das Essen für alle")
        # the audio text carries the weak words too, so filling them back stays faithful
        item["satz_voll"] = SATZ_VOLL.replace("Wir haben gestern", "Wir haben wir gestern")
        payload = bot_3._aufgabe_payload_from_item("hoerluecke", item)
        self.assertIsNotNone(payload)
        self.assertEqual([g["correct"] for g in payload["gaps"]],
                         ["auf das lange Wochenende", "mit den neuen Kollegen",
                          "darüber gesprochen", "um das Essen für alle"])
        self.assertFalse(is_degenerate_aufgabe("hoerluecke", payload))

    def test_text_not_matching_the_audio_is_not_built(self) -> None:
        item = self._item(TRANSCRIPT, *HARD_GAPS,
                          satz_voll=SATZ_VOLL.replace("im Park", "im Garten"))
        self.assertIsNone(bot_3._aufgabe_payload_from_item("hoerluecke", item))


class HoerlueckeAnswerTests(unittest.TestCase):
    payload = {
        "transcript": "Ich freue mich schon lange _____. Wir treffen uns _____ im Park. "
                      "Wir haben gestern lange _____.",
        "gaps": [
            {"correct": "auf das Wochenende", "aliases": []},
            {"correct": "mit den neuen Kollegen", "aliases": []},
            {"correct": "darüber gesprochen", "aliases": ["darueber gesprochen"]},
        ],
    }

    def test_full_groups_accepted(self) -> None:
        self.assertTrue(_check_aufgabe(
            "hoerluecke", self.payload,
            "auf das Wochenende|mit den neuen Kollegen|darüber gesprochen"))
        # keyboard without umlauts + sloppy spacing still passes
        self.assertTrue(_check_aufgabe(
            "hoerluecke", self.payload,
            "auf das Wochenende|mit den neuen Kollegen|  darueber gesprochen "))

    def test_partial_or_reordered_group_rejected(self) -> None:
        self.assertFalse(_check_aufgabe(  # only the preposition typed
            "hoerluecke", self.payload, "auf|mit den neuen Kollegen|darüber gesprochen"))
        self.assertFalse(_check_aufgabe(  # noun without its preposition
            "hoerluecke", self.payload,
            "das Wochenende|mit den neuen Kollegen|darüber gesprochen"))
        self.assertFalse(_check_aufgabe(  # wrong order inside the group
            "hoerluecke", self.payload,
            "auf das Wochenende|mit den neuen Kollegen|gesprochen darüber"))

    def test_result_compares_every_gap_with_what_was_typed(self) -> None:
        # The review must answer "where exactly did I go wrong": each gap shows the
        # spoken group AND the learner's own text, not one joined correct-answer line.
        dispatch = {"format": "hoerluecke", "payload": self.payload}
        typed = "auf das Wochenende|mit den Kollegen|darüber geschrieben"
        res = _aufgabe_result_payload(dispatch, is_correct=False, already_answered=False,
                                      user_answer=typed)
        self.assertEqual([(g["n"], g["ok"]) for g in res["gaps"]],
                         [(1, True), (2, False), (3, False)])
        self.assertEqual([g["user"] for g in res["gaps"]],
                         ["auf das Wochenende", "mit den Kollegen", "darüber geschrieben"])
        self.assertEqual([g["correct"] for g in res["gaps"]],
                         [g["correct"] for g in self.payload["gaps"]])

    def test_result_never_marks_an_accepted_answer_wrong(self) -> None:
        # Row verdicts use the grader's own matcher: an alias / umlaut-free spelling that
        # the grader accepts must not show up as ❌ in the review.
        dispatch = {"format": "hoerluecke", "payload": self.payload}
        typed = "AUF DAS WOCHENENDE|mit den neuen Kollegen|darueber gesprochen"
        self.assertTrue(_check_aufgabe("hoerluecke", self.payload, typed))
        res = _aufgabe_result_payload(dispatch, is_correct=True, already_answered=False,
                                      user_answer=typed)
        self.assertTrue(all(g["ok"] for g in res["gaps"]))

    def test_result_marks_an_unanswered_gap(self) -> None:
        dispatch = {"format": "hoerluecke", "payload": self.payload}
        res = _aufgabe_result_payload(dispatch, is_correct=False, already_answered=False,
                                      user_answer="auf das Wochenende")
        self.assertEqual([g["user"] for g in res["gaps"]], ["auf das Wochenende", "", ""])
        self.assertEqual([g["ok"] for g in res["gaps"]], [True, False, False])

    def test_client_meta_reveals_only_the_word_count(self) -> None:
        meta = aufgabe_client_meta("hoerluecke", self.payload)
        self.assertEqual(meta["gap_words"], [3, 4, 2])
        self.assertEqual(meta["gap_count"], 3)
        blob = repr(meta)
        for spoiler in ("Wochenende", "Kollegen", "gesprochen"):
            self.assertNotIn(spoiler, blob.replace(self.payload["transcript"], ""))


if __name__ == "__main__":
    unittest.main()
