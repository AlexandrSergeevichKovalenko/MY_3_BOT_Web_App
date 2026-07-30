"""Вердикт верификатора привязывается к СЛОВУ, а не к позиции в списке.

Раньше ответы сопоставлялись через zip(candidates, verdicts): порядок гарантировала
только фраза «same order as input» в промпте. Пока рассинхрон стоил одного пропущенного
слова, это терпели. С появлением стоп-листа цена стала вечной: чужое «не годится»
занесло бы нормальное слово в чёрный список навсегда.

Здесь проверяется: ответы матчатся по слову; сдвинутый или короткий ответ никого не
хоронит; «артикль зависит от смысла» — не повод для стоп-листа (такие слова живут в игре
с переводом на экране)."""
import asyncio
import unittest
from unittest.mock import patch

import backend.article_sprint_generator as gen
import backend.article_word_gate as gate
import backend.database as db
import backend.openai_manager as om
import backend.article_sprint_themes as themes_mod


class VerifyAlignmentTests(unittest.TestCase):
    WORDS = [
        {"word": "Teller", "article": "der", "meaning_ru": "тарелка"},
        {"word": "Kühlschrank", "article": "der", "meaning_ru": "холодильник"},
    ]

    def _fill(self, generated, verdicts):
        async def _fake_gen(**kwargs):
            return list(generated)

        async def _fake_verify(items):
            return list(verdicts)

        inserted: list[dict] = []
        recorded: list[tuple] = []

        with patch.object(themes_mod, "article_sprint_themes", lambda: [
                {"key": "haus_wohnen", "label_de": "Haus", "target_count": 150,
                 "subtopics": ["Küche"]}]), \
                patch.object(db, "ensure_article_sprint_schema", lambda: None), \
                patch.object(db, "count_article_sprint_nouns", lambda *a, **k: 10), \
                patch.object(db, "list_article_sprint_words", lambda t: []), \
                patch.object(db, "list_article_sprint_meanings", lambda t: []), \
                patch.object(db, "list_retired_article_words", lambda: set()), \
                patch.object(db, "list_article_word_blacklist", lambda: set()), \
                patch.object(db, "blacklist_article_words", lambda items: recorded.extend(items)), \
                patch.object(db, "insert_article_sprint_nouns",
                             lambda theme, rows: (inserted.extend(rows), {"inserted": len(rows)})[1]), \
                patch.object(om, "run_article_noun_gen", _fake_gen), \
                patch.object(om, "run_article_verify", _fake_verify), \
                patch.object(gate, "judge_everyday_words", lambda words: {w: True for w in words}), \
                patch("backend.article_authority.authoritative_article",
                      lambda w, **k: ("der", "wiktionary")), \
                patch.object(gen, "apply_reference_plurals", lambda rows: 0), \
                patch.object(gen, "_run", lambda coro: asyncio.new_event_loop().run_until_complete(coro)):
            stats = gen.fill_theme("haus_wohnen", max_to_add=10)
        return stats, inserted, recorded

    def test_verdicts_are_matched_by_word(self):
        # Ответы пришли в обратном порядке — но с именами, значит всё сходится.
        _, inserted, recorded = self._fill(self.WORDS, [
            {"word": "Kühlschrank", "ok": True, "article": "der"},
            {"word": "Teller", "ok": True, "article": "der"},
        ])
        self.assertEqual(sorted(r["word"] for r in inserted), ["Kühlschrank", "Teller"])
        self.assertEqual(recorded, [])

    def test_a_shifted_answer_never_blacklists_the_wrong_word(self):
        # «Не годится» пришло на Kühlschrank; без матчинга по слову оно село бы на
        # Teller — и хорошая тарелка исчезла бы из всех тем навсегда.
        _, inserted, recorded = self._fill(self.WORDS, [
            {"word": "Kühlschrank", "ok": False, "reason": "not_noun"},
            {"word": "Teller", "ok": True, "article": "der"},
        ])
        self.assertEqual([r["word"] for r in inserted], ["Teller"])
        self.assertEqual([w for w, _r, _t in recorded], ["Kühlschrank"])

    def test_short_answer_buries_nobody(self):
        # Модель ответила про одно слово из двух: второе — «ответа не было», а не отказ.
        _, inserted, recorded = self._fill(self.WORDS, [
            {"word": "Teller", "ok": True, "article": "der"},
        ])
        self.assertEqual([r["word"] for r in inserted], ["Teller"])
        self.assertEqual(recorded, [], "молчание модели — не приговор слову")

    def test_nameless_answer_of_wrong_length_is_dropped_whole(self):
        # Старый формат ответа (без слова) и длина не сошлась — верить нечему.
        _, inserted, recorded = self._fill(self.WORDS, [{"ok": False}])
        self.assertEqual(inserted, [])
        self.assertEqual(recorded, [])

    def test_ambiguous_verdict_does_not_bury_the_word(self):
        # У слова артикль зависит от смысла — ему место в игре, с переводом на экране.
        _, inserted, recorded = self._fill(
            [{"word": "Band", "article": "das", "meaning_ru": "лента"}],
            [{"word": "Band", "ok": False, "reason": "ambiguous"}],
        )
        self.assertEqual(inserted, [])
        self.assertEqual(recorded, [], "двуродовому дорога в игру закрыта быть не должна")

    def test_person_adjective_verdict_does_not_bury_the_word(self):
        _, _, recorded = self._fill(
            [{"word": "Angestellte", "article": "die", "meaning_ru": "сотрудница"}],
            [{"word": "Angestellte", "ok": False, "reason": "person_adjective"}],
        )
        self.assertEqual(recorded, [])


if __name__ == "__main__":
    unittest.main()
