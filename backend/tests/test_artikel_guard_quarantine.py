"""У стража на приёмке три ответа, а не два.

Раньше их было два: «беру» и «в стоп-лист навсегда». Второй молчаливый — слово в банк не
попадало, владелец его не видел никогда, только счётчик в /artikel_blacklist. Так вместе
с хламом хоронились и ошибки стража: он режет по бытовой полезности, а она не абсолютна.

Третий ответ — карантин: слово, которое страж не пропустил, но которое по частотности
выглядит ходовым, кладётся в банк снятым и уезжает владельцу в дневной разбор.
"""
import asyncio
import unittest
from unittest.mock import patch

import backend.article_sprint_generator as gen
import backend.article_sprint_themes as themes_mod
import backend.article_word_gate as gate
import backend.database as db
import backend.openai_manager as om
from backend.article_retire_review import _word_text


class GuardThirdAnswerTests(unittest.TestCase):
    def _fill(self, generated, *, judge):
        async def _fake_gen(**kwargs):
            return list(generated)

        async def _fake_verify(items):
            return [{"ok": True, "article": "der"}] * len(items)

        blacklisted: list[tuple] = []
        quarantined: list[dict] = []

        with patch.object(themes_mod, "article_sprint_themes", lambda: [
                {"key": "wetter", "label_de": "Wetter", "target_count": 150,
                 "subtopics": ["Sturm"]}]), \
                patch.object(db, "ensure_article_sprint_schema", lambda: None), \
                patch.object(db, "count_article_sprint_nouns", lambda *a, **k: 10), \
                patch.object(db, "list_article_sprint_words", lambda t: []), \
                patch.object(db, "list_article_sprint_words_all_themes", set), \
                patch.object(db, "list_article_sprint_meanings", lambda t: []), \
                patch.object(db, "list_retired_article_words", set), \
                patch.object(db, "list_article_word_blacklist", set), \
                patch.object(db, "list_retired_article_words_for_prompt", lambda t: []), \
                patch.object(db, "blacklist_article_words",
                             lambda items: blacklisted.extend(items) or len(list(items))), \
                patch.object(db, "quarantine_article_sprint_nouns",
                             lambda t, rows: quarantined.extend(rows) or len(rows)), \
                patch.object(db, "insert_article_sprint_nouns",
                             lambda t, rows: {"inserted": len(rows), "skipped": 0}), \
                patch.object(om, "run_article_noun_gen", _fake_gen), \
                patch.object(om, "run_article_verify", _fake_verify), \
                patch.object(gate, "judge_everyday_words", lambda words: judge), \
                patch.object(gen, "_run",
                             lambda coro: asyncio.new_event_loop().run_until_complete(coro)):
            stats = gen.fill_theme("wetter", max_to_add=10)
        return stats, blacklisted, quarantined

    def test_a_common_word_the_guard_refused_goes_to_the_owner(self):
        # Föhn — 33 039-е место: страж сказал «нет», но слово ходовое, значит он мог
        # и ошибиться. Хоронить такое молча нельзя.
        stats, blacklisted, quarantined = self._fill(
            generated=[{"word": "Föhn", "article": "der", "meaning_ru": "фен"}],
            judge={"Föhn": False},
        )
        self.assertEqual([q["word"] for q in quarantined], ["Föhn"])
        self.assertEqual(blacklisted, [], "спорное в стоп-лист не пишем — решает владелец")
        self.assertEqual(stats["quarantined"], 1)

    def test_real_junk_still_goes_to_the_stop_list_silently(self):
        # Слова нет в частотном списке вовсе — спрашивать про такое владельца незачем.
        _, blacklisted, quarantined = self._fill(
            generated=[{"word": "Schmetterlingstramete", "article": "die", "meaning_ru": "губка"}],
            judge={"Schmetterlingstramete": False},
        )
        self.assertEqual(quarantined, [])
        self.assertEqual([w for w, _r, _t in blacklisted], ["Schmetterlingstramete"])

    def test_the_word_the_guard_accepted_just_gets_in(self):
        _, blacklisted, quarantined = self._fill(
            generated=[{"word": "Föhn", "article": "der", "meaning_ru": "фен"}],
            judge={"Föhn": True},
        )
        self.assertEqual((blacklisted, quarantined), ([], []))

    def test_quarantined_word_keeps_its_meaning_and_article(self):
        # Без перевода и артикля владельцу в личке нечего показать.
        _, _, quarantined = self._fill(
            generated=[{"word": "Föhn", "article": "der", "meaning_ru": "фен"}],
            judge={"Föhn": False},
        )
        self.assertEqual(quarantined[0]["article"], "der")
        self.assertEqual(quarantined[0]["meaning_ru"], "фен")


class QuarantineDmWordingTests(unittest.TestCase):
    """Про слово, которого в игре не было, нельзя писать «убрано при чистке»."""

    def _text(self, **extra):
        item = {"word": "Föhn", "article": "der", "meaning_ru": "фен", "rank": 33039}
        item.update(extra)
        return _word_text(item, index=1, total=10, left=5)

    def test_a_quarantined_word_is_described_as_never_admitted(self):
        text = self._text(quarantined=True)
        self.assertIn("не пропустила", text)
        self.assertNotIn("убрано из игры при чистке", text)

    def test_a_cleaned_word_is_still_described_as_removed(self):
        text = self._text()
        self.assertIn("убрано из игры при чистке", text)

    def test_both_kinds_show_the_word_and_its_meaning(self):
        for text in (self._text(), self._text(quarantined=True)):
            self.assertIn("der Föhn", text)
            self.assertIn("фен", text)


if __name__ == "__main__":
    unittest.main()
