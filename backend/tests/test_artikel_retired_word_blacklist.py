"""Снятое слово не возвращается в банк артиклей при следующем наполнении темы.

Строка снятого слова не удаляется — она и есть память «это мы уже выбросили». Но замок
в базе (ON CONFLICT по теме+слову+артиклю) держит только внутри темы: 34 живых слова уже
пролезли в соседнюю тему, будучи снятыми в другой (Sanddorn, Mispel, Tintenfisch).
Поэтому наполнение сверяется со списком снятых слов по ВСЕМ темам и отсекает их до
платного «второго мнения» модели."""
import asyncio
import unittest
from unittest.mock import patch

import backend.article_sprint_generator as gen
import backend.article_word_gate as gate
import backend.database as db
import backend.openai_manager as om
import backend.article_sprint_themes as themes_mod


class _Judged(Exception):
    """Платный запрос «нужно ли слово в быту» — по снятым словам он звучать не должен."""


class RetiredWordsStayOutTests(unittest.TestCase):
    def _fill(self, generated, retired, *, blacklist=(), judge=None, verify=None):
        async def _fake_gen(**kwargs):
            return list(generated)

        async def _fake_verify(items):
            return list(verify) if verify is not None else [{"ok": True, "article": "der"}] * len(items)

        inserted: list[dict] = []
        recorded: list[tuple] = []

        def _fake_insert(theme_key, rows):
            inserted.extend(rows)
            return {"inserted": len(rows), "skipped": 0}

        def _fake_blacklist(items):
            recorded.extend(list(items or []))
            return len(list(items or []))

        def _judge(words):
            if judge is None:
                raise _Judged(str(words))
            if isinstance(judge, Exception):
                raise judge
            return {w: judge.get(w, False) for w in words}

        with patch.object(themes_mod, "article_sprint_themes", lambda: [
                {"key": "wetter", "label_de": "Wetter", "target_count": 150,
                 "subtopics": ["Sturm"]}]), \
                patch.object(db, "ensure_article_sprint_schema", lambda: None), \
                patch.object(db, "count_article_sprint_nouns", lambda *a, **k: 10), \
                patch.object(db, "list_article_sprint_words", lambda t: ["Regen"]), \
                patch.object(db, "list_article_sprint_meanings", lambda t: ["дождь"]), \
                patch.object(db, "list_retired_article_words", lambda: set(retired)), \
                patch.object(db, "list_article_word_blacklist", lambda: set(blacklist)), \
                patch.object(db, "blacklist_article_words", _fake_blacklist), \
                patch.object(db, "insert_article_sprint_nouns", _fake_insert), \
                patch.object(om, "run_article_noun_gen", _fake_gen), \
                patch.object(om, "run_article_verify", _fake_verify), \
                patch.object(gate, "judge_everyday_words", _judge), \
                patch.object(gen, "_run", lambda coro: asyncio.new_event_loop().run_until_complete(coro)):
            stats = gen.fill_theme("wetter", max_to_add=10)
        return stats, inserted, recorded

    def test_retired_word_is_not_taken_back(self):
        stats, inserted, _ = self._fill(
            generated=[{"word": "Föhnsturm", "article": "der", "meaning_ru": "фён"}],
            retired={"föhnsturm"},
        )
        self.assertEqual(inserted, [], "снятое слово не должно вернуться в банк")
        self.assertEqual(stats["added"], 0)
        self.assertEqual(stats["rejected"], 1)

    def test_blacklist_is_checked_before_the_paid_second_opinion(self):
        # Föhnsturm по частотности не проходит → без стоп-листа ушёл бы к модели.
        # Заглушка судьи бросает исключение: если тест зелёный, платный запрос не звучал.
        stats, _, _ = self._fill(
            generated=[{"word": "Föhnsturm", "article": "der", "meaning_ru": "фён"}],
            retired=set(), blacklist={"föhnsturm"},
        )
        self.assertEqual(stats["rejected"], 1)

    def test_a_word_retired_in_another_theme_is_blocked_too(self):
        # Мусорность — свойство слова, а не темы.
        _, inserted, _ = self._fill(
            generated=[{"word": "Sanddorn", "article": "der", "meaning_ru": "облепиха"}],
            retired={"sanddorn"},
        )
        self.assertEqual(inserted, [])


class RejectionsAreRememberedTests(unittest.TestCase):
    """Отказ должен запоминаться — но только тот, что про само слово."""

    _fill = RetiredWordsStayOutTests._fill

    def test_model_verdict_no_goes_to_the_stop_list(self):
        _, inserted, recorded = self._fill(
            generated=[{"word": "Föhnsturm", "article": "der", "meaning_ru": "фён"}],
            retired=set(), judge={"Föhnsturm": False},
        )
        self.assertEqual(inserted, [])
        self.assertEqual([(w, r) for w, r, _t in recorded], [("Föhnsturm", "не нужно в быту")])

    def test_silence_from_the_model_is_not_a_verdict(self):
        # Обрыв сети иначе занёс бы в стоп-лист навсегда целую пачку нормальных слов.
        stats, inserted, recorded = self._fill(
            generated=[{"word": "Föhnsturm", "article": "der", "meaning_ru": "фён"}],
            retired=set(), judge=gate.EverydayJudgeUnavailable("timeout"),
        )
        self.assertEqual(inserted, [], "сомнительное при молчании модели не берём")
        self.assertEqual(recorded, [], "но и в стоп-лист не пишем")
        self.assertEqual(stats["rejected"], 1)

    def test_verify_says_not_a_noun(self):
        _, _, recorded = self._fill(
            generated=[{"word": "Regenbogen", "article": "der", "meaning_ru": "радуга"}],
            retired=set(), verify=[{"ok": False}],
        )
        self.assertEqual([r for _w, r, _t in recorded], ["не существительное / не годится"])

    def test_a_theme_local_rejection_is_not_remembered(self):
        # «Уже есть в теме» — про тему, а не про слово: в соседней оно может быть нужно.
        _, _, recorded = self._fill(
            generated=[{"word": "Regen", "article": "der", "meaning_ru": "дождь"}],
            retired=set(),
        )
        self.assertEqual(recorded, [])


if __name__ == "__main__":
    unittest.main()
