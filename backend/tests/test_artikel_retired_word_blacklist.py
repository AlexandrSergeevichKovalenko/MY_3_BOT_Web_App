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
    def _fill(self, generated, retired):
        async def _fake_gen(**kwargs):
            return list(generated)

        inserted: list[dict] = []

        def _fake_insert(theme_key, rows):
            inserted.extend(rows)
            return {"inserted": len(rows), "skipped": 0}

        def _no_judge(words):
            raise _Judged(str(words))

        with patch.object(themes_mod, "article_sprint_themes", lambda: [
                {"key": "wetter", "label_de": "Wetter", "target_count": 150,
                 "subtopics": ["Sturm"]}]), \
                patch.object(db, "ensure_article_sprint_schema", lambda: None), \
                patch.object(db, "count_article_sprint_nouns", lambda *a, **k: 10), \
                patch.object(db, "list_article_sprint_words", lambda t: ["Regen"]), \
                patch.object(db, "list_article_sprint_meanings", lambda t: ["дождь"]), \
                patch.object(db, "list_retired_article_words", lambda: set(retired)), \
                patch.object(db, "insert_article_sprint_nouns", _fake_insert), \
                patch.object(om, "run_article_noun_gen", _fake_gen), \
                patch.object(gate, "judge_everyday_words", _no_judge), \
                patch.object(gen, "_run", lambda coro: asyncio.new_event_loop().run_until_complete(coro)):
            stats = gen.fill_theme("wetter", max_to_add=10)
        return stats, inserted

    def test_retired_word_is_not_taken_back(self):
        stats, inserted = self._fill(
            generated=[{"word": "Föhnsturm", "article": "der", "meaning_ru": "фён"}],
            retired={"föhnsturm"},
        )
        self.assertEqual(inserted, [], "снятое слово не должно вернуться в банк")
        self.assertEqual(stats["added"], 0)
        self.assertEqual(stats["rejected"], 1)

    def test_blacklist_is_checked_before_the_paid_second_opinion(self):
        # Föhnsturm по частотности не проходит → без чёрного списка ушёл бы к модели.
        # Заглушка судьи бросает исключение: если тест зелёный, платный запрос не звучал.
        stats, _ = self._fill(
            generated=[{"word": "Föhnsturm", "article": "der", "meaning_ru": "фён"}],
            retired={"föhnsturm"},
        )
        self.assertEqual(stats["rejected"], 1)

    def test_a_word_retired_in_another_theme_is_blocked_too(self):
        # Мусорность — свойство слова, а не темы.
        _, inserted = self._fill(
            generated=[{"word": "Sanddorn", "article": "der", "meaning_ru": "облепиха"}],
            retired={"sanddorn"},
        )
        self.assertEqual(inserted, [])


if __name__ == "__main__":
    unittest.main()
