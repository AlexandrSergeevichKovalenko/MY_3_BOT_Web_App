import unittest
import sys
import types


if "openai" not in sys.modules:
    openai_stub = types.ModuleType("openai")

    class _RateLimitError(Exception):
        pass

    openai_stub.RateLimitError = _RateLimitError
    sys.modules["openai"] = openai_stub

if "backend.openai_manager" not in sys.modules:
    openai_manager_stub = types.ModuleType("backend.openai_manager")

    async def _unused_async(*args, **kwargs):
        return {}

    openai_manager_stub.generate_sentences_multilang = _unused_async
    openai_manager_stub.llm_execute = _unused_async
    openai_manager_stub.run_check_translation_multilang = _unused_async
    openai_manager_stub.run_check_translation_story = _unused_async
    openai_manager_stub.run_check_story_guess_semantic = _unused_async
    sys.modules["backend.openai_manager"] = openai_manager_stub

if "psycopg2" not in sys.modules:
    psycopg2_stub = types.ModuleType("psycopg2")
    psycopg2_stub.Binary = lambda value: value

    class _OperationalError(Exception):
        pass

    psycopg2_stub.OperationalError = _OperationalError
    psycopg2_stub.connect = lambda *args, **kwargs: None
    psycopg2_stub.sql = types.SimpleNamespace(SQL=lambda value: value, Identifier=lambda value: value)
    sys.modules["psycopg2"] = psycopg2_stub

    psycopg2_extras_stub = types.ModuleType("psycopg2.extras")
    psycopg2_extras_stub.Json = lambda value: value
    psycopg2_extras_stub.execute_values = lambda *args, **kwargs: None
    sys.modules["psycopg2.extras"] = psycopg2_extras_stub

    psycopg2_pool_stub = types.ModuleType("psycopg2.pool")

    class _PoolError(Exception):
        pass

    class _ThreadedConnectionPool:
        def __init__(self, *args, **kwargs):
            pass

        def getconn(self):
            return None

        def putconn(self, conn, close=False):
            return None

        def closeall(self):
            return None

    psycopg2_pool_stub.ThreadedConnectionPool = _ThreadedConnectionPool
    psycopg2_pool_stub.PoolError = _PoolError
    sys.modules["psycopg2.pool"] = psycopg2_pool_stub

from backend.translation_workflow import _filter_sentence_entries_for_session


class TranslationSentenceSelectionTests(unittest.TestCase):
    def test_filters_recently_served_sentences(self):
        entries = [
            {"sentence": "Я живу в Вене.", "tested_skill_profile": [{"skill_id": "a"}]},
            {"sentence": "Завтра мы поедем в музей.", "tested_skill_profile": [{"skill_id": "b"}]},
        ]

        filtered = _filter_sentence_entries_for_session(
            entries,
            level="a2",
            excluded_sentence_keys={"я живу в вене."},
        )

        self.assertEqual([item["sentence"] for item in filtered], ["Завтра мы поедем в музей."])

    def test_filters_out_sentences_that_do_not_fit_level(self):
        entries = [
            {
                "sentence": "Хотя комитет уже несколько месяцев обсуждает реформу, окончательное решение всё ещё откладывается из-за политических разногласий.",
                "tested_skill_profile": [{"skill_id": "a"}],
            },
            {
                "sentence": "Я завтра работаю дома.",
                "tested_skill_profile": [{"skill_id": "b"}],
            },
        ]

        filtered = _filter_sentence_entries_for_session(entries, level="a2")

        self.assertEqual([item["sentence"] for item in filtered], ["Я завтра работаю дома."])

    def test_dedupes_normalized_sentences(self):
        entries = [
            {"sentence": "Я завтра работаю дома.", "tested_skill_profile": [{"skill_id": "a"}]},
            {"sentence": "  Я   завтра   работаю   дома.  ", "tested_skill_profile": [{"skill_id": "b"}]},
        ]

        filtered = _filter_sentence_entries_for_session(entries, level="a2")

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["sentence"], "Я завтра работаю дома.")


if __name__ == "__main__":
    unittest.main()
