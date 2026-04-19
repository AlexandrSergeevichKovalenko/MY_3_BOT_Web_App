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

from backend.translation_workflow import (
    _filter_sentence_entries_for_session,
    _insert_sentence_entries_into_session_with_cursor,
    _translation_fill_reached_target,
)
from backend.grammar_focuses import get_legacy_shared_pool_focus, resolve_shared_sentence_pool_focus


class TranslationSentenceSelectionTests(unittest.TestCase):
    def test_legacy_shared_pool_focus_uses_small_bucket_family(self):
        payload = get_legacy_shared_pool_focus("b1")

        self.assertIsNotNone(payload)
        self.assertEqual(payload["kind"], "legacy_pool")
        self.assertEqual(payload["key"], "legacy_general_b1")
        self.assertEqual(payload["_pool_levels"], ["b1"])

    def test_resolve_shared_sentence_pool_focus_maps_legacy_focus_by_level(self):
        payload = resolve_shared_sentence_pool_focus({"kind": "legacy"}, "c1")

        self.assertIsNotNone(payload)
        self.assertEqual(payload["key"], "legacy_general_c1")
        self.assertEqual(payload["_pool_levels"], ["c1"])

    def test_insert_limit_does_not_count_mastered_sentences_against_session_capacity(self):
        class _Cursor:
            def __init__(self):
                self._fetchone = None
                self._fetchall = []
                self.inserted_rows = []

            def execute(self, query, params=None):
                normalized = " ".join(str(query).split())
                if "SELECT id, sentence FROM bt_3_daily_sentences" in normalized:
                    self._fetchall = []
                    return
                if "SELECT COALESCE(MAX(unique_id), 0)" in normalized:
                    self._fetchone = (0,)
                    return
                if "SELECT id_for_mistake_table FROM bt_3_daily_sentences" in normalized:
                    self._fetchone = None
                    return
                if "SELECT MAX(id_for_mistake_table) FROM bt_3_daily_sentences" in normalized:
                    self._fetchone = (9000 + len(self.inserted_rows),)
                    return
                if "INSERT INTO bt_3_daily_sentences" in normalized:
                    self.inserted_rows.append(tuple(params or ()))
                    self._fetchone = (len(self.inserted_rows),)
                    return
                self._fetchone = None
                self._fetchall = []

            def fetchall(self):
                return list(self._fetchall)

            def fetchone(self):
                return self._fetchone

        cursor = _Cursor()
        entries = [
            {"sentence": f"Новое предложение {index}.", "tested_skill_profile": []}
            for index in range(1, 8)
        ]

        from unittest.mock import patch

        with patch(
            "backend.translation_workflow._get_mastered_sentence_keys_with_cursor",
            return_value={"mastered a", "mastered b"},
        ), patch(
            "backend.translation_workflow._insert_sentence_skill_targets_for_entries_with_cursor",
            return_value=None,
        ), patch(
            "backend.translation_workflow._record_translation_bucket_demand_with_cursor",
            return_value=None,
        ):
            inserted = _insert_sentence_entries_into_session_with_cursor(
                cursor,
                user_id=117649764,
                session_id=123456,
                source_lang="ru",
                target_lang="de",
                focus_key="main_clause_v2",
                level="c1",
                sentence_entries=entries,
                limit=7,
            )

        self.assertEqual(len(inserted), 7)
        self.assertEqual(len(cursor.inserted_rows), 7)

    def test_translation_fill_reached_target_only_when_ready_count_meets_expected_total(self):
        self.assertTrue(_translation_fill_reached_target({"ready_count": 7, "expected_total": 7}))
        self.assertFalse(_translation_fill_reached_target({"ready_count": 5, "expected_total": 7}))
        self.assertFalse(_translation_fill_reached_target({"ready_count": 0, "expected_total": 0}))

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
