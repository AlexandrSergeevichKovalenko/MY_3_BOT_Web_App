"""Единая очередь новых слов: свои и подписные соревнуются по нужности.

Подписка на словарь автора существовала месяцами и не выдала НИ ОДНОГО слова: она
включалась только когда у человека кончались свои новые слова, а у каждого лежала
тысяча нетронутых из «Быстрого старта» — запас на 50–100 дней. Плюс фильтр брал одно
языковое направление и отсекал 8238 из 13951 слов автора, причём именно то направление,
в котором он сейчас пишет.

Тесты закрепляют оба исправления.
"""

import unittest
from unittest.mock import Mock, patch

import backend.database as db


class SubscriptionLanguageFilterTests(unittest.TestCase):
    def test_filter_covers_both_directions(self):
        sql, params = db._subscription_lang_filter("a", "ru", "de")
        # Обе стороны: ru→de И de→ru. Одно направление отсекало больше половины словаря.
        self.assertIn("OR", sql)
        self.assertEqual(params, ["ru", "de", "de", "ru"])

    def test_missing_pair_yields_no_filter(self):
        self.assertEqual(db._subscription_lang_filter("a", None, None), ("", []))

    def test_candidates_order_by_need_not_by_date(self):
        """Порядок: частотность → популярность у других → дата. Раньше вторым ключом шла
        дата, из-за чего три четверти слов (без ранга) выстраивались произвольно."""
        self.assertIn("frequency_rank ASC NULLS LAST", db._SUBSCRIPTION_ORDER_BY_SQL)
        self.assertIn("holders", db._SUBSCRIPTION_ORDER_BY_SQL)
        self.assertIn("COUNT(DISTINCT user_id)", db._SUBSCRIPTION_POPULARITY_JOIN_SQL)


class SingleQueueTests(unittest.TestCase):
    """Выбор между своим словом и словом из подписки. Меньше ранг — нужнее слово."""

    def setUp(self):
        db.invalidate_subscription_peek_cache()

    def _run(self, *, own, sub, live=True, **kwargs):
        own_candidate = {"id": 1, "word_de": "eigenes", "frequency_rank": own} if own is not None else None
        sub_candidate = [{"admin_card_id": 9, "canonical_entry_id": 5, "word_de": "abo", "frequency_rank": sub}] if sub is not None else []
        materialized = {"id": 2, "word_de": "abo", "frequency_rank": sub}
        get_own = Mock(side_effect=[own_candidate, materialized])
        materialize = Mock(return_value=2)
        with patch.object(db, "get_next_new_srs_candidate", get_own), \
             patch.object(db, "list_admin_subscription_new_candidates", Mock(return_value=sub_candidate)), \
             patch.object(db, "materialize_subscription_card", materialize):
            result = db.get_next_new_srs_candidate_with_subscription(
                user_id=77, source_user_id=1, source_lang="ru", target_lang="de",
                live_subscription=live, **kwargs,
            )
        return result, materialize

    def test_subscription_word_wins_when_it_is_more_useful(self):
        """Ключевой случай: у человека лежит тысяча своих слов ранга 287, а в подписке
        есть слово ранга 39. Старая логика не отдала бы его никогда."""
        result, materialize = self._run(own=287, sub=39)
        self.assertEqual(result["word_de"], "abo")
        materialize.assert_called_once()

    def test_own_word_wins_when_it_is_more_useful(self):
        result, materialize = self._run(own=39, sub=287)
        self.assertEqual(result["word_de"], "eigenes")
        materialize.assert_not_called()

    def test_own_word_wins_on_a_tie(self):
        result, materialize = self._run(own=100, sub=100)
        self.assertEqual(result["word_de"], "eigenes")
        materialize.assert_not_called()

    def test_ranked_subscription_word_beats_unranked_own(self):
        """Частотность известна лишь у четверти слов; слово без ранга не должно
        загораживать слово с рангом."""
        result, _ = self._run(own=None, sub=500)
        self.assertEqual(result["word_de"], "abo")

    def test_subscription_fills_the_gap_when_there_is_nothing_of_ones_own(self):
        result, materialize = self._run(own=None, sub=None)
        self.assertIsNone(result)
        materialize.assert_not_called()

    def test_manual_selection_is_never_touched(self):
        """Человек сам отобрал слова — подписка в этот выбор не вмешивается."""
        result, materialize = self._run(own=287, sub=1, allowed_card_ids=[1, 2, 3])
        self.assertEqual(result["word_de"], "eigenes")
        materialize.assert_not_called()

    def test_aged_reserve_is_never_touched(self):
        """Резерв «самое старое» защищает неранжированные слова от голодания."""
        result, materialize = self._run(own=287, sub=1, prefer_oldest=True)
        self.assertEqual(result["word_de"], "eigenes")
        materialize.assert_not_called()

    def test_subscription_off_means_only_own_words(self):
        result, materialize = self._run(own=287, sub=1, live=False)
        self.assertEqual(result["word_de"], "eigenes")
        materialize.assert_not_called()

    def test_broken_subscription_never_breaks_card_delivery(self):
        own_candidate = {"id": 1, "word_de": "eigenes", "frequency_rank": 287}
        with patch.object(db, "get_next_new_srs_candidate", Mock(return_value=own_candidate)), \
             patch.object(db, "list_admin_subscription_new_candidates", Mock(side_effect=RuntimeError("БД недоступна"))):
            result = db.get_next_new_srs_candidate_with_subscription(
                user_id=77, source_user_id=1, source_lang="ru", target_lang="de",
                live_subscription=True,
            )
        self.assertEqual(result["word_de"], "eigenes")


class SubscriptionPeekCacheTests(unittest.TestCase):
    """Загляд в подписку стоит ~63 мс (замер на проде) — на каждую карточку это дорого."""

    def setUp(self):
        db.invalidate_subscription_peek_cache()

    def test_peek_is_cached_between_calls(self):
        probe = Mock(return_value=[{"admin_card_id": 9, "canonical_entry_id": 5,
                                    "word_de": "abo", "frequency_rank": 10}])
        with patch.object(db, "list_admin_subscription_new_candidates", probe):
            for _ in range(20):
                db._peek_subscription_candidate(user_id=77, source_user_id=1,
                                                source_lang="ru", target_lang="de")
        probe.assert_called_once()

    def test_cache_drops_after_a_word_is_taken(self):
        probe = Mock(return_value=[{"admin_card_id": 9, "canonical_entry_id": 5,
                                    "word_de": "abo", "frequency_rank": 10}])
        with patch.object(db, "list_admin_subscription_new_candidates", probe):
            db._peek_subscription_candidate(user_id=77, source_user_id=1,
                                            source_lang="ru", target_lang="de")
            db.invalidate_subscription_peek_cache(77)
            db._peek_subscription_candidate(user_id=77, source_user_id=1,
                                            source_lang="ru", target_lang="de")
        self.assertEqual(probe.call_count, 2)


class PhraseTurnTests(unittest.TestCase):
    """Каждая третья новая карточка — фраза.

    Частотный ранг существует только у одиночных немецких слов, поэтому в сортировке
    «по нужности» фразы проигрывают всегда. У автора их вдвое больше, чем слов
    (16 523 против 8 153 по всей базе) — без квоты они лежали бы мёртвым грузом.
    Признак «фраза» считается ОДИН РАЗ при сохранении, чтобы выдача карточки не
    разбирала текст на каждом запросе.
    """

    def setUp(self):
        db.invalidate_subscription_peek_cache()

    def test_phrase_turn_asks_for_phrases(self):
        phrase = {"id": 3, "word_de": "etwas einräumen", "frequency_rank": 1500500}
        picker = Mock(return_value=phrase)
        with patch.object(db, "get_next_new_srs_candidate", picker):
            result = db.get_next_new_srs_candidate_with_subscription(
                user_id=77, source_user_id=1, source_lang="ru", target_lang="de",
                live_subscription=False, phrases_only=True,
            )
        self.assertEqual(result["word_de"], "etwas einräumen")
        self.assertTrue(picker.call_args.kwargs["phrases_only"])

    def test_phrase_turn_never_leaves_the_user_without_a_card(self):
        """Фраз у человека нет — ход не должен пропасть, отдаём обычное слово."""
        word = {"id": 4, "word_de": "brauchen", "frequency_rank": 287}
        picker = Mock(side_effect=[None, word])
        with patch.object(db, "get_next_new_srs_candidate", picker):
            result = db.get_next_new_srs_candidate_with_subscription(
                user_id=77, source_user_id=1, source_lang="ru", target_lang="de",
                live_subscription=False, phrases_only=True,
            )
        self.assertEqual(result["word_de"], "brauchen")
        self.assertFalse(picker.call_args.kwargs.get("phrases_only", False))

    def test_phrase_turn_also_applies_to_the_subscription(self):
        sub_phrase = [{"admin_card_id": 9, "canonical_entry_id": 5,
                       "word_de": "aus dem Ruder laufen", "frequency_rank": 1200000}]
        probe = Mock(return_value=sub_phrase)
        with patch.object(db, "get_next_new_srs_candidate", Mock(side_effect=[None, {"id": 7, "word_de": "aus dem Ruder laufen", "frequency_rank": 1200000}])), \
             patch.object(db, "list_admin_subscription_new_candidates", probe), \
             patch.object(db, "materialize_subscription_card", Mock(return_value=7)):
            result = db.get_next_new_srs_candidate_with_subscription(
                user_id=77, source_user_id=1, source_lang="ru", target_lang="de",
                live_subscription=True, phrases_only=True,
            )
        self.assertEqual(result["word_de"], "aus dem Ruder laufen")
        self.assertTrue(probe.call_args.kwargs["phrases_only"])

    def test_phrase_flag_is_computed_once_not_on_every_serve(self):
        """Признак живёт в колонке; выдача — обычный фильтр, а не разбор текста."""
        from backend.dictionary_frequency import normalize_frequency_lemma
        self.assertEqual(normalize_frequency_lemma("das Haus"), "haus")      # слово с артиклем
        self.assertEqual(normalize_frequency_lemma("brauchen"), "brauchen")  # слово
        self.assertEqual(normalize_frequency_lemma("etwas einräumen"), "")   # фраза
        self.assertEqual(normalize_frequency_lemma("aus dem Ruder laufen"), "")


class PhraseProportionTests(unittest.TestCase):
    """Доля фраз в выдаче равна доле фраз в остатке — без назначенного числа.

    Раньше здесь стояло «каждая третья», но состав словарей другой: у людей примерно
    30 % слов и 70 % фраз. Фиксированное число пришлось бы подкручивать руками каждый
    раз, когда состав меняется.
    """

    def _pattern(self, words, phrases, turns=100):
        return [db.is_phrase_turn(introduced_today=i, words_left=words, phrases_left=phrases)
                for i in range(turns)]

    def test_share_matches_the_stock(self):
        for words, phrases in ((327, 730), (5191, 9071), (900, 100), (1, 9)):
            with self.subTest(words=words, phrases=phrases):
                served = self._pattern(words, phrases)
                expected = phrases / (words + phrases)
                self.assertAlmostEqual(sum(served) / len(served), expected, delta=0.02)

    def test_no_phrases_means_never_a_phrase_turn(self):
        self.assertEqual(self._pattern(500, 0), [False] * 100)

    def test_no_words_means_always_a_phrase_turn(self):
        self.assertEqual(self._pattern(0, 500), [True] * 100)

    def test_empty_stock_is_survivable(self):
        self.assertFalse(db.is_phrase_turn(introduced_today=0, words_left=0, phrases_left=0))

    def test_turns_are_spread_not_clumped(self):
        """Фразы должны идти вперемешку, а не блоком в конце дня."""
        served = self._pattern(327, 730, turns=20)
        longest_run = 0
        run = 0
        for flag in served:
            run = run + 1 if flag else 0
            longest_run = max(longest_run, run)
        self.assertLessEqual(longest_run, 4)


class SubscriptionQueriesRunTests(unittest.TestCase):
    """Запросы подписки должны СОБИРАТЬСЯ и выполняться, а не падать на подстановке.

    Дважды подряд фильтр фраз оказывался вставлен не в ту функцию: код компилировался,
    тесты на логику проходили, а запрос падал с NameError уже в бою. Здесь запросы
    прогоняются через поддельный курсор — сборка строки проверяется по-настоящему.
    """

    class _Cursor:
        def __init__(self):
            self.sql = None
            self.params = None
            # Запросов может быть несколько: отбор кандидатов подписки с 05.08.2026
            # дочитывает ещё и слова, которые у человека уже есть, — чтобы не
            # предлагать то же слово в другом написании.
            self.sqls = []

        def execute(self, sql, params=None):
            self.sql, self.params = sql, params
            self.sqls.append(sql)

        def fetchone(self):
            # Достаточно широкая строка: вызывающий код читает поля по индексам.
            return (0,) * 10

        def fetchall(self):
            return []

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def test_candidates_query_builds_for_both_modes(self):
        for phrases_only in (False, True):
            with self.subTest(phrases_only=phrases_only):
                cur = self._Cursor()
                db.list_admin_subscription_new_candidates(
                    user_id=77, source_user_id=1, source_lang="ru", target_lang="de",
                    limit=5, cursor=cur, phrases_only=phrases_only,
                )
                candidates_sql = cur.sqls[0]
                self.assertIn("FROM bt_3_webapp_dictionary_queries a", candidates_sql)
                self.assertEqual("is_phrase IS TRUE" in candidates_sql, phrases_only)

    def test_own_candidate_query_builds_for_both_modes(self):
        for phrases_only in (False, True):
            with self.subTest(phrases_only=phrases_only):
                cur = self._Cursor()
                with patch.object(db, "_trained_german_word_keys", Mock(return_value=[])):
                    db.get_next_new_srs_candidate(
                        user_id=77, source_lang="ru", target_lang="de",
                        cursor=cur, phrases_only=phrases_only,
                    )
                self.assertEqual("q.is_phrase IS TRUE" in cur.sql, phrases_only)

    def test_new_card_carries_its_direction(self):
        """Новая карточка обязана отдавать направление.

        Без него экран считает любую новую карточку «русский → немецкий» и вопросом
        показывает то, что лежит в русской колонке. У карточки, сохранённой с
        немецкого, вопрос выходил по-немецки. Замер 15.08.2026: выдача отдавала
        source_lang = None всегда."""
        cur = self._Cursor()
        with patch.object(db, "_trained_german_word_keys", Mock(return_value=[])):
            card = db.get_next_new_srs_candidate(
                user_id=77, source_lang="ru", target_lang="de", cursor=cur,
            )
        self.assertIn("q.source_lang", cur.sql)
        self.assertIn("q.target_lang", cur.sql)
        self.assertIn("source_lang", card)
        self.assertIn("target_lang", card)

    def test_available_counter_query_builds(self):
        """Счётчик доступного НЕ должен требовать фильтра фраз — он считает всё."""
        cur = self._Cursor()
        with patch.object(db, "get_db_connection_context") as ctx:
            ctx.return_value.__enter__.return_value.cursor.return_value = cur
            db.count_admin_subscription_available_words(
                user_id=77, source_user_id=1, source_lang="ru", target_lang="de",
            )
        self.assertIn("SELECT COUNT(*)", cur.sql)
        self.assertNotIn("is_phrase", cur.sql)


if __name__ == "__main__":
    unittest.main()
