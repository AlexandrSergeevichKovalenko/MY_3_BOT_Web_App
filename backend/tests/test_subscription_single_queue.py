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


if __name__ == "__main__":
    unittest.main()
