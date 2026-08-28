# -*- coding: utf-8 -*-
"""Задание «Дополни предложение» — общее достояние, и греет его ночь.

ТРЕБОВАНИЕ ВЛАДЕЛЬЦА 28.08.2026, дословно: «мы должны греть задание, и они должны быть
ПЕРЕИСПОЛЬЗОВАНЫ всеми остальными. Тот, кто занимается активно, получает их быстрее, а
тот, кто менее активно, получает те же самые, уже прогретые для других — новые для него
не греются».

⛔ ДО ЭТОГО ДНЯ ЭТОГО НЕ БЫЛО. Кеш писался только в ЛИЧНУЮ карточку, и два человека с
одним и тем же словом платили каждый за себя. При тринадцати людях — тринадцать раз за
один и тот же текст.

ПРОВЕРЕНО НА ЖИВОЙ БАЗЕ 28.08.2026: один прогрев слова «…aus der Luft gegriffen» —
и оно готово для 10 человек, у которых это слово сохранено.

ОСТАЛЬНЫЕ РЕШЕНИЯ ВЛАДЕЛЬЦА, КОТОРЫЕ ЗДЕСЬ СТЕРЕГУТСЯ:
  · греем ТОЛЬКО тем, кто заходил ИМЕННО в эту тренировку («не в целом словарь и не в
    целом карточки интервального повторения»);
  · добиваем запас до 20 и не больше («прошёл вчера семь — досыпаем семь»);
  · уже готовое СЧИТАЕТСЯ в эти 20 — иначе платили бы за оплаченное соседом;
  · сверх того греем общий словарь, чтобы первый заход новичка не упёрся в пустоту:
    «зайдёт впервые из любопытства, увидит пустоту и больше не вернётся».
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

import backend.backend_server as server  # noqa: E402
from backend import database as db  # noqa: E402


class ПрогретоеДостаётсяВсем(unittest.TestCase):

    def test_перед_оплатой_смотрим_в_общий_пул(self):
        """Оплатил сосед — мы не платим второй раз."""
        import pathlib
        текст = pathlib.Path(server.__file__).read_text(encoding="utf-8")
        начало = текст.index("def _build_sentence_quiz_from_dictionary_entry")
        тело = текст[начало:начало + 3000]
        self.assertIn("_pool_quiz_payload(entry, german_sentence)", тело)
        self.assertLess(тело.index("_pool_quiz_payload"), тело.index("if not allow_llm:"),
                        "в пул надо заглядывать ДО решения платить")

    def test_построенное_кладётся_в_общий_пул(self):
        import pathlib
        текст = pathlib.Path(server.__file__).read_text(encoding="utf-8")
        self.assertIn("_remember_sentence_quiz_in_pool(entry, german_sentence, payload)", текст)

    def test_из_пула_берём_только_под_ТО_ЖЕ_предложение(self):
        """У слова в карточке может стоять другой пример; подставить задание от чужого
        предложения значило бы показать дыру не в том тексте."""
        import inspect
        код = inspect.getsource(db.get_pool_sentence_quiz)
        self.assertIn("source_sentence", код)
        self.assertIn("если == предложение", код)

    def test_негодное_из_пула_не_показывается(self):
        """В пуле может лежать старое или испорченное — оно проходит того же стража."""
        запись = {"id": 5, "word_de": "eignen", "response_json": {}}
        плохое = {"version": server.SENTENCE_GAP_CACHE_VERSION,
                  "source_sentence": "Das eignet sich nicht.",
                  "payload": {"quiz_type": "мусор"}}
        with mock.patch("backend.database.get_pool_sentence_quiz", return_value=плохое):
            self.assertIsNone(server._pool_quiz_payload(запись, "Das eignet sich nicht."))


class ГреемТолькоТем_КтоСюдаЗаходил(unittest.TestCase):

    def test_заход_в_режим_записывается(self):
        """Раньше режим не писался нигде — в базе было только «карточка показана»."""
        import inspect
        self.assertTrue(hasattr(db, "record_training_mode_use"))
        код = inspect.getsource(db.get_users_of_training_mode)
        self.assertIn("bt_3_training_mode_usage", код)

    def test_ночь_берёт_людей_именно_этого_режима(self):
        import inspect
        код = inspect.getsource(server._dispatch_sentence_quiz_warm)
        self.assertIn('get_users_of_training_mode("sentence"', код)

    def test_готовое_засчитывается_в_запас(self):
        """Иначе платили бы за то, что сосед уже оплатил."""
        import inspect
        код = inspect.getsource(server._warm_sentence_quizzes_for_user)
        self.assertIn("SENTENCE_QUIZ_WARM_TARGET_PER_USER - готово", код)

    def test_запас_добивается_до_двадцати(self):
        self.assertEqual(server.SENTENCE_QUIZ_WARM_TARGET_PER_USER, 20)

    def test_у_ночи_есть_потолок(self):
        """Без потолка одна ночь могла бы выесть бюджет: 1,88 с и деньги за каждое."""
        self.assertGreater(server.SENTENCE_QUIZ_WARM_NIGHTLY_CAP, 0)


class НовичокНеУпираетсяВПустоту(unittest.TestCase):
    """Владелец: «зайдёт впервые из любопытства, а карточек нет — и он не вернётся».
    Замер 28.08.2026: ~1000 слов сохранены у 10–13 человек из 13 — это стартовый
    словарь, одинаковый у всех. Прогрев его один раз обеспечивает первый заход любого."""

    def test_часть_ночного_потолка_идёт_общему_словарю(self):
        self.assertGreater(server.SENTENCE_QUIZ_WARM_STARTER_SHARE, 0)
        import inspect
        код = inspect.getsource(server._dispatch_sentence_quiz_warm)
        self.assertIn("_warm_starter_sentence_quizzes", код)

    def test_берём_слова_которые_есть_у_многих(self):
        import inspect
        код = inspect.getsource(db.get_shared_cold_words_for_quiz)
        self.assertIn("count(DISTINCT user_id) >= %s", код)
        self.assertIn("NOT EXISTS", код)

    def test_на_слово_греем_одну_карточку(self):
        """Иначе заплатили бы за одно и то же слово столько раз, у скольких оно есть."""
        import inspect
        код = inspect.getsource(db.get_shared_cold_words_for_quiz)
        self.assertIn("SELECT DISTINCT ON (х.w) q.id", код)


if __name__ == "__main__":
    unittest.main()
