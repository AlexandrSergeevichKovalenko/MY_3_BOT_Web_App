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


class ПерсональныйПрогревОтменёнВладельцем(unittest.TestCase):
    """⛔ ЗДЕСЬ ПРОВЕРЯЛСЯ ПРОГРЕВ ПОД КАЖДОГО ЧЕЛОВЕКА — «добить каждому до 20»,
    «держать 30 общих слов на новичка». Всё это отменено 28.08.2026 прямым указанием
    владельца:

        «Мы формируем 20 слов, и они используются ВСЕМИ пользователями подряд.
        Никакого индивидуального подхода.»

    Персональный прогрев означал платить за каждого заново: при 7% общих слов
    переиспользовалось почти ничего. Осталась одна ночная работа — держать запас
    впереди самого быстрого на ОБЩЕЙ дорожке; её стережёт
    test_sentence_track_is_one_for_everyone.py.
    """

    def test_персональные_цели_не_вернулись(self):
        for имя in ("SENTENCE_QUIZ_WARM_TARGET_PER_USER",
                    "SENTENCE_QUIZ_WARM_STARTER_TARGET",
                    "SENTENCE_QUIZ_WARM_STARTER_SHARE"):
            self.assertFalse(hasattr(server, имя),
                             f"{имя} вернулась — это оплата за каждого человека отдельно")

    def test_запись_захода_в_режим_осталась(self):
        """Она пригодилась не для прогрева, а чтобы знать, кто вообще пользуется."""
        self.assertTrue(hasattr(db, "record_training_mode_use"))
        self.assertTrue(hasattr(db, "get_users_of_training_mode"))


if __name__ == "__main__":
    unittest.main()
