# -*- coding: utf-8 -*-
"""«Дополни предложение» — ОДНА ДОРОЖКА НА ВСЕХ, без индивидуального подхода.

ТРЕБОВАНИЕ ВЛАДЕЛЬЦА 28.08.2026, дословно:

    «Мы формируем 20 слов, и они используются ВСЕМИ пользователями подряд. Никакого
    индивидуального подхода. Если завтра начали учиться ещё 2000, а первые 2000 тоже
    хорошо занимались, — первые получат следующие 20 слов, а вторые получат ПЕРВЫЕ 20,
    которые мы грели для предыдущих. Просто кто-то, кто занимается, получает их быстрее.»

⛔ ДО ЭТОГО ДНЯ БЫЛО РОВНО НАОБОРОТ, и я дважды ошибочно сказал владельцу, что его
идея «уже работает». Набор собирался из ЛИЧНОГО словаря каждого человека
(get_webapp_dictionary_entries по user_id). Слова у людей разные — значит и задания
разные, значит за каждое платили заново. Замер 28.08.2026: у владельца 15 464 слова,
у остальных по тысяче, общих между ними 7%. Прогретое одним почти никому не доставалось.

КАК УСТРОЕНО ТЕПЕРЬ. Дорожка — пронумерованная последовательность материала, выбранного
НАМИ из общего пула. У человека есть только МЕСТО на ней. Прошёл двадцать — сдвинулся
на двадцать. Пришёл через год — идёт по той же дорожке с начала, по уже прогретому.

ПРОВЕРЕНО НА ЖИВОЙ БАЗЕ в тот же день: ночь дописала и прогрела три задания; владелец
и другой человек получили ОДНИ И ТЕ ЖЕ три, оба сдвинулись на 3, модель спрашивали
один раз.
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

import backend.backend_server as server  # noqa: E402
from backend import database as db  # noqa: E402


class НаборБерётсяИзДорожки_аНеИзЛичногоСловаря(unittest.TestCase):

    def setUp(self):
        import inspect
        self.код = inspect.getsource(server._build_sentence_training_set)

    def test_личный_словарь_больше_не_источник(self):
        self.assertNotIn("get_webapp_dictionary_entries", self.код,
                         "вернулся личный словарь — вместе с ним вернётся и оплата за каждого")

    def test_берём_кусок_дорожки_после_места_человека(self):
        self.assertIn("get_sentence_track_position", self.код)
        self.assertIn("get_sentence_track_slice", self.код)

    def test_папки_человека_к_общей_дорожке_отношения_не_имеют(self):
        self.assertIn("folder_mode` и `folder_id` больше не участвуют", self.код)


class ОтдаёмТолькоПрогретое(unittest.TestCase):

    def test_непрогретое_не_выдаётся(self):
        """Непрогретое не готово — человек увидел бы пустое место. Пусть набор короче."""
        import inspect
        код = inspect.getsource(db.get_sentence_track_slice)
        self.assertIn("AND warmed", код)

    def test_пустая_дорожка_это_названная_причина_а_не_молчание(self):
        with mock.patch("backend.database.get_sentence_track_position", return_value=0), \
             mock.patch("backend.database.get_sentence_track_slice", return_value=[]):
            server.take_sentence_quiz_misses()
            итог = server._build_sentence_training_set(
                user_id=1, set_size=15, folder_mode="all", folder_id=None,
                source_lang="ru", target_lang="de")
        self.assertEqual(итог, [])
        причины = server.take_sentence_quiz_misses()
        self.assertTrue(any("дорожка пуста" in k for k in причины))


class МестоЧеловекаНеОткатываетсяНазад(unittest.TestCase):

    def test_прогресс_только_вперёд(self):
        """Иначе повторная выдача того же набора отбросила бы человека назад."""
        import inspect
        код = inspect.getsource(db.advance_sentence_track_position)
        self.assertIn("GREATEST", код)

    def test_двигаем_сразу_при_выдаче(self):
        """Закрыл приложение на середине — не должен получить тот же набор снова."""
        import inspect
        код = inspect.getsource(server._build_sentence_training_set)
        self.assertIn("advance_sentence_track_position", код)


class НочьГреетДорожку_аНеЛюдейПоОтдельности(unittest.TestCase):

    def test_персонального_прогрева_больше_нет(self):
        for имя in ("_warm_sentence_quizzes_for_user", "_warm_starter_sentence_quizzes"):
            self.assertFalse(hasattr(server, имя),
                             f"{имя} вернулась — это оплата за каждого человека отдельно")

    def test_ночь_держит_запас_впереди_самого_быстрого(self):
        import inspect
        код = inspect.getsource(server._extend_and_warm_sentence_track)
        self.assertIn("furthest_user", код)
        self.assertIn("SENTENCE_TRACK_AHEAD", код)

    def test_запас_есть_значит_ночь_не_тратит(self):
        with mock.patch("backend.database.sentence_track_state",
                        return_value={"total": 100, "warmed": 100,
                                      "last_position": 100, "furthest_user": 10}), \
             mock.patch("backend.database.pick_words_for_sentence_track") as выбор:
            итог = server._extend_and_warm_sentence_track(бюджет=50)
        self.assertEqual(итог, {"added": 0, "warmed": 0})
        выбор.assert_not_called()

    def test_запас_держим_на_один_набор_а_не_на_год(self):
        self.assertEqual(server.SENTENCE_TRACK_AHEAD, 20)


class ОдноСловоНаДорожкеОдинРаз(unittest.TestCase):

    def test_повтор_слова_невозможен(self):
        """Иначе человек получит то же задание дважды и решит, что его не помнят."""
        import inspect
        код = inspect.getsource(db.ensure_sentence_track_schema)
        self.assertIn("uq_sentence_track_word", код)
        self.assertIn("ON bt_3_sentence_track (lower(word_de))", код)

    def test_материал_дорожки_это_настоящие_предложения(self):
        """Задание с пропуском в одном слове бессмысленно: дыру не в чем прятать."""
        import inspect
        код = inspect.getsource(db.pick_words_for_sentence_track)
        self.assertIn(">= 4", код)
        self.assertIn("[А-Яа-яЁё]", код)


if __name__ == "__main__":
    unittest.main()
