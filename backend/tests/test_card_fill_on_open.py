# -*- coding: utf-8 -*-
"""Открытие пустой карточки собирает разбор — платному, через ту же дверь, что и ночь.

ПОВОД. До 26.08.2026 открытие ничего не собирало: разбор ждал ночи. Замер того же дня
на живой базе — 1 780 слов и оборотов без разбора, 3 260 личных карточек у 12 человек
открывались пустыми. Ночь берёт 500 за раз, но новые слова приходят каждый день, и
человек, сохранивший слово утром, вечером всё равно упирался в плашку «завтра».

⚠ ЧТО ЗДЕСЬ СТЕРЕЖЁТСЯ, КРОМЕ САМОГО СБОРА:
  · платит только платный (решение владельца 26.08.2026), и решает это СЕРВЕР;
  · права не выяснились — платное НЕ раздаётся молча;
  · чужое слово нашими деньгами не собирается;
  · разбор, который уже есть, отдаётся даром и без единого обращения к модели;
  · запись идёт `save_unit_card` со вторым голосом — быстрый путь не значит льготный.
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SECOND_VOICE_CHECK_DISABLED", "1")

import backend.backend_server as server  # noqa: E402
from backend import lex_units  # noqa: E402

РАЗБОР = {"word_de": "Nährstoff", "translation_ru": "питательное вещество",
          "part_of_speech": "существительное",
          "usage_examples": [{"source": "Der Nährstoff fehlt.", "target": "Не хватает вещества."}]}
# Что отдаёт база на один запрос эндпоинта:
# (номер слова, есть ли разбор, сам разбор, ВИД ЗАПИСИ).
# Вид записи добавлен 27.08.2026: предложению разбор не собирается, и решает это сервер.
ЕСТЬ_РАЗБОР = (4242, True, dict(РАЗБОР), "word")
ПУСТОЕ = (4242, False, None, "word")
ПУСТОЕ_ПРЕДЛОЖЕНИЕ = (4242, False, None, "sentence")
ПРЕДЛОЖЕНИЕ_С_РАЗБОРОМ = (4242, True, dict(РАЗБОР), "sentence")
ЧУЖОЕ = None            # карточки с таким словом у этого человека нет


class ПоддельныйКурсор:
    def __init__(self, строка):
        self._строка = строка

    def execute(self, *a, **k):
        pass

    def fetchone(self):
        return self._строка

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class ПоддельноеСоединение:
    def __init__(self, строка):
        self._курсор = ПоддельныйКурсор(строка)

    def cursor(self, *a, **k):
        return self._курсор

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class ДозаполнениеПриОткрытии(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()
        # Общий страж входа проверяет подпись Telegram ДО эндпоинта: без этих двух
        # заглушек любой запрос теста получает 401 и до проверяемой логики не доходит.
        подмены = [
            mock.patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False),
            mock.patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")),
            mock.patch.object(server, "_telegram_hash_is_valid", return_value=True),
            mock.patch.object(server, "_parse_telegram_init_data",
                              return_value={"user": {"id": 117649764}}),
        ]
        for п in подмены:
            п.start()
            self.addCleanup(п.stop)

    def _спросить(self, *, строка=ПУСТОЕ, режим="pro", слово="Nährstoff",
                  права_падают=False):
        права = mock.Mock(side_effect=RuntimeError("не выяснили")) if права_падают \
            else mock.Mock(return_value={"effective_mode": режим})
        with mock.patch.object(server, "_resolve_webapp_user_id", return_value=117649764), \
             mock.patch.object(server, "_get_user_language_pair", return_value=("de", "ru", {})), \
             mock.patch.object(server, "get_db_connection_context",
                               return_value=ПоддельноеСоединение(строка)), \
             mock.patch.object(server, "resolve_entitlement", права), \
             mock.patch.object(server, "stream_dictionary_breakdown_sections") as поток:
            ответ = self.client.post("/api/webapp/dictionary/card/fill",
                                     json={"initData": "signed", "word": слово})
        return ответ, поток

    def test_no_word_is_a_plain_refusal(self):
        ответ = self.client.post("/api/webapp/dictionary/card/fill", json={"initData": "s"})
        self.assertEqual(ответ.status_code, 400)

    def test_ready_card_costs_nothing(self):
        """Собрал другой человек минуту назад — отдаём даром."""
        ответ, поток = self._спросить(строка=ЕСТЬ_РАЗБОР)
        поток.assert_not_called()
        тело = ответ.get_json()
        self.assertTrue(тело["ok"])
        self.assertEqual(тело["source"], "already")
        self.assertEqual(тело["item"]["word_de"], "Nährstoff")

    def test_free_user_keeps_the_night_plate(self):
        ответ, поток = self._спросить(режим="free")
        поток.assert_not_called()
        self.assertEqual(ответ.get_json()["reason"], "paid_required")

    def test_unknown_entitlement_never_gives_paid_away(self):
        """Права не выяснились — это не повод раздать платное молча."""
        ответ, поток = self._спросить(права_падают=True)
        поток.assert_not_called()
        self.assertEqual(ответ.get_json()["reason"], "paid_required")

    def test_someone_elses_word_is_not_built_with_our_money(self):
        """Карточки с этим словом у человека нет — значит собирать не за чей счёт."""
        ответ, поток = self._спросить(строка=ЧУЖОЕ)
        поток.assert_not_called()
        self.assertEqual(ответ.get_json()["reason"], "no_unit")

    def test_ownership_is_checked_before_money_is_spent(self):
        """Порядок важен: сначала «его ли это слово», потом деньги."""
        ответ, поток = self._спросить(строка=ЧУЖОЕ, режим="pro")
        поток.assert_not_called()
        self.assertEqual(ответ.get_json()["reason"], "no_unit")

    def test_the_unit_comes_from_the_persons_own_card(self):
        """⚠ НЕ через lex_units.lookup: он молчит, когда у слова нет связей-переводов,
        а у неразобранного слова их обычно и нет. Прогон 26.08.2026 на «Einen Eid
        ablegen» вернул «no_unit» при живом слове 18792 — дозаполнение не делало НИЧЕГО.
        """
        with mock.patch.object(lex_units, "lookup") as поиск, \
             mock.patch.object(server, "_resolve_webapp_user_id", return_value=117649764), \
             mock.patch.object(server, "_get_user_language_pair", return_value=("de", "ru", {})), \
             mock.patch.object(server, "get_db_connection_context",
                               return_value=ПоддельноеСоединение(ЕСТЬ_РАЗБОР)), \
             mock.patch.object(server, "resolve_entitlement",
                               return_value={"effective_mode": "pro"}):
            ответ = self.client.post("/api/webapp/dictionary/card/fill",
                                     json={"initData": "signed", "word": "Nährstoff"})
        поиск.assert_not_called()
        self.assertTrue(ответ.get_json()["ok"])


class ПредложениюРазборНеСобирается(unittest.TestCase):
    """⛔ Решение владельца 27.08.2026, дословно: «это уже предложение, включающее в себя
    контекст использования слов… главное — есть немецкий и русский вариант, и больше
    ничего не нужно».

    ПОВОД. Страж стоял только в браузере и смотрел на поле `unit_kind`, а экран «Мои
    слова» его не отдавал вовсе — смотреть было не на что. Замер 27.08.2026 по живой
    базе: 2 793 предложения из 6 332 носят словарный разбор. В карточке «Gesundheit ist
    mehr denn je ein wichtiges Thema.» стояли формы глагола sein (war / ist gewesen),
    антоним «Krankheit» к целому предложению и этимология слова Gesundheit.
    """

    def setUp(self):
        self.client = server.app.test_client()
        подмены = [
            mock.patch.object(server, "WEBAPP_SINGLE_INSTANCE_GUARD_ENABLED", False),
            mock.patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")),
            mock.patch.object(server, "_telegram_hash_is_valid", return_value=True),
            mock.patch.object(server, "_parse_telegram_init_data",
                              return_value={"user": {"id": 117649764}}),
        ]
        for п in подмены:
            п.start()
            self.addCleanup(п.stop)

    def _спросить(self, строка):
        with mock.patch.object(server, "_resolve_webapp_user_id", return_value=117649764), \
             mock.patch.object(server, "_get_user_language_pair", return_value=("de", "ru", {})), \
             mock.patch.object(server, "get_db_connection_context",
                               return_value=ПоддельноеСоединение(строка)), \
             mock.patch.object(server, "resolve_entitlement",
                               return_value={"effective_mode": "pro"}) as права, \
             mock.patch.object(server, "stream_dictionary_breakdown_sections") as поток:
            ответ = self.client.post("/api/webapp/dictionary/card/fill",
                                     json={"initData": "signed", "word": "Gesundheit ist wichtig."})
        return ответ, поток, права

    def test_sentence_never_reaches_the_model(self):
        ответ, поток, _ = self._спросить(ПУСТОЕ_ПРЕДЛОЖЕНИЕ)
        поток.assert_not_called()
        тело = ответ.get_json()
        self.assertFalse(тело["ok"])
        self.assertEqual(тело["reason"], "sentence")

    def test_the_refusal_is_free_even_before_entitlement(self):
        """Отказ не должен стоить нам ни запроса к модели, ни выяснения прав."""
        _, поток, права = self._спросить(ПУСТОЕ_ПРЕДЛОЖЕНИЕ)
        поток.assert_not_called()
        права.assert_not_called()

    def test_a_sentence_that_already_has_a_breakdown_still_shows_it(self):
        """Накопленное владелец решил НЕ трогать (28.08.2026): «уже как есть так пусть и
        будет». Поэтому страж стоит ПОСЛЕ выдачи готового разбора, а не до неё —
        новый не собираем, старый не прячем."""
        ответ, поток, _ = self._спросить(ПРЕДЛОЖЕНИЕ_С_РАЗБОРОМ)
        поток.assert_not_called()
        тело = ответ.get_json()
        self.assertTrue(тело["ok"])
        self.assertEqual(тело["source"], "already")

    def test_a_word_is_still_built(self):
        """Страж не должен зацепить слова и обороты — им разбор нужен."""
        _, поток, права = self._спросить(ПУСТОЕ)
        права.assert_called()          # до денег дошли, значит страж пропустил


class ЗаписьИдётЧерезДверь(unittest.TestCase):
    def test_the_fast_path_is_still_checked_by_the_second_voice(self):
        """Разбор «по открытию» ничем не надёжнее ночного — та же модель, тот же промпт."""
        self.assertIn("дозаполнение при открытии", lex_units.MODEL_INVENTED_SOURCES)


if __name__ == "__main__":
    unittest.main()
