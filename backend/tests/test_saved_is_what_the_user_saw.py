"""Что человек видит на экране — то и сохраняется. Без модели и без сюрприза.

Владелец 20.08.2026, дословно: «я как пользователь вижу фразу и вижу его перевод, почему
я должен думать, что сохранение будет какое-то другое? бред».

Вычитка моделью придумана для одного случая: человек НАБРАЛ слово руками, и в нём бывает
опечатка, чужой артикль, неверный падеж. Там она к месту и остаётся.

Но там, где текст показали МЫ САМИ — карточка «Новости дня», слово из читалки, оборот из
субтитров, вариант из нашего разбора, — вычитывать нечего: мы правим собственный текст
вслепую, без контекста, у человека на глазах и за его время. Если наш текст плох, он
плох УЖЕ НА ЭКРАНЕ, и чинить это надо там, где он рождается.

Цена была не теоретической: вычитка вызывалась ВНУТРИ сохранения и ждала ответа модели
до 10 секунд, а рядом стоял комментарий «сохранение никогда не ждёт этот вызов» — он
был неправдой. Замер по ведомости 20.08.2026: 1875 таких обращений за 30 дней.

Отдельно это закрывает сленг из стендапа: оборот, который мы показали, сохранится живым,
а не «улучшенным» до литературного немецкого.
"""

import unittest

from backend.backend_server import _save_source_was_typed_by_user


class WhoWroteTheTextTests(unittest.TestCase):
    def test_text_the_person_typed_is_still_proofread(self):
        for origin in ("webapp_dictionary_save", "webapp_quick_dictionary",
                       "mobile_dictionary_save", "bot_private_save"):
            with self.subTest(origin):
                self.assertTrue(_save_source_was_typed_by_user(origin))

    def test_text_we_showed_is_saved_exactly_as_shown(self):
        for origin in ("worldnews_phrase_save", "reader", "youtube",
                       "webapp_deep_analysis_option", "trainer_save", "synonym_save",
                       "webapp_quick_dictionary_example"):
            with self.subTest(origin):
                self.assertFalse(_save_source_was_typed_by_user(origin))

    def test_the_news_card_phrase_is_not_touched(self):
        """Живой случай владельца: «einen hohen genetischen Anteil» на карточке
        «Новости дня». Показали в винительном — в винительном и сохраняем."""
        self.assertFalse(_save_source_was_typed_by_user("worldnews_phrase_save"))

    def test_unknown_origin_is_left_alone_and_not_guessed(self):
        """Незнакомый источник не «наверное набрал человек». Молча подправить то, что
        человек видел на экране, хуже, чем не подправить, — поэтому не трогаем.
        Молчать при этом нельзя: имя уходит в лог (проверяется соседним тестом)."""
        self.assertFalse(_save_source_was_typed_by_user("совсем_новая_поверхность"))
        self.assertFalse(_save_source_was_typed_by_user(""))
        self.assertFalse(_save_source_was_typed_by_user(None))

    def test_unknown_origin_is_reported_not_swallowed(self):
        with self.assertLogs(level="WARNING") as logs:
            _save_source_was_typed_by_user("совсем_новая_поверхность")
        self.assertTrue(any("совсем_новая_поверхность" in line for line in logs.output),
                        "имя незакрытого источника обязано попасть в лог")

    def test_the_two_lists_do_not_overlap(self):
        from backend.backend_server import _SHOWN_BY_US_ORIGINS, _TYPED_BY_USER_ORIGINS
        self.assertEqual(_TYPED_BY_USER_ORIGINS & _SHOWN_BY_US_ORIGINS, set(),
                         "один источник не может быть в обоих списках")


if __name__ == "__main__":
    unittest.main()
