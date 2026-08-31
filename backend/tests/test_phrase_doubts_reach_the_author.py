# -*- coding: utf-8 -*-
"""Сомнение по фразе уходит АВТОРУ и в ту же недельную пачку, что и слова.

ПОВОД. Владелец 26.08.2026: «фраза сохранилась мгновенно, ночью проверилась, если
сомнение — оно уходит автору именно в ту же недельную пачку, что и слова, чтобы ничего
нового не строить». До этого отложенные фразы видел только администратор.

⚠ ЧТО ИМЕННО СТЕРЕЖЁТ ЭТОТ ТЕСТ. Первая версия запроса брала любого, у кого есть
карточка на это слово. Слово в общем слое одно на всех — и проверка на живой базе
26.08.2026 показала, как два разных человека получили ОДНИ И ТЕ ЖЕ три фразы, которых
сами не сохраняли («Damit man in dieses Formular…», «Sie konnte dem Vorschlag einiges
abgewinnen», «Wie gewonnen, so zerronnen»). Человек не может решить судьбу чужого
текста: он не знает, откуда тот взят и что имелось в виду.

Проверка на живой базе после правки: у трёх авторов 195 фраз, чужих 0; у трёх
подписчиков без авторства — 0 фраз. Время запроса 0.2–1.4 c против >10 минут
у версии с подзапросом на каждую строку.
"""
import os
import re
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SECOND_VOICE_CHECK_DISABLED", "1")

from backend import word_confirm_digest as сводка  # noqa: E402


class ПоддельныйКурсор:
    """Отдаёт заготовленные ответы по очереди и запоминает, о чём его спросили."""

    def __init__(self, ответы):
        self._ответы = list(ответы)
        self.запросы = []

    def execute(self, sql, params=None):
        self.запросы.append((sql, params))

    def fetchall(self):
        return self._ответы.pop(0) if self._ответы else []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class ПоддельноеСоединение:
    def __init__(self, курсор):
        self._курсор = курсор

    def cursor(self, *a, **k):
        return self._курсор

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


def _спросить(ответы, user_id=777, limit=12):
    курсор = ПоддельныйКурсор(ответы)
    with mock.patch("backend.database.get_db_connection_context",
                    return_value=ПоддельноеСоединение(курсор)):
        строки = сводка.words_for_user(user_id, limit=limit)
    return строки, курсор


ПРОВЕРКА_ПРОШЛА = {"checked": True, "grammar_ok": True, "meaning_kept": True}
# Строка проверки фразы как её отдаёт база:
# (id, текст, перевод, судьи, арбитр, слово, ВИД, карточка).
# Вид и карточка добавлены 28.08.2026: панельный вопрос спрашивает о наполнении
# карточки, и предмет спора — сами примеры — нужен на экране.
ФРАЗА = [(77, "Ich überhaupt kein Talent", "у меня совсем нет таланта",
          [{"verdict": "error", "category": "wortstellung",
            "corrected": "Ich habe überhaupt kein Talent",
            "corrected_ru": "у меня совсем нет таланта",
            "corrected_check": dict(ПРОВЕРКА_ПРОШЛА)}], None, 4242, "grammar", None)]
СЛОВО = [("Regenschirm", "зонт")]


class ФразыДоходятДоАвтора(unittest.TestCase):
    def test_phrase_lands_in_the_same_weekly_batch(self):
        строки, _ = _спросить([[], ФРАЗА])
        self.assertEqual(строки, [("Ich überhaupt kein Talent",
                                   "у меня совсем нет таланта")])

    def test_words_and_phrases_come_together(self):
        строки, _ = _спросить([СЛОВО, ФРАЗА])
        self.assertEqual([т for т, _ in строки],
                         ["Regenschirm", "Ich überhaupt kein Talent"])

    def test_phrase_query_asks_the_author_only(self):
        """Главное правило: спрашиваем автора, а не каждого подписчика общего слова."""
        _, курсор = _спросить([[], ФРАЗА])
        sql = курсор.запросы[1][0]
        плоский = re.sub(r"\s+", " ", sql)
        self.assertIn("bt_3_phrase_review", плоский)
        self.assertIn("DISTINCT ON (lex_unit_id)", плоский,
                      "автор должен считаться одним проходом, а не подзапросом на строку")
        self.assertRegex(плоский, r"ORDER BY lex_unit_id, created_at, id",
                         "автор — тот, чья карточка на это слово появилась ПЕРВОЙ")
        self.assertIn("a.user_id = %s", плоский,
                      "без этого фразы уйдут любому подписчику общего слова")

    def test_phrase_query_is_scoped_to_this_user(self):
        # Порядок подстановок: сначала виды вопроса, потом человек, потом сколько
        # взять. Вид стоит первым, потому что он в запросе первым и есть.
        _, курсор = _спросить([[], ФРАЗА], user_id=424242)
        параметры = курсор.запросы[1][1]
        self.assertEqual(параметры[0], ["grammar", "panel", "personal"])
        self.assertEqual(параметры[1], 424242)

    def test_only_open_phrases_are_asked_about(self):
        """Решённые ночью фразы человека не беспокоят."""
        _, курсор = _спросить([[], ФРАЗА])
        self.assertIn("r.status = 'open'", re.sub(r"\s+", " ", курсор.запросы[1][0]))

    def test_phrases_do_not_push_words_out_of_the_batch(self):
        """Слова идут первыми; фраз берём ровно столько, сколько осталось места."""
        слова = [(f"Wort{i}", "перевод") for i in range(5)]
        _, курсор = _спросить([слова, ФРАЗА], limit=8)
        self.assertEqual(курсор.запросы[1][1][2], 3)

    def test_full_batch_of_words_skips_the_phrase_query(self):
        слова = [(f"Wort{i}", "перевод") for i in range(12)]
        строки, курсор = _спросить([слова], limit=12)
        self.assertEqual(len(строки), 12)
        self.assertEqual(len(курсор.запросы), 1, "лишний запрос в базу на каждой рассылке")

    def test_same_text_is_not_asked_twice(self):
        та_же = [(77, "Alles Banane", "всё нормально",
                  [{"verdict": "ok", "category": ""}], None, 5, "grammar", None)]
        строки, _ = _спросить([[("Alles Banane", "всё нормально")], та_же])
        self.assertEqual(len(строки), 1)

    def test_reminder_recipients_are_authors_too(self):
        """Тот же закон в списке получателей: иначе пачка обещает чужие фразы."""
        источник = сводка._phrase_counts_by_author.__code__.co_consts
        тексты = [c for c in источник if isinstance(c, str) and "bt_3_phrase_review" in c]
        self.assertTrue(тексты, "счёт получателей больше не спрашивает про фразы")
        плоский = re.sub(r"\s+", " ", тексты[0])
        self.assertIn("DISTINCT ON (lex_unit_id)", плоский)
        self.assertIn("a.user_id", плоский)

    def test_the_letter_promises_exactly_what_the_screen_shows(self):
        """Письмо и экран считают ОДНИМ правилом — иначе обещание снова разойдётся с делом."""
        курсор = ПоддельныйКурсор([[(7, "Ich überhaupt kein Talent",
                                     [{"verdict": "error", "category": "wortstellung"}])]])
        # Пустая придирка («ошибка есть, а исправить нечего») на экран не попадает —
        # значит и в число письма входить не должна.
        self.assertEqual(сводка._phrase_counts_by_author(курсор), {})


class ПоломкаНеПритворяетсяПустотой(unittest.TestCase):
    """Пустой список УЖЕ значит «спрашивать нечего». Той же пустотой отвечать на сбой
    нельзя: снаружи два разных мира становятся неотличимы.

    ПОВОД. 26.08.2026 `except Exception: return []` живьём съел настоящую поломку —
    правка стала падать на распаковке строки, а наружу ушёл «пустой список». Нашлась
    она только по следу в логе, и то случайно.
    """

    def _с_поломкой(self):
        return mock.patch("backend.database.get_db_connection_context",
                          side_effect=RuntimeError("база молчит"))

    def test_broken_list_is_not_an_empty_batch(self):
        with self._с_поломкой(), self.assertRaises(RuntimeError):
            сводка.words_for_user(777)

    def test_broken_screen_is_not_an_empty_screen(self):
        with self._с_поломкой(), self.assertRaises(RuntimeError):
            сводка.audit_items(777)

    def test_broken_save_is_not_a_silent_success(self):
        """Человек нажал «Готово». Экран обязан сказать правду, а не нарисовать успех."""
        with self._с_поломкой(), self.assertRaises(RuntimeError):
            сводка.apply_decisions(777, [{"word": "Haus", "action": "keep"}])

    def test_a_broken_mailing_is_marked_failed_not_completed(self):
        """Сбой базы в момент рассылки выглядел как «сегодня никому не нужно писать»."""
        with mock.patch.dict("os.environ", {"TELEGRAM_Deutsch_BOT_TOKEN": "т",
                                            "TELEGRAM_BOT_USERNAME": "бот"}), \
             mock.patch("backend.database.claim_scheduler_run_guard", return_value=True), \
             mock.patch("backend.database.finish_scheduler_run_guard") as финиш, \
             self._с_поломкой():
            итог = сводка.send_word_audit_reminders()
        self.assertFalse(итог["ok"])
        self.assertEqual(финиш.call_args.kwargs["status"], "failed")


if __name__ == "__main__":
    unittest.main()
