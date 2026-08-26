# -*- coding: utf-8 -*-
"""Фразы лежат на ТОМ ЖЕ экране проверки, что и слова, и решаются теми же кнопками.

ПОВОД. 26.08.2026 письмо уже считало и слова, и фразы, а экран показывал только слова:
списки берутся из разных таблиц. Замер того же дня на живой базе: у трёх авторов 195
отложенных фраз и НОЛЬ слов на проверке — то есть письмо «слова ждут проверки» вело
на экран «проверять нечего». Обещание без содержания хуже молчания.

⚠ ЧТО ЗДЕСЬ ГЛАВНОЕ, КРОМЕ САМОГО СПИСКА:
  · номер фразы приходит из браузера, и ему нельзя верить — автор сверяется по базе;
  · «удалить» у обычного человека убирает фразу У НЕГО, а не из общего словаря,
    пока слово нужно кому-то ещё;
  · причина по-русски берётся ИЗ КАТЕГОРИИ проверяющего, а не придумывается нами.
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SECOND_VOICE_CHECK_DISABLED", "1")

from backend import word_confirm_digest as сводка  # noqa: E402

ПРОВЕРКА_ПРОШЛА = {"checked": True, "grammar_ok": True, "meaning_kept": True}
СУДЬИ = [
    {"verdict": "error", "category": "rechtschreibung",
     "corrected": "Auftreten von Krankheitssymptomen",
     "corrected_ru": "Появление симптомов заболевания",
     "corrected_check": dict(ПРОВЕРКА_ПРОШЛА)},
    {"verdict": "error", "category": "rechtschreibung",
     "corrected": "Auftreten von Krankheitssymptomen",
     "corrected_ru": "Появление симптомов заболевания",
     "corrected_check": dict(ПРОВЕРКА_ПРОШЛА)},
]
ФРАЗА_СТРОКА = (77, "Auftreten vonKrankheitssymptomen", "Появление симптомов заболевания",
                СУДЬИ, None, 4242)
СЛОВО_СТРОКА = ("Nährstoff", "питательное вещество", "не подтверждено",
                "модель: слово есть, редкое", "")


class ПоддельныйКурсор:
    def __init__(self, ответы):
        self._ответы = list(ответы)
        self.запросы = []
        self.rowcount = 1

    def execute(self, sql, params=None):
        self.запросы.append((sql, params))

    def fetchall(self):
        return self._ответы.pop(0) if self._ответы else []

    def fetchone(self):
        строки = self._ответы.pop(0) if self._ответы else []
        return строки[0] if строки else None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class ПоддельноеСоединение:
    def __init__(self, курсор):
        self._курсор = курсор
        self.commits = 0

    def cursor(self, *a, **k):
        return self._курсор

    def commit(self):
        self.commits += 1

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


def _экран(ответы, user_id=117649764):
    курсор = ПоддельныйКурсор(ответы)
    with mock.patch("backend.database.get_db_connection_context",
                    return_value=ПоддельноеСоединение(курсор)):
        return сводка.audit_items(user_id), курсор


class ФразыНаЭкране(unittest.TestCase):
    def test_phrase_appears_on_the_same_screen(self):
        карточки, _ = _экран([[СЛОВО_СТРОКА], [ФРАЗА_СТРОКА]])
        self.assertEqual([к["kind"] for к in карточки], ["word", "phrase"])

    def test_screen_is_not_empty_when_only_phrases_wait(self):
        """Тот самый случай с живой базы: слов ноль, фраз 195."""
        карточки, _ = _экран([[], [ФРАЗА_СТРОКА]])
        self.assertEqual(len(карточки), 1)
        self.assertEqual(карточки[0]["word"], "Auftreten vonKrankheitssymptomen")

    def test_phrase_carries_a_ready_variant_with_its_translation(self):
        карточки, _ = _экран([[], [ФРАЗА_СТРОКА]])
        варианты = карточки[0]["variants"]
        self.assertEqual(варианты[0]["text"], "Auftreten von Krankheitssymptomen")
        self.assertEqual(варианты[0]["ru"], "Появление симптомов заболевания")

    def test_reason_comes_from_the_judges_category(self):
        карточки, _ = _экран([[], [ФРАЗА_СТРОКА]])
        self.assertEqual(карточки[0]["why"], "Похоже, в написании ошибка.")

    def test_unknown_category_is_never_invented(self):
        """Разряд не назван — говорим это прямо, а не сочиняем грамматику."""
        причина = сводка._phrase_reason([{"verdict": "error", "category": ""}])
        self.assertIn("разошлись", причина)

    def test_every_live_category_has_human_words(self):
        """Разряды, которые вправду стоят у открытых фраз (замер 26.08.2026)."""
        for разряд in ("wortstellung", "kasus", "praeposition", "rechtschreibung",
                       "kongruenz", "stil", "sprachmischung"):
            строка = сводка._phrase_reason([{"category": разряд}])
            self.assertNotIn(разряд, строка.lower(), f"{разряд} утёк на экран как есть")
            self.assertTrue(строка.endswith("."))

    def test_empty_nitpick_never_reaches_the_person(self):
        """Заявили ошибку, а исправить нечего — вопрос без содержания.

        Правило не новое: такие записи уже отсеиваются на экране владельца
        (`database.phrase_review_is_noise`, разобрано 08.08.2026). Человеку тем
        более нечего показать — спрашивать «что не так?» и не мочь ответить нельзя.
        """
        придирка = (78, "Ich überhaupt kein Talent", "у меня совсем нет таланта",
                    [{"verdict": "error", "category": "wortstellung"}], None, 5)
        карточки, _ = _экран([[], [придирка]])
        self.assertEqual(карточки, [])

    def test_phrase_without_a_variant_but_without_a_claim_still_shows_up(self):
        """Проверяющие не назвали ошибку, но и не сошлись — решает человек."""
        спорная = (79, "Alles Banane", "всё нормально",
                   [{"verdict": "ok", "category": ""}], None, 6)
        карточки, _ = _экран([[], [спорная]])
        self.assertEqual(карточки[0]["variants"], [])
        self.assertIn("разошлись", карточки[0]["why"])

    def test_screen_asks_the_author_only(self):
        _, курсор = _экран([[], [ФРАЗА_СТРОКА]])
        текст = " ".join(str(q[0]) for q in курсор.запросы)
        self.assertIn("DISTINCT ON (lex_unit_id)", текст)
        self.assertIn("a.user_id = %s", текст)


class ПисьмоНазываетТоЧтоВнутри(unittest.TestCase):
    """Заголовок письма обязан совпасть с тем, что человек увидит на экране."""

    def test_only_phrases(self):
        текст = сводка._reminder_text(0, 5)
        self.assertIn("5 фраз", текст)
        self.assertNotIn("5 слов", текст)

    def test_only_words(self):
        текст = сводка._reminder_text(3, 0)
        self.assertIn("3 слова", текст)
        self.assertNotIn("фраз", текст)

    def test_both(self):
        текст = сводка._reminder_text(2, 3)
        self.assertIn("5 записей", текст)

    def test_one_phrase_is_singular(self):
        self.assertIn("1 фраза в твоём словаре ждёт проверки", сводка._reminder_text(0, 1))


class РешениеПоФразе(unittest.TestCase):
    def _применить(self, item, автор=(4242, "Auftreten vonKrankheitssymptomen", 117649764)):
        with mock.patch.object(сводка, "_phrase_owner", return_value=автор), \
             mock.patch("backend.database.apply_phrase_review_decision",
                        return_value={"text": "Auftreten von Krankheitssymptomen"}) as решение:
            счёт = сводка.apply_decisions(117649764, [item])
        return счёт, решение

    def test_accept_goes_through_the_owners_own_machinery(self):
        счёт, решение = self._применить(
            {"word": "Auftreten vonKrankheitssymptomen", "kind": "phrase",
             "review_id": 77, "action": "fixed", "variant": 1})
        решение.assert_called_once_with(77, "accept", "", 1, "")
        self.assertEqual(счёт["исправлено"], 1)

    def test_keep_closes_the_question(self):
        счёт, решение = self._применить(
            {"word": "Auftreten vonKrankheitssymptomen", "kind": "phrase",
             "review_id": 77, "action": "keep"})
        решение.assert_called_once_with(77, "keep")
        self.assertEqual(счёт["оставлено"], 1)

    def test_own_text_is_cleaned_before_it_is_written(self):
        """Своё поле ввода — такой же вход, как модель, и поблажки ему нет."""
        _, решение = self._применить(
            {"word": "Auftreten vonKrankheitssymptomen", "kind": "phrase",
             "review_id": 77, "action": "manual",
             "text": "  Auftreten von  Krankheiten ", "translation": " возникновение "})
        аргументы = решение.call_args[0]
        self.assertEqual(аргументы[1], "replace")
        self.assertEqual(аргументы[2], "Auftreten von Krankheiten")
        self.assertEqual(аргументы[4], "возникновение")

    def test_someone_elses_phrase_is_refused(self):
        """Номер пришёл из браузера. Чужой номер не должен сделать ничего."""
        счёт, решение = self._применить(
            {"word": "чужая", "kind": "phrase", "review_id": 999, "action": "keep"},
            автор=(4242, "чужая", 514237932))
        решение.assert_not_called()
        self.assertEqual(sum(счёт.values()), 0)

    def test_phrase_never_runs_the_word_queries(self):
        """Фраза не должна попасть в ветку слов: там её текст стал бы «голым словом»."""
        курсор = ПоддельныйКурсор([])
        with mock.patch("backend.database.get_db_connection_context",
                        return_value=ПоддельноеСоединение(курсор)), \
             mock.patch.object(сводка, "_apply_phrase_decision", return_value="оставлено"):
            сводка.apply_decisions(117649764, [
                {"word": "Ich überhaupt kein Talent", "kind": "phrase",
                 "review_id": 78, "action": "drop"}])
        self.assertEqual(курсор.запросы, [], "фраза ушла в запросы про слова")

    def test_one_broken_phrase_does_not_lose_the_rest(self):
        """Человек нажал кнопки на всём экране — терять их из-за одной строки нельзя."""
        курсор = ПоддельныйКурсор([])
        решения = [
            {"word": "первая фраза", "kind": "phrase", "review_id": 1, "action": "keep"},
            {"word": "вторая фраза", "kind": "phrase", "review_id": 2, "action": "keep"},
        ]
        with mock.patch("backend.database.get_db_connection_context",
                        return_value=ПоддельноеСоединение(курсор)), \
             mock.patch.object(сводка, "_apply_phrase_decision",
                               side_effect=[RuntimeError("база молчит"), "оставлено"]):
            счёт = сводка.apply_decisions(117649764, решения)
        self.assertEqual(счёт["оставлено"], 1)


class УдалениеФразы(unittest.TestCase):
    """«Удалить» обычного человека — решение о СЕБЕ, а не обо всех подписчиках."""

    def _удалить(self, чужих: int):
        курсор = ПоддельныйКурсор([[(чужих,)]])
        соединение = ПоддельноеСоединение(курсор)
        with mock.patch.object(сводка, "_phrase_owner",
                               return_value=(4242, "Ich überhaupt kein Talent", 7)), \
             mock.patch("backend.database.get_db_connection_context",
                        return_value=соединение), \
             mock.patch("backend.database.apply_phrase_review_decision") as решение:
            счёт = сводка._apply_phrase_decision(7, {
                "word": "Ich überhaupt kein Talent", "kind": "phrase",
                "review_id": 78, "action": "drop"})
        return счёт, курсор, решение

    def test_shared_word_survives_while_someone_else_needs_it(self):
        счёт, курсор, решение = self._удалить(чужих=3)
        решение.assert_not_called()
        self.assertEqual(счёт, "удалено")
        запросы = " ".join(str(q[0]) for q in курсор.запросы)
        self.assertIn("DELETE FROM bt_3_webapp_dictionary_queries", запросы)
        self.assertNotIn("bt_3_lex_units", запросы)

    def test_the_person_is_not_asked_about_it_again(self):
        _, курсор, _ = self._удалить(чужих=3)
        запросы = " ".join(str(q[0]) for q in курсор.запросы)
        self.assertIn("bt_3_word_confirm_digest", запросы)

    def test_last_owner_deleting_removes_the_shared_word_too(self):
        _, _, решение = self._удалить(чужих=0)
        решение.assert_called_once_with(78, "delete")


if __name__ == "__main__":
    unittest.main()
