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
# Строка запроса экрана: с 28.08.2026 она везёт ещё ВИД вопроса и карточку —
# панельный вопрос спрашивает о наполнении карточки, и предмет спора нужен на экране.
ФРАЗА_СТРОКА = (77, "Auftreten vonKrankheitssymptomen", "Появление симптомов заболевания",
                СУДЬИ, None, 4242, "grammar", None)
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
                    [{"verdict": "error", "category": "wortstellung"}], None, 5,
                    "grammar", None)
        карточки, _ = _экран([[], [придирка]])
        self.assertEqual(карточки, [])

    def test_phrase_without_a_variant_but_without_a_claim_still_shows_up(self):
        """Проверяющие не назвали ошибку, но и не сошлись — решает человек."""
        спорная = (79, "Alles Banane", "всё нормально",
                   [{"verdict": "ok", "category": ""}], None, 6, "grammar", None)
        карточки, _ = _экран([[], [спорная]])
        self.assertEqual(карточки[0]["variants"], [])
        self.assertIn("разошлись", карточки[0]["why"])

    def test_a_fix_our_own_check_rejected_is_not_offered_in_one_tap(self):
        """Вариант с `check_disputed_by_arbiter` — это правка, которую НАША проверка
        забраковала, а третейский судья назвал верной.

        У владельца такой вариант на экране есть, но рядом печатается возражение
        проверки (backend_server.py, поле «objection») — он решает зряче. Здесь места
        для возражения нет, а кнопка «Да, правильно так» читается как «система уверена».
        Отдавать одним касанием то, что система сама забраковала, нельзя: человек учит
        немецкий по нашему ответу.
        """
        спорный = dict(СУДЬИ[0], corrected="Anzeichen für einen Herzinfarkt")
        спорный["corrected_check"] = {"checked": True, "grammar_ok": True,
                                      "meaning_kept": False}
        строка = (77, "Anzeichen für einen Herzi", "Признаки сердечного приступа",
                  [спорный], {"verdict": "ok", "better": ""}, 4242, "grammar", None)
        with mock.patch("backend.database.phrase_review_variants",
                        return_value=[{"text": "Anzeichen für einen Herzinfarkt",
                                       "ru": "Признаки сердечного приступа",
                                       "check_disputed_by_arbiter": True}]), \
             mock.patch("backend.database.phrase_review_is_noise", return_value=False):
            карточки, _ = _экран([[], [строка]])
        self.assertEqual(карточки[0]["variants"], [])
        self.assertEqual(len(карточки), 1, "сама фраза с экрана исчезать не должна")

    def test_a_clean_fix_is_still_offered(self):
        with mock.patch("backend.database.phrase_review_variants",
                        return_value=[{"text": "Auftreten von Krankheitssymptomen",
                                       "ru": "Появление симптомов заболевания"}]), \
             mock.patch("backend.database.phrase_review_is_noise", return_value=False):
            карточки, _ = _экран([[], [ФРАЗА_СТРОКА]])
        self.assertEqual(len(карточки[0]["variants"]), 1)

    def test_a_question_meant_for_the_owner_never_reaches_the_learner(self):
        """В `bt_3_phrase_review` живут ТРИ вида вопроса, и они не взаимозаменяемы:
        grammar и panel — про саму фразу, translation — про перевод карточки перед
        подъёмом в общий слой, и он сформулирован ДЛЯ ВЛАДЕЛЬЦА.

        Прогон по живой базе 27.08.2026 сразу после слияния с работой соседа: все 38
        записей вида «перевод карточки» доехали до экрана проверки слов и показались
        человеку как «фраза, в которой мы усомнились» — включая одиночные слова
        «Besprechung» и «Soile». Чужой вопрос, заданный не тому человеку.
        """
        from backend.database import TRANSLATION_REVIEW_CATEGORY
        чужой = (80, "Besprechung", "совещание",
                 [{"verdict": "error", "category": TRANSLATION_REVIEW_CATEGORY}], None, 7,
                 "translation", None)
        карточки, _ = _экран([[], [чужой]])
        self.assertEqual(карточки, [])

    def test_a_real_phrase_doubt_still_reaches_the_learner(self):
        """Отсекаем ровно один вид, а не всё подряд: panel — это та же фраза,
        разобранная тремя голосами, и человеку она адресована."""
        from backend.database import PANEL_REVIEW_CATEGORY
        свой = (81, "Ich überhaupt kein Talent", "у меня совсем нет таланта",
                [{"verdict": "error", "category": PANEL_REVIEW_CATEGORY,
                  "corrected": "Ich habe überhaupt kein Talent",
                  "corrected_ru": "у меня совсем нет таланта",
                  "corrected_check": dict(ПРОВЕРКА_ПРОШЛА)}], None, 8, "panel", None)
        карточки, _ = _экран([[], [свой]])
        self.assertEqual(len(карточки), 1)

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
    ХОЗЯИН = (4242, "Auftreten vonKrankheitssymptomen", 117649764, СУДЬИ, None)

    def _применить(self, item, автор=None):
        with mock.patch.object(сводка, "_phrase_owner", return_value=автор or self.ХОЗЯИН), \
             mock.patch("backend.database.apply_phrase_review_decision",
                        return_value={"text": "Auftreten von Krankheitssymptomen"}) as решение:
            счёт = сводка.apply_decisions(117649764, [item])
        return счёт, решение

    def test_accept_goes_through_the_owners_own_machinery(self):
        """Принятый вариант уезжает ТЕКСТОМ, а не номером кнопки.

        Номер с этого экрана указывал в другой список — полный, а не урезанный, —
        и 28.08.2026 на живой базе дважды записал не то, что человек нажал (#317,
        #319). Текст в чужой список указать не может."""
        счёт, решение = self._применить(
            {"word": "Auftreten vonKrankheitssymptomen", "kind": "phrase",
             "review_id": 77, "action": "fixed",
             "variant_text": "Auftreten von Krankheitssymptomen"})
        решение.assert_called_once_with(
            77, "accept", "", 0, "", chosen_text="Auftreten von Krankheitssymptomen")
        self.assertEqual(счёт["исправлено"], 1)

    def test_a_variant_the_person_was_never_shown_is_refused(self):
        """Текст не с этого экрана не применяется, и «похожий» вместо него не берётся.

        Так закрыт весь класс: если список кнопок под рукой у человека успел
        смениться (ночь дописала судью) или текст подставлен мимо экрана, мы не
        угадываем, а честно оставляем фразу открытой — она придёт снова."""
        счёт, решение = self._применить(
            {"word": "Auftreten vonKrankheitssymptomen", "kind": "phrase",
             "review_id": 77, "action": "fixed",
             "variant_text": "Auftreten von Krankheiten"})
        решение.assert_not_called()
        self.assertEqual(счёт["не применено"], 1)
        self.assertEqual(счёт["исправлено"], 0)

    def test_accept_without_any_text_does_nothing(self):
        """Пустой выбор — не повод взять первый вариант молча."""
        счёт, решение = self._применить(
            {"word": "Auftreten vonKrankheitssymptomen", "kind": "phrase",
             "review_id": 77, "action": "fixed", "variant_text": ""})
        решение.assert_not_called()
        self.assertEqual(счёт["не применено"], 1)

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
            автор=(4242, "чужая", 514237932, СУДЬИ, None))
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
                               return_value=(4242, "Ich überhaupt kein Talent", 7,
                                             СУДЬИ, None)), \
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
