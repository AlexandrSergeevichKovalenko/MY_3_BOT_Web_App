# -*- coding: utf-8 -*-
"""Вопрос про НАПОЛНЕНИЕ карточки решается поштучно, а не «всё на пересборку».

ПОВОД, 28.08.2026. В `bt_3_phrase_review` живут разные вопросы, и «panel» — это
сомнение НЕ во фразе, а в её карточке: примеры не иллюстрируют выражение, перевод не
той формы. Кнопки «правильно так» у него не бывает и быть не может — исправлять нечего,
спор о другом. А экран рисовал его как вопрос о фразе: «проверяющие разошлись во мнении
об этой фразе» и ни одной кнопки. Замер того же дня: 77 таких вопросов из 218 открытых,
и все 77 доезжали до человека в таком виде.

Владелец 28.08.2026, дословно: «если вопрос не в самой фразе, а в наполнении карточки,
то нужно сделать гибко, чтобы я мог отдельно каждую фразу внутри карточки либо удалить,
либо откорректировать, либо оставить как есть… зачем же её полностью отправлять на
пересборку, если конкретно в этом случае я могу просто удалить один пример и всё».

ЗДЕСЬ ЗАКРЕПЛЕНО:
  1. панельный вопрос везёт на экран ПРЕДМЕТ спора — сами примеры и претензии по пунктам;
  2. правки человека применяются как есть, содержимым, а не командой «пересобери»;
  3. стороны примеров не переворачиваются: где в карточке лежал немецкий, там и лежит;
  4. вопрос закрывается ТАМ, где он стоял (`bt_3_field_checks.phrase_panel`), иначе
     карточка вернётся следующей ночью с уже исправленной претензией;
  5. «оставить как есть» — тоже решение, и оно тоже закрывает вопрос.
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SECOND_VOICE_CHECK_DISABLED", "1")

from backend import database, word_confirm_digest as сводка  # noqa: E402

ФРАЗА = "Folgen nicht erahnen können"
ПЕРЕВОД = "Не могу предугадать последствия"
ПРИМЕРЫ = [
    {"source": "Ich konnte die Folgen nicht erahnen", "target": "Я не мог предвидеть последствия"},
    {"source": "Damals konnte niemand die Folgen ahnen.", "target": "Тогда никто не мог предвидеть."},
    {"source": "Ich habe nicht geahnt, was passieren würde.", "target": "Я не ожидал, что произойдёт."},
]
ПАНЕЛЬ = [{"verdict": "doubt", "category": "панель из трёх голосов",
           "why": "Последний пример не содержит слова 'Folgen' и не иллюстрирует выражение.; "
                  "Перевод дан в форме первого лица («Не могу…») вместо инфинитива."}]


class Курсор:
    def __init__(self, ответы):
        self._ответы = list(ответы)
        self.запросы = []
        self.rowcount = 1

    def execute(self, sql, params=None):
        self.запросы.append((" ".join(str(sql).split()), params))

    def fetchone(self):
        return self._ответы.pop(0) if self._ответы else None

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class Соединение:
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


class ЭкранВезётПредметСпора(unittest.TestCase):
    """Человеку показывают то, о чём в самом деле спорят."""

    def test_claims_are_split_into_points(self):
        """Панель складывает претензии в одну строку через «; » — человеку нужны пункты."""
        пункты = сводка._претензии(ПАНЕЛЬ)
        self.assertEqual(len(пункты), 2)
        self.assertIn("Folgen", пункты[0])
        self.assertIn("первого лица", пункты[1])
        self.assertTrue(all(п.endswith(".") for п in пункты))

    def test_judge_words_reach_the_screen(self):
        """319 мнений из 322 несут обоснование, и до 28.08 не доезжало ни одно."""
        судьи = [{"why": "Неправильный порядок слов."}, {"why": "Нет подлежащего."}]
        self.assertEqual([с["n"] for с in сводка._слова_судей(судьи)], [1, 2])
        self.assertEqual(сводка._слова_судей([{"why": ""}]), [],
                         "пустое мнение не выдаём за обоснование")

    def test_arbiter_verdict_reaches_the_screen(self):
        арбитр = сводка._слова_арбитра({"winner": 2, "why": "Второй вариант полнее."})
        self.assertEqual(арбитр["winner"], 2)
        self.assertIn("полнее", арбитр["why"])
        self.assertIsNone(сводка._слова_арбитра({"winner": 1, "why": ""}),
                          "вердикт без объяснения — не вердикт")

    def test_variants_are_no_longer_cut_to_two(self):
        """Обрезка прятала третий годный вариант у 3 фраз из 104 (замер 28.08.2026)."""
        проверка = {"checked": True, "grammar_ok": True, "meaning_kept": True}
        судьи = [
            {"verdict": "error", "category": "wortstellung",
             "corrected": "первый вариант", "corrected_check": dict(проверка),
             "proposal": "второй вариант", "proposal_check": dict(проверка)},
            {"verdict": "error", "category": "wortstellung",
             "corrected": "третий вариант", "corrected_check": dict(проверка)},
        ]
        кнопки = сводка.кнопки_вариантов(судьи, "исходная фраза", None)
        self.assertEqual(len(кнопки), 3)
        self.assertEqual([к["judge"] for к in кнопки], [1, 1, 2],
                         "вариант должен знать своего судью — он стоит рядом с его словами")
        self.assertIn("достройка", кнопки[1]["kind"])
        self.assertIn("правка", кнопки[0]["kind"])


class ПравкиПрименяютсяКакЕсть(unittest.TestCase):
    def _применить(self, **kw):
        курсор = Курсор([(526, ФРАЗА, ПЕРЕВОД), ({"usage_examples": list(ПРИМЕРЫ),
                                                  "translation_ru": ПЕРЕВОД,
                                                  "word_ru": ПЕРЕВОД},)])
        with mock.patch.object(database, "get_db_connection_context",
                               return_value=Соединение(курсор)), \
             mock.patch.object(database, "_ensure_phrase_check_tables"), \
             mock.patch.object(database, "promote_owner_translation", return_value=True), \
             mock.patch("backend.lex_units.save_unit_card", return_value=True) as запись:
            итог = database.apply_panel_card_edit(348, **kw)
        return итог, курсор, запись

    def test_only_the_examples_the_person_kept_are_written(self):
        """Удалил один — остальные остаются нетронутыми, пересборки нет."""
        оставил = [{"de": ПРИМЕРЫ[0]["source"], "ru": ПРИМЕРЫ[0]["target"]},
                   {"de": ПРИМЕРЫ[1]["source"], "ru": ПРИМЕРЫ[1]["target"]}]
        итог, курсор, запись = self._применить(examples=оставил)
        карточка = запись.call_args.args[1]
        self.assertEqual(итог["examples"], 2)
        self.assertEqual([e["source"] for e in карточка["usage_examples"]],
                         [ПРИМЕРЫ[0]["source"], ПРИМЕРЫ[1]["source"]])
        запросы = " ".join(q for q, _ in курсор.запросы)
        self.assertNotIn("'дефект'", запросы, "карточка ушла на пересборку — а не должна")

    def test_sides_are_not_flipped(self):
        """В части карточек немецкий лежит в `target`. Пишем в той же ориентации."""
        курсор = Курсор([(526, ФРАЗА, ПЕРЕВОД),
                         ({"usage_examples": [{"source": "Я не мог предвидеть",
                                               "target": "Ich konnte es nicht"}]},)])
        with mock.patch.object(database, "get_db_connection_context",
                               return_value=Соединение(курсор)), \
             mock.patch.object(database, "_ensure_phrase_check_tables"), \
             mock.patch.object(database, "promote_owner_translation", return_value=True), \
             mock.patch("backend.lex_units.save_unit_card", return_value=True) as запись:
            database.apply_panel_card_edit(
                348, examples=[{"de": "Ich konnte es nicht", "ru": "Я не мог предвидеть"}])
        пример = запись.call_args.args[1]["usage_examples"][0]
        self.assertEqual(пример["source"], "Я не мог предвидеть")
        self.assertEqual(пример["target"], "Ich konnte es nicht")

    def test_half_written_example_is_not_saved(self):
        """Строка без перевода — это недописанная строка, а не «пример без перевода»."""
        итог, _, запись = self._применить(
            examples=[{"de": "Ein Satz", "ru": ""}, {"de": "", "ru": "перевод"}])
        self.assertEqual(итог["examples"], 0)
        self.assertEqual(запись.call_args.args[1]["usage_examples"], [])

    def test_translation_is_written_only_where_the_old_one_was(self):
        """Поле, куда человек вписал своё, чужой правкой не затираем."""
        итог, курсор, запись = self._применить(translation="не мочь предугадать последствия")
        карточка = запись.call_args.args[1]
        self.assertTrue(итог["translation_set"])
        self.assertEqual(карточка["translation_ru"], "не мочь предугадать последствия")
        self.assertEqual(карточка["word_ru"], "не мочь предугадать последствия")

    def test_the_question_is_closed_where_it_stood(self):
        """Иначе карточка вернётся ночью с уже исправленной претензией."""
        _, курсор, _ = self._применить(examples=[{"de": "a", "ru": "б"}])
        отметки = [(q, p) for q, p in курсор.запросы if "bt_3_field_checks" in q]
        self.assertTrue(отметки, "вопрос не закрыт там, где стоял")
        self.assertIn("подтверждено", отметки[0][1])
        решения = [q for q, _ in курсор.запросы if "UPDATE bt_3_phrase_review" in q]
        self.assertIn("'edited'", решения[0],
                      "решение записано не своим статусом — по нему потом не посчитать")

    def test_top_up_is_a_separate_verdict_not_a_rebuild(self):
        """«Добери недостающие» и «перепиши всё» — разные задания для ночи."""
        from backend.example_retry import TOP_UP
        _, курсор, _ = self._применить(examples=[{"de": "a", "ru": "б"}], top_up=True)
        отметки = [p for q, p in курсор.запросы if "bt_3_field_checks" in q]
        self.assertIn(TOP_UP, отметки[0])


class РешенияЧеловекаДоезжают(unittest.TestCase):
    """Экран шлёт содержимое, а не команду; сервер зовёт нужную дверь."""

    def _решить(self, item):
        хозяин = (526, ФРАЗА, 117649764, ПАНЕЛЬ, None)
        with mock.patch.object(сводка, "_phrase_owner", return_value=хозяин), \
             mock.patch("backend.database.apply_panel_card_edit",
                        return_value={"unit_id": 526}) as правка, \
             mock.patch("backend.database.send_panel_card_to_rewrite",
                        return_value={"unit_id": 526}) as пересборка:
            счёт = сводка.apply_decisions(117649764, [dict(item, kind="phrase",
                                                          word=ФРАЗА, review_id=348)])
        return счёт, правка, пересборка

    def test_edit_goes_to_the_card_editor(self):
        счёт, правка, пересборка = self._решить({
            "action": "edit", "translation": "  не мочь предугадать  ",
            "examples": [{"de": " Ein Satz ", "ru": " Предложение "},
                         {"de": "", "ru": "без немецкого"}],
            "top_up": True})
        пересборка.assert_not_called()
        kw = правка.call_args.kwargs
        self.assertEqual(kw["translation"], "не мочь предугадать", "текст не почищен")
        self.assertEqual(kw["examples"], [{"de": "Ein Satz", "ru": "Предложение"}])
        self.assertTrue(kw["top_up"])
        self.assertEqual(счёт["карточка поправлена"], 1)

    def test_rebuild_still_goes_to_the_night(self):
        счёт, правка, пересборка = self._решить({"action": "rebuild"})
        правка.assert_not_called()
        пересборка.assert_called_once_with(348)
        self.assertEqual(счёт["карточка на пересборку"], 1)


if __name__ == "__main__":
    unittest.main()
