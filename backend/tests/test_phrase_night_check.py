"""Ночная проверка фраз: молча правим только то, в чём согласны ОБА судьи.

Почему два судьи. Замер 06.08.2026 на выборке 80 фраз: один судья дал 10 «ошибок», и
примерно треть из них — выдумка. Признак выдумки виден сразу: «предлог не тот»,
а в исправленном варианте дописана только точка. Второй независимый прогон такое не
повторяет, поэтому расхождение — надёжный сигнал «не уверен».

Почему порядок слов никогда не правим молча. Кусок, вырванный из предложения, законно
выглядит переставленным: «Was Sie sich dadurch erhoffen?» верно как придаточное и
неверно как самостоятельный вопрос. Откуда человек взял фразу, мы не знаем — значит
решает владелец, а не мы. Решение владельца 06.08.2026.
"""
import unittest

from backend.phrase_night_check import SILENT_CATEGORIES, _both_agree


def v(verdict="error", category="rechtschreibung", corrected="", why="", proposal=""):
    return {"verdict": verdict, "category": category, "corrected": corrected,
            "proposal": proposal, "why": why}


class JudgeVariantsTests(unittest.TestCase):
    """Что владелец видит кнопками в /admin_phrase_review.

    Судей двое, и расходятся они постоянно — из-за этого фраза туда и попадает. Одна
    кнопка «Принять» молча брала вариант первого судьи: по кнопке нельзя было понять,
    какую из двух правок принимаешь. Теперь у каждого варианта своя кнопка со своим
    номером, и тот же номер стоит рядом с вариантом в тексте."""

    def test_two_different_fixes_give_two_buttons(self):
        from backend.database import phrase_review_variants
        got = phrase_review_variants([
            v(category="wortstellung", corrected="Er hat hochbekommen"),
            v(category="wortstellung", corrected="Er hat hoch bekommen"),
        ])
        self.assertEqual([x["text"] for x in got],
                         ["Er hat hochbekommen", "Er hat hoch bekommen"])
        self.assertEqual([x["judge"] for x in got], [1, 2])

    def test_identical_fixes_collapse_into_one_button(self):
        from backend.database import phrase_review_variants
        got = phrase_review_variants([v(corrected="Ich habe Hunger"), v(corrected="Ich habe Hunger")])
        self.assertEqual(len(got), 1)

    def test_incomplete_phrase_offers_the_completed_text(self):
        """Судья сказал «фраза не полная, нет местоимения» — и обязан показать, ЧТО
        дописать. Диагноз без готового варианта заставляет владельца печатать руками."""
        from backend.database import phrase_review_variants
        got = phrase_review_variants([
            v(verdict="error", category="kongruenz", corrected="",
              proposal="das Anliegen an ihn", why="Фраза не полная, нет местоимения."),
        ])
        self.assertEqual([x["text"] for x in got], ["das Anliegen an ihn"])
        self.assertEqual(got[0]["field"], "proposal")

    def test_completion_never_reaches_the_silent_night_fix(self):
        """Дописать слова за человека — решение о смысле, его принимает владелец.
        Ночь молча правит только то, что оба судьи выдали в `corrected`."""
        agreed, _c, _f = _both_agree([
            v(corrected="", proposal="das Anliegen an ihn"),
            v(corrected="", proposal="das Anliegen an ihn"),
        ])
        self.assertFalse(agreed)


class BothJudgesMustAgreeTests(unittest.TestCase):
    def test_word_for_word_agreement_is_accepted(self):
        agreed, category, fix = _both_agree([
            v(corrected="Ich habe mir beim Sport das Bein gezerrt"),
            v(corrected="Ich habe mir beim Sport das Bein gezerrt"),
        ])
        self.assertTrue(agreed)
        self.assertEqual(category, "rechtschreibung")
        self.assertEqual(fix, "Ich habe mir beim Sport das Bein gezerrt")

    def test_different_correction_is_refused(self):
        """Та самая выдумка: категория одна, а правки разные."""
        agreed, _c, _f = _both_agree([
            v(category="praeposition", corrected="Heute bin ich mit Arbeit überlastet."),
            v(category="praeposition", corrected="Heute bin ich durch Arbeit überlastet"),
        ])
        self.assertFalse(agreed)

    def test_different_category_is_refused(self):
        agreed, _c, _f = _both_agree([
            v(category="kasus", corrected="одинаково"),
            v(category="praeposition", corrected="одинаково"),
        ])
        self.assertFalse(agreed)

    def test_one_judge_says_context_and_nothing_is_applied(self):
        agreed, _c, _f = _both_agree([
            v(corrected="Was erhoffen Sie sich dadurch?"),
            v(verdict="context", category="wortstellung", corrected=""),
        ])
        self.assertFalse(agreed)

    def test_empty_correction_is_refused(self):
        agreed, _c, _f = _both_agree([v(corrected=""), v(corrected="")])
        self.assertFalse(agreed)

    def test_a_dead_judge_never_lets_a_fix_through(self):
        self.assertFalse(_both_agree([v(corrected="x"), {}])[0])
        self.assertFalse(_both_agree([{}, {}])[0])
        self.assertFalse(_both_agree([v(corrected="x")])[0])


class WhatWeFixSilentlyTests(unittest.TestCase):
    def test_word_order_is_never_fixed_silently(self):
        self.assertNotIn("wortstellung", SILENT_CATEGORIES)
        self.assertNotIn("stil", SILENT_CATEGORIES)

    def test_context_independent_errors_are_fixed_silently(self):
        for category in ("rechtschreibung", "kongruenz", "kasus", "praeposition"):
            self.assertIn(category, SILENT_CATEGORIES)


class DeletionRuleTests(unittest.TestCase):
    """Правило владельца 06.08.2026: удаляя фразу из общего словаря, убираем и подписные
    карточки — но ТОЛЬКО те, куда человек не вписал своих полей. Свою карточку человека
    не трогаем никогда."""

    def test_rule_is_written_down_in_the_deletion_query(self):
        import pathlib
        src = (pathlib.Path(__file__).resolve().parents[1] / "database.py").read_text(encoding="utf-8")
        # ровно тело функции: окно «столько-то символов» рвалось, стоило добавить в
        # начало функции ещё одну ветку решения
        start = src.index("def apply_phrase_review_decision")
        block = src[start:src.index("def rebuild_unit_breakdown", start)]
        self.assertIn("origin_process = 'subscription'", block,
                      "удаляем не только подписные карточки")
        self.assertIn("user_notes", block, "не проверяем личные поля человека")
        self.assertIn("jsonb_array_length(q.user_notes) = 0", block)


class EmptyComplaintTests(unittest.TestCase):
    """Придирка без содержания не должна доходить до владельца.

    Два вида, оба пойманы на живой очереди 08.08.2026 (18 из 40 открытых вопросов):
      • «лучше 'an mir' заменить на 'an mir'» — правки нет, только слова;
      • правка, отличающаяся от фразы одной точкой в конце, при вердикте «неправильный
        предлог с дательным падежом».
    Кнопка «Принять» на такое не меняет ничего, но выглядит как решение. Владелец
    справедливо назвал это издевательством: «он же тупо переписал мою фразу»."""

    def test_a_fix_that_only_adds_a_final_dot_is_not_a_fix(self):
        from backend.database import phrase_review_variants
        text = "Wir treffen uns am fünften jeden Monats"
        got = phrase_review_variants(
            [v(category="kasus", corrected=text + ".")], text)
        self.assertEqual(got, [])

    def test_end_punctuation_alone_is_noise_not_a_question(self):
        from backend.database import phrase_review_is_noise
        text = "Wir treffen uns am fünften jeden Monats"
        self.assertTrue(phrase_review_is_noise(
            [v(category="kasus", corrected=text + ".", why="Неправильный падеж.")], text))

    def test_a_complaint_without_any_fix_is_noise(self):
        from backend.database import phrase_review_is_noise
        text = "Die Erinnerung hat an mir genagt"
        self.assertTrue(phrase_review_is_noise(
            [v(category="praeposition", corrected="",
               why="Лучше 'an mir' заменить на 'an mir'.")], text))

    def test_a_real_disagreement_is_still_a_question(self):
        from backend.database import phrase_review_is_noise
        text = "Er hat hoch bekommen"
        self.assertFalse(phrase_review_is_noise(
            [v(category="wortstellung", corrected="Er hat hochbekommen"),
             v(category="wortstellung", corrected="Er hat es hochbekommen")], text))

    def test_judges_are_told_to_ignore_the_final_dot(self):
        """Фильтр на выходе — страховка. Судью надо учить не придираться к точке в
        конце: словарная запись это не связный текст."""
        import pathlib
        src = (pathlib.Path(__file__).resolve().parents[1] / "openai_manager.py").read_text(encoding="utf-8")
        # ровно тело функции: окно «столько-то символов» рвётся, стоит дописать
        # в docstring абзац
        start = src.index("def run_phrase_grammar_verdict")
        block = src[start:src.index("\ndef ", start + 10)]
        self.assertIn("IGNORE punctuation at the very END", block)
        self.assertIn("Never claim an error you cannot fix", block)

    def test_night_does_not_queue_an_empty_complaint(self):
        import pathlib
        src = (pathlib.Path(__file__).resolve().parents[1] / "phrase_night_check.py").read_text(encoding="utf-8")
        start = src.index("def run_phrase_night_check")
        block = src[start:]
        self.assertIn("phrase_review_is_noise", block,
                      "пустые придирки снова копятся в очереди владельца")
        self.assertLess(block.index("phrase_review_is_noise"),
                        block.index('if any(str(j.get("verdict") or "") == "error"'),
                        "проверка на пустоту должна стоять ДО постановки вопроса")


class MeaningIsTheContextTests(unittest.TestCase):
    """Предлог в немецком выбирается по СМЫСЛУ, значит судья обязан видеть перевод.

    `sich mit etw. wappnen` — вооружиться ЧЕМ (средством), `sich gegen etw. wappnen` —
    вооружиться ПРОТИВ чего (угрозы). Верны оба. Судья, видевший только немецкую строку,
    брал более частое управление: на «Wappnen mit» с переводом «запастись чем-то,
    вооружаться аргументами» ОБА судьи независимо потребовали `gegen` — и оба ошиблись.
    Владелец это заметил и был прав: контекст — это перевод.

    Проверено после правки (08.08.2026, два прогона): с переводом вердикт «ошибки нет»,
    без перевода — «зависит от контекста», а не «ошибка»."""

    def test_night_hands_the_translation_to_both_judges(self):
        import pathlib
        src = (pathlib.Path(__file__).resolve().parents[1] / "phrase_night_check.py").read_text(encoding="utf-8")
        start = src.index("def _judge_twice")
        self.assertIn("translation", src[start:start + 700],
                      "судья снова судит вслепую, без смысла фразы")
        self.assertIn('_judge_twice(row["text"], row["kind"], row.get("translation")',
                      src, "ночь не передаёт перевод в судью")

    def test_prompt_makes_the_saved_meaning_decide(self):
        import pathlib
        src = (pathlib.Path(__file__).resolve().parents[1] / "openai_manager.py").read_text(encoding="utf-8")
        start = src.index("def run_phrase_grammar_verdict")
        block = src[start:src.index("\ndef ", start + 10)]
        self.assertIn("That meaning is the CONTEXT", block)
        self.assertIn("sich gegen etw. wappnen", block,
                      "правило без примера читается как общие слова")
        self.assertIn('verdict=\\"context\\"', block,
                      "без перевода предлог всё ещё объявляется ошибкой")

    def test_judge_translates_its_own_suggestion(self):
        """Владелец должен видеть, сохранил ли судья смысл или подменил его."""
        from backend.database import phrase_review_variants
        got = phrase_review_variants([
            v(category="praeposition", corrected="Wappnen gegen",
              why="…") | {"corrected_ru": "вооружиться против чего-то"},
        ], "Wappnen mit")
        self.assertEqual(got[0]["ru"], "вооружиться против чего-то")

    def test_self_contradicting_verdict_never_reaches_the_owner(self):
        """«ошибка в предлоге… однако предложение грамматически корректно» и никакой
        правки — это шум. Настоящий ответ судьи на живой фразе владельца."""
        from backend.database import phrase_review_is_noise
        text = "Die Erinnerung hat an mir genagt"
        self.assertTrue(phrase_review_is_noise([
            v(category="praeposition", corrected="",
              why="Ошибка в предлоге, однако предложение грамматически корректно."),
        ], text))


if __name__ == "__main__":
    unittest.main()
