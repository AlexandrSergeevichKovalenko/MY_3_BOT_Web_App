"""Ночная проверка фраз: что именно позволяет ночи поправить фразу МОЛЧА.

⚠ ЗАКОН ПЕРЕПИСАН 04.09.2026 (решение владельца: «мне достаточно чтобы один раз модель
посмотрела и всё»). Судей больше не два, и «согласие двух ответов» больше не страховка —
оно и не было ею: 19.08.2026 оба судьи дословно предложили «in den Taschen» (неверный
падеж и другое число), и спасла не их сверка, а отдельная проверка исправленного текста.

Что держит молчаливую правку теперь — четыре проверяемых условия:
  1) вердикт «ошибка» и категория из тех, что верны или неверны сами по себе;
  2) правка непустая и отличается от исходного текста не одной точкой на конце;
  3) модель ПРОЦИТИРОВАЛА неверный кусок, и цитата нашлась в самом тексте (наша
     арифметика, без запросов);
  4) исправленный текст прошёл свою проверку, а спор с ней снимает печатный справочник.

Почему порядок слов никогда не правим молча. Кусок, вырванный из предложения, законно
выглядит переставленным: «Was Sie sich dadurch erhoffen?» верно как придаточное и
неверно как самостоятельный вопрос. Откуда человек взял фразу, мы не знаем — значит
решает владелец, а не мы. Решение владельца 06.08.2026.
"""
import unittest

from backend.phrase_night_check import SILENT_CATEGORIES, _both_agree


# С 19.08.2026 согласия двух судей МАЛО: правка обязана ещё пройти проверку самой
# себя — грамотна ли она и сохранён ли смысл. Оба судьи могут ошибиться одинаково:
# на «Steck das Portemonnaie in die Tasche» оба дословно предложили «in den Taschen»
# (неверный падеж + другое число). Поэтому судья в этих проверках по умолчанию несёт
# пройденную проверку — иначе тесты мерили бы не то, ради чего написаны.
# Что бывает, когда проверка НЕ пройдена или не отвечала — test_phrase_fix_is_checked.py.
CHECK_PASSED = {"checked": True, "grammar_ok": True, "meaning_kept": True, "why": ""}


# Текст, на который «показывает» судья в этих проверках, и цитата из него: без цитаты
# молчаливая правка не проходит по условию 3.
ТЕКСТ = "Ich habe mir beim Sport das Bein gezehrt."      # опечатка: gezehrt


def v(verdict="error", category="rechtschreibung", corrected="", why="", proposal="",
      check=CHECK_PASSED, span="Bein"):
    return {"verdict": verdict, "category": category, "corrected": corrected,
            "proposal": proposal, "why": why, "span": span,
            "corrected_check": check, "proposal_check": check}


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
        Ночь молча правит только то, что судья выдал в `corrected`."""
        agreed, _c, _f = _both_agree(
            [v(corrected="", proposal="das Anliegen an ihn")], ТЕКСТ)
        self.assertFalse(agreed)


class ЧтоПропускаетМолчаливуюПравкуTests(unittest.TestCase):
    """⚠ ЗДЕСЬ БЫЛ ЗАКОН «СОГЛАСНЫ ОБА СУДЬИ». Отменён 04.09.2026 владельцем.
    Условия перечислены в шапке файла; каждое из них — проверяемый факт, а не совпадение
    двух мнений."""

    def test_a_quoted_and_checked_fix_is_applied(self):
        agreed, category, fix = _both_agree(
            [v(corrected="Ich habe mir beim Sport das Bein gezerrt", span="gezehrt")],
            ТЕКСТ)
        self.assertTrue(agreed)
        self.assertEqual(category, "rechtschreibung")
        self.assertEqual(fix, "Ich habe mir beim Sport das Bein gezerrt")

    def test_a_fix_without_a_quote_is_refused(self):
        """Не показал пальцем — не имеет права молча переписать чужую фразу."""
        self.assertFalse(_both_agree([v(corrected="Ich habe es", span="")], ТЕКСТ)[0])

    def test_a_quote_that_is_not_in_the_text_is_refused(self):
        """Та самая выдумка: цитата, которой в записи нет вовсе."""
        self.assertFalse(
            _both_agree([v(corrected="Ich habe es", span="Fahrrad")], ТЕКСТ)[0])

    def test_a_fix_that_only_adds_a_full_stop_is_refused(self):
        """«Предлог не тот», а в правке дописана только точка — пустая придирка."""
        self.assertFalse(
            _both_agree([v(category="praeposition",
                           corrected="Ich habe mir beim Sport das Bein gezerrt",
                           span="Bein")],
                        "Ich habe mir beim Sport das Bein gezerrt.")[0])

    def test_context_verdict_is_never_applied(self):
        self.assertFalse(_both_agree([v(verdict="context", category="wortstellung",
                                        corrected="Was erhoffen Sie sich dadurch?")],
                                     ТЕКСТ)[0])

    def test_empty_correction_is_refused(self):
        self.assertFalse(_both_agree([v(corrected="")], ТЕКСТ)[0])

    def test_a_dead_judge_never_lets_a_fix_through(self):
        self.assertFalse(_both_agree([{}], ТЕКСТ)[0])
        self.assertFalse(_both_agree([], ТЕКСТ)[0])
        # Два ответа сюда больше не приходят — форма изменилась, и это не «согласие».
        self.assertFalse(_both_agree([v(corrected="x"), v(corrected="x")], ТЕКСТ)[0])


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

    def test_night_hands_the_translation_to_the_judge(self):
        """⚠ Судья с 04.09.2026 один (`_judge_once`), но перевод ему уезжает так же:
        предлог и падеж выбираются по смыслу, и без перевода он судит вслепую."""
        import pathlib
        src = (pathlib.Path(__file__).resolve().parents[1] / "phrase_night_check.py").read_text(encoding="utf-8")
        start = src.index("def _judge_once")
        self.assertIn("translation", src[start:start + 900],
                      "судья снова судит вслепую, без смысла фразы")
        self.assertIn('_judge_once(row["text"], row["kind"], row.get("translation")',
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


class ПустаяПридиркаСнимаетсяАрифметикойTests(unittest.TestCase):
    """⛔ «Предлог не тот», а в правке дописана только точка — это не ошибка.

    ┌─ ЗАМЕР 04.09.2026, 63 ЖИВЫЕ ФРАЗЫ. ─────────────────────────────────────────┐
    │ Сначала я попросил судью ЦИТИРОВАТЬ неверное место полем `span`. Он вернул    │
    │ `null` ВО ВСЕХ 63 случаях, включая контрольные с заведомой ошибкой: «Die      │
    │ Finster der Nacht» исправил верно, а показать пальцем не смог. Требование,    │
    │ которое модель игнорирует, — не защита, а самообман: строгое правило «нет     │
    │ цитаты — нет находки» ослепило проверку полностью, 0 находок из 63.           │
    │                                                                              │
    │ Изменённое место теперь СЧИТАЕМ САМИ из разницы между текстом и правкой. На   │
    │ том же замере: 3 находки, обе контрольные ошибки пойманы, одна пустая         │
    │ придирка снята арифметикой, 63 фразы = 63 обращения к модели.                 │
    └──────────────────────────────────────────────────────────────────────────────┘
    """

    def test_a_fix_that_changes_nothing_is_not_an_error(self):
        from backend.phrase_night_check import _что_изменено
        self.assertEqual(
            _что_изменено("Heute bin ich mit Arbeit überlastet",
                          "Heute bin ich mit Arbeit überlastet."), "")

    def test_the_changed_word_is_named_whole(self):
        """Человеку нужно видеть СЛОВО, а не разницу в три буквы."""
        from backend.phrase_night_check import _что_изменено
        self.assertEqual(
            _что_изменено("Die Finster der Nacht machte die Straße gefährlich",
                          "Die Finsternis der Nacht machte die Straße gefährlich"),
            "Finster")
        self.assertEqual(
            _что_изменено("Ich habe gestern ins Kino gegangen",
                          "Ich bin gestern ins Kino gegangen"), "habe")

    def test_we_do_not_ask_the_model_to_quote_any_more(self):
        import pathlib
        src = (pathlib.Path(__file__).resolve().parents[1] / "openai_manager.py").read_text(encoding="utf-8")
        i = src.index("def run_phrase_grammar_verdict")
        тело = src[i:src.index("\ndef ", i + 10)]
        self.assertNotIn("`span` MUST", тело,
                         "снова просим цитату, которую модель не заполняет")
        self.assertIn("AN ERROR YOU CANNOT FIX IS NOT AN ERROR", тело)
