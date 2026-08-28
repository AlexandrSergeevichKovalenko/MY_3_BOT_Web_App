# -*- coding: utf-8 -*-
"""Экран проверки слов применяет РОВНО ту кнопку, которую нажал человек.

ПОВОД, 28.08.2026. Экран проверки показывает урезанный список вариантов: забракованные
нашей же проверкой не показываются вовсе, и кнопок не больше двух. А применение решения
брало присланный НОМЕР из ПОЛНОГО списка (`include_disputed=True`). Стоило спорному
варианту оказаться раньше — и номера разъезжались.

Замер по живой базе в тот же день: из 40 решений владельца за сутки ДВА записали не тот
текст, который он нажал. Оба случая здесь и воспроизведены — это не выдуманные данные,
а судьи и арбитр строк #317 и #319 как есть:

    #317 «Jmdm klagen über + A»
         на экране: [«Jmdm über + A klagen», «Jemand klagt über etwas.»]
         нажато второе → записалось «Jemand klagt über + A»
    #319 «Sich bewerben bei + D»
         на экране: [«Sich bei + D bewerben», «Ich bewerbe mich bei der Firma.»]
         нажато второе → записалось «Ich bewerbe mich bei + D»

Вред тут не косметический. «Jemand klagt über + A» — это ровно тот вариант, который НАША
проверка забраковала («изменился субъект и падеж»), и которого человек на экране не
видел именно поэтому. Номер провёл его на экран в обход запрета.

ЧТО ЗАКРЫВАЕТ ЭТОТ ТЕСТ: номер кнопки с этого экрана не ходит вовсе — уезжает текст, и
он обязан найтись среди тех кнопок, которые экран имел право показать.
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SECOND_VOICE_CHECK_DISABLED", "1")

from backend import word_confirm_digest as сводка  # noqa: E402

# ── Живые данные строки #317 (bt_3_phrase_review, 28.08.2026) ──────────────────
СУДЬИ_317 = [
    {"verdict": "error", "category": "wortstellung", "proposal": "", "proposal_ru": "",
     "corrected": "Jmdm über + A klagen",
     "corrected_ru": "Кому-то жаловаться на что-то",
     "corrected_check": {"checked": True, "grammar_ok": True, "meaning_kept": True}},
    {"verdict": "error", "category": "wortstellung", "proposal": "", "proposal_ru": "",
     "corrected": "Jemand klagt über + A",
     "corrected_ru": "Кто-то жалуется на + Винительный падеж",
     # НАША проверка эту правку забраковала: «изменился субъект и падеж».
     "corrected_check": {"checked": True, "grammar_ok": True, "meaning_kept": False}},
]
АРБИТР_317 = {"winner": 2, "better": "Jemand klagt über etwas.",
              "better_ru": "Кто-то жалуется на что-то.",
              "better_check": {"checked": True, "grammar_ok": True, "meaning_kept": True}}
ТЕКСТ_317 = "Jmdm klagen über + A"


class СписокКнопок(unittest.TestCase):
    """Что человек в самом деле видит на экране."""

    def test_screen_hides_the_fix_our_own_check_rejected(self):
        кнопки = сводка.кнопки_вариантов(СУДЬИ_317, ТЕКСТ_317, АРБИТР_317)
        тексты = [к["text"] for к in кнопки]
        self.assertEqual(тексты, ["Jmdm über + A klagen", "Jemand klagt über etwas."])
        self.assertNotIn("Jemand klagt über + A", тексты,
                         "забракованная проверкой правка попала человеку на кнопку")

    def test_the_full_list_is_a_different_list(self):
        """Тот самый сдвиг: в полном списке забракованный вариант стоит ВТОРЫМ.

        Пока с экрана уезжал номер, «второй» значил на двух концах разное. Тест
        держит это различие на виду, чтобы правка «а давайте выровняем флаги»
        не выглядела безобидной: списки РАЗНЫЕ по замыслу."""
        from backend.database import phrase_review_variants
        полный = [v["text"] for v in phrase_review_variants(
            СУДЬИ_317, ТЕКСТ_317, АРБИТР_317, include_disputed=True)]
        self.assertEqual(полный, ["Jmdm über + A klagen", "Jemand klagt über + A",
                                  "Jemand klagt über etwas."])


class НажатоеИЗаписанное(unittest.TestCase):
    """Что уезжает на сервер, когда человек нажал вторую кнопку."""

    def _нажать(self, текст_кнопки, судьи=СУДЬИ_317, арбитр=АРБИТР_317, текст=ТЕКСТ_317):
        хозяин = (4242, текст, 117649764, судьи, арбитр)
        with mock.patch.object(сводка, "_phrase_owner", return_value=хозяин), \
             mock.patch("backend.database.apply_phrase_review_decision",
                        return_value={"text": текст_кнопки}) as решение:
            счёт = сводка.apply_decisions(117649764, [{
                "word": текст, "kind": "phrase", "review_id": 317,
                "action": "fixed", "variant_text": текст_кнопки}])
        return счёт, решение

    def test_the_second_button_writes_the_second_button(self):
        """Живой случай #317: нажата «Jemand klagt über etwas.» — она и записана."""
        счёт, решение = self._нажать("Jemand klagt über etwas.")
        self.assertEqual(счёт["исправлено"], 1)
        self.assertEqual(решение.call_args.kwargs["chosen_text"],
                         "Jemand klagt über etwas.")

    def test_the_rejected_fix_cannot_be_applied_from_this_screen(self):
        """Даже присланный впрямую — он не с этого экрана, и мы его не применяем."""
        счёт, решение = self._нажать("Jemand klagt über + A")
        решение.assert_not_called()
        self.assertEqual(счёт["не применено"], 1)

    def test_no_index_travels_from_this_screen(self):
        """Номер не участвует: он и был причиной подмены."""
        _, решение = self._нажать("Jmdm über + A klagen")
        self.assertEqual(решение.call_args.args[3], 0,
                         "номер варианта всё ещё что-то значит — подмена вернётся")


class ВыборПоТексту(unittest.TestCase):
    """Сам отбор варианта в `apply_phrase_review_decision`: по тексту, а не по номеру."""

    def _выбранный(self, chosen_text):
        from backend import database
        итог = {}

        class Курсор:
            def __init__(self):
                self.шаги = 0

            def execute(self, sql, params=None):
                self.последний = (sql, params)

            def fetchone(self):
                self.шаги += 1
                if self.шаги == 1:      # строка проверки
                    return (4242, ТЕКСТ_317, СУДЬИ_317, АРБИТР_317)
                return None             # двойника нет

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        class Соединение:
            def cursor(self, *a, **k):
                return курсор

            def commit(self):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        курсор = Курсор()
        with mock.patch.object(database, "get_db_connection_context",
                               return_value=Соединение()), \
             mock.patch.object(database, "_ensure_phrase_check_tables"), \
             mock.patch.object(database, "spread_correction_everywhere",
                               return_value={"cards": 0, "places": 0}), \
             mock.patch.object(database, "rebuild_unit_breakdown", return_value=True), \
             mock.patch.object(database, "promote_owner_translation", return_value=True), \
             mock.patch("backend.lex_units.retitle_unit"), \
             mock.patch("backend.card_complaints.подчистить_после_переименования",
                        return_value={"пул": 0, "кеш": 0}):
            итог = database.apply_phrase_review_decision(
                317, "accept", "", 0, "", chosen_text=chosen_text)
        return итог

    def test_text_wins_over_the_number(self):
        итог = self._выбранный("Jemand klagt über etwas.")
        self.assertEqual(итог["text"], "Jemand klagt über etwas.")

    def test_a_text_that_is_not_a_variant_writes_nothing(self):
        """Не нашли — НЕ берём «похожий» и не берём первый. Вопрос закрывается пустым."""
        итог = self._выбранный("Jemand klagt laut.")
        self.assertEqual(итог["text"], "")


if __name__ == "__main__":
    unittest.main()
