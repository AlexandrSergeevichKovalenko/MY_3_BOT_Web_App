# -*- coding: utf-8 -*-
"""Разрешённый спор — это уже ОТВЕТ, и ночь применяет его сама.

ПОВОД. Владелец 28.08.2026, глядя на экран проверки: «а если третий судья рассудил
спор, то зачем тут я? Давай принимать то, что судья оставил».

Он прав. Третьего судью научили только ПОКАЗЫВАТЬ вердикт — половину «применить» не
написал никто. Замер по живой базе 28.08.2026 по 104 открытым вопросам про немецкий:
вердикт третьего есть у ВСЕХ 104, и 64 из них выбирают правку судьи, прошедшую нашу
проверку. Три четверти очереди владельца висели на нём без причины.

ПЛАНКА ЗДЕСЬ НЕ НИЖЕ, ЧЕМ У МОЛЧАЛИВОЙ ПРАВКИ, а по одному признаку выше:
    `_both_agree`         — двое судей сошлись дословно + обе правки прошли проверку;
    вердикт третьего      — правку предложил судья + НАША проверка её пропустила +
                            третий судья, видевший обе стороны, выбрал именно её.

ЧТО НОЧЬ НЕ БЕРЁТ, И ЭТО ГЛАВНОЕ В ЭТОМ ФАЙЛЕ:
  · `proposal` — достройку. Судья дописывает подлежащее или сказуемое, и словарная
    запись превращается в предложение («Leiche verwesen» → «Die Leiche verwest»).
    Это решение о СМЫСЛЕ, оно за владельцем — его правило от 06.08.2026, живущее
    в `_both_agree`. Замер 28.08.2026: таких вердиктов 22 из 104;
  · СВОЙ текст третьего судьи. У правки судьи есть его собственная подпись, чем она
    является; у текста, написанного третьим, такой подписи нет. Разбор всех 14 таких
    случаев 29.08.2026: семь перестраивают запись ровно как достройка («Die Karriere
    von Null an aufgebaut» → «Ich baue die Karriere von Null an auf.»), и отличить их
    можно только счётом слов — то есть догадкой. Гадать здесь нельзя;
  · правку, которую наша проверка не пропустила, — независимо от мнения третьего.
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SECOND_VOICE_CHECK_DISABLED", "1")

from backend import phrase_night_check as ночь  # noqa: E402

ПРОШЛА = {"checked": True, "grammar_ok": True, "meaning_kept": True}
НЕ_ПРОШЛА = {"checked": True, "grammar_ok": True, "meaning_kept": False}

# Живая запись #344: оба судьи дали одну правку, третий выбрал первого.
СУДЬИ_ПРАВКА = [
    {"verdict": "error", "category": "wortstellung",
     "corrected": "Er hat sich hartnäckig geweigert",
     "corrected_check": dict(ПРОШЛА)},
    {"verdict": "error", "category": "wortstellung",
     "corrected": "Er hat sich hartnäckig geweigert",
     "corrected_check": dict(ПРОШЛА)},
]
# Живая запись #441: третий выбрал ДОСТРОЙКУ — фраза стала предложением.
СУДЬИ_ДОСТРОЙКА = [
    {"verdict": "error", "category": "wortstellung",
     "corrected": "Im Hof einen Schneemann bauen", "corrected_check": dict(ПРОШЛА),
     "proposal": "Ich baue im Hof einen Schneemann", "proposal_check": dict(ПРОШЛА)},
]


class ЧтоНочьБерётСама(unittest.TestCase):
    def test_a_checked_fix_chosen_by_the_arbiter_is_taken(self):
        текст, почему = ночь.settled_verdict_to_apply(
            СУДЬИ_ПРАВКА, {"winner": 1, "why": "Первый вариант верен."})
        self.assertEqual(текст, "Er hat sich hartnäckig geweigert")
        self.assertIn("верен", почему)

    def test_a_built_up_phrase_stays_with_the_owner(self):
        """Дописанные слова меняют смысл записи — это его решение, не наше."""
        # winner=2 указывает на второй предложенный текст, а это proposal.
        текст, почему = ночь.settled_verdict_to_apply(
            СУДЬИ_ДОСТРОЙКА, {"winner": 2, "why": "Полное предложение лучше."})
        self.assertEqual(текст, "")
        self.assertIn("достройка", почему)

    def test_an_unsigned_text_of_the_arbiter_is_not_applied(self):
        """Вердикты до 29.08.2026 подписи не несут — их текст не применяется."""
        текст, почему = ночь.settled_verdict_to_apply(
            СУДЬИ_ПРАВКА, {"winner": 1, "better": "Er weigerte sich hartnäckig.",
                           "better_check": dict(ПРОШЛА)})
        self.assertEqual(текст, "")
        self.assertIn("не подписал", почему)

    def test_the_arbiter_signs_his_text_as_a_fix_and_it_is_applied(self):
        """Владелец 29.08.2026: «научи третьего судью самого подписывать свой текст»."""
        текст, _ = ночь.settled_verdict_to_apply(
            СУДЬИ_ПРАВКА, {"winner": 0, "better": "Er weigerte sich hartnäckig.",
                           "better_kind": "fix", "better_check": dict(ПРОШЛА)})
        self.assertEqual(текст, "Er weigerte sich hartnäckig.")

    def test_the_arbiter_signs_a_rebuild_and_it_stays_with_the_owner(self):
        """«Die Karriere von Null an aufgebaut» → «Ich baue die Karriere…» — его решение."""
        текст, почему = ночь.settled_verdict_to_apply(
            СУДЬИ_ПРАВКА, {"winner": 0, "better": "Ich baue die Karriere von Null an auf.",
                           "better_kind": "rebuild", "better_check": dict(ПРОШЛА)})
        self.assertEqual(текст, "")
        self.assertIn("перестроил", почему)

    def test_a_signed_fix_our_check_rejected_is_still_refused(self):
        """Подпись судьи не отменяет приговор нашей проверки."""
        текст, почему = ночь.settled_verdict_to_apply(
            СУДЬИ_ПРАВКА, {"winner": 0, "better": "Er weigert sich hartnäckig.",
                           "better_kind": "fix", "better_check": dict(НЕ_ПРОШЛА)})
        self.assertEqual(текст, "")
        self.assertIn("не пропустила", почему)

    def test_case_and_spaces_do_not_break_the_signature(self):
        """Модель возвращает то «Fix», то «fix » — это одна и та же подпись.
        Регистр и пробелы снимаются и здесь, и в `openai_manager`: разъедься эти два
        места — подпись перестала бы узнаваться молча."""
        текст, _ = ночь.settled_verdict_to_apply(
            СУДЬИ_ПРАВКА, {"winner": 0, "better": "Er weigerte sich.",
                           "better_kind": "  FIX ", "better_check": dict(ПРОШЛА)})
        self.assertEqual(текст, "Er weigerte sich.")

    def test_a_made_up_signature_is_not_a_signature(self):
        """Что угодно, кроме двух написанных слов, — это «не подписал»."""
        for подпись in ("maybe", "правка", "", "fixed"):
            текст, _ = ночь.settled_verdict_to_apply(
                СУДЬИ_ПРАВКА, {"winner": 0, "better": "Er weigerte sich.",
                               "better_kind": подпись, "better_check": dict(ПРОШЛА)})
            self.assertEqual(текст, "", подпись)

    def test_a_fix_our_check_rejected_is_never_taken(self):
        """Мнение третьего не отменяет приговор нашей проверки."""
        судьи = [{"verdict": "error", "corrected": "Jemand klagt über + A",
                  "corrected_check": dict(НЕ_ПРОШЛА)}]
        текст, почему = ночь.settled_verdict_to_apply(судьи, {"winner": 1, "why": "ок"})
        self.assertEqual(текст, "")
        self.assertIn("не пропустила", почему)

    def test_no_verdict_means_no_action(self):
        self.assertEqual(ночь.settled_verdict_to_apply(СУДЬИ_ПРАВКА, None)[0], "")
        self.assertEqual(ночь.settled_verdict_to_apply(СУДЬИ_ПРАВКА, {"winner": 9})[0], "")


class Прогон(unittest.TestCase):
    """Ночь ходит ТОЙ ЖЕ дверью, что и кнопка владельца, и отчитывается строками."""

    def _прогнать(self, строки, итоги):
        class Курсор:
            def execute(self, sql, params=None):
                self.последний = sql

            def fetchall(self):
                return строки

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        class Соединение:
            def cursor(self, *a, **k):
                return Курсор()

            def commit(self):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        with mock.patch("backend.database.get_db_connection_context",
                        return_value=Соединение()), \
             mock.patch("backend.database.apply_phrase_review_decision",
                        side_effect=итоги) as дверь:
            out = ночь.apply_settled_disputes()
        return out, дверь

    def test_it_applies_through_the_same_door_and_reports_lines(self):
        строки = [(344, "hartnäckig haben er sich weigern", СУДЬИ_ПРАВКА,
                   {"winner": 1, "why": "Первый вариант верен."})]
        out, дверь = self._прогнать(
            строки, [{"text": "Er hat sich hartnäckig geweigert"}])
        дверь.assert_called_once()
        self.assertEqual(дверь.call_args.args[1], "accept")
        self.assertEqual(дверь.call_args.kwargs["chosen_text"],
                         "Er hat sich hartnäckig geweigert")
        self.assertEqual(out["применено"], 1)
        self.assertEqual(out["строки"][0]["было"], "hartnäckig haben er sich weigern")
        self.assertEqual(out["строки"][0]["стало"], "Er hat sich hartnäckig geweigert")

    def test_what_it_does_not_take_is_counted_and_named(self):
        """Оставленное владельцу — не молчание, а число с причиной."""
        строки = [(441, "der Im Hof einen Schneemann bauen", СУДЬИ_ДОСТРОЙКА,
                   {"winner": 2, "why": "Полное предложение лучше."})]
        out, дверь = self._прогнать(строки, [])
        дверь.assert_not_called()
        self.assertEqual(out["оставлено владельцу"], 1)
        self.assertEqual(sum(out["причины"].values()), 1)

    def test_an_edit_that_changed_nothing_is_not_counted_as_applied(self):
        """Пустой ответ двери — правка не применилась. Считать её сделанной нельзя."""
        строки = [(344, "hartnäckig haben er sich weigern", СУДЬИ_ПРАВКА,
                   {"winner": 1, "why": "ок"})]
        out, _ = self._прогнать(строки, [{"text": ""}])
        self.assertEqual(out["применено"], 0)
        self.assertEqual(out["не вышло"], 1)


class ПримеровДва(unittest.TestCase):
    """Владелец 29.08.2026: «двух примеров достаточно; меньше — догнать до двух»."""

    def test_the_card_holds_two(self):
        from backend.example_retry import ПРИМЕРОВ_В_КАРТОЧКЕ, SYSTEM
        self.assertEqual(ПРИМЕРОВ_В_КАРТОЧКЕ, 2)
        self.assertIn("TWO new examples", SYSTEM,
                      "промпт полной пересборки просит другое число — они разъедутся")


if __name__ == "__main__":
    unittest.main()
