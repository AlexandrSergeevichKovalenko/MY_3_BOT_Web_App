"""Правка судьи проверяется ДО того, как её кто-то увидит или применит.

Повод, разобранный с владельцем 19.08.2026. Фраза в общем словаре:

    Steck das Portemonnaie in die Tasche.      «Положи кошелек в карман»

ОБА судьи независимо предложили заменить её на:

    Steck das Portemonnaie in den Taschen.     «Положи кошелек в карманы»

Здесь два брака сразу. Немецкий неверен: «stecken in» про направление и требует
Akkusativ («in die Taschen»), а «in den Taschen» — Dativ, то есть «где». И смысл
другой: одно число стало другим — судья САМ написал это в своём переводе.

От молчаливой записи в общий словарь нас отделило только то, что судьи назвали разные
категории («kasus» и «praeposition»). Обе — в списке «правим молча», то есть совпади
они, и неверный немецкий уехал бы к людям без единого человеческого взгляда.

Отсюда два правила, которые проверяются здесь:
  1. Молча правим только правку, ПРОШЕДШУЮ проверку. Молчание проверки — не согласие.
  2. Забракованная правка не получает кнопку «Принять» на экране владельца.
"""
import unittest

import backend.database as db
from backend.phrase_night_check import _both_agree, fix_passed_check

TEXT = "Steck das Portemonnaie in die Tasche."
BAD_FIX = "Steck das Portemonnaie in den Taschen."


def judge(category, fix=BAD_FIX, check=None, span="Tasche"):
    out = {"verdict": "error", "category": category, "corrected": fix, "span": span,
           "corrected_ru": "Положи кошелек в карманы."}
    if check is not None:
        out["corrected_check"] = check
    return out


PASSED = {"checked": True, "grammar_ok": True, "meaning_kept": True, "why": ""}
BAD_GRAMMAR = {"checked": True, "grammar_ok": False, "meaning_kept": True,
               "why": "После «stecken in» нужен винительный падеж: in die Taschen."}
BAD_MEANING = {"checked": True, "grammar_ok": True, "meaning_kept": False,
               "why": "Был один карман, стало несколько."}
NOT_CHECKED = {"checked": False}


class PhraseFixIsCheckedTests(unittest.TestCase):
    def test_an_ungrammatical_fix_never_goes_silently(self):
        """⚠ 04.09.2026 судья стал один, и проверка правки — главная страховка молчаливой
        записи. Именно она поймала «in den Taschen», когда сверка двух судей не поймала."""
        self.assertFalse(_both_agree([judge("kasus", check=BAD_GRAMMAR)], TEXT)[0],
                         "неграмотная правка не имеет права уехать молча")

    def test_meaning_change_also_stops_the_silent_write(self):
        self.assertFalse(_both_agree([judge("kasus", check=BAD_MEANING)], TEXT)[0])

    def test_unchecked_fix_is_not_treated_as_a_good_one(self):
        """«Проверка не ответила» и «проверка сказала, что всё хорошо» — разные миры."""
        self.assertFalse(_both_agree([judge("kasus", check=NOT_CHECKED)], TEXT)[0])
        self.assertFalse(_both_agree([judge("kasus")], TEXT)[0])

    def test_a_checked_fix_still_goes_through(self):
        """Проверка не должна запирать всё подряд: годная правка едет как раньше."""
        good = "Weißt du zufällig Bescheid, wem sie gehört?"
        agree, category, fix = _both_agree(
            [judge("rechtschreibung", fix=good, check=PASSED)], TEXT)
        self.assertTrue(agree)
        self.assertEqual(category, "rechtschreibung")
        self.assertEqual(fix, good)

    def test_rejected_fix_gets_no_accept_button(self):
        """На экране владельца забракованная правка остаётся видимой, но без кнопки."""
        judges = [judge("kasus", check=BAD_GRAMMAR), judge("praeposition", check=BAD_MEANING)]
        variants = db.phrase_review_variants(judges, TEXT)
        self.assertEqual(variants, [], "кнопки «Принять» у неверной правки быть не должно")

    def test_unchecked_fix_keeps_its_button_but_is_marked(self):
        """Молчание проверки не повод прятать вариант от владельца — он решает сам."""
        judges = [judge("kasus", check=NOT_CHECKED)]
        variants = db.phrase_review_variants(judges, TEXT)
        self.assertEqual(len(variants), 1)
        self.assertFalse(variants[0]["checked"], "вариант помечен как непроверенный")

    def test_verdict_helper_tells_the_three_states_apart(self):
        self.assertIs(fix_passed_check(judge("kasus", check=PASSED), "corrected"), True)
        self.assertIs(fix_passed_check(judge("kasus", check=BAD_GRAMMAR), "corrected"), False)
        self.assertIsNone(fix_passed_check(judge("kasus", check=NOT_CHECKED), "corrected"))


if __name__ == "__main__":
    unittest.main()
