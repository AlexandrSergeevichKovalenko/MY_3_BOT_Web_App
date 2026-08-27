# -*- coding: utf-8 -*-
"""Перевод карточки поднимается в общий слой — с проверкой и без молчания.

ЧТО БЫЛО СЛОМАНО (разобрано с владельцем 27.08.2026). Дверь сохранения клала на склад
только немецкую половину фразы, а связь на русский тянула ТОЛЬКО из разбора — из
значений, сочинённых моделью. Русский, с которым карточку сохранили, не поднимался
никогда: строчки кода для этого не было. У фразы разбора обычно не бывает (ночной добор
берёт одиночные слова), поэтому связь у неё не появлялась ВООБЩЕ.

Цена дефекта: ночная проверка грамматики отбирает фразы по связи, значит такие фразы не
проверялись ни разу. Замер 27.08.2026: 1 216 единиц без связи, и за неделю мимо ушло 345
новых фраз из 404 — 85%.

Здесь заперты правила починки. Каждое из них — решение владельца, «упрощать» их нельзя.
"""
import pathlib
import unittest
from unittest.mock import patch


def _src(rel: str) -> str:
    return (pathlib.Path(__file__).resolve().parents[2] / rel).read_text(encoding="utf-8")


ROWS = [
    {"unit_id": 101, "display": "Schwein haben", "kind": "collocation",
     "translation": "повезти (разг.)", "people": 3},
    {"unit_id": 102, "display": "Glück haben", "kind": "collocation",
     "translation": "иметь свинью", "people": 1},
]


class PromotionRulesTests(unittest.TestCase):
    def _run(self, verdicts, **kw):
        """Прогон порции с подменёнными проверкой и записью."""
        from backend import translation_links as tl

        calls = {"linked": [], "asked": []}

        def fake_check(*, german, russian, kind="collocation"):
            return verdicts[german]

        def fake_link(unit_id, russian):
            calls["linked"].append((unit_id, russian))
            return True

        def fake_ask(unit_id, display, russian, why):
            calls["asked"].append((unit_id, russian, why))
            return True

        with patch.object(tl, "units_missing_ru_link", return_value=list(ROWS)), \
             patch.object(tl, "_link_translation", side_effect=fake_link), \
             patch.object(tl, "_ask_owner", side_effect=fake_ask), \
             patch.object(tl, "count_units_missing_ru_link", return_value=0), \
             patch("backend.openai_manager.run_translation_pair_check", side_effect=fake_check):
            report = tl.promote_card_translations(**kw)
        return report, calls

    def test_confirmed_translation_becomes_a_link(self):
        report, calls = self._run({
            "Schwein haben": {"checked": True, "ok": True, "why": ""},
            "Glück haben": {"checked": True, "ok": True, "why": ""},
        })
        self.assertEqual(report["поднято"], 2)
        self.assertEqual(calls["linked"], [(101, "повезти (разг.)"), (102, "иметь свинью")])
        self.assertEqual(report["ушло владельцу"], 0)

    def test_rejected_translation_goes_to_the_owner_and_is_not_linked(self):
        report, calls = self._run({
            "Schwein haben": {"checked": True, "ok": True, "why": ""},
            "Glück haben": {"checked": True, "ok": False,
                            "why": "буквальный перевод не передаёт значение"},
        })
        self.assertEqual(report["поднято"], 1)
        self.assertEqual(report["ушло владельцу"], 1)
        self.assertEqual([u for u, _r in calls["linked"]], [101])
        self.assertEqual(calls["asked"][0][0], 102)

    def test_no_answer_is_neither_good_nor_bad(self):
        """⛔ «Спросить не удалось» — не «хорошо» и не «плохо».

        Записать непроверенное значит вернуть ровно ту дыру, ради которой всё и делалось,
        а завести вопрос владельцу — отнять его время на молчание сети."""
        report, calls = self._run({
            "Schwein haben": {"checked": False},
            "Glück haben": {"checked": False},
        })
        self.assertEqual(report["не смогли спросить"], 2)
        self.assertEqual(calls["linked"], [])
        self.assertEqual(calls["asked"], [])

    def test_budget_stops_the_batch(self):
        """Потолок расхода обязателен: ночная работа без потолка однажды уже показала
        себя на счёте, а не в логе."""
        report, calls = self._run(
            {"Schwein haben": {"checked": True, "ok": True, "why": ""},
             "Glück haben": {"checked": True, "ok": True, "why": ""}},
            budget_usd=0.0)
        self.assertEqual(report["поднято"], 0)
        self.assertEqual(calls["linked"], [])


class RankTests(unittest.TestCase):
    """Ранг связи — решение владельца 27.08.2026: ниже вычитки, выше пула."""

    def test_rank_sits_between_owner_and_pool(self):
        from backend.lex_units import OWNER_CHOICE_SOURCE
        from backend.translation_links import LINK_RANK, LINK_SOURCE
        self.assertEqual(LINK_SOURCE, "перевод карточки")
        self.assertGreater(LINK_RANK, 1, "перевод карточки перебивает вычитку владельца")
        self.assertLess(LINK_RANK, 10, "перевод карточки уехал ниже машинного пула")
        self.assertEqual(OWNER_CHOICE_SOURCE, "вычитка")

    def test_owner_own_translation_goes_the_owner_door(self):
        """Свой перевод владельца пишется тем же путём, что и на экране спорных фраз:
        один путь на всё приложение, иначе выдача и запись разъедутся (20.08.2026)."""
        block = _src("backend/database.py")
        i = block.index("def apply_translation_link_decision(")
        tail = block[i:i + 3000]
        self.assertIn("promote_owner_translation", tail)
        self.assertIn("_link_translation", tail)


class QuestionKindTests(unittest.TestCase):
    def test_three_kinds_are_told_apart(self):
        from backend.database import (
            PANEL_REVIEW_CATEGORY, TRANSLATION_REVIEW_CATEGORY, phrase_review_is_panel,
            phrase_review_kind,
        )
        grammar = [{"verdict": "error", "category": "praeposition"}]
        panel = [{"verdict": "doubt", "category": PANEL_REVIEW_CATEGORY}]
        translation = [{"verdict": "doubt", "category": TRANSLATION_REVIEW_CATEGORY}]
        self.assertEqual(phrase_review_kind(grammar), "grammar")
        self.assertEqual(phrase_review_kind(panel), "panel")
        self.assertEqual(phrase_review_kind(translation), "translation")
        # Грамматические механизмы (третий судья, пересуд вслепую, закрытие бесспорных)
        # спрашивают одно: их это работа или нет.
        self.assertFalse(phrase_review_is_panel(grammar))
        self.assertTrue(phrase_review_is_panel(panel))
        self.assertTrue(phrase_review_is_panel(translation))

    def test_payload_marks_the_translation_question(self):
        from backend.backend_server import _phrase_review_payload
        from backend.database import TRANSLATION_REVIEW_CATEGORY
        review = [{
            "id": 9, "unit_id": 101, "text": "Schwein haben", "translation": "иметь свинью",
            "judges": [{"verdict": "doubt", "category": TRANSLATION_REVIEW_CATEGORY,
                        "why": "буквальный перевод не передаёт значение"}],
            "arbiter": None, "card": None, "history": [],
        }]
        with patch("backend.database.list_open_phrase_reviews", return_value=review):
            item = _phrase_review_payload()["items"][0]
        self.assertEqual(item["kind"], "translation")
        self.assertEqual(item["variants"], [], "у вопроса о переводе нечего выбирать")

    def test_cards_counter_holds_both_card_questions(self):
        """Владельцу они приходят ОДНИМ сообщением по вторникам и пятницам."""
        block = _src("backend/database.py")
        i = block.index("def count_open_phrase_reviews_by_kind(")
        self.assertIn('out["cards"] = out["panel"] + out["translation"]', block[i:i + 1500])
        bot = _src("bot_3.py")
        j = bot.index("def _send_panel_cards_reminder(")
        self.assertIn('.get("cards")', bot[j:j + 2500],
                      "сообщение считает только один вид карточек")


class ScreenTests(unittest.TestCase):
    def test_buttons_say_what_happens(self):
        src = _src("frontend/src/answer/PhraseReviewScreen.jsx")
        self.assertIn("Сохранить этот перевод как общий", src)
        self.assertIn("Сохранить свой перевод", src)
        self.assertIn("Оставить личным", src,
                      "«оставить как есть» на вопросе о переводе значит другое")

    def test_own_field_of_a_translation_question_is_the_russian_half(self):
        src = _src("frontend/src/answer/PhraseReviewScreen.jsx")
        i = src.index("const saveOwn = ")
        self.assertIn("link_own", src[i:i + 700],
                      "свой перевод уходит решением о немецком тексте")

    def test_the_check_objection_is_shown_not_hidden(self):
        src = _src("frontend/src/answer/PhraseReviewScreen.jsx")
        self.assertIn("Что говорит проверка:", src)

    def test_the_card_door_opens_both_card_questions(self):
        overlay = _src("frontend/src/answer/AnswerOverlay.jsx")
        self.assertIn('only="cards"', overlay)
        src = _src("frontend/src/answer/PhraseReviewScreen.jsx")
        self.assertIn("only === 'cards'", src)


class NightWiringTests(unittest.TestCase):
    def test_the_lift_runs_before_the_grammar_check(self):
        """Связь, протянутая ночью, должна успеть сделать фразу видимой для проверки
        грамматики В ТУ ЖЕ НОЧЬ, а не через сутки."""
        bot = _src("bot_3.py")
        lift = bot.index("_run_translation_links_safe,\n            \"cron\",")
        grammar = bot.index("_run_phrase_night_check_safe,\n            \"cron\",")
        self.assertLess(lift, grammar, "подъём переводов переехал ПОСЛЕ проверки фраз")
        self.assertIn('"TRANSLATION_LINKS_MINUTE") or "20"', bot)
        self.assertIn('"PHRASE_NIGHT_CHECK_MINUTE") or "40"', bot)

    def test_the_owner_sees_the_numbers(self):
        bot = _src("bot_3.py")
        i = bot.index("def _translation_links_line(")
        tail = bot[i:i + 2000]
        for word in ("поднято", "ушло владельцу", "не смогли спросить", "осталось"):
            self.assertIn(word, tail, f"число «{word}» не доходит до владельца")


if __name__ == "__main__":
    unittest.main()
