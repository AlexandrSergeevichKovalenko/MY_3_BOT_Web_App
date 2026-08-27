# -*- coding: utf-8 -*-
"""Экран спорных фраз: два разных вопроса, готовый ответ и никаких кругов.

Разобрано с владельцем 26.08.2026 по скриншоту живого экрана. Он задал вопрос, на
который у экрана не было ответа: «какое решение я могу принять, рассматривая это?»
Замер той же минуты по живой базе (232 открытых вопроса):

  106 — оба судьи объявили ошибку, ОБЕ их правки забракованы нашей же проверкой,
        на экране ни одной кнопки: спор двух моделей без ответа;
   79 — вообще не про грамматику: спор трёх голосов о примерах и переводе в карточке,
        положен в ту же очередь разовым скриптом 23.08 и никак не помечен;
    9 — оба судьи сказали «ошибки нет» — вопроса к человеку нет вовсе;
   29 — объяснение судьи приехало по-немецки, хотя промпт требует русский;
    0 — сохранённых вердиктов третьего судьи: его звала только кнопка.

Здесь заперты правила, которыми это чинится. Каждое из них однажды стоило владельцу
времени, поэтому «упростить» их без разговора с ним нельзя.
"""
import pathlib
import unittest
from unittest.mock import patch


def _src(rel: str) -> str:
    return (pathlib.Path(__file__).resolve().parents[2] / rel).read_text(encoding="utf-8")


PANEL_REVIEW = {
    "id": 353, "unit_id": 1439,
    "text": "Vertrauen zu jemandem haben", "translation": "",
    "judges": [{"verdict": "doubt", "category": "панель из трёх голосов",
                "corrected": "", "proposal": "",
                "why": "Примеры иллюстрируют «Vertrauen fassen», а не «Vertrauen haben»."}],
    "arbiter": None,
    "card": {"usage_examples": [
        {"source": "Am Anfang war er schüchtern, aber bald fasste er Vertrauen zu uns.",
         "target": "Сначала он стеснялся, но вскоре почувствовал к нам доверие."},
        # Часть карточек собрана со стороны «русский → немецкий»: там в `source` русский.
        {"source": "Важно почувствовать доверие к новым коллегам.",
         "target": "Es ist wichtig, bei neuen Mitarbeitern Vertrauen zu fassen."},
    ]},
    "history": [],
}

GRAMMAR_REVIEW = {
    "id": 465, "unit_id": 5146,
    "text": "Richter besteht auf der Vernehmung aller Zeugen",
    "translation": "Судья настаивает на допросе всех свидетелей",
    "judges": [{"verdict": "error", "category": "praeposition",
                "corrected": "Richter besteht auf die Vernehmung aller Zeugen",
                "corrected_ru": "Судья настаивает на допросе всех свидетелей",
                "corrected_check": {"checked": True, "grammar_ok": False,
                                    "meaning_kept": True,
                                    "why": "После bestehen auf нужен дательный падеж."},
                "why": "Нужен винительный падеж."}],
    "arbiter": {"winner": 0, "why": "Оба мимо: исходная фраза верна.",
                "better": "", "better_ru": ""},
    "card": None,
    "history": [{"status": "accepted",
                 "text": "Richter besteht auf die Vernehmung aller Zeugen",
                 "decided_text": "Richter besteht auf der Vernehmung aller Zeugen",
                 "decided_at": "2026-08-20T01:40:34+00:00"}],
}


class RepeatQuestionTests(unittest.TestCase):
    """Круг: фраза, которую владелец уже решал, второй раз не спрашивается.

    Живой случай, unit 5146: 13.08 он выбрал «auf die», 20.08 у него спросили снова и он
    вернул «auf der», 26.08 спросили в третий раз. Три решения — ноль движения."""

    def setUp(self):
        from backend.database import _phrase_text_key
        self.settled = {_phrase_text_key("Richter besteht auf der Vernehmung aller Zeugen"),
                        _phrase_text_key("Richter besteht auf die Vernehmung aller Zeugen")}

    def test_the_same_question_is_not_asked_again(self):
        from backend.database import phrase_question_is_a_repeat
        self.assertTrue(phrase_question_is_a_repeat(
            "Richter besteht auf der Vernehmung aller Zeugen",
            ["Richter besteht auf die Vernehmung aller Zeugen"], self.settled))

    def test_a_trailing_dot_does_not_make_it_a_new_question(self):
        from backend.database import phrase_question_is_a_repeat
        self.assertTrue(phrase_question_is_a_repeat(
            "Richter besteht auf der Vernehmung aller Zeugen.",
            ["Richter besteht auf die Vernehmung aller Zeugen."], self.settled))

    def test_a_variant_he_never_saw_still_reaches_him(self):
        """Ночь нашла ДРУГУЮ ошибку — вопрос обязан дойти. Иначе защита от круга
        превратится в глушилку, и владелец перестанет узнавать о настоящих дефектах."""
        from backend.database import phrase_question_is_a_repeat
        self.assertFalse(phrase_question_is_a_repeat(
            "Richter besteht auf der Vernehmung aller Zeugen",
            ["Der Richter besteht auf der Vernehmung aller Zeugen"], self.settled))

    def test_a_phrase_he_never_decided_is_not_a_repeat(self):
        from backend.database import phrase_question_is_a_repeat
        self.assertFalse(phrase_question_is_a_repeat("Der Bus nimmt 100 Personen mit", [], set()))

    def test_the_queue_writer_asks_the_rule(self):
        block = _src("backend/database.py")
        i = block.index("def queue_phrase_for_review(")
        self.assertIn("phrase_question_is_a_repeat", block[i:i + 4000],
                      "постановка вопроса больше не спрашивает правило о повторе")


class TwoQuestionsInOneQueueTests(unittest.TestCase):
    """Грамматика фразы и качество карточки — разные вопросы и разные кнопки."""

    def _payload(self, reviews):
        from backend.backend_server import _phrase_review_payload
        with patch("backend.database.list_open_phrase_reviews", return_value=reviews):
            return _phrase_review_payload()

    def test_panel_card_is_marked_and_carries_its_examples(self):
        item = self._payload([PANEL_REVIEW])["items"][0]
        self.assertEqual(item["kind"], "panel")
        self.assertEqual(len(item["examples"]), 2)
        self.assertEqual(item["examples"][0]["de"][:10], "Am Anfang ")

    def test_examples_are_shown_german_first_even_when_the_card_is_inverted(self):
        """У части карточек в `source` лежит русский. Немецкий пример, подписанный
        как русский, — это враньё на экране, а не мелочь оформления."""
        item = self._payload([PANEL_REVIEW])["items"][0]
        second = item["examples"][1]
        self.assertTrue(second["de"].startswith("Es ist wichtig"))
        self.assertTrue(second["ru"].startswith("Важно"))

    def test_grammar_question_stays_grammar(self):
        item = self._payload([GRAMMAR_REVIEW])["items"][0]
        self.assertEqual(item["kind"], "grammar")
        self.assertEqual(item["examples"], [])

    def test_history_of_owner_decisions_reaches_the_screen(self):
        item = self._payload([GRAMMAR_REVIEW])["items"][0]
        self.assertEqual(len(item["history"]), 1)
        self.assertEqual(item["history"][0]["decided_text"],
                         "Richter besteht auf der Vernehmung aller Zeugen")

    def test_panel_cards_are_never_sent_to_the_grammar_judge(self):
        """Кнопка «Судили без перевода» отправила бы 79 карточек про ПРИМЕРЫ
        грамматическому судье: у них нет ключа `corrected_ru`, и признак слепоты
        срабатывал на них ровно так же. Замер 26.08.2026: 79 и 79, то есть кнопка
        целиком состояла из чужих вопросов."""
        block = _src("backend/database.py")
        i = block.index("def list_open_phrase_reviews_judged_blind(")
        self.assertIn("phrase_review_is_panel", block[i:i + 2000],
                      "панельные карточки снова попадают под «пересудить со смыслом»")


class ArbiterAnswersBeforeTheScreenTests(unittest.TestCase):
    """Третий судья зовётся ночью, и его текст проходит ту же проверку."""

    def test_his_rejected_text_gets_no_button(self):
        from backend.database import phrase_review_variants
        arbiter = {"winner": 0, "why": "Оба мимо.",
                   "better": "Der Bus fährt 100 Personen mit.",
                   "better_ru": "Автобус везёт 100 человек.",
                   "better_check": {"checked": True, "grammar_ok": False,
                                    "meaning_kept": True, "why": "Так не говорят."}}
        variants = phrase_review_variants([], "Der Bus fährt 100 Personen", arbiter)
        self.assertEqual(variants, [],
                         "забракованный текст третьего судьи снова стал кнопкой")

    def test_his_checked_text_becomes_a_button(self):
        from backend.database import phrase_review_variants
        arbiter = {"winner": 0, "why": "Нужен mitnehmen.",
                   "better": "Der Bus nimmt 100 Personen mit.",
                   "better_ru": "Автобус берёт с собой 100 человек.",
                   "better_check": {"checked": True, "grammar_ok": True, "meaning_kept": True}}
        variants = phrase_review_variants([], "Der Bus fährt 100 Personen mit.", arbiter)
        self.assertEqual([v["text"] for v in variants], ["Der Bus nimmt 100 Personen mit."])
        self.assertEqual(variants[0]["kind"] if "kind" in variants[0] else variants[0]["field"],
                         "arbiter")

    def test_the_night_calls_him_and_closes_what_is_not_a_question(self):
        block = _src("backend/phrase_night_check.py")
        i = block.index("def run_phrase_night_check(")
        tail = block[i:]
        self.assertIn("settle_open_disputes()", tail,
                      "третий судья снова зовётся только кнопкой")
        self.assertIn("close_all_ok_phrase_reviews", tail,
                      "«оба судьи: ошибки нет» снова требует тапа владельца")

    def test_he_is_not_called_where_there_is_nothing_to_settle(self):
        from backend.phrase_night_check import _judge_proposals
        self.assertEqual(_judge_proposals(PANEL_REVIEW["judges"]), [])
        self.assertEqual(_judge_proposals(GRAMMAR_REVIEW["judges"]),
                         ["Richter besteht auf die Vernehmung aller Zeugen"])


class ExplanationIsInRussianTests(unittest.TestCase):
    """29 вопросов из 232 приехали к владельцу с немецким объяснением."""

    def test_the_guard_recognises_german(self):
        from backend.phrase_night_check import _in_russian
        self.assertFalse(_in_russian("Das Verb 'fahren' wird hier transitiv verwendet."))
        self.assertTrue(_in_russian("Глагол «fahren» здесь переходный."))

    def test_the_judge_is_asked_again_when_he_answers_in_german(self):
        from backend import phrase_night_check as nc
        german = {"verdict": "error", "why": "Das Verb ist falsch.", "corrected": "x"}
        russian = {"verdict": "error", "why": "Глагол не тот.", "corrected": "x"}
        with patch("backend.openai_manager.run_phrase_grammar_verdict",
                   return_value=russian) as again:
            out = nc._russian_why_or_retry("t", "sentence", "перевод", german)
        self.assertEqual(out["why"], "Глагол не тот.")
        self.assertEqual(again.call_count, 1)

    def test_a_russian_answer_costs_nothing_extra(self):
        from backend import phrase_night_check as nc
        russian = {"verdict": "error", "why": "Глагол не тот."}
        with patch("backend.openai_manager.run_phrase_grammar_verdict") as again:
            nc._russian_why_or_retry("t", "sentence", "перевод", russian)
        self.assertEqual(again.call_count, 0, "переспрашиваем там, где правило не нарушено")


class ScreenExplainsItselfTests(unittest.TestCase):
    """Владелец: «мне приходит перечёркнутый текст — и что это значит?»"""

    def test_nothing_is_struck_through_any_more(self):
        css = _src("frontend/src/answer/answer.css")
        i = css.index("Спорные фразы словаря")
        self.assertNotIn("line-through", css[i:],
                         "перечёркнутый текст вернулся: черта не объясняет ничего")

    def test_rejected_variants_are_named_in_words_and_explain_what_is_wrong(self):
        src = _src("frontend/src/answer/PhraseReviewScreen.jsx")
        self.assertIn("мы проверили и не советуем", src.lower(),
                      "отклонённые варианты снова показываются без объяснения")
        self.assertIn("Что не так:", src, "не написано, ЧЕМ именно плох вариант")

    def test_buttons_are_named_by_what_they_do(self):
        src = _src("frontend/src/answer/PhraseReviewScreen.jsx")
        self.assertIn("Сохранить этот вариант", src)
        self.assertIn("Сохранить свой вариант", src)
        self.assertIn("Переписать примеры и перевод заново", src)
        self.assertNotIn("✅ Принять {v.index + 1}", src,
                         "кнопка снова называется номером, а не действием")

    def test_the_kind_of_question_is_stated_on_screen(self):
        src = _src("frontend/src/answer/PhraseReviewScreen.jsx")
        self.assertIn("Спор о карточке, а не о фразе", src)
        self.assertIn("Судьи разошлись о грамматике", src)


class PanelCardsHaveTheirOwnDoorTests(unittest.TestCase):
    """79 карточек не приходили владельцу НИКАК — он наткнулся на них случайно."""

    def test_the_bot_writes_about_them_on_tuesday_and_friday(self):
        bot = _src("bot_3.py")
        self.assertIn("_send_panel_cards_reminder", bot)
        i = bot.index("_send_panel_cards_reminder,\n            \"cron\",")
        self.assertIn('"tue,fri"', bot[i:i + 400], "дни, выбранные владельцем, изменились")

    def test_the_message_is_silent_when_there_is_nothing_to_decide(self):
        bot = _src("bot_3.py")
        i = bot.index("def _send_panel_cards_reminder(")
        self.assertIn("if waiting <= 0:", bot[i:i + 2500],
                      "сообщение придёт с «ждут решения: 0»")

    def test_the_button_opens_the_card_queue_only(self):
        bot = _src("bot_3.py")
        i = bot.index("def _send_panel_cards_reminder(")
        self.assertIn('get_webapp_deeplink("ans_frvp_0")', bot[i:i + 2500])
        overlay = _src("frontend/src/answer/AnswerOverlay.jsx")
        self.assertIn("kind === 'frvp'", overlay)
        # С 27.08.2026 дверь открывает ОБА вопроса о карточке: спор о примерах и
        # неподтверждённый перевод. Владельцу они приходят одним сообщением.
        self.assertIn('only="cards"', overlay)

    def test_the_screen_shows_only_its_own_kind_behind_that_door(self):
        src = _src("frontend/src/answer/PhraseReviewScreen.jsx")
        self.assertIn("only === 'cards'", src,
                      "отдельная дверь снова показывает всю очередь вперемешку")
        self.assertIn("!== 'grammar'", src, "грамматика подмешалась в очередь карточек")


if __name__ == "__main__":
    unittest.main()
