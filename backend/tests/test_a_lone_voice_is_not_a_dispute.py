# -*- coding: utf-8 -*-
"""ОДИН ГОЛОС — ЭТО НЕ СПОР. И панель не работает на двух голосах молча.

ПОВОД, 04.09.2026. Владелец о карточке «die Zahl versechsfacht sich»: «я не вижу, что
слово versechsfacht написано неверно, по-моему оно написано верно». Он прав: слово
написано верно. Претензию «глагол написан с ошибкой» высказал ОДИН голос, и никто её
не подтвердил — потому что спросить было больше некого.

ЦЕПОЧКА, ПРОВЕРЕННАЯ НА ЖИВОЙ БАЗЕ И ЖИВЫМ ЗАПРОСОМ:
  1. У Gemini кончились деньги: `429 Your prepayment credits are depleted`.
  2. `unavailable_reason` смотрела только на НАЛИЧИЕ КЛЮЧА и говорила «работать можно».
  3. Каждую ночь отвечали два голоса OpenAI — обученные одинаково (замер 23.08.2026:
     15% разногласий против 2,5% у трёх голосов).
  4. Один придирался, второй молчал. Правило «молчание большинства — это „чисто“»
     требует ДВУХ молчащих, а из двоих их взяться неоткуда.
  5. Одинокая догадка ехала владельцу под видом «голоса разошлись».

Замер того же дня: ВСЕ 51 вопрос на экране владельца родились в прогоне, где голос не
ответил; 45 из них держались на ОДНОМ голосе.
"""
import pathlib
import unittest
from unittest.mock import patch


def _src(rel: str) -> str:
    return (pathlib.Path(__file__).resolve().parents[2] / rel).read_text(encoding="utf-8")


def _панель_без_сети(self, budget_usd: float = 0.0) -> None:
    self.budget_usd = float(budget_usd)
    self.cost = 0.0


class ОдинокаяПретензияНеДоезжаетДоЧеловекаTests(unittest.TestCase):

    def _вердикт(self, голоса):
        """голоса: список множеств полей; None — голос не ответил."""
        from backend import phrase_panel as pp
        panel = pp.Panel.__new__(pp.Panel)
        panel.budget_usd, panel.cost = 1.0, 0.0
        ответы = iter(голоса)

        def голос(*_a, **_k):
            поля = next(ответы)
            if поля is None:
                raise RuntimeError("голос не ответил")
            return (поля, "почему" if поля else "",
                    [{"field": f, "what": "претензия", "fix": ""} for f in поля])

        with patch.object(pp.Panel, "_openai_vote", голос), \
             patch.object(pp.Panel, "_gemini_vote", lambda self, p: голос()), \
             patch("time.sleep", lambda _s: None):
            verdict, _why, _claims = pp.Panel.judge(panel, {"headword": "x"})
        return verdict

    def _разобрать(self, голоса, приговор):
        """Полный разбор: голоса + проверка их претензий (`разобрать`)."""
        from backend import phrase_panel as pp
        panel = pp.Panel.__new__(pp.Panel)
        panel.budget_usd, panel.cost = 1.0, 0.0
        ответы = iter(голоса)

        def голос(*_a, **_k):
            поля = next(ответы)
            if поля is None:
                raise RuntimeError("голос не ответил")
            return (поля, "почему" if поля else "",
                    [{"field": f, "what": "так не говорят", "fix": "иначе"} for f in поля])

        with patch.object(pp.Panel, "_openai_vote", голос), \
             patch.object(pp.Panel, "_gemini_vote", lambda self, p: голос()), \
             patch.object(pp.Panel, "проверить_претензию", return_value=приговор), \
             patch("time.sleep", lambda _s: None):
            return pp.разобрать(panel, "die Zahl versechsfacht sich", "collocation",
                                {}, "число увеличивается в шесть раз")

    def test_a_lone_claim_the_checker_rejects_never_reaches_a_human(self):
        """Живой случай владельца: слово написано верно, придрался один голос.

        Третий голос молчал (у Gemini кончились деньги), проверяющий претензию
        опроверг — карточка чистая, и вопроса не возникает вовсе."""
        from backend import phrase_panel as pp
        verdict, _why, claims = self._разобрать(
            [{"headword"}, set(), None],
            {"claim": "wrong", "fix": "", "why": "фраза написана верно"})
        self.assertEqual(verdict, pp.CLEAN)
        self.assertEqual(claims, [])

    def test_a_lone_claim_the_checker_confirms_is_a_finding_not_a_dispute(self):
        """Проверяющий встал на сторону претензии — это находка, а не спор: она едет
        АВТОРУ фразы с готовым вариантом, а не владельцу в очередь споров."""
        from backend import phrase_panel as pp
        verdict, _why, claims = self._разобрать(
            [{"headword"}, set(), None],
            {"claim": "right", "fix": "ok", "why": ""})
        self.assertEqual(verdict, pp.HUMANS_OWN)
        self.assertEqual(len(claims), 1)

    def test_a_fix_in_another_tense_is_marked_as_rejected(self):
        """⛔ ЩЕЛЬ, НАЗВАННАЯ ВЛАДЕЛЬЦЕМ 04.09.2026. Прежняя проверка сверяла только
        смысл и пропускала «versechsfachte» (прошедшее) при русском «увеличивается»."""
        _verdict, _why, claims = self._разобрать(
            [{"headword"}, {"headword"}, None],
            {"claim": "right", "fix": "bad", "why": "замена в прошедшем времени"})
        self.assertEqual(claims[0]["fix_check"]["state"], "bad")
        self.assertIn("времени", claims[0]["fix_check"]["why"])

    def test_three_voices_that_truly_disagree_still_reach_the_owner(self):
        """Настоящий спор — когда ответили ТРОЕ и разошлись. Его не трогаем."""
        from backend import phrase_panel as pp
        self.assertEqual(
            self._вердикт([{"headword"}, {"examples"}, set()]), pp.DISPUTED)

    def test_two_voices_naming_the_same_field_are_still_a_defect(self):
        from backend import phrase_panel as pp
        self.assertEqual(
            self._вердикт([{"examples"}, {"examples"}, None]), pp.DEFECT)

    def test_a_lone_claim_among_three_stays_clean(self):
        """Молчание двоих — это вердикт «чисто». Правило не изменилось."""
        from backend import phrase_panel as pp
        self.assertEqual(self._вердикт([{"headword"}, set(), set()]), pp.CLEAN)


class ПанельНеРаботаетНаДвухГолосахМолчаTests(unittest.TestCase):

    def test_the_key_being_present_is_not_the_voice_answering(self):
        """Ключ лежал на месте, а Gemini отвечал 429 «кончились деньги»."""
        from backend import phrase_panel as pp

        class Ответ:
            status_code = 429
        with patch.dict("os.environ", {"GEMINI_API_KEY": "x"}), \
             patch("requests.post", return_value=Ответ()):
            причина = pp.третий_голос_молчит()
        self.assertIn("429", причина)
        self.assertIn("деньги", причина, "владельцу не сказано, что именно делать")

    def test_a_live_voice_lets_the_night_run(self):
        from backend import phrase_panel as pp

        class Ответ:
            status_code = 200
        with patch.dict("os.environ", {"GEMINI_API_KEY": "x"}), \
             patch("requests.post", return_value=Ответ()):
            self.assertEqual(pp.третий_голос_молчит(), "")

    def test_the_night_works_without_the_third_voice_but_says_so(self):
        """⚠ РЕШЕНИЕ ВЛАДЕЛЬЦА 04.09.2026: «если нету у нас Gemini, пусть OpenAI
        отрабатывают». Ночь не встаёт — но молчать об этом нельзя: пока голосов двое,
        до человека доходит только то, что подтвердила проверка претензий."""
        from backend import phrase_panel as pp
        with patch.object(pp, "unavailable_reason", return_value=""), \
             patch.object(pp, "третий_голос_молчит",
                          return_value="третий голос не отвечает: HTTP 429"), \
             patch.object(pp, "ensure_judged_ru_column"), \
             patch.object(pp, "поднять_старые_отметки", return_value=0), \
             patch.object(pp, "count_personal_backlog", return_value=0), \
             patch.object(pp, "count_unchecked", return_value=7), \
             patch.object(pp, "count_stale_translation", return_value=0), \
             patch.object(pp, "count_prose_questions", return_value=0), \
             patch.object(pp, "count_questions_on_the_old_prompt", return_value=0), \
             patch.object(pp, "count_without_translation", return_value=0), \
             patch.object(pp, "unchecked_units", return_value=[]) as отбор:
            отчёт = pp.run_batch(limit=5)
        self.assertNotIn("пропущено", отчёт)
        self.assertIn("429", отчёт["третий голос"])
        отбор.assert_called_once()

    def test_the_owner_reads_about_the_missing_voice_in_the_morning(self):
        bot = _src("bot_3.py")
        i = bot.index("def _phrase_panel_line(")
        тело = bot[i:bot.index("\ndef ", i + 1)]
        self.assertIn('meta.get("третий голос")', тело)

    def test_the_owner_reads_why_in_the_morning(self):
        bot = _src("bot_3.py")
        i = bot.index("def _phrase_panel_line(")
        тело = bot[i:bot.index("\ndef ", i + 1)]
        self.assertIn('meta.get("пропущено")', тело)
        self.assertIn("не работала", тело)


if __name__ == "__main__":
    unittest.main()
