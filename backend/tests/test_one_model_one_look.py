# -*- coding: utf-8 -*-
"""ОДНА МОДЕЛЬ, ОДИН ВЗГЛЯД. Сверять несколько мнений — не проверка, а шум.

РЕШЕНИЕ ВЛАДЕЛЬЦА 04.09.2026, дословно:
  «мне достаточно чтобы один раз модель посмотрела и всё… убирай вторую, убирай третью
   модель… нам не нужно так усложнять»;
  «если мы ещё поставим одного или двух или десятерых судей, у нас мнения будут разные.
   Это не судьи — это хаотическое распределение случайности».

ПОЧЕМУ ОН ПРАВ, ЧИСЛАМИ. Замер 04.09.2026 на 60 живых карточках:
  прежний вопрос («проверь карточку, назови поле и опиши дефект»), два голоса
      → претензии у 20 карточек из 60, всего 31 штука; настоящей была ОДНА
        («freigebracht werden» вместо «freigegeben werden»);
        остальные тридцать — вкусовщина: «steile These не употребляется» (употребляется),
        «обычно говорят durchtrieben sein» (стиль), «в русском не говорят „варить пиво“»;
  новый вопрос (процитируй кусок; только грамматика, понятность и перевод)
      → претензия осталась у ОДНОЙ карточки из 60 — той самой настоящей,
        и стоило это $0.063 против $0.118.

Дело было в ВОПРОСЕ, а не в числе голосов: «правильно ли это?» — просьба оценить, а
оценка бездонна. Три голоса на плохом вопросе дают втрое больше выдумок, а не втрое
меньше. Здесь заперто то, что нельзя потерять.
"""
import hashlib
import json
import pathlib
import unittest
from unittest.mock import patch


def _src(rel: str) -> str:
    return (pathlib.Path(__file__).resolve().parents[2] / rel).read_text(encoding="utf-8")


class ГолосОдинTests(unittest.TestCase):

    def test_there_is_no_second_or_third_voice_left(self):
        src = _src("backend/phrase_panel.py")
        for убрано in ("MODEL_B", "MODEL_C", "_gemini_vote", "PRICE_GEMINI",
                       "третий_голос_молчит", "проверить_претензию"):
            self.assertNotIn(f"{убрано} =", src, f"{убрано} вернулся в файл")
            self.assertNotIn(f"def {убрано}", src, f"{убрано} вернулся в файл")

    def test_the_gemini_key_is_no_longer_required(self):
        from backend import phrase_panel as pp
        with patch.dict("os.environ", {"OPENAI_API_KEY": "x", "GEMINI_API_KEY": ""}):
            self.assertEqual(pp.unavailable_reason(), "")
        with patch.dict("os.environ", {"OPENAI_API_KEY": ""}):
            self.assertIn("OPENAI_API_KEY", pp.unavailable_reason())

    def test_one_card_costs_one_request(self):
        from backend import phrase_panel as pp
        panel = pp.Panel.__new__(pp.Panel)
        panel.budget_usd, panel.cost = 1.0, 0.0
        звонков = []

        def голос(self, model, payload, заголовок=""):
            звонков.append(model)
            return set(), "", []

        with patch.object(pp.Panel, "_openai_vote", голос):
            verdict, _why, _находки = pp.Panel.judge(panel, {"headword": "x"}, "x")
        self.assertEqual(звонков, [pp.MODEL_A], "карточку снова смотрят несколько моделей")
        self.assertEqual(verdict, pp.CLEAN)

    def test_a_model_that_did_not_answer_is_not_silence(self):
        """Не ответила — это НЕ «чисто»: отметку не ставим, карточка вернётся."""
        from backend import phrase_panel as pp
        panel = pp.Panel.__new__(pp.Panel)
        panel.budget_usd, panel.cost = 1.0, 0.0
        with patch.object(pp.Panel, "_openai_vote", side_effect=RuntimeError("сеть")), \
             patch("time.sleep", lambda _s: None):
            verdict, _why, _n = pp.Panel.judge(panel, {"headword": "x"}, "x")
        self.assertEqual(verdict, pp.NOT_ASKED)


class ВопросЗакрытыйTests(unittest.TestCase):
    """Вопрос — выбор из названных исходов, а не свободная оценка."""

    def test_the_prompt_forbids_taste(self):
        import re
        from backend.phrase_panel import SYSTEM
        одной = re.sub(r"\s+", " ", SYSTEM)
        for кусок in ("not a set expression", "not in dictionaries", "rare",
                      "one would rather say", "THE ENTRY IS HIS OWN SENTENCE"):
            self.assertIn(кусок, одной, "запрет на придирки исчез из вопроса")
        self.assertIn("CHOOSE FROM THIS CLOSED LIST", одной)
        self.assertIn("choose FINE and report nothing", одной,
                      "правило «сомневаешься — молчи» исчезло")

    def test_the_examples_rule_is_a_single_shared_word(self):
        import re
        from backend.phrase_panel import SYSTEM
        одной = re.sub(r"\s+", " ", SYSTEM)
        self.assertIn("shares not a single word with the entry", одной)
        self.assertIn("does not have to be the perfect illustration", одной)

    def test_prompt_version_is_bumped_with_the_prompt(self):
        """⛔ МЕНЯЕШЬ ВОПРОС — ПОДНИМАЙ ВЕРСИЮ И ОТПЕЧАТОК. Это одно действие.

        По версии в отметке пересуживаются вопросы, стоящие у человека на экране.
        Молча изменённый вопрос означает, что старые вердикты выданы за новые."""
        from backend.phrase_panel import PROMPT_VERSION, SYSTEM
        отпечаток = hashlib.sha256(SYSTEM.encode("utf-8")).hexdigest()[:16]
        self.assertEqual((PROMPT_VERSION, отпечаток), (3, "0c83722a1cb219d7"),
                         "текст вопроса изменился — подними PROMPT_VERSION и отпечаток")


class ПалецОбязанПоказыватьНаНастоящийКусокTests(unittest.TestCase):
    """Бесплатный фильтр: цитата обязана найтись в самой фразе.

    Замер 04.09.2026: он снял 13 «находок» из 19 — модель показывала на то, чего в
    записи нет вовсе."""

    def test_a_quote_that_is_not_in_the_entry_is_dropped(self):
        from backend.phrase_panel import _fields
        ответ = json.dumps({"defects": [
            {"field": "headword", "span": "versechsfachte", "fix": "x", "what": "y"}]})
        поля, _why, находки = _fields(ответ, "die Zahl versechsfacht sich")
        self.assertEqual(находки, [])
        self.assertEqual(поля, set())

    def test_a_real_quote_survives(self):
        from backend.phrase_panel import _fields
        ответ = json.dumps({"defects": [
            {"field": "headword", "span": "freigebracht", "fix": "freigegeben",
             "what": "неверная форма причастия"}]})
        _поля, _why, находки = _fields(ответ, "freigebracht werden")
        self.assertEqual(len(находки), 1)
        self.assertEqual(находки[0]["fix"], "freigegeben")

    def test_claims_about_examples_are_not_filtered_by_the_quote(self):
        """У примеров свой текст — цитата из них в заголовке не встретится никогда."""
        from backend.phrase_panel import _fields
        ответ = json.dumps({"defects": [
            {"field": "examples", "span": "Er geht nach Hause", "fix": "",
             "what": "пример ни одним словом не связан с записью"}]})
        _поля, _why, находки = _fields(ответ, "die Zahl versechsfacht sich")
        self.assertEqual(len(находки), 1)


if __name__ == "__main__":
    unittest.main()
