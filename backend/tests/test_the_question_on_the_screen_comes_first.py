# -*- coding: utf-8 -*-
"""ТО, ЧТО ЧЕЛОВЕК ЧИТАЕТ ПРЯМО СЕЙЧАС, ЧИНИТСЯ ПЕРВЫМ. И вопрос к судьям — честный.

ПОВОД, 04.09.2026. Владелец в ТРЕТИЙ раз показал один и тот же оборванный текст на
своём экране: «Такого выражения «durchs Bett bekommen» в немецком языке не
существует.; Выражение 'durchs Bett bekommen' … означает 'получить что-то (обычно
работу[обрыв]». Двумя днями раньше я доложил, что починил это.

ПОЧЕМУ НЕ ПОЧИНИЛОСЬ. Карточки я в очередь поставил, а очередь берёт СВЕЖИЕ первыми —
оборванные же вопросы стоят на старых карточках. Замер ночи 04.09.2026: панель
проверила 50 карточек, из 36 оборванных не тронула НИ ОДНОЙ, их стало 37. Список был,
работы не было — ровно то, что в этом проекте запрещено.

ВТОРАЯ НАХОДКА ТОГО ЖЕ ДНЯ. 29 из 49 живых вопросов владельца были «в немецком нет
такого устойчивого выражения» — про его собственные предложения («Wolle spinnen»,
«Etat festlegen», «auf jeden Fall kommen»). Владелец: «Я сам задал вопрос — просто
предложение, которое меня интересует. Это не устойчивое выражение. Это моё
предложение». Виноват был вопрос к судьям, а не судьи.
"""
import hashlib
import pathlib
import unittest


def _src(rel: str) -> str:
    return (pathlib.Path(__file__).resolve().parents[2] / rel).read_text(encoding="utf-8")


class ПервымЧинимТоЧтоЧитаютTests(unittest.TestCase):

    def test_a_card_with_an_open_question_is_taken_first(self):
        from backend import phrase_panel as pp
        src = _src("backend/phrase_panel.py")
        i = src.index("def unchecked_units(")
        тело = src[i:src.index("\ndef ", i + 1)]
        self.assertIn("ЕСТЬ_ВОПРОС_НА_ЭКРАНЕ", тело,
                      "порядок снова только по свежести — вопрос на экране ждёт месяц")
        self.assertIn("(NOT ", тело)
        self.assertIn("status = 'open'", pp.ЕСТЬ_ВОПРОС_НА_ЭКРАНЕ)
        self.assertIn("= 'panel'", pp.ЕСТЬ_ВОПРОС_НА_ЭКРАНЕ)

    def test_the_owner_sees_this_pile_shrink(self):
        from backend import phrase_panel as pp
        self.assertTrue(hasattr(pp, "count_questions_on_the_old_prompt"))
        bot = _src("bot_3.py")
        i = bot.index("def _phrase_panel_line(")
        тело = bot[i:bot.index("\ndef ", i + 1)]
        self.assertIn('meta.get("вопросы старой формулировкой")', тело)


class ВопросКМоделиЧестныйTests(unittest.TestCase):
    """⚠ 04.09.2026 вопрос переписан целиком, версия поднята до 3, а сверка мнений
    убрана. Содержание вопроса и его версия стерегутся в `test_one_model_one_look.py`;
    здесь остаётся только то, ради чего заведена версия: по ней пересуживаются
    вопросы, стоящие у человека на экране, и только они."""

    def test_a_stale_verdict_is_re_asked_only_where_a_human_reads_it(self):
        """Пересуживать все 6738 карточек из-за правки формулировки — деньги за то,
        чего никто не читает."""
        from backend.phrase_panel import PROMPT_VERSION, _где_судить
        условие = _где_судить("(SELECT 'живой')")
        self.assertIn(f"COALESCE(c.prompt_v, 1) <> {PROMPT_VERSION}", условие)
        self.assertIn("bt_3_phrase_review", условие)


class СыраяОшибкаНеУходитЧеловекуTests(unittest.TestCase):
    """«голос не ответил: ClientError» — авария связи, а не претензия к немецкому."""

    def test_the_technical_error_is_stripped_from_the_claim(self):
        from backend.phrase_panel import раскрыть_отметку
        претензии = раскрыть_отметку(
            "headword :: Так не говорят.; голос не ответил: ClientError")
        self.assertEqual(len(претензии), 1)
        self.assertNotIn("ClientError", претензии[0]["what"])
        self.assertIn("Так не говорят.", претензии[0]["what"])

    def test_a_claim_that_was_only_a_connection_error_is_no_claim(self):
        from backend.phrase_panel import раскрыть_отметку
        self.assertEqual(раскрыть_отметку("голос не ответил: ClientError"), [])


if __name__ == "__main__":
    unittest.main()
