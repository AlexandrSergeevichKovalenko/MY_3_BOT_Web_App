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


class ВопросКСудьямЧестныйTests(unittest.TestCase):

    def test_not_being_an_idiom_is_not_a_defect(self):
        import re
        from backend.phrase_panel import SYSTEM
        # Переносы строк в промпте не значат ничего — сравниваем по словам.
        одной_строкой = re.sub(r"\s+", " ", SYSTEM)
        for кусок in ("not a set phrase", "not an idiom", "not a fixed expression"):
            self.assertIn(кусок, одной_строкой,
                          "судьям снова не сказано, что «не устойчивое» — не ошибка")
        self.assertIn("THE ENTRY IS USUALLY THE LEARNER'S OWN SENTENCE", одной_строкой)

    def test_prompt_version_is_bumped_with_the_prompt(self):
        """⛔ МЕНЯЕШЬ ВОПРОС — ПОДНИМАЙ ВЕРСИЮ.

        Вердикт стоит ровно столько, сколько стоит вопрос, по которому он вынесен. По
        версии в отметке пересуживаются вопросы, стоящие у человека на экране. Молча
        изменённый вопрос означает, что старые вердикты выданы за новые.
        Поднял версию — обнови и отпечаток ниже, это одно действие.
        """
        from backend.phrase_panel import PROMPT_VERSION, SYSTEM
        отпечаток = hashlib.sha256(SYSTEM.encode("utf-8")).hexdigest()[:16]
        self.assertEqual(
            (PROMPT_VERSION, отпечаток), (2, "4b399db4604ca93d"),
            "текст вопроса к судьям изменился — подними PROMPT_VERSION и отпечаток")

    def test_a_stale_verdict_is_re_asked_only_where_a_human_reads_it(self):
        """Пересуживать все 6738 карточек из-за правки формулировки — $28 за то, чего
        никто не читает. Пересуживаем только вопросы на экране."""
        from backend.phrase_panel import _где_судить
        условие = _где_судить("(SELECT 'живой')")
        self.assertIn("COALESCE(c.prompt_v, 1) <> 2", условие)
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
