"""Второе мнение на приёмке артиклей спрашивается несколько раз, решает большинство.

Замер 16.08.2026. Один и тот же вопрос про одни и те же слова, заданный дважды подряд
(gpt-4.1, температура 0), дал РАЗНЫЕ ответы по 12% слов. Тот же судья, спрошенный
заново про 859 слов, которые он сам пропустил в игру, 15% из них выбрасывал: Überfahrt,
Inventur, Ebbe, Archäologe, Montage, Pollen. А из 150 слов, лежащих в карантине по его
же отказу, 36% при повторе получали «да»: Mängel, Geruchssinn, Schnellstraße, Anrede.

Частотность вердикт не объясняла: отвергнутые (медиана ранга 36452) и живущие в игре
(медиана 31267) лежали в одной полосе. То есть слово попадало в игру или в карантин
во многом по жребию, и в карантине накопилось 454 слова, которые владелец разбирал
руками по 10 в день.

Планка при этом НЕ менялась: в промпте как стояло «сомневаешься — отвечай НЕТ», так и
стоит. Чинилась только устойчивость ответа.
"""

import unittest
from unittest.mock import patch

import backend.article_word_gate as gate


class _Judge:
    """Отдаёт заранее заданные ответы по одному на каждый вызов."""

    def __init__(self, *rounds):
        self.rounds = list(rounds)
        self.calls = 0

    def __call__(self, words):
        verdict = self.rounds[min(self.calls, len(self.rounds) - 1)]
        self.calls += 1
        return {w: bool(verdict.get(w, False)) for w in words}


def _run(judge, words, **kw):
    with patch.object(gate, "_judge_everyday_once", judge):
        return gate.judge_everyday_words(list(words), **kw)


class MajorityTests(unittest.TestCase):
    def test_asks_three_times_by_default(self):
        judge = _Judge({"Steg": True})
        _run(judge, ["Steg"])
        self.assertEqual(judge.calls, gate.EVERYDAY_VOTES)
        self.assertEqual(gate.EVERYDAY_VOTES, 3)

    def test_two_of_three_yes_wins(self):
        """Ровно тот случай, ради которого всё делалось: голос-одиночка отправлял слово
        в карантин, хотя два других за него."""
        judge = _Judge({"Schnellstraße": False}, {"Schnellstraße": True}, {"Schnellstraße": True})
        self.assertEqual(_run(judge, ["Schnellstraße"]), {"Schnellstraße": True})

    def test_two_of_three_no_wins(self):
        judge = _Judge({"Trepang": True}, {"Trepang": False}, {"Trepang": False})
        self.assertEqual(_run(judge, ["Trepang"]), {"Trepang": False})

    def test_unanimous_no_stays_no(self):
        """Слово, по которому судья устойчив, ведёт себя как раньше."""
        judge = _Judge({"Jostabeere": False})
        self.assertEqual(_run(judge, ["Jostabeere"]), {"Jostabeere": False})

    def test_each_word_counted_on_its_own(self):
        judge = _Judge(
            {"Steg": True, "Mispel": True},
            {"Steg": True, "Mispel": False},
            {"Steg": False, "Mispel": False},
        )
        self.assertEqual(_run(judge, ["Steg", "Mispel"]), {"Steg": True, "Mispel": False})


class TieBreakTests(unittest.TestCase):
    """Ничья — это «нет»: та же осторожность, что записана в самом промпте."""

    def test_tie_on_two_votes_is_no(self):
        judge = _Judge({"Barren": True}, {"Barren": False})
        self.assertEqual(_run(judge, ["Barren"], votes=2), {"Barren": False})

    def test_single_vote_still_works(self):
        judge = _Judge({"Steckdose": True})
        self.assertEqual(_run(judge, ["Steckdose"], votes=1), {"Steckdose": True})


class SilenceIsNotRefusalTests(unittest.TestCase):
    """«Модель сказала нет» и «ответа не было» — разные вещи. Один обрыв сети не должен
    хоронить пачку нормальных слов."""

    def test_majority_counted_from_votes_that_arrived(self):
        def judge(words):
            judge.calls += 1
            if judge.calls == 1:
                raise gate.EverydayJudgeUnavailable("таймаут")
            return {w: True for w in words}
        judge.calls = 0
        self.assertEqual(_run(judge, ["Gully"]), {"Gully": True})

    def test_one_vote_lost_does_not_flip_the_verdict(self):
        """Дошло два голоса, оба «за» — слово проходит, а не проваливается из-за
        потерянного третьего."""
        rounds = [gate.EverydayJudgeUnavailable("сеть"), {"Anrede": True}, {"Anrede": True}]

        def judge(words):
            step = rounds[judge.calls]
            judge.calls += 1
            if isinstance(step, Exception):
                raise step
            return {w: bool(step.get(w, False)) for w in words}
        judge.calls = 0
        self.assertEqual(_run(judge, ["Anrede"]), {"Anrede": True})

    def test_no_votes_at_all_raises(self):
        def judge(_words):
            raise gate.EverydayJudgeUnavailable("ключа нет")
        with self.assertRaises(gate.EverydayJudgeUnavailable):
            _run(judge, ["Anrede"])

    def test_empty_input_asks_nobody(self):
        judge = _Judge({})
        self.assertEqual(_run(judge, []), {})
        self.assertEqual(judge.calls, 0)


class PromptIsUnchangedTests(unittest.TestCase):
    """Строгость планки — решение владельца от 31.07, её мы не трогали."""

    def test_doubt_still_means_no(self):
        import inspect
        src = inspect.getsource(gate._judge_everyday_once)
        self.assertIn("Сомневаешься — отвечай НЕТ", src)

    def test_bar_is_set_for_a_russian_speaking_learner(self):
        import inspect
        src = inspect.getsource(gate._judge_everyday_once)
        self.assertIn("русскоязычный", src)


if __name__ == "__main__":
    unittest.main()
