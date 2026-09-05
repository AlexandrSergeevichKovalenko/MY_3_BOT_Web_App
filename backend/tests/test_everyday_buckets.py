# -*- coding: utf-8 -*-
"""Годится ли слово в банк артиклей: ОДИН запрос, разряды, решение за нами.

РЕШЕНИЕ ВЛАДЕЛЬЦА 05.09.2026, дословно: «Три раза спрашивать модель, нужно ли это
слово и полезно ли его учить, — ну это перебор. Зачем столько запросов? Одного запроса
достаточно». И следом про границу: «Я считаю, а и б подходит» (быт и термины), а про
мелкие детали предмета — «it is necessary to learn».

┌─ ПОЧЕМУ ГОЛОСОВАНИЕ БЫЛО НЕ ЛЕКАРСТВОМ, А ЛАТКОЙ. ──────────────────────────────┐
│ Прежний вопрос — «стоит ли обычному человеку учить артикль этого слова» — просил │
│ ВЗВЕСИТЬ ПОЛЬЗУ, а у пользы нет правильного ответа: есть степень. Замер          │
│ 16.08.2026: тот же вопрос про те же слова, заданный дважды, дал 12% разных        │
│ ответов. Это лечили тремя голосами — то есть платили втрое, чтобы усреднить шум. │
│                                                                                 │
│ Новый вопрос НЕ просит приговора: он просит разложить слова по названным         │
│ разрядам, а какие разряды проходят — решаем мы. Замер 05.09.2026 на 120 словах   │
│ банка, два прогона подряд: разошёлся ОДИН ответ из 120 (0,8%) против прежних 12%.│
└─────────────────────────────────────────────────────────────────────────────────┘

СВЕРКА С РУЧНЫМИ РЕШЕНИЯМИ ВЛАДЕЛЬЦА (05.09.2026). Из 36 слов, помеченных им
«вещь, которой обычный человек не видел», новый вопрос отсеивает 30. Пропускает шесть,
и пять из них объясняются его же новой границей: mulch и splitt — «термин», tape и
jersey — обиходные. Первая версия вопроса отсеивала лишь 24: определение «быт» в ней
потеряло половину — «и СТАЛКИВАЕТСЯ с ней», — и модель поняла «быт» как «нормальное
немецкое слово», пропустив Alm (альпийский луг), Laute (лютню) и Münster (собор).

⚠ СПИСОК «владелец: мусор» (57 слов) ЭТАЛОНОМ НЕ СЛУЖИТ: в нём лежат Großmutter
(бабушка), Erwachsene (взрослый), Jugendliche (подросток) — слова, которым в тренажёре
самое место. Помечались они пачкой и по разным причинам. Подгонять вопрос под этот
список значило бы подгонять его под шум.
"""
import unittest
from unittest.mock import patch


class ОдинЗапросTests(unittest.TestCase):

    def test_the_model_is_asked_once(self):
        from backend import article_word_gate as gate
        звонков = []

        def раз(words):
            звонков.append(tuple(words))
            return {w: "быт" for w in words}

        with patch.object(gate, "_judge_everyday_once", раз):
            gate.judge_everyday_words(["Steckdose", "Iglu"])
        self.assertEqual(len(звонков), 1, "голосование вернулось — платим втрое зря")
        self.assertEqual(gate.EVERYDAY_VOTES, 1)

    def test_the_votes_argument_is_ignored_not_obeyed(self):
        """Аргумент оставлен ради старых вызовов, но голосования больше нет."""
        from backend import article_word_gate as gate
        звонков = []
        with patch.object(gate, "_judge_everyday_once",
                          lambda words: звонков.append(1) or {w: "быт" for w in words}):
            gate.judge_everyday_words(["Steckdose"], votes=5)
        self.assertEqual(len(звонков), 1)


class ГраницуДержимМыАНеМодельTests(unittest.TestCase):

    def _разряд(self, разряд):
        from backend import article_word_gate as gate
        with patch.object(gate, "_judge_everyday_once", lambda w: {x: разряд for x in w}):
            return gate.judge_everyday_words(["Wort"])["Wort"]

    def test_everyday_and_terms_and_parts_pass(self):
        """Решение владельца 05.09.2026: быт, термины и части предметов — учим."""
        for разряд in ("быт", "термин", "деталь"):
            self.assertTrue(self._разряд(разряд), разряд)

    def test_exotic_and_unknown_are_refused(self):
        for разряд in ("экзотика", "незнакомое"):
            self.assertFalse(self._разряд(разряд), разряд)

    def test_an_unknown_bucket_is_not_a_yes(self):
        """Модель ответила своим словом — это НЕ «да». Неизвестное не пускаем."""
        self.assertFalse(self._разряд("что-то своё"))
        self.assertFalse(self._разряд(""))


class ВопросДержитГраницуTests(unittest.TestCase):

    def test_the_everyday_bucket_demands_both_halves(self):
        """⛔ «Знать слово по-русски» — МАЛО. Без второй половины определения модель
        пропустила альпийский луг, лютню и собор (замер 05.09.2026)."""
        import inspect
        from backend import article_word_gate as gate
        текст = inspect.getsource(gate._judge_everyday_once)
        self.assertIn("СТАЛКИВАЕТСЯ В СВОЕЙ ЖИЗНИ", текст)
        self.assertIn("МАЛО", текст)

    def test_the_question_does_not_ask_for_a_verdict(self):
        import inspect
        from backend import article_word_gate as gate
        текст = inspect.getsource(gate._judge_everyday_once)
        self.assertIn("Не оценивай, стоит ли это учить", текст)


class МолчаниеНеОтказTests(unittest.TestCase):

    def test_a_failed_request_is_not_a_no(self):
        """Один обрыв сети не должен хоронить пачку нормальных слов."""
        from backend import article_word_gate as gate
        with patch.object(gate, "_judge_everyday_once",
                          side_effect=gate.EverydayJudgeUnavailable("сеть")):
            with self.assertRaises(gate.EverydayJudgeUnavailable):
                gate.judge_everyday_words(["Steckdose"])


class РасходВидноTests(unittest.TestCase):

    def test_the_spend_goes_into_the_ledger(self):
        """Этот судья ходил в OpenAI мимо учёта: в отчёте по деньгам его не было."""
        import inspect
        from backend import article_word_gate as gate
        текст = inspect.getsource(gate._judge_everyday_once)
        self.assertIn("log_openai_raw_usage", текст)
        self.assertIn("article_everyday_bucket", текст)


if __name__ == "__main__":
    unittest.main()
