# -*- coding: utf-8 -*-
"""Два отказа — и слово идёт к владельцу с кнопками, а не по бесконечному кругу.

РЕШЕНИЕ ВЛАДЕЛЬЦА 29.08.2026, дословно: «если по какой-то причине оно не может быть
обработано — в чём смысл ещё ждать и его запускать и тратить деньги? Нужен список таких
слов с кнопками, чтобы я принял решение: оставить как есть или удалить».

До этого было: три отказа → пауза на неделю → снова в очередь → снова два платных
запроса. Круг без выхода.

Тест держит четыре обещания:
  1. порог 2, и возврата «через неделю» в выборке больше нет;
  2. умолчание — ОСТАВИТЬ: удаляет только явный тап (правило владельца 25.08.2026,
     молчание не удаляет);
  3. кнопки ровно две, «вернуть в работу» среди них нет;
  4. решённое слово к владельцу больше не приходит.
"""
import unittest
from unittest import mock


class ВыборкаНочи(unittest.TestCase):
    def test_threshold_is_two_and_there_is_no_weekly_return(self):
        from backend import lex_units
        исходник = open(lex_units.__file__, encoding="utf-8").read()
        начало = исходник.index("def units_needing_card")
        тело = исходник[начало:начало + 6000]
        self.assertIn("refusals >= 2", тело, "порог перестал быть двойкой")
        self.assertNotIn("INTERVAL '7 days'", тело,
                         "вернулся возврат через неделю — это и есть бесконечный круг, "
                         "от которого владелец просил уйти")


class ЭкранРешения(unittest.TestCase):
    def _экран(self, отмечено=()):
        import bot_3
        сессия = {"candidates": [{"id": 1, "w": "Sich reißen", "t": "рваться",
                                  "r": "судья забраковал: пример не о том слове", "d": 3},
                                 {"id": 2, "w": "Jemandem vorkommen", "t": "казаться",
                                  "r": "судья забраковал: перевод не о своём предложении",
                                  "d": 1}],
                  "kept_ids": list(отмечено)}
        return bot_3._build_unit_decision_review(сессия, "sid", 0)

    def test_nothing_is_marked_for_deletion_by_default(self):
        текст, разметка = self._экран()
        self.assertIn("Отмечено на удаление: <b>0</b>", текст)
        подписи = [к.text for ряд in разметка.inline_keyboard for к in ряд]
        self.assertTrue(all(not п.startswith("🗑") for п in подписи if п[:1] in "✅🗑"),
                        "слово помечено на удаление само — молчание не должно удалять")

    def test_tapping_marks_for_deletion(self):
        текст, _ = self._экран(отмечено=(1,))
        self.assertIn("Отмечено на удаление: <b>1</b>", текст)

    def test_there_is_no_put_back_to_work_button(self):
        _, разметка = self._экран()
        подписи = " ".join(к.text for ряд in разметка.inline_keyboard for к in ряд)
        self.assertNotIn("вернуть", подписи.lower(),
                         "вернулась кнопка «вернуть в работу» — это тот самый круг")
        self.assertIn("Применить", подписи)
        self.assertIn("Закрыть без изменений", подписи)

    def test_the_screen_says_that_keeping_is_not_a_failure(self):
        """Владелец должен понимать, что «оставить» — нормальный исход, а не брак."""
        текст, _ = self._экран()
        self.assertIn("слово живёт в словаре", текст.lower())


class РешённоеБольшеНеПриходит(unittest.TestCase):
    def test_selection_skips_decided_words(self):
        from backend import database
        with mock.patch.object(database, "get_db_connection_context") as соединение:
            курсор = mock.MagicMock()
            курсор.fetchall.return_value = []
            соединение.return_value.__enter__.return_value.cursor.return_value \
                .__enter__.return_value = курсор
            database.get_units_awaiting_owner_decision(10)
        запросы = " ".join(str(c.args[0]) for c in курсор.execute.call_args_list)
        self.assertIn("decided_at IS NULL", запросы,
                      "решённые слова вернутся в список — его перестанут читать")

    def test_keep_only_marks_the_question_closed(self):
        """«Оставить» не имеет права трогать само слово."""
        from backend import database
        with mock.patch.object(database, "get_db_connection_context") as соединение:
            курсор = mock.MagicMock()
            курсор.rowcount = 2
            соединение.return_value.__enter__.return_value.cursor.return_value \
                .__enter__.return_value = курсор
            итог = database.apply_unit_refusal_decisions(keep_ids=[1, 2], delete_ids=[])
        запросы = " ".join(str(c.args[0]) for c in курсор.execute.call_args_list)
        self.assertIn("decision = 'оставлено'", запросы)
        self.assertNotIn("DELETE FROM bt_3_lex_units", запросы,
                         "«оставить» удалило слово")
        self.assertEqual(2, итог["оставлено"])


if __name__ == "__main__":
    unittest.main()
