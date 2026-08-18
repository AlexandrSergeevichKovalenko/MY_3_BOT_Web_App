"""Ерунда в поле перевода не должна становиться «ошибкой» человека.

Замер 18.08.2026 по живой базе: из 293 записей «тип ошибки не определён» у настоящих
людей 270 (92%) стояли за ответами вроде «Оашв», «Чттч», «Ich weiß nicht» или текстом
по-русски. Они ложились в список ошибок и возвращались человеку как задания: у одного
человека из 59 накопленных ошибок 58 были такими, и его набор переставал содержать
новые предложения.

Правило (решение владельца 18.08.2026) опирается на ДВА признака, и оба даёт модель:
  • балл 0 — её вердикт «пусто или не по делу»;
  • ни одного названного типа ошибки — разбирать нечего.
Порознь они не годятся: 68 записей с нулём оказались живыми обрывочными попытками, и
типы ошибок модель у них назвала. Такие записи обязаны сохраняться.
"""

import unittest
from unittest.mock import MagicMock

from backend.translation_workflow import _log_translation_mistake_with_cursor


def _cursor_with_sentence_id(sentence_id: int | None = 4242):
    """Курсор-заглушка: на любой SELECT отдаёт id предложения, INSERT молча принимает."""
    cursor = MagicMock()
    cursor.fetchone.return_value = (sentence_id,) if sentence_id is not None else None
    return cursor


def _insert_statements(cursor):
    return [
        call.args[0]
        for call in cursor.execute.call_args_list
        if call.args and "INSERT INTO bt_3_detailed_mistakes" in str(call.args[0])
    ]


class NotATranslationIsNotAMistakeTests(unittest.TestCase):
    def test_garbage_answer_creates_no_mistake_row(self):
        """Балл 0 + ни одного типа ошибки → записи не появляется вовсе."""
        cursor = _cursor_with_sentence_id()
        applied = _log_translation_mistake_with_cursor(
            cursor,
            user_id=77,
            original_text="Если бы сотрудники могли побыть тише хотя бы полчаса…",
            categories=[],
            subcategories=[],
            score=0,
            correct_translation="Wenn die Mitarbeiter…",
        )
        self.assertEqual(applied, [])
        self.assertEqual(_insert_statements(cursor), [])

    def test_garbage_answer_with_unusable_labels_creates_no_mistake_row(self):
        """Модель назвала ярлыки вне таксономии и поставила 0 — тоже не ошибка."""
        cursor = _cursor_with_sentence_id()
        applied = _log_translation_mistake_with_cursor(
            cursor,
            user_id=77,
            original_text="Ну если подумать, чаще всего в выходные…",
            categories=["Gibberish"],
            subcategories=["Random letters"],
            score=0,
            correct_translation="Wenn ich so darüber nachdenke…",
        )
        self.assertEqual(applied, [])
        self.assertEqual(_insert_statements(cursor), [])

    def test_real_attempt_scored_zero_is_still_recorded(self):
        """Живой обрывок с нулём, но с названным типом — ошибка сохраняется.

        Это ровно те 68 записей, которые правило обязано пощадить.
        """
        cursor = _cursor_with_sentence_id()
        applied = _log_translation_mistake_with_cursor(
            cursor,
            user_id=77,
            original_text="Даже несмотря на то, что нам было непросто…",
            categories=["Verbs"],
            subcategories=["Conjugation"],
            score=0,
            correct_translation="Auch wenn es für uns schwierig war…",
        )
        self.assertEqual(applied, [("Verbs", "Conjugation")])
        self.assertEqual(len(_insert_statements(cursor)), 1)

    def test_normal_mistake_with_positive_score_is_recorded(self):
        """Обычная ошибка (балл выше нуля, тип назван) — как и раньше."""
        cursor = _cursor_with_sentence_id()
        applied = _log_translation_mistake_with_cursor(
            cursor,
            user_id=77,
            original_text="Коллега сообщил, что…",
            categories=["Word Order"],
            subcategories=["Verb-Second Rule (V2)"],
            score=68,
            correct_translation="Der Kollege teilte mit…",
        )
        self.assertEqual(applied, [("Word Order", "Verb-Second Rule (V2)")])
        self.assertEqual(len(_insert_statements(cursor)), 1)

    def test_unnamed_type_with_positive_score_is_still_recorded(self):
        """Балл выше нуля, но тип назвать не удалось — запись НУЖНА.

        Это настоящая попытка человека; потерять её нельзя. Она ложится как
        «Other mistake / Unclassified mistake» и попадает в лог как случай «не знаю».
        """
        cursor = _cursor_with_sentence_id()
        applied = _log_translation_mistake_with_cursor(
            cursor,
            user_id=77,
            original_text="Мне кажется, что в современном мире…",
            categories=["Something the taxonomy does not know"],
            subcategories=[],
            score=55,
            correct_translation="Ich finde, dass…",
        )
        self.assertEqual(applied, [("Other mistake", "Unclassified mistake")])
        self.assertEqual(len(_insert_statements(cursor)), 1)


if __name__ == "__main__":
    unittest.main()
