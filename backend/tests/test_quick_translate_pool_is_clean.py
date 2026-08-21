"""Опечатка одного человека не становится общим словарём — и никто её не ждёт.

Быстрый перевод — самый частый вход слова к нам, и пара «слово ↔ перевод» уезжает в
ОБЩИЙ пул, даже если человек ничего не сохранял. Текст сюда НАБИРАЕТ человек, значит
опечатка возможна; а пул общий, значит ошибка одного досталась бы всем следующим.

Две вещи проверяются здесь.

Первая: человек не ждёт. Вычитка едет в фоне — тем же путём, что и добор артикля рядом.
Владелец 20.08.2026: «я нажимаю сохранить и мгновенно листаю к следующему слову, я не
буду ждать, что мне ответит модель через 5 секунд».

Вторая: если вычитка нашла ошибку, в пул НЕ КЛАДЁТСЯ НИЧЕГО. Положить исправленное слово
рядом с переводом ОПЕЧАТКИ значило бы завести пару, где стороны не сходятся, — это хуже
самой опечатки. Человек свой перевод уже получил, а пул подхватит слово в следующий раз,
когда его наберут правильно.
"""

import unittest
from unittest.mock import patch

from backend import backend_server as bs


class QuickTranslatePoolTests(unittest.TestCase):
    def _run(self, *, typed: str, corrected: str):
        """Прогоняем фоновую работу синхронно, чтобы увидеть её решение."""
        stored = []
        jobs = []

        class _Executor:
            @staticmethod
            def submit(fn):
                jobs.append(fn)

        with patch.object(bs, "_QUICK_ARTICLE_EXECUTOR", _Executor), \
             patch.object(bs, "_proofread_dictionary_phrase", return_value=corrected), \
             patch.object(bs, "_store_quick_translate_in_pool",
                          side_effect=lambda **kw: stored.append(kw)):
            bs._schedule_quick_translate_pool_store(
                text=typed, result={"translation": "перевод"},
                source_lang="de", target_lang="ru", user_id_for_billing=1,
            )
            self.assertEqual(len(jobs), 1, "работа обязана уйти в фон, а не выполниться сразу")
            jobs[0]()
        return stored

    def test_clean_text_goes_into_the_shared_pool(self):
        stored = self._run(typed="die Regierung", corrected="")
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0]["text"], "die Regierung")

    def test_nothing_to_fix_reported_as_same_text_also_stores(self):
        """Вычитка часто отвечает тем же текстом — это не повод выбрасывать пару."""
        stored = self._run(typed="die Regierung", corrected="die Regierung")
        self.assertEqual(len(stored), 1)

    def test_typo_is_not_stored_at_all(self):
        """Ни опечатку, ни исправленное слово с чужим переводом — в общий пул не кладём."""
        stored = self._run(typed="die Regirung", corrected="die Regierung")
        self.assertEqual(stored, [])

    def test_a_broken_proofread_never_breaks_the_translation(self):
        """Отказ вычитки — не повод терять пару: человек уже получил свой перевод,
        и пул не должен молча зависеть от чужой доступности."""
        stored = []
        jobs = []

        class _Executor:
            @staticmethod
            def submit(fn):
                jobs.append(fn)

        with patch.object(bs, "_QUICK_ARTICLE_EXECUTOR", _Executor), \
             patch.object(bs, "_proofread_dictionary_phrase", side_effect=RuntimeError), \
             patch.object(bs, "_store_quick_translate_in_pool",
                          side_effect=lambda **kw: stored.append(kw)):
            bs._schedule_quick_translate_pool_store(
                text="die Regierung", result={}, source_lang="de", target_lang="ru")
            jobs[0]()
        self.assertEqual(stored, [], "при отказе вычитки в общий пул не пишем вслепую")


if __name__ == "__main__":
    unittest.main()
