# -*- coding: utf-8 -*-
"""Приглашение на батл: кому уходит, кому нет и что видит создатель.

Повод — батл #13, 14.09.2026: владелец увидел «📨 20 · 🚫 Не дошло: 10» и не смог понять,
ошибка это или отказ людей. Разбор: 3 тестовые строки списка доступа + 7 закрывших бота,
а кнопку «🛡 Готов к батлам» рассылка «всем» не смотрела вообще.
Стратегия — docs/tasks/battle_invite_targets_strategy.md
"""
import asyncio
import os
import unittest
from unittest import mock
from unittest.mock import patch

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend import database as db  # noqa: E402
import bot_3  # noqa: E402

СОЗДАТЕЛЬ = 117649764
ГОТОВЫЙ_1 = 423718443
ГОТОВЫЙ_2 = 514237932
ЗАКРЫЛ_БОТА = 362151600
ВЫКЛЮЧИЛ_КНОПКУ = 886154130


def _conn(курсор):
    ctx = mock.MagicMock()
    ctx.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = курсор
    return ctx


def _строки_из_базы():
    """Что вернёт JOIN: (user_id, заблокировал_бота, готов_к_батлам)."""
    return [
        (СОЗДАТЕЛЬ, False, True),
        (ГОТОВЫЙ_1, False, True),
        (ГОТОВЫЙ_2, None, None),          # строки в реестре кнопки нет = ГОТОВ по умолчанию
        (ЗАКРЫЛ_БОТА, True, True),
        (ВЫКЛЮЧИЛ_КНОПКУ, False, False),
    ]


class СоставРассылки(unittest.TestCase):
    """list_battle_invite_targets отвечает «кому можно» и «почему остальным нельзя»."""

    def _targets(self, *, exclude=None, всего_строк=8):
        курсор = mock.MagicMock()
        # первый execute — сырой COUNT(*), второй — сам JOIN
        курсор.fetchone.return_value = (всего_строк,)
        # COALESCE в SQL превращает NULL в TRUE/FALSE; в тесте эмулируем это здесь,
        # потому что мокается курсор, а не Postgres.
        курсор.fetchall.return_value = [
            (uid, bool(b), True if r is None else bool(r)) for uid, b, r in _строки_из_базы()
        ]
        with patch.object(db, "ensure_article_battle_available_schema", lambda: None), \
             patch.object(db, "ensure_bot_blocked_table", lambda: None), \
             patch.object(db, "get_db_connection_context", _conn(курсор)):
            return db.list_battle_invite_targets(exclude_user_id=exclude)

    def test_кто_не_выключал_кнопку_считается_готовым(self):
        """Решение владельца 14.09.2026: по умолчанию человек ГОТОВ к батлам."""
        info = self._targets(exclude=СОЗДАТЕЛЬ)
        self.assertIn(ГОТОВЫЙ_2, info["targets"], "нет строки в реестре ≠ отказ")

    def test_закрывшему_бота_и_выключившему_кнопку_не_отправляем(self):
        info = self._targets(exclude=СОЗДАТЕЛЬ)
        self.assertEqual(info["targets"], sorted([ГОТОВЫЙ_1, ГОТОВЫЙ_2]))
        self.assertEqual(info["blocked"], 1)
        self.assertEqual(info["opted_out"], 1)
        self.assertEqual(info["blocked_ids"], [ЗАКРЫЛ_БОТА])
        self.assertEqual(info["opted_out_ids"], [ВЫКЛЮЧИЛ_КНОПКУ])

    def test_создатель_себе_вызов_не_получает(self):
        self.assertNotIn(СОЗДАТЕЛЬ, self._targets(exclude=СОЗДАТЕЛЬ)["targets"])

    def test_строки_не_людей_считаются_отдельно(self):
        """Разница сырого счёта и правила «настоящий человек» — это наряд на работу,
        а не «не дошло»: 14.09.2026 таких было 3."""
        self.assertEqual(self._targets(всего_строк=8)["not_real"], 3)

    def test_выбор_конкретных_людей_проходит_ту_же_проверку(self):
        """Пикер показывает готовых, но между показом и отправкой человек мог закрыть бота."""
        info = self._targets(exclude=СОЗДАТЕЛЬ)
        with patch.object(bot_3, "list_battle_invite_targets", lambda **kw: info):
            targets, закрыли, не_готовы = asyncio.run(bot_3._battle_invite_targets(
                creator_id=СОЗДАТЕЛЬ,
                ind_targets=[ГОТОВЫЙ_1, ЗАКРЫЛ_БОТА, ВЫКЛЮЧИЛ_КНОПКУ, СОЗДАТЕЛЬ]))
        self.assertEqual(targets, [ГОТОВЫЙ_1])
        self.assertEqual((закрыли, не_готовы), (1, 1))


class ПодписьСоздателю(unittest.TestCase):
    """Причина видна ВСЕГДА. Прежняя подпись писала пояснение только когда все неудачи
    одного типа — и тестовая строка в списке доступа ломала это условие каждый раз."""

    def test_каждая_причина_своей_строкой(self):
        текст = bot_3._battle_delivery_caption_line(
            20, 0, 0, skipped_blocked=7, skipped_optout=2)
        self.assertIn("Вызов получили: 20", текст)
        self.assertIn("Закрыли бота: 7", текст)
        self.assertIn("Не готовы к батлам: 2", текст)
        self.assertNotIn("Не дошло", текст)
        self.assertNotIn("Не отправилось", текст)

    def test_отказ_телеграма_и_осознанный_пропуск_складываются(self):
        текст = bot_3._battle_delivery_caption_line(5, 2, 0, skipped_blocked=3)
        self.assertIn("Закрыли бота: 5", текст)

    def test_настоящая_ошибка_называется_ошибкой(self):
        текст = bot_3._battle_delivery_caption_line(5, 0, 1)
        self.assertIn("Не отправилось: 1", текст)

    def test_когда_всё_дошло_лишних_строк_нет(self):
        текст = bot_3._battle_delivery_caption_line(20, 0, 0)
        self.assertEqual(текст.strip(), "📨 Вызов получили: 20")

    def test_несостоявшаяся_рассылка_не_выдаётся_за_ноль_адресатов(self):
        """`except: targets = []` писал «получили 0» — неудача базы была неотличима
        от «никого не оказалось»."""
        self.assertIn("Не удалось разослать", bot_3._battle_invites_failed_line())


class КнопкаГотовностиПоУмолчанию(unittest.TestCase):

    def _состояние(self, row):
        курсор = mock.MagicMock()
        курсор.fetchone.return_value = row
        with patch.object(db, "ensure_article_battle_available_schema", lambda: None), \
             patch.object(db, "get_db_connection_context", _conn(курсор)):
            return db.is_article_battle_available(ГОТОВЫЙ_1)

    def test_нет_строки_значит_готов(self):
        self.assertTrue(self._состояние(None))

    def test_выключено_только_то_что_человек_выключил_сам(self):
        self.assertFalse(self._состояние((False,)))
        self.assertTrue(self._состояние((True,)))

    def test_ярлык_показывает_оба_состояния(self):
        with patch.object(bot_3, "is_article_battle_available", lambda uid: True):
            self.assertTrue(bot_3._battle_available_button_text(ГОТОВЫЙ_1).endswith("✅"))
        with patch.object(bot_3, "is_article_battle_available", lambda uid: False):
            self.assertTrue(bot_3._battle_available_button_text(ГОТОВЫЙ_1).endswith("⚪"))

    def test_состояние_не_прочиталось_значит_метки_нет(self):
        """Подставить ⚪ вместо неизвестного — значит сказать человеку, что его
        выключили за него."""
        def падает(uid):
            raise RuntimeError("база молчит")
        with patch.object(bot_3, "is_article_battle_available", падает):
            ярлык = bot_3._battle_available_button_text(ГОТОВЫЙ_1)
        self.assertEqual(ярлык, bot_3.ARTIKEL_BATTLE_AVAILABLE_BUTTON_TEXT)


class ОбещанияЗарегистрированы(unittest.TestCase):

    def test_оба_обещания_в_реестре(self):
        from backend import fix_promises
        ключи = {p.key for p in fix_promises.PROMISES}
        self.assertIn("allowed_rows_are_real_people", ключи)
        self.assertIn("battle_invites_respect_the_button", ключи)


if __name__ == "__main__":
    unittest.main()
