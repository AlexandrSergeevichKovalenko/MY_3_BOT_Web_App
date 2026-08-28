# -*- coding: utf-8 -*-
"""Денежные сторожа не врут: пометка после доставки, стоп читается из базы.

ПОВОД — две находки аудита костылей 16.08.2026, обе про деньги и обе одного рода:
программа считала «не знаю» за «всё хорошо».

  · 03. Порог затрат помечался уведомлённым ВНУТРИ расчёта, а письмо владельцу слал
    другой код позже. Telegram моргнул или список админов не прочитался — пометка уже
    стоит, следующий тик считает порог пройденным. Дедупликация недельная: владелец за
    ВСЮ НЕДЕЛЮ не получит ни одного предупреждения. Потолок в проде включён
    (APP_SPEND_CEILING_ENFORCE=1), так что первое заметное — тяжёлые функции выключились
    сами через 2 часа. Недели W30–W32 упирались в 80%: механизм рабочий, не запасной.

  · 57. `is_tier_blocked` возвращал False на любой сбой Redis, подписано «fail-open:
    never break UX». Владелец нажал «Остановить сейчас», Redis моргнул — деньги
    тратятся дальше. Стоп — предохранитель; «не смог прочитать предохранитель» не
    равно «предохранителя нет». Правда про стоп всегда лежит в базе (blocked_tiers),
    Redis лишь быстрый кеш.
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

from backend import spend_ceiling as sc  # noqa: E402


class РасчётБольшеНеПомечаетУведомлённым(unittest.TestCase):
    """Пометку ставит доставивший. Расчёт только считает, кому ещё не сказали."""

    def _посчитать(self, pct, notified):
        строка = {"period_week": "2026-W35", "effective_limit_eur": 10.0,
                  "notified_thresholds": notified, "blocked_tiers": [],
                  "hard_reached_at": None, "auto_stop_at": None}
        with mock.patch.object(sc, "get_or_create_app_spend_ceiling", return_value=строка), \
             mock.patch.object(sc, "get_week_spend_breakdown", return_value=(pct / 10.0, 0.0)), \
             mock.patch.object(sc, "set_app_spend_ceiling_hard_state"), \
             mock.patch.object(sc, "mark_app_spend_ceiling_threshold_notified") as пометка:
            решение = sc.evaluate_ceiling()
        return решение, пометка

    def test_расчёт_ничего_не_помечает(self):
        """Главное: расчёт не имеет права трогать пометку — он не знает про доставку."""
        _решение, пометка = self._посчитать(85.0, {})
        пометка.assert_not_called()

    def test_расчёт_называет_что_ещё_не_доставлено(self):
        решение, _ = self._посчитать(85.0, {})
        self.assertIn(80, решение.get("pending_thresholds") or [])

    def test_уже_доставленное_второй_раз_не_предлагается(self):
        решение, _ = self._посчитать(85.0, {"80": "2026-08-27"})
        self.assertNotIn(80, решение.get("pending_thresholds") or [])

    def test_жёсткий_порог_повторяется_пока_не_доставлен(self):
        """Раньше повтор глушила отметка hard_reached_at — та ставится для автостопа
        и о доставке письма не знает ничего."""
        строка = {"period_week": "2026-W35", "effective_limit_eur": 10.0,
                  "notified_thresholds": {}, "blocked_tiers": [],
                  "hard_reached_at": "2026-08-27T10:00:00+00:00",
                  "auto_stop_at": "2026-08-27T12:00:00+00:00"}
        with mock.patch.object(sc, "get_or_create_app_spend_ceiling", return_value=строка), \
             mock.patch.object(sc, "get_week_spend_breakdown", return_value=(10.5, 0.0)), \
             mock.patch.object(sc, "set_app_spend_ceiling_hard_state"), \
             mock.patch.object(sc, "mark_app_spend_ceiling_threshold_notified"):
            решение = sc.evaluate_ceiling()
        self.assertTrue(решение.get("hard_newly"),
                        "письмо не ушло — значит на следующем тике оно обязано предложиться снова")

    def test_подтверждение_помечает_то_что_доставлено(self):
        with mock.patch.object(sc, "mark_app_spend_ceiling_threshold_notified") as пометка:
            помечено = sc.confirm_thresholds_notified([80, 95], week="2026-W35")
        self.assertEqual(помечено, [80, 95])
        self.assertEqual(пометка.call_count, 2)

    def test_незаписанная_пометка_не_выдаётся_за_записанную(self):
        """Цена незаписи — повторное письмо; цена ложной пометки — неделя молчания."""
        with mock.patch.object(sc, "mark_app_spend_ceiling_threshold_notified",
                               side_effect=RuntimeError("база молчит")):
            self.assertEqual(sc.confirm_thresholds_notified([80], week="2026-W35"), [])


class СтопЧитаетсяИзБазыКогдаRedisМолчит(unittest.TestCase):

    def setUp(self):
        sc._TIER_BLOCK_LAST_KNOWN.clear()

    def test_redis_ответил_верим_ему(self):
        клиент = mock.Mock()
        клиент.get.return_value = "1"
        with mock.patch.object(sc, "_redis", return_value=клиент):
            self.assertTrue(sc.is_tier_blocked("heavy"))

    def test_redis_молчит_но_база_знает_про_стоп(self):
        """Ровно тот случай, который тратил деньги: владелец нажал «Остановить»."""
        with mock.patch.object(sc, "_redis", return_value=None), \
             mock.patch.object(sc, "_tier_blocked_from_db", return_value=True):
            self.assertTrue(sc.is_tier_blocked("heavy"))

    def test_redis_упал_с_ошибкой_база_знает_про_стоп(self):
        клиент = mock.Mock()
        клиент.get.side_effect = RuntimeError("redis лёг")
        with mock.patch.object(sc, "_redis", return_value=клиент), \
             mock.patch.object(sc, "_tier_blocked_from_db", return_value=True):
            self.assertTrue(sc.is_tier_blocked("heavy"))

    def test_база_говорит_что_стопа_нет_значит_нет(self):
        with mock.patch.object(sc, "_redis", return_value=None), \
             mock.patch.object(sc, "_tier_blocked_from_db", return_value=False):
            self.assertFalse(sc.is_tier_blocked("heavy"))

    def test_молчат_оба_берём_последнее_известное(self):
        клиент = mock.Mock()
        клиент.get.return_value = None
        with mock.patch.object(sc, "_redis", return_value=клиент):
            sc.is_tier_blocked("heavy")                       # запомнили: не заблокирован
        sc._TIER_BLOCK_LAST_KNOWN["heavy"] = (False, 0.0)     # состарили запись
        with mock.patch.object(sc, "_redis", return_value=None), \
             mock.patch.object(sc, "_tier_blocked_from_db", return_value=None):
            self.assertFalse(sc.is_tier_blocked("heavy"))

    def test_не_знаем_вообще_ничего_считаем_остановленным(self):
        """Решение владельца 27.08.2026: молчат И Redis, И база — это состояние, когда
        приложение и так почти ничего не может. Дешевле остановить тяжёлое, чем тратить
        деньги вслепую. Дешёвое ядро (перевод, словарь) этим гейтом не закрывается."""
        with mock.patch.object(sc, "_redis", return_value=None), \
             mock.patch.object(sc, "_tier_blocked_from_db", return_value=None):
            self.assertTrue(sc.is_tier_blocked("heavy"))


if __name__ == "__main__":
    unittest.main()
