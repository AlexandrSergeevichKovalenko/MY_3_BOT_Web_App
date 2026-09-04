# -*- coding: utf-8 -*-
"""Тариф «Лайт» и четыре состояния доступа.

ПОВОД. Владелец 04.09.2026: постоянного бесплатного тарифа нет. 30 дней после первого
контакта (внутри — 7 дней полного), дальше «Лайт» (наполнение прежнего бесплатного,
160 ⭐ / 30 дней) или «Полный доступ». Стратегия — docs/tasks/light_tier_strategy.md.

ЧТО СТЕРЕЖЁТСЯ:
  1. «Лайт» — платный, но effective_mode у него free: ни одни ворота по effective_mode
     не должны принять его за полный доступ.
  2. access_state: pro / light / free_month / locked / unknown. «Не знаем начало» —
     НЕ locked: замок на догадку не вешаем.
  3. Счёт на Лайт — подписка на 30 дней с purpose=light; при действующем Полном не
     выписывается (Telegram не умеет сменить подписку — вышло бы два списания).
  4. При покупке Полного действующий Лайт отменяется через Bot API; не вышло —
     Полный всё равно выдан, администратору письмо.
  5. Цена Лайта по умолчанию — 160 ⭐ (2 € + комиссия, тот же курс, что у Полного).
"""
import asyncio
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")

import backend.backend_server as server  # noqa: E402
from backend import database as db  # noqa: E402
import bot_3  # noqa: E402

NOW = datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc)
UID = 8546091375

PLANS = {
    "free": {"plan_code": "free", "name": "Free", "is_paid": False, "daily_cost_cap_eur": 0.5},
    "light": {"plan_code": "light", "name": "Лайт", "is_paid": True, "daily_cost_cap_eur": 0.5},
    "pro": {"plan_code": "pro", "name": "Pro", "is_paid": True, "daily_cost_cap_eur": 2.0},
}


def _sub(plan, *, sub_id="stars_abc", period_end=None, status="active"):
    return {
        "user_id": UID, "plan_code": plan, "status": status, "trial_ends_at": None,
        "current_period_end": (period_end or (NOW + timedelta(days=20))).isoformat(),
        "stripe_customer_id": None, "stripe_subscription_id": sub_id,
        "created_at": None, "updated_at": None,
    }


def _period(days_ago):
    started = NOW - timedelta(days=days_ago)
    return {"started_at": started, "ends_at": started + timedelta(days=30), "source": "signup"}


class ПравоДоступа(unittest.TestCase):

    def _resolve(self, subscription, period):
        with patch.object(db, "get_billing_plan", side_effect=lambda code: PLANS.get(code, {})), \
             patch.object(db, "get_active_pro_grant_detail", return_value=(None, None)), \
             patch.object(db, "get_user_subscription", return_value=subscription), \
             patch.object(db, "get_access_period", return_value=period):
            return db.resolve_entitlement(UID, now_ts_utc=NOW, subscription=subscription)

    def test_лайт_платный_но_наполнение_бесплатного(self):
        ent = self._resolve(_sub("light"), _period(40))
        self.assertEqual(ent["effective_mode"], "free")
        self.assertEqual(ent["plan_code"], "light")
        self.assertEqual(ent["plan_name"], "Лайт")
        self.assertEqual(ent["source_of_entitlement"], "paid_light")
        self.assertEqual(ent["access_state"], "light")
        self.assertEqual(ent["cap_eur"], 0.5)

    def test_просроченный_лайт_запирает_если_месяц_прошёл(self):
        ent = self._resolve(_sub("light", period_end=NOW - timedelta(days=5)), _period(40))
        self.assertEqual(ent["source_of_entitlement"], "stars_subscription_lapsed")
        self.assertEqual(ent["access_state"], "locked")

    def test_полный_доступ_остаётся_полным(self):
        ent = self._resolve(_sub("pro"), _period(40))
        self.assertEqual(ent["effective_mode"], "pro")
        self.assertEqual(ent["access_state"], "pro")

    def test_бесплатный_месяц_идёт(self):
        ent = self._resolve(None, _period(5))
        self.assertEqual(ent["effective_mode"], "free")
        self.assertEqual(ent["access_state"], "free_month")
        self.assertEqual(ent["free_month_days_left"], 25)
        self.assertTrue(ent["free_month_ends_at"])

    def test_месяц_кончился_без_подписки_заперт(self):
        ent = self._resolve(None, _period(31))
        self.assertEqual(ent["access_state"], "locked")
        self.assertEqual(ent["free_month_days_left"], 0)

    def test_начала_отсчёта_нет_это_не_замок(self):
        ent = self._resolve(None, None)
        self.assertEqual(ent["access_state"], "unknown")
        self.assertEqual(ent["effective_mode"], "free")

    def test_is_user_pro_не_считает_лайт_полным(self):
        with patch.object(db, "resolve_entitlement", return_value={"effective_mode": "free"}):
            self.assertFalse(db.is_user_pro(UID))


class СчётНаЛайт(unittest.TestCase):

    def setUp(self):
        self.client = server.app.test_client()

    def _patches(self, entitlement):
        return (
            patch.object(server, "_telegram_hash_is_valid", return_value=True),
            patch.object(server, "_parse_telegram_init_data", return_value={"user": {"id": UID}}),
            patch.object(server, "_resolve_webapp_user_allowed", return_value=(True, "test")),
            patch.object(server, "_sync_user_subscription_from_live_stripe", lambda **kwargs: {}),
            patch.object(server, "enforce_daily_cost_cap", lambda **kwargs: None),
            patch.object(server, "resolve_entitlement", return_value=entitlement),
            patch.object(server, "create_stars_invoice_link", return_value=("https://t.me/$inv", None)),
        )

    def test_подписка_на_30_дней_с_purpose_light(self):
        p = self._patches({"effective_mode": "free", "source_of_entitlement": "free_default"})
        with p[0], p[1], p[2], p[3], p[4], p[5], p[6] as link:
            r = self.client.post("/api/webapp/billing/stars_invoice",
                                 json={"initData": "x", "plan_code": "light"})
        self.assertEqual(r.status_code, 200, r.get_json())
        self.assertEqual(r.get_json()["stars"], 160)
        kw = link.call_args.kwargs
        self.assertEqual(kw["payload_obj"], {"purpose": "light", "user_id": UID})
        self.assertEqual(kw["subscription_period"], server.STARS_SUBSCRIPTION_PERIOD_SECONDS)
        self.assertEqual(kw["stars"], 160)

    def test_при_действующем_полном_лайт_не_продаём(self):
        p = self._patches({"effective_mode": "pro", "source_of_entitlement": "paid_subscription"})
        with p[0], p[1], p[2], p[3], p[4], p[5], p[6] as link:
            r = self.client.post("/api/webapp/billing/stars_invoice",
                                 json={"initData": "x", "plan_code": "light"})
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.get_json()["error_code"], "already_pro")
        link.assert_not_called()

    def test_цена_по_умолчанию_160(self):
        self.assertEqual(server.light_price_stars(), 160)


class ЛайтОтменяетсяПриПокупкеПолного(unittest.TestCase):

    def _run(self, sub, edit_result):
        sent = []

        class _Bot:
            async def send_message(self, chat_id, text):
                sent.append((chat_id, text))

        class _Ctx:
            bot = _Bot()

        with patch.object(db, "get_user_subscription", return_value=sub), \
             patch.object(bot_3, "_edit_user_star_subscription", return_value=edit_result) as edit, \
             patch.object(bot_3, "get_admin_telegram_ids", return_value={117649764}):
            asyncio.run(bot_3._cancel_light_subscription_after_pro(_Ctx(), UID))
        return edit, sent

    def test_действующий_лайт_отменяется(self):
        edit, sent = self._run(_sub("light", sub_id="stars_ch1"), (True, "ok"))
        edit.assert_called_once_with(UID, "ch1", is_canceled=True)
        self.assertEqual(sent, [])

    def test_без_лайта_ничего_не_трогаем(self):
        edit, sent = self._run(_sub("pro", sub_id="stars_ch1"), (True, "ok"))
        edit.assert_not_called()
        edit, sent = self._run(None, (True, "ok"))
        edit.assert_not_called()

    def test_не_вышло_отменить_письмо_администратору(self):
        edit, sent = self._run(_sub("light", sub_id="stars_ch1"), (False, "Bad Request: nope"))
        self.assertEqual(len(sent), 1)
        self.assertIn("ch1", sent[0][1])
        self.assertIn("/refund_star ch1", sent[0][1])


if __name__ == "__main__":
    unittest.main()
