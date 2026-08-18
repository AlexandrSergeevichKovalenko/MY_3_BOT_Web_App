# -*- coding: utf-8 -*-
"""Почему человек признан бесплатным — должно быть ЗАПИСАНО, а не потеряно.

Замер 18.08.2026: у платящего человека (подписка pro/active) 920 из 971 запуска ярлыка
записаны как «бесплатный». Установить причину оказалось нечем — решение нигде не
объяснялось, а журналы Railway живут 33 часа. Эти тесты закрепляют диагностику:

  • «тариф не прочитался» и «человек правда бесплатный» — РАЗНЫЕ источники;
  • причина вердикта доходит из проверки прав до записи о запуске.

Когда причина будет найдена и подмена убрана, тесты останутся: они запрещают
вернуться к состоянию, где эти два случая снова неразличимы.
"""
from __future__ import annotations

import backend.database as D


def _fake_subscription(plan_code="pro", status="active"):
    return {"user_id": 1, "plan_code": plan_code, "status": status,
            "trial_ends_at": None, "current_period_end": None,
            "stripe_customer_id": None, "stripe_subscription_id": "sub_x",
            "created_at": None, "updated_at": None}


def test_нечитаемый_тариф_отличается_от_честного_бесплатного(monkeypatch):
    """Тариф не прочитался → источник «plan_unreadable», а не «free_default»."""
    monkeypatch.setattr(D, "get_billing_plan", lambda code: None)
    monkeypatch.setattr(D, "get_active_pro_grant_detail", lambda uid: (None, None))
    result = D.resolve_entitlement(user_id=1, subscription=_fake_subscription())
    assert result["effective_mode"] == "free"
    assert result["source_of_entitlement"] == "plan_unreadable", (
        "«не смогли прочитать тариф» обязано быть отличимо от «человек бесплатный»")


def test_честный_бесплатный_остаётся_free_default(monkeypatch):
    monkeypatch.setattr(D, "get_billing_plan",
                        lambda code: {"plan_code": code, "is_paid": False,
                                      "name": "Free", "daily_cost_cap_eur": 0.5})
    monkeypatch.setattr(D, "get_active_pro_grant_detail", lambda uid: (None, None))
    result = D.resolve_entitlement(user_id=1,
                                   subscription=_fake_subscription("free", "inactive"))
    assert result["effective_mode"] == "free"
    assert result["source_of_entitlement"] == "free_default"


def test_платная_подписка_читается_как_платная(monkeypatch):
    monkeypatch.setattr(D, "get_billing_plan",
                        lambda code: {"plan_code": code, "is_paid": code == "pro",
                                      "name": code, "daily_cost_cap_eur": 5.0})
    monkeypatch.setattr(D, "get_active_pro_grant_detail", lambda uid: (None, None))
    result = D.resolve_entitlement(user_id=1, subscription=_fake_subscription())
    assert result["effective_mode"] == "pro"
    assert result["source_of_entitlement"] == "paid_subscription"


def test_запись_о_запуске_принимает_причину_вердикта():
    """Колонка есть в сигнатуре — иначе причину некуда положить."""
    import inspect
    params = inspect.signature(D.record_shortcut_run_check).parameters
    assert "entitlement_source" in params


def test_гейт_ярлыка_отдаёт_причину_вместе_с_вердиктом():
    """Причина обязана выходить из гейта наружу, иначе она не доедет до записи."""
    import inspect
    import backend.backend_server as B
    source = inspect.getsource(B._shortcut_run_gate)
    assert '"entitlement_source": ent_source' in source
    assert 'ent_source = "ошибка чтения прав"' in source, (
        "сбой чтения прав обязан отличаться от честного «человек бесплатный»")
