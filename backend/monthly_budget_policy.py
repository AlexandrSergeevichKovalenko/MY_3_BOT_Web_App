"""Политика платных резервов сверх бесплатных квот провайдеров — раз и навсегда.

Зачем. Владелец согласился на доп. расходы до $8/мес на озвучку «Классики»: Standard-голос
стоит $4 за 1M символов, значит $8 = 2 млн символов сверх бесплатных 4M. Резерв ставится
через `extra_limit_units` в bt_3_provider_budget_controls — НО эта строка живёт ПОМЕСЯЧНО
(period_month), и первого числа резерв обнуляется вместе с новым месяцем.

Договорённость была «$8 в месяц», а не «$8 в июле», поэтому проставлять руками каждый месяц
нельзя: один пропущенный месяц — и озвучка молча выключится на 4-миллионном символе.
Эта политика применяется автоматически: сверяет текущий месяц с настройкой и добирает
резерв, если он ниже. Идемпотентна — можно звать сколько угодно раз.

⚠️ Резерв — это ПОТОЛОК, а не предоплата. Ничего не синтезируется заранее: деньги тратятся
только когда живой человек нажал «слушать». Не запросили — не потратили.
"""
from __future__ import annotations

import logging
import os
from typing import Any


def _env_int(name: str, default: int) -> int:
    try:
        return max(0, int((os.getenv(name) or "").strip() or default))
    except Exception:
        return default


def configured_extras() -> dict[str, int]:
    """{провайдер: сколько единиц докупаем сверх бесплатной квоты}.

    google_tts_standard: 2 000 000 символов ≈ $8/мес при цене $4 за 1M — озвучка «Классики»
    по требованию. Остальные вёдра сознательно на нуле: WaveNet тратят только фразы (2% от
    лимита), у платного ведра свой потолок под оплаченные книги.
    """
    return {
        "google_tts_standard": _env_int("GOOGLE_TTS_STANDARD_EXTRA_LIMIT_CHARS", 2_000_000),
    }


def ensure_monthly_extra_limits() -> dict[str, Any]:
    """Добрать резервы текущего месяца до настроенных значений. Идемпотентно."""
    from backend.database import (
        get_provider_monthly_budget_status,
        set_provider_budget_extra_limit,
    )
    applied: list[dict[str, Any]] = []
    for provider, want in configured_extras().items():
        if want <= 0:
            continue
        try:
            status = get_provider_monthly_budget_status(
                provider=provider, units_type="chars", unit_label="chars"
            ) or {}
            have = int(status.get("extra_limit_units") or 0)
            if have >= want:
                continue
            res = set_provider_budget_extra_limit(
                provider=provider,
                extra_limit_units=want,
                metadata={"why": "месячный резерв по политике monthly_budget_policy",
                          "was": have},
            )
            if res:
                applied.append({"provider": provider, "from": have, "to": want})
                logging.info("monthly budget policy: %s резерв %d → %d единиц", provider, have, want)
        except Exception:
            logging.warning("monthly budget policy: не смог проставить резерв для %s",
                            provider, exc_info=True)
    return {"ok": True, "applied": applied}
