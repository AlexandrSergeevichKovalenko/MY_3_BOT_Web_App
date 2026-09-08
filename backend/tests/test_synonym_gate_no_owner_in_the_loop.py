"""Человека из цепочки синонимов убрать (владелец 08.09.2026).

«Я не понимаю, зачем мне вообще высылается это на согласование. Я человек, не модель…
Модель ставит итоговую точку.» Проверяем то, что видит владелец: письмо не планируется,
строки здоровья про письмо нет, генератор не урезает список сам, слово с нехваткой
подтверждённых не выбрасывается, а ждёт судью снятым."""
from __future__ import annotations

import asyncio
import inspect
from unittest.mock import patch


def test_письмо_с_кнопками_больше_не_планируется():
    from backend import scheduler_service
    src = inspect.getsource(scheduler_service)
    assert "SYNONYM_REVIEW_ENABLED" not in src, "письмо 12:45 вернулось в планировщик"
    assert "ОТМЕНЕНО 08.09.2026" in src


def test_строки_здоровья_про_письмо_нет():
    import bot_3
    keys = {row[0] for row in bot_3._SCHEDULER_HEALTH_CATALOG}
    assert "sprint_accepted_review_dm" not in keys
    assert "sprint_bank_hygiene_job" in keys   # судья живёт внутри неё


def test_генератор_просит_полный_список_а_не_урезает():
    from backend.openai_manager import system_message
    for key in ("sprint_synonym", "sprint_antonym"):
        text = system_message[key]
        assert "lieber 4 echte" not in text, f"{key}: модель снова просят урезать список"
        assert "VOLLSTÄNDIGE Liste" in text and "Mindestens 3" in text


def test_тонкое_слово_кладётся_снятым_и_идёт_судье():
    """Раньше слово с 9 кандидатами и одним подтверждённым исчезало без суда вместе со
    всеми кандидатами; теперь оно в банке снятым, кандидаты — судье, судья возвращает."""
    import bot_3
    from backend.sprint_intake import GateResult, Rejected, UNCONFIRMED
    stored, queued, judged = [], [], []
    gate = GateResult(kept=[{"de": "abhängig", "ru": "зависимый"}],
                      rejected=[Rejected("hörig", "", UNCONFIRMED, [UNCONFIRMED]),
                                Rejected("unfrei", "", UNCONFIRMED, [UNCONFIRMED])],
                      stats={"duplicate": 0, "self": 0, "no_dictionary": 0, "article_mismatch": 0,
                             "article_unknown": 0, "unconfirmed": 2}, min_needed=3)

    async def gen(fmt, count, level):
        return [{"wort": "unabhängig", "accepted": [{"de": "abhängig", "ru": "зависимый"}],
                 "hint_ru": "независимый"}]

    with patch("backend.openai_manager.run_generate_aufgabe", gen), \
         patch("backend.sprint_intake.clean_accepted", lambda *a, **k: gate), \
         patch.object(bot_3, "upsert_sprint_item", lambda item: stored.append(item)), \
         patch("backend.sprint_intake.queue_for_judge", lambda **k: queued.append(k) or 2), \
         patch("backend.sprint_intake.remember_last_stats", lambda kind, stats: None), \
         patch("backend.synonym_judge.judge_open_reviews", lambda **k: judged.append(k) or {}):
        made = asyncio.run(bot_3._sprint_topup("antonym", 1))
    assert made == 0, "снятое слово не считается добавленным в показ"
    assert stored and stored[0]["retired"] is True and stored[0]["retired_reason"] == "thin_accepted"
    assert queued and queued[0]["sprint_id"] == "sp_antonym_unabh_ngig"
    assert judged, "судья не вызван — кандидаты остались бы висеть до ночи"
