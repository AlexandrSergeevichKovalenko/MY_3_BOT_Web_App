"""Разбор противоречивых записей словаря.

Владелец 26.08.2026 выбрал устройство: разбираем в мини-приложении, приглашение приходит
в понедельник и воскресенье, ничего не удаляется само, неразобранное переезжает дальше.

Что тут держится:
1. Правка предлагается ТОЛЬКО подтверждённая справочником. «inkelgasse» и «degeneriert»
   заглавной буквой словами не становятся — у них кнопки «Исправить» быть не должно.
2. Очередь копится: повторный скан не плодит дублей и не трогает уже лежащее.
3. «Применить» выполняет по ОДНОМУ действию на запись — не два над одной.
4. Разбор открыт только владельцу.
"""
import inspect

from backend import backend_server
from backend import database


def test_fix_is_offered_only_when_the_reference_confirms_it(monkeypatch):
    import backend.german_word_gate as gate

    monkeypatch.setattr(gate, "check_word", lambda word, **k: (
        {"status": gate.CONFIRMED, "text": word} if word == "Hammer"
        else {"status": gate.NOT_A_WORD, "text": word}
    ))

    fix = database._word_integrity_suggestion("hammer", "hammer", "noun", "der")
    assert fix and fix["to_display"] == "der Hammer"

    assert database._word_integrity_suggestion("inkelgasse", "inkelgasse", "noun", "die") is None, (
        "предложили исправить обрывок — заглавная буква не делает слово словом"
    )


def test_reference_repair_to_a_lowercase_word_is_not_a_noun_fix(monkeypatch):
    """«degeneriert» справочник чинит в глагол «degenerieren» — артикль к нему не клеим."""
    import backend.german_word_gate as gate
    monkeypatch.setattr(gate, "check_word", lambda word, **k: {
        "status": gate.REPAIRED, "text": "degenerieren",
    })
    assert database._word_integrity_suggestion("degeneriert", "degeneriert", "noun", "der") is None


def test_queue_carries_over_and_never_duplicates():
    """Повторный скан не плодит дубли: строка держится на unit_id."""
    src = inspect.getsource(database.scan_word_integrity)
    assert "ON CONFLICT (unit_id) DO NOTHING" in src, "повторный скан начнёт плодить дубли"
    assert "DELETE" not in src.upper(), "скан не имеет права ничего удалять"


def test_apply_does_one_action_per_record():
    """Одно решение на запись. «Применить» — это один заход по списку, а не два действия."""
    src = inspect.getsource(database.apply_word_integrity_decisions)
    assert '{"fix", "keep", "delete"}' in src, "набор решений изменился — проверить экран"
    assert "status = %s, decided_at = NOW()" in src, "решение не фиксируется в очереди"


def test_review_is_admin_only():
    for name in ("list_webapp_word_integrity", "apply_webapp_word_integrity"):
        src = inspect.getsource(getattr(backend_server, name))
        assert "_word_diff_is_admin" in src, f"{name} открыт не только владельцу"


def test_invitation_goes_monday_and_sunday_and_reminds_midweek():
    """Приглашение — Пн и Вс, отдельное напоминание среди недели, если не разобрано."""
    import io
    from pathlib import Path
    bot = Path(backend_server.__file__).resolve().parents[1] / "bot_3.py"
    text = io.open(bot, encoding="utf-8").read()
    assert '"mon,sun"' in text, "приглашение больше не приходит в понедельник и воскресенье"
    assert "WORD_INTEGRITY_REMINDER_DAYS" in text, "напоминание среди недели пропало"
    assert "startapp=slovarcheck" in text, "кнопка ведёт не на экран разбора"
