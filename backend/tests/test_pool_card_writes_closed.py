"""Общий пул больше не принимает разбор.

Дом разбора — единица. Два склада одного и того же неизбежно расходятся: именно на
сведение таких двух складов ушли 04–05.08.2026, и повторять это незачем.

Пул при этом остаётся читаемым — готовые разборы никуда не деваются и продолжают
находиться. И строки в нём по-прежнему заводятся: на них держится связь карточки с
общим словом и защита от дублей. Замирает только содержимое.

Правило вынесено в одну функцию, чтобы оба места записи (обновление существующей
строки и вставка новой) не разъехались, и чтобы его можно было проверить без базы.
"""
import importlib

from backend import database


def _sql(enabled):
    """Собрать кусок SQL при заданном положении рубильника."""
    saved = database.DICTIONARY_POOL_CARD_WRITES_ENABLED
    try:
        database.DICTIONARY_POOL_CARD_WRITES_ENABLED = enabled
        return database._pool_card_update_sql("EXCLUDED.response_json",
                                              "bt_3_dictionary_entries.response_json")
    finally:
        database.DICTIONARY_POOL_CARD_WRITES_ENABLED = saved


def test_closed_pool_keeps_what_is_stored():
    """Ничего не сравниваем и не перезаписываем — оставляем как лежит."""
    assert _sql(False) == "bt_3_dictionary_entries.response_json"
    assert "EXCLUDED" not in _sql(False)


def test_open_pool_still_prefers_the_fuller_card():
    """Открыли обратно — работает прежнее правило: полная карточка вытесняет тонкую."""
    sql = _sql(True)
    assert sql.startswith("CASE WHEN")
    assert "EXCLUDED.response_json" in sql


def test_switch_is_off_by_default():
    """По умолчанию пул закрыт: включать надо осознанно, переменной окружения."""
    fresh = importlib.reload(database)
    assert fresh.DICTIONARY_POOL_CARD_WRITES_ENABLED is False
