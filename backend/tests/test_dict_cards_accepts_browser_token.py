"""Список «Мои слова» обязан открываться и в словаре с рабочего стола.

Владелец 10.08.2026 открыл словарь иконкой, нажал «Мои слова» и увидел пустой экран
с ошибкой. Причина: этот словарь живёт ВНЕ Telegram и работает по долговременному
токену, а обработчик списка карточек требовал именно сессию Telegram (initData) —
единственный из всех эндпоинтов словаря.

Путь при этом в разрешённых по токену был: спотыкалась не проверка доступа, а сам
обработчик.
"""
import inspect

from backend import backend_server


def _source_of_cards_endpoint() -> str:
    """Только тело этого обработчика. Окно «столько-то символов» захватывало соседний
    эндпоинт, который сессию Telegram требует законно, — и тест ругался на чужое."""
    src = inspect.getsource(backend_server)
    start = src.index('@app.route("/api/webapp/dictionary/cards"')
    end = src.index("@app.route(", start + 20)
    return src[start:end]


def test_cards_endpoint_resolves_user_the_common_way():
    body = _source_of_cards_endpoint()
    assert "_resolve_webapp_user_id(" in body, (
        "список карточек определяет пользователя по-своему — словарь с рабочего стола снова отвалится"
    )


def test_cards_endpoint_does_not_demand_telegram_session():
    body = _source_of_cards_endpoint()
    assert "initData обязателен" not in body, "обработчик всё ещё требует сессию Telegram"
    assert "_telegram_hash_is_valid" not in body, (
        "обработчик проверяет только подпись Telegram — токен словаря так не пройдёт"
    )


def test_path_is_reachable_with_a_dict_token():
    assert backend_server._dict_token_access_allowed("/api/webapp/dictionary/cards"), (
        "путь списка карточек закрыт для токена словаря"
    )
