"""Каждый эндпоинт, который зовёт словарь с рабочего стола, обязан принимать его токен.

Этот словарь живёт ВНЕ Telegram: он открывается иконкой и работает по долговременному
токену. Сессия Telegram (initData) у него есть только первые тридцать дней после
установки — она кешируется при запуске и потом протухает.

Поэтому эндпоинт, который требует именно сессию, ломается не сразу, а через месяц —
и уже у человека, а не у нас. Так и вышло с «Моими словами» 10.08.2026: список открылся
на разработке и отказал у владельца. Проверочный проход в тот же день нашёл вторую
такую же дыру — в кнопке «Поделиться», до того как она успела проявиться.

Список путей взят из самого DictionaryOverlay: если он позовёт что-то новое, тест
это увидит.
"""
import inspect
import re
from pathlib import Path

from backend import backend_server

DICT_DIR = Path(__file__).resolve().parents[2] / "frontend/src/dictionary"
# Экран словаря — не один файл. Часть своих запросов он делает через общие модули
# рядом (27.08.2026 туда уехала история поиска), и такой запрос сторож обязан видеть
# так же, как запрос из самого экрана: иначе дыру закрывает не тест, а случай.
FRONT_FILES = [DICT_DIR / "DictionaryOverlay.jsx", DICT_DIR / "searchHistory.js"]
SRC = inspect.getsource(backend_server)


def _paths_the_standalone_calls() -> set[str]:
    paths: set[str] = set()
    for path in FRONT_FILES:
        if not path.exists():
            continue
        paths |= set(re.findall(r"'(/api/webapp/[a-z/-]+)'", path.read_text(encoding="utf-8")))
    return paths


def _handler_body(path: str) -> str | None:
    marker = f'@app.route("{path}"'
    i = SRC.find(marker)
    if i == -1:
        return None
    j = SRC.find("@app.route(", i + 20)
    return SRC[i:j if j != -1 else len(SRC)]


def test_every_called_endpoint_accepts_the_dict_token():
    broken = []
    for path in sorted(_paths_the_standalone_calls()):
        body = _handler_body(path)
        if body is None:
            continue  # путь обслуживается иначе (префиксом или другим модулем)
        demands_session = "_telegram_hash_is_valid" in body
        accepts_token = "_resolve_webapp_user_id" in body
        if demands_session and not accepts_token:
            broken.append(path)
    assert not broken, (
        "эти эндпоинты требуют сессию Telegram, а словарь с рабочего стола работает по "
        f"токену — через месяц после установки они откажут: {broken}"
    )


def test_paths_are_reachable_with_a_token_at_all():
    for path in sorted(_paths_the_standalone_calls()):
        assert backend_server._dict_token_access_allowed(path), (
            f"путь {path} закрыт для токена словаря — запрос не дойдёт до обработчика"
        )
