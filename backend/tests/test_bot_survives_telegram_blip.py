"""Сетевой сбой на старте не должен убивать бота.

09.08.2026 бот лёг в цикл падений: при первом обращении к Telegram (get_me внутри
initialize) соединение не поднялось — httpx.ConnectTimeout, — и процесс умирал целиком.
Railway перезапускал контейнер трижды с интервалом ~50 секунд, потом сдался, и бот
остался лежать. Люди писали в тишину.

Таймаут был ни при чём: на соединение уже даётся 20 секунд. Фатальной считалась ОДНА
неудача. Проверяем по исходнику, что запуск переживает сетевой сбой и что повтор
касается ТОЛЬКО сети: неверный токен должен валить процесс сразу, иначе причина
спрячется за молчаливым циклом.
"""
import ast
import re
from pathlib import Path

BOT = Path(__file__).resolve().parents[2] / "bot_3.py"
SRC = BOT.read_text(encoding="utf-8")


def _retry_block() -> str:
    start = SRC.index("_POLL_RETRY_DELAYS")
    return SRC[start:start + 1400]


def test_polling_start_is_retried():
    block = _retry_block()
    assert "run_polling" in block, "запуск опроса не обёрнут повтором"
    assert "for attempt" in block, "нет цикла повторов"


def test_only_network_errors_are_retried():
    block = _retry_block()
    m = re.search(r"except \(([^)]*)\)", block)
    assert m, "не найден перехват ошибок"
    caught = {x.strip() for x in m.group(1).split(",")}
    assert caught == {"TimedOut", "NetworkError"}, (
        f"повторяются не только сетевые ошибки: {caught} — неверный токен уйдёт в тихий цикл"
    )


def test_gives_up_eventually_instead_of_looping_forever():
    block = _retry_block()
    assert "raise" in block, "после всех попыток ошибка обязана всплыть, а не потеряться"


def test_names_used_in_the_retry_are_actually_imported():
    """Ловушка, на которой я споткнулся: в файле нет имени `telegram`, а time импортирован
    как pytime. Обращение к telegram.error.TimedOut упало бы NameError — в проде, при
    первом же сбое сети, то есть ровно тогда, когда повтор и нужен."""
    bound = set()
    for node in ast.walk(ast.parse(SRC)):
        if isinstance(node, ast.ImportFrom):
            bound.update(a.asname or a.name for a in node.names)
        elif isinstance(node, ast.Import):
            bound.update((a.asname or a.name).split(".")[0] for a in node.names)
    block = _retry_block()
    for name in ("TimedOut", "NetworkError"):
        assert name in bound, f"{name} не импортирован"
    assert "pytime.sleep" in block, "пауза между повторами зовёт несвязанное имя time"
    assert "telegram.error." not in block, "обращение к несвязанному имени telegram"
