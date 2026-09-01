# -*- coding: utf-8 -*-
"""Всё, что ночные задачи ребуса импортируют, обязано существовать.

Повод (01.09.2026): при переписывании ночного прохода я заменил кусок файла по двум
меткам и захватил заодно `retry_unanswered_dwds_words`. Вызов в bot_3.py остался,
определение исчезло — ночное пополнение банка ребусов падало на КАЖДОМ прогоне:

    WARNING:root:rebus_pool_job failed
    ImportError: cannot import name 'retry_unanswered_dwds_words'
                 from 'backend.rebus_generator'

Полный прогон тестов этого не поймал: имя не импортировал никто, кроме самой
задачи, а задачу тесты не запускают. Поэтому проверка здесь идёт ОТ ИСХОДНИКА
`bot_3.py`: что он импортирует из `backend.rebus_generator`, то и обязано там быть.
Так страж не устареет, когда в задачу добавят следующую функцию.
"""

import ast
import pathlib

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
BOT = REPO / "bot_3.py"


def _names_imported_from(module: str) -> set[str]:
    """Все имена, которые bot_3.py импортирует из указанного модуля —
    включая импорты внутри функций (ночные задачи делают именно так)."""
    tree = ast.parse(BOT.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == module:
            for alias in node.names:
                if alias.name != "*":
                    names.add(alias.name)
    return names


@pytest.mark.parametrize("module", ["backend.rebus_generator", "backend.rebus_word_gate",
                                    "backend.dwds_frequency"])
def test_всё_что_бот_импортирует_существует(module):
    import importlib

    wanted = _names_imported_from(module)
    if not wanted:
        pytest.skip(f"bot_3.py ничего не импортирует из {module}")
    mod = importlib.import_module(module)
    missing = sorted(n for n in wanted if not hasattr(mod, n))
    assert not missing, (
        f"bot_3.py импортирует из {module} то, чего там нет: {missing}. "
        f"Ночная задача упадёт с ImportError на первом же прогоне."
    )


def test_ночное_пополнение_ребуса_собирается_целиком():
    """Именно тот импорт, который падал в проде, — отдельной строкой, чтобы
    поломка была видна по имени теста, а не по параметру."""
    from backend.rebus_generator import (  # noqa: F401
        fill_missing_rebus_image_versions,
        prepare_rebus_pool,
        retry_unanswered_dwds_words,
    )
