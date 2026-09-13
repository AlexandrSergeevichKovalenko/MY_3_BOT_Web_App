# -*- coding: utf-8 -*-
"""`logging` обязан быть виден во ВСЁМ backend/answer_eval.py, а не в трёх функциях.

Повод, 13.09.2026. Владелец открыл «Подставь синоним» на слове `sich erinnern`:
четыре заготовки не построились, сработала строка со счётчиком отказов — и вместо
записи в лог прилетел NameError, а человеку экран «Не удалось загрузить». Разбор по
всему файлу нашёл ТРИ такие функции, и две из них жили до новой игры: обработчики
ошибок в `evaluate_aufgabe` и `evaluate_sprint`. То есть ровно там, где приложение
должно было тихо записать «не смогли и пошли дальше», оно падало с 500.

Тест держит КЛАСС: не «эта строка починена», а «в файле не осталось функций, которые
зовут logging, не имея его в своей области».
"""
import ast
import pathlib
import types

import backend.answer_eval as answer_eval

ФАЙЛ = pathlib.Path(answer_eval.__file__)


def test_logging_est_na_urovne_modulya():
    assert isinstance(getattr(answer_eval, "logging", None), types.ModuleType)


def test_ni_odna_funktsiya_ne_zovyot_logging_bez_importa():
    tree = ast.parse(ФАЙЛ.read_text(encoding="utf-8"))
    module_level = any(
        isinstance(n, ast.Import) and any(a.name == "logging" for a in n.names)
        for n in tree.body
    )
    assert module_level, "import logging пропал с уровня модуля — вернётся NameError в проде"

    свои_импорты, зовут = set(), {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for sub in ast.walk(node):
            if isinstance(sub, ast.Import) and any(a.name == "logging" for a in sub.names):
                свои_импорты.add(node.name)
            if (isinstance(sub, ast.Attribute) and isinstance(sub.value, ast.Name)
                    and sub.value.id == "logging"):
                зовут.setdefault(node.name, []).append(sub.lineno)
    # С импортом на уровне модуля видно всем — но если его когда-нибудь снимут,
    # этот список покажет поимённо, что сломается.
    без_своего = {k: v for k, v in зовут.items() if k not in свои_импорты}
    assert module_level or not без_своего, f"зовут logging без импорта: {без_своего}"
