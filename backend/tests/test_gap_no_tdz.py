# -*- coding: utf-8 -*-
"""Экран «Подставь синоним» не ссылается на то, что объявлено НИЖЕ.

Повод, 15.09.2026: владелец открыл задание и увидел ПУСТОТУ. Обработчик клавиш стоял
на строке 105 и держал `check` в списке зависимостей useCallback, а сам `check`
объявлялся на 210-й. Список зависимостей вычисляется прямо во время отрисовки, ссылка
на ещё не созданную переменную роняет весь компонент — и экран не рисуется вовсе.

`npm run build` это НЕ ЛОВИТ: сборка прошла зелёной, всё упало только в браузере.
Поэтому правило держит тест: ни один useCallback/useMemo/useEffect в этом файле не
должен зависеть от имени, объявленного позже него. Ссылаться на такое можно только
через ref (см. checkRef).
"""
import pathlib
import re

GAP = (pathlib.Path(__file__).resolve().parents[2]
       / "frontend" / "src" / "answer" / "GapGame.jsx")
SRC = GAP.read_text(encoding="utf-8")
CODE = re.sub(r"/\*.*?\*/", "", SRC, flags=re.S)
CODE = re.sub(r"(?m)^\s*//.*$", "", CODE)

ОБЪЯВЛЕНИЕ = re.compile(r"(?m)^\s*const\s+([A-Za-z_$][\w$]*)\s*=")
ХУК = re.compile(r"use(?:Callback|Memo|Effect)\((?:.|\n)*?\}\s*,\s*\[([^\]]*)\]\s*\)")


def test_hook_ne_zavisit_ot_obyavlennogo_nizhe():
    # где какое имя объявлено
    где = {}
    for m in ОБЪЯВЛЕНИЕ.finditer(CODE):
        где.setdefault(m.group(1), m.start())

    беда = []
    for m in ХУК.finditer(CODE):
        конец_хука = m.start()
        for имя in re.findall(r"[A-Za-z_$][\w$]*", m.group(1)):
            позиция = где.get(имя)
            if позиция is not None and позиция > конец_хука:
                беда.append(f"{имя} (объявлен ниже строки хука)")
    assert not беда, (
        "хук зависит от имени, объявленного НИЖЕ — экран упадёт при первой отрисовке "
        f"и человек увидит пустоту: {sorted(set(беда))}")


def test_ssylka_na_check_idyot_cherez_ref():
    """Именно так починено 15.09.2026: если кто-то вернёт прямой вызов, тест упадёт."""
    i = CODE.index("const onCellKey")
    тело = CODE[i:CODE.index("}, [setCharAt, focusCell]);", i)]
    assert "checkRef.current" in тело
    assert re.search(r"(?<![\w.])check\s*\(", тело) is None, (
        "onCellKey снова зовёт check напрямую — он объявлен ниже, экран упадёт")
