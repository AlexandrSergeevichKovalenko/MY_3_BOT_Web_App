# -*- coding: utf-8 -*-
"""Клавиатура в «Подставь синоним» поднимается после лампочки и после второй попытки.

Повод, 14.09.2026 — первый живой проход владельца. Десять пропусков подряд ушли на
проверку с одними подсказанными буквами, дважды каждый, с интервалом в две секунды:

    ждали           ввёл   исход   попытка  подсказок
    Revision        Re     wrong      1         1
    Revision        Re     wrong      2         1
    Modifizierung   Mod    wrong      1         2
    Modifizierung   Mod    wrong      2         2

Человек нажимал лампочку, она вписывала буквы — и дописать было нечем: клавиатура не
выезжала, оставалось только жать «Проверить».

Две причины, обе в коде экрана:
  1. фокус возвращался через setTimeout — а iOS поднимает клавиатуру ТОЛЬКО внутри
     самого касания, отложенный вызов её не поднимает;
  2. поле ввода размонтировалось на время разбора (стояла развилка «либо ввод, либо
     вердикт»), и после «Попробовать ещё раз» фокусировать было уже нечего.

ПОЧЕМУ ТЕСТ ЧИТАЕТ ИСХОДНИК, А НЕ КЛИКАЕТ. Поломка живёт только там, где клавиатура
экранная: в безголовом браузере она «есть» всегда, и ни один прогон её не воспроизведёт.
Поэтому здесь держится не поведение, а ПРАВИЛО, из-за нарушения которого оно ломается.
"""
import pathlib
import re

GAP = (pathlib.Path(__file__).resolve().parents[2]
       / "frontend" / "src" / "answer" / "GapGame.jsx")
SRC = GAP.read_text(encoding="utf-8")

# Комментарии вырезаем: в этом файле описана САМА поломка, и слова «setTimeout(30)»
# в объяснении не должны ронять проверку. Ищем правило в коде, а не в тексте о нём.
CODE = re.sub(r"/\*.*?\*/", "", SRC, flags=re.S)
CODE = re.sub(r"(?m)^\s*//.*$", "", CODE)


def test_fokus_ne_otkladyvaetsya_cherez_setTimeout():
    """setTimeout вокруг focus() — ровно та поломка. Фокус только синхронно."""
    otlozhennye = re.findall(r"setTimeout\((?:[^)]|\)(?!;))*?focus\(\)", CODE, re.S)
    assert not otlozhennye, (
        "focus() снова вызывается из setTimeout — на айфоне клавиатура не выедет: "
        f"{otlozhennye}")


def test_lampochka_ne_uvodit_fokus_s_polya():
    """Нажатие на кнопку забирает фокус у поля, и клавиатура прячется. Гасим само
    событие наведения, а не лечим последствия."""
    i = SRC.index("gp-hint-btn")
    okno = SRC[i:i + 400]
    assert "onPointerDown" in okno and "preventDefault" in okno, (
        "у кнопки-лампочки пропал preventDefault на onPointerDown")


def test_pole_vvoda_ne_razmontiruetsya_na_vremya_razbora():
    """Поле живёт всегда: иначе после «Попробовать ещё раз» фокусировать нечего."""
    assert "readOnly={!!verdict}" in SRC, (
        "поле снова прячут вместо readOnly — вернётся потеря фокуса на второй попытке")
    # Развилка «либо ввод, либо вердикт» вокруг самих клеток вернуться не должна.
    assert "{!verdict ? (\n          <div className=\"gp-form\">" not in SRC


def test_fokus_stavitsya_do_izmeneniya_sostoyaniya():
    """Внутри касания: focus() обязан стоять ПЕРЕД setValue/setHints, иначе React
    успевает перерисовать и жест теряется."""
    i = SRC.index("const useHint = useCallback")
    telo = SRC[i:SRC.index("}, [hints, verdict, item, haptic]);", i)]
    assert telo.index("focus()") < telo.index("setValue("), "focus() ушёл после setValue"
    assert telo.index("focus()") < telo.index("setHints("), "focus() ушёл после setHints"
