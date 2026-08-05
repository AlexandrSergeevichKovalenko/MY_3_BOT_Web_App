# -*- coding: utf-8 -*-
"""Форма кроссворда: какие буквы открыты и сколько клеток остаётся человеку.

Загаданное слово стоит в сетке не в одиночестве: его пересекают открытые слова, и
каждая такая клетка показывает букву — иначе открытое слово читалось бы с дыркой.
Сверх этого раньше всегда открывалась «опора» (первая буква, середина, конец —
`_extra_revealed_positions`). Вместе эти два источника доводили слово до одной
пустой клетки: FLUSS в сетке от 05.08.2026 стоял как F L U _ S — вписать туда
можно было только S, и подсказка 💡 дарила разгадку целиком.

Правило теперь одно и жёсткое: **в загаданном слове не бывает одной пустой клетки**
(`MIN_OPEN_CELLS`). Опора добавляется только до этой границы, а слово, которое даже
без опоры остаётся с одной клеткой (его насквозь прошили открытые слова), — брак:
такой кроссворд не собирается, не выбирается для отправки и не даёт подсказку.

Модуль намеренно без зависимостей (никакого PIL): его зовут и генератор, и слой БД,
и проверка ответов, и рендер картинки — все должны видеть ОДНУ И ТУ ЖЕ сетку.
"""
from __future__ import annotations

# Меньше двух пустых клеток в загаданном слове не бывает.
MIN_OPEN_CELLS = 2


def _extra_revealed_positions(word_len: int) -> list[int]:
    """
    Positions within a hidden word that are always shown as a solving aid.
    Rules (on top of any intersection-revealed letters):
      ≤ 4 letters : first letter only
      5-7 letters : first + middle
      8-10 letters: first + middle + last
      11+  letters: first + 1/3 + 2/3 + last

    Это ЖЕЛАЕМЫЕ опоры, а не обещание: `compute_revealed_cells` берёт их по порядку
    и останавливается, как только слову остаётся `MIN_OPEN_CELLS` пустых клеток.
    """
    if word_len <= 1:
        return [0]
    if word_len <= 4:
        return [0]
    if word_len <= 7:
        return [0, word_len // 2]
    if word_len <= 10:
        return [0, word_len // 2, word_len - 1]
    return [0, word_len // 3, (word_len * 2) // 3, word_len - 1]


def word_cells(word: dict) -> list[tuple[int, int]]:
    """Клетки слова по порядку букв. Пустой список, если слово не размещено."""
    text = str(word.get("word") or "")
    if not text or word.get("row") is None or word.get("col") is None:
        return []
    r, c = int(word["row"]), int(word["col"])
    dr, dc = (0, 1) if word.get("direction") == "across" else (1, 0)
    return [(r + dr * i, c + dc * i) for i in range(len(text))]


def compute_revealed_cells(
    words_json: list[dict], *, min_open: int = MIN_OPEN_CELLS,
) -> set[tuple[int, int]]:
    """
    Return set of (row, col) that show their letter:
    - All cells of visible (non-hidden) words
    - Extra positions of hidden words (first letter etc.) so user has a pattern,
      но только пока у КАЖДОГО загаданного слова остаётся min_open пустых клеток.

    Опора, из-за которой слово (своё или соседнее загаданное — клетка бывает общей)
    сваливается ниже границы, не открывается.
    """
    hidden: list[tuple[dict, list[tuple[int, int]]]] = []
    revealed: set[tuple[int, int]] = set()

    for word in words_json:
        cells = word_cells(word)
        if not cells:
            continue
        if word.get("hidden"):
            hidden.append((word, cells))
        else:
            revealed.update(cells)

    def _open_count(cells: list[tuple[int, int]]) -> int:
        return sum(1 for cell in cells if cell not in revealed)

    for word, cells in hidden:
        for i in _extra_revealed_positions(len(cells)):
            cell = cells[i]
            if cell in revealed:
                continue
            revealed.add(cell)
            # Клетка бывает общей у двух загаданных слов — проверяем все, кого задели.
            if any(cell in other_cells and _open_count(other_cells) < min_open
                   for _other, other_cells in hidden):
                revealed.discard(cell)

    return revealed


def open_cells_by_word(
    words_json: list[dict], *, min_open: int = MIN_OPEN_CELLS,
) -> dict[int, int]:
    """Сколько пустых клеток в каждом загаданном слове: {номер слова: клеток}."""
    revealed = compute_revealed_cells(words_json, min_open=min_open)
    counts: dict[int, int] = {}
    for word in words_json:
        if not word.get("hidden") or word.get("number") is None:
            continue
        cells = word_cells(word)
        counts[int(word["number"])] = sum(1 for cell in cells if cell not in revealed)
    return counts


def giveaway_problem(
    words_json: list[dict] | None, *, min_open: int = MIN_OPEN_CELLS,
) -> str | None:
    """Причина, по которой кроссворд нельзя показывать, либо None.

    Сторож на всех путях: сборка, выбор для отправки, ночная прополка банка.
    """
    words = list(words_json or [])
    if not words:
        return None
    revealed = compute_revealed_cells(words, min_open=min_open)
    bad: list[str] = []
    for word in words:
        if not word.get("hidden"):
            continue
        cells = word_cells(word)
        if not cells:
            continue
        count = sum(1 for cell in cells if cell not in revealed)
        if count < min_open:
            bad.append(f"{word.get('number')} ({word.get('word')}): пустых клеток {count}")
    if bad:
        return "загаданное слово почти дописано за человека — " + "; ".join(bad)
    return None
