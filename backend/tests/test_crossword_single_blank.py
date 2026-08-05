# -*- coding: utf-8 -*-
"""В загаданном слове никогда не бывает одной пустой клетки.

Кроссворд от 05.08.2026: FLUSS стоял в сетке как F L U _ S — первая буква и середина
открывались «опорой», ещё две клетки показывали пересекающие открытые слова. Вписать
туда можно было только S, а кнопка 💡 дарила и её. Проверка сторожит три рубежа:
форму сетки, сборку кроссворда и саму подсказку.
"""
import pytest

from backend import answer_eval
from backend.crossword_shape import (
    MIN_OPEN_CELLS, compute_revealed_cells, giveaway_problem, open_cells_by_word,
)


def _word(number, text, direction, row, col, hidden):
    return {"number": number, "word": text, "direction": direction,
            "row": row, "col": col, "hidden": hidden,
            "clue_de": "", "clue_ru": "", "translation_ru": ""}


# FLUSS вниз, дважды прошитый открытыми словами: WELLE через L (строка 1) и
# FISCH через последнюю S (строка 4). Опоры (F и U) добили бы слово до одной клетки.
FLUSS_GRID = [
    _word(1, "FLUSS", "down", 0, 2, True),
    _word(2, "WELLE", "across", 1, 2, False),
    _word(3, "FISCH", "across", 4, 2, False),
]


def test_a_word_pierced_by_visible_words_keeps_two_blanks():
    counts = open_cells_by_word(FLUSS_GRID)
    assert counts[1] >= MIN_OPEN_CELLS
    assert not giveaway_problem(FLUSS_GRID)


def test_old_rule_left_a_single_blank():
    """Так было до правила — снимок прежнего поведения (опоры открывались всегда)."""
    counts = open_cells_by_word(FLUSS_GRID, min_open=0)
    assert counts[1] == 1


def test_visible_word_letters_are_still_all_shown():
    revealed = compute_revealed_cells(FLUSS_GRID)
    assert (1, 2) in revealed and (1, 6) in revealed   # WELLE читается целиком
    assert (4, 2) in revealed and (4, 6) in revealed   # FISCH тоже


def test_structurally_flat_word_is_a_giveaway():
    """Слово, прошитое насквозь: пустая клетка одна даже без единой опоры."""
    grid = [
        _word(1, "SEE", "down", 0, 1, True),
        _word(2, "SAND", "across", 0, 1, False),
        _word(3, "EIS", "across", 2, 1, False),
    ]
    problem = giveaway_problem(grid)
    assert problem and "SEE" in problem


def test_generator_refuses_to_hide_a_flat_word():
    from backend.crossword_generator import _select_hidden_words

    grid = [
        _word(1, "SEE", "down", 0, 1, False),
        _word(2, "SAND", "across", 0, 1, False),
        _word(3, "EIS", "across", 2, 1, False),
        _word(4, "STRAND", "down", 0, 4, False),
        _word(5, "DÜNE", "across", 5, 4, False),
    ]
    chosen = _select_hidden_words(grid, 2)
    assert not giveaway_problem(chosen)
    assert sum(1 for w in chosen if w["hidden"]) == 2


# ─── Подсказка ────────────────────────────────────────────────────────────────

HINT_WORDS = [
    _word(1, "SEE", "down", 0, 1, True),      # прошито насквозь: одна пустая клетка
    _word(2, "SAND", "across", 0, 1, False),
    _word(3, "EIS", "across", 2, 1, False),
    _word(4, "WIESE", "across", 6, 0, True),  # нормальное слово: клеток хватает
]


@pytest.fixture
def bank(monkeypatch):
    state = {"hints": [], "answers": []}

    monkeypatch.setattr(
        answer_eval, "_load_crossword_hidden",
        lambda dispatch_id: [w for w in HINT_WORDS if w["hidden"]],
    )
    import backend.database as db
    monkeypatch.setattr(db, "get_crossword_dispatch_by_id",
                        lambda dispatch_id: {"words_json": HINT_WORDS, "topic": "Natur"})
    monkeypatch.setattr(db, "get_crossword_answers", lambda **kw: list(state["answers"]))
    monkeypatch.setattr(db, "get_crossword_hints", lambda **kw: list(state["hints"]))

    def _record(*, dispatch_id, user_id, word_number, cell_row, cell_col):
        if any(h["word_number"] == word_number for h in state["hints"]):
            return False
        state["hints"].append({"word_number": word_number,
                               "cell_row": cell_row, "cell_col": cell_col})
        return True

    monkeypatch.setattr(db, "record_crossword_hint", _record)
    return state


def test_hint_is_refused_when_only_one_cell_is_empty(bank):
    # (1, 1) — единственная пустая клетка слова 1 SEE
    res = answer_eval.reveal_crossword_hint(dispatch_id=1, user_id=7, row=1, col=1)
    assert res.get("error")
    assert "одна буква" in res["error"]
    assert not bank["hints"]      # подсказка не потрачена


def test_hint_still_works_in_a_normal_word(bank):
    res = answer_eval.reveal_crossword_hint(dispatch_id=1, user_id=7, row=6, col=1)
    assert res.get("letter") == "I"
    assert res["word_number"] == 4
