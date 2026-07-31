# -*- coding: utf-8 -*-
"""Подсказка по одной букве на слово и досрочное завершение кроссворда."""
import pytest

from backend import answer_eval


WORDS = [
    {"number": 1, "word": "KÜCHE", "direction": "across", "row": 0, "col": 0,
     "clue_de": "Hier wird gekocht", "clue_ru": "Здесь готовят",
     "translation_ru": "кухня", "hidden": True},
    {"number": 2, "word": "KAFFEE", "direction": "down", "row": 0, "col": 0,
     "clue_de": "Braunes Getränk am Morgen", "clue_ru": "Коричневый напиток утром",
     "translation_ru": "кофе", "hidden": True},
    {"number": 3, "word": "TELLER", "direction": "across", "row": 5, "col": 0,
     "clue_de": "Davon isst man", "clue_ru": "С неё едят",
     "translation_ru": "тарелка", "hidden": False},
]


@pytest.fixture
def bank(monkeypatch):
    """Кроссворд в банке + пустые таблицы ответов и подсказок, всё в памяти."""
    state = {"hints": [], "answers": []}

    monkeypatch.setattr(
        answer_eval, "_load_crossword_hidden",
        lambda dispatch_id: [
            {k: w[k] for k in ("number", "word", "direction", "clue_de", "clue_ru", "translation_ru")}
            for w in WORDS if w["hidden"]
        ],
    )

    import backend.database as db
    monkeypatch.setattr(db, "get_crossword_dispatch_by_id",
                        lambda dispatch_id: {"words_json": WORDS, "topic": "Kochen und Küche"})
    monkeypatch.setattr(db, "get_crossword_answers",
                        lambda **kw: list(state["answers"]))
    monkeypatch.setattr(db, "get_crossword_hints",
                        lambda **kw: list(state["hints"]))

    def _record(*, dispatch_id, user_id, word_number, cell_row, cell_col):
        if any(h["word_number"] == word_number for h in state["hints"]):
            return False
        state["hints"].append({"word_number": word_number, "cell_row": cell_row, "cell_col": cell_col})
        return True

    monkeypatch.setattr(db, "record_crossword_hint", _record)
    return state


def test_hint_opens_one_letter(bank):
    # (0, 3) — четвёртая буква слова 1 KÜCHE; первая и средняя открыты изначально
    res = answer_eval.reveal_crossword_hint(dispatch_id=1, user_id=7, row=0, col=3)
    assert res["letter"] == "H"
    assert res["word_number"] == 1
    assert res["used"] == 1 and res["total"] == 2


def test_already_visible_letter_is_not_wasted_on_a_hint(bank):
    # (0, 2) в KÜCHE открыта с самого начала как опора — тратить на неё подсказку глупо
    res = answer_eval.reveal_crossword_hint(dispatch_id=1, user_id=7, row=0, col=2)
    assert res.get("error")
    assert not bank["hints"]


def test_second_hint_in_the_same_word_is_refused(bank):
    answer_eval.reveal_crossword_hint(dispatch_id=1, user_id=7, row=0, col=3)
    again = answer_eval.reveal_crossword_hint(dispatch_id=1, user_id=7, row=0, col=4)
    assert again.get("error")
    assert "уже открыта" in again["error"]
    # …а другое слово по-прежнему доступно
    other = answer_eval.reveal_crossword_hint(dispatch_id=1, user_id=7, row=1, col=0)
    assert other["letter"] == "A"
    assert other["word_number"] == 2


def test_hint_outside_a_hidden_word_is_refused(bank):
    res = answer_eval.reveal_crossword_hint(dispatch_id=1, user_id=7, row=5, col=1)
    assert res.get("error")


def test_hint_after_the_puzzle_is_solved_is_refused(bank):
    bank["answers"].append({"word_number": 1, "user_answer": "KÜCHE", "is_correct": True})
    res = answer_eval.reveal_crossword_hint(dispatch_id=1, user_id=7, row=0, col=2)
    assert res.get("error")


def test_unfinished_words_keep_the_answers_aligned():
    """Досрочное завершение: пустые клетки приходят как «_».

    Если бы клиент слал пустую строку, разбор съехал бы на одну позицию и человек
    увидел бы чужие ответы напротив своих слов."""
    hidden = [
        {"number": 1, "word": "KÜCHE", "direction": "across", "clue_de": "", "clue_ru": "", "translation_ru": "кухня"},
        {"number": 2, "word": "KAFFEE", "direction": "down", "clue_de": "", "clue_ru": "", "translation_ru": "кофе"},
    ]
    results = answer_eval.check_crossword(hidden_words=hidden, raw_input="_____ KAFFEE")
    assert [r["number"] for r in results] == [1, 2]
    assert results[0]["is_correct"] is False
    assert results[1]["is_correct"] is True
    assert results[1]["correct"] == "KAFFEE"
