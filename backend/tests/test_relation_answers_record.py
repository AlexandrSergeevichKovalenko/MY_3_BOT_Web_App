# -*- coding: utf-8 -*-
"""Ответы игр рельса вправду доходят до записи.

До 13.09.2026 ни один ответ тренировки и «Подставь синоним» не сохранялся нигде: обе
игры считались целиком в браузере, и сервер знал только факт открытия. Эти тесты держат
КЛАСС «ответ посчитали и потеряли»: проверяют, что оценка зовёт запись и передаёт ей то,
что человек вправду сделал, — исход, номер попытки и число нажатых подсказок.

Живая база тут ни при чём: она доказывает доставку (обещание relation_gap_reaches_
learners), а работу самого механизма обязан доказывать тест.
"""
from unittest import mock

import backend.answer_eval as answer_eval


ITEM = {
    "wort": "detailliert", "relation": "synonym", "trainer_ready": True,
    "accepted": [{"de": "ausführlich", "ru": "подробный"}, {"de": "gründlich", "ru": "тщательный"}],
    "trainer_json": {"correct_examples": [
        {"word": "ausführlich", "sentence_de": "Der Bericht enthält eine ausführliche Beschreibung.",
         "sentence_ru": "В отчёте подробное описание.", "nuance": "о полноте"},
    ]},
}
DISPATCH = {"id": 42, "sprint_id": "s-1", "relation": "synonym",
            "target_user_id": 7, "chat_id": 7}


def _run(answer, attempt=1, hints=0):
    """Оценить ответ и вернуть то, ЧТО ушло в запись."""
    with mock.patch.object(answer_eval, "__name__", answer_eval.__name__):
        with mock.patch("backend.database.get_relation_gap_dispatch_by_id", return_value=DISPATCH), \
             mock.patch("backend.database.get_sprint_trainer_item", return_value=ITEM), \
             mock.patch("backend.database.record_relation_answer") as rec:
            out = answer_eval.evaluate_gap_answer(
                dispatch_id=42, user_id=7, index=0, answer=answer,
                attempt=attempt, hints_used=hints)
    assert rec.call_count == 1, "ответ оценили, но не записали"
    return out, rec.call_args.kwargs


def test_verniy_otvet_zapisan():
    out, rec = _run("ausführliche")
    assert out["outcome"] == "correct"
    assert rec["outcome"] == "correct"
    assert rec["kind"] == "lk" and rec["dispatch_id"] == 42 and rec["user_id"] == 7
    assert rec["target_word"] == "detailliert"
    assert rec["expected"] == "ausführliche"


def test_slovarnaya_forma_pishetsya_svoim_ishodom():
    """Не «wrong» и не «correct»: отдельный исход, иначе потом не отличить «человек не
    знал слова» от «знал, но не склонил»."""
    out, rec = _run("ausführlich")
    assert out["outcome"] == "wrong_form" and rec["outcome"] == "wrong_form"


def test_drugoy_sinonim_pishetsya_svoim_ishodom():
    out, rec = _run("gründlich")
    assert out["outcome"] == "other_synonym" and rec["outcome"] == "other_synonym"


def test_popytka_i_podskazki_doezhayut_do_zapisi():
    """Без числа подсказок «верно с первой попытки» ничего не значит: неизвестно,
    вспомнил человек слово или ему открыли треть."""
    _out, rec = _run("ausführliche", attempt=2, hints=2)
    assert rec["attempt"] == 2
    assert rec["hints_used"] == 2


def test_otvet_na_nesushchestvuyushchiy_propusk_nichego_ne_pishet():
    with mock.patch("backend.database.get_relation_gap_dispatch_by_id", return_value=DISPATCH), \
         mock.patch("backend.database.get_sprint_trainer_item", return_value=ITEM), \
         mock.patch("backend.database.record_relation_answer") as rec:
        out = answer_eval.evaluate_gap_answer(dispatch_id=42, user_id=7, index=99,
                                              answer="ausführliche", attempt=1)
    assert out is None
    assert rec.call_count == 0


def test_trenirovka_pishet_kazhdiy_raund():
    """Дыра №1: тренажёр играется в браузере, и до 13.09.2026 его ответы не сохранялись.
    Клиент шлёт список раундов в конце — каждый обязан стать строкой."""
    with mock.patch("backend.database.get_trainer_dispatch_by_id", return_value=DISPATCH), \
         mock.patch("backend.database.get_sprint_trainer_item", return_value=ITEM), \
         mock.patch("backend.database.record_relation_answer") as rec:
        written = answer_eval.record_trainer_round(dispatch_id=42, user_id=7, rounds=[
            {"word": "ausführlich", "picked": "ausführlich", "correct": True},
            {"word": "gründlich", "picked": "oberflächlich", "correct": False},
            {"word": "", "picked": "", "correct": True},          # мусор — не пишем
        ])
    assert written == 2 and rec.call_count == 2
    kinds = {c.kwargs["kind"] for c in rec.call_args_list}
    outcomes = [c.kwargs["outcome"] for c in rec.call_args_list]
    assert kinds == {"tr"}
    assert outcomes == ["correct", "wrong"]
