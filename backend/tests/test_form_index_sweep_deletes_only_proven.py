# -*- coding: utf-8 -*-
"""Ночная чистка указателей сносит только ДОКАЗАННОЕ и ОБЪЯСНЁННОЕ.

Сверка идёт каждую ночь сама, сразу после прогрева справочника, и удаляет строки из
живой базы. Значит её правило отбора обязано быть заперто тестом: один невнимательный
«ну и это тоже похоже на мусор» — и мы потеряем настоящие формы.

Что здесь охраняется (проверено на живых данных 01.09.2026):

  • «в таблице не напечатано» ≠ «не является формой». «gehauen» — настоящее причастие
    от hauen, но в таблице стоит только «gehaut»; «auszulaugen» — настоящий
    zu-инфинитив, строки для него в нашем разборе таблицы нет. Такие указатели
    остаются жить и в удаление НЕ попадают;
  • «durchziehe» начинается со слова «durchziehen», но это форма 1-го лица, а не
    приставка: отбор по началу строки запрещён, приставку называет разбор;
  • справочник молчит про глагол — судить нечем, указатель остаётся.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.lex_form_index_sweep import (  # noqa: E402
    CLASS_A,
    CLASS_B,
    CLASS_B2,
    CLASS_D,
    CONFIRMED,
    DELETABLE,
    NO_REF,
    build_owner_index,
    classify,
)

# Ячейки так, как они напечатаны на страницах Flexion (выборка 01.09.2026).
REFERENCE = {
    "gehen": {"gehe", "gehst", "geht", "ging", "gingst", "gegangen", "ist gegangen"},
    "zugehen": {"gehe zu", "gehst zu", "geht zu", "ging zu", "zugegangen"},
    "aufgeben": {"gebe auf", "gibst auf", "gibt auf", "gab auf", "aufgegeben"},
    "geben": {"gebe", "gibst", "gibt", "gab", "gegeben"},
    "durchziehen": {"durchziehe", "durchziehst", "durchzieht", "durchzog", "durchzogen"},
    "hauen": {"haue", "haust", "haut", "haute", "gehaut"},
}
OWNER_OF = build_owner_index(REFERENCE)
KNOWN = set(REFERENCE) | {"stellen", "fangen"}


def _verdict(surface, lemma):
    return classify(surface, lemma, REFERENCE, OWNER_OF, KNOWN)[0]


def test_форма_своего_глагола_подтверждается():
    assert _verdict("ging", "gehen") == CONFIRMED
    assert _verdict("gegangen", "gehen") == CONFIRMED


def test_голая_основа_на_приставочном_глаголе_сносится():
    """«ging» напечатано у «gehen»; у «zugehen» напечатано «ging zu»."""
    verdict, owner = classify("ging", "zugehen", REFERENCE, OWNER_OF, KNOWN)
    assert verdict == CLASS_B
    assert owner == "gehen"
    assert verdict in DELETABLE


def test_отделяемая_приставка_сносится():
    verdict, owner = classify("auf", "aufgeben", REFERENCE, OWNER_OF, KNOWN)
    assert verdict == CLASS_A
    assert owner == "auf"
    assert verdict in DELETABLE


def test_форма_похожая_на_приставку_НЕ_сносится():
    """«durchziehe» — начало слова «durchziehen», но это форма, а не приставка."""
    assert _verdict("durchziehe", "durchziehen") == CONFIRMED


def test_настоящая_форма_которой_нет_в_таблице_остаётся():
    """«gehauen» — настоящее причастие hauen, в таблице только «gehaut». Не трогаем."""
    verdict = _verdict("gehauen", "hauen")
    assert verdict not in DELETABLE
    assert verdict in (CLASS_D, CLASS_B2)


def test_молчание_справочника_не_повод_удалять():
    assert _verdict("wühlt", "wühlen") == NO_REF
    assert NO_REF not in DELETABLE


def test_недоказанное_никогда_не_в_списке_на_снос():
    """Главный замок: удалять можно ровно два класса, и ни одним больше."""
    assert set(DELETABLE) == {CLASS_A, CLASS_B}
    for verdict in (CONFIRMED, CLASS_B2, CLASS_D, NO_REF):
        assert verdict not in DELETABLE
