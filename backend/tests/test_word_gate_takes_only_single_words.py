# -*- coding: utf-8 -*-
"""Дверь слова разбирает ОДНО СЛОВО. Фразу и предложение она не берёт вовсе.

ЖАЛОБА ВЛАДЕЛЬЦА 01.09.2026, дословно:

    «Приходят слова и пишут, что модель не знает таких слов — ну конечно не знает,
    потому что это ПРЕДЛОЖЕНИЯ. Но модель же знает, как это перевести, почему мне обман
    приходит о том, что мы не знаем, что это такое?! Зачем эти предложения попадаются в
    разбор слов?!»

ЧТО БЫЛО. Живая база 01.09.2026: в очереди к владельцу 79 записей, 54 из них
многословные — две трети рассылки. У 47 из 59 многословных строка `bt_3_word_check`
появлялась через секунды после решения по фразе (`bt_3_phrase_review.decided_text`
совпадал дословно). Круг замыкался сам на себя:

    владелец правит фразу на экране спорных фраз
      → `database.apply_phrase_review_decision`
      → `card_complaints.подчистить_после_переименования`
      → `_вернуть_в_круг_после_переименования` спрашивает дверь слова про ВЕСЬ текст
      → модель: «такого слова нет»
      → владельцу личка «убрать из словаря?»

Проверка «внутри пробел — не наше дело» стояла у восьми вызывающих из девяти. Копия
правила у каждого вызывающего ломается на следующем — поэтому правило переехало в саму
дверь, и эти тесты держат его там.

ЭТО НЕ МОЛЧАНИЕ. Фраза остаётся проверенной: её грамматику разбирает
`backend/phrase_night_check.py` → `bt_3_phrase_check` → очередь `bt_3_phrase_review`.
Дверь слова отказывается отвечать на вопрос, которого никто не задавал, — «существует ли
в немецком слово „Es löst Kopfschütteln aus.“».
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.german_word_gate import (  # noqa: E402
    NOT_A_WORD_QUESTION,
    OWNER_STATUSES,
    check_word,
    is_single_word,
)

ФРАЗЫ = [
    "Es löst Kopfschütteln aus.",                                # из скриншота владельца
    "Man schließt sich der Reihe fest hinter Trump an",
    "jemanden der Datenfälschung beschuldigen",
    "Sich gekränkt fühlen",
    "ist ein Zungenbrecher",
    "Ich verstehe kein Wort!",
    "dies und das",
]

СЛОВА = [
    "Hammer",
    "die Fahne",          # определённый артикль — это наш формат заголовка
    "eine Schnapsidee",   # неопределённый снимается правилом заголовка
    "ein Türgriff",
    "rumwühlen",
    "Mainstreamwissen",
]


@pytest.mark.parametrize("фраза", ФРАЗЫ)
def test_фраза_не_считается_словом(фраза):
    assert is_single_word(фраза) is False, f"{фраза!r} — это не одно слово"


@pytest.mark.parametrize("слово", СЛОВА)
def test_слово_считается_словом(слово):
    assert is_single_word(слово) is True, f"{слово!r} — это одно слово"


@pytest.mark.parametrize("фраза", ФРАЗЫ)
def test_дверь_не_выносит_приговор_фразе(фраза, monkeypatch):
    """Ни справочника, ни модели, ни кеша: отказ раньше любого источника."""
    import backend.german_word_gate as дверь

    def нельзя(*_а, **_к):
        raise AssertionError("дверь пошла к источнику по ФРАЗЕ — этого не должно быть")

    monkeypatch.setattr(дверь, "_cached", нельзя)
    monkeypatch.setattr(дверь, "_remember", нельзя)
    monkeypatch.setattr(дверь, "_decide", нельзя)

    вердикт = check_word(фраза)
    assert вердикт["status"] == NOT_A_WORD_QUESTION
    # Текст возвращается КАК СПРОШЕНО: вызывающие переименовывают запись при
    # `text != asked`, и подменять здесь нечего.
    assert вердикт["text"] == фраза


@pytest.mark.parametrize("фраза", ФРАЗЫ)
def test_фраза_не_попадает_в_очередь_владельца(фраза):
    """Главное следствие: такой ответ не имеет статуса, по которому идёт рассылка."""
    assert check_word(фраза)["status"] not in OWNER_STATUSES


def test_ночное_применение_приговоров_фразу_не_переспрашивает(monkeypatch):
    """`word_gate_apply` зовёт `_decide` МИМО двери — сторож нужен и там.

    Без него строка-фраза, попавшая в таблицу раньше, каждую ночь снова уходила бы к
    модели и снова всплывала бы у владельца со снятой отметкой `reviewed`.
    """
    import backend.word_gate_apply as применение

    monkeypatch.setattr(применение, "ПАУЗА_СЕК", 0)
    monkeypatch.setattr(
        "backend.german_word_gate._decide",
        lambda *_а, **_к: (_ for _ in ()).throw(
            AssertionError("переспросили фразу у модели")),
    )
    шаги = применение.переспросить(
        [("Es löst Kopfschütteln aus.", "Es löst Kopfschütteln aus.", "не слово", "")],
        переспрашивать=True,
    )
    assert шаги[0]["действие"] == "не одно слово — вопрос не к двери слова"


def test_ответы_разошлись_не_ложатся_в_таблицу(monkeypatch):
    """«Ответы разошлись» — это «модель не ответила», а не вердикт.

    Сама дверь такое не кеширует (`_is_final`), а ночное переписывание клало его в
    таблицу в обход — да ещё и со снятой отметкой `reviewed`, то есть вопрос уходил
    владельцу заново. Замер 01.09.2026: так пришли 4 записи из 79.
    """
    import backend.word_gate_apply as применение

    monkeypatch.setattr(применение, "ПАУЗА_СЕК", 0)
    monkeypatch.setattr(
        применение, "_переписать_приговор",
        lambda *_а, **_к: (_ for _ in ()).throw(
            AssertionError("неответ модели записан в таблицу")),
    )
    monkeypatch.setattr(
        "backend.german_word_gate._decide",
        lambda *_а, **_к: {"text": "Arbeitsumfeld", "status": "не подтверждено",
                           "pos": "", "source": "ответы разошлись", "note": "спросим позже"},
    )
    шаги = применение.переспросить(
        [("Arbeitsumfeld", "Arbeitsumfeld", "не слово", "noun")], переспрашивать=True)
    assert шаги[0]["действие"] == "источник молчал — отложено"
