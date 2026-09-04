# -*- coding: utf-8 -*-
"""Экран проверки слов: обвиняешь — предлагай. И не зови человека попусту.

ЧТО СЛОМАЛОСЬ, 04.09.2026. Владелец открыл экран проверки и увидел карточку
«aufknuspern» с надписью «Похоже, при сохранении слово потеряло часть букв» — и
без единого варианта рядом. «Ну а где было вот так — предлагаю вот так?»

Разбор дал три дефекта, и здесь закреплён каждый:

  1. ВАРИАНТ ВЫБРАСЫВАЛСЯ. Дверь слова спрашивала модель, та отвечала «слово есть,
     но пишется иначе» — и это написание умирало прямо в двери: в базу шла одна
     пометка «что-то предлагала». Замер того дня: 6 слов в журнале с этой пометкой
     и ни одной подсказки. Теперь вариант кладётся в `bt_3_word_suggestion`.
  2. НАДПИСЬ ДОДУМЫВАЛА ПРИЧИНУ. «Потеряло часть букв» писалось на каждый ответ
     модели «такого слова нет». У «aufknuspern» букв не терялось: слово сохранено
     01.09 целиком, с переводом «разгрызть». Теперь про потерянные буквы говорим
     ТОЛЬКО когда рядом стоит целое слово.
  3. ЭКРАН ЗВАЛ ПОПУСТУ. Класс «модель: слово есть, справочник не знает» — это не
     сомнение, а устройство немецкого: сложные слова склеиваются бесконечно, и
     справочники их все не держат. Замер: 13 из 16 карточек на экране владельца
     были этого класса. Решение владельца 04.09.2026 — не показывать их вовсе.
"""
from __future__ import annotations

import inspect

import pytest

import backend.german_word_gate as G
from backend import word_confirm_digest as D


# ── 1. Вариант, который модель уже назвала, не выбрасывается ─────────────────
def test_дверь_кладёт_предложенное_написание_в_подсказки(monkeypatch):
    monkeypatch.setattr(G, "_cached", lambda asked: None)
    monkeypatch.setattr(G, "_remember", lambda asked, verdict: None)
    monkeypatch.setattr(G, "_known_by_our_data", lambda word: (False, "", ""))
    monkeypatch.setattr(G, "_second_reference_says", lambda words: {})
    monkeypatch.setattr(G, "_reference_says_about_all", lambda words: {})
    monkeypatch.setattr("backend.german_reference_forms.word_exists_by_model",
                        lambda w: {"existiert": True, "sprache": "de",
                                   "wortart": "Substantiv", "korrekt": "Quecksilber"})
    записано: dict[str, str] = {}
    monkeypatch.setattr(G, "remember_suggestion",
                        lambda asked, suggestion: записано.setdefault(asked, suggestion))

    verdict = G.check_word("Quicksilber")

    assert verdict["status"] == G.UNCONFIRMED, "слово остаётся как было — решает человек"
    assert записано == {"Quicksilber": "Quecksilber"}, (
        "написание, которое модель уже назвала и за которое уже заплачено, "
        "обязано доехать до экрана — иначе человеку опять нечего нажать")


# ── 2. Один спрос, а не два ──────────────────────────────────────────────────
def test_подсказка_написания_спрашивается_один_раз(monkeypatch):
    """Решение владельца 04.09.2026: два спроса одной модели — не проверка."""
    спросов = {"n": 0}

    def ask(task, word):
        спросов["n"] += 1
        return {"gemeint": "die Abschiebung"}

    monkeypatch.setattr("backend.german_reference_forms._ask_once", ask)
    monkeypatch.setattr("backend.openai_manager.system_message", {})

    assert G.suggest_spelling("Abschiebu") == "die Abschiebung"
    assert спросов["n"] == 1, "второй спрос вернулся — это удвоенный счёт на ровном месте"


def test_модель_не_восстановила_значит_подсказки_нет(monkeypatch):
    """Пустой ответ — честное «не знаю», а не повод выдумывать."""
    monkeypatch.setattr("backend.german_reference_forms._ask_once",
                        lambda task, word: {"gemeint": ""})
    monkeypatch.setattr("backend.openai_manager.system_message", {})
    assert G.suggest_spelling("Painka") == ""


# ── 3. Надпись говорит ровно то, что мы знаем ────────────────────────────────
def test_без_варианта_не_обвиняем_в_потерянных_буквах():
    надпись = D._human_reason("не слово", "модель: такого слова нет", "")
    assert "потеря" not in надпись and "не целиком" not in надпись, (
        "причину додумали: модель сказала только «такого слова нет»")
    assert "не смогли" in надпись


def test_с_вариантом_говорим_про_целое_слово():
    надпись = D._human_reason("не слово", "модель: такого слова нет", "die Abschiebung")
    assert "не целиком" in надпись


def test_спорное_написание_объясняется_отдельно():
    источник = "модель предложила другое написание, справочник не подтвердил"
    с_вариантом = D._human_reason("не подтверждено", источник, "das Quecksilber")
    без = D._human_reason("не подтверждено", источник, "")
    assert "спорн" in с_вариантом.lower() and "спорн" in без.lower(), (
        "класс попадал в общее «мы не нашли это слово» и выглядел как приговор")


# ── 4. Настоящее слово не зовёт человека на экран ────────────────────────────
ТРИ_ВЫБОРКИ = (D.words_for_user, D.audit_items, D.send_word_audit_reminders)


@pytest.mark.parametrize("функция", ТРИ_ВЫБОРКИ, ids=lambda f: f.__name__)
def test_класс_слово_настоящее_не_идёт_человеку(функция):
    """Список экрана, список письма и подсчёт получателей — фильтр во всех трёх.

    Разойдутся они — и письмо снова позовёт на экран, где показывать нечего:
    ровно этот дефект чинили 26.08.2026 и 04.09.2026.
    """
    src = inspect.getsource(функция)
    assert "не_спрашиваем" in src, (
        f"{функция.__name__} снова зовёт человека из-за слова, "
        "которое модель уже признала настоящим")


def test_английское_слово_с_экрана_не_убрано():
    """Решение владельца 04.09.2026: остаются обрубки, чужие языки, спорное написание."""
    assert "язык en" not in D._НЕ_СПРАШИВАЕМ
    assert D._НЕ_СПРАШИВАЕМ == "w.source <> 'модель: слово есть, справочник не знает'"
    assert "английск" in D._human_reason("не подтверждено",
                                         "модель: слово есть, язык en").lower()
