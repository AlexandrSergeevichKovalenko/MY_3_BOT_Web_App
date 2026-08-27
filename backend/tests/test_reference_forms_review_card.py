# -*- coding: utf-8 -*-
"""Карточка вопроса о формах: она обязана СКАЗАТЬ, что решают и что будет после.

Повод (владелец, 26.08.2026, о прежней карточке): «Что я должен принять? В чём задача?
Где подсказка модели относительно того, что она предлагает?» На экране были слово,
строка «форм нет нигде» и кнопки der/die/das, которые ни разу не смогли ничего
записать. Эти тесты не дают такому вернуться.
"""
from __future__ import annotations

import backend.reference_forms_review as RV

СЛОВО = {
    "id": 42,
    "word": "Gehen",
    "pos": "noun",
    "reason": "модель предложила разное",
    "candidates": [
        {"nom_sg": "das Gehen", "gen_sg": "des Gehens", "dat_sg": "dem Gehen",
         "akk_sg": "das Gehen", "nom_pl": "", "gen_pl": "", "dat_pl": "", "akk_pl": ""},
        {"nom_sg": "das Gehen", "gen_sg": "des Gehen", "dat_sg": "dem Gehen",
         "akk_sg": "das Gehen", "nom_pl": "die Gehen", "gen_pl": "der Gehen",
         "dat_pl": "den Gehen", "akk_pl": "die Gehen"},
    ],
}

БЕЗ_ПРЕДЛОЖЕНИЙ = {"id": 43, "word": "Bierhausschwätzer", "pos": "noun",
                   "reason": "ни справочник, ни композит, ни модель", "candidates": []}


def _текст(item, monkeypatch):
    monkeypatch.setattr(RV, "_translation", lambda word: "ходьба")
    return RV._word_text(item, index=1, total=2, left=2)


def test_в_карточке_видно_что_предлагает_модель(monkeypatch):
    текст = _текст(СЛОВО, monkeypatch)
    assert "des Gehens" in текст and "des Gehen" in текст, "оба варианта обязаны быть видны"
    assert "①" in текст and "②" in текст


def test_в_карточке_видно_чем_варианты_расходятся(monkeypatch):
    текст = _текст(СЛОВО, monkeypatch)
    assert "Расходятся" in текст
    assert "род." in текст, "расхождение в родительном падеже названо прямо"


def test_в_карточке_есть_перевод_и_последствие_нажатия(monkeypatch):
    текст = _текст(СЛОВО, monkeypatch)
    assert "ходьба" in текст, "человек должен понимать, о каком слове речь"
    assert "карточк" in текст, "сказано, куда попадёт выбранное"
    assert "de.wiktionary.org" in текст, "дана статья справочника"


def test_кнопки_носят_номер_строки_а_не_обрезанное_слово():
    клавиатура = RV._keyboard(42, 2)
    данные = [b["callback_data"] for row in клавиатура["inline_keyboard"] for b in row]
    assert "reffrm:v1:42" in данные and "reffrm:v2:42" in данные
    assert "reffrm:later:42" in данные
    assert all(d.split(":")[2].isdigit() for d in данные)


def test_артикля_в_кнопках_больше_нет():
    """der/die/das собирает СВОЙ механизм (article_review) — и он работает.
    Здесь эта кнопка ничего не записывала и записать не могла."""
    все = [b["text"] for row in RV._keyboard(42, 2)["inline_keyboard"] for b in row]
    assert not {"der", "die", "das"} & set(все)


def test_когда_предлагать_нечего_карточка_говорит_это_прямо(monkeypatch):
    текст = _текст(БЕЗ_ПРЕДЛОЖЕНИЙ, monkeypatch)
    assert "Модель тоже не ответила" in текст
    кнопки = [b["text"] for row in RV._keyboard(43, 0)["inline_keyboard"] for b in row]
    assert any("негодный заголовок" in t for t in кнопки)
    assert not any("вариант" in t for t in кнопки)


def test_несостоявшаяся_запись_не_убирает_слово_из_очереди(monkeypatch):
    """Прежний код помечал слово разобранным ДАЖЕ когда запись падала, и писал при
    этом «слово осталось в очереди». Записи не было ни одной за всё время."""
    monkeypatch.setattr("backend.german_reference_forms.unresolved_row",
                        lambda rid: dict(СЛОВО))
    monkeypatch.setattr("backend.german_reference_forms.apply_owner_choice",
                        lambda rid, variant: None)
    помечено = []
    monkeypatch.setattr("backend.german_reference_forms.mark_headword_defect",
                        lambda w, p, why: помечено.append(w))
    ответ = RV.apply_reference_forms_review("v1", 42)
    assert "не смог записать" in ответ and "ОСТАЁТСЯ в очереди" in ответ
    assert помечено == [], "слово не должно быть тихо закрыто"


def test_успешная_запись_говорит_человеку_что_изменилось(monkeypatch):
    monkeypatch.setattr("backend.german_reference_forms.unresolved_row",
                        lambda rid: dict(СЛОВО))
    monkeypatch.setattr("backend.german_reference_forms.apply_owner_choice",
                        lambda rid, variant: dict(СЛОВО))
    ответ = RV.apply_reference_forms_review("v1", 42)
    assert "склонение" in ответ and "карточке слова" in ответ


def test_отложить_откладывает_а_не_хоронит(monkeypatch):
    отложено = []
    monkeypatch.setattr("backend.german_reference_forms.unresolved_row",
                        lambda rid: dict(СЛОВО))
    monkeypatch.setattr("backend.german_reference_forms.postpone_unresolved",
                        lambda rid, days: отложено.append((rid, days)) or True)
    ответ = RV.apply_reference_forms_review("later", 42)
    assert отложено and отложено[0][1] > 0
    assert "вернётся" in ответ
