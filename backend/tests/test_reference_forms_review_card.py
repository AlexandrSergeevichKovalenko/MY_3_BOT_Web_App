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


НАША_ЗАПИСЬ = {"unit_id": 7, "перевод": "ходьба", "шапка": "das Gehen",
               "множественное": "", "родительный": "des Gehens", "сравнительная": ""}


def _текст(item, monkeypatch, diagnosis=("нет_таблицы", "страница есть, таблицы нет")):
    monkeypatch.setattr(RV, "_our_entry", lambda word: dict(НАША_ЗАПИСЬ))
    return RV._word_text(item, index=1, total=2, left=2, diagnosis=diagnosis)


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
    клавиатура = RV._keyboard(42, 2, можно_убрать=True)
    данные = [b["callback_data"] for row in клавиатура["inline_keyboard"] for b in row]
    assert "reffrm:v1:42" in данные and "reffrm:v2:42" in данные
    assert "reffrm:fix:42" in данные and "reffrm:keep:42" in данные
    assert all(d.split(":")[2].isdigit() for d in данные)


def test_артикля_в_кнопках_больше_нет():
    """der/die/das собирает СВОЙ механизм (article_review) — и он работает.
    Здесь эта кнопка ничего не записывала и записать не могла."""
    все = [b["text"] for row in RV._keyboard(42, 2, можно_убрать=True)["inline_keyboard"]
           for b in row]
    assert not {"der", "die", "das"} & set(все)


def test_когда_предлагать_нечего_карточка_говорит_это_прямо(monkeypatch):
    текст = _текст(БЕЗ_ПРЕДЛОЖЕНИЙ, monkeypatch)
    assert "Модель тоже ничего не предложила" in текст
    кнопки = [b["text"] for row in RV._keyboard(43, 0, можно_убрать=False)["inline_keyboard"]
              for b in row]
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


def test_оставить_как_есть_закрывает_вопрос_и_обещает_машинный_переспрос(monkeypatch):
    закрыто = []
    monkeypatch.setattr("backend.german_reference_forms.unresolved_row",
                        lambda rid: dict(СЛОВО))
    monkeypatch.setattr("backend.german_reference_forms.mark_reviewed",
                        lambda rid, reason="": закрыто.append((rid, reason)) or True)
    ответ = RV.apply_reference_forms_review("keep", 42)
    assert закрыто and "оставить" in закрыто[0][1]
    assert "переспрошу сам" in ответ


def test_разобраться_заводит_жалобу_а_не_второй_редактор(monkeypatch):
    """Правка заголовка живёт в разборе карточки — там уже есть экран «было → станет»
    и разгон правки по всем местам. Второго редактора в проекте быть не должно."""
    заведено = []
    monkeypatch.setattr("backend.german_reference_forms.unresolved_row",
                        lambda rid: dict(СЛОВО))
    monkeypatch.setattr(RV, "_our_entry", lambda word: dict(НАША_ЗАПИСЬ))
    monkeypatch.setattr("backend.card_complaints.add_complaint",
                        lambda **kw: заведено.append(kw) or {"ok": True, "id": 5})
    monkeypatch.setattr("backend.german_reference_forms.mark_reviewed",
                        lambda rid, reason="": True)
    ответ = RV.apply_reference_forms_review("fix", 42, 777)
    assert заведено and заведено[0]["word"] == "Gehen" and заведено[0]["unit_id"] == 7
    assert заведено[0]["user_id"] == 777
    assert "было → станет" in ответ


def test_несостоявшаяся_жалоба_не_закрывает_слово(monkeypatch):
    monkeypatch.setattr("backend.german_reference_forms.unresolved_row",
                        lambda rid: dict(СЛОВО))
    monkeypatch.setattr(RV, "_our_entry", lambda word: dict(НАША_ЗАПИСЬ))
    monkeypatch.setattr("backend.card_complaints.add_complaint",
                        lambda **kw: {"ok": False, "reason": "error"})
    закрыто = []
    monkeypatch.setattr("backend.german_reference_forms.mark_reviewed",
                        lambda rid, reason="": закрыто.append(rid) or True)
    ответ = RV.apply_reference_forms_review("fix", 42, 777)
    assert "осталось в очереди" in ответ and закрыто == []


def test_убрать_показывается_только_когда_написание_не_подтверждено():
    """Слово настоящее, не хватает лишь таблицы форм — удалять его нельзя."""
    для_настоящего = [b["text"] for row in RV._keyboard(1, 0, можно_убрать=False)["inline_keyboard"]
                      for b in row]
    для_кривого = [b["text"] for row in RV._keyboard(1, 0, можно_убрать=True)["inline_keyboard"]
                   for b in row]
    assert not any("убрать" in t for t in для_настоящего)
    assert any("убрать" in t for t in для_кривого)


def test_карточка_называет_точную_причину_а_не_общую_фразу(monkeypatch):
    """«Справочник их не печатает» на странице, где таблица есть, — это враньё.
    Владелец открыл «Finster», увидел склонение фамилии и назвал это обманом."""
    текст = _текст(БЕЗ_ПРЕДЛОЖЕНИЙ, monkeypatch,
                   diagnosis=("только_фамилия", "справочник знает это написание только "
                                                "как ФАМИЛИЮ"))
    assert "ФАМИЛИЮ" in текст
    assert "У нас записано" in текст and "das Gehen" in текст


def test_нечитаемая_очередь_не_выглядит_пустой(monkeypatch):
    """«Разбирать нечего 🎉» и «не смог посчитать» — два разных мира. Счётчик
    возвращает -1, когда база не ответила, и это НЕ ноль."""
    monkeypatch.setattr("backend.database.claim_scheduler_run_guard",
                        lambda **kw: True, raising=False)
    monkeypatch.setattr("backend.database.get_admin_telegram_ids", lambda: [1], raising=False)
    monkeypatch.setattr("backend.german_reference_forms.unresolved_batch",
                        lambda limit=20: [])
    monkeypatch.setattr("backend.german_reference_forms.unresolved_count", lambda: -1)
    monkeypatch.setenv("TELEGRAM_Deutsch_BOT_TOKEN", "тест")
    итог = RV.send_reference_forms_review_dm(force=True)
    assert итог["ok"] is False and "очередь" in итог["error"]


def test_точная_причина_доезжает_до_жалобы(monkeypatch):
    """Причина считается перед отправкой; если её не записать, дальше — в жалобу, в
    отчёт, к модели-судье — уйдёт старая общая фраза. Так и вышло с «Finster»."""
    записано = []
    monkeypatch.setattr("backend.database.claim_scheduler_run_guard",
                        lambda **kw: True, raising=False)
    monkeypatch.setattr("backend.database.get_admin_telegram_ids", lambda: [1], raising=False)
    monkeypatch.setattr("backend.german_reference_forms.unresolved_batch",
                        lambda limit=20: [{"id": 9, "word": "Finster", "pos": "noun",
                                           "reason": "ни справочник, ни композит, ни модель",
                                           "candidates": []}])
    monkeypatch.setattr("backend.german_reference_forms.unresolved_count", lambda: 1)
    monkeypatch.setattr("backend.german_reference_forms.fetch_sources_bulk",
                        lambda titles: {"Finster": "{{Wortart|Substantiv|Deutsch}}"
                                                   "{{Wortart|Nachname|Deutsch}}"
                                                   "{{Deutsch Nachname Übersicht|x=1}}"})
    monkeypatch.setattr("backend.german_reference_forms.store_reason",
                        lambda rid, reason: записано.append((rid, reason)))
    monkeypatch.setattr("backend.german_reference_forms.mark_asked", lambda ids: None)
    monkeypatch.setattr(RV, "_our_entry", lambda word: {})
    monkeypatch.setattr(RV.requests, "post",
                        lambda *a, **kw: type("Ответ", (), {"status_code": 200, "text": ""})())
    monkeypatch.setenv("TELEGRAM_Deutsch_BOT_TOKEN", "тест")
    RV.send_reference_forms_review_dm(force=True)
    assert записано and записано[0][0] == 9
    assert "ФАМИЛИИ" in записано[0][1], "в очередь обязана лечь ТОЧНАЯ причина"
