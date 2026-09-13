# -*- coding: utf-8 -*-
"""Добор банка аудирования обязан СЧИТАТЬ удачу удачей.

Зачем этот тест существует
──────────────────────────
С 05.06.2026 по 13.09.2026 `generate_listening_entry` падала с NameError на строке
журнала СРАЗУ ПОСЛЕ записи в банк: коммит 7060fa07 убрал присвоение `audio_status`,
а упоминание в `logging.info` оставил. Снаружи это выглядело так:

  • запись в банке ЕСТЬ — работа сделана;
  • заказчик `prepare_listening_pool` считает её провалом, `succeeded` не растёт;
  • цикл не может добрать заказ никогда и крутится до упора `max_attempts` —
    десять запросов к GPT вместо нужных двух-трёх, каждую ночь добора;
  • отчёт владельцу пишет «сделано 0, провалено 10» при легших в базу записях.

Тест держит именно КЛАСС, а не одну строку: он проходит всю функцию целиком, поэтому
любое неопределённое имя, любое падение после записи снова сделают его красным.
Проверка «succeeded == нужное И attempted == нужное» ловит обе половины дефекта:
и несчитанную удачу, и лишние платные попытки.
"""
from __future__ import annotations

import pytest

from backend import listening_generator as lg


def _fake_generated() -> dict:
    return {
        "german_text": (
            "Guten Tag, hier ist die Praxis Doktor Berger. "
            "Ihr Termin am Dienstag um halb neun wurde auf Donnerstag verschoben. "
            "Bitte bringen Sie Ihre Versichertenkarte und den alten Befund mit. "
            "Wenn Sie erst nach sechzehn Uhr können, rufen Sie bis Mittwoch zurück. "
            "Für Patienten mit Privatversicherung gilt die Ausnahme nicht."
        ),
        "questions": [
            {"number": i, "question_de": f"Frage {i}?", "correct_answer_de": f"Antwort {i}."}
            for i in range(1, 5)
        ],
    }


@pytest.fixture
def bank(monkeypatch):
    """Подменяем ТОЛЬКО внешний мир: модель, базу и паузу. Сама функция идёт целиком."""
    saved: list[dict] = []

    def _fake_upsert(**kwargs):
        saved.append(kwargs)

    monkeypatch.setattr(lg, "_call_gpt_generate", lambda topic: _fake_generated())
    monkeypatch.setattr(lg.time, "sleep", lambda *_a, **_k: None)

    from backend import database as db
    monkeypatch.setattr(db, "upsert_listening_bank_entry", _fake_upsert)
    monkeypatch.setattr(db, "count_listening_bank_entries", lambda **_k: 0)
    return saved


def test_generated_entry_does_not_blow_up_after_it_is_saved(bank):
    """Запись легла — значит функция ОБЯЗАНА вернуть id, а не упасть следом."""
    listening_id = lg.generate_listening_entry(topic_id="telefonat")

    assert listening_id, "функция не вернула listening_id"
    assert len(bank) == 1, f"в банк ушла не одна запись, а {len(bank)}"
    assert bank[0]["listening_id"] == listening_id
    # Состояние озвучки в журнале и в базе — одна и та же величина, разъехаться нечему.
    assert bank[0]["audio_status"] == "ready"


def test_pool_stops_as_soon_as_the_order_is_filled(bank):
    """Добрали заказ — остановились. Лишняя попытка здесь стоит денег."""
    stats = lg.prepare_listening_pool(target_ready=3, max_attempts=10)

    assert stats["needed"] == 3
    assert stats["succeeded"] == 3, f"удача не засчитана: {stats}"
    assert stats["failed"] == 0, f"удачная генерация записана в провал: {stats}"
    assert stats["attempted"] == 3, (
        f"заказ на 3 стоил {stats['attempted']} запросов к модели — это деньги: {stats}"
    )
    assert len(bank) == 3
