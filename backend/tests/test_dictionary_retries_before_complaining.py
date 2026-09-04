# -*- coding: utf-8 -*-
"""Моргнула база — повторяем под капотом. Лежит — говорим честно.

Владелец 03.09.2026: «если мы видим, что база моргнула, почему не дослать запрос ещё
раз? Человек знать этого не может, а мы-то знаем, моргнула база или нет. И уже если
несколько раз не смогли — тогда да, сказать, что база недоступна».

Это НЕ фолбэк из правила ноль: мы не подменяем ответ ничем другим, а спрашиваем то же
самое ещё раз. Правило прямо это разрешает: «Ретрай сети с ЧЕСТНЫМ финальным падением —
не fallback».

Тест держит обе половины: и повтор, и честное падение. Без второй половины повтор
превратился бы в бесконечное «сейчас-сейчас», без первой — человек видел бы жалобу на
каждое моргание.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import backend.database as database  # noqa: E402
import backend.dictionary_entries as de  # noqa: E402


class _Соединение:
    """Подделка: падает заданное число раз, потом отдаёт пустую выборку."""

    def __init__(self, счётчик, падать_раз):
        self.счётчик = счётчик
        self.падать_раз = падать_раз

    def __enter__(self):
        self.счётчик["попыток"] += 1
        if self.счётчик["попыток"] <= self.падать_раз:
            raise OSError("связь моргнула")
        return self

    def __exit__(self, *_):
        return False

    def cursor(self):
        return self

    def execute(self, *_a, **_k):
        return None

    def fetchall(self):
        return []

    def fetchone(self):
        return None


def _подставить(monkeypatch, падать_раз):
    счётчик = {"попыток": 0}
    monkeypatch.setattr(database, "get_db_connection_context",
                        lambda *a, **k: _Соединение(счётчик, падать_раз))
    monkeypatch.setattr(de, "_ПАУЗА_СЕК", 0.0)   # в тесте не спим
    monkeypatch.setattr(de, "СПАСЕНО_ПОВТОРОМ", 0, raising=False)
    return счётчик


# Внутри ОДНОЙ попытки слой открывает связь дважды: сначала свои единицы, потом
# справочник форм («чья это форма»). Поэтому счётчик считает ОТКРЫТИЯ, а не попытки —
# сверяться с ним числом попыток нельзя. Поймано этим же тестом 03.09.2026.
def test_одно_моргание_человек_не_замечает(monkeypatch):
    """Первая попытка упала, следующая ответила — наружу обычный ответ, без жалобы."""
    счётчик = _подставить(monkeypatch, падать_раз=1)
    assert de.entries_for_query("Kugel", source_lang="de", target_lang="ru") == []
    assert счётчик["попыток"] > 1, "повтор обязан был случиться"
    assert de.СПАСЕНО_ПОВТОРОМ == 1, "случай, когда повтор выручил, обязан считаться"


def test_два_моргания_подряд_тоже_переживаем(monkeypatch):
    _подставить(monkeypatch, падать_раз=2)
    assert de.entries_for_query("Kugel", source_lang="de", target_lang="ru") == []
    assert de.СПАСЕНО_ПОВТОРОМ == 1


def test_база_лежит_говорим_честно(monkeypatch):
    """Повтор не превращается в бесконечное «сейчас-сейчас»: три попытки и правда."""
    счётчик = _подставить(monkeypatch, падать_раз=99)
    with pytest.raises(de.DictionaryLayerUnavailable):
        de.entries_for_query("Kugel", source_lang="de", target_lang="ru")
    assert счётчик["попыток"] == de._ПОПЫТОК, "попыток ровно столько, сколько назначено"
    assert de.СПАСЕНО_ПОВТОРОМ == 0, "тут повтор никого не выручил"


def test_повтор_не_трогает_запросы_без_базы(monkeypatch):
    """Негодный запрос отсекается ДО базы — за него не платим ни одной попыткой."""
    счётчик = _подставить(monkeypatch, падать_раз=0)
    assert de.entries_for_query("", source_lang="de", target_lang="ru") == []
    assert de.entries_for_query("две слова", source_lang="de", target_lang="ru") == []
    assert счётчик["попыток"] == 0
