# -*- coding: utf-8 -*-
"""Сбой словаря — это не «такого слова нет».

Поймано 02.09.2026 на живом замере: слой статей вернул ПУСТОЙ список для слова
«Kugel», которое в базе есть, — и я чуть не объявил регресс продукта. Причина:
`except Exception: return []`.

Пустой список означает «мы честно не знаем этого слова», и по нему вызывающие идут
дальше: к машинному переводчику, к модели, к платному разбору. Пока тем же пустым
списком отвечал обрыв связи, обрыв ТИХО превращался в машинный перевод вместо
словарной статьи. Правило ноль про это прямо: «Пустой результат от ошибки НЕ ОТЛИЧИМ
от пустого результата от „данных правда нет“ — а это два разных мира».

Владелец 02.09.2026: «чинить».
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import backend.database as database  # noqa: E402
from backend.dictionary_entries import (  # noqa: E402
    DictionaryLayerUnavailable,
    entries_for_query,
)


class _ПадающееСоединение:
    def __enter__(self):
        raise OSError("связь с базой оборвалась")

    def __exit__(self, *_):
        return False


def test_обрыв_связи_называется_вслух(monkeypatch):
    """Не пустой список, а названный сбой — иначе обрыв читается как «слова нет»."""
    monkeypatch.setattr(database, "get_db_connection_context",
                        lambda *a, **k: _ПадающееСоединение())
    with pytest.raises(DictionaryLayerUnavailable):
        entries_for_query("Kugel", source_lang="de", target_lang="ru")


def test_сбой_не_маскируется_под_обычную_ошибку():
    """У сбоя свой тип: вызывающий должен уметь отличить его от чего угодно ещё."""
    assert issubclass(DictionaryLayerUnavailable, RuntimeError)


def test_негодный_запрос_это_честное_пусто_а_не_сбой():
    """Пустой список остаётся законным ответом «нам это неизвестно».

    Эти запросы отсекаются ДО обращения к базе, поэтому проверяются без неё:
    пустая строка, слишком длинная строка и фраза с пробелом."""
    assert entries_for_query("", source_lang="de", target_lang="ru") == []
    assert entries_for_query("x" * 65, source_lang="de", target_lang="ru") == []
    assert entries_for_query("guten Tag zusammen", source_lang="de", target_lang="ru") == []


def test_одинаковые_языки_это_тоже_честное_пусто():
    assert entries_for_query("Kugel", source_lang="de", target_lang="de") == []
