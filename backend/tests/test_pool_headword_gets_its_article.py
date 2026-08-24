# -*- coding: utf-8 -*-
"""Общий словарь показывает артикль — но только там, где источник за него ручается.

Откуда взялся этот тест
───────────────────────
24.08.2026, разбор карточки «Schnapsidee». Личная карточка собирает заголовок через
`compose_german_headword` и артикль показывает, а выдача ОБЩЕГО словаря собирала его как
«что лежит, то и показываем». Одно и то же слово выглядело на двух экранах по-разному:
человек искал слово и видел «Kosten» вместо «die Kosten».

Замер того дня: 1871 заголовок выдачи пула стоял без артикля.

Почему артикль ставится не всем
───────────────────────────────
Сырое число «у 306 род известен, ставим всем» — неверное, и три ловушки к нему поймала
соседняя сессия на живых данных. Тест сторожит каждую, потому что каждая уже случалась:

1. **Форма множественного.** У «Türen» таблица найдётся по ключу «tür», и её «die»
   относится к «die Tür». Приклеить артикль единственного к множественному — ошибка.
2. **Не существительное.** «gehen», «wenn», «vier» лежали с pos='noun'. Справочник на них
   честно отвечает «das Gehen», «das Wenn», «die Vier»: такие существительные ЕСТЬ.
   Источник прав, вопрос неверен.
3. **Слабое подтверждение части речи.** `pos_source='wiktionary'` отвечает на вопрос
   «существует ли существительное с таким написанием», а не «про существительное ли эта
   карточка». Отсюда «die Manche» у карточки «некоторые» и «das Eigen» у «собственный».

Плюс четвёртая, наша: если наш род спорит с источником — не показываем ни один.
Выбирать между двумя ответами значит угадывать, а это запрещено правилом ноль.
"""
from __future__ import annotations

import pytest

from backend import database as db


@pytest.fixture
def справочник(monkeypatch):
    """Подменяет ОБА источника: таблицы склонения и слой единиц. Тест проверяет правило,
    а не наличие боевой базы."""
    def подставить(*, склонения: dict, единицы: dict):
        monkeypatch.setattr(
            "backend.noun_declension_reference.articles_from_declension_reference",
            lambda words: {
                w: (склонения.get(w), "справочник склонений" if склонения.get(w) else "не знает")
                for w in words
            },
        )

        class _Cursor:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def execute(self, sql, params=None):
                self._rows = [
                    (key, *единицы[key]) for key in
                    (params[0] if params else []) if key in единицы
                ]
            def fetchall(self): return self._rows

        class _Conn:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def cursor(self): return _Cursor()

        monkeypatch.setattr(db, "get_db_connection_context", lambda *a, **k: _Conn())
    return подставить


def _заголовки(*слова):
    return [{"display_word": w} for w in слова]


class TestАртикльПоявляется:
    def test_существительное_с_надёжной_частью_речи_получает_артикль(self, справочник):
        справочник(
            склонения={"Wandel": "der", "Auffassung": "die", "Zugeständnis": "das"},
            единицы={"wandel": ("noun", "card", "der"),
                     "auffassung": ("noun", "пул", "die"),
                     "zugeständnis": ("noun", "справочник", "das")},
        )
        items = _заголовки("Wandel", "Auffassung", "Zugeständnis")
        db._attach_pool_articles(items)
        assert [i["display_word"] for i in items] == [
            "der Wandel", "die Auffassung", "das Zugeständnis"]

    def test_род_на_единице_может_отсутствовать_источника_достаточно(self, справочник):
        справочник(склонения={"Schnapsidee": "die"},
                   единицы={"schnapsidee": ("noun", "дверь слова", None)})
        items = _заголовки("Schnapsidee")
        db._attach_pool_articles(items)
        assert items[0]["display_word"] == "die Schnapsidee"


class TestАртикльНеПоявляется:
    def test_ловушка_1_форма_множественного(self, справочник):
        # Справочник промолчал: внутри него стоит сторож «именительный единственного».
        справочник(склонения={"Türen": None},
                   единицы={"türen": ("noun", "card", "die")})
        items = _заголовки("Türen")
        db._attach_pool_articles(items)
        assert items[0]["display_word"] == "Türen"

    def test_ловушка_2_карточка_не_про_существительное(self, справочник):
        # Существительное «das Gehen» есть, но карточка про глагол.
        справочник(склонения={"Gehen": "das"},
                   единицы={"gehen": ("verb", "card", None)})
        items = _заголовки("Gehen")
        db._attach_pool_articles(items)
        assert items[0]["display_word"] == "Gehen"

    def test_ловушка_3_часть_речи_подтверждена_слабо(self, справочник):
        # Ровно живой случай: карточка «некоторые», а справочник знает «die Manche».
        справочник(склонения={"Manche": "die"},
                   единицы={"manche": ("noun", "wiktionary", "die")})
        items = _заголовки("Manche")
        db._attach_pool_articles(items)
        assert items[0]["display_word"] == "Manche"

    def test_наш_род_спорит_с_источником(self, справочник):
        справочник(склонения={"Vertriebene": "der"},
                   единицы={"vertriebene": ("noun", "card", "die")})
        items = _заголовки("Vertriebene")
        db._attach_pool_articles(items)
        assert items[0]["display_word"] == "Vertriebene"

    def test_справочник_не_знает_слова(self, справочник):
        справочник(склонения={"Kosten": None},
                   единицы={"kosten": ("noun", "card", "die")})
        items = _заголовки("Kosten")
        db._attach_pool_articles(items)
        assert items[0]["display_word"] == "Kosten"

    def test_заголовок_из_нескольких_слов_не_трогается(self, справочник):
        справочник(склонения={}, единицы={})
        items = _заголовки("einen Kater haben", "die Rache")
        db._attach_pool_articles(items)
        assert [i["display_word"] for i in items] == ["einen Kater haben", "die Rache"]

    def test_пустая_выдача_не_ходит_в_базу(self, справочник):
        # Ходить в справочник ради нуля заголовков — лишний запрос на каждый поиск.
        db._attach_pool_articles([])
