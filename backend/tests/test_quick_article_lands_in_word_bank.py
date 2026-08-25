# -*- coding: utf-8 -*-
"""Найденный артикль ложится в БАНК СЛОВ, а не только в память процесса.

Откуда (владелец, 25.08.2026)
─────────────────────────────
Быстрый словарь, не найдя артикль сразу, ищет его в фоне и кладёт ответ в
`_QUICK_TRANSLATE_CACHE` — обычный словарь в памяти ОДНОГО процесса. Экран потом
опрашивает сервер до пяти раз и забирает готовое.

Пока процесс один, это работает. Ломается МОЛЧА и сразу в трёх случаях:
  • WEB_CONCURRENCY станет 2 — опрос попадёт в соседний процесс, где артикля нет,
    и будет впустую ВСЕГДА, без единой ошибки в логе;
  • gunicorn перезапускает воркер каждые 2000 запросов — память стирается;
  • деплой стирает её же, а он у нас по нескольку раз в день.

Владелец: «если это проблема на будущее — её нужно устранять, а не записывать».
Разбор сделан вместе с сессией, ведущей экран (agent/e6), она же и предложила решение:
писать в `bt_3_lex_units.gender` — то самое место, откуда артикль читает мгновенный путь.

ЭТОТ ТЕСТ СТОРОЖИТ ЧЕТЫРЕ ЗАПРЕТА. Каждый — не теория:
  1. род ФОРМЫ множественного в банк не идёт: у «Probleme» артикль «die» принадлежит
     форме, а не лемме «das Problem»;
  2. ответ МОДЕЛИ в банк не идёт: банк — источник, а модель догадка (правило ноль);
  3. известный род НЕ перезаписывается: банк выверялся с владельцем поимённо;
  4. фраза в банк не идёт: у оборота рода нет.
"""
from __future__ import annotations

from contextlib import contextmanager

import pytest

import backend.backend_server as server


class _Cursor:
    def __init__(self, sink):
        self.sink = sink
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.sink.append((sql, params))
        self.rowcount = 1


class _Conn:
    def __init__(self, sink):
        self.sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def cursor(self):
        return _Cursor(self.sink)

    def commit(self):
        pass


@pytest.fixture
def записи(monkeypatch):
    """Собирает всё, что функция попыталась записать в базу."""
    sink = []

    @contextmanager
    def _ctx(*a, **k):
        yield _Conn(sink)

    import backend.database as db
    monkeypatch.setattr(db, "get_db_connection_context", _ctx)
    return sink


class TestРодЗаписываетсяВБанк:
    def test_ответ_справочника_ложится_в_банк(self, записи):
        server._remember_article_in_word_bank(
            {"translation": "Zeitnot"}, "die", "wiktionary")
        assert len(записи) == 1
        sql, params = записи[0]
        assert "bt_3_lex_units" in sql and "gender" in sql
        assert params[0] == "die"
        assert "быстрый словарь" in params[1]
        assert params[2] == "zeitnot"

    def test_пишем_только_в_пустое(self, записи):
        server._remember_article_in_word_bank(
            {"translation": "Zeitnot"}, "die", "wiktionary")
        sql, _params = записи[0]
        # Банк выверялся с владельцем поимённо: фоновый добор не спорит с ним.
        assert "gender IS NULL" in sql

    def test_артикль_снимается_с_заголовка(self, записи):
        server._remember_article_in_word_bank(
            {"translation": "die Zeitnot"}, "die", "wiktionary")
        assert записи[0][1][2] == "zeitnot"


class TestЧтоВБанкНеПопадает:
    def test_форма_множественного_не_пишется(self, записи):
        # «die Probleme» — артикль ФОРМЫ, а лемма «das Problem». Записать его родом
        # слова значит испортить банк.
        server._remember_article_in_word_bank(
            {"translation": "Probleme", "lemma_de": "Problem"},
            "die", "wiktionary", number="pl")
        assert записи == []

    def test_ответ_модели_не_пишется(self, записи):
        server._remember_article_in_word_bank({"translation": "Testwort"}, "die", "llm")
        assert записи == []

    def test_фраза_не_пишется(self, записи):
        server._remember_article_in_word_bank(
            {"translation": "eine Pressekonferenz abhalten"}, "die", "wiktionary")
        assert записи == []

    @pytest.mark.parametrize("article", ["", "ein", "eine", "de", "DIE?", None])
    def test_не_артикль_не_пишется(self, записи, article):
        server._remember_article_in_word_bank({"translation": "Zeitnot"}, article, "wiktionary")
        assert записи == []

    def test_слово_со_строчной_не_пишется(self, записи):
        # Существительные в немецком с заглавной. Строчное — не существительное.
        server._remember_article_in_word_bank({"translation": "zeitnot"}, "die", "wiktionary")
        assert записи == []

    def test_без_источника_не_пишется(self, записи):
        server._remember_article_in_word_bank({"translation": "Zeitnot"}, "die", "")
        assert записи == []


class TestОтказЗаписиНеМолчит:
    def test_упавшая_запись_логируется(self, monkeypatch, caplog):
        """Молчание тут запрещено: «банк не пополняется» должно быть отличимо от
        «нечего записывать». Иначе поломка живёт годами и выглядит как норма."""
        @contextmanager
        def _boom(*a, **k):
            raise RuntimeError("база недоступна")
            yield  # pragma: no cover

        import backend.database as db
        monkeypatch.setattr(db, "get_db_connection_context", _boom)
        with caplog.at_level("WARNING"):
            # Падение записи НЕ должно ронять ответ человеку: артикль он уже получил.
            server._remember_article_in_word_bank(
                {"translation": "Zeitnot"}, "die", "wiktionary")
        assert "в банк НЕ записан" in caplog.text
