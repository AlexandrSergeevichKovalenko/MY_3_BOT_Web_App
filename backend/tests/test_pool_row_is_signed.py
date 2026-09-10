# -*- coding: utf-8 -*-
"""Строка общего словаря подписана: у неё есть автор перевода.

┌─ ПОВОД, 10.09.2026. ПОДПИСЬ ЛЕЖАЛА В ТОМ, ЧТО ВЫБРАСЫВАЕТСЯ. ────────────────────────┐
│ 09.09 из быстрого перевода убрали MyMemory (6 ошибок на 20 живых фраз против одной у │
│ DeepL) и стали писать имя переводчика — но клали его ВНУТРЬ response_json. А пул      │
│ закрыт для разбора с 05.08.2026: на дне записи payload выбрасывается целиком.        │
│ Замер по живой базе 10.09: 01.07–04.08 (пул открыт) пустых json 0 из 1966;           │
│ 06.08–31.08 (пул закрыт) — 2406 из 2484; за сутки 10.09 — 49 из 49. Имени не было ни │
│ у одной новой строки, и обещание ломалось каждое утро.                               │
│                                                                                      │
│ Решение владельца 10.09.2026, вариант «А»: подпись живёт КОЛОНКОЙ, рядом с текстом,  │
│ и не зависит от рубильника пула. Автора называет тот, кто пишет, — здесь ничего не   │
│ выводится из данных строки.                                                          │
└──────────────────────────────────────────────────────────────────────────────────────┘

Тест держит ровно это: при ЗАКРЫТОМ пуле (боевое положение рубильника) разбор в базу не
идёт, а подпись и вид записи — идут.
"""
import pytest

from backend import database


class _RecordingCursor:
    """Курсор, который ничего не пишет, а запоминает SQL и параметры."""

    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchall(self):
        return []          # слот не нашёлся — значит пойдёт ветка INSERT

    def fetchone(self):
        return (777,)

    @property
    def insert(self):
        for sql, params in self.calls:
            if "INSERT INTO bt_3_dictionary_entries" in sql:
                return sql, params
        raise AssertionError("вставки строки пула не было вовсе")


def _write(translator, payload=None):
    cursor = _RecordingCursor()
    saved = database.DICTIONARY_POOL_CARD_WRITES_ENABLED
    database.DICTIONARY_POOL_CARD_WRITES_ENABLED = False   # боевое положение
    try:
        database._upsert_dictionary_canonical_entry_with_cursor(
            cursor,
            source_lang="de", target_lang="ru",
            source_text="Der Kunde ist König.", target_text="Клиент всегда прав.",
            word_ru=None, translation_de=None, word_de=None, translation_ru=None,
            response_json=payload,
            translator=translator,
        )
    finally:
        database.DICTIONARY_POOL_CARD_WRITES_ENABLED = saved
    return cursor.insert


def test_signature_survives_the_closed_pool():
    """Разбор выброшен, подпись доехала — ради этого всё и делалось."""
    sql, params = _write("deepl_free", {"entry_kind": "sentence", "meanings": ["…"]})
    assert "translator" in sql and "entry_kind" in sql, "колонок подписи нет в записи"
    assert None in params, "разбор при закрытом пуле обязан уходить в базу как NULL"
    assert "deepl_free" in params, "имя переводчика до базы не доехало"
    assert "sentence" in params, "вид записи до базы не доехал"


def test_the_name_is_taken_from_the_payload_when_the_caller_stayed_silent():
    """Быстрый перевод кладёт имя и в payload — оттуда и берём, если аргумента нет."""
    _, params = _write(None, {"translator": "azure_translator"})
    assert "azure_translator" in params


def test_nobody_signed_means_null_and_not_a_guess():
    """Никто не назвался — в колонке пусто. Придумывать автора запрещено: такие строки
    считает обещание pool_rows_carry_their_translator и они видны владельцу числом."""
    _, params = _write(None, {"entry_kind": "phrase"})
    assert params.count(None) >= 2, "неизвестный автор обязан остаться NULL"


def test_the_signature_is_not_written_into_the_json():
    """Подпись — колонка. Вернуть её внутрь json значит вернуть 10.09.2026."""
    исходник = (database.__file__)
    with open(исходник, encoding="utf-8") as f:
        текст = f.read()
    начало = текст.index("def _upsert_dictionary_canonical_entry_with_cursor(")
    конец = текст.index("def _create_or_attach_user_dictionary_entry_with_cursor(")
    тело = текст[начало:конец]
    assert 'payload["translator"]' not in тело, "подпись снова кладут в выбрасываемый json"
    assert "resolved_translator" in тело and "resolved_entry_kind" in тело


def test_every_writer_names_itself():
    """Пять путей заводят строку пула. Безымянный путь — это NULL в базе назавтра."""
    with open(database.__file__, encoding="utf-8") as f:
        db = f.read()
    assert 'translator="сохранение человека"' in db
    assert 'translator="связывание карточки"' in db
    from backend import backend_server
    with open(backend_server.__file__, encoding="utf-8") as f:
        srv = f.read()
    assert 'translator="разбор модели"' in srv
    assert 'translator="обогащение"' in srv
    assert "translator=translator or None" in srv, "быстрый перевод перестал называть переводчика"


def test_the_promise_measures_the_column_and_not_the_json():
    """Замер по json был непроверяемым: у новых строк json пуст ВСЕГДА."""
    from backend import fix_promises
    with open(fix_promises.__file__, encoding="utf-8") as f:
        текст = f.read()
    начало = текст.index("def _pool_rows_without_translator(")
    конец = текст.index("def _pool_signature_screen(")
    тело = текст[начало:конец]
    assert "translator IS NULL" in тело
    assert "response_json ? 'translator'" not in тело, "замер снова смотрит в выброшенный json"
