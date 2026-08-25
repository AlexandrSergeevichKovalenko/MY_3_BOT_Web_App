"""Вкладка «Отличия»: чем похожие слова отличаются друг от друга.

Четыре вещи, которые обязаны держаться, иначе фича начинает врать или разорять:

1. Слова нет в источниках → модель НЕ зовётся и дневная единица НЕ тратится.
   Разбор отличий между словом и выдумкой — это выдумка целиком.
2. Пара уже разобрана → отдаём из общего кеша, дневную единицу НЕ тратим.
   Открыть то, что у нас уже лежит, стоит ноль, и брать за это плату не за что.
3. Порядок и регистр слов не создают вторую запись кеша: «Anzahlung, Vorschuss» и
   «vorschuss, anzahlung» — одно и то же сравнение.
4. Ответ модели без обязательного блока «Главное» на экран не идёт и попадает в
   счётчик промахов. Показать половину разбора — то же самое, что показать выдумку.
"""
import asyncio

import pytest

from backend import backend_server
from backend.database import build_word_diff_pair_key


ENTRY = {
    "word": "Anzahlung",
    "entries": [{"headword": "Anzahlung", "pos": "noun", "translations": ["задаток"], "examples": []}],
    "source": "dictionary_units",
}

FULL_ANSWER = {
    "verdict": [
        {"word": "Anzahlung", "line": "часть цены вперёд, остальное потом"},
        {"word": "Vorschuss", "line": "деньги вперёд человеку за работу"},
    ],
    "interchangeable": {"value": "no", "note": "сферы разные"},
    "words": [],
    "examples": [],
    "chooser": [],
    "trap": "",
    "collocations": [],
}


@pytest.fixture
def client(monkeypatch):
    # Общий страж доступа проверяет подпись Telegram ДО обработчика — без этой подмены
    # запрос не доходит до кода вкладки вовсе (400 «initData обязателен»).
    monkeypatch.setattr(backend_server, "_telegram_hash_is_valid", lambda *a, **k: True)
    monkeypatch.setattr(
        backend_server, "_parse_telegram_init_data", lambda *a, **k: {"user": {"id": 777}}
    )
    monkeypatch.setattr(backend_server, "_get_user_language_pair", lambda uid: ("ru", "de", {}))
    return backend_server.app.test_client()


def _post(client, words):
    return client.post(
        "/api/webapp/dictionary/diff", json={"initData": "signed", "words": words}
    )


def _forbid(name):
    def _boom(*args, **kwargs):
        raise AssertionError(f"{name} не должен вызываться в этом случае")
    return _boom


def _patch_db(monkeypatch, **overrides):
    """Подменяем работу с базой: тесты не имеют права ходить в боевую базу."""
    from backend import database
    defaults = {
        "get_word_diff_card": lambda *a, **k: None,
        "save_word_diff_card": lambda *a, **k: None,
        "record_word_diff_open": lambda *a, **k: None,
        "record_word_diff_miss": lambda *a, **k: None,
    }
    defaults.update(overrides)
    for name, func in defaults.items():
        monkeypatch.setattr(database, name, func)


def test_unknown_word_stops_before_the_model_and_before_the_limit(client, monkeypatch):
    misses = []
    _patch_db(monkeypatch, record_word_diff_miss=lambda uid, words, reason, detail="": misses.append((words, reason)))
    monkeypatch.setattr(backend_server, "reserve_free_feature_usage", _forbid("резерв лимита"))
    monkeypatch.setattr(backend_server, "run_word_diff_multilang", _forbid("модель"))
    monkeypatch.setattr(
        backend_server, "_word_diff_lookup_sources",
        lambda word, studied, explain: ENTRY if word == "Anzahlung" else None,
    )
    monkeypatch.setattr(backend_server, "_word_diff_spelling_suggestion", lambda *a, **k: "Vorschuss")
    monkeypatch.setattr(backend_server, "_word_diff_queue_for_sources", _forbid("очередь на карточку"))

    resp = _post(client, ["Anzahlung", "Vorschuß"])

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is False and body["reason"] == "not_found"
    # Написание поправимо — предлагаем правку и НЕ заводим кривую форму в словарь.
    assert body["missing"] == [{"word": "Vorschuß", "suggestion": "Vorschuss", "queued": False}]
    assert misses and misses[0][1] == "not_found", "промах не посчитан — владелец его не увидит"


def test_unknown_word_without_a_suggestion_goes_into_the_source_queue(client, monkeypatch):
    """Слова нет и поправить нечего → оно уходит в общий слой, ночная работа достроит карточку.

    Пустой ответ «не нашли» — незакрытая задача. Закрывается она достройкой ИСТОЧНИКА,
    и произойти это должно само, без человека.
    """
    queued = []
    _patch_db(monkeypatch)
    monkeypatch.setattr(backend_server, "reserve_free_feature_usage", _forbid("резерв лимита"))
    monkeypatch.setattr(backend_server, "run_word_diff_multilang", _forbid("модель"))
    monkeypatch.setattr(
        backend_server, "_word_diff_lookup_sources",
        lambda word, studied, explain: ENTRY if word == "Anzahlung" else None,
    )
    monkeypatch.setattr(backend_server, "_word_diff_spelling_suggestion", lambda *a, **k: "")
    monkeypatch.setattr(
        backend_server, "_word_diff_queue_for_sources",
        lambda word, lang: queued.append(word) or True,
    )

    resp = _post(client, ["Anzahlung", "Vorauszahlung"])

    assert resp.status_code == 200
    assert queued == ["Vorauszahlung"], "слово не поставлено в очередь — источник не достроится"
    assert resp.get_json()["missing"][0]["queued"] is True


def test_cached_pair_costs_no_daily_unit(client, monkeypatch):
    cached = {
        "words": ["Anzahlung", "Vorschuss"],
        "payload": FULL_ANSWER,
        "sources": {"Anzahlung": "dictionary_units"},
        "created_at": "2026-08-25T10:00:00+00:00",
    }
    _patch_db(monkeypatch, get_word_diff_card=lambda *a, **k: cached)
    monkeypatch.setattr(backend_server, "reserve_free_feature_usage", _forbid("резерв лимита"))
    monkeypatch.setattr(backend_server, "run_word_diff_multilang", _forbid("модель"))
    monkeypatch.setattr(backend_server, "_word_diff_lookup_sources", lambda *a, **k: ENTRY)

    resp = _post(client, ["Anzahlung", "Vorschuss"])

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True and body["from_cache"] is True
    assert body["diff"] == FULL_ANSWER


def test_incomplete_answer_is_not_shown_and_is_counted(client, monkeypatch):
    misses = []
    saved = []
    _patch_db(
        monkeypatch,
        record_word_diff_miss=lambda uid, words, reason, detail="": misses.append(reason),
        save_word_diff_card=lambda **k: saved.append(k),
    )
    monkeypatch.setattr(
        backend_server, "reserve_free_feature_usage",
        lambda **k: {"ok": True, "blocked": False},
    )
    monkeypatch.setattr(backend_server, "_word_diff_lookup_sources", lambda *a, **k: ENTRY)

    async def _half_answer(*args, **kwargs):
        # Модель забыла второе слово в блоке «Главное».
        return {"verdict": [{"word": "Anzahlung", "line": "часть цены вперёд"}]}

    monkeypatch.setattr(backend_server, "run_word_diff_multilang", _half_answer)

    resp = _post(client, ["Anzahlung", "Vorschuss"])

    assert resp.status_code == 502
    assert "Не удалось" in resp.get_json()["error"]
    assert misses == ["incomplete"], "неполный ответ не посчитан"
    assert not saved, "неполный разбор не имеет права попасть в общий кеш"


def test_pair_key_ignores_word_order_and_case():
    a = build_word_diff_pair_key(["Anzahlung", "Vorschuss"], "de", "ru")
    b = build_word_diff_pair_key(["vorschuss", "anzahlung"], "de", "ru")
    assert a == b, "порядок или регистр слов плодит вторую запись кеша — платим дважды за одно"
    assert build_word_diff_pair_key(["Anzahlung", "Vorschuss"], "de", "en") != a, (
        "язык объяснения обязан входить в ключ: иначе английскому человеку придёт русский разбор"
    )


def test_validation_drops_words_nobody_asked_about():
    raw = dict(FULL_ANSWER)
    raw["chooser"] = [
        {"situation": "покупка", "word": "Anzahlung"},
        {"situation": "выдумка", "word": "Vorauszahlung"},
    ]
    diff, reason = backend_server._word_diff_validate(raw, ["Anzahlung", "Vorschuss"])
    assert reason == ""
    assert [row["word"] for row in diff["chooser"]] == ["Anzahlung"], (
        "в разбор просочилось слово, которого человек не вводил"
    )
