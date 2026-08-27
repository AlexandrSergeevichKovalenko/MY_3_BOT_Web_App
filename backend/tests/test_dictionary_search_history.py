"""История поиска в словаре — ОДНА НА ВСЕ УСТРОЙСТВА.

До 27.08.2026 история жила только в памяти браузера (localStorage). У Telegram, у
приложения с рабочего стола и у Safari память РАЗНАЯ: человек искал слова в Telegram,
открывал приложение с иконки — и видел пустую историю, хотя слов у него сотни.

Здесь держатся три вещи, без которых починка разваливается обратно:

1. Найденное человеком слово попадает на сервер и возвращается ему списком.
   Не «записали и молчим» — экран обязан получить свежий список тем же ответом.
2. Накопленное устройством ДО переезда вливается — и вливается ровно один раз.
   Иначе человек теряет свою историю в тот самый день, когда мы её чинили.
3. Человека не узнали — честный 401, а НЕ пустой список. Пустая история от «мы не
   поняли, кто ты» и пустая история от «ты ничего не искал» — разные миры, и путать
   их нельзя: во втором случае экран покажет «здесь появятся слова» и соврёт.
"""
import pytest

from backend import backend_server


@pytest.fixture
def client(monkeypatch):
    # Общий страж доступа проверяет подпись Telegram ДО обработчика.
    monkeypatch.setattr(backend_server, "_telegram_hash_is_valid", lambda *a, **k: True)
    monkeypatch.setattr(
        backend_server, "_parse_telegram_init_data", lambda *a, **k: {"user": {"id": 777}}
    )
    return backend_server.app.test_client()


def _patch_db(monkeypatch, **overrides):
    """Тесты не имеют права ходить в боевую базу."""
    from backend import database
    defaults = {
        "record_dictionary_search": lambda *a, **k: None,
        "merge_dictionary_search_history": lambda *a, **k: None,
        "list_dictionary_search_history": lambda *a, **k: [],
    }
    defaults.update(overrides)
    for name, func in defaults.items():
        monkeypatch.setattr(database, name, func)


def test_found_word_is_written_and_comes_back_in_the_list(client, monkeypatch):
    written = []
    _patch_db(
        monkeypatch,
        record_dictionary_search=lambda uid, word, lang="": written.append((uid, word, lang)),
        list_dictionary_search_history=lambda uid, limit=60: ["abschieben", "ausweisen"],
    )

    resp = client.post(
        "/api/webapp/dictionary/history/record",
        json={"initData": "signed", "word": "abschieben", "lookup_lang": "de-ru"},
    )

    assert resp.status_code == 200
    assert written == [(777, "abschieben", "de-ru")], "поиск не записан — история снова живёт только на этом устройстве"
    assert resp.get_json()["items"][0] == "abschieben", (
        "экран не получил свежий список тем же ответом и покажет вчерашний порядок"
    )


def test_empty_word_is_not_written(client, monkeypatch):
    written = []
    _patch_db(monkeypatch, record_dictionary_search=lambda *a, **k: written.append(a))

    resp = client.post(
        "/api/webapp/dictionary/history/record", json={"initData": "signed", "word": "   "}
    )

    assert resp.status_code == 400
    assert written == [], "пустая строка ушла в историю"


def test_device_history_is_merged_when_the_device_sends_it(client, monkeypatch):
    merged = []
    _patch_db(
        monkeypatch,
        merge_dictionary_search_history=lambda uid, words: merged.append((uid, list(words))),
        list_dictionary_search_history=lambda uid, limit=60: ["laufen"],
    )

    resp = client.post(
        "/api/webapp/dictionary/history",
        json={"initData": "signed", "merge": ["laufen", "gehen"]},
    )

    assert resp.status_code == 200
    assert merged == [(777, ["laufen", "gehen"])], (
        "накопленное устройством не влилось — человек потерял историю в день починки"
    )


def test_no_merge_call_when_the_device_has_nothing_to_give(client, monkeypatch):
    merged = []
    _patch_db(
        monkeypatch,
        merge_dictionary_search_history=lambda *a, **k: merged.append(a),
        list_dictionary_search_history=lambda uid, limit=60: [],
    )

    resp = client.post("/api/webapp/dictionary/history", json={"initData": "signed"})

    assert resp.status_code == 200
    assert merged == [], "слияние вызвано без данных — лишняя работа на каждом открытии"


@pytest.mark.parametrize("path", [
    "/api/webapp/dictionary/history",
    "/api/webapp/dictionary/history/record",
])
def test_unknown_user_gets_an_honest_refusal_not_an_empty_list(path, monkeypatch):
    monkeypatch.setattr(backend_server, "_telegram_hash_is_valid", lambda *a, **k: True)
    monkeypatch.setattr(backend_server, "_parse_telegram_init_data", lambda *a, **k: {})
    _patch_db(monkeypatch)
    client = backend_server.app.test_client()

    resp = client.post(path, json={"initData": "signed", "word": "gehen"})

    # 400 приходит от общего стража доступа (он отбивает раньше обработчика), 401 —
    # от самого обработчика. Проверяем не номер, а суть: это ОТКАЗ, и никакого списка
    # в ответе нет. Пустой список здесь означал бы «ты ничего не искал» — то есть враньё.
    assert resp.status_code in (400, 401), (
        "неизвестный человек получил бы пустой список, и экран сказал бы ему, "
        "что он ничего не искал"
    )
    assert "items" not in (resp.get_json() or {}), "в отказе приехал список"
