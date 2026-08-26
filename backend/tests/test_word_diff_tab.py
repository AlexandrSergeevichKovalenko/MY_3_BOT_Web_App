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


def _entry(word: str) -> dict:
    """Готовая статья: значения с НАШИМИ номерами, конструкции и сочетания с ярлыками."""
    return {
        "word": word,
        "headword": word,
        "pos": "noun",
        "senses": [{"id": "s1", "meaning": "задаток", "context": "часть суммы вперёд"}],
        "reflexivity": {},
        "constructions": [{"id": "c1", "pattern": f"{word} leisten", "case": "Akkusativ",
                           "obligatory": True, "sense_id": "s1",
                           "example_de": f"Eine {word} leisten.", "example_ru": "Внести задаток."}],
        "collocations": [{"id": "l1", "phrase": f"eine {word} leisten",
                          "translation": "внести задаток", "sense_id": "s1"}],
        "word_family": [],
        "examples": [{"de": f"Eine {word} ist fällig.", "ru": "Нужен задаток."}],
        "register": "",
        "source": "dictionary_units",
    }


ENTRY = _entry("Anzahlung")

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
    # Сбор «полноты слова» — это обращение к модели и к базе. В тестах он подменён всегда.
    monkeypatch.setattr(backend_server, "_word_diff_usage", lambda *a, **k: {})
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
        "get_word_usage": lambda *a, **k: {},
        "save_word_usage": lambda *a, **k: {},
        "get_lex_unit_card": lambda *a, **k: None,
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
    monkeypatch.setattr(backend_server, "_word_diff_can_create", lambda uid: True)
    monkeypatch.setattr(backend_server, "run_word_diff_multilang", _forbid("модель"))
    monkeypatch.setattr(
        backend_server, "_word_diff_lookup_sources",
        lambda word, studied, explain: ENTRY if word == "Anzahlung" else None,
    )
    monkeypatch.setattr(backend_server, "_word_diff_spelling_suggestion", lambda *a, **k: "Vorschuss")

    resp = _post(client, ["Anzahlung", "Vorschuß"])

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is False and body["reason"] == "not_found"
    # Написание поправимо — предлагаем правку и НЕ заводим кривую форму в словарь.
    assert body["missing"] == [{"word": "Vorschuß", "suggestion": "Vorschuss"}]
    assert misses and misses[0][1] == "not_found", "промах не посчитан — владелец его не увидит"


def test_unknown_word_is_looked_up_right_now_not_tomorrow(client, monkeypatch):
    """Слова нет в наших источниках → разбираем его СЕЙЧАС, в этом же запросе.

    Владелец 25.08.2026: «то есть я запросил сейчас, а ответ дадут утром?». Нет.
    Незнакомое слово проходит обычный путь словаря (тот же, что в поиске), карточка
    ложится в общий пул, и сравнение идёт дальше. Ночная очередь ответом человеку
    быть не может — этот тест держит именно это.
    """
    looked_up = []
    _patch_db(monkeypatch)
    monkeypatch.setattr(backend_server, "_word_diff_can_create", lambda uid: True)
    monkeypatch.setattr(
        backend_server, "_word_diff_full_lookup",
        lambda word, studied, explain: looked_up.append(word) or {
            "word_de": word, "part_of_speech": "noun",
            "translations": [{"value": "предоплата", "context": ""}],
            "usage_examples": [{"source": f"Die {word} ist fällig.", "target": "Предоплата к оплате."}],
            "government_patterns": [], "common_collocations": [f"{word} leisten"],
        },
    )
    monkeypatch.setattr(backend_server, "_fetch_wiktionary_entry", lambda *a, **k: None)
    monkeypatch.setattr(backend_server, "_word_diff_word_gate_blocks", lambda *a, **k: False)

    # «Anzahlung» у нас есть, и статья полная — её трогать не нужно.
    # «Vorauszahlung» нет вовсе: вот его и надо разобрать на месте.
    def _fake_entries(word, source_lang="", target_lang=""):
        return [] if word == "Vorauszahlung" else [
            {"headword": word, "pos": "noun", "unit_id": 7, "translations": ["задаток"], "examples": []}
        ]

    import backend.dictionary_entries as de
    import backend.database as db
    monkeypatch.setattr(de, "entries_for_query", _fake_entries)
    monkeypatch.setattr(db, "get_lex_unit_card", lambda unit_id: {
        "word_de": "Anzahlung", "part_of_speech": "noun",
        "translations": [{"value": "задаток", "context": "часть суммы"}],
        "usage_examples": [{"source": "Eine Anzahlung ist fällig.", "target": "Нужен задаток."}],
        "government_patterns": [{"pattern": "eine Anzahlung leisten", "case": "Akkusativ",
                                 "example_source": "", "example_target": ""}],
        "common_collocations": ["eine Anzahlung leisten"],
    })

    async def _answer(*args, **kwargs):
        return {
            "verdict": [
                {"word": "Anzahlung", "line": "часть цены вперёд"},
                {"word": "Vorauszahlung", "line": "вся сумма до услуги"},
            ],
        }

    monkeypatch.setattr(backend_server, "run_word_diff_multilang", _answer)

    resp = _post(client, ["Anzahlung", "Vorauszahlung"])

    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json()
    assert body["ok"] is True, "человека отправили ждать вместо ответа"
    assert looked_up == ["Vorauszahlung"], "незнакомое слово не разобрали на месте"
    assert body["sources"]["Vorauszahlung"] == "dictionary_lookup"


def test_nothing_sends_the_person_to_wait_until_tomorrow():
    """Ни сервер, ни экран не имеют права предлагать человеку подождать до завтра.

    Проверяем две стороны: обработчик не заводит слово в ночную очередь вместо ответа,
    и на экране нет обещания «появится к следующему дню».
    """
    import inspect
    from pathlib import Path

    prepare = inspect.getsource(backend_server._word_diff_prepare)
    assert "ensure_unit" not in prepare, "слово снова уходит в ночную очередь вместо разбора"
    assert "_word_diff_full_lookup" in inspect.getsource(backend_server._word_diff_lookup_sources), (
        "незнакомое слово больше не разбирается на месте"
    )

    screen = Path(backend_server.__file__).resolve().parents[1] / "frontend/src/dictionary/WordDiff.jsx"
    text = screen.read_text(encoding="utf-8")
    for promise in ("к следующему дню", "взяли в работу"):
        assert promise not in text, f"экран снова обещает ждать: {promise!r}"


def test_cached_pair_costs_no_daily_unit(client, monkeypatch):
    cached = {
        "words": ["Anzahlung", "Vorschuss"],
        "payload": FULL_ANSWER,
        "sources": {"Anzahlung": "dictionary_units"},
        "created_at": "2026-08-25T10:00:00+00:00",
    }
    _patch_db(monkeypatch, get_word_diff_card=lambda *a, **k: cached)
    monkeypatch.setattr(backend_server, "_word_diff_can_create", lambda uid: True)
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
    monkeypatch.setattr(backend_server, "_word_diff_can_create", lambda uid: True)
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


def test_stream_serves_cache_as_plain_json_without_touching_the_model(client, monkeypatch):
    """Готовая пара обязана прийти сразу и без потока: поток тут не нужен и не бесплатен."""
    cached = {"words": ["Miete", "Pacht"], "payload": FULL_ANSWER, "sources": {}, "created_at": None}
    _patch_db(monkeypatch, get_word_diff_card=lambda *a, **k: cached)
    monkeypatch.setattr(backend_server, "_word_diff_can_create", lambda uid: True)
    monkeypatch.setattr(backend_server, "_word_diff_lookup_sources", lambda *a, **k: ENTRY)

    resp = client.post(
        "/api/webapp/dictionary/diff/stream",
        json={"initData": "signed", "words": ["Miete", "Pacht"]},
    )
    body = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert "event: done" in body and '"from_cache": true' in body.replace("True", "true")


def test_stream_never_stores_half_an_answer(client, monkeypatch):
    """Поток оборвался на середине → в общий кеш не попадает ничего, промах считается.

    Половина разбора в общем кеше страшнее пустого экрана: она достанется ВСЕМ
    следующим людям и будет выглядеть законченной.
    """
    saved, misses = [], []
    _patch_db(
        monkeypatch,
        save_word_diff_card=lambda **k: saved.append(k),
        record_word_diff_miss=lambda uid, words, reason, detail="": misses.append(reason),
    )
    monkeypatch.setattr(backend_server, "_word_diff_can_create", lambda uid: True)
    monkeypatch.setattr(backend_server, "_word_diff_lookup_sources", lambda *a, **k: ENTRY)

    def _half_stream(*args, **kwargs):
        # Пришло «Главное» только про одно слово из двух — и связь оборвалась.
        yield {"section": "verdict", "verdict": [{"word": "Anzahlung", "line": "часть цены вперёд"}]}

    import backend.openai_manager as om
    monkeypatch.setattr(om, "stream_word_diff_sections", _half_stream)

    resp = client.post(
        "/api/webapp/dictionary/diff/stream",
        json={"initData": "signed", "words": ["Anzahlung", "Vorschuss"]},
    )
    body = resp.get_data(as_text=True)

    assert "event: error" in body, "человеку не сказали, что разбор не собрался"
    assert "event: done" not in body, "оборванный разбор выдан за готовый"
    assert not saved, "половина разбора попала в ОБЩИЙ кеш — её увидят все следующие"
    assert misses == ["incomplete"]


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


def test_thin_entry_is_enriched_before_the_comparison(monkeypatch):
    """Бедная статья достраивается ДО сравнения, иначе модель честно сравнит мусор.

    Замер владельца 25.08.2026: у «abschieben» в слое лежал один перевод «убраться»,
    и на экран ушло сравнение «удостоверять» против «убраться».
    """
    enriched_calls = []
    saved = []

    import backend.dictionary_entries as de
    import backend.database as db
    monkeypatch.setattr(de, "entries_for_query", lambda word, source_lang="", target_lang="": [
        {"headword": word, "pos": "verb", "unit_id": 42, "translations": ["убраться"], "examples": []}
    ])
    monkeypatch.setattr(db, "get_lex_unit_card", lambda unit_id: {
        "word_de": "abschieben", "translation_ru": "убраться", "part_of_speech": "verb",
    })
    monkeypatch.setattr(
        backend_server, "_word_diff_full_lookup",
        lambda word, studied, explain: enriched_calls.append(word) or {
            "word_de": "abschieben", "part_of_speech": "verb",
            "translations": [{"value": "выдворять", "context": "принудительно из страны"}],
            "usage_examples": [{"source": "Er wurde abgeschoben.", "target": "Его выдворили."}],
            "government_patterns": [{"pattern": "jdn. abschieben", "case": "Akkusativ",
                                     "example_source": "", "example_target": ""}],
            "common_collocations": ["einen Migranten abschieben"],
        },
    )
    import backend.lex_units as lex
    monkeypatch.setattr(lex, "save_unit_card_if_richer",
                        lambda unit_id, card, **k: saved.append(unit_id) or True)
    # Сбор «полноты слова» — отдельная работа с моделью и базой; здесь он не проверяется.
    monkeypatch.setattr(backend_server, "_word_diff_usage", lambda *a, **k: {})

    article = backend_server._word_diff_lookup_sources("abschieben", "de", "ru")

    assert enriched_calls == ["abschieben"], "бедную статью не достроили"
    assert saved == [42], "достроенная карточка не сохранена — починка не осталась всем"
    assert [x["meaning"] for x in article["senses"]] == ["выдворять"]
    assert article["constructions"][0]["pattern"] == "jdn. abschieben"


def test_part_of_speech_and_construction_never_come_from_the_model():
    """Часть речи и управление — данные справочника. Ответ модели по ним отбрасывается."""
    sources = [dict(_entry("Anzahlung"), pos="noun"), dict(_entry("Vorschuss"), pos="noun")]
    raw = {
        "verdict": [{"word": "Anzahlung", "line": "часть суммы"}, {"word": "Vorschuss", "line": "деньги вперёд"}],
        # Модель пытается сказать своё: и часть речи, и конструкцию, и ярлык чужой.
        "words": [{"word": "Anzahlung", "meaning": "задаток", "pos": "adverb"}],
        "highlight": {"constructions": ["Anzahlung:c999"]},
    }
    diff, reason = backend_server._word_diff_validate(raw, ["Anzahlung", "Vorschuss"], sources)
    assert reason == ""
    assert diff["words"][0]["pos"] == "noun", "часть речи взята у модели"
    patterns = [c["pattern"] for c in diff["constructions"]]
    assert "Anzahlung leisten" in patterns, "наша конструкция не показана"
    assert all("выдумка" not in p for p in patterns), "в разбор просочилась конструкция от модели"


def test_word_is_shown_as_the_dictionary_writes_it():
    """На экране слово пишется по-словарному: глагол строчными, а не как ответила модель.

    Скриншот владельца 25.08.2026: глагол стоял «Abschieben» с заглавной буквы.
    """
    sources = [dict(_entry("abschieben"), headword="abschieben", pos="verb")]
    raw = {"verdict": [{"word": "Abschieben", "line": "выдворить из страны"}]}
    diff, reason = backend_server._word_diff_validate(raw, ["abschieben"], sources)
    assert reason == ""
    assert diff["verdict"][0]["word"] == "abschieben"


def test_missing_examples_or_collocations_for_a_word_are_counted():
    """Слово без верного примера или без сочетаний — половина ответа, и это считается.

    Владелец 25.08.2026: «примеры пишутся только для первого слова, для второго я не вижу,
    как его использовать». Показывать такой разбор можно, молчать о нём — нет.
    """
    diff = {
        "verdict": [{"word": "Anzahlung", "line": "..."}, {"word": "Vorschuss", "line": "..."}],
        "examples": [{"word": "Anzahlung", "de": "...", "translation": "..."}],
        "collocations": [{"word": "Anzahlung", "phrase": "eine Anzahlung leisten", "translation": ""}],
        "constructions": [{"word": "Anzahlung", "pattern": "Anzahlung leisten"}],
    }
    gaps = backend_server._word_diff_gaps(diff, ["Anzahlung", "Vorschuss"])
    assert "нет примера: vorschuss" in gaps
    assert "нет сочетаний: vorschuss" in gaps
    assert not [g for g in gaps if "anzahlung" in g], "у первого слова всё есть, а его посчитали"


def test_old_cached_answer_is_not_served_after_the_format_changes():
    """Кеш обязан знать версию разбора, иначе починка не доходит до человека.

    Владелец 25.08.2026 открыл ту же пару после переделки и увидел один в один прежнюю
    карточку: запись лежала в кеше, а о том, что разбор изменился, кеш не знал.
    """
    import inspect
    from backend import database

    assert getattr(database, "WORD_DIFF_SCHEMA_VERSION", 0) >= 2, "версия разбора не заведена"

    read_src = inspect.getsource(database.get_word_diff_card)
    assert "schema_version = %s" in read_src, "из кеша отдаётся запись любой версии"

    write_src = inspect.getsource(database.save_word_diff_card)
    assert "DO UPDATE" in write_src, "устаревшая запись не переписывается новым разбором"
    assert "schema_version < EXCLUDED.schema_version" in write_src, (
        "новый разбор может затереть более свежий"
    )


def test_free_user_reads_the_shared_shelf_but_orders_nothing_new(client, monkeypatch):
    """Бесплатному — всё уже разобранное, но НОВЫЙ разбор не запускается.

    Владелец 25.08.2026 снял дневную норму в три пары: новый разбор дорог. Зато готовое
    открывается всем и бесплатно — это витрина, по которой человек решает, нужен ли доступ.
    """
    _patch_db(monkeypatch)
    monkeypatch.setattr(backend_server, "_word_diff_can_create", lambda uid: False)
    monkeypatch.setattr(backend_server, "run_word_diff_multilang", _forbid("модель"))
    monkeypatch.setattr(backend_server, "_word_diff_lookup_sources", lambda *a, **k: ENTRY)

    resp = _post(client, ["Anzahlung", "Vorschuss"])

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is False and body["reason"] == "paid_only"
    assert "бесплатно" in body["message"]


def test_ready_pair_opens_for_everyone_including_free(client, monkeypatch):
    """Готовая пара приходит из базы и тарифа не спрашивает — она нам ничего не стоит."""
    cached = {"words": ["Miete", "Pacht"], "payload": FULL_ANSWER, "sources": {}, "created_at": None}
    _patch_db(monkeypatch, get_word_diff_card=lambda *a, **k: cached)
    monkeypatch.setattr(backend_server, "_word_diff_can_create", _forbid("проверка тарифа"))
    monkeypatch.setattr(backend_server, "run_word_diff_multilang", _forbid("модель"))
    monkeypatch.setattr(backend_server, "_word_diff_lookup_sources", lambda *a, **k: ENTRY)

    resp = _post(client, ["Miete", "Pacht"])

    assert resp.status_code == 200
    assert resp.get_json()["from_cache"] is True


def test_shared_shelf_is_sorted_by_how_often_a_pair_is_opened():
    """Общий список ранжируется частотой: случайная ерунда сама уходит вниз."""
    import inspect
    from backend import database
    src = inspect.getsource(database.list_word_diff_popular)
    assert "ORDER BY open_count DESC" in src, "список не ранжирован по частоте"
    assert "schema_version = %s" in src, (
        "в общий список попадут устаревшие пары — их открытие потребует новой оплаты"
    )


def test_stream_prompt_spells_out_every_block_in_full():
    """Потоковое задание обязано описывать форму КАЖДОГО блока целиком.

    Замер 26.08.2026: в потоке блоки были показаны сокращённо («verdict»:[...]), и модель
    выдумывала форму — «comparable» приходило строкой, «verdict» списком строк без слова.
    Проверка такой ответ отвергала, и человек получал «не удалось разобрать» на ЛЮБОЙ паре.
    """
    from backend.openai_manager import system_message

    prompt = system_message["word_diff_multilang_stream"]
    for shape in (
        '"comparable":{"value":"broad|partial|none"',
        '"verdict":[{"word":"ausweisen","line":"..."}]',
        '"roles":[{"word":"ausweisen","role":"..."}]',
        '"interchangeable":[{"a":"ausweisen","b":"abschieben"',
        '"words":[{"word":"ausweisen","sense_id"',
        '"examples":[{"word":"ausweisen","de":"..."',
        '"chooser":[{"situation":"...","word":"ausweisen"}]',
    ):
        assert shape in prompt, f"в потоковом задании нет полной формы блока: {shape}"


def test_both_prompts_ask_for_the_same_thing():
    """Обычное и потоковое задания не имеют права разойтись по смыслу."""
    from backend.openai_manager import system_message

    plain = system_message["word_diff_multilang"]
    stream = system_message["word_diff_multilang_stream"]
    for rule in (
        "Ты НЕ источник словарных фактов",
        "СНАЧАЛА РЕШИ, СРАВНИМЫ ЛИ СЛОВА",
        "НЕ БОЛЬШЕ ДВУХ на слово",
        "possible_but_different_meaning",
    ):
        assert rule in plain and rule in stream, f"задания разошлись: {rule!r}"


def test_internal_sense_numbers_never_reach_the_screen():
    """Наши номера значений (ausweisen:s2) — служебные. Человеку они не показываются.

    Владелец 26.08.2026 увидел на экране: «Сравниваем значение „выдворять"
    (ausweisen:s2 и abschieben:s1)» — и справедливо спросил, зачем это ему.
    """
    clean = backend_server._word_diff_clean_text
    assert clean("Сравниваем «выдворять» (ausweisen:s2 и abschieben:s1)") == "Сравниваем «выдворять»"
    assert clean("значение выдворять ausweisen:s2") == "значение выдворять"
    assert clean("Обычный текст без номеров") == "Обычный текст без номеров"


def test_pair_is_shown_the_way_the_dictionary_writes_it(client, monkeypatch):
    """В заголовке и в списках — словарное написание, а не то, как набрал человек."""
    saved = {}
    _patch_db(monkeypatch, save_word_diff_card=lambda **k: saved.update(k))
    monkeypatch.setattr(backend_server, "_word_diff_can_create", lambda uid: True)
    monkeypatch.setattr(
        backend_server, "_word_diff_lookup_sources",
        lambda word, studied, explain: dict(_entry(word), headword=word.lower()),
    )

    async def _answer(*args, **kwargs):
        return {"verdict": [{"word": "Ausweisen", "line": "решение о выдворении"},
                            {"word": "Abschieben", "line": "исполнение выдворения"}]}

    monkeypatch.setattr(backend_server, "run_word_diff_multilang", _answer)

    resp = _post(client, ["Ausweisen", "Abschieben"])

    assert resp.status_code == 200, resp.get_data(as_text=True)
    assert resp.get_json()["words"] == ["ausweisen", "abschieben"], "заголовок не по-словарному"
    assert saved.get("words") == ["ausweisen", "abschieben"], "в общую полку легло не то написание"
