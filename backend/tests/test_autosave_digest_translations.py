# -*- coding: utf-8 -*-
"""Ночная подборка обязана прийти С ПЕРЕВОДАМИ. Слово без перевода не сохраняется.

Что случилось 17.08.2026
────────────────────────
Владелец получил утреннюю подборку из 30 строк — только немецкий, без единого перевода,
а кнопка «Сохранить выбранные» ответила «Не удалось».

Обе беды — одна причина. `_autosave_prepare_cards` делает ОДИН вызов модели на всю
пачку и сверял ответ по СЧЁТУ: `len(cards) == len(terms)`. Модель разбила
«Sie krempelte die Ärmel hoch, um zu arbeiten.» на лишнюю запись «der Ärmel» и вернула
31 объект на 30 входов — после чего код выбросил ВСЕ тридцать переводов, включая
двадцать девять правильных, и отдал заглушку с пустыми строками. Дальше обработчик
кнопки (bot_3.py) пропускает элементы без перевода — сохранять оказалось нечего.

Код этого места не менялся с 09.06.2026: ломается вероятностно, когда модель вернёт на
один объект больше или меньше. Поэтому проверка не на «работает сегодня», а на само
правило: лишняя запись не должна стоить нам ни одного перевода.
"""
import json

import pytest

import backend.backend_server as bs


class _FakeMessage:
    def __init__(self, content):
        self.content = content


class _FakeChoice:
    def __init__(self, content):
        self.message = _FakeMessage(content)
        self.finish_reason = "stop"


class _FakeResponse:
    def __init__(self, content):
        self.choices = [_FakeChoice(content)]
        self.usage = None


class _FakeCompletions:
    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = []

    async def create(self, **kwargs):
        self.calls.append(kwargs)
        return _FakeResponse(self.replies.pop(0) if self.replies else "{}")


class _FakeChat:
    def __init__(self, completions):
        self.completions = completions


class _FakeClient:
    def __init__(self, replies):
        self.chat = _FakeChat(_FakeCompletions(replies))


def _card(text, translation):
    return {"input": text, "canonical": text, "translation": translation,
            "semantic_category": ""}


@pytest.fixture
def terms():
    return ["anrichten", "Sie krempelte die Ärmel hoch.", "herablassend"]


def _install(monkeypatch, replies):
    import backend.openai_manager as om
    client = _FakeClient(replies)
    monkeypatch.setattr(om, "client", client, raising=False)
    return client


def test_extra_invented_entry_does_not_cost_a_single_translation(monkeypatch, terms):
    """Модель придумала лишнюю запись — она отбрасывается, переводы остаются."""
    reply = json.dumps({"cards": [
        _card("anrichten", "причинять"),
        _card("Sie krempelte die Ärmel hoch.", "Она закатала рукава."),
        _card("der Ärmel", "рукав"),            # ← лишняя, такого входа не было
        _card("herablassend", "снисходительный"),
    ]}, ensure_ascii=False)
    _install(monkeypatch, [reply])

    result = bs._autosave_prepare_cards(terms, source_lang="de", target_lang="ru")

    assert len(result) == len(terms)
    assert [r["translation"] for r in result] == [
        "причинять", "Она закатала рукава.", "снисходительный"]


def test_missing_translation_is_asked_again_not_left_empty(monkeypatch, terms):
    """Перевода нет — спрашиваем ещё раз про НЕГО, а не оставляем пустым."""
    first = json.dumps({"cards": [
        _card("anrichten", "причинять"),
        _card("Sie krempelte die Ärmel hoch.", ""),   # пусто
        _card("herablassend", "снисходительный"),
    ]}, ensure_ascii=False)
    second = json.dumps({"cards": [
        _card("Sie krempelte die Ärmel hoch.", "Она закатала рукава."),
    ]}, ensure_ascii=False)
    client = _install(monkeypatch, [first, second])

    result = bs._autosave_prepare_cards(terms, source_lang="de", target_lang="ru")

    assert [r["translation"] for r in result] == [
        "причинять", "Она закатала рукава.", "снисходительный"]
    # второй вызов спрашивает ТОЛЬКО про недостающую строку, а не про всю пачку
    asked = json.loads(client.chat.completions.calls[1]["messages"][1]["content"])
    assert asked["items"] == ["Sie krempelte die Ärmel hoch."]


def test_digest_body_shows_the_translation(terms):
    """Строка подборки — «слово — перевод», а не голое слово."""
    items = [{"canonical": "anrichten", "translation": "причинять"}]
    body = bs._autosave_digest_body_text(items)
    assert "anrichten</b> — причинять" in body


def test_prompt_requires_the_input_echo(monkeypatch, terms):
    """Сопоставление ответа с запросом держится на дословном эхе входной строки."""
    client = _install(monkeypatch, [json.dumps({"cards": []})])
    bs._autosave_prepare_cards(terms, source_lang="de", target_lang="ru")
    system_prompt = client.chat.completions.calls[0]["messages"][0]["content"]
    assert '"input"' in system_prompt
    assert "VERBATIM" in system_prompt


@pytest.mark.parametrize("stored, other", [
    ("hab' ich", "hab ich"),
    ("Was laberst du da?!", "was laberst du da"),
    ("  doppelte   Leerzeichen ", "doppelte Leerzeichen"),
])
def test_match_key_survives_what_the_model_rewrites(stored, other):
    assert bs._autosave_match_key(stored) == bs._autosave_match_key(other)
