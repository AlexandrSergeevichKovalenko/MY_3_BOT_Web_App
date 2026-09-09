"""Предложение переводится как предложение; форму ввода решает сервер, а не модель.

Владелец 09.09.2026 набрал в быстром словаре «Ich weiß die Antwort nicht, ich rate ins
Blaue hinein», нажал «Подробный разбор» — и крупно вместо перевода предложения встало
«угадывать без оснований, наугад»: толкование идиомы, найденной внутри. Кто решал,
предложение это или выражение? Только модель, полем phrase_kind. На двери сохранения
то же решает наш текст (_looks_like_dictionary_sentence). Теперь судья один на обе
двери — _lookup_input_kind, его вердикт уходит модели фактом (input_kind) и экрану
(ответ /api/translate/quick), а выражение внутри предложения приходит отдельным полем
embedded_expression и рисуется блоком под заголовком.
"""
import asyncio

from backend import openai_manager
from backend.backend_server import (
    _build_multilang_dictionary_result,
    _lookup_input_kind,
)

OWNERS_SENTENCE = "Ich weiß die Antwort nicht, ich rate ins Blaue hinein"


class TestTheServerDecidesTheShapeOfTheInput:
    def test_the_owners_sentence_is_a_sentence(self):
        assert _lookup_input_kind(OWNERS_SENTENCE, "de") == "sentence"

    def test_the_idiom_itself_is_a_phrase(self):
        assert _lookup_input_kind("ins Blaue hinein raten", "de") == "phrase"

    def test_a_single_word_is_a_word(self):
        assert _lookup_input_kind("raten", "de") == "word"
        assert _lookup_input_kind("die Antwort", "de") == "word"

    def test_the_two_collapsed_pool_rows_are_sentences(self):
        # Накопленные в пуле до починки (id 33971, 30299) — те же предложения.
        assert _lookup_input_kind("Ich hoffe, dass sich eine Lösung finden lässt", "de") == "sentence"
        assert _lookup_input_kind("Ich fange langsam an, Vertrauen zu jemandem zu haben", "de") == "sentence"

    def test_same_rule_as_the_save_door(self):
        # Судья один, и граница у него та же, что на двери сохранения: до четырёх слов
        # без знака конца — фраза; от пяти слов — предложение, даже если это идиома в
        # словарной форме («alles unter einen Hut bringen»). Это ГРАНИЦА ПРАВИЛА, а не
        # дефект: заголовок такой идиомы — перевод переводчика, разбор идёт блоком ниже.
        assert _lookup_input_kind("auf den Arm nehmen", "de") == "phrase"
        assert _lookup_input_kind("alles unter einen Hut bringen", "de") == "sentence"
        assert _lookup_input_kind("Ich mache mich vom Acker", "de") == "sentence"


class TestTheModelIsToldNotAsked:
    def test_stream_payload_carries_input_kind(self, monkeypatch):
        seen = {}

        def fake_stream(**kwargs):
            seen.update(kwargs.get("payload") or {})
            yield {"section": "head", "word_source": OWNERS_SENTENCE}

        monkeypatch.setattr(openai_manager, "_stream_json_objects", fake_stream)
        list(openai_manager.stream_dictionary_breakdown_sections(
            word=OWNERS_SENTENCE, source_lang="de", target_lang="ru",
            explanation_lang="ru", input_kind="sentence",
        ))
        assert seen.get("input_kind") == "sentence"
        assert seen.get("word") == OWNERS_SENTENCE

    def test_core_fast_payload_carries_input_kind(self, monkeypatch):
        seen = {}

        async def fake_lookup(**kwargs):
            seen.update(kwargs)
            return {"word_source": OWNERS_SENTENCE}

        monkeypatch.setattr(openai_manager, "run_dictionary_lookup_multilang", fake_lookup)
        asyncio.run(openai_manager.run_dictionary_lookup_multilang_core_fast(
            word=OWNERS_SENTENCE, source_lang="de", target_lang="ru",
            explanation_lang="ru", input_kind="sentence",
        ))
        assert (seen.get("extra_payload") or {}).get("input_kind") == "sentence"

    def test_phrase_enrichment_payload_carries_input_kind(self, monkeypatch):
        seen = {}

        async def fake_lookup(**kwargs):
            seen.update(kwargs)
            return {}

        monkeypatch.setattr(openai_manager, "run_dictionary_lookup_multilang", fake_lookup)
        asyncio.run(openai_manager.run_dictionary_enrichment_multilang(
            word=OWNERS_SENTENCE, source_lang="de", target_lang="ru",
            core_result={"part_of_speech": "phrase"}, explanation_lang="ru", input_kind="sentence",
        ))
        assert seen.get("task_name") == "dictionary_enrichment_multilang_phrase_compact"
        assert (seen.get("extra_payload") or {}).get("input_kind") == "sentence"

    def test_prompts_name_the_rule(self):
        for key in ("dictionary_assistant_multilang_stream",
                    "dictionary_assistant_multilang_core_fast",
                    "dictionary_enrichment_multilang_phrase_compact"):
            text = openai_manager.system_message[key]
            assert "input_kind" in text, key
            assert "embedded_expression" in text, key
            assert "DECIDED BY THE APPLICATION" in text, key


class TestTheExpressionInsideTheSentenceReachesTheCard:
    def test_embedded_expression_passes_through_the_builder(self):
        raw = {
            "detected_language": "source",
            "word_source": OWNERS_SENTENCE,
            "word_target": "Я не знаю ответа, я гадаю наугад",
            "part_of_speech": "phrase",
            "phrase_kind": "sentence",
            "embedded_expression": {"source": "ins Blaue hinein raten",
                                    "target": "угадывать наугад", "kind": "idiom"},
        }
        item, _detected, _src, _tgt = _build_multilang_dictionary_result(
            raw=raw, query_word=OWNERS_SENTENCE, source_lang="de", target_lang="ru",
        )
        assert item["embedded_expression"]["source"] == "ins Blaue hinein raten"
        assert item["phrase_kind"] == "sentence"
        assert item["translation_ru"] == "Я не знаю ответа, я гадаю наугад"


class TestEveryServedCardCarriesTheVerdict:
    """Кеш, пул, обратная сторона и хвост дообогащения собирают карточку мимо потокового
    пути; без штампа полный словарь возвращал подмену заголовка через опрос статуса
    (найдено опровергателем 09.09.2026)."""

    def test_stamp_from_the_asked_word(self):
        from backend.backend_server import _stamp_input_kind
        item = _stamp_input_kind({"source_text": "x"}, word=OWNERS_SENTENCE, lang="de")
        assert item["input_kind"] == "sentence"

    def test_stamp_from_the_card_itself_when_no_word_given(self):
        from backend.backend_server import _stamp_input_kind
        item = _stamp_input_kind({"source_text": OWNERS_SENTENCE, "language_pair": {"source_lang": "de"}})
        assert item["input_kind"] == "sentence"
        word = _stamp_input_kind({"word_de": "der Hund", "word_ru": "собака"})
        assert word["input_kind"] == "word"

    def test_an_existing_verdict_is_never_overwritten(self):
        from backend.backend_server import _stamp_input_kind
        item = _stamp_input_kind({"input_kind": "phrase", "source_text": OWNERS_SENTENCE}, word=OWNERS_SENTENCE, lang="de")
        assert item["input_kind"] == "phrase"
