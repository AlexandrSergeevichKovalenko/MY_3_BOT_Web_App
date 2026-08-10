"""Промпт, которым РАБОТАЕТ живой словарь, обязан просить синонимы.

История ошибки. 08.08.2026 я «починил» синонимы, поправив правило языка в промпте
dictionary_assistant_multilang_stream. Замер через сутки показал 0 из 13 — починка
не сработала, и диагноз был неверным дважды.

Живой поиск идёт двумя вызовами: быстрым ядром (dictionary_assistant_multilang_core_fast)
и обогащением (dictionary_enrichment_multilang). Ядро синонимы не просит СОЗНАТЕЛЬНО —
оно про скорость. А обогащение не просило их вовсе: ключей synonyms/antonyms/related_words
в нём не было. То есть словарь никогда за ними и не обращался, а те 11%, что попадались
в базе, приходили другим путём.

Мораль, ради которой этот тест и написан: правку промпта нельзя считать починкой, пока
не проверено, что правишь ИМЕННО ТОТ промпт, которым работает живой путь.
"""
from backend.openai_manager import system_message


def _prompt(name: str) -> str:
    """Промпты лежат в общем словаре system_message."""
    assert name in system_message, f"промпт {name} исчез — живой путь мог смениться"
    return system_message[name]


def test_enrichment_asks_for_synonyms():
    """Обогащение — то место, где синонимы и должны появляться: оно про «богаче, чем
    первый быстрый ответ»."""
    text = _prompt("dictionary_enrichment_multilang")
    for key in ("synonyms", "antonyms", "related_words"):
        assert f'"{key}"' in text, f"обогащение не просит {key} — в карточке их не будет"


def test_enrichment_asks_for_the_studied_language():
    """У немецкого слова нельзя просить русские синонимы: страж их вычистит как чужой
    язык, и в карточке снова окажется пусто."""
    text = _prompt("dictionary_enrichment_multilang")
    assert "explanation_language" in text.split("LANGUAGE:")[1][:400], (
        "правило языка синонимов не привязано к языку объяснений"
    )


def test_enrichment_asks_for_a_translation_next_to_the_synonym():
    text = _prompt("dictionary_enrichment_multilang")
    chunk = text.split("SHAPE:")[1][:400]
    assert '"word"' in chunk and '"gloss"' in chunk, (
        "синоним просят голым словом — человек, который его не знает, ничего не поймёт"
    )


def test_fast_core_stays_fast():
    """Ядро синонимы не просит намеренно — оно про скорость первого ответа. Если они
    туда переедут, каждый поиск станет дороже и медленнее."""
    text = _prompt("dictionary_assistant_multilang_core_fast")
    assert "synonym" not in text.lower(), (
        "синонимы уехали в быстрое ядро — первый ответ станет дороже и медленнее"
    )
