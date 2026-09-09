"""Словарь выражений важнее счёта слов; переводчики опрашиваются по очереди.

Владелец, 09.09.2026, два решения подряд:

1. «А зачем нам отделять фразу от предложения? В чём разница?» — разница в том, кто
   отвечает СВЕРХУ. Идиому переводит словарь, предложение — переводчик. Пока форму
   ввода решал счёт слов, идиома из пяти слов («Haare auf den Zähnen haben») считалась
   предложением и получала буквальный машинный перевод крупным шрифтом.

2. «Зачем мы отправляем всем наперегонки? Почему не отдать кому-то одному, если не дал
   ответ — то другому?» — MyMemory убран совсем: замер 09.09.2026 на 20 живых фразах
   дал у него 6 ошибок против одной у DeepL, и именно он выдал владельцу «Я даю совет
   наугад» вместо «Я гадаю наугад».
"""
import backend.backend_server as bs
from backend.german_expressions import meaning_from_wikitext, normalize_expression_key

IDIOM = "Haare auf den Zähnen haben"


class TestTheReferenceDecidesBeforeTheWordCount:
    def test_word_count_alone_calls_the_idiom_a_sentence(self):
        # Правило формы само по себе ошибается — ради этого и заведён справочник.
        assert bs._lookup_input_kind(IDIOM, "de") == "sentence"

    def test_the_reference_turns_it_back_into_an_expression(self, monkeypatch):
        import backend.german_expressions as ge
        monkeypatch.setattr(ge, "expression_of", lambda text: {
            "lemma": IDIOM, "kind": "idiom", "meaning_de": "zäh, robust und hart im Nehmen sein",
            "source": "de.wiktionary:Kategorie:Redewendung (Deutsch)"})
        assert bs._resolve_input_kind(IDIOM, "de") == "phrase"

    def test_a_real_sentence_stays_a_sentence(self, monkeypatch):
        import backend.german_expressions as ge
        monkeypatch.setattr(ge, "expression_of", lambda text: None)
        assert bs._resolve_input_kind(
            "Ich weiß die Antwort nicht, ich rate ins Blaue hinein", "de") == "sentence"

    def test_a_single_word_never_asks_the_reference(self, monkeypatch):
        import backend.german_expressions as ge

        def _взорвись(text):
            raise AssertionError("справочник спрошен про одиночное слово — лишний запрос")

        monkeypatch.setattr(ge, "expression_of", _взорвись)
        assert bs._resolve_input_kind("raten", "de") == "word"
        assert bs._resolve_input_kind("ins Blaue hinein raten", "de") == "phrase"

    def test_a_silent_reference_does_not_become_an_answer(self, monkeypatch):
        """Справочник не ответил — это отказ, а не «значит, предложение». Работаем по
        правилу формы и говорим об этом в лог, а не выдаём молчание за вердикт."""
        import backend.german_expressions as ge

        def _упади(text):
            raise RuntimeError("база недоступна")

        monkeypatch.setattr(ge, "expression_of", _упади)
        assert bs._resolve_input_kind(IDIOM, "de") == "sentence"


class TestTheReferenceItself:
    def test_key_keeps_the_leading_article(self):
        # У одиночного слова артикль снимается, у выражения — нет: «die Katze aus dem
        # Sack lassen» без «die» это другое выражение.
        assert normalize_expression_key("die Katze aus dem Sack lassen") == "die katze aus dem sack lassen"
        assert normalize_expression_key("  Haare   auf den Zähnen haben ") == "haare auf den zähnen haben"

    def test_meaning_is_read_from_the_reference_not_invented(self):
        wikitext = (
            "{{Bedeutungen}}\n"
            ":[1] {{ugs.|:}} [[zäh]], [[robust]] und [[hart]] im Nehmen sein\n"
            "\n{{Herkunft}}\n:unbekannt\n"
        )
        assert meaning_from_wikitext(wikitext) == "zäh, robust und hart im Nehmen sein"

    def test_no_meaning_section_means_empty_not_a_guess(self):
        assert meaning_from_wikitext("{{Herkunft}}\n:unbekannt") == ""
        assert meaning_from_wikitext(None) == ""


class TestTheTranslatorChain:
    def test_mymemory_is_gone_from_the_code(self):
        source = open(bs.__file__, encoding="utf-8").read()
        assert "_quick_translate_mymemory" not in source, (
            "MyMemory вернулся в код: замер 09.09.2026 дал у него 6 ошибок на 20 фраз"
        )

    def test_no_provider_race_left(self):
        source = open(bs.__file__, encoding="utf-8").read()
        assert "_QUICK_TRANSLATE_PROVIDER_EXECUTOR" not in source, (
            "гонка провайдеров вернулась — переводчики опрашиваются по очереди"
        )

    def test_deepl_gets_a_second_attempt_before_the_next_provider(self):
        assert bs.QUICK_TRANSLATE_DEEPL_ATTEMPTS >= 2

    def test_the_user_gets_words_when_no_provider_answers(self):
        from pathlib import Path
        errors_js = Path(bs.__file__).resolve().parents[1] / "frontend/src/dictionary/errors.js"
        text = errors_js.read_text(encoding="utf-8")
        assert "quick_translation_failed" in text and "Переводчик сейчас не отвечает" in text, (
            "человек увидит машинный код вместо человеческой строки"
        )
