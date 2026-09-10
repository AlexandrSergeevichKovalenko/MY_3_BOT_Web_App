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


class TestAFoundEntryIsNeverCalledASingleWord:
    """Написание, у которого НАШЛАСЬ словарная статья, — словарная единица, и форма
    ввода у него не может быть «одиночное слово», если слов несколько.

    Найдено на живом экране 10.09.2026, сразу после того как слой статей открыли для
    многословного ввода: «jemanden an der Nase herumführen» находилось статьёй («водить
    за нос»), но уезжало к модели с пометкой «word»."""

    def test_multiword_entry_is_a_phrase(self):
        result = bs._build_quick_translate_from_entries(
            [{"headword": "jemanden an der Nase herumführen", "pos": "", "gender": "",
              "translations": ["водить за нос"], "display": "jemanden an der Nase herumführen"}],
            "jemanden an der Nase herumführen", "de", "ru",
        )
        assert result["input_kind"] == "phrase"

    def test_single_word_entry_stays_a_word(self):
        result = bs._build_quick_translate_from_entries(
            [{"headword": "raten", "pos": "verb", "gender": "", "translations": ["угадать"],
              "display": "raten"}],
            "raten", "de", "ru",
        )
        assert result["input_kind"] == "word"


class TestThePromiseMeasuresTheRightRows:
    """Замер «строк без следа переводчика» считает строки ОТ ПЕРЕВОДЧИКА, а не всё
    подряд.

    Проверено 10.09.2026: первая версия замера показала наутро 49 «нарушений», и все 49
    оказались сохранениями людей (примеры и синонимы из карточки, response_json = NULL).
    Переводчика у них не было вовсе. Тест держит границу замера, чтобы её не расширили
    обратно и утро снова не начало кричать на устройство системы."""

    def test_the_query_excludes_thin_user_saves(self):
        import inspect

        from backend import fix_promises
        текст = inspect.getsource(fix_promises._pool_rows_without_translator)
        assert "response_json IS NOT NULL" in текст, (
            "замер снова считает тонкие сохранения людей — у них переводчика не было"
        )
        assert "NOT (response_json ? 'translator')" in текст


class TestTheChainOrderIsAboutMoneyAtScale:
    """Запасные переводчики стоят в порядке DeepL → Azure → Google.

    Решение владельца 10.09.2026. Считаем на завтра, а не на сегодня: у DeepL Free
    500 000 знаков в месяц (у нас 19 000), но при тысячах людей лимит кончается, и весь
    поток переливается в ПЕРВЫЙ запасной. Azure F0 бесплатен до 2 млн знаков в месяц;
    у Google бесплатные 500 000 действуют только первые 12 месяцев жизни аккаунта, потом
    он считает с первого знака. Поэтому Azure идёт раньше Google."""

    def test_azure_stands_before_google(self):
        source = open(bs.__file__, encoding="utf-8").read()
        начало = source.index("translate_chain: list[tuple[str, callable, int]] = []")
        кусок = source[начало:начало + 700]
        assert кусок.index("azure_translator") < кусок.index("google_translate"), (
            "Google снова впереди Azure — на масштабе это платный запасной впереди бесплатного"
        )
        assert кусок.index("deepl_free") < кусок.index("azure_translator")


class TestTheExplanationIsShownBeforeTheBreakdown:
    """Объяснение идиомы приходит СРАЗУ, до «Подробного разбора».

    Владелец 10.09.2026: «на идиоме до нажатия человек секунду видит буквальный
    перевод». Объяснение уже лежит в справочнике — то самое, по которому мы опознали
    выражение. Лишних запросов ноль."""

    def test_the_article_travels_with_the_verdict(self, monkeypatch):
        import backend.german_expressions as ge
        статья = {"lemma": IDIOM, "kind": "idiom",
                  "meaning_de": "zäh, robust und hart im Nehmen sein",
                  "source": "de.wiktionary:Kategorie:Redewendung (Deutsch)"}
        monkeypatch.setattr(ge, "expression_of", lambda text: статья)
        вид, пришло = bs._resolve_input_kind_with_expression(IDIOM, "de")
        assert вид == "phrase"
        assert значение_из(пришло) == "zäh, robust und hart im Nehmen sein"

    def test_a_short_idiom_is_asked_too(self, monkeypatch):
        """Короткую идиому («auf taube Ohren stoßen», 4 слова) правило формы называет
        фразой само — но объяснение у неё есть, и до 10.09.2026 его никто не читал."""
        import backend.german_expressions as ge
        спрошено = []

        def _запомни(text):
            спрошено.append(text)
            return {"lemma": text, "kind": "idiom", "meaning_de": "kein Gehör finden", "source": "x"}

        monkeypatch.setattr(ge, "expression_of", _запомни)
        вид, пришло = bs._resolve_input_kind_with_expression("auf taube Ohren stoßen", "de")
        assert спрошено == ["auf taube Ohren stoßen"]
        assert вид == "phrase" and значение_из(пришло) == "kein Gehör finden"

    def test_a_single_word_still_never_asks(self, monkeypatch):
        import backend.german_expressions as ge

        def _взорвись(text):
            raise AssertionError("справочник спрошен про одиночное слово")

        monkeypatch.setattr(ge, "expression_of", _взорвись)
        вид, пришло = bs._resolve_input_kind_with_expression("raten", "de")
        assert вид == "word" and пришло is None

    def test_the_screen_shows_it(self):
        from pathlib import Path
        overlay = Path(bs.__file__).resolve().parents[1] / "frontend/src/dictionary/DictionaryOverlay.jsx"
        текст = overlay.read_text(encoding="utf-8")
        assert "dq-expression" in текст and "По-немецки объясняют так" in текст, (
            "объяснение выражения не доезжает до экрана"
        )


def значение_из(статья):
    return (статья or {}).get("meaning_de")
