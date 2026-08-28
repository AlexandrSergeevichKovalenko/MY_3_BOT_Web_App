"""Артикль принадлежит СЛОВУ, а не фразе.

Владелец 22.08.2026 прислал экран: запрос «Ноющая, тянущая боль в боку», в ответе над
заголовком «ziehender, dumpfer Schmerz in der Seite» отдельной строкой стоит «der», а
фраза помечена «существительное». Несогласованно в обе стороны: при артикле окончания
прилагательных должны быть слабыми («der ziehende, dumpfe Schmerz»), без артикля —
сильными, как и написано. Человек заучивает смесь.

Машинка авторитетного артикля тут ни при чём — она давно работает только на одном слове.
Артикль приходит ПОЛЕМ МОДЕЛИ: она разбирает существительное ВНУТРИ фразы и подписывает
им всю фразу. На многословном его не снимал никто: функция выходила раньше, на проверке
`entry_kind != 'word'`, а прежняя защита ловила только `entry_kind = sentence`.

Замер 22.08.2026: 518 разборов из 10 337 помечены существительным при многословном
заголовке, у 208 заголовок начинается со строчной — там артикль виден как ошибка сразу.

Чиним ПОМЕТКУ, а не только её последствие: пока фраза считается существительным, артикль
будет приклеиваться снова, просто в другом месте.
"""
from backend.backend_server import _apply_german_headword_normalization


def _run(payload):
    return _apply_german_headword_normalization(
        payload=payload, source_lang="de", target_lang="ru")


class TestPhraseLosesTheArticle:
    def test_the_owners_case(self):
        result = _run({
            "entry_kind": "phrase",
            "part_of_speech": "noun",
            "article": "der",
            "source_text": "ziehender, dumpfer Schmerz in der Seite",
            "word_de": "ziehender, dumpfer Schmerz in der Seite",
        })
        assert result["article"] == ""
        assert result["part_of_speech"] == "phrase"
        assert result["source_text"] == "ziehender, dumpfer Schmerz in der Seite"

    def test_a_verb_phrase_labelled_a_noun_is_relabelled(self):
        result = _run({
            "entry_kind": "phrase", "part_of_speech": "noun", "article": "das",
            "source_text": "Pfand zurückgeben", "word_de": "Pfand zurückgeben",
        })
        assert result["article"] == ""
        assert result["part_of_speech"] == "phrase"

    def test_a_glued_article_is_stripped_from_the_text_too(self):
        result = _run({
            "entry_kind": "sentence", "part_of_speech": "noun", "article": "der",
            "source_text": "der Im Flur brennt das Licht",
            "word_de": "der Im Flur brennt das Licht",
        })
        assert result["article"] == ""
        assert not result["source_text"].startswith("der Im Flur")


class TestSingleWordsAreUntouched:
    def test_a_noun_keeps_its_article(self):
        result = _run({
            "entry_kind": "word", "part_of_speech": "noun", "article": "die",
            "source_text": "die Brücke", "word_de": "die Brücke",
        })
        assert result["article"] == "die"
        assert result["part_of_speech"] == "noun"

    def test_a_bare_noun_keeps_its_article(self):
        result = _run({
            "entry_kind": "word", "part_of_speech": "noun", "article": "das",
            "source_text": "Haus", "word_de": "Haus",
        })
        assert result["article"] == "das"

    def test_a_phrase_without_an_article_is_left_alone(self):
        payload = {
            "entry_kind": "phrase", "part_of_speech": "phrase", "article": "",
            "source_text": "die Hose anhaben", "word_de": "die Hose anhaben",
        }
        result = _run(payload)
        assert result["article"] == ""
        assert result["part_of_speech"] == "phrase"
        assert result["source_text"] == "die Hose anhaben"


class TestTheLiveAnswerToo:
    """Правило обязано стоять на ЖИВОМ ответе, а не только на сохранении.

    Утром 22.08.2026 я поставил его в `_prepare_dictionary_response_json_for_save` и
    проверил вызовом функции. Владелец в тот же день прислал экран, где оно не
    сработало: он набрал «Aus Gag» в быстром словаре, ответ был помечен «машинный
    перевод — этого слова нет в словаре», то есть через сохранение не проходил вовсе,
    и над двумя словами снова стояло «die». Проверять надо было путём экрана.

    Теперь правило стоит в общем сборщике ответа — через него идут и живой поиск, и
    обогащение карточки; на сохранении оно остаётся вторым рубежом для импортов.
    """

    def _build(self, raw):
        import backend.backend_server as server
        item, *_rest = server._build_dictionary_result_from_raw(
            raw=raw, query_word=raw.get("word_source", ""),
            source_lang="de", target_lang="ru",
            query_source_lang="de", query_target_lang="ru", lookup_lang="de")
        return item

    def test_the_owners_case_on_the_live_path(self):
        item = self._build({
            "detected_language": "source", "word_source": "Aus Gag",
            "word_target": "шутка", "article": "die", "part_of_speech": "noun",
            "entry_kind": "word",
            "translations": [{"value": "шутка", "is_primary": True}],
        })
        assert item.get("article") == ""
        assert item.get("part_of_speech") == "phrase"

    def test_a_single_noun_keeps_its_article_on_the_live_path(self):
        item = self._build({
            "detected_language": "source", "word_source": "Brücke",
            "word_target": "мост", "article": "die", "part_of_speech": "noun",
            "entry_kind": "word",
            "translations": [{"value": "мост", "is_primary": True}],
        })
        assert item.get("article") == "die"
        assert item.get("part_of_speech") == "noun"


class TestTheServePointIsTheLastWord:
    """Правило стоит там, где карточка УХОДИТ человеку, — это третий заход.

    Сначала я закрыл его на пути сохранения, потом в общем сборщике ответа. Оба раза
    владелец видел «die Aus Gag» на экране: эндпоинт быстрого словаря до сборщика не
    доходит — он отдаёт карточку из кеша, из пула или с обратной стороны, а сборщик
    зовётся только на четвёртом пути, при свежем походе к модели.

    `_serve_dictionary_item` — единственная общая точка на все семь путей. И правило
    обязано стоять ДО построения таблиц: иначе склонение успевает родиться на
    несуществующем слове.
    """

    def _serve(self, item):
        import backend.backend_server as server
        return server._serve_dictionary_item(dict(item))

    def test_a_phrase_from_the_pool_loses_the_article(self):
        out = self._serve({
            "word_de": "Aus Gag", "source_text": "Aus Gag", "article": "die",
            "part_of_speech": "noun", "entry_kind": "word",
            "language_pair": {"source_lang": "de", "target_lang": "ru"},
            "translations": [{"value": "шутка", "is_primary": True}],
        })
        assert out.get("article") == ""
        assert out.get("part_of_speech") == "phrase"

    def test_no_declension_table_is_built_for_a_phrase(self):
        out = self._serve({
            "word_de": "Aus Gag", "source_text": "Aus Gag", "article": "die",
            "part_of_speech": "noun", "entry_kind": "word",
            "forms": {"plural": "Aus Gags"},
            "language_pair": {"source_lang": "de", "target_lang": "ru"},
        })
        assert not (out.get("grammar_tables") or {}).get("declension"), \
            "таблица склонения построена на несуществующем слове"

    def test_a_real_noun_keeps_its_article_on_the_way_out(self):
        out = self._serve({
            "word_de": "Brücke", "source_text": "Brücke", "article": "die",
            "part_of_speech": "noun", "entry_kind": "word",
            "language_pair": {"source_lang": "de", "target_lang": "ru"},
        })
        assert out.get("article") == "die"
        assert out.get("part_of_speech") == "noun"


class TestGermanSideComesFromTheCardNotFromTheLanguageSetting:
    """Немецкую сторону берём из карточки, а не из настройки языковой пары.

    НАЙДЕНО 28.08.2026. Правило «многословное — не существительное» верное, но опору
    под ним перенесли из прежней узкой версии не глядя: немецкую сторону выбирали по
    языковой паре пользователя. У пары «русский → немецкий» — а это большинство —
    немецкой стороной объявлялся `target_text`, где лежит РУССКИЙ перевод. «рыночная
    цена» это два слова, поэтому обычное существительное «der Marktpreis» теряло
    артикль и переклеивалось в «фразу».

    Замер 27.08.2026 на одном слове, ответ модели побуквенно одинаковый:
        пара de→ru:  article='der'  part_of_speech='noun'
        пара ru→de:  article=''     part_of_speech='phrase'   ← дефект

    Почему это жило незамеченным: во всех тестах этого файла (и трёх соседних) была
    прописана ТОЛЬКО пара de→ru — то есть ровно тот случай, где ошибки нет. Поэтому
    здесь пара развёрнута нарочно.
    """

    @staticmethod
    def _run_ru_de(payload):
        # Языковая пара человека: учит немецкий, объяснения по-русски.
        return _apply_german_headword_normalization(
            payload=payload, source_lang="ru", target_lang="de")

    def test_a_noun_with_a_multiword_translation_keeps_its_article(self):
        result = self._run_ru_de({
            "entry_kind": "word",
            "part_of_speech": "noun",
            "article": "der",
            "word_de": "der Marktpreis",
            "source_text": "der Marktpreis",
            "target_text": "рыночная цена",
        })
        assert result["part_of_speech"] == "noun", (
            "существительное объявлено фразой из-за длины РУССКОГО перевода"
        )
        assert result["article"] == "der", "у существительного отобрали артикль"
        assert result["target_text"] == "рыночная цена", "русский перевод переписан"

    def test_the_owners_phrase_is_still_caught_on_the_other_pair(self):
        """Правка не ослабляет правило: настоящая фраза теряет артикль на любой паре."""
        result = self._run_ru_de({
            "entry_kind": "phrase",
            "part_of_speech": "noun",
            "article": "der",
            "word_de": "ziehender, dumpfer Schmerz in der Seite",
            "source_text": "ziehender, dumpfer Schmerz in der Seite",
            "target_text": "ноющая, тянущая боль в боку",
        })
        assert result["article"] == ""
        assert result["part_of_speech"] == "phrase"

    def test_the_german_text_is_never_written_into_the_translation_field(self):
        """Исправленный заголовок уходит в немецкое поле, а не в поле перевода."""
        result = self._run_ru_de({
            "entry_kind": "word",
            "part_of_speech": "noun",
            "article": "der",
            "word_de": "der Im Flur brennt das Licht",
            "source_text": "der Im Flur brennt das Licht",
            "target_text": "в коридоре горит свет",
        })
        assert result["word_de"] == "Im Flur brennt das Licht"
        assert result["source_text"] == "Im Flur brennt das Licht"
        assert result["target_text"] == "в коридоре горит свет", (
            "немецкий заголовок уехал в поле русского перевода"
        )

    def test_a_bare_noun_keeps_its_article_on_the_learners_pair(self):
        result = self._run_ru_de({
            "entry_kind": "word",
            "part_of_speech": "noun",
            "article": "die",
            "word_de": "Brücke",
            "source_text": "die Brücke",
            "target_text": "мост, переправа через реку",
        })
        assert result["article"] == "die"
        assert result["part_of_speech"] == "noun"
