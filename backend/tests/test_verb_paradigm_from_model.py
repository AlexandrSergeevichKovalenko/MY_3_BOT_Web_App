# -*- coding: utf-8 -*-
"""Справочника нет — спрашиваем модель, и только при СОВПАДЕНИИ двух ответов.

Владелец 23.08.2026: «как мы можем просто брать и механически что-то делать, когда это
касается языка? у нас же есть либо справочник, либо, если справочника нет, нужно
запрашивать у модели. а как мы строим это механически — что это за бред?»

До этого дня у глаголов ступени с моделью не было вовсе: каскад обрывался на справочнике,
а таблицу досчитывала наша арифметика. Здесь проверяется новая ступень — та же, что у
существительных и прилагательных с 17.08.2026: два независимых спроса, ответ принимается
только при полном совпадении, иначе честное «не знаю».
"""
from backend import german_verb_paradigms as V

GEHEN = {
    "praesens": {"ich": "gehe", "du": "gehst", "er/sie/es": "geht",
                 "wir": "gehen", "ihr": "geht", "sie/Sie": "gehen"},
    "praeteritum": {"ich": "ging", "du": "gingst", "er/sie/es": "ging",
                    "wir": "gingen", "ihr": "gingt", "sie/Sie": "gingen"},
    "imperativ": {"du": "geh", "ihr": "geht"},
    "partizip2": "gegangen",
    "auxiliary": "sein",
}


def _answers(monkeypatch, *replies):
    """Подставить ответы модели по очереди и посчитать, сколько раз её спросили."""
    calls = {"n": 0}

    def fake(verb):
        index = min(calls["n"], len(replies) - 1)
        calls["n"] += 1
        return replies[index]

    monkeypatch.setattr(V, "_ask_paradigm_once", fake)
    return calls


class TestTwoAnswersMustAgree:
    def test_identical_answers_are_accepted(self, monkeypatch):
        calls = _answers(monkeypatch, GEHEN, GEHEN)
        result, reason = V.paradigm_from_model("gehen")
        assert calls["n"] == 2, "спрашивать обязаны дважды"
        assert reason == ""
        assert result["praesens"]["du"] == "gehst"
        assert result["auxiliary"] == "sein"

    def test_one_differing_cell_kills_the_whole_answer(self, monkeypatch):
        other = {**GEHEN, "praeteritum": {**GEHEN["praeteritum"], "ihr": "ginget"}}
        _answers(monkeypatch, GEHEN, other)
        assert V.paradigm_from_model("gehen") == (None, V.DISAGREED)

    def test_a_different_auxiliary_kills_it(self, monkeypatch):
        _answers(monkeypatch, GEHEN, {**GEHEN, "auxiliary": "haben"})
        assert V.paradigm_from_model("gehen") == (None, V.DISAGREED)

    def test_an_empty_answer_is_not_agreement(self, monkeypatch):
        _answers(monkeypatch, GEHEN, {})
        assert V.paradigm_from_model("gehen") == (None, V.DISAGREED)


class TestSilenceIsNotAnAnswer:
    """Обрыв связи НЕ ОТВЕТ. Иначе ошибка сети записывается как «слово не подтвердилось».

    Ровно это случилось 23.08.2026 на первом прогоне починки: сеть рвалась, и настоящие
    глаголы «wehren», «anmaßen», «entpuppen» попали в список «таблицы нет» — навсегда,
    потому что повторно мы такие уже не спрашиваем.
    """

    def test_a_failed_call_is_reported_as_no_answer(self, monkeypatch):
        _answers(monkeypatch, None, GEHEN)
        assert V.paradigm_from_model("wehren") == (None, V.NO_ANSWER)

    def test_the_second_call_failing_counts_too(self, monkeypatch):
        _answers(monkeypatch, GEHEN, None)
        assert V.paradigm_from_model("wehren") == (None, V.NO_ANSWER)

    def test_we_do_not_pay_for_the_second_ask_after_the_first_failed(self, monkeypatch):
        calls = _answers(monkeypatch, None, GEHEN)
        V.paradigm_from_model("wehren")
        assert calls["n"] == 1, "первый спрос сорвался — второй платить незачем"


class TestWhatIsNotAVerbGetsNoTable:
    def test_the_model_may_say_it_is_not_a_verb(self, monkeypatch):
        """«besagt», «aspettiamo», «bore» спрягать нельзя — и это ОТВЕТ, а не отказ."""
        _answers(monkeypatch, {"not_a_verb": True}, {"not_a_verb": True})
        assert V.paradigm_from_model("aspettiamo") == (None, V.NOT_A_VERB)

    def test_one_vote_for_not_a_verb_is_no_agreement(self, monkeypatch):
        """Один спрос спрягает, другой отказывается — согласия нет, ярлыка тоже."""
        _answers(monkeypatch, GEHEN, {"not_a_verb": True})
        assert V.paradigm_from_model("bore") == (None, V.DISAGREED)


class TestTheShapeIsChecked:
    def test_an_auxiliary_outside_haben_sein_is_refused(self, monkeypatch):
        broken = {**GEHEN, "auxiliary": "werden"}
        _answers(monkeypatch, broken, broken)
        assert V.paradigm_from_model("gehen") == (None, V.DISAGREED)

    def test_without_praesens_there_is_no_table(self, monkeypatch):
        only_tail = {"partizip2": "gegangen", "auxiliary": "sein"}
        _answers(monkeypatch, only_tail, only_tail)
        assert V.paradigm_from_model("gehen") == (None, V.DISAGREED)

    def test_extra_spaces_do_not_count_as_disagreement(self, monkeypatch):
        spaced = {**GEHEN, "praesens": {**GEHEN["praesens"], "ich": " gehe "}}
        _answers(monkeypatch, GEHEN, spaced)
        table, reason = V.paradigm_from_model("gehen")
        assert reason == "" and table["praesens"]["ich"] == "gehe"


class TestTheServingPathNeverPaysForTheModel:
    def test_by_default_the_model_is_not_asked(self, monkeypatch):
        """Два обращения к модели на глазах у человека — это секунды и деньги.

        Днём выдача читает подтверждённое, спрашивает ночь (warm_verb_paradigms).
        """
        monkeypatch.setattr(V, "load_paradigm", lambda verb: None)
        monkeypatch.setattr(V, "_full_form_of_colloquial", lambda verb: "")
        monkeypatch.setattr(V, "_paradigm_from_base_verb",
                            lambda verb, allow_network=False: None)

        def explode(verb):
            raise AssertionError("модель спрошена на пути выдачи")

        monkeypatch.setattr(V, "paradigm_from_model", explode)
        assert V.paradigm_for_verb("irgendwas") is None

    def test_a_confirmed_answer_is_served_from_the_cache(self, monkeypatch):
        stored = {"модель:einleben": {**GEHEN}}
        monkeypatch.setattr(V, "load_paradigm", lambda verb: stored.get(verb))
        monkeypatch.setattr(V, "_full_form_of_colloquial", lambda verb: "")
        monkeypatch.setattr(V, "_paradigm_from_base_verb",
                            lambda verb, allow_network=False: None)
        result = V.paradigm_for_verb("einleben")
        assert result["source"] == "модель"
        assert result["infinitive"] == "einleben"

    def test_a_failed_confirmation_is_remembered_and_not_re_asked(self, monkeypatch):
        stored = {"модель:aspettiamo": {"reason": V.NOT_A_VERB}}
        monkeypatch.setattr(V, "load_paradigm", lambda verb: stored.get(verb))
        monkeypatch.setattr(V, "_full_form_of_colloquial", lambda verb: "")
        monkeypatch.setattr(V, "_paradigm_from_base_verb",
                            lambda verb, allow_network=False: None)

        def explode(verb):
            raise AssertionError("платим за тот же вопрос второй раз")

        monkeypatch.setattr(V, "paradigm_from_model", explode)
        assert V.paradigm_for_verb("aspettiamo", allow_model=True) is None

    def test_the_reference_still_wins_over_the_model(self, monkeypatch):
        monkeypatch.setattr(
            V, "load_paradigm",
            lambda verb: {"praesens": {"ich": "gehe"}} if verb == "gehen" else None)

        def explode(verb):
            raise AssertionError("модель спрошена, хотя справочник знает ответ")

        monkeypatch.setattr(V, "paradigm_from_model", explode)
        assert V.paradigm_for_verb("gehen", allow_model=True)["source"] == "wiktionary-flexion"


class TestAnUnansweredQuestionStaysOpen:
    """Сеть оборвалась — НИЧЕГО не записываем: вопрос остался, ночь спросит снова."""

    def test_nothing_is_written_when_the_model_did_not_answer(self, monkeypatch):
        monkeypatch.setattr(V, "load_paradigm", lambda verb: None)
        monkeypatch.setattr(V, "_full_form_of_colloquial", lambda verb: "")
        monkeypatch.setattr(V, "_paradigm_from_base_verb",
                            lambda verb, allow_network=False: None)
        monkeypatch.setattr(V, "paradigm_from_model", lambda verb: (None, V.NO_ANSWER))
        written: list = []
        monkeypatch.setattr(V, "store_paradigm",
                            lambda verb, tables: written.append((verb, tables)))
        assert V.paradigm_for_verb("wehren", allow_model=True) is None
        assert written == [], "обрыв связи записан как ответ"

    def test_a_real_no_is_written_down(self, monkeypatch):
        monkeypatch.setattr(V, "load_paradigm", lambda verb: None)
        monkeypatch.setattr(V, "_full_form_of_colloquial", lambda verb: "")
        monkeypatch.setattr(V, "_paradigm_from_base_verb",
                            lambda verb, allow_network=False: None)
        monkeypatch.setattr(V, "paradigm_from_model", lambda verb: (None, V.NOT_A_VERB))
        written: list = []
        monkeypatch.setattr(V, "store_paradigm",
                            lambda verb, tables: written.append((verb, tables)))
        assert V.paradigm_for_verb("aspettiamo", allow_model=True) is None
        assert written == [("модель:aspettiamo", {"reason": V.NOT_A_VERB})]
