"""Примеры на экране: сначала с общего слова, свои — ниже, ничего не теряется.

Владелец 22.08.2026 прислал экран «die Hose anhaben». Он сохранил карточку со смыслом
«быть главным», слово я пересобрал под этот смысл — и ЗНАЧЕНИЕ на экране поменялось на
«быть главным», а ПРИМЕРЫ остались про брюки: «Welche Hose hast du heute an?». Подпись и
содержимое снова про разное.

ПРИЧИНА. Склейка «личная карточка + общее слово» выбирала между двумя списками, и
последним правилом стояло «своих больше — оставляем своё». Оно писалось для синонимов
(три синонима против одного — отнимать нельзя), но на примерах держало УСТАРЕВШУЮ копию:
в карточке с 02.06 лежало два примера, на пересобранном слове — два свежих, и старые
побеждали по длине.

ПОЧЕМУ НЕ «ОБЩЕЕ ВСЕГДА ПОБЕЖДАЕТ». Замер 22.08.2026: из 9 915 пар «карточка + слово» с
примерами у 551 своих больше, и обычно это тот же пример плюс ещё пара. Простая победа
общего отняла бы у них примеры — прямо против правила «человек не должен увидеть меньше,
чем видел вчера».

РЕШЕНИЕ: не выбор, а порядок. Свежее с общего слова сверху, своё следом, повторы не
дублируются. После правки: из 877 отставших карточек свежий пример стоит первым у 857.
Оставшиеся 20 — те, где карточка вообще про другое слово («Die Maße» при слове «das Maß»,
опечатка в тексте), и там срабатывает отдельная защита по заголовку. Это верно.
"""
from backend.database import merge_unit_card_for_serve


def _ex(source, target=""):
    return {"source": source, "target": target}


class TestExamplesAreMerged:
    def test_the_owners_case(self):
        card = {"usage_examples": [_ex("Welche Hose hast du heute an?", "В каких брюках?")]}
        unit = {"usage_examples": [
            _ex("Wenn sie zu Hause die Hose anhat, entscheidet sie über alles.", "…"),
            _ex("Wer hier die Hose anhat, bestimmt die Regeln.", "…"),
        ]}
        merged = merge_unit_card_for_serve(card, unit)
        shown = [item["source"] for item in merged["usage_examples"]]
        assert shown[0] == "Wenn sie zu Hause die Hose anhat, entscheidet sie über alles."
        assert "Welche Hose hast du heute an?" in shown, "личный пример пропал"

    def test_nothing_is_lost_when_the_person_has_more(self):
        card = {"usage_examples": [_ex("A"), _ex("B"), _ex("C")]}
        unit = {"usage_examples": [_ex("Z")]}
        merged = merge_unit_card_for_serve(card, unit)
        shown = [item["source"] for item in merged["usage_examples"]]
        assert shown[0] == "Z", "свежее обязано стоять первым"
        assert set(shown) == {"Z", "A", "B", "C"}, "личные примеры потерялись"

    def test_the_same_example_is_not_doubled(self):
        card = {"usage_examples": [_ex("Один и тот же."), _ex("Свой.")]}
        unit = {"usage_examples": [_ex("один и тот же."), _ex("Общий.")]}
        merged = merge_unit_card_for_serve(card, unit)
        shown = [item["source"] for item in merged["usage_examples"]]
        assert len(shown) == 3, f"повтор не схлопнулся: {shown}"
        assert shown[:2] == ["один и тот же.", "Общий."]

    def test_an_empty_shared_block_leaves_the_personal_one(self):
        card = {"usage_examples": [_ex("Своё.")]}
        merged = merge_unit_card_for_serve(card, {"usage_examples": []})
        assert [i["source"] for i in merged["usage_examples"]] == ["Своё."]

    def test_a_card_without_examples_takes_the_shared_ones(self):
        merged = merge_unit_card_for_serve({}, {"usage_examples": [_ex("Общий.")]})
        assert [i["source"] for i in merged["usage_examples"]] == ["Общий."]


class TestOtherBlocksKeepTheOldRule:
    """Правило «своих больше — оставляем своё» остаётся везде, кроме примеров.

    Оно там не случайно: три синонима против одного отнимать нельзя.
    """

    def test_synonyms_still_prefer_the_longer_personal_list(self):
        card = {"synonyms": [{"word": "A"}, {"word": "B"}, {"word": "C"}]}
        unit = {"synonyms": [{"word": "Z"}]}
        merged = merge_unit_card_for_serve(card, unit)
        assert len(merged["synonyms"]) == 3
