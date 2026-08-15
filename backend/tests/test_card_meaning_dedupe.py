"""Одно значение не показывается дважды.

Модель пишет один и тот же перевод повторно, меняя пояснение: у «entsorgen» пять
значений, а разных три — «утилизировать» и «избавляться» стоят по два раза. Замер
15.08.2026: в личных карточках 11 848 повторов из 33 954 значений (у 6 642 карточек),
на общих словах 614 из 13 799 (у 349 слов).

Сравниваем по ТЕКСТУ значения: пояснением модель как раз и оправдывает повтор.
"""
from backend.database import dedupe_card_meanings, merge_unit_card_for_serve


ENTSORGEN = {
    "dictionary_senses": [
        {"rank": 1, "value": "утилизировать", "context": "процесс удаления отходов"},
        {"rank": 2, "value": "избавляться", "context": "освобождаться от ненужного"},
        {"rank": 3, "value": "утилизировать", "context": "основное значение"},
        {"rank": 4, "value": "избавляться", "context": "в более общем смысле"},
        {"rank": 5, "value": "вывозить мусор", "context": "разговорное"},
    ],
}


def test_repeated_meaning_is_shown_once():
    kept = dedupe_card_meanings(dict(ENTSORGEN))["dictionary_senses"]
    assert [s["value"] for s in kept] == ["утилизировать", "избавляться", "вывозить мусор"]


def test_ranks_are_renumbered_without_holes():
    kept = dedupe_card_meanings(dict(ENTSORGEN))["dictionary_senses"]
    assert [s["rank"] for s in kept] == [1, 2, 3], "иначе на карточке будет «1, 3, 5»"


def test_first_occurrence_wins():
    """Первое вхождение идёт под первым рангом — оно признано главным."""
    kept = dedupe_card_meanings(dict(ENTSORGEN))["dictionary_senses"]
    assert kept[0]["context"] == "процесс удаления отходов"


def test_case_and_trailing_dot_do_not_make_a_new_meaning():
    card = {"dictionary_senses": [
        {"rank": 1, "value": "скобка"},
        {"rank": 2, "value": "Скобка"},
        {"rank": 3, "value": "скобка."},
        {"rank": 4, "value": "зажим"},
    ]}
    kept = dedupe_card_meanings(card)["dictionary_senses"]
    assert [s["value"] for s in kept] == ["скобка", "зажим"]


def test_translations_are_deduped_too():
    card = {"translations": [{"value": "скобка"}, {"value": "скобка"}, {"value": "зажим"}]}
    assert [t["value"] for t in dedupe_card_meanings(card)["translations"]] == ["скобка", "зажим"]


def test_different_meanings_are_all_kept():
    card = {"dictionary_senses": [
        {"rank": 1, "value": "изменение"}, {"rank": 2, "value": "перемена"},
    ]}
    assert len(dedupe_card_meanings(card)["dictionary_senses"]) == 2


def test_serving_a_card_removes_repeats():
    """Отсев стоит на общем пути показа, а не в одном экране."""
    served = merge_unit_card_for_serve(dict(ENTSORGEN), None, None)
    assert [s["value"] for s in served["dictionary_senses"]] == [
        "утилизировать", "избавляться", "вывозить мусор",
    ]


def test_junk_input_survives():
    assert dedupe_card_meanings({}) == {}
    assert dedupe_card_meanings({"dictionary_senses": None}) == {"dictionary_senses": None}
