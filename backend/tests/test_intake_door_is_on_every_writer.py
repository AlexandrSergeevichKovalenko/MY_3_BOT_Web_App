"""Дверь обязана стоять на ДНЕ записи, а не у каждого входа.

Так дырка и открывалась заново: чистка жила у той двери, которую сейчас правили, а
писали в те же таблицы ещё пять путей — бот, «Ярлык», ночной добор, импорты, разовые
скрипты. Поэтому механическая чистка стоит в трёх функциях, ниже которых записи нет:

    _create_or_attach_user_dictionary_entry_with_cursor  — единственный писатель карточки
    _upsert_dictionary_canonical_entry_with_cursor       — единственный писатель словаря
    lex_units.ensure_unit                                — единственный заводчик единицы

Пропадёт вызов из любой — и мимо чистки снова пойдёт всё, что пишет не через веб.
"""
import pathlib
import re

BACKEND = pathlib.Path(__file__).resolve().parents[1]


def _body_of(text: str, name: str) -> str:
    start = text.index(f"def {name}(")
    rest = text[start:]
    nxt = re.search(r"\ndef ", rest[1:])
    return rest[: nxt.start()] if nxt else rest


def test_card_writer_cleans_its_input():
    body = _body_of((BACKEND / "database.py").read_text(encoding="utf-8"),
                    "_create_or_attach_user_dictionary_entry_with_cursor")
    assert "intake.clean_all(" in body, "карточка пишется мимо общей двери"


def test_pool_writer_cleans_its_input():
    body = _body_of((BACKEND / "database.py").read_text(encoding="utf-8"),
                    "_upsert_dictionary_canonical_entry_with_cursor")
    assert "intake.clean_all(" in body, "общий словарь пишется мимо общей двери"


def test_unit_writer_cleans_its_input():
    body = _body_of((BACKEND / "lex_units.py").read_text(encoding="utf-8"), "ensure_unit")
    assert "clean_text(" in body, "единица заводится мимо общей двери"


def test_shortcut_uses_the_common_door_instead_of_its_own():
    """У «Ярлыка» была своя чистка. Теперь общая идёт первой, а своё остаётся только
    про мусор из скриншотов — иначе два места правят одно и расходятся."""
    body = _body_of((BACKEND / "backend_server.py").read_text(encoding="utf-8"),
                    "_shortcut_normalize_unit_text")
    assert "dictionary_intake.clean_text(" in body
