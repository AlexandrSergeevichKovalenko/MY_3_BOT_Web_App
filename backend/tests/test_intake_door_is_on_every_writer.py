"""Дверь обязана стоять на ДНЕ записи, а не у каждого входа.

Так дырка и открывалась заново: чистка жила у той двери, которую сейчас правили, а
писали в те же таблицы ещё пять путей — бот, «Ярлык», ночной добор, импорты, разовые
скрипты. Поэтому механическая чистка стоит в трёх функциях, ниже которых записи нет:

    _create_or_attach_user_dictionary_entry_with_cursor  — единственный писатель карточки
    _upsert_dictionary_canonical_entry_with_cursor       — единственный писатель словаря
    lex_units.ensure_unit                                — заводчик единицы с живого пути
    lex_units.sync_unit_links_from_card                  — заводчик единиц из РАЗБОРА

Пропадёт вызов из любой — и мимо чистки снова пойдёт всё, что пишет не через веб.

Четвёртая в этом списке появилась 20.08.2026. До того она заводила единицы ПРЯМЫМ
запросом в базу: ни чистки, ни проверки языка, ни запрета на свалку значений. Зовут её
ночные работы и восемь скриптов — то есть мимо двери шёл поток, а не единичный случай.
Этот файл о ней не знал, поэтому и не поймал. Общая половина двери теперь вынесена в
`lex_units.door_check`, и обе функции спрашивают именно её.
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


def test_the_door_itself_still_cleans():
    body = _body_of((BACKEND / "lex_units.py").read_text(encoding="utf-8"), "door_check")
    assert "clean_text(" in body, "дверь перестала чистить текст"
    assert "text_matches_language(" in body, "дверь перестала проверять язык"
    assert "split_numbered_senses(" in body, "дверь перестала отбрасывать свалку значений"


def test_unit_writer_asks_the_door():
    body = _body_of((BACKEND / "lex_units.py").read_text(encoding="utf-8"), "ensure_unit")
    assert "door_check(" in body, "единица заводится мимо общей двери"


def test_card_analysis_writer_asks_the_door_too():
    """Единицы из разбора — четвёртый заводчик. Прямой INSERT без двери здесь и был
    дырой: разбор приносит склеенные значения и чужой язык не реже человека."""
    body = _body_of((BACKEND / "lex_units.py").read_text(encoding="utf-8"),
                    "sync_unit_links_from_card")
    assert "INSERT INTO bt_3_lex_units" in body, "тест устарел: запись переехала"
    assert "door_check(" in body, "единицы из разбора заводятся мимо общей двери"


def test_shortcut_uses_the_common_door_instead_of_its_own():
    """У «Ярлыка» была своя чистка. Теперь общая идёт первой, а своё остаётся только
    про мусор из скриншотов — иначе два места правят одно и расходятся."""
    body = _body_of((BACKEND / "backend_server.py").read_text(encoding="utf-8"),
                    "_shortcut_normalize_unit_text")
    assert "dictionary_intake.clean_text(" in body
