"""Сторож правил словаря: они обязаны оставаться в схеме, а не только в живой базе.

Зачем это. До 06.08.2026 у таблицы карточек и у общего словаря не было НИ ОДНОГО
правила, кроме первичного ключа, а пишут в них пять разных путей: сохранение человеком,
ночной добор, импорт подписки, массовые сборки и разовые скрипты. Проверка в коде одного
пути остальных не касается — отсюда и брались записи, которые мы потом ловили и правили
задним числом: карточка без слова, русский текст в немецкой колонке, разбор в виде
строки вместо объекта, ненормализованный ключ, который не находится по своему же тексту.

Правила поставлены в САМУ БАЗУ и проверены на живой (06.08.2026): 22 попытки записать
мусор — 22 отказа, 8 честных записей приняты. Этот тест следит за другим: чтобы
ограничение не исчезло из файлов схемы при следующем рефакторинге. Пропадёт из файла —
на новой базе (или после пересоздания таблицы) его просто не будет, и дырка откроется
заново.

Правила НЕ привязаны к паре языков: «стороны разноязычные» и «немецкая сторона на
латинице» одинаково верны для немецко-русской и для планируемой немецко-английской пары.
"""
import pathlib
import re

BACKEND = pathlib.Path(__file__).resolve().parents[1]

UNIT_RULES = [
    "chk_lex_units_kind",
    "chk_lex_units_lang_code",
    "chk_lex_units_names_filled",
    "chk_lex_units_key_normalized",
    "chk_lex_units_gender_values",
    "chk_lex_units_gender_german_only",
    "chk_lex_units_card_is_object",
    "chk_lex_units_script_matches_lang",
    "uq_lex_units_id_lang",
    "chk_lex_surfaces_key_normalized",
    "fk_lex_surfaces_unit_lang",
    "chk_lex_links_not_self",
    "chk_lex_links_rank_sane",
]

CARD_RULES = [
    "chk_wdq_owner_set",
    "chk_wdq_has_a_side",
    "chk_wdq_german_side_is_latin",
    "chk_wdq_pair_is_two_languages",
    "chk_wdq_response_is_object",
    "chk_wdq_notes_is_array",
]

POOL_RULES = [
    "chk_pool_texts_filled",
    "chk_pool_keys_normalized",
    "chk_pool_pair_is_two_languages",
    "chk_pool_response_is_object",
]


def test_unit_layer_rules_stay_in_the_schema_file():
    text = (BACKEND / "lex_units_schema.sql").read_text(encoding="utf-8")
    missing = [name for name in UNIT_RULES if name not in text]
    assert not missing, f"правила пропали из lex_units_schema.sql: {missing}"


def test_card_and_pool_rules_stay_in_ensure_webapp_tables():
    text = (BACKEND / "database.py").read_text(encoding="utf-8")
    missing = [name for name in CARD_RULES + POOL_RULES if name not in text]
    assert not missing, f"правила пропали из database.py: {missing}"


def test_rules_are_added_idempotently():
    """Схема выполняется при каждом старте сервиса. Ограничение, добавленное без
    проверки «уже есть?», уронит запуск на втором деплое."""
    for path in ("lex_units_schema.sql", "database.py"):
        text = (BACKEND / path).read_text(encoding="utf-8")
        for name in UNIT_RULES + CARD_RULES + POOL_RULES:
            marker = f"ADD CONSTRAINT {name}"
            if marker not in text:
                continue
            head = text[: text.index(marker)]
            guards = list(re.finditer(r"IF NOT EXISTS\s*\(\s*SELECT 1 FROM pg_constraint", head))
            block = head.rfind("DO $$")
            assert guards and guards[-1].start() > block >= 0, (
                f"{name} в {path} добавляется без проверки «уже есть?»"
            )


def test_language_rules_do_not_hardcode_the_russian_pair():
    """Планируется немецко-английская пара. Правило «стороны разноязычные» и запрет
    кириллицы в немецкой колонке должны работать для любой пары, а не только для ru."""
    db = (BACKEND / "database.py").read_text(encoding="utf-8")
    start = db.index("chk_wdq_pair_is_two_languages")
    rule = db[start : start + 400]
    assert "source_lang <> target_lang" in rule
    assert "'ru'" not in rule, "правило пары не должно знать про конкретный язык"
