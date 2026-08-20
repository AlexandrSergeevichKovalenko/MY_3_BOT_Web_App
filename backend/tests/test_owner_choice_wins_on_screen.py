"""Перевод, выбранный владельцем руками, стоит на экране ПЕРВЫМ и не исчезает.

Замер 20.08.2026 по 119 решениям владельца о спорных фразах. В базе всё было записано
верно — связь «вычитка» с рангом 1, — а на экране его выбор проигрывал машинному
переводу в 56 случаях и пропадал совсем в 3. Две независимые причины:

  1. ПОРЯДОК. Выдача сортировала переводы сначала по «есть ли у связи номер значения», и
     только потом по рангу. Номер значения машина ставит, а связь, которую человек
     завёл рукой, его не получает. «Es tut mir leid wegen der Verwirrung!»: выбрано
     «Извините из-за путаницы!», показано «Извините за путаницу!».
  2. ОТСЕВ ПЕРЕСКАЗОВ. Правило «из двух оставляем короткий» выбрасывало выбор владельца,
     если внутри него сидел другой перевод: «перелезать» ⊂ «Перелезать через что-то».

Оба правила по отдельности верны и остаются в силе — они просто не знали, что решение
человека выше машинной сортировки.
"""
from backend.lex_units import (
    OWNER_CHOICE_SOURCE,
    drop_nested_translations,
    is_owner_choice,
)


def _link(display, *, rank, source="разбор"):
    return {"display": display, "rank": rank, "source": source}


def test_owner_link_is_recognised_only_at_rank_one():
    assert is_owner_choice(_link("не хватать терпения", rank=1, source=OWNER_CHOICE_SOURCE))
    # Той же подписью помечены ПОНИЖЕННЫЕ связи слов, которые владелец забраковал
    # раньше (ранг 950). Поднимать их наверх нельзя — это его же отказ.
    assert not is_owner_choice(_link("оклад", rank=950, source=OWNER_CHOICE_SOURCE))
    assert not is_owner_choice(_link("недоставать терпения", rank=1))


def test_owner_choice_survives_the_shorter_wins_rule():
    # Живой случай: «Über etwas steigen», выбор владельца «перелезать через что-то».
    values = ["перелезать через что-то", "переступать через что-то", "перелезать"]
    without = drop_nested_translations(values)
    assert "перелезать через что-то" not in without, "иначе тест не про тот дефект"

    kept = drop_nested_translations(values, protected={"перелезать через что-то"})
    assert kept[0] == "перелезать через что-то"
    # Пересказ убрать всё равно надо — просто не тот, который выбрал человек.
    assert "перелезать" not in kept


def test_owner_choice_survives_being_a_longer_variant():
    # Карибский круиз: пул хранил обрубок, владелец выбрал полную фразу.
    short = "карибский круиз спокойно перенесет вас в сказочные места, такие как"
    full = short + " эти"
    kept = drop_nested_translations([full, short], protected={full})
    assert kept == [full]


def test_protection_never_empties_the_list():
    # Защищены оба — не выбрасываем ни одного: пустая карточка хуже пересказа.
    kept = drop_nested_translations(["деньги", "деньги (разг.)"],
                                    protected={"деньги", "деньги (разг.)"})
    assert len(kept) == 2


def test_rule_is_untouched_when_nothing_is_protected():
    # Без выбора владельца правило работает ровно как раньше: остаётся короткий.
    assert drop_nested_translations(["скобка", "скобка, скрепка"]) == ["скобка"]
    assert drop_nested_translations(["деньги", "деньги (разг.)"]) == ["деньги (разг.)"]


def test_query_asks_the_database_for_owner_first():
    """Порядок задаётся в SQL, поэтому и стережём его в SQL.

    Проверяем не текст ради текста: если убрать эту строку из ORDER BY или поставить её
    ПОСЛЕ сортировки по номеру значения, дефект 20.08.2026 вернётся целиком, а по данным
    в базе его не увидеть — там всё лежит верно.
    """
    from backend import lex_units

    captured = {}

    class _Cursor:
        def execute(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params

        def fetchall(self):
            return []

    lex_units._fetch_links(_Cursor(), 1, want_lang="ru")
    sql = captured["sql"]
    assert OWNER_CHOICE_SOURCE in captured["params"], "подпись выбора владельца не уехала в запрос"
    order = sql[sql.index("ORDER BY"):]
    owner_clause = order.index("l.source IS DISTINCT FROM")
    sense_clause = order.index("l.sense_id IS NULL")
    assert owner_clause < sense_clause, "решение человека обязано стоять ВЫШЕ машинной сортировки"
