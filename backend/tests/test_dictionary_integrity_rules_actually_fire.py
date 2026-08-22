"""Сводная проверка словаря обязана УМЕТЬ СРАБАТЫВАТЬ.

Владелец 22.08.2026: «есть какой-то предел? мы должны строить стражей». Предел выглядит
как одно число, которое приходит ночью: ноль — словарь чист. Но число «ноль» стоит ровно
столько, сколько стоит проверка, которая его посчитала: правило, которое не срабатывает
никогда, отчитывается нулём и в чистой базе, и в грязной.

Поэтому здесь каждому правилу подсовывается запись, которая его нарушает, и проверяется,
что оно её ВИДИТ. Тест — сторож сторожей.
"""
import pytest

from backend import dictionary_integrity as integrity


class _Cursor:
    """Подставная база: отдаёт заготовленные строки на любой запрос."""

    def __init__(self, rows):
        self._rows = rows

    def execute(self, sql, params=None):
        # Правило «разбор лицом не в ту сторону» читает порциями по id — вторую порцию
        # отдаём пустой, иначе цикл не кончится.
        if params and self._served:
            self._rows = []
        self._served = True

    _served = False

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_phrase_labelled_a_noun_is_seen():
    count, sample = integrity._rule_phrase_is_not_a_noun(
        _Cursor([(1, "ziehender, dumpfer Schmerz in der Seite")]))
    assert count == 1 and sample


def test_a_single_word_noun_is_not_a_violation():
    count, _ = integrity._rule_phrase_is_not_a_noun(_Cursor([(1, "die Brücke")]))
    assert count == 0


def test_gender_on_a_phrase_is_seen():
    count, _ = integrity._rule_gender_on_multiword(_Cursor([(1, "Pfand zurückgeben")]))
    assert count == 1


def test_school_tail_is_seen():
    # Правило целиком в SQL, поэтому здесь проверяем, что счётчик считает отданное.
    count, _ = integrity._rule_school_tail(_Cursor([(1, "die Brücke, -n")]))
    assert count == 1


def test_russian_hint_inside_german_is_seen():
    count, _ = integrity._rule_russian_hint_inside_german(
        _Cursor([(1, "abheben (снять трубку)")]))
    assert count == 1


def test_an_ordinary_russian_note_is_not_a_violation():
    # «деньги (разг.)» — законное русское уточнение, снаружи скобки кириллица.
    count, _ = integrity._rule_russian_hint_inside_german(_Cursor([(1, "деньги (разг.)")]))
    assert count == 0


def test_a_word_invisible_to_the_night_is_seen():
    # Одно слово, но записано оборотом и без разбора — ночь его не возьмёт никогда.
    count, _ = integrity._rule_invisible_to_night(_Cursor([(1, "die Rutsche")]))
    assert count == 1


def test_a_real_phrase_without_a_card_is_not_a_violation():
    count, _ = integrity._rule_invisible_to_night(_Cursor([(1, "die Hose anhaben")]))
    assert count == 0


def test_a_mirrored_card_is_seen():
    card = {"word_source": "Диаграмма", "word_target": "das Diagramm"}
    count, _ = integrity._rule_card_faces_away(_Cursor([(1, "de", "das Diagramm", card)]))
    assert count == 1


def test_a_pool_answer_in_the_wrong_language_is_seen():
    count, _ = integrity._rule_pool_answers_wrong_language(
        _Cursor([(1, "Укладывать", "Складывать", "de")]))
    assert count == 1


def test_an_exercise_blank_in_the_pool_is_seen():
    count, _ = integrity._rule_pool_answers_wrong_language(
        _Cursor([(1, "Ich ___ mein Geld", "anlegen", "de")]))
    assert count == 1


def test_a_healthy_pool_row_is_not_a_violation():
    count, _ = integrity._rule_pool_answers_wrong_language(
        _Cursor([(1, "die Brücke", "мост", "ru")]))
    assert count == 0


def test_every_rule_is_listed_in_the_report():
    """Правило, забытое в списке, не гоняется — и его ноль ничего не значит."""
    listed = {rule for _title, rule in integrity.RULES}
    defined = {value for name, value in vars(integrity).items()
               if name.startswith("_rule_") and callable(value)}
    assert defined == listed, "правило написано, но в список проверки не включено"


def test_a_broken_rule_is_reported_and_not_counted_as_clean(monkeypatch):
    """Упавшее правило обязано быть видно: молчание неотличимо от чистоты."""
    def _explode(cur):
        raise RuntimeError("нет такой колонки")

    monkeypatch.setattr(integrity, "RULES", (("подставное правило", _explode),))

    class _Conn:
        def cursor(self):
            return _Cursor([])

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(integrity, "get_db_connection_context", lambda **kw: _Conn())
    result = integrity.run()
    assert result["failed"], "падение правила потерялось"
    assert "подставное правило" in result["failed"]
