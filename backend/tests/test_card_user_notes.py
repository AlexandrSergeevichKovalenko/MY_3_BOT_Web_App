"""Личные заметки к слову: что можно хранить, а что отсекается.

Заметка принадлежит одному человеку и лежит отдельной колонкой при его карточке — не
внутри разбора. Это и есть главная гарантия: общий разбор слова улучшается для всех и
никогда не затрёт чужую заметку, а заметка никогда не уедет другим людям.

Правила длины и потолка живут ЗДЕСЬ, а не в интерфейсе: интерфейсов несколько
(карточка, повторение, дальше будет бот), а правило должно быть одно.
"""
from backend.database import (
    USER_NOTES_MAX,
    USER_NOTE_LABEL_MAX,
    USER_NOTE_TEXT_MAX,
    normalize_user_notes,
)


def test_note_keeps_label_and_text():
    notes = normalize_user_notes([{"label": "Ассоциация", "text": "вандал наоборот"}])
    assert notes == [{"label": "Ассоциация", "text": "вандал наоборот"}]


def test_label_is_optional():
    """Человек может просто написать текст — заставлять придумывать название нельзя."""
    assert normalize_user_notes([{"text": "просто мысль"}]) == [{"label": "", "text": "просто мысль"}]
    assert normalize_user_notes(["строкой тоже"]) == [{"label": "", "text": "строкой тоже"}]


def test_empty_note_is_dropped_silently():
    """Нажал «плюс» и передумал — это не ошибка, просто пустая строка не сохраняется."""
    assert normalize_user_notes([{"label": "Есть", "text": "   "}, {"text": ""}]) == []


def test_more_than_the_limit_is_cut():
    many = [{"text": f"заметка {i}"} for i in range(USER_NOTES_MAX + 4)]
    assert len(normalize_user_notes(many)) == USER_NOTES_MAX


def test_long_values_are_trimmed_not_refused():
    """Обрезаем, а не отказываем: человек уже написал, терять текст целиком обиднее."""
    notes = normalize_user_notes([{"label": "Я" * 80, "text": "с" * 900}])
    assert len(notes[0]["label"]) == USER_NOTE_LABEL_MAX
    assert len(notes[0]["text"]) == USER_NOTE_TEXT_MAX


def test_junk_input_gives_an_empty_list():
    for value in (None, "заметка", 42, {"text": "не список"}, [123, None, []]):
        assert normalize_user_notes(value) == [], value


def test_order_is_kept():
    notes = normalize_user_notes([{"text": "первая"}, {"text": "вторая"}, {"text": "третья"}])
    assert [n["text"] for n in notes] == ["первая", "вторая", "третья"]
