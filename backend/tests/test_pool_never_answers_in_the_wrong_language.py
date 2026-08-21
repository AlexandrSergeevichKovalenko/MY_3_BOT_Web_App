"""Общий словарь не отдаёт ответ на чужом языке и не хранит задания тренажёра.

Замер 21.08.2026 по 17 333 записям пула, правило отбора взято у продукта (поиск идёт по
`source_text` в паре source_lang→target_lang, `get_pool_dictionary_candidates`):

    ответ ЦЕЛИКОМ на чужом языке          48   ← «Укладывать» (рус→нем) → «Складывать»
    задания тренажёра вместо слова       143   ← «Ich ___ mein Geld…» → «anlegen»
                                               из них побеждали в поиске 119

ПОЧЕМУ ПРОВЕРЯЕТСЯ ТОЛЬКО ОТВЕТ. Если требовать чистоты и от запроса, отсеивается 334
записи, и в 133 «грязь» набрал сам человек («Мой телефон ist kaputt» → «Mein Handy ist
hin») — ответ там верный, а запрос это ключ поиска, и находить по нему польза. Узкое
правило вместо широкого выбрано намеренно.

ДВЕРЕЙ ДВЕ, и вторая не лишняя: вход не пускает новое, выдача не показывает накопленное.
Обратный путь (`dictionary_pool_reverse`) проверку языка имел изначально и потому был
чист; у прямого её не было вовсе — отсюда весь класс.
"""
import pytest

from backend.dictionary_intake import answer_language_is_wrong, is_exercise_blank


class TestAnswerLanguage:
    def test_russian_answer_to_a_german_question_is_refused(self):
        assert answer_language_is_wrong("Складывать", "de")
        assert answer_language_is_wrong("Если не считать", "de")

    def test_german_answer_to_a_russian_question_is_refused(self):
        assert answer_language_is_wrong("akribisch", "ru")
        assert answer_language_is_wrong("der Schuhmacher", "ru")

    def test_a_proper_answer_passes(self):
        assert not answer_language_is_wrong("anlegen", "de")
        assert not answer_language_is_wrong("мост", "ru")

    def test_a_mixed_answer_passes_because_the_needed_language_is_there(self):
        # «die Verbindlichkeit (обязательство)» — немецкое в нём есть, это годный ответ.
        assert not answer_language_is_wrong("die Verbindlichkeit (обязательство)", "de")
        assert not answer_language_is_wrong("поднос (das Tablett)", "ru")

    def test_an_empty_answer_is_refused(self):
        assert answer_language_is_wrong("", "de")
        assert answer_language_is_wrong(None, "ru")


class TestExerciseBlank:
    def test_a_trainer_task_is_recognised(self):
        assert is_exercise_blank("Ich ___ mein Geld lieber langfristig.")

    def test_an_ordinary_word_is_not(self):
        assert not is_exercise_blank("das Haus")
        assert not is_exercise_blank("die Verantwortung übergeben")


class TestTheDoorItself:
    """Дверь пула отказывает, а не молчит — и отказ не роняет сохранение человека."""

    def test_the_upsert_refuses_a_wrong_language_answer(self):
        from backend.database import _upsert_dictionary_canonical_entry_with_cursor

        class _Cursor:
            def execute(self, *a, **k):
                raise AssertionError("до записи дойти не должно")

        with pytest.raises(ValueError, match="not in de"):
            _upsert_dictionary_canonical_entry_with_cursor(
                _Cursor(), source_lang="ru", target_lang="de",
                source_text="Укладывать", target_text="Складывать",
                word_ru="Укладывать", translation_de="Складывать",
                word_de=None, translation_ru=None, response_json=None,
            )

    def test_the_upsert_refuses_an_exercise_blank(self):
        from backend.database import _upsert_dictionary_canonical_entry_with_cursor

        class _Cursor:
            def execute(self, *a, **k):
                raise AssertionError("до записи дойти не должно")

        with pytest.raises(ValueError, match="exercise blanks"):
            _upsert_dictionary_canonical_entry_with_cursor(
                _Cursor(), source_lang="ru", target_lang="de",
                source_text="Ich ___ mein Geld lieber langfristig.", target_text="anlegen",
                word_ru=None, translation_de="anlegen",
                word_de=None, translation_ru=None, response_json=None,
            )
