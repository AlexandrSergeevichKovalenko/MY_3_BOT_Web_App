# -*- coding: utf-8 -*-
"""Предложению словарный разбор не собирается — ни ночью, ни при сохранении, ни экраном.

РЕШЕНИЕ ВЛАДЕЛЬЦА 27.08.2026, дословно:

    «это уже предложение, включающее в себя контекст использования слов… неясно, на что
    делать упор… главное — есть немецкий и русский вариант, и больше ничего не нужно»

и 28.08.2026 про уже накопленное:

    «накопление не трогай, уже как есть так пусть и будет»

ПОЧЕМУ ЭТО ВАЖНО, А НЕ ПРОСТО ЭКОНОМИЯ. Словарный разбор описывает СЛОВО. Наложенный на
целое предложение, он описывает какое-то одно слово изнутри него — и человек заучивает
это как разбор предложения. Живой случай 27.08.2026, единица 48723 «Gesundheit ist mehr
denn je ein wichtiges Thema.»:

    forms.praeteritum = "war", forms.perfekt = "ist gewesen"   ← формы глагола sein
    antonyms = [{"word": "Krankheit"}]                          ← антоним к предложению
    etymology_note = «Слово Gesundheit происходит от…»          ← этимология другого слова
    source_lang = "ru" при немецком source_text                 ← запись противоречит себе

ЗАМЕР 27.08.2026 по живой базе: 2 793 предложения из 6 332 носят словарный разбор.
Живых дверей было три, и каждая закрывается своим тестом ниже:

    1. открытие карточки  → `дозаполнение при открытии`   2 записи (обе за один день)
    2. ночь после правки  → `пересборка после правки`      64 записи (час 01 UTC)
    3. сохранение         → `сохранение`                   28 записей (5 за один день)

Четвёртая причина — не дверь, а слепота: экран «Мои слова» (`list_user_vocabulary`) вида
записи не отдавал вовсе, поэтому браузерный страж смотрел в пустоту и молчал.
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SECOND_VOICE_CHECK_DISABLED", "1")

from backend import lex_units  # noqa: E402
from backend import phrase_night_check  # noqa: E402


class ВидЗаписиСчитаетсяОдинаково(unittest.TestCase):
    """Правило «слово / оборот / предложение» живёт в одном месте на всё приложение."""

    def test_the_classifier_says_what_we_expect(self):
        self.assertEqual(lex_units._kind_for_text("Nährstoff"), "word")
        self.assertEqual(lex_units._kind_for_text("die Jagd auf"), "collocation")
        self.assertEqual(
            lex_units._kind_for_text("Gesundheit ist mehr denn je ein wichtiges Thema."),
            "sentence",
        )
        # Точка в конце делает предложением даже короткую запись — так задумано.
        self.assertEqual(lex_units._kind_for_text("Es regnet."), "sentence")


class СохранениеНеКладётРазборНаПредложение(unittest.TestCase):
    """Дверь вторая: разбор приезжал ВМЕСТЕ с сохранением и ложился на единицу."""

    def _сохранить(self, немецкий):
        разбор = {"word_de": немецкий, "translation_ru": "перевод"}
        with mock.patch.object(lex_units, "ensure_unit", return_value=777), \
             mock.patch.object(lex_units, "get_db_connection_context"), \
             mock.patch.object(lex_units, "save_unit_card_if_richer") as запись, \
             mock.patch.object(lex_units, "sync_unit_links_from_card") as связь:
            lex_units.attach_entry_to_unit(
                1, word_de=немецкий, word_ru="перевод",
                source_lang="de", target_lang="ru", card=разбор,
            )
        return запись, связь

    def test_sentence_gets_no_card(self):
        запись, _ = self._сохранить("Gesundheit ist mehr denn je ein wichtiges Thema.")
        запись.assert_not_called()

    def test_sentence_still_gets_its_translation_link(self):
        """Связь НЕ отключается: у предложения перевод — всё его содержимое, и без связи
        его не увидит ночная проверка грамматики (backend/translation_links.py)."""
        _, связь = self._сохранить("Gesundheit ist mehr denn je ein wichtiges Thema.")
        связь.assert_called_once()

    def test_a_word_still_gets_its_card(self):
        запись, связь = self._сохранить("Nährstoff")
        запись.assert_called_once()
        связь.assert_called_once()

    def test_a_collocation_still_gets_its_card(self):
        """Владелец 25.08.2026: «словосочетания греем, предложения не греем»."""
        запись, _ = self._сохранить("die Jagd auf")
        запись.assert_called_once()


class НочьНеПересобираетРазборПредложения(unittest.TestCase):
    """Дверь третья: ночная проверка грамматики исправляла фразу и тут же собирала ей
    полный СЛОВАРНЫЙ разбор — 64 предложения этим путём (замер 27.08.2026)."""

    def _ночь(self, текст, исправленный):
        строка = {"unit_id": 777, "text": текст, "kind": "sentence", "translation": "перевод"}
        приговор = [{"verdict": "error"}, {"verdict": "error"}]
        import backend.database as db
        with mock.patch.object(phrase_night_check, "pick_phrases_for_grammar_check",
                               return_value=[строка]), \
             mock.patch.object(phrase_night_check, "_judge_once", return_value=приговор), \
             mock.patch.object(phrase_night_check, "_both_agree",
                               return_value=(True, sorted(phrase_night_check.SILENT_CATEGORIES)[0],
                                             исправленный)), \
             mock.patch.object(phrase_night_check, "_apply_silent_fix", return_value=True), \
             mock.patch.object(phrase_night_check, "mark_phrase_checked"), \
             mock.patch.object(phrase_night_check, "count_phrases_left_for_grammar_check",
                               return_value=0), \
             mock.patch.object(phrase_night_check, "count_open_phrase_reviews", return_value=0), \
             mock.patch.object(db, "rebuild_unit_breakdown") as пересборка:
            отчёт = phrase_night_check.run_phrase_night_check(limit=1)
        return отчёт, пересборка

    def test_corrected_sentence_is_not_rebuilt(self):
        отчёт, пересборка = self._ночь("Gesundheit ist mehr denn je ein wichtige Thema.",
                                       "Gesundheit ist mehr denn je ein wichtiges Thema.")
        пересборка.assert_not_called()
        self.assertEqual(отчёт["fixed"], 1)          # текст исправлен — это по-прежнему работа
        self.assertEqual(отчёт["разбор не собирали, это предложение"], 1)

    def test_a_corrected_collocation_is_still_rebuilt(self):
        """Оборот разбор получает — правка текста делает прежний разбор неверным."""
        отчёт, пересборка = self._ночь("die Jagd nach", "die Jagd auf")
        пересборка.assert_called_once()
        self.assertNotIn("разбор не собирали, это предложение", отчёт)

    def test_the_kind_is_taken_from_the_corrected_text(self):
        """Правка может свести предложение к обороту — считаем вид по НОВОМУ тексту,
        иначе оборот навсегда останется без разбора (тот же класс, что «der Simulator,
        -en» 21.08.2026)."""
        _, пересборка = self._ночь("Die Feinde verzehnfachen sich.", "sich verzehnfachen")
        пересборка.assert_called_once()


class СписокСловОтдаётВидЗаписи(unittest.TestCase):
    """Причина, по которой браузерный страж молчал: экран «Мои слова» вида записи не
    получал вовсе. Страж смотрел в поле, которого в ответе не было."""

    def test_library_list_returns_unit_kind(self):
        from backend import database

        # См. SELECT в list_user_vocabulary. Колонки там добавляются ТОЛЬКО В КОНЕЦ —
        # строки разбираются по номерам. 31.08.2026 в конец приехали четыре колонки
        # источника слова (source_id / kind / title / external_key): 29 → 33.
        колонки = 33
        строка = [None] * колонки
        строка[0] = 1                      # q.id
        строка[3] = "Gesundheit ist wichtig."
        строка[8] = None
        строка[9] = None
        строка[10] = False
        строка[20] = {}                    # q.response_json
        строка[21] = None                  # q.user_notes
        строка[24] = "Gesundheit ist wichtig."
        строка[25] = "Здоровье важно"
        строка[28] = "sentence"            # lu.kind

        class Курсор:
            def __init__(self):
                self._счёт = 0

            def execute(self, *a, **k):
                self._счёт += 1

            def fetchone(self):
                return (1,)

            def fetchall(self):
                return [tuple(строка)]

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        class Соединение:
            def cursor(self, *a, **k):
                return Курсор()

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        with mock.patch.object(database, "get_db_connection_context", return_value=Соединение()), \
             mock.patch.object(database, "get_user_word_overrides", return_value={}):
            ответ = database.list_user_vocabulary(user_id=1)

        self.assertEqual(ответ["items"][0]["unit_kind"], "sentence")


if __name__ == "__main__":
    unittest.main()
