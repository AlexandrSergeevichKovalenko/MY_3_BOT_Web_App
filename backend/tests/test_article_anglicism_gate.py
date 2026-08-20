"""Англицизм вне ходового языка в банк артиклей не попадает — и не уносит с собой ходовые.

19.08.2026 владелец сыграл спринт по теме «Computer & Geräte» и получил экран из
Upload, Backup, Controller, Export, Tab. Слова не выдуманы — они есть в словаре; беда
в том, что учить немецкий род на английском слове нечему, его род держится на
договорённости и часто спорен (der/das Tab).

Но «англицизм» — не приговор: der Bus, der Film, das Radio, der Sport, der Computer
тоже заимствования, и это слова первого учебника. Поэтому режущая граница двойная:
справочник назвал заимствование ИЗ АНГЛИЙСКОГО **и** слово лежит за 20 000 в частотном
списке живой речи (решение владельца 19.08.2026).

Тест держит три вещи, каждая из которых уже ломалась при разборе:
  • совпадение написания обязательно — иначе в англицизмы уезжают кальки
    (die Einbahnstraße ← one-way street) и родственники (der Zimmermann ~ timber);
  • ходовое заимствование не режется частотой;
  • «справочник молчит» — это НЕ «англицизм»: недоказанное не снимается.
"""
import unittest
from unittest.mock import patch

import backend.article_anglicism as ang


class HerkunftJudgeTests(unittest.TestCase):
    """Разбор раздела Herkunft — на настоящих кусках статей de.wiktionary."""

    def test_a_borrowing_with_the_same_spelling_is_an_anglicism(self):
        self.assertTrue(ang.judge_herkunft(
            "Upload", "aus dem [[Englisch]]en ''{{Ü|en|upload}}'' („[[hochladen]]“)"))
        self.assertTrue(ang.judge_herkunft(
            "Computer",
            "in der zweiten Hälfte des zwanzigsten Jahrhunderts übernommen vom "
            "gleichbedeutenden [[englisch]]en ''{{Ü|en|computer}}''"))

    def test_a_loan_translation_is_not_an_anglicism(self):
        # Смысл переведён с английского, но СЛОВО немецкое и род у него немецкий.
        # Без проверки написания правило сносило die Einbahnstraße и die Auszeit.
        self.assertFalse(ang.judge_herkunft(
            "Einbahnstraße",
            "[[Lehnübersetzung]] von [[englisch]] ''{{Ü|en|one-way street}}'' aus den 1920er Jahren"))
        self.assertFalse(ang.judge_herkunft(
            "Auszeit", "*''etymologisch:'' :[[Lehnübersetzung]] von englisch ''[[time-out]]''"))

    def test_a_mere_cognate_is_not_an_anglicism(self):
        # Справочник упоминает английский как РОДСТВЕННИКА, а не как источник.
        self.assertFalse(ang.judge_herkunft(
            "Zimmermann",
            "von [[zimmern]], verwandt mit englisch ''{{Ü|en|timber}}'' und dänisch "
            "''{{Ü|da|tømmer}}'' (= Holz), und [[Mann]]"))
        self.assertFalse(ang.judge_herkunft(
            "Pfeil",
            "[[mittelhochdeutsch|mittel-]] und [[althochdeutsch]]: ''pfīl,'' [[englisch]]: "
            "''pile'' (Pfahl, Lanze, Grashalm)"))

    def test_a_german_noun_built_on_an_english_verb_is_an_anglicism(self):
        # «engl. to boil» → der Boiler. Совпадение точное, ничего не додумываем.
        self.assertTrue(ang.judge_herkunft(
            "Boiler", "von [[englisch]] ''{{Ü|en|boil}}'' „kochen“"))

    def test_the_reference_tag_alone_is_enough_when_the_spelling_matches(self):
        # У der Messenger в Herkunft нет слова «englisch», но шаблон {{Ü|en|…}}
        # ставит сам справочник — этого достаточно.
        self.assertTrue(ang.judge_herkunft(
            "Messenger", "von gleichbedeutend ''{{Ü|en|messenger}}'' entlehnt"))

    def test_typography_of_the_english_source_does_not_matter(self):
        self.assertTrue(ang.judge_herkunft(
            "Backup", "von gleichbedeutend [[englisch]] ''{{Ü|en|back-up}}''"))

    def test_an_empty_herkunft_never_accuses(self):
        self.assertFalse(ang.judge_herkunft("Sync", ""))


class TailCutTests(unittest.TestCase):
    """Режется только хвост. Ходовое заимствование остаётся в игре."""

    ORIGINS = {
        "Upload": (ang.ANGLICISM, "de.wiktionary Herkunft"),
        "Bus": (ang.ANGLICISM, "de.wiktionary Herkunft"),
        "Film": (ang.ANGLICISM, "de.wiktionary Herkunft"),
        "Sneaker": (ang.ANGLICISM, "de.wiktionary Herkunft"),
        "Bildschirm": (ang.OTHER, "de.wiktionary Herkunft"),
        "Autowäsche": (ang.UNKNOWN, "оба справочника молчат о происхождении"),
    }
    # Ранги из живого списка bt_3_word_frequency (замер 19.08.2026).
    RANKS = {"Upload": 46639, "Bus": 1603, "Film": 673, "Bildschirm": 4000}

    def _cut(self):
        with patch.object(ang, "origin_of", return_value=self.ORIGINS), \
             patch.object(ang, "everyday_ranks", return_value=self.RANKS):
            return ang.tail_anglicisms(list(self.ORIGINS))

    def test_a_rare_anglicism_is_cut(self):
        self.assertIn("Upload", self._cut())

    def test_an_everyday_anglicism_stays(self):
        cut = self._cut()
        self.assertNotIn("Bus", cut, "der Bus — слово первого учебника, его артикль и учат")
        self.assertNotIn("Film", cut)

    def test_a_word_missing_from_the_frequency_list_counts_as_tail(self):
        # Нет в списке из 50 000 — значит в живой речи не встречается вовсе.
        self.assertIn("Sneaker", self._cut())

    def test_a_german_word_is_never_cut(self):
        self.assertNotIn("Bildschirm", self._cut())

    def test_silence_of_the_reference_is_not_a_verdict(self):
        # Оба справочника промолчали. Снять слово на этом основании нельзя:
        # недоказанное не равно доказанному. Оно остаётся и идёт в отчёт числом.
        self.assertNotIn("Autowäsche", self._cut())


class UnsourcedEtymologyGoesToTheSecondReferenceTests(unittest.TestCase):
    """Справочник сам пометил этимологию неподтверждённой — это не ответ.

    Живой случай: у «Scanner» в de.wiktionary написано «aus dem engl. to scan»
    с пометкой {{QS Herkunft|unbelegt}}. По этому тексту слово не опознаётся —
    «scan» + «er» даёт «scaner», а не «Scanner». Дописать сюда правило удвоения
    согласной значило бы додумывать грамматику; вместо этого спрашиваем второй
    источник, который говорит прямо: {{bor+|de|en|scanner}}.
    """

    DE_PAGE = ("== Scanner ({{Sprache|Deutsch}}) ==\n{{Herkunft}}\n"
               ":aus dem [[engl.]] to scan „abtasten\" {{QS Herkunft|unbelegt}}\n")
    EN_PAGE = "==German==\n===Etymology===\n{{bor+|de|en|scanner|t=electronic device}}.\n"

    def test_the_second_reference_decides_when_the_first_admits_it_is_unsourced(self):
        pages = iter([{"Scanner": self.DE_PAGE}, {"Scanner": self.EN_PAGE}])
        with patch.object(ang, "_cached", return_value={}), \
             patch.object(ang, "_remember"), \
             patch.object(ang, "_fetch_wikitext", side_effect=lambda api, titles: next(pages)):
            verdicts = ang.origin_of(["Scanner"])
        self.assertEqual(verdicts["Scanner"][0], ang.ANGLICISM)
        self.assertIn("en.wiktionary", verdicts["Scanner"][1])

    def test_a_sourced_german_etymology_is_not_second_guessed(self):
        page = ("== Bildschirm ({{Sprache|Deutsch}}) ==\n{{Herkunft}}\n"
                ":[[Determinativkompositum]] aus ''[[Bild]]'' und ''[[Schirm]]''\n")
        with patch.object(ang, "_cached", return_value={}), \
             patch.object(ang, "_remember"), \
             patch.object(ang, "_fetch_wikitext", return_value={"Bildschirm": page}) as fetch:
            verdicts = ang.origin_of(["Bildschirm"])
        self.assertEqual(verdicts["Bildschirm"][0], ang.OTHER)
        self.assertEqual(fetch.call_count, 1, "второй справочник тут не нужен")


class UnknownIsNeverCachedTests(unittest.TestCase):
    """«Не знаем» не запоминается: иначе одно молчание застывает навсегда."""

    def test_unknown_verdicts_do_not_reach_the_cache(self):
        remembered: list = []
        with patch.object(ang, "_cached", return_value={}), \
             patch.object(ang, "_remember", side_effect=lambda rows: remembered.extend(rows)), \
             patch.object(ang, "_fetch_wikitext", return_value={"Sync": None}):
            verdicts = ang.origin_of(["Sync"])
        self.assertEqual(verdicts["Sync"][0], ang.UNKNOWN)
        self.assertEqual(remembered, [], "молчание справочника в кэш попадать не должно")


if __name__ == "__main__":
    unittest.main()


class DigestedBorrowingsStayInTheGameTests(unittest.TestCase):
    """Заимствование, у которого немецкий ПЕРЕПИСАЛ написание, — не наш случай.

    Игра учит НЕМЕЦКИЙ РОД. У «der Upload» род держится только на договорённости —
    учить на нём нечему. А «die Koalition», «die Empathie», «die Harmonika» немецкий
    давно переварил: они читаются по-немецки, и род у них выводится немецкими
    правилами (-ion → die). Такие слова в тренажёре артиклей как раз нужны.

    Замер 20.08.2026: второй справочник (en.wiktionary) отвечает только на вопрос
    «взял ли немецкий это у английского» и про написание не думает — поэтому по нему
    в англицизмы уезжали coalition→Koalition, empathy→Empathie, harmonica→Harmonika,
    closet→Klosett, dog→Dogge, mungos→Mungo, typhoon→Taifun. Четверо из них уже были
    сняты с показа и вернулись.
    """

    def test_a_borrowing_kept_as_written_in_english_is_an_anglicism(self):
        self.assertTrue(ang._keeps_english_spelling("Upload", "upload"))
        self.assertTrue(ang._keeps_english_spelling("Airbag", "airbag"))

    def test_a_respelled_borrowing_is_not(self):
        for german, english in (("Koalition", "coalition"), ("Empathie", "empathy"),
                                ("Harmonika", "harmonica"), ("Klosett", "closet"),
                                ("Dogge", "dog"), ("Taifun", "typhoon")):
            self.assertFalse(ang._keeps_english_spelling(german, english),
                             f"{german} немецкий уже переварил — его род учится по правилам")

    def test_a_compound_with_an_english_part_still_counts(self):
        self.assertTrue(ang._keeps_english_spelling("USB-Stick", "stick"))

    def test_an_unnamed_source_word_never_accuses(self):
        # Справочник сказал «заимствование», но слова не назвал — сравнивать не с чем.
        self.assertFalse(ang._keeps_english_spelling("Administrator", ""))


class PhraseSourcesAreReadTests(unittest.TestCase):
    """Источник назван фразой — немецкое слово может быть одним из её слов."""

    def test_a_phrase_source_still_identifies_the_word(self):
        # das Shopping: «von englisch ''to go shopping'' entlehnt». Раньше правило
        # требовало совпадения со всей фразой и выносило вердикт «не англицизм».
        self.assertTrue(ang.judge_herkunft(
            "Shopping",
            ":im 20. Jahrhundert von englisch ''to {{Ü|en|go shopping}}'' „einkaufen gehen“ entlehnt"))

    def test_a_calque_phrase_still_does_not_count(self):
        # У кальки ни одно слово фразы не совпадает с немецким написанием.
        self.assertFalse(ang.judge_herkunft(
            "Einbahnstraße",
            ":[[Lehnübersetzung]] von [[englisch]] ''{{Ü|en|one-way street}}''"))
