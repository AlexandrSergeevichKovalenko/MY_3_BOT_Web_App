"""Опознание «слово или форма слова» — фундамент починки артикля у множественного.

Находка владельца: запрос «Проблемы» показывал карточку «das Probleme». «das» —
артикль леммы «das Problem», приклеенный к форме множественного числа. Замер на
проде: модель, у которой быстрый путь спрашивает артикль, ошибается на множественном
в 10 случаях из 20; в данных нашлось 242 формы, живущие как самостоятельные слова.

Здесь проверяется единственное, на чём держится вся починка: система обязана знать,
что у неё в руках — слово или его форма, — и НЕ печатать артикль, когда не знает.
"""

import unittest
from unittest.mock import patch

import backend.german_surface as gs


# Роды, «документированные в Wiktionary» для теста. Ровно то, что отдаёт
# authoritative_article с источником 'wiktionary'.
DOCUMENTED = {
    "problem": "das", "buch": "das", "haus": "das", "kind": "das", "land": "das",
    "wort": "das", "auto": "das", "zimmer": "das", "tisch": "der", "mann": "der",
    "freund": "der", "termin": "der", "frau": "die", "regel": "die", "straße": "die",
    "freundin": "die", "blume": "die", "ende": "das", "reise": "die", "reis": "der",
    "lehrer": "der", "kalender": "der", "fehler": "der", "stadt": "die",
}


def _fake_authority(word, *, allow_network=False):
    article = DOCUMENTED.get(str(word or "").strip().casefold())
    return (article, "wiktionary") if article else (None, "нет данных")


class GermanSurfaceTests(unittest.TestCase):
    def setUp(self):
        gs.reset_caches()
        self._authority = patch("backend.article_authority.authoritative_article",
                                side_effect=_fake_authority)
        self._authority.start()
        self.addCleanup(self._authority.stop)
        # По умолчанию индекс форм пуст: проверяем, что и без него детектор не врёт.
        self._index = patch.object(gs, "_load_form_index", return_value={})
        self._index.start()
        self.addCleanup(self._index.stop)

    # ── единственное число ────────────────────────────────────────────────────
    def test_documented_lemma_is_singular_with_its_own_article(self):
        for word, article in (("Problem", "das"), ("Tisch", "der"), ("Frau", "die")):
            with self.subTest(word=word):
                verdict = gs.german_surface(word)
                self.assertEqual(verdict["number"], gs.SG)
                self.assertEqual(verdict["article"], article)

    def test_leading_article_is_stripped_before_lookup(self):
        self.assertEqual(gs.german_surface("das Problem")["number"], gs.SG)

    def test_surface_equal_to_its_own_plural_stays_singular(self):
        """«Lehrer», «Zimmer», «Kalender», «Fehler» — единственное и множественное
        пишутся одинаково. Род документирован, значит это лемма: артикль леммы верен,
        и превращать такое слово в «форму» нельзя."""
        for word, article in (("Lehrer", "der"), ("Zimmer", "das"),
                              ("Kalender", "der"), ("Fehler", "der")):
            with self.subTest(word=word):
                verdict = gs.german_surface(word)
                self.assertEqual(verdict["number"], gs.SG)
                self.assertEqual(verdict["article"], article)

    def test_documented_lemma_wins_over_ending_rule(self):
        """«Reise» оканчивается на -e, и «Reis» существует. Но род самой «Reise»
        документирован, поэтому это слово, а не форма от «der Reis»."""
        verdict = gs.german_surface("Reise")
        self.assertEqual(verdict["number"], gs.SG)
        self.assertEqual(verdict["article"], "die")

    # ── множественное число ───────────────────────────────────────────────────
    def test_pluralia_tantum_get_die_and_are_not_singularised(self):
        for word in ("Eltern", "Ferien", "Kosten", "Geschwister", "Leute"):
            with self.subTest(word=word):
                verdict = gs.german_surface(word)
                self.assertEqual(verdict["number"], gs.PL)
                self.assertEqual(verdict["article"], "die")
                self.assertEqual(verdict["confidence"], "high")

    def test_form_index_gives_plural_with_die_and_the_lemma(self):
        """Главный случай владельца: «Probleme» — форма от «das Problem», и артикль
        рядом с ней может быть только «die». Никогда «das»."""
        with patch.object(gs, "_load_form_index",
                          return_value={"probleme": ("Problem", gs.PL, "wiktionary")}):
            verdict = gs.german_surface("Probleme")
        self.assertEqual(verdict["number"], gs.PL)
        self.assertEqual(verdict["article"], "die")
        self.assertEqual(verdict["lemma"], "Problem")
        self.assertEqual(verdict["confidence"], "high")

    def test_ending_rule_detects_plural_but_never_prints_an_article(self):
        """Пока индекс форм не прогрет, правило окончания узнаёт множественное —
        но это догадка: артикль не печатаем вовсе. Пустое честнее выдуманного, и
        главное — «das Probleme» стать не может."""
        for word, lemma in (("Probleme", "Problem"), ("Bücher", "Buch"),
                            ("Häuser", "Haus"), ("Kinder", "Kind"),
                            ("Frauen", "Frau"), ("Autos", "Auto"),
                            ("Tische", "Tisch"), ("Männer", "Mann"),
                            ("Freunde", "Freund"), ("Termine", "Termin"),
                            ("Regeln", "Regel"), ("Freundinnen", "Freundin")):
            with self.subTest(word=word):
                verdict = gs.german_surface(word)
                self.assertEqual(verdict["number"], gs.PL)
                self.assertEqual(verdict["lemma"], lemma)
                self.assertEqual(verdict["article"], "")
                self.assertEqual(verdict["confidence"], "low")

    def test_lemma_article_never_leaks_onto_a_plural_surface(self):
        """Ровно тот дефект, из-за которого всё началось: артикль леммы не имеет
        права появиться рядом с формой множественного ни на одном пути."""
        for word in ("Probleme", "Bücher", "Häuser", "Autos", "Tische", "Männer"):
            with self.subTest(word=word):
                self.assertNotIn(gs.german_surface(word)["article"], ("das", "der"))

    # ── «не знаю» ─────────────────────────────────────────────────────────────
    def test_unknown_surface_yields_no_article(self):
        for word in ("Quastelbrumm", "Xyzzyfax"):
            with self.subTest(word=word):
                verdict = gs.german_surface(word)
                self.assertEqual(verdict["number"], gs.UNKNOWN)
                self.assertEqual(verdict["article"], "")

    def test_non_german_input_is_rejected_outright(self):
        for word in ("Проблемы", "", "   ", "zwei Wörter"):
            with self.subTest(word=word):
                verdict = gs.german_surface(word)
                self.assertEqual(verdict["number"], gs.UNKNOWN)
                self.assertEqual(verdict["article"], "")

    def test_missing_database_degrades_to_unknown_not_to_a_guess(self):
        """Индекс форм недоступен — детектор теряет ступень, но не начинает
        выдумывать: он либо молчит, либо отвечает по документированному роду."""
        with patch("backend.database.get_db_connection_context", side_effect=RuntimeError("no db")):
            gs.reset_caches()
            with patch.object(gs, "_documented_singular", return_value=""):
                verdict = gs.german_surface("Probleme")
        self.assertEqual(verdict["article"], "")


if __name__ == "__main__":
    unittest.main()
