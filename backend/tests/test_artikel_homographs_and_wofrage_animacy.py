"""Две находки владельца в тренажёрах.

1) Карточка «der Band» звучала как «die Band». В базе всё верно — три записи с тремя
   файлами, — но озвучка искалась ПО СЛОВУ без рода, и три записи схлопывались в одну.
   Задето 26 слов-омонимов и 58 карточек: See, Tor, Leiter, Schild, Kiefer, Steuer…

2) «Worauf nimmst du Rücksicht? — Ребёнок.» Ребёнок — человек, правильный вопрос
   «Auf wen». Карточка противоречила сама себе: в пояснении писала «о человеке было бы
   Auf wen», а ответом считала вопрос о вещи. Причина — люди в списках «вещей».
"""

import re
import unittest
from unittest.mock import MagicMock, patch

import backend.database as db
import backend.wofrage_generator as wg


PERSON_HEADS = {
    "kind", "mann", "frau", "freund", "freundin", "bruder", "schwester", "lehrer",
    "lehrerin", "chef", "chefin", "vater", "mutter", "kollege", "kollegin", "arzt",
    "sohn", "tochter", "oma", "opa", "nachbar", "nachbarin", "gast", "kunde",
    "partner", "eltern", "kinder", "leute", "mensch", "junge", "mädchen", "schüler",
    "student", "trainer",
}


def _head_noun(phrase: str) -> str:
    return re.sub(r"^(der|die|das)\s+", "", str(phrase or "")).strip().lower()


class ArtikelHomographAudioTests(unittest.TestCase):
    """Ключ выборки обязан включать род, иначе омоним получает чужую озвучку."""

    def _fake_rows(self, rows):
        cursor = MagicMock()
        cursor.fetchall.return_value = rows
        cursor.__enter__ = lambda s: s
        cursor.__exit__ = lambda s, *a: False
        conn = MagicMock()
        conn.cursor.return_value = cursor
        ctx = MagicMock()
        ctx.__enter__ = lambda s: conn
        ctx.__exit__ = lambda s, *a: False
        return ctx

    def test_each_gender_gets_its_own_audio(self):
        rows = [
            ("band", "der", "artikel/audio/der-band.mp3"),
            ("band", "die", "artikel/audio/die-band.mp3"),
            ("band", "das", "artikel/audio/das-band.mp3"),
        ]
        with patch.object(db, "get_db_connection_context", return_value=self._fake_rows(rows)):
            got = db.get_article_noun_audio([("Band", "der"), ("Band", "die"), ("Band", "das")])
        self.assertEqual(got[("band", "der")], "artikel/audio/der-band.mp3")
        self.assertEqual(got[("band", "die")], "artikel/audio/die-band.mp3")
        self.assertEqual(got[("band", "das")], "artikel/audio/das-band.mp3")

    def test_mnemonics_and_images_are_keyed_the_same_way(self):
        for getter, column in ((db.get_article_noun_mnemonics, "подсказка"),
                               (db.get_article_noun_images, "картинка")):
            with self.subTest(getter=getter.__name__):
                rows = [("see", "der", f"{column} про озеро"), ("see", "die", f"{column} про море")]
                with patch.object(db, "get_db_connection_context", return_value=self._fake_rows(rows)):
                    got = getter([("See", "der"), ("See", "die")])
                self.assertNotEqual(got[("see", "der")], got[("see", "die")])

    def test_plain_word_lookup_still_works(self):
        """Старые вызовы «просто по слову» не должны сломаться."""
        rows = [("haus", "das", "artikel/audio/das-haus.mp3")]
        with patch.object(db, "get_db_connection_context", return_value=self._fake_rows(rows)):
            got = db.get_article_noun_audio(["Haus"])
        self.assertEqual(got["haus"], "artikel/audio/das-haus.mp3")


class WoFrageAnimacyTests(unittest.TestCase):
    """О человеке спрашивают «Auf wen», о вещи — «Worauf». Список «вещей» не должен
    содержать людей: генератор берёт оттуда объект именно для вопроса о вещи."""

    def test_no_people_among_things(self):
        offenders = [
            (entry["lemma"], obj)
            for entry in wg._BANK
            if not entry.get("person_only")
            for obj, _ru in (entry.get("obj") or [])
            if _head_noun(obj) in PERSON_HEADS
        ]
        self.assertEqual(offenders, [], f"люди в списках вещей: {offenders}")

    def test_person_question_uses_wen_form(self):
        self.assertEqual(wg._person_form("auf", "akk"), "Auf wen")
        self.assertEqual(wg._person_form("an", "akk"), "An wen")
        self.assertEqual(wg._wo_form("auf").capitalize(), "Worauf")

    def test_generated_items_never_ask_worauf_about_a_person(self):
        for _ in range(200):
            item = wg._build_one(wg._BANK[_ % len(wg._BANK)])
            head = _head_noun(item.get("obj") or "")
            if head in PERSON_HEADS:
                self.assertEqual(
                    item.get("target"), "person",
                    f"о человеке спрашивают как о вещи: {item.get('a')} … {item.get('obj')}",
                )


# Утверждённые объекты-ВЕЩИ из банка Wo-Fragen. Список закреплён намеренно: проверка
# должна падать на НЕЗНАКОМОМ слове, а не молча его пропускать. Список людей защищает
# только от известных слов — заведи кто-нибудь «die Bäckerin», и ни список, ни страж
# в генераторе её не узнают. А этот тест упадёт сразу и потребует подтвердить, что
# добавленное слово действительно вещь, а не человек.
_APPROVED_THINGS = {
    "das Amt",
    "das Angebot",
    "das Auto",
    "das Buch",
    "das Eis",
    "das Ende",
    "das Erbe",
    "das Ereignis",
    "das Ergebnis",
    "das Erlebnis",
    "das Essen",
    "das Fest",
    "das Gehalt",
    "das Geld",
    "das Gemüse",
    "das Gepäck",
    "das Geschenk",
    "das Gesetz",
    "das Gewissen",
    "das Glas",
    "das Glück",
    "das Haus",
    "das Interview",
    "das Kapital",
    "das Kleid",
    "das Klima",
    "das Meer",
    "das Metall",
    "das Obst",
    "das Original",
    "das Parfüm",
    "das Prinzip",
    "das Problem",
    "das Projekt",
    "das Rauchen",
    "das Recht",
    "das Salz",
    "das Schicksal",
    "das Stipendium",
    "das Studium",
    "das Talent",
    "das Team",
    "das Thema",
    "das Treffen",
    "das Verhalten",
    "das Vorbild",
    "das Wetter",
    "das Wissen",
    "das Wochenende",
    "das Wort",
    "das Wunder",
    "das Ziel",
    "das Überleben",
    "der Anlass",
    "der Antrag",
    "der Artikel",
    "der Begriff",
    "der Bericht",
    "der Bus",
    "der Erfolg",
    "der Fall",
    "der Fehler",
    "der Film",
    "der Garten",
    "der Geburtstag",
    "der Hund",
    "der Job",
    "der Kaffee",
    "der Kauf",
    "der Knoblauch",
    "der Kompromiss",
    "der Konflikt",
    "der Krieg",
    "der Kurs",
    "der Lärm",
    "der Name",
    "der Pessimismus",
    "der Plan",
    "der Preis",
    "der Rat",
    "der Rauch",
    "der Regen",
    "der Roman",
    "der Schlüssel",
    "der Schmerz",
    "der Schmutz",
    "der Schritt",
    "der Sieg",
    "der Sport",
    "der Stau",
    "der Streit",
    "der Stress",
    "der Sturm",
    "der Termin",
    "der Umzug",
    "der Unfall",
    "der Urlaub",
    "der Verlust",
    "der Verzicht",
    "der Vorschlag",
    "der Vorsitz",
    "der Vorteil",
    "der Vorwurf",
    "der Weg",
    "der Witz",
    "der Zweifel",
    "der Ärger",
    "die Allergie",
    "die Angelegenheit",
    "die Angst",
    "die Annahme",
    "die Antwort",
    "die Arbeit",
    "die Aufgabe",
    "die Ausrede",
    "die Aussage",
    "die Bank",
    "die Behörde",
    "die Bewerbung",
    "die Bildung",
    "die Botschaft",
    "die Diskussion",
    "die Dunkelheit",
    "die Einladung",
    "die Entscheidung",
    "die Erfahrung",
    "die Farbe",
    "die Firma",
    "die Frage",
    "die Freiheit",
    "die Gefahr",
    "die Gerechtigkeit",
    "die Gesundheit",
    "die Gewalt",
    "die Gewohnheit",
    "die Grippe",
    "die Gruppe",
    "die Hand",
    "die Heimat",
    "die Hilfe",
    "die Hitze",
    "die Hose",
    "die Idee",
    "die Kindheit",
    "die Kleidung",
    "die Kontrolle",
    "die Korruption",
    "die Krankheit",
    "die Kritik",
    "die Kunst",
    "die Kälte",
    "die Leistung",
    "die Liebe",
    "die Lösung",
    "die Macht",
    "die Meinung",
    "die Miete",
    "die Musik",
    "die Müdigkeit",
    "die Nachricht",
    "die Ordnung",
    "die Panik",
    "die Party",
    "die Politik",
    "die Prüfung",
    "die Pünktlichkeit",
    "die Qualität",
    "die Reaktion",
    "die Regel",
    "die Reise",
    "die Rente",
    "die Ruhe",
    "die Sicherheit",
    "die Situation",
    "die Sitzung",
    "die Sonne",
    "die Spinne",
    "die Stelle",
    "die Tasche",
    "die Uhrzeit",
    "die Umwelt",
    "die Ungerechtigkeit",
    "die Unterstützung",
    "die Vernunft",
    "die Versicherung",
    "die Verspätung",
    "die Vorschrift",
    "die Wahrheit",
    "die Woche",
    "die Zeit",
    "die Zitrone",
    "die Zukunft",
    "die Änderung",
    "die Überraschung",
    "die Übertreibung",
}




class WoFrageApprovedObjectsTests(unittest.TestCase):
    def test_every_thing_object_is_approved(self):
        current = {
            obj
            for entry in wg._BANK
            if not entry.get("person_only")
            for obj, _ru in (entry.get("obj") or [])
        }
        unknown = sorted(current - _APPROVED_THINGS)
        self.assertEqual(
            unknown, [],
            "В банке Wo-Fragen появились объекты, которых нет в утверждённом списке: "
            f"{unknown}.\n"
            "Проверь каждое: это ВЕЩЬ или ЧЕЛОВЕК? О человеке спрашивают «Auf wen», и такое "
            "слово в списке вещей ломает задание. Если это вещь — добавь его в "
            "_APPROVED_THINGS в этом тесте.",
        )

    def test_approved_list_has_no_people(self):
        """Сам утверждённый список тоже проверяем — на случай, если человек проскочил
        в него при пополнении."""
        offenders = sorted(o for o in _APPROVED_THINGS if wg._is_person_noun(o))
        self.assertEqual(offenders, [], f"люди в утверждённом списке вещей: {offenders}")


if __name__ == "__main__":
    unittest.main()
