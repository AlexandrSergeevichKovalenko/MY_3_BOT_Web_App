"""Deterministic generator for German "Wo-Frage" items (Präpositionaladverbien).

Asking about a PREPOSITIONAL OBJECT in German is a CLOSED, rule-based system, so
we can build UNLIMITED, 100%-correct items with no LLM and zero "бред":

  A verb/adjective governs a FIXED preposition (warten AUF, denken AN, träumen VON…).
  To ask about it there are exactly two rules, and which one applies is decided by
  whether the object is a THING or a PERSON:

    • THING  → wo(r) + preposition        (vowel-initial prep inserts -r-)
                 auf → worAUF, an → worAN, über → worÜBER, um → worUM
                 mit → woMIT, von → woVON, für → woFÜR, nach → woNACH
    • PERSON → preposition + wen/wem       (case decided by the preposition)
                 auf j-n → AUF WEN, an j-n → AN WEN, mit j-m → MIT WEM

The person/thing split is the single most common real learner mistake
(Worüber ↔ Über wen), so every item carries the OPPOSITE form as a hard distractor.

One item = a tiny Q&A dialog with the question word blanked, e.g.

    ___ wartest du schon so lange?
    — Auf den Bus.            (thing → correct: "Worauf")

The correct answer is derived from the bank's governed preposition, NOT parsed from
the clue line, so grading is unambiguous. The clue line is a natural context hint.

Variety = (~185 governed verbs/adjectives) × (thing objects | person names) ×
(question frames) × (thing/person mode) → tens of thousands of unique, correct items.
Question frames use ONLY the 'du' + 'ihr' present forms (ihr is always regular;
the few strong 'du' stem-changes are written out correctly in the bank), so the
generated questions are always grammatical.
"""
from __future__ import annotations

import hashlib
import logging
import random
from collections import Counter

# Перевод вопроса лежит рядом отдельным файлом: это данные, заполненные руками,
# и в банке им не место — там управление, а здесь смысл фразы.
try:
    from backend.wofrage_question_ru import QUESTION_RU, question_ru
except ImportError:  # запуск из каталога backend
    from wofrage_question_ru import QUESTION_RU, question_ru

# ── Rules ───────────────────────────────────────────────────────────────────
_VOWELS = "aeiouäöü"


# ── Одушевлённость ──────────────────────────────────────────────────────────
# О ЧЕЛОВЕКЕ спрашивают «Auf wen», о ВЕЩИ — «Worauf». Перепутать эти две вещи —
# значит выдать задание, которое противоречит собственному пояснению («о человеке
# было бы Auf wen»), и наказать человека за правильный ответ. Так и случилось:
# в списках вещей лежали das Kind, der Chef, die Kollegin, der Lehrer — девять
# готовых заданий спрашивали «Worauf … ребёнок».
#
# Список ниже — не единственная защита: даже если завтра сюда впишут человека,
# которого здесь нет, _build_one НЕ выдаст о нём вопрос о вещи (см. страж там).
_PERSON_NOUNS = {
    "abonnent", "abonnenten", "abonnentin", "abonnentinnen", "amateur", "amateure", "amateurin",
    "amateurinnen", "analyst", "analyste", "anfänger", "anfängerin", "anfängerinnen",
    "anführer", "anführerin", "anführerinnen", "angehörige", "angehörigen", "angehöriger",
    "angehörigerin", "angehörigerinnen", "anwalt", "anwalte", "anwender", "anwenderin",
    "anwenderinnen", "anwältin", "anwältinnen", "apotheker", "apothekerin", "apothekerinnen",
    "arbeitslose", "arbeitslosen", "arbeitsloser", "arbeitsloserin", "arbeitsloserinnen",
    "architekt", "architekte", "arzt", "arzte", "assistent", "assistenten", "assistentin",
    "assistentinnen", "astronom", "astronome", "ausbilder", "ausbilderin", "ausbilderinnen",
    "ausländer", "ausländerin", "ausländerinnen", "autor", "autore", "autorin", "autorinnen",
    "azubi", "azubie", "baby", "babye", "babys", "barkeeper", "barkeeperin", "barkeeperinnen",
    "bauer", "bauerin", "bauerinnen", "bauern", "beamte", "beamten", "beamter", "beamterin",
    "beamterinnen", "beamtinnen", "behinderte", "behinderten", "behinderter", "behinderterin",
    "behinderterinnen", "bekannte", "bekannten", "bekannter", "bekannterin", "bekannterinnen",
    "benutzer", "benutzerin", "benutzerinnen", "berater", "beraterin", "beraterinnen",
    "bergsteiger", "bergsteigerin", "bergsteigerinnen", "besucher", "besucherin",
    "besucherinnen", "bewerber", "bewerberin", "bewerberinnen", "bewohner", "bewohnerin",
    "bewohnerinnen", "bibliothekar", "bibliothekare", "bibliothekarin", "bibliothekarinnen",
    "biologe", "biologein", "biologeinnen", "biologen", "boss", "bosse", "botschafter",
    "botschafterin", "botschafterinnen", "braut", "braute", "bruder", "bruderin", "bruderinnen",
    "bräutigam", "bräutigame", "brüder", "bub", "bube", "buchhalter", "buchhalterin",
    "buchhalterinnen", "busfahrer", "busfahrerin", "busfahrerinnen", "bäcker", "bäckerin",
    "bäckerinnen", "bürger", "bürgerin", "bürgerinnen", "bürgermeister", "bürgermeisterin",
    "bürgermeisterinnen", "chef", "chefe", "chefin", "chefinnen", "chefs", "chemiker",
    "chemikerin", "chemikerinnen", "chirurg", "chirurge", "cousin", "cousine", "cousinen",
    "cousinnen", "dachdecker", "dachdeckerin", "dachdeckerinnen", "dame", "damen", "designer",
    "designerin", "designerinnen", "dichter", "dichterin", "dichterinnen", "dieb", "diebe",
    "diplomat", "diplomaten", "diplomatin", "diplomatinnen", "direktor", "direktore",
    "direktorin", "direktorinnen", "dolmetscher", "dolmetscherin", "dolmetscherinnen", "dozent",
    "dozenten", "dozentin", "dozentinnen", "ehefrau", "ehefraue", "eheleute", "ehemann",
    "ehemanne", "ehemänner", "ehepaare", "ehepaarin", "ehepaarinnen", "einwohner",
    "einwohnerin", "einwohnerinnen", "elektriker", "elektrikerin", "elektrikerinnen", "eltern",
    "elterne", "enkel", "enkelin", "enkelinnen", "enkelkinder", "entwickler", "entwicklerin",
    "entwicklerinnen", "erwachsene", "erwachsenen", "erwachsener", "erwachsenerin",
    "erwachsenerinnen", "erzieher", "erzieherin", "erzieherinnen", "experte", "experten",
    "fachfrau", "fachleute", "fachmann", "fachmanne", "fachmänner", "fahrer", "fahrerin",
    "fahrerinnen", "fahrgast", "fahrgaste", "feind", "feinde", "feuerwehrfrau",
    "feuerwehrleute", "feuerwehrmann", "feuerwehrmanne", "feuerwehrmänner", "fischer",
    "fischerin", "fischerinnen", "fleischer", "fleischerin", "fleischerinnen", "flüchtling",
    "flüchtlinge", "forscher", "forscherin", "forscherinnen", "fotograf", "fotografe",
    "fotografin", "fotografinnen", "frau", "fraue", "frauen", "freiwilliger", "freiwilligerin",
    "freiwilligerinnen", "fremde", "fremden", "fremder", "fremderin", "fremderinnen", "freund",
    "freunde", "freundin", "freundinnen", "friseur", "friseure", "friseurin", "friseurinnen",
    "fußballer", "fußballerin", "fußballerinnen", "fußgänger", "fußgängerin", "fußgängerinnen",
    "gast", "gaste", "gefährte", "gefährten", "gegner", "gegnerin", "gegnerinnen", "genosse",
    "genossen", "geschwister", "geschwisterin", "geschwisterinnen", "geschäftsführer",
    "geschäftsführerin", "geschäftsführerinnen", "gewinner", "gewinnerin", "gewinnerinnen",
    "gläubige", "gläubigen", "gläubiger", "gläubigerin", "gläubigerinnen", "großeltern",
    "großmutter", "großmutterin", "großmutterinnen", "großvater", "großvaterin",
    "großvaterinnen", "gärtner", "gärtnerin", "gärtnerinnen", "gäste", "hausarzt", "hausarzte",
    "hausarztin", "hausarztinnen", "hausmeister", "hausmeisterin", "hausmeisterinnen",
    "hebamme", "hebammen", "held", "helde", "helfer", "helferin", "helferinnen", "herr",
    "herre", "herren", "historiker", "historikerin", "historikerinnen", "häftling", "häftlinge",
    "informatiker", "informatikerin", "informatikerinnen", "ingenieur", "ingenieure",
    "ingenieurin", "ingenieurinnen", "installateur", "installateure", "installateurin",
    "installateurinnen", "journalist", "journalisten", "journalistin", "journalistinnen",
    "jugendliche", "jugendlichen", "jugendlicher", "jugendlicherin", "jugendlicherinnen",
    "junge", "jungen", "kamerafrau", "kameraleute", "kameramann", "kameramanne", "kameramänner",
    "kandidat", "kandidaten", "kandidatin", "kandidatinnen", "kapitän", "kapitäne", "kassierer",
    "kassiererin", "kassiererinnen", "kellner", "kellnerin", "kellnerinnen", "kerl", "kerle",
    "kind", "kinde", "kinder", "kinderarzt", "kinderarzte", "kinderarztin", "kinderarztinnen",
    "klempner", "klempnerin", "klempnerinnen", "klient", "klienten", "klientin", "klientinnen",
    "koch", "koche", "kollege", "kollegen", "kolleginnen", "komponist", "komponisten",
    "komponistin", "komponistinnen", "konditor", "konditore", "konditorin", "konditorinnen",
    "konkurrent", "konkurrenten", "konkurrentin", "konkurrentinnen", "kosmetikerin",
    "kosmetikerinnen", "kranke", "kranken", "krankenpfleger", "krankenpflegerin",
    "krankenpflegerinnen", "kranker", "krankerin", "krankerinnen", "kumpel", "kunde", "kunden",
    "kundinnen", "kurier", "kurierin", "kurierinnen", "käufer", "käuferin", "käuferinnen",
    "köche", "köchin", "könig", "könige", "königin", "königinnen", "künstler", "künstlerin",
    "künstlerinnen", "laie", "laien", "landwirt", "landwirte", "lehrer", "lehrerin",
    "lehrerinnen", "leiter", "leiterin", "leiterinnen", "leser", "leserin", "leserinnen",
    "leute", "leuten", "lokführer", "lokführerin", "lokführerinnen", "läufer", "läuferin",
    "läuferinnen", "maler", "malerin", "malerinnen", "manager", "managerin", "managerinnen",
    "mann", "manne", "mathematiker", "mathematikerin", "mathematikerinnen", "matrose",
    "matrosen", "maurer", "maurerin", "maurerinnen", "mechaniker", "mechanikerin",
    "mechanikerinnen", "meister", "meisterin", "meisterinnen", "mensch", "mensche", "menschen",
    "metzger", "metzgerin", "metzgerinnen", "mieter", "mieterin", "mieterinnen", "migrant",
    "migranten", "migrantin", "migrantinnen", "minister", "ministerin", "ministerinnen",
    "mitarbeiter", "mitarbeiterin", "mitarbeiterinnen", "mitglied", "mitgliede", "moderator",
    "moderatore", "moderatorin", "moderatorinnen", "musiker", "musikerin", "musikerinnen",
    "mutter", "mutterin", "mutterinnen", "mädchen", "männer", "mönch", "mönche", "mütter",
    "nachbar", "nachbare", "nachbarin", "nachbarinnen", "nachbarn", "neffe", "neffen", "nichte",
    "nichten", "nonne", "nonnen", "notar", "notare", "notarin", "notarinnen", "nutzer",
    "nutzerin", "nutzerinnen", "näherin", "näherinnen", "obdachlose", "obdachlosen",
    "obdachloser", "obdachloserin", "obdachloserinnen", "oma", "omae", "onkel", "opa", "opae",
    "opfer", "opferin", "opferinnen", "papst", "papste", "partner", "partnerin", "partnerinnen",
    "passagier", "passagierin", "passagierinnen", "pate", "paten", "patient", "patienten",
    "patientin", "patientinnen", "patin", "patinnen", "pensionär", "pensionäre", "person",
    "persone", "pfarrer", "pfarrerin", "pfarrerinnen", "pfleger", "pflegerin", "pflegerinnen",
    "physiker", "physikerin", "physikerinnen", "pilot", "piloten", "politiker", "politikerin",
    "politikerinnen", "polizist", "polizisten", "polizistin", "polizistinnen", "portier",
    "portierin", "portierinnen", "postbote", "postboten", "praktikant", "praktikanten",
    "praktikantin", "praktikantinnen", "priester", "priesterin", "priesterinnen", "prinz",
    "prinze", "prinzessin", "prinzessinnen", "professor", "professore", "professorin",
    "professorinnen", "profi", "profie", "programmierer", "programmiererin",
    "programmiererinnen", "psychologe", "psychologein", "psychologeinnen", "psychologen",
    "putzfrau", "putzfraue", "radfahrer", "radfahrerin", "radfahrerinnen", "redakteur",
    "redakteure", "redakteurin", "redakteurinnen", "regisseur", "regisseure", "regisseurin",
    "regisseurinnen", "reinigungskraft", "reinigungskrafte", "reiseleiter", "reiseleiterin",
    "reiseleiterinnen", "reisender", "reisenderin", "reisenderinnen", "rentner", "rentnerin",
    "rentnerinnen", "reporter", "reporterin", "reporterinnen", "retter", "retterin",
    "retterinnen", "rezeptionist", "rezeptionisten", "rezeptionistin", "rezeptionistinnen",
    "richter", "richterin", "richterinnen", "rivale", "rivalen", "sanitäter", "sanitäterin",
    "sanitäterinnen", "schauspieler", "schauspielerin", "schauspielerinnen", "schiedsrichter",
    "schiedsrichterin", "schiedsrichterinnen", "schneider", "schneiderin", "schneiderinnen",
    "schreiner", "schreinerin", "schreinerinnen", "schriftsteller", "schriftstellerin",
    "schriftstellerinnen", "schwager", "schwagerin", "schwagerinnen", "schwester",
    "schwesterin", "schwesterinnen", "schwestern", "schwiegermutter", "schwiegermutterin",
    "schwiegermutterinnen", "schwiegersohn", "schwiegersohne", "schwiegertochter",
    "schwiegertochterin", "schwiegertochterinnen", "schwiegervater", "schwiegervaterin",
    "schwiegervaterinnen", "schwimmer", "schwimmerin", "schwimmerinnen", "schwägerin",
    "schwägerinnen", "schüler", "schülerin", "schülerinnen", "sekretär", "sekretäre", "senior",
    "seniore", "seniorin", "seniorinnen", "sieger", "siegerin", "siegerinnen", "single",
    "singlen", "sohn", "sohne", "soldat", "soldaten", "soldatin", "soldatinnen", "spender",
    "spenderin", "spenderinnen", "spezialist", "spezialisten", "spezialistin",
    "spezialistinnen", "sportler", "sportlerin", "sportlerinnen", "sprecher", "sprecherin",
    "sprecherinnen", "staatsanwalt", "staatsanwalte", "stewardess", "stewardesse",
    "stiefmutter", "stiefmutterin", "stiefmutterinnen", "stiefsohn", "stiefsohne",
    "stieftochter", "stieftochterin", "stieftochterinnen", "stiefvater", "stiefvaterin",
    "stiefvaterinnen", "student", "studenten", "studentin", "studentinnen", "sänger",
    "sängerin", "sängerinnen", "söhne", "tante", "tanten", "taxifahrer", "taxifahrerin",
    "taxifahrerinnen", "techniker", "technikerin", "technikerinnen", "teenager", "teenagerin",
    "teenagerinnen", "teilnehmer", "teilnehmerin", "teilnehmerinnen", "therapeut", "therapeute",
    "tierarzt", "tierarzte", "tierarztin", "tierarztinnen", "tischler", "tischlerin",
    "tischlerinnen", "tochter", "tochterin", "tochterinnen", "tourist", "touristen",
    "touristin", "touristinnen", "trainer", "trainerin", "trainerinnen", "typ", "type", "täter",
    "täterin", "täterinnen", "töchter", "unternehmer", "unternehmerin", "unternehmerinnen",
    "vater", "vaterin", "vaterinnen", "verbrecher", "verbrecherin", "verbrecherinnen",
    "verbündeter", "verbündeterin", "verbündeterinnen", "verkäufer", "verkäuferin",
    "verkäuferinnen", "verlierer", "verliererin", "verliererinnen", "verlobte", "verlobten",
    "verlobter", "verlobterin", "verlobterinnen", "vermieter", "vermieterin", "vermieterinnen",
    "vertreter", "vertreterin", "vertreterinnen", "verwandte", "verwandten", "verwandter",
    "verwandterin", "verwandterinnen", "vorgesetzte", "vorgesetzten", "vorgesetzter",
    "vorgesetzterin", "vorgesetzterinnen", "vorsitzender", "vorsitzenderin",
    "vorsitzenderinnen", "väter", "wissenschaftler", "wissenschaftlerin",
    "wissenschaftlerinnen", "witwe", "witwen", "witwer", "witwerin", "witwerinnen", "wächter",
    "wächterin", "wächterinnen", "zahnarzt", "zahnarzte", "zahnarztin", "zahnarztinnen",
    "zeuge", "zeugen", "zuhörer", "zuhörerin", "zuhörerinnen", "zuschauer", "zuschauerin",
    "zuschauerinnen", "zwilling", "zwillinge", "ärzte", "ärzten", "ärztin", "ärztinnen",
    "übersetzer", "übersetzerin", "übersetzerinnen",
}


# ── Проверенные ВЕЩИ ────────────────────────────────────────────────────────
# Каждое слово здесь проверено человеком: это точно предмет/понятие, а не тот,
# о ком спрашивают «Auf wen». Банк вещей заполняется руками, и незнакомое слово
# не должно проскочить молча — проверка банка (check_bank) не выпустит объект,
# которого нет в этом списке. Добавляешь глагол с новым объектом — впиши его сюда
# ОСОЗНАННО, ответив себе на один вопрос: о нём спрашивают «Worauf» или «Auf wen»?
_VETTED_THINGS = {
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
    "die Verspätung",
    "die Vorschrift",
    "die Tasse",
    "die Wahrheit",
    "die Wohnung",
    "die Woche",
    "die Zeit",
    "die Zitrone",
    "die Zukunft",
    "die Änderung",
    "die Überraschung",
    "die Übertreibung",
}


def _is_person_noun(phrase: str) -> bool:
    """Человек ли это. Артикль отбрасываем, сравниваем по основному слову."""
    head = str(phrase or "").strip()
    for article in ("der ", "die ", "das ", "den ", "dem ", "des "):
        if head.lower().startswith(article):
            head = head[len(article):]
            break
    return head.strip().lower() in _PERSON_NOUNS


def _wo_form(prep: str) -> str:
    """warten AUF → 'worauf'; träumen VON → 'wovon'. Vowel-initial preps insert -r-."""
    prep = prep.lower()
    return ("wor" + prep) if prep[0] in _VOWELS else ("wo" + prep)


def _person_form(prep: str, case: str) -> str:
    """auf j-n (Akk) → 'Auf wen'; mit j-m (Dat) → 'Mit wem'."""
    wq = "wen" if case == "akk" else "wem"
    return f"{prep.capitalize()} {wq}"


# Article declension for the natural clue line (governed case is always Akk or Dat).
_GENDER = {"der": "m", "die": "f", "das": "n"}
_ART_AKK = {"der": "den", "die": "die", "das": "das"}
_ART_DAT = {"der": "dem", "die": "der", "das": "dem"}
# Obligatory/idiomatic contractions so the clue reads like real German.
_CONTRACT = {
    "an dem": "am", "an das": "ans", "in dem": "im", "in das": "ins",
    "zu dem": "zum", "zu der": "zur", "bei dem": "beim", "von dem": "vom",
    "um das": "ums", "für das": "fürs", "auf das": "aufs", "durch das": "durchs",
    "vor dem": "vorm",
}

_CASE_RU = {"akk": "Akkusativ", "dat": "Dativ"}

# ── Distractor pools (capitalized, sentence-start form) — the full set of German
#    pronominal adverbs used with verb government, plus their person counterparts ──
_WO_DISTRACTORS = [
    "Woran", "Worauf", "Woraus", "Worin", "Worüber", "Worum", "Worunter",
    "Wovon", "Wovor", "Womit", "Wonach", "Wozu", "Wofür", "Wodurch",
    "Wogegen", "Wobei",
]
_PERSON_DISTRACTORS = [
    "An wen", "Auf wen", "Für wen", "Gegen wen", "In wen", "Über wen", "Um wen",
    "Mit wem", "Nach wem", "Von wem", "Vor wem", "Zu wem", "Bei wem",
    "Aus wem", "Unter wem",
]

# First names only — they never decline after a preposition, so the clue line
# stays 100% correct for person items ("Auf Anna", "Mit Max", "Über Paul").
_PERSON_NAMES = [
    ("Anna", ""), ("Thomas", ""), ("Julia", ""), ("Max", ""), ("Lena", ""),
    ("Paul", ""), ("Sofia", ""), ("David", ""), ("Emma", ""), ("Jonas", ""),
    ("Nina", ""), ("Felix", ""), ("Klara", ""), ("Ben", ""),
]

# ── The governed-preposition bank ────────────────────────────────────────────
# entry = {
#   "lemma": display combo, "prep": governed preposition, "case": akk|dat,
#   "ru": Russian gloss, "person": can the object be a person?,
#   "person_only": True → always a person (e.g. "sich treffen mit"),
#   "q": [question frames placed AFTER the wo-word], "obj": [(nom phrase, ru)…],
# }
_BANK: list[dict] = [
    # ── Пополнение банка: глаголы с управлением, которых не хватало ───────────
    # Объекты здесь — ВЕЩИ. О людях спрашивает отдельная ветка по флагу person.
    {"lemma": "nachdenken über", "prep": "über", "case": "akk", "ru": "размышлять", "person": False,
     "q": ["denkst du nach?", "denkt ihr nach?"],
     "obj": [("die Frage", "вопрос"), ("das Problem", "проблема"), ("der Vorschlag", "предложение"), ("die Zukunft", "будущее")]},
    {"lemma": "reagieren auf", "prep": "auf", "case": "akk", "ru": "реагировать", "person": True,
     "q": ["reagierst du?", "reagiert ihr?"],
     "obj": [("die Kritik", "критика"), ("die Nachricht", "новость")]},
    {"lemma": "antworten auf", "prep": "auf", "case": "akk", "ru": "отвечать (на)", "person": False,
     "q": ["antwortest du?", "antwortet ihr?"],
     "obj": [("die Frage", "вопрос"), ("die Einladung", "приглашение")]},
    {"lemma": "sich beschweren über", "prep": "über", "case": "akk", "ru": "жаловаться", "person": True,
     "q": ["beschwerst du dich?", "beschwert ihr euch?"],
     "obj": [("der Lärm", "шум"), ("das Essen", "еда"), ("die Verspätung", "опоздание")]},
    {"lemma": "sich beschäftigen mit", "prep": "mit", "case": "dat", "ru": "заниматься", "person": True,
     "q": ["beschäftigst du dich?", "beschäftigt ihr euch?"],
     "obj": [("die Musik", "музыка"), ("das Thema", "тема"), ("die Aufgabe", "задача")]},
    {"lemma": "sich bewerben um", "prep": "um", "case": "akk", "ru": "претендовать (на)", "person": False,
     "q": ["bewirbst du dich?", "bewerbt ihr euch?"],
     "obj": [("die Stelle", "должность"), ("das Stipendium", "стипендия")]},
    {"lemma": "sich beziehen auf", "prep": "auf", "case": "akk", "ru": "ссылаться (на)", "person": True,
     "q": ["beziehst du dich?", "bezieht ihr euch?"],
     "obj": [("der Artikel", "статья"), ("das Gesetz", "закон")]},
    {"lemma": "rechnen mit", "prep": "mit", "case": "dat", "ru": "рассчитывать (на)", "person": True,
     "q": ["rechnest du?", "rechnet ihr?"],
     "obj": [("der Regen", "дождь"), ("die Verspätung", "опоздание")]},
    {"lemma": "sich einigen auf", "prep": "auf", "case": "akk", "ru": "договариваться (о)", "person": False,
     "q": ["einigt ihr euch?", "einigen Sie sich?"],
     "obj": [("der Preis", "цена"), ("der Termin", "срок")]},
    {"lemma": "sich einsetzen für", "prep": "für", "case": "akk", "ru": "заступаться", "person": True,
     "q": ["setzt du dich ein?", "setzt ihr euch ein?"],
     "obj": [("das Recht", "право"), ("die Gerechtigkeit", "справедливость"), ("die Umwelt", "окружающая среда")]},
    {"lemma": "sich beteiligen an", "prep": "an", "case": "dat", "ru": "участвовать (в)", "person": False,
     "q": ["beteiligst du dich?", "beteiligt ihr euch?"],
     "obj": [("das Projekt", "проект"), ("die Diskussion", "дискуссия")]},
    {"lemma": "sich orientieren an", "prep": "an", "case": "dat", "ru": "ориентироваться (на)", "person": True,
     "q": ["orientierst du dich?", "orientiert ihr euch?"],
     "obj": [("das Vorbild", "образец"), ("die Regel", "правило")]},
    {"lemma": "leiden unter", "prep": "unter", "case": "dat", "ru": "страдать (от)", "person": True,
     "q": ["leidest du?", "leidet ihr?"],
     "obj": [("der Stress", "стресс"), ("die Hitze", "жара")]},
    {"lemma": "sich richten nach", "prep": "nach", "case": "dat", "ru": "руководствоваться", "person": True,
     "q": ["richtest du dich?", "richtet ihr euch?"],
     "obj": [("die Vorschrift", "предписание"), ("der Plan", "план")]},
    {"lemma": "riechen nach", "prep": "nach", "case": "dat", "ru": "пахнуть", "person": False,
     "q": ["riecht es?", "riecht das?"],
     "obj": [("der Kaffee", "кофе"), ("das Parfüm", "духи"), ("der Rauch", "дым")]},
    {"lemma": "schmecken nach", "prep": "nach", "case": "dat", "ru": "иметь вкус", "person": False,
     "q": ["schmeckt es?", "schmeckt das?"],
     "obj": [("die Zitrone", "лимон"), ("der Knoblauch", "чеснок"), ("das Salz", "соль")]},
    {"lemma": "protestieren gegen", "prep": "gegen", "case": "akk", "ru": "протестовать", "person": True,
     "q": ["protestierst du?", "protestiert ihr?"],
     "obj": [("das Gesetz", "закон"), ("der Krieg", "война"), ("die Entscheidung", "решение")]},
    {"lemma": "kandidieren für", "prep": "für", "case": "akk", "ru": "баллотироваться (на)", "person": False,
     "q": ["kandidierst du?", "kandidiert ihr?"],
     "obj": [("das Amt", "должность"), ("der Vorsitz", "председательство")]},
    {"lemma": "zählen auf", "prep": "auf", "case": "akk", "ru": "рассчитывать (на)", "person": True,
     "q": ["zählst du?", "zählt ihr?"],
     "obj": [("die Unterstützung", "поддержка"), ("das Glück", "удача")]},
    {"lemma": "schwärmen von", "prep": "von", "case": "dat", "ru": "восторгаться", "person": True,
     "q": ["schwärmst du?", "schwärmt ihr?"],
     "obj": [("die Reise", "поездка"), ("die Musik", "музыка")]},
    {"lemma": "berichten über", "prep": "über", "case": "akk", "ru": "сообщать (о)", "person": True,
     "q": ["berichtest du?", "berichtet ihr?"],
     "obj": [("das Ereignis", "событие"), ("der Unfall", "авария")]},
    {"lemma": "profitieren von", "prep": "von", "case": "dat", "ru": "выигрывать (от)", "person": True,
     "q": ["profitierst du?", "profitiert ihr?"],
     "obj": [("die Erfahrung", "опыт"), ("das Angebot", "предложение")]},
    {"lemma": "sich distanzieren von", "prep": "von", "case": "dat", "ru": "дистанцироваться", "person": True,
     "q": ["distanzierst du dich?", "distanziert ihr euch?"],
     "obj": [("die Aussage", "высказывание"), ("der Plan", "план"), ("die Gewalt", "насилие")]},
    # ── VOR + Dativ ──
    {"lemma": "abraten von", "prep": "von", "case": "dat", "ru": "отговаривать (от)", "person": False,
     "q": ["rätst du ab?", "ratet ihr ab?"],
     "obj": [("die Reise", "поездка"), ("der Kauf", "покупка")]},
    {"lemma": "sich entwickeln zu", "prep": "zu", "case": "dat", "ru": "превращаться (в)", "person": False,
     "q": ["entwickelt sich das?", "entwickelt sich die Lage?"],
     "obj": [("das Problem", "проблема"), ("die Gewohnheit", "привычка")]},
    {"lemma": "neigen zu", "prep": "zu", "case": "dat", "ru": "быть склонным (к)", "person": False,
     "q": ["neigst du?", "neigt ihr?"],
     "obj": [("die Übertreibung", "преувеличение"), ("der Pessimismus", "пессимизм"), ("der Zweifel", "сомнение")]},
    {"lemma": "passen zu", "prep": "zu", "case": "dat", "ru": "подходить (к)", "person": True,
     "q": ["passt das?", "passt es gut?"],
     "obj": [("das Kleid", "платье"), ("die Farbe", "цвет"), ("die Hose", "брюки"), ("der Anlass", "повод")]},
    {"lemma": "vergleichen mit", "prep": "mit", "case": "dat", "ru": "сравнивать (с)", "person": False,
     "q": ["vergleichst du das?", "vergleicht ihr das?"],
     "obj": [("das Original", "оригинал"), ("der Preis", "цена")]},
    {"lemma": "verwechseln mit", "prep": "mit", "case": "dat", "ru": "путать (с)", "person": False,
     "q": ["verwechselst du das?", "verwechselt ihr das?"],
     "obj": [("der Name", "имя"), ("die Farbe", "цвет")]},
    {"lemma": "sich anpassen an", "prep": "an", "case": "akk", "ru": "приспосабливаться (к)", "person": False,
     "q": ["passt du dich an?", "passt ihr euch an?"],
     "obj": [("das Klima", "климат"), ("die Situation", "ситуация")]},
    {"lemma": "sich bemühen um", "prep": "um", "case": "akk", "ru": "стараться (ради)", "person": True,
     "q": ["bemühst du dich?", "bemüht ihr euch?"],
     "obj": [("die Stelle", "должность"), ("die Lösung", "решение")]},
    {"lemma": "sich melden bei", "prep": "bei", "case": "dat", "ru": "обращаться (к)", "person_only": True,
     "q": ["meldest du dich?", "meldet ihr euch?"],
     "obj": []},
    {"lemma": "sich austauschen über", "prep": "über", "case": "akk", "ru": "обмениваться мнениями (о)", "person": False,
     "q": ["tauscht ihr euch aus?", "tauschen Sie sich aus?"],
     "obj": [("die Erfahrung", "опыт"), ("das Ergebnis", "результат")]},
    {"lemma": "sich eignen für", "prep": "für", "case": "akk", "ru": "подходить (для)", "person": False,
     "q": ["eignest du dich?", "eignet ihr euch?"],
     "obj": [("der Job", "работа"), ("die Aufgabe", "задача"), ("das Fest", "праздник")]},
    {"lemma": "dienen zu", "prep": "zu", "case": "dat", "ru": "служить (для)", "person": False,
     "q": ["dient das Gerät?", "dient die Regel?"],
     "obj": [("die Sicherheit", "безопасность"), ("die Kontrolle", "контроль")]},
    {"lemma": "sich beklagen über", "prep": "über", "case": "akk", "ru": "сетовать (на)", "person": True,
     "q": ["beklagst du dich?", "beklagt ihr euch?"],
     "obj": [("das Wetter", "погода"), ("die Miete", "аренда")]},
    {"lemma": "sich verabreden mit", "prep": "mit", "case": "dat", "ru": "договариваться о встрече (с)", "person_only": True,
     "q": ["verabredest du dich?", "verabredet ihr euch?"],
     "obj": []},
    {"lemma": "sich erholen von", "prep": "von", "case": "dat", "ru": "оправиться (от)", "person": False,
     "q": ["erholst du dich?", "erholt ihr euch?"],
     "obj": [("die Krankheit", "болезнь"), ("der Stress", "стресс"), ("die Woche", "неделя")]},
    {"lemma": "denken an", "prep": "an", "case": "akk", "ru": "думать", "person": True,
     "q": ["denkst du gerade?", "denkt ihr oft?"],
     "obj": [("die Zukunft", "будущее"), ("der Urlaub", "отпуск")]},
    {"lemma": "sich erinnern an", "prep": "an", "case": "akk", "ru": "вспоминать", "person": True,
     "q": ["erinnerst du dich gern?", "erinnert ihr euch?"],
     "obj": [("die Kindheit", "детство"), ("das Treffen", "встреча")]},
    {"lemma": "glauben an", "prep": "an", "case": "akk", "ru": "верить", "person": True,
     "q": ["glaubst du?", "glaubt ihr wirklich?"],
     "obj": [("die Liebe", "любовь"), ("das Schicksal", "судьба")]},
    {"lemma": "sich gewöhnen an", "prep": "an", "case": "akk", "ru": "привыкать", "person": True,
     "q": ["gewöhnst du dich?", "gewöhnt ihr euch?"],
     "obj": [("das Klima", "климат"), ("die Arbeit", "работа")]},
    {"lemma": "sich wenden an", "prep": "an", "case": "akk", "ru": "обращаться", "person_only": True,
     # «Woran wendest du dich? — Ведомство» — так не говорят: обращаются К КОМУ-ТО,
     # и ведомство здесь такой же адресат, как человек.
     "q": ["wendest du dich?", "wendet ihr euch?"],
     "obj": []},
    {"lemma": "appellieren an", "prep": "an", "case": "akk", "ru": "апеллировать", "person": False,
     "q": ["appellierst du?", "appelliert ihr?"],
     "obj": [("die Vernunft", "разум"), ("das Gewissen", "совесть")]},
    # ── AN + Dativ ──
    {"lemma": "teilnehmen an", "prep": "an", "case": "dat", "ru": "участвовать (в)", "person": False,
     "q": ["nimmst du teil?", "nehmt ihr teil?"],
     "obj": [("der Kurs", "курс"), ("die Sitzung", "заседание")]},
    {"lemma": "arbeiten an", "prep": "an", "case": "dat", "ru": "работать (над)", "person": False,
     "q": ["arbeitest du?", "arbeitet ihr?"],
     "obj": [("das Projekt", "проект"), ("der Roman", "роман")]},
    {"lemma": "zweifeln an", "prep": "an", "case": "dat", "ru": "сомневаться (в)", "person": True,
     "q": ["zweifelst du?", "zweifelt ihr?"],
     "obj": [("der Plan", "план"), ("der Erfolg", "успех")]},
    {"lemma": "leiden an", "prep": "an", "case": "dat", "ru": "страдать (болезнью)", "person": False,
     "also_ok": [{"prep": "unter", "mode": "thing", "note": "«an» — о болезни как диагнозе («an der Grippe leiden»), «unter» — о том, что тяготит. Когда речь о болезни, немцы говорят и так, и так — засчитываем оба."}],
     "q": ["leidest du?", "leidet ihr?"],
     "obj": [("die Grippe", "грипп"), ("die Allergie", "аллергия")]},
    # ── AUF + Akkusativ ──
    {"lemma": "warten auf", "prep": "auf", "case": "akk", "ru": "ждать", "person": True,
     "q": ["wartest du schon so lange?", "wartet ihr gerade?"],
     "obj": [("der Bus", "автобус"), ("die Antwort", "ответ")]},
    {"lemma": "hoffen auf", "prep": "auf", "case": "akk", "ru": "надеяться", "person": False,
     "q": ["hoffst du?", "hofft ihr insgeheim?"],
     "obj": [("der Erfolg", "успех"), ("das Wunder", "чудо")]},
    {"lemma": "sich freuen auf", "prep": "auf", "case": "akk", "ru": "радоваться (предстоящему)", "person": False,
     "also_ok": [{"prep": "über", "mode": "thing", "note": "«auf» — радуешься тому, что ВПЕРЕДИ, «über» — тому, что УЖЕ случилось. По одной подсказке не различить — засчитываем оба."}],
     "q": ["freust du dich am meisten?", "freut ihr euch so?"],
     "obj": [("der Urlaub", "отпуск"), ("das Wochenende", "выходные")]},
    {"lemma": "achten auf", "prep": "auf", "case": "akk", "ru": "обращать внимание", "person": True,
     "q": ["achtest du besonders?", "achtet ihr?"],
     "obj": [("die Gesundheit", "здоровье"), ("der Preis", "цена")]},
    {"lemma": "aufpassen auf", "prep": "auf", "case": "akk", "ru": "присматривать", "person": True,
     "q": ["passt du auf?", "passt ihr auf?"],
     "obj": [("das Gepäck", "багаж"), ("die Tasche", "сумка")]},
    {"lemma": "sich verlassen auf", "prep": "auf", "case": "akk", "ru": "полагаться", "person": True,
     "q": ["verlässt du dich?", "verlasst ihr euch?"],
     "obj": [("das Glück", "удача"), ("der Plan", "план")]},
    {"lemma": "sich vorbereiten auf", "prep": "auf", "case": "akk", "ru": "готовиться", "person": False,
     "q": ["bereitest du dich vor?", "bereitet ihr euch vor?"],
     "obj": [("die Prüfung", "экзамен"), ("das Interview", "собеседование")]},
    {"lemma": "verzichten auf", "prep": "auf", "case": "akk", "ru": "отказываться (от)", "person": False,
     "q": ["verzichtest du?", "verzichtet ihr?"],
     "obj": [("der Urlaub", "отпуск"), ("das Auto", "машина")]},
    {"lemma": "sich konzentrieren auf", "prep": "auf", "case": "akk", "ru": "сосредотачиваться", "person": False,
     "q": ["konzentrierst du dich?", "konzentriert ihr euch?"],
     "obj": [("die Arbeit", "работа"), ("das Ziel", "цель")]},
    {"lemma": "bestehen auf", "prep": "auf", "case": "akk", "ru": "настаивать (на)", "person": False,
     "q": ["bestehst du?", "besteht ihr?"],
     "obj": [("die Regel", "правило"), ("das Recht", "право")]},
    {"lemma": "hinweisen auf", "prep": "auf", "case": "akk", "ru": "указывать (на)", "person": False,
     "q": ["weist du hin?", "weist ihr hin?"],
     "obj": [("die Gefahr", "опасность"), ("der Fehler", "ошибка")]},
    {"lemma": "stolz sein auf", "prep": "auf", "case": "akk", "ru": "гордиться", "person": True,
     "q": ["bist du stolz?", "seid ihr stolz?"],
     "obj": [("der Erfolg", "успех"), ("das Ergebnis", "результат")]},
    {"lemma": "neugierig sein auf", "prep": "auf", "case": "akk", "ru": "быть любопытным (к)", "person": True,
     "q": ["bist du neugierig?", "seid ihr neugierig?"],
     "obj": [("das Ergebnis", "результат"), ("die Überraschung", "сюрприз")]},
    {"lemma": "angewiesen sein auf", "prep": "auf", "case": "akk", "ru": "нуждаться (в)", "person": True,
     "q": ["bist du angewiesen?", "seid ihr angewiesen?"],
     "obj": [("die Hilfe", "помощь"), ("das Geld", "деньги")]},
    {"lemma": "Wert legen auf", "prep": "auf", "case": "akk", "ru": "придавать значение", "person": False,
     "q": ["legst du Wert?", "legt ihr Wert?"],
     "obj": [("die Qualität", "качество"), ("die Pünktlichkeit", "пунктуальность")]},
    {"lemma": "Lust haben auf", "prep": "auf", "case": "akk", "ru": "хотеть (чего-то)", "person": False,
     "q": ["hast du Lust?", "habt ihr Lust?"],
     "obj": [("der Kaffee", "кофе"), ("das Eis", "мороженое")]},
    {"lemma": "Rücksicht nehmen auf", "prep": "auf", "case": "akk", "ru": "считаться (с)", "person": True,
     "q": ["nimmst du Rücksicht?", "nehmt ihr Rücksicht?"],
     "obj": [("die Gesundheit", "здоровье"), ("die Umwelt", "окружающая среда")]},
    # ── AUS + Dativ ──
    {"lemma": "bestehen aus", "prep": "aus", "case": "dat", "ru": "состоять (из)", "person": False,
     "q": ["besteht das Team?", "besteht die Mischung?"],
     "obj": [("das Glas", "стекло"), ("das Metall", "металл")]},
    {"lemma": "entstehen aus", "prep": "aus", "case": "dat", "ru": "возникать (из)", "person": False,
     "q": ["entsteht es?", "entsteht das?"],
     "obj": [("die Idee", "идея"), ("der Konflikt", "конфликт")]},
    {"lemma": "folgen aus", "prep": "aus", "case": "dat", "ru": "следовать (из)", "person": False,
     "q": ["folgt es?", "folgt das?"],
     "obj": [("die Regel", "правило"), ("das Gesetz", "закон")]},
    # ── BEI + Dativ ──
    {"lemma": "helfen bei", "prep": "bei", "case": "dat", "ru": "помогать (в)", "person": False,
     "q": ["hilfst du?", "helft ihr?"],
     "obj": [("die Arbeit", "работа"), ("der Umzug", "переезд")]},
    {"lemma": "sich bedanken bei", "prep": "bei", "case": "dat", "ru": "благодарить (кого)", "person": True,
     "person_only": True, "q": ["bedankst du dich?", "bedankt ihr euch?"],
     "obj": []},
    {"lemma": "sich entschuldigen bei", "prep": "bei", "case": "dat", "ru": "извиняться (перед)", "person": True,
     "person_only": True, "q": ["entschuldigst du dich?", "entschuldigt ihr euch?"],
     "obj": []},
    {"lemma": "bleiben bei", "prep": "bei", "case": "dat", "ru": "оставаться (при)", "person": False,
     "q": ["bleibst du?", "bleibt ihr?"],
     "obj": [("die Meinung", "мнение"), ("der Plan", "план")]},
    {"lemma": "sich bewerben bei", "prep": "bei", "case": "dat", "ru": "подавать заявку (куда)", "person_only": True,
     # «Wobei bewirbst du dich?» — не по-немецки: фирма тут адресат (Bei wem).
     "q": ["bewirbst du dich?", "bewerbt ihr euch?"],
     "obj": []},
    # ── FÜR + Akkusativ ──
    {"lemma": "sich interessieren für", "prep": "für", "case": "akk", "ru": "интересоваться", "person": True,
     "q": ["interessierst du dich?", "interessiert ihr euch?"],
     "obj": [("die Kunst", "искусство"), ("der Sport", "спорт")]},
    {"lemma": "danken für", "prep": "für", "case": "akk", "ru": "благодарить (за)", "person": False,
     "q": ["dankst du?", "dankt ihr?"],
     "obj": [("die Hilfe", "помощь"), ("das Geschenk", "подарок")]},
    {"lemma": "sich bedanken für", "prep": "für", "case": "akk", "ru": "благодарить (за)", "person": False,
     "q": ["bedankst du dich?", "bedankt ihr euch?"],
     "obj": [("die Einladung", "приглашение"), ("das Geschenk", "подарок")]},
    {"lemma": "sorgen für", "prep": "für", "case": "akk", "ru": "заботиться", "person": True,
     "q": ["sorgst du?", "sorgt ihr?"],
     "obj": [("die Ordnung", "порядок"), ("das Essen", "еда")]},
    {"lemma": "sich entscheiden für", "prep": "für", "case": "akk", "ru": "выбирать", "person": True,
     "q": ["entscheidest du dich?", "entscheidet ihr euch?"],
     "obj": [("der Plan", "план"), ("das Auto", "машина")]},
    {"lemma": "sich engagieren für", "prep": "für", "case": "akk", "ru": "активно участвовать", "person": False,
     "q": ["engagierst du dich?", "engagiert ihr euch?"],
     "obj": [("die Umwelt", "экология"), ("das Projekt", "проект")]},
    {"lemma": "kämpfen für", "prep": "für", "case": "akk", "ru": "бороться (за)", "person": False,
     "also_ok": [{"prep": "um", "mode": "thing", "note": "«für» — за что борешься, «um» — за что борешься, рискуя это потерять («um den Sieg kämpfen»). Для свободы и победы верны оба."}],
     "q": ["kämpfst du?", "kämpft ihr?"],
     "obj": [("die Freiheit", "свобода"), ("das Recht", "право")]},
    {"lemma": "sich schämen für", "prep": "für", "case": "akk", "ru": "стыдиться", "person": True,
     "q": ["schämst du dich?", "schämt ihr euch?"],
     "obj": [("der Fehler", "ошибка"), ("das Verhalten", "поведение")]},
    {"lemma": "verantwortlich sein für", "prep": "für", "case": "akk", "ru": "отвечать (за)", "person": True,
     "q": ["bist du verantwortlich?", "seid ihr verantwortlich?"],
     "obj": [("das Projekt", "проект"), ("die Sicherheit", "безопасность")]},
    {"lemma": "ausgeben für", "prep": "für", "case": "akk", "ru": "тратить (на)", "person": False,
     "q": ["gibst du aus?", "gebt ihr aus?"],
     "obj": [("das Essen", "еда"), ("die Kleidung", "одежда")]},
    # ── GEGEN + Akkusativ ──
    {"lemma": "kämpfen gegen", "prep": "gegen", "case": "akk", "ru": "бороться (против)", "person": True,
     "q": ["kämpfst du?", "kämpft ihr?"],
     "obj": [("die Ungerechtigkeit", "несправедливость"), ("die Korruption", "коррупция")]},
    {"lemma": "sich wehren gegen", "prep": "gegen", "case": "akk", "ru": "защищаться", "person": True,
     "q": ["wehrst du dich?", "wehrt ihr euch?"],
     "obj": [("der Vorwurf", "упрёк"), ("die Kritik", "критика")]},
    {"lemma": "verstoßen gegen", "prep": "gegen", "case": "akk", "ru": "нарушать", "person": False,
     "q": ["verstößt du?", "verstoßt ihr?"],
     "obj": [("die Regel", "правило"), ("das Gesetz", "закон")]},
    {"lemma": "stimmen gegen", "prep": "gegen", "case": "akk", "ru": "голосовать (против)", "person": True,
     "q": ["stimmst du?", "stimmt ihr?"],
     "obj": [("der Antrag", "заявление"), ("der Plan", "план")]},
    # ── IN + Akkusativ ──
    {"lemma": "sich verlieben in", "prep": "in", "case": "akk", "ru": "влюбляться", "person": True,
     "person_only": True, "q": ["verliebst du dich?", "verliebt ihr euch?"],
     "obj": []},
    {"lemma": "einwilligen in", "prep": "in", "case": "akk", "ru": "соглашаться (на)", "person": False,
     "q": ["willigst du ein?", "willigt ihr ein?"],
     "obj": [("der Plan", "план"), ("die Änderung", "изменение")]},
    {"lemma": "geraten in", "prep": "in", "case": "akk", "ru": "попадать (в)", "person": False,
     "q": ["gerätst du?", "geratet ihr?"],
     "obj": [("die Panik", "паника"), ("der Streit", "ссора")]},
    {"lemma": "sich einmischen in", "prep": "in", "case": "akk", "ru": "вмешиваться", "person": False,
     "q": ["mischst du dich ein?", "mischt ihr euch ein?"],
     "obj": [("die Angelegenheit", "дело"), ("der Streit", "ссора")]},
    {"lemma": "investieren in", "prep": "in", "case": "akk", "ru": "инвестировать", "person": False,
     "q": ["investierst du?", "investiert ihr?"],
     "obj": [("die Firma", "фирма"), ("die Bildung", "образование")]},
    # ── IN + Dativ ──
    {"lemma": "bestehen in", "prep": "in", "case": "dat", "ru": "заключаться (в)", "person": False,
     "q": ["besteht es?", "besteht das?"],
     "obj": [("das Problem", "проблема"), ("der Vorteil", "преимущество")]},
    # ── MIT + Dativ ──
    {"lemma": "sprechen mit", "prep": "mit", "case": "dat", "ru": "говорить (с)", "person": True,
     "person_only": True, "q": ["sprichst du?", "sprecht ihr?"],
     "obj": []},
    {"lemma": "reden mit", "prep": "mit", "case": "dat", "ru": "разговаривать (с)", "person": True,
     "person_only": True, "q": ["redest du?", "redet ihr?"],
     "obj": []},
    {"lemma": "telefonieren mit", "prep": "mit", "case": "dat", "ru": "звонить (кому)", "person": True,
     "person_only": True, "q": ["telefonierst du?", "telefoniert ihr?"],
     "obj": []},
    {"lemma": "sich treffen mit", "prep": "mit", "case": "dat", "ru": "встречаться (с)", "person": True,
     "person_only": True, "q": ["triffst du dich?", "trefft ihr euch?"],
     "obj": []},
    {"lemma": "sich streiten mit", "prep": "mit", "case": "dat", "ru": "ссориться (с)", "person": True,
     "person_only": True, "q": ["streitest du dich?", "streitet ihr euch?"],
     "obj": []},
    {"lemma": "sich unterhalten mit", "prep": "mit", "case": "dat", "ru": "беседовать (с)", "person": True,
     "person_only": True, "q": ["unterhältst du dich?", "unterhaltet ihr euch?"],
     "obj": []},
    {"lemma": "sich verstehen mit", "prep": "mit", "case": "dat", "ru": "ладить (с)", "person": True,
     "person_only": True, "q": ["verstehst du dich?", "versteht ihr euch?"],
     "obj": []},
    {"lemma": "verheiratet sein mit", "prep": "mit", "case": "dat", "ru": "быть в браке (с)", "person": True,
     "person_only": True, "q": ["bist du verheiratet?", "seid ihr verheiratet?"],
     "obj": []},
    {"lemma": "anfangen mit", "prep": "mit", "case": "dat", "ru": "начинать (с)", "person": False,
     "q": ["fängst du an?", "fangt ihr an?"],
     "obj": [("die Arbeit", "работа"), ("das Studium", "учёба")]},
    {"lemma": "aufhören mit", "prep": "mit", "case": "dat", "ru": "прекращать", "person": False,
     "q": ["hörst du auf?", "hört ihr auf?"],
     "obj": [("das Rauchen", "курение"), ("der Lärm", "шум")]},
    {"lemma": "beginnen mit", "prep": "mit", "case": "dat", "ru": "начинать (с)", "person": False,
     "q": ["beginnst du?", "beginnt ihr?"],
     "obj": [("die Arbeit", "работа"), ("das Projekt", "проект")]},
    {"lemma": "zufrieden sein mit", "prep": "mit", "case": "dat", "ru": "быть довольным", "person": True,
     "q": ["bist du zufrieden?", "seid ihr zufrieden?"],
     "obj": [("das Ergebnis", "результат"), ("die Arbeit", "работа")]},
    {"lemma": "einverstanden sein mit", "prep": "mit", "case": "dat", "ru": "быть согласным", "person": True,
     "q": ["bist du einverstanden?", "seid ihr einverstanden?"],
     "obj": [("der Plan", "план"), ("der Vorschlag", "предложение")]},
    {"lemma": "übereinstimmen mit", "prep": "mit", "case": "dat", "ru": "совпадать (с)", "person": True,
     "q": ["stimmst du überein?", "stimmt ihr überein?"],
     "obj": [("die Aussage", "высказывание"), ("der Bericht", "отчёт")]},
    # ── NACH + Dativ ──
    {"lemma": "fragen nach", "prep": "nach", "case": "dat", "ru": "спрашивать (о)", "person": True,
     "q": ["fragst du?", "fragt ihr?"],
     "obj": [("der Weg", "дорога"), ("die Uhrzeit", "время")]},
    {"lemma": "suchen nach", "prep": "nach", "case": "dat", "ru": "искать", "person": True,
     "q": ["suchst du?", "sucht ihr?"],
     "obj": [("die Lösung", "решение"), ("der Schlüssel", "ключ")]},
    {"lemma": "sich erkundigen nach", "prep": "nach", "case": "dat", "ru": "справляться (о)", "person": True,
     "q": ["erkundigst du dich?", "erkundigt ihr euch?"],
     "obj": [("der Preis", "цена"), ("der Weg", "дорога")]},
    {"lemma": "streben nach", "prep": "nach", "case": "dat", "ru": "стремиться", "person": False,
     "q": ["strebst du?", "strebt ihr?"],
     "obj": [("der Erfolg", "успех"), ("die Macht", "власть")]},
    {"lemma": "sich sehnen nach", "prep": "nach", "case": "dat", "ru": "тосковать", "person": True,
     "q": ["sehnst du dich?", "sehnt ihr euch?"],
     "obj": [("die Heimat", "родина"), ("die Ruhe", "покой")]},
    {"lemma": "greifen nach", "prep": "nach", "case": "dat", "ru": "хватать", "person": False,
     "q": ["greifst du?", "greift ihr?"],
     "obj": [("das Glas", "стакан"), ("die Tasse", "чашка")]},
    {"lemma": "klingen nach", "prep": "nach", "case": "dat", "ru": "звучать (как)", "person": False,
     "q": ["klingt es?", "klingt das?"],
     "obj": [("der Ärger", "проблемы"), ("die Ausrede", "отговорка")]},
    # ── ÜBER + Akkusativ ──
    {"lemma": "sprechen über", "prep": "über", "case": "akk", "ru": "говорить (о)", "person": True,
     "also_ok": [{"prep": "mit", "mode": "person", "note": "«mit» — С КЕМ говоришь, «über» — О КОМ говоришь. По одному имени не понять, что имелось в виду, — засчитываем оба."}],
     "q": ["sprichst du?", "sprecht ihr?"],
     "obj": [("die Politik", "политика"), ("der Film", "фильм")]},
    {"lemma": "reden über", "prep": "über", "case": "akk", "ru": "говорить (о)", "person": True,
     "also_ok": [{"prep": "mit", "mode": "person", "note": "«mit» — С КЕМ говоришь, «über» — О КОМ говоришь. По одному имени не понять, что имелось в виду, — засчитываем оба."}],
     "q": ["redest du?", "redet ihr?"],
     "obj": [("das Problem", "проблема"), ("die Arbeit", "работа")]},
    {"lemma": "sich freuen über", "prep": "über", "case": "akk", "ru": "радоваться (случившемуся)", "person": True,
     "also_ok": [{"prep": "auf", "mode": "thing", "note": "«auf» — радуешься тому, что ВПЕРЕДИ, «über» — тому, что УЖЕ случилось. По одной подсказке не различить — засчитываем оба."}],
     "q": ["freust du dich?", "freut ihr euch so sehr?"],
     "obj": [("das Geschenk", "подарок"), ("die Nachricht", "новость")]},
    {"lemma": "sich ärgern über", "prep": "über", "case": "akk", "ru": "злиться", "person": True,
     "q": ["ärgerst du dich?", "ärgert ihr euch?"],
     "obj": [("der Fehler", "ошибка"), ("das Wetter", "погода")]},
    {"lemma": "sich aufregen über", "prep": "über", "case": "akk", "ru": "нервничать (из-за)", "person": True,
     "q": ["regst du dich auf?", "regt ihr euch auf?"],
     "obj": [("die Nachricht", "новость"), ("der Stau", "пробка")]},
    {"lemma": "schreiben über", "prep": "über", "case": "akk", "ru": "писать (о)", "person": True,
     "q": ["schreibst du?", "schreibt ihr?"],
     "obj": [("die Reise", "поездка"), ("das Ereignis", "событие")]},
    {"lemma": "sich informieren über", "prep": "über", "case": "akk", "ru": "узнавать (о)", "person": True,
     "q": ["informierst du dich?", "informiert ihr euch?"],
     "obj": [("das Thema", "тема"), ("der Kurs", "курс")]},
    {"lemma": "lachen über", "prep": "über", "case": "akk", "ru": "смеяться (над)", "person": True,
     "q": ["lachst du?", "lacht ihr?"],
     "obj": [("der Witz", "шутка"), ("die Situation", "ситуация")]},
    {"lemma": "sich wundern über", "prep": "über", "case": "akk", "ru": "удивляться", "person": True,
     "q": ["wunderst du dich?", "wundert ihr euch?"],
     "obj": [("das Ergebnis", "результат"), ("die Reaktion", "реакция")]},
    {"lemma": "diskutieren über", "prep": "über", "case": "akk", "ru": "обсуждать", "person": True,
     "q": ["diskutierst du?", "diskutiert ihr?"],
     "obj": [("das Thema", "тема"), ("die Politik", "политика")]},
    {"lemma": "verfügen über", "prep": "über", "case": "akk", "ru": "располагать (чем)", "person": False,
     "q": ["verfügst du?", "verfügt ihr?"],
     "obj": [("das Wissen", "знание"), ("das Kapital", "капитал")]},
    {"lemma": "klagen über", "prep": "über", "case": "akk", "ru": "жаловаться (на)", "person": False,
     "q": ["klagst du?", "klagt ihr?"],
     "obj": [("der Schmerz", "боль"), ("die Müdigkeit", "усталость")]},
    {"lemma": "urteilen über", "prep": "über", "case": "akk", "ru": "судить (о)", "person": True,
     "q": ["urteilst du?", "urteilt ihr?"],
     "obj": [("der Fall", "случай"), ("das Buch", "книга")]},
    {"lemma": "froh sein über", "prep": "über", "case": "akk", "ru": "быть радым", "person": False,
     "q": ["bist du froh?", "seid ihr froh?"],
     "obj": [("die Nachricht", "новость"), ("das Geschenk", "подарок")]},
    {"lemma": "traurig sein über", "prep": "über", "case": "akk", "ru": "грустить", "person": True,
     "q": ["bist du traurig?", "seid ihr traurig?"],
     "obj": [("der Verlust", "потеря"), ("das Ende", "конец")]},
    # ── UM + Akkusativ ──
    {"lemma": "sich kümmern um", "prep": "um", "case": "akk", "ru": "заботиться", "person": True,
     "q": ["kümmerst du dich?", "kümmert ihr euch?"],
     "obj": [("der Garten", "сад"), ("das Haus", "дом")]},
    {"lemma": "bitten um", "prep": "um", "case": "akk", "ru": "просить", "person": False,
     "q": ["bittest du?", "bittet ihr?"],
     "obj": [("die Hilfe", "помощь"), ("der Rat", "совет")]},
    {"lemma": "sich Sorgen machen um", "prep": "um", "case": "akk", "ru": "беспокоиться (о)", "person": True,
     "q": ["machst du dir Sorgen?", "macht ihr euch Sorgen?"],
     "obj": [("die Zukunft", "будущее"), ("die Gesundheit", "здоровье")]},
    {"lemma": "kämpfen um", "prep": "um", "case": "akk", "ru": "бороться (за)", "person": False,
     "also_ok": [{"prep": "für", "mode": "thing", "note": "«für» — за что борешься, «um» — за что борешься, рискуя это потерять («um den Sieg kämpfen»). Для свободы и победы верны оба."}],
     "q": ["kämpfst du?", "kämpft ihr?"],
     "obj": [("der Sieg", "победа"), ("das Überleben", "выживание")]},
    {"lemma": "es geht um", "prep": "um", "case": "akk", "ru": "речь идёт (о)", "person": False,
     "q": ["geht es?", "geht es hier?"],
     "obj": [("das Geld", "деньги"), ("die Wahrheit", "правда")]},
    {"lemma": "es handelt sich um", "prep": "um", "case": "akk", "ru": "речь (о)", "person": False,
     "q": ["handelt es sich?", "handelt es sich hier?"],
     "obj": [("der Fehler", "ошибка"), ("das Problem", "проблема")]},
    {"lemma": "sich streiten um", "prep": "um", "case": "akk", "ru": "спорить (из-за)", "person": False,
     "q": ["streitet ihr euch?", "streiten sie sich?"],
     "obj": [("das Geld", "деньги"), ("das Erbe", "наследство")]},
    {"lemma": "beneiden um", "prep": "um", "case": "akk", "ru": "завидовать (из-за)", "person": False,
     "q": ["beneidest du ihn?", "beneidet ihr sie?"],
     "obj": [("der Erfolg", "успех"), ("das Talent", "талант")]},
    {"lemma": "trauern um", "prep": "um", "case": "akk", "ru": "скорбеть", "person": True,
     "q": ["trauerst du?", "trauert ihr?"],
     "obj": [("der Verlust", "потеря"), ("die Zeit", "время")]},
    {"lemma": "fürchten um", "prep": "um", "case": "akk", "ru": "опасаться (за)", "person": False,
     "q": ["fürchtest du?", "fürchtet ihr?"],
     "obj": [("die Sicherheit", "безопасность"), ("der Job", "работа")]},
    # ── UNTER + Dativ ──
    {"lemma": "verstehen unter", "prep": "unter", "case": "dat", "ru": "понимать (под)", "person": False,
     "q": ["verstehst du?", "versteht ihr?"],
     "obj": [("der Begriff", "понятие"), ("das Wort", "слово")]},
    # ── VON + Dativ ──
    {"lemma": "träumen von", "prep": "von", "case": "dat", "ru": "мечтать", "person": True,
     "q": ["träumst du?", "träumt ihr?"],
     "obj": [("die Zukunft", "будущее"), ("das Meer", "море")]},
    {"lemma": "erzählen von", "prep": "von", "case": "dat", "ru": "рассказывать (о)", "person": True,
     "q": ["erzählst du?", "erzählt ihr?"],
     "obj": [("die Reise", "поездка"), ("das Erlebnis", "впечатление")]},
    {"lemma": "abhängen von", "prep": "von", "case": "dat", "ru": "зависеть (от)", "person": True,
     "q": ["hängt es ab?", "hängt das ab?"],
     "obj": [("das Wetter", "погода"), ("die Entscheidung", "решение")]},
    {"lemma": "sich verabschieden von", "prep": "von", "case": "dat", "ru": "прощаться (с)", "person": True,
     "person_only": True, "q": ["verabschiedest du dich?", "verabschiedet ihr euch?"],
     "obj": []},
    {"lemma": "handeln von", "prep": "von", "case": "dat", "ru": "быть (о чём)", "person": False,
     "q": ["handelt es?", "handelt das Buch?"],
     "obj": [("die Liebe", "любовь"), ("der Krieg", "война")]},
    {"lemma": "überzeugen von", "prep": "von", "case": "dat", "ru": "убеждать (в)", "person": False,
     "q": ["überzeugst du?", "überzeugt ihr?"],
     "obj": [("die Idee", "идея"), ("der Plan", "план")]},
    {"lemma": "sich ernähren von", "prep": "von", "case": "dat", "ru": "питаться", "person": False,
     "q": ["ernährst du dich?", "ernährt ihr euch?"],
     "obj": [("das Gemüse", "овощи"), ("das Obst", "фрукты")]},
    {"lemma": "ausgehen von", "prep": "von", "case": "dat", "ru": "исходить (из)", "person": False,
     "q": ["gehst du aus?", "geht ihr aus?"],
     "obj": [("die Annahme", "предположение"), ("das Prinzip", "принцип")]},
    {"lemma": "leben von", "prep": "von", "case": "dat", "ru": "жить (на что)", "person": False,
     "q": ["lebst du?", "lebt ihr?"],
     "obj": [("das Gehalt", "зарплата"), ("die Rente", "пенсия")]},
    {"lemma": "halten von", "prep": "von", "case": "dat", "ru": "думать (о, мнение)", "person": True,
     "q": ["hältst du?", "haltet ihr?"],
     "obj": [("die Idee", "идея"), ("der Vorschlag", "предложение")]},
    {"lemma": "Angst haben vor", "prep": "vor", "case": "dat", "ru": "бояться", "person": True,
     "q": ["hast du Angst?", "habt ihr Angst?"],
     "obj": [("die Prüfung", "экзамен"), ("die Dunkelheit", "темнота")]},
    {"lemma": "sich fürchten vor", "prep": "vor", "case": "dat", "ru": "бояться", "person": True,
     "q": ["fürchtest du dich?", "fürchtet ihr euch?"],
     "obj": [("die Zukunft", "будущее"), ("der Hund", "собака")]},
    {"lemma": "warnen vor", "prep": "vor", "case": "dat", "ru": "предупреждать (о)", "person": True,
     "q": ["warnst du?", "warnt ihr?"],
     "obj": [("die Gefahr", "опасность"), ("der Sturm", "шторм")]},
    {"lemma": "sich schützen vor", "prep": "vor", "case": "dat", "ru": "защищаться (от)", "person": True,
     "q": ["schützt du dich?", "schützt ihr euch?"],
     "obj": [("die Sonne", "солнце"), ("der Regen", "дождь")]},
    {"lemma": "fliehen vor", "prep": "vor", "case": "dat", "ru": "бежать (от)", "person": True,
     "q": ["fliehst du?", "flieht ihr?"],
     "obj": [("der Krieg", "война"), ("die Gefahr", "опасность")]},
    {"lemma": "sich verstecken vor", "prep": "vor", "case": "dat", "ru": "прятаться (от)", "person": True,
     "q": ["versteckst du dich?", "versteckt ihr euch?"],
     "obj": [("der Regen", "дождь"), ("die Hitze", "жара")]},
    {"lemma": "Respekt haben vor", "prep": "vor", "case": "dat", "ru": "испытывать уважение (к)", "person": True,
     "q": ["hast du Respekt?", "habt ihr Respekt?"],
     "obj": [("die Aufgabe", "задача"), ("die Leistung", "достижение")]},
    {"lemma": "sich ekeln vor", "prep": "vor", "case": "dat", "ru": "испытывать отвращение", "person": True,
     "q": ["ekelst du dich?", "ekelt ihr euch?"],
     "obj": [("die Spinne", "паук"), ("der Schmutz", "грязь")]},
    {"lemma": "zittern vor", "prep": "vor", "case": "dat", "ru": "дрожать (от)", "person": False,
     "q": ["zitterst du?", "zittert ihr?"],
     "obj": [("die Angst", "страх"), ("die Kälte", "холод")]},
    # ── ZU + Dativ ──
    {"lemma": "gehören zu", "prep": "zu", "case": "dat", "ru": "относиться (к)", "person": False,
     # Про группу и команду человек законно спросит «Zu wem» — уводим вопрос
     # на предметы, тогда верный ответ ровно один.
     "q": ["gehört dieser Schlüssel?", "gehört dieses Zimmer?"],
     "obj": [("das Auto", "машина"), ("die Wohnung", "квартира")]},
    {"lemma": "einladen zu", "prep": "zu", "case": "dat", "ru": "приглашать (на)", "person": False,
     "q": ["lädst du ein?", "ladet ihr ein?"],
     "obj": [("die Party", "вечеринка"), ("das Essen", "ужин")]},
    {"lemma": "gratulieren zu", "prep": "zu", "case": "dat", "ru": "поздравлять (с)", "person": False,
     "q": ["gratulierst du?", "gratuliert ihr?"],
     "obj": [("der Geburtstag", "день рождения"), ("der Erfolg", "успех")]},
    {"lemma": "führen zu", "prep": "zu", "case": "dat", "ru": "вести (к)", "person": False,
     "q": ["führt es?", "führt das?"],
     "obj": [("der Erfolg", "успех"), ("das Problem", "проблема")]},
    {"lemma": "beitragen zu", "prep": "zu", "case": "dat", "ru": "способствовать", "person": False,
     "q": ["trägst du bei?", "tragt ihr bei?"],
     "obj": [("der Erfolg", "успех"), ("die Lösung", "решение")]},
    {"lemma": "sich entschließen zu", "prep": "zu", "case": "dat", "ru": "решиться (на)", "person": False,
     "q": ["entschließt du dich?", "entschließt ihr euch?"],
     "obj": [("der Umzug", "переезд"), ("die Reise", "поездка")]},
    {"lemma": "zwingen zu", "prep": "zu", "case": "dat", "ru": "принуждать (к)", "person": False,
     "q": ["zwingst du ihn?", "zwingt ihr sie?"],
     "obj": [("die Entscheidung", "решение"), ("die Arbeit", "работа")]},
    {"lemma": "ermutigen zu", "prep": "zu", "case": "dat", "ru": "поощрять (к)", "person": False,
     "q": ["ermutigst du ihn?", "ermutigt ihr sie?"],
     "obj": [("der Schritt", "шаг"), ("die Bewerbung", "заявка")]},
    {"lemma": "bereit sein zu", "prep": "zu", "case": "dat", "ru": "быть готовым (к)", "person": False,
     "q": ["bist du bereit?", "seid ihr bereit?"],
     "obj": [("der Kompromiss", "компромисс"), ("der Verzicht", "отказ"), ("der Verzicht", "отказ")]},
    {"lemma": "Stellung nehmen zu", "prep": "zu", "case": "dat", "ru": "высказаться (по)", "person": False,
     "q": ["nimmst du Stellung?", "nehmt ihr Stellung?"],
     "obj": [("das Thema", "тема"), ("die Frage", "вопрос")]},
]


def check_bank() -> list[str]:
    """Проверка ДАННЫХ банка. Возвращает список претензий (пусто — всё чисто).

    Почему это здесь, а не только в тестах: банк заполняют руками, и одна опечатка
    («der Feind» среди вещей) превращается в задание, которое противоречит своему же
    пояснению. Задания собираются заранее и ложатся в базу на весь день — ошибку
    увидят все, и она там останется. Значит ловим на входе, а не по жалобе.
    """
    problems: list[str] = []
    for entry in _BANK:
        lemma = str(entry.get("lemma") or "?")
        prep = str(entry.get("prep") or "")
        case = str(entry.get("case") or "")
        if not prep or not prep.isalpha():
            problems.append(f"{lemma}: нет предлога")
        if case not in ("akk", "dat"):
            problems.append(f"{lemma}: падеж «{case}» — бывает только akk или dat")
        if not (entry.get("q") or []):
            problems.append(f"{lemma}: нет ни одной фразы-вопроса")
        for frame in entry.get("q") or []:
            frame = str(frame)
            if not frame.strip().endswith("?"):
                problems.append(f"{lemma}: фраза «{frame}» не кончается вопросительным знаком")
            if frame[:1].isupper():
                problems.append(f"{lemma}: фраза «{frame}» с большой буквы — она идёт ПОСЛЕ вопросительного слова")
        objs = entry.get("obj") or []
        if entry.get("person_only"):
            if objs:
                problems.append(f"{lemma}: помечен «только о людях», но у него есть список вещей")
            continue
        if not objs:
            problems.append(f"{lemma}: нет ни одной вещи для вопроса")
        for pair in objs:
            noun = str(pair[0])
            if _is_person_noun(noun):
                problems.append(f"{lemma}: «{noun}» — человек, о нём спрашивают «{_person_form(prep, case or 'akk')}»")
            elif noun not in _VETTED_THINGS:
                problems.append(f"{lemma}: «{noun}» никем не проверен — вещь это или человек?")
            if not str(pair[1] or "").strip():
                problems.append(f"{lemma}: у «{noun}» нет русской подсказки")
    return problems


def missing_translations() -> list[str]:
    """Фразы без русского перевода. Пусто — переведено всё, что банк может выдать.

    Отдельно от check_bank НАМЕРЕННО: пропущенный перевод — это дырка в объяснении,
    но задание от него не становится неверным. Выкидывать из-за него живой глагол
    (как делает _healthy_bank с ошибками данных) значит молча урезать игру. Поэтому
    претензии копим здесь, а ловят их тесты — до того, как это увидят люди.
    """
    problems: list[str] = []
    for entry in _BANK:
        lemma = str(entry.get("lemma") or "?")
        targets = ["person"] if entry.get("person_only") else (
            ["thing", "person"] if entry.get("person") else ["thing"]
        )
        for frame in entry.get("q") or []:
            frame = str(frame)
            if (lemma, frame) not in QUESTION_RU:
                problems.append(f"{lemma}: у фразы «{frame}» нет перевода")
                continue
            for target in targets:
                if not question_ru(lemma, frame, target):
                    mode = "о вещи" if target == "thing" else "о человеке"
                    problems.append(f"{lemma}: у фразы «{frame}» нет перевода вопроса {mode}")
    extra = {key[0] for key in QUESTION_RU} - {str(e.get("lemma")) for e in _BANK}
    for lemma in sorted(extra):
        problems.append(f"{lemma}: перевод есть, а управления в банке нет — опечатка в ключе?")
    return problems


def _healthy_bank() -> list[dict]:
    """Банк без записей, к которым есть претензии. Бот не падает, но и мусор не выдаёт."""
    problems = check_bank()
    if not problems:
        return list(_BANK)
    broken = {p.split(":", 1)[0] for p in problems}
    logging.error("wofrage: банк заданий с ошибками, эти глаголы не выдаём: %s", sorted(broken))
    for p in problems:
        logging.error("wofrage: %s", p)
    return [e for e in _BANK if str(e.get("lemma")) not in broken]


def _decline_clue(prep: str, case: str, obj_phrase: str) -> str:
    """Build the natural answer line for a THING object: 'auf' + 'der Bus'(Akk) → 'Auf den Bus'."""
    parts = obj_phrase.split(" ", 1)
    if len(parts) != 2 or parts[0] not in _GENDER:
        combo = f"{prep} {obj_phrase}"
        return combo[:1].upper() + combo[1:]
    art, noun = parts
    decl = _ART_AKK[art] if case == "akk" else _ART_DAT[art]
    combo = f"{prep} {decl}"
    combo = _CONTRACT.get(combo, combo)
    combo = f"{combo} {noun}"
    return combo[:1].upper() + combo[1:]


def _lemma_head(lemma: str) -> str:
    """«sich freuen auf» → «sich freuen». Один глагол живёт с разными предлогами."""
    return str(lemma or "").strip().rsplit(" ", 1)[0].strip()


def _build_rivals() -> tuple[dict, dict]:
    """Второй верный ответ — главная ловушка этого тренажёра, и не для ученика, а для нас.

    «___ leidest du?» подходит и «Woran» (leiden an), и «Worunter» (leiden unter).
    Если оба лежат в вариантах, человек отвечает верно, а игра засчитывает ошибку.
    Замер до правки: 2.1% выданных заданий имели два верных ответа.

    Собираем два указателя: по КОРНЮ глагола (kämpfen für/gegen/um) и по ТЕКСТУ
    рамки (на случай, когда одну фразу делят разные глаголы). Формы соперников
    потом просто не попадают в варианты ответа.
    """
    by_head: dict[str, set] = {}
    by_frame: dict[str, set] = {}
    for entry in _BANK:
        gov = (entry.get("prep"), entry.get("case"))
        by_head.setdefault(_lemma_head(entry.get("lemma", "")), set()).add(gov)
        for frame in entry.get("q") or []:
            by_frame.setdefault(str(frame).strip().lower(), set()).add(gov)
    return by_head, by_frame


def _rival_forms(entry: dict, frame: str) -> set[str]:
    """Все вопросительные формы, которые для ЭТОЙ фразы тоже верны."""
    own = (entry.get("prep"), entry.get("case"))
    govs = set(_RIVALS_BY_HEAD.get(_lemma_head(entry.get("lemma", "")), set()))
    govs |= set(_RIVALS_BY_FRAME.get(str(frame).strip().lower(), set()))
    out: set[str] = set()
    for prep, case in govs:
        if (prep, case) == own or not prep:
            continue
        out.add(_wo_form(prep).capitalize())
        out.add(_person_form(prep, case or "akk"))
    return out


def _options(correct: str, target: str, prep: str, case: str,
             banned: set[str] | None = None, extra: list[str] | None = None) -> list[str]:
    """4 варианта: верный + противоположная форма (вещь↔человек) + 2 похожих.

    `banned` — формы второго верного ответа: их в вариантах быть не должно,
    иначе у задания два правильных ответа.
    """
    banned = set(banned or ())
    opts = {correct}
    # Объявленные «тоже верные» показываем осознанно: человек выбирает любой,
    # а в разборе читает, чем они отличаются.
    opts.update(a for a in (extra or []) if a)
    trap = _person_form(prep, case) if target == "thing" else _wo_form(prep).capitalize()
    opts.add(trap)
    pool = [d for d in (_WO_DISTRACTORS + _PERSON_DISTRACTORS) if d not in banned]
    random.shuffle(pool)
    for d in pool:
        if len(opts) >= 4:
            break
        opts.add(d)
    out = list(opts)
    random.shuffle(out)
    return out


# Указатели соперников строим один раз при загрузке — банк не меняется на ходу.
_RIVALS_BY_HEAD, _RIVALS_BY_FRAME = _build_rivals()


def _erklaerung(entry: dict, target: str, woword: str, personword: str) -> str:
    lemma, prep, case_ru = entry["lemma"], entry["prep"], _CASE_RU[entry["case"]]
    if target == "thing":
        return (f"«{lemma}» управляет предлогом «{prep}» ({case_ru}). "
                f"Вопрос о вещи → {woword}. О человеке было бы «{personword}».")
    return (f"«{lemma}» с предлогом «{prep}», но вопрос о человеке → {personword}. "
            f"Wo-наречие ({woword}) здесь нельзя — оно только для вещей.")


def _tip(entry: dict, target: str, woword: str) -> str:
    prep, case = entry["prep"], entry["case"]
    if target == "thing":
        if prep[0] in _VOWELS:
            return f"Предлог «{prep}» на гласную → вставляем -r-: wo+r+{prep} = {woword}."
        return f"Предлог «{prep}» на согласную → просто wo+{prep} = {woword}."
    wq = "wen (Akkusativ)" if case == "akk" else "wem (Dativ)"
    return f"Про людей Wo-наречия не образуются: только «{prep}» + {wq}."


def item_key(item: dict) -> str:
    """Устойчивое имя задания: одно и то же задание из разных наборов — один ключ.

    Нужно, чтобы копить статистику ответов по КОНКРЕТНОМУ заданию: без этого
    сломанное задание не видно, пока на него не пожалуются вручную.
    """
    raw = "|".join(str(item.get(k) or "") for k in ("lemma", "prep", "case", "target", "s", "obj"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _alternatives(entry: dict, target: str) -> tuple[list[str], str]:
    """Ответы, которые тоже верны, и объяснение разницы.

    Прятать второй верный ответ —полумера: человек всё равно знает, что «Worunter
    leidest du?» — нормальный немецкий. Честнее показать оба, засчитать любой и
    объяснить оттенок. Взаимозаменяемость объявляется в банке РУКАМИ (поле also_ok),
    потому что она зависит от смысла: «unter der Grippe» — норма, а «an der Hitze» — нет.
    """
    forms: list[str] = []
    notes: list[str] = []
    for alt in entry.get("also_ok") or []:
        mode = str(alt.get("mode") or "both")
        if mode not in ("both", target):
            continue
        prep = str(alt.get("prep") or "")
        if not prep:
            continue
        case = str(alt.get("case") or entry.get("case") or "akk")
        forms.append(_wo_form(prep).capitalize() if target == "thing" else _person_form(prep, case))
        note = str(alt.get("note") or "").strip()
        if note and note not in notes:
            notes.append(note)
    return forms, " ".join(notes)


def _build_one(entry: dict) -> dict:
    prep, case = entry["prep"], entry["case"]
    woword = _wo_form(prep).capitalize()
    personword = _person_form(prep, case)
    can_person = bool(entry.get("person"))
    person_only = bool(entry.get("person_only"))
    # Choose thing vs person mode. Person mode is the valuable contrast, so give it
    # a healthy share when the verb allows it.
    if person_only:
        target = "person"
    elif can_person:
        target = random.choice(["thing", "thing", "person"])
    else:
        target = "thing"

    frame = random.choice(entry["q"])
    if target == "thing":
        # СТРАЖ: вопрос о вещи не должен достаться человеку. Список объектов заполняют
        # руками, и туда уже попадали люди — тогда карточка спрашивала «Worauf … ребёнок»
        # и считала ошибкой верный ответ «Auf wen».
        things = [pair for pair in entry["obj"] if not _is_person_noun(pair[0])]
        if not things:
            logging.error(
                "wofrage: в списке вещей для «%s» одни люди (%s) — выдаю вопрос о человеке",
                entry.get("lemma"), [o for o, _ in entry["obj"]],
            )
            target = "person"

    if target == "person":
        correct = personword
        name, _ = random.choice(_PERSON_NAMES)
        # Подсказка — голое ИМЯ: сразу видно, что речь о человеке, и при этом
        # немецкий предлог (он же текст верной кнопки) не подсказан.
        clue, obj_display, obj_ru = f"{name}.", name, ""
    else:
        correct = woword
        obj_display, obj_ru = random.choice(things)
        # Подсказка — русское слово: видно, что речь о вещи, но управление
        # глаголом человек должен вспомнить сам.
        clue = (obj_ru[:1].upper() + obj_ru[1:] + ".") if obj_ru else (_decline_clue(prep, case, obj_display) + ".")

    # Второй верный ответ: объявленный в банке — показываем и засчитываем,
    # необъявленный — прячем из вариантов (иначе накажем за верный ответ).
    also_ok, unterschied = _alternatives(entry, target)
    banned = _rival_forms(entry, frame) - set(also_ok)

    item = {
        "s": f"___ {frame}",
        "clue": f"— {clue}",
        "a": correct,
        "also_ok": also_ok,
        "unterschied": unterschied,
        "opts": _options(correct, target, prep, case, banned, extra=also_ok),
        "target": target,
        "prep": prep,
        "case": case,
        "lemma": entry["lemma"],
        "verb_ru": entry["ru"],
        "obj": obj_display,
        "obj_ru": obj_ru,
        "erklaerung": _erklaerung(entry, target, woword, personword),
        "tip": _tip(entry, target, woword),
        # Что фраза значит по-русски. Без этого разбор — ребус: видно верную форму
        # и правило, но не видно, о чём вообще был вопрос.
        "frage_ru": question_ru(entry["lemma"], frame, target),
    }
    item["key"] = item_key(item)
    return item


def frage_ru_for_item(item: dict) -> str:
    """Перевод вопроса для УЖЕ собранного задания.

    Наборы живут в базе (дневной сет — сутки, ошибки в «работе над ошибками» — недели),
    и собранные до появления переводов лежат там без них. Ждать, пока они истекут,
    незачем: фраза, управление и режим (вещь/человек) в задании есть, а перевод —
    те же данные, что и при сборке. Поэтому достаём его по фразе.
    """
    ready = str(item.get("frage_ru") or "").strip()
    if ready:
        return ready
    frame = str(item.get("s") or "").replace("___", "").strip()
    target = str(item.get("target") or "thing")
    return question_ru(str(item.get("lemma") or ""), frame, target)


def accepted_answers(item: dict) -> set[str]:
    """Все ответы, которые засчитываются. Одна точка правды для сервера и клиента."""
    out = {str(item.get("a") or "").strip()}
    out.update(str(a).strip() for a in (item.get("also_ok") or []) if str(a).strip())
    return {a for a in out if a}


def check_item(item: dict) -> list[str]:
    """Проверка ГОТОВОГО задания — последний рубеж перед тем, как его увидит человек.

    Проверяем ровно то, из-за чего задание становится нечестным:
      • верный ответ есть среди вариантов, вариантов четыре и они разные;
      • среди вариантов нет ВТОРОГО верного (Woran/Worunter для «leidest du?»);
      • ответ выведен из предлога глагола, а не из чего-то ещё;
      • о человеке спрашивают «Bei wem», о вещи — «Wobei», и подсказка не спорит
        с этим;
      • во фразе ровно один пропуск, и она осталась вопросом.
    """
    problems: list[str] = []
    answer = str(item.get("a") or "")
    opts = [str(o) for o in (item.get("opts") or [])]
    prep, case = str(item.get("prep") or ""), str(item.get("case") or "")
    target = str(item.get("target") or "")
    if answer not in opts:
        problems.append("верного ответа нет среди вариантов")
    if len(opts) != 4 or len(set(opts)) != 4:
        problems.append(f"вариантов должно быть 4 разных, а тут {opts}")
    expected = _wo_form(prep).capitalize() if target == "thing" else _person_form(prep, case)
    if answer != expected:
        problems.append(f"ответ «{answer}» не выводится из управления: ждали «{expected}»")
    opposite = _person_form(prep, case) if target == "thing" else _wo_form(prep).capitalize()
    if opposite not in opts:
        problems.append("нет главной ловушки — противоположной формы (вещь↔человек)")
    rivals = _rival_forms(
        {"lemma": item.get("lemma"), "prep": prep, "case": case},
        str(item.get("s") or "").replace("___", "").strip(),
    )
    declared = accepted_answers(item)
    if len(declared) > 1 and not str(item.get("unterschied") or "").strip():
        problems.append("несколько верных ответов без объяснения разницы")
    also_right = sorted((rivals & set(opts)) - declared)
    if also_right:
        problems.append(f"в вариантах лежит второй верный ответ: {also_right}")
    if target == "thing" and _is_person_noun(str(item.get("obj") or "")):
        problems.append(f"о человеке «{item.get('obj')}» спрашивают как о вещи")
    sentence = str(item.get("s") or "")
    if sentence.count("___") != 1 or not sentence.rstrip().endswith("?"):
        problems.append(f"фраза сломана: «{sentence}»")
    if not str(item.get("clue") or "").strip("— .").strip():
        problems.append("пустая подсказка-ответ")
    if answer.lower() in str(item.get("clue") or "").lower():
        problems.append("подсказка выдаёт ответ")
    return problems


# ┌─ ПРОВЕРЕНО 24.08.2026. НЕ ПОДНИМАТЬ ЭТО КАК НОВУЮ НАХОДКУ. ────────────────┐
# │ 29.07.2026 поставили стражи от «человека в списке вещей» — но ТОЛЬКО на    │
# │ сборке. Собранное задание живёт своей копией: очередь «работа над ошибками»│
# │ хранит фотографию карточки и месяцами показывает её как есть. Поэтому      │
# │ 24.08 владельцу выпала карточка от 21.07 «Worauf nimmst du Rücksicht? —    │
# │ Ребёнок», где вопрос о ВЕЩИ задан о человеке.                              │
# │                                                                            │
# │ Замер по живой базе 24.08.2026 (scripts/wofrage_review_audit.py):          │
# │   очередь ошибок: до 29.07 — 80 карточек, кривых 2; после — 29, кривых 0.  │
# │   дневные наборы: до 29.07 — 610 заданий, кривых 2; после — 800, кривых 0. │
# │ Вывод: стражи сборки РАБОТАЮТ, дыра была только в показе старых копий.     │
# │                                                                            │
# │ ЛОЖНЫЙ СЛЕД, не повторять: 27 карточек «вещь в вопросе о человеке» — это   │
# │ НЕ дефект. В режиме человека подсказка это голое имя (David, Julia), и в   │
# │ списке существительных его быть не должно.                                 │
# └────────────────────────────────────────────────────────────────────────────┘
def stored_item_problems(payload: dict) -> list[str]:
    """Перепроверка УЖЕ СОБРАННОГО задания — теми же источниками, что и на сборке.

    Зачем отдельно от check_item: у сохранённой карточки нет ни предлога, ни падежа
    (их не кладут в payload) — есть лемма. Поэтому управление достаём ИЗ БАНКА по
    лемме и выводим верный ответ заново. Проверяем ровно то, из-за чего карточка
    становится нечестной:

      • одушевлённость — о человеке нельзя спрашивать «Worauf» (класс, из-за
        которого это написано; от банка не зависит вовсе);
      • управление — ответ обязан выводиться из предлога глагола;
      • вариантов четыре разных и верный среди них.

    Если глагола в банке НЕТ (лемму переименовали, запись убрали) — управление не
    судим: судить не по чему, а догадываться запрещено. Проверка одушевлённости
    при этом всё равно работает.

    Стоит одно обращение к словарю в памяти: ~1 мкс на карточку (замер 24.08.2026
    на 1519 живых карточках), страница из 15 карточек — 0,016 мс. Ни базы, ни сети,
    ни модели, поэтому её не жалко звать перед каждой выдачей.
    """
    p = payload if isinstance(payload, dict) else {}
    problems: list[str] = []
    target = str(p.get("target") or "")
    obj = str(p.get("obj") or "")
    # В очереди ошибок верный ответ лежит под ключом "correct", в наборе — под "a".
    answer = str(p.get("correct") or p.get("a") or "")
    opts = [str(o) for o in (p.get("opts") or []) if str(o).strip()]
    if target == "thing" and _is_person_noun(obj):
        problems.append(f"о человеке «{obj}» спрашивают как о вещи")
    entry = _bank_by_lemma().get(str(p.get("lemma") or "").strip().lower())
    if entry and answer:
        expected = (_wo_form(entry["prep"]).capitalize() if target == "thing"
                    else _person_form(entry["prep"], entry["case"]))
        if answer != expected:
            problems.append(f"ответ «{answer}» не выводится из управления: ждали «{expected}»")
    if answer and opts and answer not in opts:
        problems.append("верного ответа нет среди вариантов")
    if opts and (len(opts) != 4 or len(set(opts)) != 4):
        problems.append(f"вариантов должно быть 4 разных, а тут {opts}")
    return problems


def repair_stored_item(payload: dict) -> tuple[dict | None, str]:
    """Правильная версия сохранённой карточки — или None, если починить нечем.

    Решение владельца 24.08.2026: кривую карточку в очереди ошибок НЕ удалять, а
    приводить к правильному виду. Человек уже ошибся на этой конструкции — она и
    должна ему вернуться, только верной.

    Чиним двумя способами, оба — по банку управления, ничего не придумывая:

      • глагол допускает вопрос о ЧЕЛОВЕКЕ («Rücksicht nehmen auf j-n») —
        значит карточка просто стояла не в том режиме: объект (das Kind) остаётся,
        верным ответом становится «Auf wen», пояснение и правило пересобираются;
      • глагол о человеке не спрашивают («bereit sein zu jemandem» — не немецкий),
        значит неверен ОБЪЕКТ: берём проверенный из банка того же глагола
        (das Opfer → der Kompromiss). Ответ не меняется — тренируется то же самое.

    Починенная карточка ОБЯЗАНА пройти полную check_item — ту же, что стоит на
    сборке. Не прошла — возвращаем None и причину: пусть лучше карточка уйдёт,
    чем человек снова увидит неверный немецкий.
    """
    p = dict(payload if isinstance(payload, dict) else {})
    entry = _bank_by_lemma().get(str(p.get("lemma") or "").strip().lower())
    if not entry:
        return None, "глагола нет в банке управления — чинить не по чему"
    prep, case = entry["prep"], entry["case"]
    woword, personword = _wo_form(prep).capitalize(), _person_form(prep, case)
    frame = str(p.get("s") or "").replace("___", "").strip()

    if bool(entry.get("person")) or bool(entry.get("person_only")):
        target = "person"
        p["target"], p["correct"] = "person", personword
        changed = f"верный ответ «{payload.get('correct')}» → «{personword}»: спрашиваем о человеке"
    else:
        things = [pair for pair in entry["obj"] if not _is_person_noun(pair[0])]
        if not things:
            return None, "у этого глагола в банке не осталось проверенных вещей"
        obj_de, obj_ru = things[0]
        target = "thing"
        p["target"], p["correct"] = "thing", woword
        p["obj"], p["obj_ru"] = obj_de, obj_ru
        p["clue"] = "— " + obj_ru[:1].upper() + obj_ru[1:] + "."
        old_clue = str(payload.get("clue") or "").strip("— .").strip()
        changed = f"подсказка «{old_clue}» → «{obj_ru}»: о человеке этот глагол не спрашивают"

    p["erklaerung"] = _erklaerung(entry, target, woword, personword)
    p["tip"] = _tip(entry, target, woword)
    p["frage_ru"] = question_ru(entry["lemma"], frame, target)
    p["hint_ru"] = p.get("verb_ru") or p.get("clue")

    # check_item'у нужны предлог и падеж, которых в сохранённой карточке нет:
    # подставляем из банка — оттуда они и взялись при сборке.
    check = dict(p)
    check["a"], check["prep"], check["case"] = p["correct"], prep, case
    problems = check_item(check)
    if problems:
        return None, "починенная карточка не прошла проверку: " + "; ".join(problems)
    return p, changed



_BANK_BY_LEMMA: dict | None = None


def _bank_by_lemma() -> dict:
    """Указатель «лемма → запись банка». Банк — константа модуля, строим один раз."""
    global _BANK_BY_LEMMA
    if _BANK_BY_LEMMA is None:
        _BANK_BY_LEMMA = {str(e.get("lemma") or "").strip().lower(): e for e in _BANK}
    return _BANK_BY_LEMMA



def build_wofrage_items(n: int = 10) -> list[dict]:
    """`n` заданий: проверенных поштучно и без повторов внутри набора.

    Набор ложится в базу на весь день и уходит сразу всем, поэтому «почти верное»
    задание тут недопустимо: то, что не прошло проверку, не выдаём вовсе.
    Внутри одного набора один глагол встречается один раз — иначе десятка выглядит
    как сбой (замер до правки: 16% наборов повторяли глагол, 5% — целую фразу).
    """
    bank = _healthy_bank()
    if not bank:
        return []
    want = max(0, int(n))
    out: list[dict] = []
    used_lemmas: set[str] = set()
    used_sentences: set[str] = set()
    rejected = 0
    attempts = 0
    used_preps: Counter = Counter()
    # Не больше двух заданий на один предлог: до этой правки в 46% наборов какой-то
    # предлог встречался 3+ раза из 10, и десятка выглядела как одно и то же задание.
    prep_cap = max(2, -(-want // 5))
    while len(out) < want and attempts < want * 40:
        attempts += 1
        pool = [e for e in bank
                if str(e.get("lemma")) not in used_lemmas
                and used_preps[str(e.get("prep"))] < prep_cap]
        pool = pool or [e for e in bank if str(e.get("lemma")) not in used_lemmas] or bank
        entry = random.choice(pool)
        item = _build_one(entry)
        problems = check_item(item)
        if problems:
            rejected += 1
            logging.error("wofrage: задание не прошло проверку и не выдано (%s): %s",
                          item.get("lemma"), "; ".join(problems))
            continue
        sentence = str(item.get("s") or "").lower()
        if sentence in used_sentences:
            continue
        used_sentences.add(sentence)
        used_lemmas.add(str(item.get("lemma")))
        used_preps[str(item.get("prep"))] += 1
        out.append(item)
    if rejected:
        logging.error("wofrage: отбраковано заданий при сборке набора: %d", rejected)
    return out


if __name__ == "__main__":  # smoke test
    print(f"bank size: {len(_BANK)} governed verbs/adjectives")
    items = build_wofrage_items(12)
    for it in items:
        print(f'{it["s"]}\n  {it["clue"]}')
        print(f'  correct: {it["a"]}   ({it["target"]})   opts: {it["opts"]}')
        print(f'  {it["tip"]}\n')
    # invariant checks across a big sample
    bad = 0
    sample = [_build_one(e) for e in _BANK for _ in range(6)]
    for it in sample:
        if it["a"] not in it["opts"]:
            bad += 1; print("!! correct not in opts:", it["s"], it["a"], it["opts"])
        if len(set(it["opts"])) != 4:
            bad += 1; print("!! opts not 4 unique:", it["opts"])
    print(f"checked {len(sample)} generated items, {bad} problems")
