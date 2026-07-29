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

import random

# ── Rules ───────────────────────────────────────────────────────────────────
_VOWELS = "aeiouäöü"


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
    # ── AN + Akkusativ ──
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
    {"lemma": "sich wenden an", "prep": "an", "case": "akk", "ru": "обращаться", "person": True,
     "q": ["wendest du dich?", "wendet ihr euch?"],
     "obj": [("die Behörde", "ведомство"), ("die Botschaft", "посольство")]},
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
    {"lemma": "sich beteiligen an", "prep": "an", "case": "dat", "ru": "участвовать (в)", "person": False,
     "q": ["beteiligst du dich?", "beteiligt ihr euch?"],
     "obj": [("das Projekt", "проект"), ("die Diskussion", "дискуссия")]},
    {"lemma": "leiden an", "prep": "an", "case": "dat", "ru": "страдать (болезнью)", "person": False,
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
    {"lemma": "reagieren auf", "prep": "auf", "case": "akk", "ru": "реагировать", "person": True,
     "q": ["reagierst du?", "reagiert ihr?"],
     "obj": [("die Kritik", "критика"), ("die Nachricht", "новость")]},
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
    {"lemma": "sich beziehen auf", "prep": "auf", "case": "akk", "ru": "ссылаться (на)", "person": True,
     "q": ["beziehst du dich?", "bezieht ihr euch?"],
     "obj": [("der Artikel", "статья"), ("das Gesetz", "закон")]},
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
     "obj": [("die Firma", "фирма"), ("die Behörde", "ведомство")]},
    {"lemma": "sich entschuldigen bei", "prep": "bei", "case": "dat", "ru": "извиняться (перед)", "person": True,
     "person_only": True, "q": ["entschuldigst du dich?", "entschuldigt ihr euch?"],
     "obj": [("der Lehrer", "учитель"), ("die Nachbarin", "соседка")]},
    {"lemma": "bleiben bei", "prep": "bei", "case": "dat", "ru": "оставаться (при)", "person": False,
     "q": ["bleibst du?", "bleibt ihr?"],
     "obj": [("die Meinung", "мнение"), ("der Plan", "план")]},
    {"lemma": "sich bewerben bei", "prep": "bei", "case": "dat", "ru": "подавать заявку (куда)", "person": False,
     "q": ["bewirbst du dich?", "bewerbt ihr euch?"],
     "obj": [("die Firma", "фирма"), ("die Bank", "банк")]},
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
     "q": ["kämpfst du?", "kämpft ihr?"],
     "obj": [("die Freiheit", "свобода"), ("das Recht", "право")]},
    {"lemma": "sich eignen für", "prep": "für", "case": "akk", "ru": "подходить (для)", "person": False,
     "q": ["eignest du dich?", "eignet ihr euch?"],
     "obj": [("der Job", "работа"), ("die Aufgabe", "задача")]},
    {"lemma": "sich schämen für", "prep": "für", "case": "akk", "ru": "стыдиться", "person": True,
     "q": ["schämst du dich?", "schämt ihr euch?"],
     "obj": [("der Fehler", "ошибка"), ("das Verhalten", "поведение")]},
    {"lemma": "sich einsetzen für", "prep": "für", "case": "akk", "ru": "заступаться", "person": True,
     "q": ["setzt du dich ein?", "setzt ihr euch ein?"],
     "obj": [("das Recht", "право"), ("die Gerechtigkeit", "справедливость")]},
    {"lemma": "verantwortlich sein für", "prep": "für", "case": "akk", "ru": "отвечать (за)", "person": True,
     "q": ["bist du verantwortlich?", "seid ihr verantwortlich?"],
     "obj": [("das Projekt", "проект"), ("die Sicherheit", "безопасность")]},
    {"lemma": "ausgeben für", "prep": "für", "case": "akk", "ru": "тратить (на)", "person": False,
     "q": ["gibst du aus?", "gebt ihr aus?"],
     "obj": [("das Essen", "еда"), ("die Kleidung", "одежда")]},
    # ── GEGEN + Akkusativ ──
    {"lemma": "kämpfen gegen", "prep": "gegen", "case": "akk", "ru": "бороться (против)", "person": True,
     "q": ["kämpfst du?", "kämpft ihr?"],
     "obj": [("die Ungerechtigkeit", "несправедливость"), ("der Feind", "враг")]},
    {"lemma": "protestieren gegen", "prep": "gegen", "case": "akk", "ru": "протестовать", "person": True,
     "q": ["protestierst du?", "protestiert ihr?"],
     "obj": [("das Gesetz", "закон"), ("der Krieg", "война")]},
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
     "obj": [("der Nachbar", "сосед"), ("die Kollegin", "коллега")]},
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
     "obj": [("die Firma", "фирма"), ("die Behörde", "ведомство")]},
    {"lemma": "reden mit", "prep": "mit", "case": "dat", "ru": "разговаривать (с)", "person": True,
     "person_only": True, "q": ["redest du?", "redet ihr?"],
     "obj": [("der Nachbar", "сосед"), ("die Lehrerin", "учительница")]},
    {"lemma": "telefonieren mit", "prep": "mit", "case": "dat", "ru": "звонить (кому)", "person": True,
     "person_only": True, "q": ["telefonierst du?", "telefoniert ihr?"],
     "obj": [("die Mutter", "мама"), ("der Freund", "друг")]},
    {"lemma": "sich treffen mit", "prep": "mit", "case": "dat", "ru": "встречаться (с)", "person": True,
     "person_only": True, "q": ["triffst du dich?", "trefft ihr euch?"],
     "obj": [("der Kollege", "коллега"), ("die Freundin", "подруга")]},
    {"lemma": "sich streiten mit", "prep": "mit", "case": "dat", "ru": "ссориться (с)", "person": True,
     "person_only": True, "q": ["streitest du dich?", "streitet ihr euch?"],
     "obj": [("der Bruder", "брат"), ("die Schwester", "сестра")]},
    {"lemma": "sich unterhalten mit", "prep": "mit", "case": "dat", "ru": "беседовать (с)", "person": True,
     "person_only": True, "q": ["unterhältst du dich?", "unterhaltet ihr euch?"],
     "obj": [("der Gast", "гость"), ("die Kollegin", "коллега")]},
    {"lemma": "sich verstehen mit", "prep": "mit", "case": "dat", "ru": "ладить (с)", "person": True,
     "person_only": True, "q": ["verstehst du dich?", "versteht ihr euch?"],
     "obj": [("der Nachbar", "сосед"), ("die Chefin", "начальница")]},
    {"lemma": "verheiratet sein mit", "prep": "mit", "case": "dat", "ru": "быть в браке (с)", "person": True,
     "person_only": True, "q": ["bist du verheiratet?", "seid ihr verheiratet?"],
     "obj": [("der Arzt", "врач"), ("die Lehrerin", "учительница")]},
    {"lemma": "rechnen mit", "prep": "mit", "case": "dat", "ru": "рассчитывать (на)", "person": True,
     "q": ["rechnest du?", "rechnet ihr?"],
     "obj": [("der Regen", "дождь"), ("die Verspätung", "опоздание")]},
    {"lemma": "sich beschäftigen mit", "prep": "mit", "case": "dat", "ru": "заниматься", "person": True,
     "q": ["beschäftigst du dich?", "beschäftigt ihr euch?"],
     "obj": [("die Musik", "музыка"), ("das Thema", "тема")]},
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
    {"lemma": "riechen nach", "prep": "nach", "case": "dat", "ru": "пахнуть", "person": False,
     "q": ["riecht es?", "riecht das?"],
     "obj": [("der Kaffee", "кофе"), ("das Parfüm", "духи")]},
    {"lemma": "schmecken nach", "prep": "nach", "case": "dat", "ru": "иметь вкус", "person": False,
     "q": ["schmeckt es?", "schmeckt das?"],
     "obj": [("die Zitrone", "лимон"), ("der Knoblauch", "чеснок")]},
    {"lemma": "streben nach", "prep": "nach", "case": "dat", "ru": "стремиться", "person": False,
     "q": ["strebst du?", "strebt ihr?"],
     "obj": [("der Erfolg", "успех"), ("die Macht", "власть")]},
    {"lemma": "sich sehnen nach", "prep": "nach", "case": "dat", "ru": "тосковать", "person": True,
     "q": ["sehnst du dich?", "sehnt ihr euch?"],
     "obj": [("die Heimat", "родина"), ("die Ruhe", "покой")]},
    {"lemma": "greifen nach", "prep": "nach", "case": "dat", "ru": "хватать", "person": False,
     "q": ["greifst du?", "greift ihr?"],
     "obj": [("das Glas", "стакан"), ("die Hand", "рука")]},
    {"lemma": "klingen nach", "prep": "nach", "case": "dat", "ru": "звучать (как)", "person": False,
     "q": ["klingt es?", "klingt das?"],
     "obj": [("der Ärger", "проблемы"), ("die Ausrede", "отговорка")]},
    # ── ÜBER + Akkusativ ──
    {"lemma": "sprechen über", "prep": "über", "case": "akk", "ru": "говорить (о)", "person": True,
     "q": ["sprichst du?", "sprecht ihr?"],
     "obj": [("die Politik", "политика"), ("der Film", "фильм")]},
    {"lemma": "reden über", "prep": "über", "case": "akk", "ru": "говорить (о)", "person": True,
     "q": ["redest du?", "redet ihr?"],
     "obj": [("das Problem", "проблема"), ("die Arbeit", "работа")]},
    {"lemma": "sich freuen über", "prep": "über", "case": "akk", "ru": "радоваться (случившемуся)", "person": True,
     "q": ["freust du dich?", "freut ihr euch so sehr?"],
     "obj": [("das Geschenk", "подарок"), ("die Nachricht", "новость")]},
    {"lemma": "sich ärgern über", "prep": "über", "case": "akk", "ru": "злиться", "person": True,
     "q": ["ärgerst du dich?", "ärgert ihr euch?"],
     "obj": [("der Fehler", "ошибка"), ("das Wetter", "погода")]},
    {"lemma": "sich beschweren über", "prep": "über", "case": "akk", "ru": "жаловаться", "person": True,
     "q": ["beschwerst du dich?", "beschwert ihr euch?"],
     "obj": [("der Lärm", "шум"), ("das Essen", "еда")]},
    {"lemma": "sich aufregen über", "prep": "über", "case": "akk", "ru": "нервничать (из-за)", "person": True,
     "q": ["regst du dich auf?", "regt ihr euch auf?"],
     "obj": [("die Nachricht", "новость"), ("der Stau", "пробка")]},
    {"lemma": "nachdenken über", "prep": "über", "case": "akk", "ru": "размышлять", "person": False,
     "q": ["denkst du nach?", "denkt ihr nach?"],
     "obj": [("die Frage", "вопрос"), ("das Problem", "проблема")]},
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
    {"lemma": "sich bewerben um", "prep": "um", "case": "akk", "ru": "претендовать (на)", "person": False,
     "q": ["bewirbst du dich?", "bewerbt ihr euch?"],
     "obj": [("die Stelle", "должность"), ("das Stipendium", "стипендия")]},
    {"lemma": "sich Sorgen machen um", "prep": "um", "case": "akk", "ru": "беспокоиться (о)", "person": True,
     "q": ["machst du dir Sorgen?", "macht ihr euch Sorgen?"],
     "obj": [("die Zukunft", "будущее"), ("die Gesundheit", "здоровье")]},
    {"lemma": "kämpfen um", "prep": "um", "case": "akk", "ru": "бороться (за)", "person": False,
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
    {"lemma": "leiden unter", "prep": "unter", "case": "dat", "ru": "страдать (от)", "person": True,
     "q": ["leidest du?", "leidet ihr?"],
     "obj": [("der Stress", "стресс"), ("die Hitze", "жара")]},
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
    {"lemma": "profitieren von", "prep": "von", "case": "dat", "ru": "выигрывать (от)", "person": True,
     "q": ["profitierst du?", "profitiert ihr?"],
     "obj": [("die Erfahrung", "опыт"), ("das Angebot", "предложение")]},
    {"lemma": "sich verabschieden von", "prep": "von", "case": "dat", "ru": "прощаться (с)", "person": True,
     "person_only": True, "q": ["verabschiedest du dich?", "verabschiedet ihr euch?"],
     "obj": [("der Gast", "гость"), ("die Freundin", "подруга")]},
    {"lemma": "sich erholen von", "prep": "von", "case": "dat", "ru": "оправиться (от)", "person": False,
     "q": ["erholst du dich?", "erholt ihr euch?"],
     "obj": [("die Krankheit", "болезнь"), ("der Stress", "стресс")]},
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
    {"lemma": "sich distanzieren von", "prep": "von", "case": "dat", "ru": "дистанцироваться", "person": True,
     "q": ["distanzierst du dich?", "distanziert ihr euch?"],
     "obj": [("die Aussage", "высказывание"), ("der Plan", "план")]},
    # ── VOR + Dativ ──
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
    {"lemma": "gehören zu", "prep": "zu", "case": "dat", "ru": "относиться (к)", "person": True,
     "q": ["gehörst du?", "gehört ihr?"],
     "obj": [("die Gruppe", "группа"), ("das Team", "команда")]},
    {"lemma": "passen zu", "prep": "zu", "case": "dat", "ru": "подходить (к)", "person": True,
     "q": ["passt das?", "passt es gut?"],
     "obj": [("das Kleid", "платье"), ("die Farbe", "цвет")]},
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
    {"lemma": "neigen zu", "prep": "zu", "case": "dat", "ru": "быть склонным (к)", "person": False,
     "q": ["neigst du?", "neigt ihr?"],
     "obj": [("die Übertreibung", "преувеличение"), ("der Pessimismus", "пессимизм")]},
    {"lemma": "bereit sein zu", "prep": "zu", "case": "dat", "ru": "быть готовым (к)", "person": False,
     "q": ["bist du bereit?", "seid ihr bereit?"],
     "obj": [("der Kompromiss", "компромисс"), ("das Opfer", "жертва")]},
    {"lemma": "Stellung nehmen zu", "prep": "zu", "case": "dat", "ru": "высказаться (по)", "person": False,
     "q": ["nimmst du Stellung?", "nehmt ihr Stellung?"],
     "obj": [("das Thema", "тема"), ("die Frage", "вопрос")]},
]


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


def _options(correct: str, target: str, prep: str, case: str) -> list[str]:
    """4 choices: correct + the opposite thing/person trap + 2 confusable fillers."""
    opts = {correct}
    trap = _person_form(prep, case) if target == "thing" else _wo_form(prep).capitalize()
    opts.add(trap)
    pool = _WO_DISTRACTORS + _PERSON_DISTRACTORS
    random.shuffle(pool)
    for d in pool:
        if len(opts) >= 4:
            break
        opts.add(d)
    out = list(opts)
    random.shuffle(out)
    return out


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
    if target == "person":
        correct = personword
        name, _ = random.choice(_PERSON_NAMES)
        # Clue in RU-context: a bare NAME signals a PERSON, without leaking the
        # German preposition (which is the exact string on the correct button).
        clue = f"{name}."
        obj_display, obj_ru = name, ""
    else:
        correct = woword
        obj_phrase, obj_ru = random.choice(entry["obj"])
        # Clue = Russian gloss of the THING → signals "вещь" but hides the German
        # preposition, so the learner must recall the verb government themselves.
        clue = (obj_ru[:1].upper() + obj_ru[1:] + ".") if obj_ru else (_decline_clue(prep, case, obj_phrase) + ".")
        obj_display = obj_phrase

    return {
        "s": f"___ {frame}",
        "clue": f"— {clue}",
        "a": correct,
        "opts": _options(correct, target, prep, case),
        "target": target,
        "prep": prep,
        "case": case,
        "lemma": entry["lemma"],
        "verb_ru": entry["ru"],
        "obj": obj_display,
        "obj_ru": obj_ru,
        "erklaerung": _erklaerung(entry, target, woword, personword),
        "tip": _tip(entry, target, woword),
    }


def build_wofrage_items(n: int = 10) -> list[dict]:
    """`n` rule-perfect, de-duplicated Wo-Frage items."""
    if not _BANK:
        return []
    out: list[dict] = []
    seen: set[str] = set()
    attempts = 0
    while len(out) < int(n) and attempts < int(n) * 12:
        attempts += 1
        item = _build_one(random.choice(_BANK))
        key = (item["s"] + "|" + item["a"]).lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
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
