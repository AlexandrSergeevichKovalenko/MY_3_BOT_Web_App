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

Variety = (~50 governed verbs/adjectives) × (thing objects | person names) ×
(question frames) × (thing/person mode) → tens of thousands of unique, correct items.
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

# ── Distractor pools (capitalized, sentence-start form) ──────────────────────
_WO_DISTRACTORS = [
    "Worauf", "Woran", "Wofür", "Worüber", "Womit", "Wovon", "Wonach",
    "Wozu", "Worum", "Wogegen", "Wovor", "Woraus", "Worin", "Wodurch",
]
_PERSON_DISTRACTORS = [
    "Auf wen", "An wen", "Für wen", "Über wen", "Um wen",
    "Mit wem", "Von wem", "Zu wem", "Nach wem", "Vor wem",
]

# First names only — they never decline after a preposition, so the clue line
# stays 100% correct for person items ("Auf Anna", "Mit Max", "Über Paul").
_PERSON_NAMES = [
    ("Anna", ""), ("Thomas", ""), ("Julia", ""), ("Max", ""), ("Lena", ""),
    ("Paul", ""), ("Sofia", ""), ("David", ""), ("Emma", ""), ("Jonas", ""),
]

# ── The governed-preposition bank ────────────────────────────────────────────
# entry = {
#   "lemma": display combo, "prep": governed preposition, "case": akk|dat,
#   "ru": Russian gloss, "person": can the object be a person?,
#   "q": [question frames placed AFTER the wo-word], "obj": [(nom phrase, ru)…],
# }
_BANK: list[dict] = [
    # ── Akkusativ-governed ──
    {"lemma": "warten auf", "prep": "auf", "case": "akk", "ru": "ждать", "person": True,
     "q": ["wartest du schon so lange?", "wartet ihr gerade?"],
     "obj": [("der Bus", "автобус"), ("die Antwort", "ответ"), ("das Taxi", "такси")]},
    {"lemma": "hoffen auf", "prep": "auf", "case": "akk", "ru": "надеяться", "person": False,
     "q": ["hoffst du?", "hofft ihr insgeheim?"],
     "obj": [("der Erfolg", "успех"), ("die Zusage", "согласие"), ("das Wunder", "чудо")]},
    {"lemma": "sich freuen auf", "prep": "auf", "case": "akk", "ru": "радоваться (предстоящему)", "person": False,
     "q": ["freust du dich am meisten?", "freut ihr euch so?"],
     "obj": [("der Urlaub", "отпуск"), ("die Reise", "поездка"), ("das Wochenende", "выходные")]},
    {"lemma": "sich freuen über", "prep": "über", "case": "akk", "ru": "радоваться (случившемуся)", "person": True,
     "q": ["freust du dich?", "freut ihr euch so sehr?"],
     "obj": [("das Geschenk", "подарок"), ("die Nachricht", "новость"), ("der Erfolg", "успех")]},
    {"lemma": "achten auf", "prep": "auf", "case": "akk", "ru": "обращать внимание", "person": True,
     "q": ["achtest du besonders?", "achtet ihr?"],
     "obj": [("die Gesundheit", "здоровье"), ("der Preis", "цена"), ("die Zeit", "время")]},
    {"lemma": "denken an", "prep": "an", "case": "akk", "ru": "думать", "person": True,
     "q": ["denkst du gerade?", "denkt ihr oft?"],
     "obj": [("die Zukunft", "будущее"), ("der Urlaub", "отпуск"), ("die Prüfung", "экзамен")]},
    {"lemma": "sich erinnern an", "prep": "an", "case": "akk", "ru": "вспоминать", "person": True,
     "q": ["erinnerst du dich gern?", "erinnert ihr euch?"],
     "obj": [("die Kindheit", "детство"), ("der Unfall", "авария"), ("das Treffen", "встреча")]},
    {"lemma": "glauben an", "prep": "an", "case": "akk", "ru": "верить", "person": True,
     "q": ["glaubst du?", "glaubt ihr wirklich?"],
     "obj": [("die Liebe", "любовь"), ("der Erfolg", "успех"), ("das Schicksal", "судьба")]},
    {"lemma": "sich interessieren für", "prep": "für", "case": "akk", "ru": "интересоваться", "person": True,
     "q": ["interessierst du dich?", "interessiert ihr euch?"],
     "obj": [("die Kunst", "искусство"), ("der Sport", "спорт"), ("die Politik", "политика")]},
    {"lemma": "danken für", "prep": "für", "case": "akk", "ru": "благодарить (за)", "person": False,
     "q": ["dankst du ihm?", "dankt ihr?"],
     "obj": [("die Hilfe", "помощь"), ("das Geschenk", "подарок"), ("die Einladung", "приглашение")]},
    {"lemma": "sorgen für", "prep": "für", "case": "akk", "ru": "заботиться", "person": True,
     "q": ["sorgst du?", "sorgt ihr?"],
     "obj": [("die Ordnung", "порядок"), ("das Essen", "еда"), ("die Sicherheit", "безопасность")]},
    {"lemma": "sich bewerben um", "prep": "um", "case": "akk", "ru": "претендовать (на)", "person": False,
     "q": ["bewirbst du dich?", "bewerbt ihr euch?"],
     "obj": [("die Stelle", "должность"), ("der Job", "работа"), ("das Stipendium", "стипендия")]},
    {"lemma": "bitten um", "prep": "um", "case": "akk", "ru": "просить", "person": False,
     "q": ["bittest du?", "bittet ihr?"],
     "obj": [("die Hilfe", "помощь"), ("der Rat", "совет"), ("die Erlaubnis", "разрешение")]},
    {"lemma": "sich kümmern um", "prep": "um", "case": "akk", "ru": "заботиться", "person": True,
     "q": ["kümmerst du dich?", "kümmert ihr euch?"],
     "obj": [("der Garten", "сад"), ("das Haus", "дом"), ("die Sache", "дело")]},
    {"lemma": "sich ärgern über", "prep": "über", "case": "akk", "ru": "злиться", "person": True,
     "q": ["ärgerst du dich?", "ärgert ihr euch?"],
     "obj": [("der Fehler", "ошибка"), ("das Wetter", "погода"), ("die Verspätung", "опоздание")]},
    {"lemma": "sich beschweren über", "prep": "über", "case": "akk", "ru": "жаловаться", "person": True,
     "q": ["beschwerst du dich?", "beschwert ihr euch?"],
     "obj": [("der Lärm", "шум"), ("das Essen", "еда"), ("die Kälte", "холод")]},
    {"lemma": "nachdenken über", "prep": "über", "case": "akk", "ru": "размышлять", "person": False,
     "q": ["denkst du nach?", "denkt ihr nach?"],
     "obj": [("die Frage", "вопрос"), ("das Problem", "проблема"), ("die Zukunft", "будущее")]},
    {"lemma": "sprechen über", "prep": "über", "case": "akk", "ru": "говорить (о)", "person": True,
     "q": ["sprichst du?", "sprecht ihr?"],
     "obj": [("die Politik", "политика"), ("das Wetter", "погода"), ("der Film", "фильм")]},
    {"lemma": "schreiben über", "prep": "über", "case": "akk", "ru": "писать (о)", "person": True,
     "q": ["schreibst du?", "schreibt ihr?"],
     "obj": [("die Reise", "поездка"), ("das Ereignis", "событие"), ("die Stadt", "город")]},
    {"lemma": "sich aufregen über", "prep": "über", "case": "akk", "ru": "нервничать (из-за)", "person": True,
     "q": ["regst du dich auf?", "regt ihr euch auf?"],
     "obj": [("die Nachricht", "новость"), ("der Stau", "пробка"), ("das Chaos", "хаос")]},
    {"lemma": "stolz sein auf", "prep": "auf", "case": "akk", "ru": "гордиться", "person": True,
     "q": ["bist du stolz?", "seid ihr stolz?"],
     "obj": [("der Erfolg", "успех"), ("das Ergebnis", "результат"), ("die Leistung", "достижение")]},
    {"lemma": "neugierig sein auf", "prep": "auf", "case": "akk", "ru": "быть любопытным (к)", "person": True,
     "q": ["bist du neugierig?", "seid ihr neugierig?"],
     "obj": [("das Ergebnis", "результат"), ("die Antwort", "ответ"), ("die Überraschung", "сюрприз")]},

    # ── Dativ-governed ──
    {"lemma": "sprechen mit", "prep": "mit", "case": "dat", "ru": "говорить (с)", "person": True,
     "q": ["sprichst du?", "sprecht ihr?"], "person_only": True,
     "obj": [("der Chef", "начальник"), ("die Kollegin", "коллега"), ("der Nachbar", "сосед")]},
    {"lemma": "rechnen mit", "prep": "mit", "case": "dat", "ru": "рассчитывать (на)", "person": True,
     "q": ["rechnest du?", "rechnet ihr?"],
     "obj": [("der Regen", "дождь"), ("die Verspätung", "опоздание"), ("das Schlimmste", "худшее")]},
    {"lemma": "sich beschäftigen mit", "prep": "mit", "case": "dat", "ru": "заниматься", "person": True,
     "q": ["beschäftigst du dich?", "beschäftigt ihr euch?"],
     "obj": [("die Musik", "музыка"), ("das Thema", "тема"), ("die Aufgabe", "задача")]},
    {"lemma": "anfangen mit", "prep": "mit", "case": "dat", "ru": "начинать (с)", "person": False,
     "q": ["fängst du an?", "fangt ihr an?"],
     "obj": [("die Arbeit", "работа"), ("das Studium", "учёба"), ("der Sport", "спорт")]},
    {"lemma": "aufhören mit", "prep": "mit", "case": "dat", "ru": "прекращать", "person": False,
     "q": ["hörst du auf?", "hört ihr auf?"],
     "obj": [("die Arbeit", "работа"), ("der Lärm", "шум"), ("das Rauchen", "курение")]},
    {"lemma": "träumen von", "prep": "von", "case": "dat", "ru": "мечтать", "person": True,
     "q": ["träumst du?", "träumt ihr?"],
     "obj": [("die Zukunft", "будущее"), ("das Meer", "море"), ("der Urlaub", "отпуск")]},
    {"lemma": "erzählen von", "prep": "von", "case": "dat", "ru": "рассказывать (о)", "person": True,
     "q": ["erzählst du?", "erzählt ihr?"],
     "obj": [("die Reise", "поездка"), ("das Erlebnis", "впечатление"), ("der Urlaub", "отпуск")]},
    {"lemma": "profitieren von", "prep": "von", "case": "dat", "ru": "выигрывать (от)", "person": True,
     "q": ["profitierst du?", "profitiert ihr?"],
     "obj": [("die Erfahrung", "опыт"), ("das Angebot", "предложение"), ("die Reform", "реформа")]},
    {"lemma": "gehören zu", "prep": "zu", "case": "dat", "ru": "относиться (к)", "person": True,
     "q": ["gehörst du?", "gehört ihr?"],
     "obj": [("die Gruppe", "группа"), ("das Team", "команда"), ("die Familie", "семья")]},
    {"lemma": "passen zu", "prep": "zu", "case": "dat", "ru": "подходить (к)", "person": True,
     "q": ["passt das?", "passt es gut?"],
     "obj": [("das Kleid", "платье"), ("die Farbe", "цвет"), ("der Anzug", "костюм")]},
    {"lemma": "einladen zu", "prep": "zu", "case": "dat", "ru": "приглашать (на)", "person": False,
     "q": ["lädst du ein?", "ladet ihr ein?"],
     "obj": [("die Party", "вечеринка"), ("das Essen", "ужин"), ("die Feier", "праздник")]},
    {"lemma": "fragen nach", "prep": "nach", "case": "dat", "ru": "спрашивать (о)", "person": True,
     "q": ["fragst du?", "fragt ihr?"],
     "obj": [("der Weg", "дорога"), ("die Uhrzeit", "время"), ("die Adresse", "адрес")]},
    {"lemma": "suchen nach", "prep": "nach", "case": "dat", "ru": "искать", "person": True,
     "q": ["suchst du?", "sucht ihr?"],
     "obj": [("die Lösung", "решение"), ("der Schlüssel", "ключ"), ("die Wahrheit", "правда")]},
    {"lemma": "streben nach", "prep": "nach", "case": "dat", "ru": "стремиться", "person": False,
     "q": ["strebst du?", "strebt ihr?"],
     "obj": [("der Erfolg", "успех"), ("die Macht", "власть"), ("das Glück", "счастье")]},
    {"lemma": "teilnehmen an", "prep": "an", "case": "dat", "ru": "участвовать (в)", "person": False,
     "q": ["nimmst du teil?", "nehmt ihr teil?"],
     "obj": [("der Kurs", "курс"), ("die Sitzung", "заседание"), ("das Turnier", "турнир")]},
    {"lemma": "arbeiten an", "prep": "an", "case": "dat", "ru": "работать (над)", "person": False,
     "q": ["arbeitest du?", "arbeitet ihr?"],
     "obj": [("das Projekt", "проект"), ("die Aufgabe", "задача"), ("der Roman", "роман")]},
    {"lemma": "zweifeln an", "prep": "an", "case": "dat", "ru": "сомневаться (в)", "person": True,
     "q": ["zweifelst du?", "zweifelt ihr?"],
     "obj": [("der Plan", "план"), ("die Aussage", "высказывание"), ("der Erfolg", "успех")]},
    {"lemma": "leiden unter", "prep": "unter", "case": "dat", "ru": "страдать (от)", "person": True,
     "q": ["leidest du?", "leidet ihr?"],
     "obj": [("der Stress", "стресс"), ("die Hitze", "жара"), ("der Lärm", "шум")]},
    {"lemma": "bestehen aus", "prep": "aus", "case": "dat", "ru": "состоять (из)", "person": False,
     "q": ["besteht das Team?", "besteht die Mischung?"],
     "obj": [("das Glas", "стекло"), ("das Metall", "металл"), ("die Watte", "вата")]},
    {"lemma": "sich fürchten vor", "prep": "vor", "case": "dat", "ru": "бояться", "person": True,
     "q": ["fürchtest du dich?", "fürchtet ihr euch?"],
     "obj": [("die Zukunft", "будущее"), ("der Hund", "собака"), ("das Gewitter", "гроза")]},
    {"lemma": "Angst haben vor", "prep": "vor", "case": "dat", "ru": "бояться", "person": True,
     "q": ["hast du Angst?", "habt ihr Angst?"],
     "obj": [("die Prüfung", "экзамен"), ("der Tod", "смерть"), ("die Dunkelheit", "темнота")]},
    {"lemma": "warnen vor", "prep": "vor", "case": "dat", "ru": "предупреждать (о)", "person": True,
     "q": ["warnst du?", "warnt ihr?"],
     "obj": [("die Gefahr", "опасность"), ("der Sturm", "шторм"), ("das Risiko", "риск")]},
    {"lemma": "sich schützen vor", "prep": "vor", "case": "dat", "ru": "защищаться (от)", "person": True,
     "q": ["schützt du dich?", "schützt ihr euch?"],
     "obj": [("die Sonne", "солнце"), ("der Regen", "дождь"), ("die Kälte", "холод")]},
    {"lemma": "zufrieden sein mit", "prep": "mit", "case": "dat", "ru": "быть довольным", "person": True,
     "q": ["bist du zufrieden?", "seid ihr zufrieden?"],
     "obj": [("das Ergebnis", "результат"), ("die Arbeit", "работа"), ("der Lohn", "зарплата")]},
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
        clue = f"{prep.capitalize()} {name}."
        obj_display, obj_ru = name, ""
    else:
        correct = woword
        obj_phrase, obj_ru = random.choice(entry["obj"])
        correct = woword
        clue = _decline_clue(prep, case, obj_phrase) + "."
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
    import json
    items = build_wofrage_items(10)
    for it in items:
        print(f'{it["s"]}\n  {it["clue"]}')
        print(f'  correct: {it["a"]}   ({it["target"]})   opts: {it["opts"]}')
        print(f'  {it["tip"]}')
        print(f'  {it["erklaerung"]}\n')
    # invariant checks
    bad = 0
    for it in items:
        if it["a"] not in it["opts"]:
            bad += 1; print("!! correct not in opts:", it)
        if len(set(it["opts"])) != 4:
            bad += 1; print("!! opts not 4 unique:", it["opts"])
    print(f"checked {len(items)} items, {bad} problems")
