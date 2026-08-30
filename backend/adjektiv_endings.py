"""Deterministic generator for German adjective-ending items.

Adjective endings are a CLOSED, fully rule-based system: the ending depends only
on declension type (weak after der/die/das, mixed after ein/kein/mein, strong
with no determiner) × case (Nom/Akk/Dat) × gender (m/f/n). So we can build
UNLIMITED, 100%-correct items with no LLM and zero "bред":
  noun (+gender) from the Artikel noun bank  ×  a regular adjective  ×  a random
  declension type/case  →  phrase + correct ending + a Russian rule + tip.

We stay in the SINGULAR Nom/Akk/Dat (the noun keeps its dictionary form), which
already exercises every ending (-e/-en/-er/-es/-em). Akk/Dat get a governing
preposition so the case is natural and unambiguous.
"""
from __future__ import annotations

import logging
import random

# Regular adjectives only — consonant/vowel stems that just take the ending.
# Deliberately EXCLUDES -el/-er/-e stems (dunkel→dunkles, teuer→teures,
# leise→leiser) and irregular "hoch" → no wrong forms can be produced.
# German adjective → Russian (lemma, masculine nominative). Used both to generate
# items AND to show an accurate per-WORD translation on tap (the phrase itself is
# auto-generated and may be unreal/funny — meaning lives at the word level, not the
# phrase). Regular stems ONLY (no -el/-er/-e stems, no irregular "hoch") so no wrong
# forms can be produced.
ADJECTIVE_RU = {
    "gut": "хороший", "neu": "новый", "alt": "старый", "jung": "молодой",
    "klein": "маленький", "groß": "большой", "lang": "длинный", "kurz": "короткий",
    "breit": "широкий", "schmal": "узкий", "tief": "глубокий", "dick": "толстый",
    "dünn": "тонкий", "schwer": "тяжёлый", "leicht": "лёгкий", "schnell": "быстрый",
    "langsam": "медленный", "laut": "громкий", "warm": "тёплый", "kalt": "холодный",
    "kühl": "прохладный", "heiß": "горячий", "frisch": "свежий", "schmutzig": "грязный",
    "nass": "мокрый", "trocken": "сухой", "hart": "твёрдый", "weich": "мягкий",
    "glatt": "гладкий", "scharf": "острый", "stark": "сильный", "schwach": "слабый",
    "gesund": "здоровый", "krank": "больной", "wach": "бодрствующий", "reich": "богатый",
    "arm": "бедный", "billig": "дешёвый", "günstig": "выгодный", "wichtig": "важный",
    "nützlich": "полезный", "interessant": "интересный", "langweilig": "скучный",
    "schön": "красивый", "hässlich": "уродливый", "freundlich": "дружелюбный",
    "höflich": "вежливый", "ehrlich": "честный", "klug": "умный", "dumm": "глупый",
    "fleißig": "прилежный", "faul": "ленивый", "mutig": "смелый", "ruhig": "спокойный",
    "glücklich": "счастливый", "traurig": "грустный", "lustig": "весёлый", "ernst": "серьёзный",
    "modern": "современный", "bekannt": "известный", "berühmt": "знаменитый", "fremd": "чужой",
    "ganz": "целый", "voll": "полный", "leer": "пустой", "offen": "открытый",
    "frei": "свободный", "möglich": "возможный", "nötig": "нужный", "richtig": "правильный",
    "falsch": "неправильный", "einfach": "простой", "schwierig": "трудный", "sicher": "надёжный",
    "gefährlich": "опасный", "bequem": "удобный", "praktisch": "практичный",
    "natürlich": "естественный", "echt": "настоящий", "typisch": "типичный", "normal": "нормальный",
    "grün": "зелёный", "blau": "синий", "rot": "красный", "gelb": "жёлтый",
    "schwarz": "чёрный", "weiß": "белый", "grau": "серый", "braun": "коричневый",
    # ── expanded set for variety (single-word, regular stems only) ──────────────
    "hell": "светлый", "klar": "ясный", "rund": "круглый", "spitz": "острый (заострённый)",
    "flach": "плоский", "steil": "крутой", "eng": "тесный", "weit": "далёкий",
    "fern": "далёкий", "nah": "близкий", "niedrig": "низкий", "tapfer": "храбрый",
    "stolz": "гордый", "bescheiden": "скромный", "geduldig": "терпеливый",
    "neugierig": "любопытный", "vorsichtig": "осторожный", "nervös": "нервный",
    "munter": "бодрый", "satt": "сытый", "hungrig": "голодный", "durstig": "жаждущий",
    "berauscht": "опьянённый", "verrückt": "сумасшедший", "wahr": "истинный",
    "geheim": "тайный", "öffentlich": "публичный", "privat": "частный", "sozial": "социальный",
    "politisch": "политический", "wirtschaftlich": "экономический", "kulturell": "культурный",
    "historisch": "исторический", "religiös": "религиозный", "wissenschaftlich": "научный",
    "technisch": "технический", "digital": "цифровой", "elektrisch": "электрический",
    "automatisch": "автоматический", "manuell": "ручной", "global": "глобальный",
    "lokal": "местный", "national": "национальный", "international": "международный",
    "modisch": "модный", "elegant": "элегантный", "schick": "стильный", "schlicht": "простой (скромный)",
    "bunt": "пёстрый", "farbig": "цветной", "blass": "бледный", "golden": "золотой",
    "silbern": "серебряный", "violett": "фиолетовый", "lecker": "вкусный", "süß": "сладкий",
    "bitter": "горький", "salzig": "солёный", "würzig": "пряный", "fettig": "жирный",
    "mager": "постный", "roh": "сырой", "reif": "спелый", "faulig": "гнилой",
    "giftig": "ядовитый", "sauber": "чистый", "ordentlich": "аккуратный", "chaotisch": "хаотичный",
    "still": "тихий (безмолвный)", "ruhelos": "беспокойный", "hektisch": "суетливый",
    "beliebt": "популярный", "unbeliebt": "непопулярный", "wertvoll": "ценный",
    "wertlos": "бесполезный", "kostbar": "драгоценный", "altmodisch": "старомодный",
    "klassisch": "классический", "tragisch": "трагический", "komisch": "комичный",
    "seltsam": "странный", "merkwürdig": "странный", "gewöhnlich": "обычный",
    "besonders": "особенный", "selten": "редкий", "häufig": "частый", "plötzlich": "внезапный",
    "endlos": "бесконечный", "ewig": "вечный", "kurzfristig": "краткосрочный",
    "duftend": "ароматный", "glänzend": "блестящий", "matt": "матовый", "durchsichtig": "прозрачный",
    "undurchsichtig": "непрозрачный", "fest": "крепкий", "locker": "рыхлый", "elastisch": "эластичный",
    "zerbrechlich": "хрупкий", "robust": "прочный", "stabil": "устойчивый", "beweglich": "подвижный",
    "unbeweglich": "неподвижный", "lebendig": "живой", "tot": "мёртвый", "wild": "дикий",
    "zahm": "ручной", "gefährdet": "под угрозой", "harmlos": "безобидный", "freudig": "радостный",
    "wütend": "злой", "ängstlich": "боязливый", "eifersüchtig": "ревнивый", "großzügig": "щедрый",
    "geizig": "скупой", "sparsam": "экономный", "verschwenderisch": "расточительный",
    "begabt": "одарённый", "talentiert": "талантливый", "erfahren": "опытный",
    "unerfahren": "неопытный", "geschickt": "ловкий", "ungeschickt": "неуклюжий",
    "pünktlich": "пунктуальный", "zuverlässig": "надёжный", "verantwortlich": "ответственный",
    "treu": "верный", "untreu": "неверный", "loyal": "лояльный", "gerecht": "справедливый",
    "ungerecht": "несправедливый", "streng": "строгий", "grausam": "жестокий", "sanft": "нежный",
    "zärtlich": "ласковый", "herzlich": "сердечный", "fröhlich": "радостный", "heiter": "весёлый",
    "düster": "мрачный", "gemütlich": "уютный", "ungemütlich": "неуютный", "geräumig": "просторный",
    "winzig": "крошечный", "riesig": "огромный", "gigantisch": "гигантский", "mächtig": "могучий",
    "klebrig": "липкий", "rutschig": "скользкий", "rau": "шершавый", "kantig": "угловатый",
    "schlau": "хитрый", "naiv": "наивный",
    # ── second wave (real, regular stems: -ig/-lich/-isch/-bar/-sam/-haft + participles) ──
    "eisig": "ледяной", "windig": "ветреный", "sonnig": "солнечный", "wolkig": "облачный",
    "neblig": "туманный", "staubig": "пыльный", "sandig": "песчаный", "steinig": "каменистый",
    "schlammig": "илистый", "matschig": "слякотный", "feucht": "влажный", "schwül": "душный",
    "frostig": "морозный", "schattig": "тенистый", "sumpfig": "болотистый", "stürmisch": "бурный",
    "haarig": "волосатый", "borstig": "щетинистый", "wollig": "шерстяной", "seidig": "шелковистый",
    "samtig": "бархатистый", "faltig": "морщинистый", "runzlig": "морщинистый", "körnig": "зернистый",
    "klumpig": "комковатый", "schaumig": "пенистый", "schleimig": "слизистый", "saftig": "сочный",
    "fruchtig": "фруктовый", "milchig": "молочный", "wässrig": "водянистый", "ölig": "маслянистый",
    "knusprig": "хрустящий", "zäh": "жёсткий", "ranzig": "прогорклый", "gierig": "жадный",
    "eifrig": "усердный", "emsig": "хлопотливый", "stur": "упрямый", "hartnäckig": "упорный",
    "prächtig": "великолепный", "heftig": "резкий", "wuchtig": "массивный", "schläfrig": "сонный",
    "fiebrig": "лихорадочный", "blutig": "кровавый", "holprig": "ухабистый", "kurvig": "извилистый",
    "zackig": "зубчатый", "fehlerhaft": "ошибочный", "zweifelhaft": "сомнительный",
    "schmerzhaft": "болезненный", "ekelhaft": "отвратительный", "lebhaft": "оживлённый",
    "standhaft": "стойкий", "tugendhaft": "добродетельный", "mangelhaft": "недостаточный",
    "zaghaft": "робкий", "essbar": "съедобный", "trinkbar": "питьевой", "lesbar": "читаемый",
    "machbar": "выполнимый", "denkbar": "мыслимый", "sichtbar": "видимый", "hörbar": "слышимый",
    "spürbar": "ощутимый", "brauchbar": "пригодный", "fruchtbar": "плодородный", "dankbar": "благодарный",
    "sonderbar": "странный", "wunderbar": "чудесный", "furchtbar": "ужасный", "unsichtbar": "невидимый",
    "heilbar": "излечимый", "aufmerksam": "внимательный", "gehorsam": "послушный", "sorgsam": "заботливый",
    "behutsam": "бережный", "gewaltsam": "насильственный", "mühsam": "трудоёмкий", "wachsam": "бдительный",
    "biegsam": "гибкий", "genügsam": "неприхотливый", "ratsam": "целесообразный", "ärgerlich": "досадный",
    "kindlich": "детский", "männlich": "мужской", "weiblich": "женский", "tödlich": "смертельный",
    "schädlich": "вредный", "heimlich": "тайный", "unheimlich": "жуткий", "köstlich": "восхитительный",
    "peinlich": "неловкий", "reichlich": "обильный", "ländlich": "сельский", "jährlich": "ежегодный",
    "täglich": "ежедневный", "wöchentlich": "еженедельный", "monatlich": "ежемесячный",
    "feierlich": "торжественный", "gründlich": "основательный", "verständlich": "понятный",
    "erträglich": "терпимый", "beträchtlich": "значительный", "erheblich": "существенный",
    "kläglich": "жалкий", "schrecklich": "страшный", "herrlich": "великолепный", "kindisch": "ребяческий",
    "neidisch": "завистливый", "launisch": "капризный", "wählerisch": "привередливый",
    "heimisch": "местный", "tierisch": "звериный", "teuflisch": "дьявольский", "magisch": "магический",
    "logisch": "логичный", "kritisch": "критический", "exotisch": "экзотический", "mechanisch": "механический",
    "organisch": "органический", "chemisch": "химический", "medizinisch": "медицинский",
    "künstlerisch": "художественный", "gekocht": "варёный", "gebraten": "жареный", "gebacken": "печёный",
    "gegrillt": "приготовленный на гриле", "gefroren": "замороженный", "getrocknet": "сушёный",
    "geräuchert": "копчёный", "gemahlen": "молотый", "gehackt": "рубленый", "gepresst": "прессованный",
    "poliert": "полированный", "lackiert": "лакированный", "gestreift": "полосатый", "kariert": "клетчатый",
    "gepunktet": "в горошек", "gemustert": "узорчатый", "zerbrochen": "разбитый", "zerrissen": "порванный",
    "verbrannt": "сгоревший", "gefüllt": "наполненный", "geleert": "опустошённый",
}

# Citation list for random generation (keys of ADJECTIVE_RU).
ADJECTIVES = list(ADJECTIVE_RU.keys())

# ⛔ ЗДЕСЬ БЫЛИ 30 ВШИТЫХ СУЩЕСТВИТЕЛЬНЫХ. УБРАНЫ 28.08.2026.
#
# `_load_nouns` при любом сбое молча отдавал этот список, и тренажёр крутил одни и те
# же тридцать слов вместо 5835 из банка. Немецкий в них был верный — но сбой прятался:
# программа делала вид, что всё хорошо, и владелец не узнавал об этом никогда.
#
# РЕШЕНИЕ ВЛАДЕЛЬЦА 28.08.2026, дословно: «чего мы 30 одинаковых всегда давать будем?
# Давай посмотрим, сколько их готовых есть, и каждый раз будем делать перемешивание. И
# ОБЯЗАТЕЛЬНО МНЕ СООБЩАТЬ ОБ ЭТОМ, чтобы я знал, что нормальная схема не сработала.
# Это уже не заглушка, а аварийный выход.»
#
# Как устроено теперь: аварийный запас — 250 РАЗНЫХ готовых заданий в банке
# (`ensure_adjektiv_reserve` в database.py). Он собирается тем же детерминированным
# правилом бесплатно — 250 штук за 2,8 секунды, без единого обращения к модели.
# Не удалось взять слова из банка — берём 15 СЛУЧАЙНЫХ из этих 250 и СООБЩАЕМ владельцу.
# Совсем ничего нет — честно ничего не отдаём, и владелец получает то же сообщение.

_ART_GENDER = {"der": "m", "die": "f", "das": "n"}

# Ending tables: [case][gender], gender ∈ m/f/n. Singular only.
_WEAK = {"Nom": {"m": "e", "f": "e", "n": "e"},
         "Akk": {"m": "en", "f": "e", "n": "e"},
         "Dat": {"m": "en", "f": "en", "n": "en"}}
_MIXED = {"Nom": {"m": "er", "f": "e", "n": "es"},
          "Akk": {"m": "en", "f": "e", "n": "es"},
          "Dat": {"m": "en", "f": "en", "n": "en"}}
_STRONG = {"Nom": {"m": "er", "f": "e", "n": "es"},
           "Akk": {"m": "en", "f": "e", "n": "es"},
           "Dat": {"m": "em", "f": "er", "n": "em"}}
_TABLES = {"weak": _WEAK, "mixed": _MIXED, "strong": _STRONG}

# Determiner forms by [case][gender].
_DEF = {"Nom": {"m": "der", "f": "die", "n": "das"},
        "Akk": {"m": "den", "f": "die", "n": "das"},
        "Dat": {"m": "dem", "f": "der", "n": "dem"}}
_EIN = {"Nom": {"m": "ein", "f": "eine", "n": "ein"},
        "Akk": {"m": "einen", "f": "eine", "n": "ein"},
        "Dat": {"m": "einem", "f": "einer", "n": "einem"}}

_AKK_PREPS = ["für", "ohne", "gegen", "durch", "um"]
_DAT_PREPS = ["mit", "bei", "nach", "aus", "von", "zu"]

_CASE_RU = {"Nom": "Именительный", "Akk": "Винительный", "Dat": "Дательный"}
_GEN_RU = {"m": "муж. род", "f": "жен. род", "n": "ср. род"}
_TYPE_RU = {"weak": "слабое склонение (после der/die/das)",
            "mixed": "смешанное склонение (после ein/kein)",
            "strong": "сильное склонение (без артикля)"}
_TYPE_SHORT = {"weak": "слабое", "mixed": "смешанное", "strong": "сильное"}


_GENDER_ART = {"m": "der", "f": "die", "n": "das"}


class NounBankUnavailable(RuntimeError):
    """База не ответила на запрос к банку существительных.

    Это НЕ «банк пуст». Про банк в этом случае мы не знаем ВООБЩЕ ничего, и подавать
    незнание как знание нельзя: владельцу уйдёт доклад «банк существительных пуст»,
    он полезет искать пропавшие слова, а слова на месте — молчала база.
    """


def _load_nouns(limit: int = 400) -> list[tuple]:
    """(word, gender, meaning_ru). meaning_ru lets the trainer show an accurate
    per-word translation on tap (and save the noun with its article).

    Два исхода, и путать их нельзя:
      • список (возможно, пустой) — база ОТВЕТИЛА, и это правда о банке;
      • NounBankUnavailable — база не ответила, про банк мы ничего не знаем.

    ┌─ ПОЧЕМУ ЗДЕСЬ БОЛЬШЕ НЕТ `except: return []` (30.08.2026) ────────────────────┐
    │ Было: любая ошибка базы превращалась в пустой список. Пустой список от ошибки  │
    │ неотличим от пустого банка, и pick_adjektiv_payloads докладывал владельцу      │
    │ «банк существительных пуст» — неверный диагноз при целом банке.                │
    │ Замер связи с машины владельца 30.08: 15 обращений, среднее 1,3 c, самое       │
    │ долгое 15,5 c — всплеск случается, и он не значит, что слова пропали.          │
    │ Решение владельца 28.08 («не подставлять свои слова, взять аварийный запас и   │
    │ ОБЯЗАТЕЛЬНО сообщить») в силе и не тронуто: вызывающий по-прежнему берёт запас  │
    │ и по-прежнему докладывает — просто теперь называет НАСТОЯЩУЮ причину.          │
    └───────────────────────────────────────────────────────────────────────────────┘
    """
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT word, article, COALESCE(meaning_ru, '') FROM bt_3_article_sprint_nouns "
                    "WHERE retired=FALSE AND verified=TRUE AND article IN ('der','die','das') "
                    "ORDER BY random() LIMIT %s;",
                    (int(limit),),
                )
                rows = [(str(r[0]).strip(), _ART_GENDER.get(str(r[1]), "n"), str(r[2] or "").strip())
                        for r in (cur.fetchall() or []) if r and r[0]]
    except Exception as exc:
        logging.warning("тренажёр окончаний: банк существительных НЕДОСТУПЕН (%s) — "
                        "это не «банк пуст», слова могут быть на месте", exc, exc_info=True)
        raise NounBankUnavailable(f"банк существительных недоступен: {exc}") from exc
    if not rows:
        # Банк правда пуст. Подставлять свои слова здесь мы права не имеем:
        # вызывающий возьмёт аварийный запас и СООБЩИТ владельцу.
        logging.warning("тренажёр окончаний: банк существительных вернул ПУСТО — "
                        "включается аварийный запас готовых заданий")
    return rows


# The strong adjective ending "signals" the gender/case like the definite article
# would: -er↔der, -es↔das, -em↔dem, -e↔die, -en↔den. This is the core "gut feeling".
_REF_ART = {"er": "der", "es": "das", "em": "dem", "e": "die", "en": "den"}
# Canonical example words per gender (for the deterministic "пример" line).
_EX_NOUNS = {"m": ["Wein", "Tag"], "f": ["Suppe", "Idee"], "n": ["Auto", "Kind"]}
_EX_ADJS = ["neu", "klein"]


def _rule(typ: str, case: str, gender: str, ending: str) -> str:
    base = {"weak": "После der/die/das", "mixed": "После ein/kein", "strong": "Без артикля"}[typ]
    return f"{base}, {_GEN_RU[gender]}, {_CASE_RU[case]} → окончание -{ending}."


def _feeling(typ: str, case: str, gender: str, ending: str) -> str:
    ref = _REF_ART.get(ending, "der")
    if typ == "weak":
        return f"der/die/das уже показал род и падеж → прилагательному легко: -{ending}."
    if typ == "mixed":
        strong_slot = (case == "Nom" and gender in ("m", "n")) or (case == "Akk" and gender == "n")
        if strong_slot:
            return f"ein «молчит» о роде → прилагательное досказывает сигнал -{ending} (как {ref})."
        return f"ein/kein здесь уже показал род/падеж → лёгкое -{ending}."
    return f"Без артикля прилагательное «надевает шляпу артикля» → копирует -{ending} (как {ref})."


def _example(typ: str, case: str, gender: str, ending: str) -> str:
    det = _DEF[case][gender] if typ == "weak" else (_EIN[case][gender] if typ == "mixed" else "")
    out = []
    for adj, noun in zip(_EX_ADJS, _EX_NOUNS[gender]):
        out.append(" ".join([p for p in (det, adj + ending, noun) if p]))
    return ", ".join(out)


def _build_one(noun: tuple, adjective: str) -> dict:
    word, gender = noun[0], noun[1]
    noun_ru = str(noun[2]) if len(noun) > 2 else ""
    typ = random.choice(["weak", "mixed", "strong"])
    case = random.choice(["Nom", "Akk", "Dat"])
    ending = _TABLES[typ][case][gender]
    if typ == "weak":
        det = _DEF[case][gender]
    elif typ == "mixed":
        det = _EIN[case][gender]
    else:
        det = ""
    prep = ""
    if case == "Akk":
        prep = random.choice(_AKK_PREPS)
    elif case == "Dat":
        prep = random.choice(_DAT_PREPS)
    pre = [p for p in (prep, det) if p]
    full = " ".join(pre + [adjective + ending, word])
    before = " ".join(pre + [adjective]).strip()
    after = " " + word
    hint_ru = f"{_CASE_RU[case]}, {_GEN_RU[gender]}, {_TYPE_SHORT[typ]}"
    return {
        "before": before, "after": after, "correct": ending, "full": full,
        "erklaerung": _rule(typ, case, gender, ending),
        "tip": _feeling(typ, case, gender, ending),
        "example": _example(typ, case, gender, ending),
        "hint_ru": hint_ru,
        # Из чего задание СОБРАНО. До 15.08.2026 эти три величины считались и тут же
        # выбрасывались — оставалась только русская подсказка строкой. Без них нельзя
        # сказать, на чём человек спотыкается: клеток всего 27 (3 типа × 3 падежа ×
        # 3 рода), и люди обычно стабильно валят две-три из них.
        "typ": typ, "case": case, "gender": gender,
        # Per-word data for tap-to-translate + save-to-dictionary.
        "adj": adjective, "adj_ru": ADJECTIVE_RU.get(adjective, ""),
        "noun": word, "noun_ru": noun_ru, "noun_article": _GENDER_ART.get(gender, "das"),
    }


def build_adjektiv_items(n: int = 15) -> list[dict]:
    """`n` rule-perfect, de-duplicated adjective-ending items."""
    nouns = _load_nouns()
    if not nouns:
        return []
    out: list[dict] = []
    seen: set[str] = set()
    attempts = 0
    while len(out) < int(n) and attempts < int(n) * 8:
        attempts += 1
        item = _build_one(random.choice(nouns), random.choice(ADJECTIVES))
        key = item["full"].lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out
