"""Deterministic German grammar-table engine for the Быстрый словарь deep card.

Design decision (see memory project_quick_dict_deep_redesign): German declension
and conjugation are largely RULE-BASED, so we build the full tables here instead
of trusting an LLM to render them (fewer wrong forms, zero token cost). The LLM
only supplies the few genuinely irregular cells it is good at — the noun plural /
genitive, a verb's principal parts (Präteritum, Partizip II / Perfekt, the
irregular du-/er-Präsens forms) — which we feed in as `seed` values. Everything
else is computed.

Every public builder returns a plain dict that is safe to ship to the frontend as
JSON; absent data yields None/empty rather than raising, so a partial LLM payload
still produces a usable (if smaller) table.
"""

from __future__ import annotations

import logging
import re
from typing import Any


# ── Definite-article declension by gender × case (singular) and plural ──────────
_ART_SG = {
    "m": {"nom": "der", "akk": "den", "dat": "dem", "gen": "des"},
    "f": {"nom": "die", "akk": "die", "dat": "der", "gen": "der"},
    "n": {"nom": "das", "akk": "das", "dat": "dem", "gen": "des"},
}
_ART_PL = {"nom": "die", "akk": "die", "dat": "den", "gen": "der"}

_CASES = ("nom", "akk", "dat", "gen")
_CASE_LABELS_RU = {
    "nom": "Nominativ",
    "akk": "Akkusativ",
    "dat": "Dativ",
    "gen": "Genitiv",
}


def gender_from_article(article: str | None) -> str | None:
    """Map a definite article to a gender key (m/f/n). Plural «die» is ambiguous
    on its own, so a bare «die» maps to feminine here; the noun builder takes an
    explicit plural form separately."""
    a = str(article or "").strip().lower()
    if a in ("der",):
        return "m"
    if a in ("die",):
        return "f"
    if a in ("das",):
        return "n"
    return None


# Any leading definite/indefinite article in any case — so a seed like
# "des Tisches" (genitive) or "dem Kind" (dative) yields the bare noun.
_ARTICLE_TOKENS = {
    "der", "die", "das", "den", "dem", "des",
    "ein", "eine", "einen", "einem", "einer", "eines",
}


def _strip_article(noun: str) -> str:
    parts = str(noun or "").strip().split()
    if parts and parts[0].lower() in _ARTICLE_TOKENS:
        return " ".join(parts[1:])
    return str(noun or "").strip()


def _genitive_singular(noun: str, gender: str, seed_gen: str | None) -> str:
    """Masculine/neuter add -(e)s in the genitive singular; feminine is unchanged.
    Prefer the LLM-provided genitive form when present (handles -ns, irregulars)."""
    seed = str(seed_gen or "").strip()
    if seed:
        return _strip_article(seed)
    if gender == "f":
        return noun
    low = noun.lower()
    # Monosyllabic / sibilant endings take -es, the rest take -s. This is a
    # pragmatic default; the LLM seed overrides it whenever it matters.
    if low.endswith(("s", "ß", "x", "z", "sch")):
        return noun + "es"
    return noun + "s"


def _dative_plural(plural: str) -> str:
    """Dative plural adds -n unless the plural already ends in -n or -s."""
    p = str(plural or "").strip()
    if not p:
        return p
    if p.lower().endswith(("n", "s")):
        return p
    return p + "n"


def _documented_declension(noun: str) -> dict | None:
    """Склонение из справочника (Wiktionary → композит). Модуль подключается внутри:
    он ходит в базу, а этот модуль обязан оставаться чистым для тех, кто зовёт его без
    базы (тесты, оффлайн-скрипты). Метка та же, что у спряжения."""
    import os
    if os.getenv("SKIP_STARTUP_SCHEMA_BOOTSTRAP") == "1" and not os.getenv("REFERENCE_FORMS_LOOKUP"):
        return None
    try:
        from backend.german_reference_forms import noun_declension_for
        return noun_declension_for(noun)
    except Exception:
        logging.debug("справочник склонений недоступен для %s", noun, exc_info=True)
        return None


def _documented_degrees(adjective: str) -> dict | None:
    import os
    if os.getenv("SKIP_STARTUP_SCHEMA_BOOTSTRAP") == "1" and not os.getenv("REFERENCE_FORMS_LOOKUP"):
        return None
    try:
        from backend.german_reference_forms import adjective_degrees_for
        return adjective_degrees_for(adjective)
    except Exception:
        logging.debug("справочник степеней недоступен для %s", adjective, exc_info=True)
        return None


def build_noun_declension(
    *,
    word_de: str,
    article: str | None,
    plural: str | None = None,
    genitive: str | None = None,
) -> dict[str, Any] | None:
    """Full 4-case × singular/plural declension table for a noun, with articles.

    Returns None when the gender can't be determined (no usable article)."""
    noun = _strip_article(word_de)
    if not noun:
        return None
    gender = gender_from_article(article)

    # ТОЛЬКО СПРАВОЧНИК. Прежний счёт падежей отсюда УДАЛЁН: он брал из данных лишь
    # родительный, а винительный и дательный печатал голым словом — «den Student»
    # вместо «den Studenten». Наполнение данных это не лечило, потому что неверна была
    # сама конструкция. Владелец 17.08.2026: «МЫ НИЧЕГО НЕ ПРИДУМЫВАЕМ. Вообще».
    documented = _documented_declension(noun)
    if not documented:
        return None
    picked = documented.get(gender) if gender else None
    if not picked:
        # Род не назван или таблицы под него нет — берём единственную, если она одна.
        tables = [v for k, v in documented.items()
                  if k in ("m", "f", "n", "pl") and isinstance(v, dict)]
        picked = tables[0] if len(tables) == 1 else None
        if not picked:
            return None
        gender = next(k for k, v in documented.items() if v is picked)

    rows = [{"case": r.get("case"), "label": _CASE_LABELS_RU.get(r.get("case"), r.get("label")),
             "singular": r.get("singular") or "",
             **({"plural": r.get("plural")} if r.get("plural") else {})}
            for r in (picked.get("rows") or [])]
    nominative = next((r["singular"] for r in rows if r["case"] == "nom"), "")
    plural_form = next((r.get("plural") for r in rows if r["case"] == "nom"), "") or ""
    return {
        "gender": gender if gender in ("m", "f", "n") else None,
        "article": nominative.split(" ")[0] if nominative else None,
        "singular": noun,
        "plural": " ".join(plural_form.split(" ")[1:]) or None,
        "has_plural": bool(picked.get("has_plural")),
        "source": documented.get("source"),
        "rows": rows,
    }


# ── Verb conjugation ────────────────────────────────────────────────────────────
_PRON = ("ich", "du", "er/sie/es", "wir", "ihr", "sie/Sie")


_SEPARABLE_PREFIXES = (
    "ab", "an", "auf", "aus", "bei", "durch", "ein", "fest", "her", "hin", "los", "mit",
    "nach", "über", "um", "unter", "vor", "weg", "weiter", "zurück", "zusammen", "klar",
    "fort", "heim", "statt", "teil", "wieder", "zu",
)


# Приставки, отделяемость которых ЗАВИСИТ ОТ СМЫСЛА, а не от написания:
# «übersetzen» — переводить (неотделяемая) и переправлять через реку (отделяемая),
# «umfahren» — объезжать и сбивать, «durchschauen» — просматривать и раскусывать.
# Написание у обоих значений одно, поэтому по нему решать нельзя: молчим и оставляем
# слитную форму, которая верна хотя бы для одного из значений.
_AMBIGUOUS_SEPARABLE_PREFIXES = ("über", "um", "unter", "durch", "wider", "wieder")

# Составные приставки: «hinein», «heraus», «zurecht». Их обязательно проверять ЦЕЛИКОМ и
# РАНЬШЕ коротких, иначе «hineingehen» разбирается как «hin» + «eingehen» и печатается
# «ich eingehe hin». Поймано существующим тестом до записи в прод.
_COMPOUND_SEPARABLE_PREFIXES = (
    "hinein", "heraus", "herein", "hinauf", "hinaus", "hinunter", "hinüber",
    "herunter", "herüber", "hervor", "herbei", "heran", "herab", "einher",
    "voran", "voraus", "vorbei", "vorüber", "entlang", "entgegen", "empor",
    "zurecht", "zusammen", "davon", "dabei", "daran", "darauf", "hinweg",
    # Наречия и прилагательные, приросшие к глаголу. Список собран не на глаз, а
    # прогоном по всем 439 отделяемым глаголам справочника 17.08.2026: без них
    # «zunichtemachen» разбирался как «zu» + «nichtemachen», «herumkommandieren» как
    # «her» + «umkommandieren», «hinterherkommen» как «hin» + «terherkommen».
    "aufrecht", "herum", "hinterher", "zufrieden", "zugute", "zunichte",
    "fehl", "frei", "gut", "kaputt", "kennen", "krank", "leer", "still", "übrig",
    # Добавлено 18.08.2026: без них «bereitlegen» и «kleinschneiden» не разбирались на
    # приставку и основу, а значит не могли взять таблицу у своей основы («legen»,
    # «schneiden»). Проверка разбора идёт по справочнику: остаток обязан оказаться
    # документированным глаголом, иначе разбор не принимается.
    "bereit", "klein", "fertig", "hoch", "tief", "wach", "satt", "wett", "wahr",
    "nieder", "heim", "teil", "breit", "schief", "tot", "kurz",
)


# Глаголы, которые ДЕЙСТВИТЕЛЬНО начинаются с «ge-». Список в языке закрытый, поэтому
# всё остальное на «ge-» и «-en» — причастие, а не основа («geordneten», «gearbeiteten»).
_GE_VERBS = frozenset({
    "geben", "gehen", "geraten", "gefallen", "gelingen", "genießen", "gewinnen",
    "gestehen", "gelten", "geschehen", "gewöhnen", "gedeihen", "gehören",
    "gestalten", "genehmigen", "gedenken", "gebären", "gebrauchen", "gehorchen",
})


def split_separable_verb(word: str) -> tuple[str, str]:
    """«klarkommen» → («klar», «kommen»). Не отделяемый — ('', слово).

    Зачем: у отделяемого глагола в личной форме приставка УХОДИТ В КОНЕЦ —
    «ich komme klar», а не «ich klarkomme». Движок таблиц приклеивал окончания к
    целому слову и печатал «ich ankomme», «ich aufstehe», «ich klarkomme» — форм,
    которых в немецком нет. Владелец увидел это 17.08.2026 на «klarkommen».

    Признак жёсткий: слово начинается ОДНОЗНАЧНО отделяемой приставкой, а остаток —
    настоящая глагольная основа (от четырёх букв, на -en/-eln/-ern). Приставки, чья
    отделяемость зависит от значения, исключены списком выше."""
    body = str(word or "").strip()
    low = body.casefold()
    if not low or " " in low or not low.endswith(("en", "eln", "ern")):
        return "", body
    # От самой ДЛИННОЙ приставки к самой короткой: иначе «hineingehen» разберётся как
    # «hin» + «eingehen», а «zusammenarbeiten» как «zu» + «sammenarbeiten».
    candidates = sorted(
        set(_SEPARABLE_PREFIXES) | set(_COMPOUND_SEPARABLE_PREFIXES),
        key=len, reverse=True,
    )
    for prefix in candidates:
        if prefix in _AMBIGUOUS_SEPARABLE_PREFIXES or not low.startswith(prefix):
            continue
        rest = body[len(prefix):]
        if len(rest) < 4 or not rest.casefold().endswith(("en", "eln", "ern")):
            continue
        # Причастие основой быть не может: «abgeordneten» — это существительное «die
        # Abgeordneten», а не «ab» + «geordneten». Настоящих глаголов на «ge-» в языке
        # закрытый десяток, они перечислены; всё остальное на «ge-» + «-en» — причастие.
        low_rest = rest.casefold()
        if low_rest.startswith("ge") and low_rest not in _GE_VERBS:
            continue
        return body[:len(prefix)], rest
    return "", body


# Неотделяемые приставки немецкого. Это ЗАКРЫТЫЙ класс морфем из грамматики, а не
# догадка по хвосту слова: список не растёт и не «дополняется на глаз». Нужен он ровно
# для одного — понять, ЕСТЬ ЛИ у глагола приставка вообще. Отделяется она или нет,
# отвечает справочник напечатанной формой, а не этот список.
_INSEPARABLE_PREFIXES = ("be", "emp", "ent", "er", "ge", "miss", "ver", "zer")


def leading_verb_prefix(word: str) -> str:
    """Приставка, которой начинается глагол, или '' — если её нет или она не опознана.

    Вопрос «отделяемая ли приставка» имеет смысл только там, где приставка есть:
    у «machen» её нет, и строка про отделяемость была бы враньём о морфологии.
    Здесь НЕ решается, отделяется она или нет: решает справочник (см.
    `verb_prefix_separability`). Здесь только «есть, что решать».
    """
    body = str(word or "").strip()
    low = body.casefold()
    if not low or " " in low or not low.endswith(("en", "eln", "ern")):
        return ""
    # Настоящих глаголов на «ge-» в языке закрытый десяток; у них «ge» не приставка,
    # а часть основы. Тот же список стережёт разбор в `split_separable_verb`.
    if low in _GE_VERBS:
        return ""
    candidates = sorted(
        set(_SEPARABLE_PREFIXES) | set(_COMPOUND_SEPARABLE_PREFIXES)
        | set(_AMBIGUOUS_SEPARABLE_PREFIXES) | set(_INSEPARABLE_PREFIXES),
        key=len, reverse=True,
    )
    for prefix in candidates:
        if low.startswith(prefix) and len(low) - len(prefix) >= 4:
            return body[:len(prefix)]
    return ""


def _separability_verdict(prefix: str, reading: dict) -> dict:
    """Вердикт по ОДНОМУ прочтению. Читается, а не вычисляется.

    Порядок свидетелей — от прямого к косвенному:
      1. помета самой статьи «trennbar» / «untrennbar» — прямой ответ источника;
      2. третье лицо Präsens: «bietet an» — два слова, приставка уехала;
      3. Partizip II без «ge-» в начале — подтверждение, что приставка вообще есть
         («begonnen»); с «ge-» («geerntet») это опровержение: «ernten» — не «er+nten».
    """
    present = " ".join(str(reading.get("present") or "").split())
    partizip = " ".join(str(reading.get("partizip2") or "").split())
    label = str(reading.get("label") or "")
    parts = present.split()

    if label == "trennbar" or (len(parts) >= 2 and parts[-1].casefold() == prefix.casefold()):
        if len(parts) < 2:
            return {}
        return {"value": "separable", "present": present, "partizip2": partizip,
                "example": f"er {' '.join(parts[:-1])} … {parts[-1]}"}
    if len(parts) != 1 or not present:
        return {}
    if label != "untrennbar":
        # Пометы нет — нужен второй свидетель, иначе мы бы назвали приставкой то,
        # что ею не является.
        if not partizip or partizip == "—" or partizip.casefold().startswith("ge"):
            return {}
    return {"value": "inseparable", "present": present, "partizip2": partizip,
            "example": f"er {present}"}


def verb_prefix_separability(infinitive: str, *, allow_network: bool = False) -> dict[str, Any]:
    """Отделяемая у глагола приставка или нет — ЧИТАЕТСЯ из справочника.

    Владелец 30.08.2026: «если мы описываем глаголы, то не только возвратный или нет
    нужно рассматривать, но и отделяемая приставка у него или нет. И показывать это
    на примере в том числе». На экране сравнения «anbieten ↔ unterbreiten» этого не
    было ни словом, хотя различаются они в речи именно этим.

    ПО НАПИСАНИЮ ЭТО НЕ РЕШАЕТСЯ, и мы этого не делаем: «unterbringen» приставку
    отделяет, «unterbreiten» — нет, «unter-» у обоих одна. Больше того, одним
    написанием бывают записаны ДВА РАЗНЫХ ГЛАГОЛА: «unterbreiten» — и «подстилать»
    (отделяемый), и «предлагать» (неотделяемый); «umfahren» — «объезжать» и «сбивать».
    Тогда одного ответа не существует, и выбирать за человека мы не будем (правило
    «не решаем за пользователя», 26.08.2026): показываем оба прочтения с их формами.

    Возвращает {} — «не знаем»: справочник молчит, приставки нет, или формы не дают
    однозначного чтения. Пустой ответ считается в `_word_diff_gaps`, а не выдаётся
    за ответ.
    """
    verb = str(infinitive or "").strip()
    if not verb or " " in verb:
        return {}
    # Спрягаемый глагол пишется со строчной; в базе заголовок бывает с заглавной.
    verb = verb[:1].lower() + verb[1:]
    prefix = leading_verb_prefix(verb)
    if not prefix:
        return {}
    try:
        from backend.german_verb_paradigms import verb_readings
        readings = verb_readings(verb, allow_network=allow_network)
    except Exception:
        logging.warning("справочник не ответил про приставку %s", verb, exc_info=True)
        return {}
    if not readings:
        return {}

    variants = [v for v in (_separability_verdict(prefix, r) for r in readings) if v]
    if not variants:
        return {}
    values = {v["value"] for v in variants}
    if len(values) == 1:
        answer = dict(variants[0])
        answer["prefix"] = prefix
        return answer
    # Два разных глагола под одним написанием. Ответ честный: их два.
    return {"value": "both_readings", "prefix": prefix, "readings": variants}


def looks_like_zu_infinitive(word: str) -> bool:
    """«klarzukommen», «anzulehnen» — это zu-инфинитив, а не словарная форма глагола.

    Спрягать такое нельзя: наш движок режет «-en» и печатает «ich klarzukomme,
    du klarzukommst» — форм, которых в языке не существует. Замер 14.08.2026: таких
    заголовков пять (klarzukommen, anzulehnen, auszulaugen, aufzudecken, umzukrempeln),
    и у всех пяти была напечатана выдуманная парадигма.

    Признак: слово начинается отделяемой приставкой, сразу за ней «zu», а дальше
    остаётся настоящая глагольная основа на -en/-eln/-ern. «hinzufügen» под правило не
    попадает: там приставка «hinzu-», и после снятия «zu» остаётся «higen» — не глагол.
    """
    body = str(word or "").strip().lower()
    if not body or " " in body or not body.endswith(("en", "eln", "ern")):
        return False
    # Приставки, которые сами КОНЧАЮТСЯ на «zu»: «hinzufügen», «dazugeben» — обычные
    # глаголы, никакого zu-инфинитива там нет.
    if body.startswith(("hinzu", "dazu", "herzu", "wozu", "darzu")):
        return False
    for prefix in _SEPARABLE_PREFIXES:
        if not body.startswith(prefix + "zu"):
            continue
        rest = body[len(prefix) + 2:]
        # Основа не короче пяти букв. Иначе «zusammenzucken» разбирается как
        # «zusammen» + «zu» + «cken» — обрывок, а не глагол. У всех пяти настоящих
        # случаев основа от шести букв: kommen, lehnen, laugen, decken, krempeln.
        if len(rest) >= 5 and rest.endswith(("en", "eln", "ern")):
            return True
    return False


def strip_zu_infinitive(word: str) -> str:
    """«klarzukommen» → «klarkommen». Пустая строка, если это не zu-инфинитив.

    Снятие безопасно и проверяемо: «zu» между отделяемой приставкой и основой — частица,
    а не часть слова. Этим оно отличается от лемматизации spaCy, которую из пути
    сохранения убрали за откусывание окончаний (Felge→Felg, beibringen→beibring)."""
    if not looks_like_zu_infinitive(word):
        return ""
    body = str(word or "").strip()
    low = body.lower()
    for prefix in _SEPARABLE_PREFIXES:
        if low.startswith(prefix + "zu"):
            rest = body[len(prefix) + 2:]
            if len(rest) >= 5:
                return body[: len(prefix)] + rest
    return ""


# Части речи, которые в немецком пишутся со СТРОЧНОЙ. Существительных здесь нет
# намеренно: они с заглавной всегда.
_LOWERCASE_POS = {"verb", "adjective", "adverb", "adj", "adv",
                  "preposition", "conjunction", "particle", "pronoun", "numeral",
                  # Междометие тоже со строчной: «danke», «schade», «nanu». С заглавной
                  # в немецком пишется ТОЛЬКО существительное, и исключений у правила нет.
                  # Добавлено 18.08.2026, когда служебным словам проставили часть речи и
                  # стало видно, что «Danke» и «Abgemacht» остаются заглавными.
                  "interjection", "participle"}


_INDEFINITE_HEAD_RE = re.compile(r"^(?:ein|eine|einen|einem|einer|eines)\s+", re.I)
_DEFINITE_HEAD_RE = re.compile(r"^((?:der|die|das)\s+)", re.I)


def german_dictionary_headword(word: str | None) -> str:
    """Заголовок словарной статьи: то, что имеет право стоять крупно над карточкой.

    ОДНО правило на весь проект. Раньше каждый класс дефекта чинился отдельным
    скриптом по факту, и класс возвращался: замер 16.08.2026 нашёл zu-инфинитивы и
    неопределённые артикли в заголовках, заведённых уже ПОСЛЕ прошлой уборки.

    1. zu-инфинитив → словарная форма: «klarzukommen» → «klarkommen». «zu» между
       отделяемой приставкой и основой — синтаксическая частица; словарной формы с
       ней нет ни у одного немецкого глагола, а таблицы строятся от заголовка и
       печатали «ich klarzukomme».
    2. Неопределённый артикль у одиночного существительного снимается: «eine Pleite»
       → «Pleite», «die eine Pleite» → «die Pleite». В словарной статье «ein» не
       бывает — статья описывает слово, а не один его экземпляр. Экран же дописывает
       свой артикль по роду, и владелец видел «die eine Pleite».

    ⚠ ФРАЗУ НЕ ТРОГАЕМ. В «eine Pressekonferenz abhalten» артикль принадлежит фразе,
    и снять его значит испортить пример. Признак — после снятия остаётся РОВНО ОДНО
    слово. Проверено на живых данных: из 463 заголовков с «ein» фразами оказались 458.

    ⚠ Определённый артикль остаётся: «die Fahne» — это и есть наш формат заголовка.

    Ничего не выдумывает: не подошло ни одно правило — возвращает как было."""
    text = re.sub(r"\s+", " ", str(word or "").strip())
    if not text:
        return ""

    if " " not in text and looks_like_zu_infinitive(text):
        stripped = strip_zu_infinitive(text)
        if stripped:
            return stripped

    head = _DEFINITE_HEAD_RE.match(text)
    prefix = head.group(1) if head else ""
    rest = text[len(prefix):]
    if _INDEFINITE_HEAD_RE.match(rest):
        bare = _INDEFINITE_HEAD_RE.sub("", rest).strip()
        # Заглавная где-то внутри — признак существительного; длина от трёх букв
        # отсекает мусор вроде «Einer n», где после снятия остаётся «n».
        if bare and " " not in bare and len(bare) >= 3 and any(c.isupper() for c in bare):
            return (prefix + bare).strip()
    return text


def german_headword_case(word: str | None, pos: str | None) -> str:
    """Заголовок немецкого слова в правильном регистре.

    В немецком с заглавной пишутся только существительные. Глагол, прилагательное,
    наречие — со строчной. Но в базе заголовок часто стоит с заглавной: так приходит
    сохранение («Abbuchen», «Akut», «Nahtlos»), особенно когда слово попало в словарь
    из начала предложения. Замер 15.08.2026: 357 карточек — 210 глаголов, 118
    прилагательных, 29 наречий.

    Правило работает ТОЛЬКО когда часть речи названа явно и она не существительное.
    Пустая часть речи — не разрешение: под ней прячутся и существительные, и имена
    собственные. Это тот же урок, что с русскими переводами, где проверка «а это
    существительное?» написала «афины».
    """
    text = str(word or "")
    if not text or not text[:1].isupper():
        return text
    if str(pos or "").strip().lower() not in _LOWERCASE_POS:
        return text
    # Заголовок КАПСОМ («ERNEUERBARE») опускается целиком. Правило «снять заглавную с
    # первой буквы» дало бы «eRNEUERBARE» — поймано сухим прогоном 16.08.2026. Капс в
    # словарной статье не бывает осмысленным: он приходит из текста, где слово было
    # выделено, а не из языка.
    letters = [c for c in text if c.isalpha()]
    if len(letters) > 1 and all(c.isupper() for c in letters):
        return text.lower()
    return text[:1].lower() + text[1:]


def _documented_conjugation(infinitive: str) -> dict[str, Any] | None:
    """Таблица из справочника. Модуль подключается внутри: он ходит в базу и в сеть,
    а этот модуль обязан оставаться чистым для тех, кто зовёт его без базы (тесты,
    оффлайн-скрипты). Справочник молчит — возвращаем None, и таблица считается по
    основе, как раньше."""
    # Тесты в боевую базу не ходят — это правило проекта. Метка та же, что уже стоит
    # в backend/tests/conftest.py; под ней проверяется чистый счёт по основе, а
    # обращение к справочнику покрыто своими тестами на подставном источнике.
    # Скрипты уборки включают справочник явно: VERB_PARADIGM_LOOKUP=1.
    import os
    if os.getenv("SKIP_STARTUP_SCHEMA_BOOTSTRAP") == "1" and not os.getenv("VERB_PARADIGM_LOOKUP"):
        return None
    try:
        from backend.german_verb_paradigms import paradigm_for_verb
        return paradigm_for_verb(infinitive)
    except Exception:
        logging.debug("справочник спряжений недоступен для %s", infinitive, exc_info=True)
        return None


def build_verb_conjugation(
    *,
    word_de: str,
    seed: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Таблица спряжения ИЗ ИСТОЧНИКА или None. Своих форм код не строит.

    Владелец 23.08.2026: «как мы можем просто брать и механически что-то делать, когда
    это касается языка? у нас же есть либо справочник, либо, если справочника нет, нужно
    запрашивать у модели».

    До этого дня здесь стоял счёт от основы: резали инфинитив, приклеивали окончания,
    недостающее брали из `seed` — полей, которые модель приписала к карточке мимоходом и
    которые никто не сверял. На настоящих немецких глаголах чаще всего совпадало, но
    заголовком бывает не глагол, и тогда на экран уходило несуществующее слово: «ich
    boree», «ich aspettiamoe», «ich besagte». Замер 22.08.2026 — 96 таких таблиц.

    Теперь источник один: `german_verb_paradigms.paradigm_for_verb` — своя страница
    Flexion в de.wiktionary, полная форма разговорного усечения, основа составного
    глагола, а когда справочника нет — модель, спрошенная дважды с полным совпадением
    ответов (спрашивает ночь, выдача читает подтверждённое). Не подтвердилось ничем —
    таблицы нет, и глагол считается в отчёте как незакрытая задача. Существительные
    (:141) и прилагательные (:619) живут по этому же правилу с 17.08.2026.

    `seed` остаётся в подписи ради вызывающих, но формы из него БОЛЬШЕ НЕ СТРОЯТСЯ:
    непроверенный ответ модели — не источник.
    """
    inf = _strip_article(word_de)
    if not inf or " " in inf:
        return None
    # Заголовок в форме zu-инфинитива спрягать нельзя — «ich klarzukomme» не существует.
    if looks_like_zu_infinitive(inf):
        return None
    # СПРЯГАЕМЫЙ ГЛАГОЛ ПИШЕТСЯ СО СТРОЧНОЙ. В базе заголовок бывает с заглавной
    # («Aufwachen» — субстантивированный инфинитив), и справочник по такому написанию
    # не найдётся, хотя глагол документирован.
    inf = inf[:1].lower() + inf[1:]
    return _documented_conjugation(inf)


_ADJ_DECLENSION_ENDINGS = ("en", "em", "es", "er", "e")
_ADJ_SUFFIXES = ("ig", "lich", "isch", "bar", "sam", "haft", "los", "voll", "iv", "abel")


def looks_like_declined_adjective(word: str) -> bool:
    """«schlammigen», «winzigen», «beispiellosen» — это склонённые формы, а не словарная.

    Строить от них степени сравнения нельзя: получается «schlammigener» и
    «am schlammigensten» — таких слов нет.

    Признак осторожный, намеренно: требуем И падежное окончание, И под ним настоящий
    прилагательный суффикс. Поэтому «sauber» и «teuer» под правило не попадают (снимешь
    «er» — останется «saub», это не суффикс), а «richtig» не попадает вовсе, потому что
    падежного окончания на нём нет. Цена осторожности — пропустим склонённое
    прилагательное без такого суффикса; пропустить безопаснее, чем испортить хорошее.
    """
    body = str(word or "").strip().lower()
    if not body or " " in body:
        return False
    for ending in _ADJ_DECLENSION_ENDINGS:
        if not body.endswith(ending):
            continue
        stem = body[: -len(ending)]
        if len(stem) >= 4 and stem.endswith(_ADJ_SUFFIXES):
            return True
    return False


def build_adjective_comparison(
    *,
    word_de: str,
    comparative: str | None = None,
    superlative: str | None = None,
) -> dict[str, Any] | None:
    """Three degrees of comparison. Uses LLM forms when present, else a regular
    -er / am -sten default (irregular adjectives must come from the seed)."""
    positive = _strip_article(word_de)
    if not positive or " " in positive:
        return None
    # Лучше НЕ показать таблицу, чем показать выдуманную: от склонённой формы степени
    # сравнения выходят несуществующими («schlammigen» → «schlammigener»,
    # «am schlammigensten»). Замер 14.08.2026: 11 таких заголовков среди размеченных
    # прилагательных, и у каждого печаталась своя выдуманная лесенка.
    if looks_like_declined_adjective(positive):
        return None
    # Прилагательное в степенях сравнения тоже со строчной: «Nahtlos» в заголовке —
    # след сохранения, а не немецкая орфография. См. пояснение у спряжения.
    positive = positive[:1].lower() + positive[1:]
    # ТОЛЬКО СПРАВОЧНИК. Прежнее дописывание окончания отсюда УДАЛЕНО: оно давало
    # «gut → guter / am gutesten», «alt → alter / am altesten», «hoch → hocher».
    # Умлаут и супплетивные формы правилом не выводятся в принципе.
    documented = _documented_degrees(positive)
    if not documented:
        return None
    comp = str(documented.get("comparative") or "").strip()
    sup = str(documented.get("superlative") or "").strip()
    if not comp or not sup:
        return None
    return {"positive": str(documented.get("positive") or positive),
            "comparative": comp, "superlative": sup,
            "source": documented.get("source")}


# ── Word formation (compound / affix breakdown) ─────────────────────────────────
def build_grammar_tables(item: dict[str, Any] | None) -> dict[str, Any]:
    """Top-level dispatcher: from a looked-up dictionary `item` (the existing
    /api/webapp/dictionary payload) produce the deep, POS-aware grammar tables.
    Only the section matching the part of speech is populated."""
    if not item or not isinstance(item, dict):
        return {}
    pos = str(item.get("part_of_speech") or "").strip().lower()
    forms = item.get("forms") if isinstance(item.get("forms"), dict) else {}
    word_de = str(item.get("word_de") or "").strip()
    out: dict[str, Any] = {"part_of_speech": pos}

    if pos == "noun":
        table = build_noun_declension(
            word_de=word_de,
            article=item.get("article"),
            plural=forms.get("plural"),
            genitive=forms.get("genitive"),
        )
        if table:
            out["declension"] = table
    elif pos == "verb":
        table = build_verb_conjugation(
            word_de=word_de,
            seed={
                "present_2sg": forms.get("present_2sg"),
                "present_3sg": forms.get("present_3sg"),
                "praeteritum": forms.get("praeteritum"),
                "perfekt": forms.get("perfekt"),
                "konjunktiv2": forms.get("konjunktiv2"),
                "imperative_sg": forms.get("imperative_sg"),
                "is_separable": item.get("is_separable"),
            },
        )
        if table:
            out["conjugation"] = table
    elif pos in ("adjective", "adverb"):
        table = build_adjective_comparison(
            word_de=word_de,
            comparative=forms.get("comparative"),
            superlative=forms.get("superlative"),
        )
        if table:
            out["comparison"] = table

    return out


# ── Ячейка таблицы → словоформа ────────────────────────────────────────────────
#
# ⛔ ЗДЕСЬ РОДИЛСЯ ДЕФЕКТ «ging → ausgehen». НЕ ВОЗВРАЩАТЬ НАРЕЗКУ ПО ПРОБЕЛАМ.
#
# До 01.09.2026 ячейку парадигмы резали на слова, и КАЖДОЕ слово становилось указателем
# на лемму (`scripts/dict_units_paradigm_surfaces.py`, `scripts/dict_units_wikt_forms.py`).
# У отделяемого глагола в ячейке стоит «ging aus» — в указатели уезжали и «ging», и «aus».
#
# Что это дало на живой базе (замер 01.09.2026, `scripts/dict_units_forms_confirm.py`):
#     «auf»   вело к 34 глаголам,  «ging» — к 15,  «stellt» — к 18;
#     быстрый словарь на «ging» отвечал «выходить» (ausgehen), а базового «gehen»
#     в ответе не было вообще — его вообще нет у нас глаголом, только «das Gehen».
#
# Правило ниже — не догадка, а то, КАК ФОРМА НАПЕЧАТАНА в источнике. Так же печатает
# dict.cc («eingehen | ging ein | eingegangen») и Wiktionary на странице Flexion.
_CELL_SPACE_RE = re.compile(r"\s+")

# Служебные слова: они стоят в ячейке, но частью формы не являются. Артикли —
# «des Studenten», вспомогательные глаголы — «hat angefangen».
_CELL_SERVICE_WORDS = frozenset({
    "der", "die", "das", "den", "dem", "des", "ein", "eine", "einen", "einem", "einer", "eines",
    "hat", "habe", "hast", "haben", "habt", "ist", "bin", "bist", "sind", "seid", "war", "waren",
    "wird", "werde", "wirst", "werden", "werdet", "sich", "zu", "nicht", "wurde", "wurden",
})
_CELL_MIN_LEN = 3  # «am», «im» и прочие обрывки формой не считаются


def form_token_of_cell(cell: str) -> str:
    """Словоформа из ОДНОЙ ячейки таблицы — или пустая строка, если её там нет.

        1. выбрасываем служебные слова (артикль, вспомогательный глагол);
        2. осталось РОВНО ОДНО слово — это форма;
        3. осталось два и больше — форма разнесена по словам («ging aus»), и брать
           из неё отдельное слово НЕЛЬЗЯ: «ging» — форма «gehen», а не «ausgehen».

    Проверено на живых ячейках 01.09.2026:
        «des Studenten»   → studenten     «ging aus»    → (пусто)
        «hat angefangen»  → angefangen    «geht aus»    → (пусто)
        «ist ausgegangen» → ausgegangen   «am ältesten» → ältesten
    """
    tokens = []
    for token in _CELL_SPACE_RE.split(str(cell or "").strip()):
        token = token.strip(" ,;.—-").casefold()
        if len(token) < _CELL_MIN_LEN or token in _CELL_SERVICE_WORDS:
            continue
        tokens.append(token)
    return tokens[0] if len(tokens) == 1 else ""
