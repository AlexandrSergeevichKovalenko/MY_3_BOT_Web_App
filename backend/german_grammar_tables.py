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


def build_noun_declension(
    *,
    word_de: str,
    article: str | None,
    plural: str | None = None,
    genitive: str | None = None,
) -> dict[str, Any] | None:
    """Full 4-case × singular/plural declension table for a noun, with articles.

    Returns None when the gender can't be determined (no usable article)."""
    gender = gender_from_article(article)
    if not gender:
        return None
    noun = _strip_article(word_de)
    if not noun:
        return None

    plural_noun = _strip_article(plural) if plural else ""
    gen_sg = _genitive_singular(noun, gender, genitive)

    sg_forms = {"nom": noun, "akk": noun, "dat": noun, "gen": gen_sg}
    rows = []
    for case in _CASES:
        sg_art = _ART_SG[gender][case]
        row = {
            "case": case,
            "label": _CASE_LABELS_RU[case],
            "singular": f"{sg_art} {sg_forms[case]}".strip(),
        }
        if plural_noun:
            pl_noun = _dative_plural(plural_noun) if case == "dat" else plural_noun
            row["plural"] = f"{_ART_PL[case]} {pl_noun}".strip()
        rows.append(row)

    return {
        "gender": gender,
        "article": _ART_SG[gender]["nom"],
        "singular": noun,
        "plural": plural_noun or None,
        "has_plural": bool(plural_noun),
        "rows": rows,
    }


# ── Verb conjugation ────────────────────────────────────────────────────────────
_PRON = ("ich", "du", "er/sie/es", "wir", "ihr", "sie/Sie")


def _verb_stem(infinitive: str) -> tuple[str, str]:
    """Return (stem, plural_ending). Most verbs end in -en (stem = drop -en,
    plural ending -en); -eln/-ern verbs drop only -n (plural ending -n)."""
    inf = str(infinitive or "").strip()
    if inf.endswith("eln") or inf.endswith("ern"):
        return inf[:-1], "n"
    if inf.endswith("en"):
        return inf[:-2], "en"
    if inf.endswith("n"):
        return inf[:-1], "n"
    return inf, "en"


def _present_du_er(stem: str) -> tuple[str, str]:
    """Regular 2sg/3sg present endings with the -est/-et expansion after t/d and
    the -t (not -st) shortcut after a sibilant stem."""
    low = stem.lower()
    if low.endswith(("t", "d")) or low.endswith(("ffn", "chn", "tm")):
        return stem + "est", stem + "et"
    if low.endswith(("s", "ß", "z", "x")):
        return stem + "t", stem + "t"  # du gives just -t after a sibilant
    return stem + "st", stem + "t"


# Отделяемые приставки: с них начинается zu-инфинитив «klar-zu-kommen».
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


def _detach(form: str, prefix: str) -> str:
    """Личная форма отделяемого глагола: приставка становится отдельным словом в конце.

    Умеет и починить уже склеенную форму («klarkommst» → «kommst klar»): именно
    такие лежат в разборах, собранных до этой правки, и приходят в `seed`."""
    if not prefix:
        return form
    text = str(form or "").strip()
    if not text:
        return text
    low, low_prefix = text.casefold(), prefix.casefold()
    if low.startswith(low_prefix) and len(text) > len(prefix):
        text = text[len(prefix):]
    elif low.endswith(" " + low_prefix):
        return text  # приставка уже отделена
    return f"{text} {prefix}".strip()


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
                  "preposition", "conjunction", "particle", "pronoun", "numeral"}


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
    """Conjugation tables (Präsens, Präteritum, Perfekt, Konjunktiv II, Imperativ)
    built from the infinitive plus the LLM `seed` for the irregular parts:
        present_2sg, present_3sg, praeteritum (ich/er form), perfekt
        ("hat/ist <PII>"), konjunktiv2 (ich form), imperative_sg.
    Regular cells are computed; seed cells override when present."""
    inf = _strip_article(word_de)
    if not inf or " " in inf:
        return None
    # СНАЧАЛА СПРАВОЧНИК. У de.wiktionary для глагола напечатана полная таблица, и
    # формы берутся из неё дословно — включая то, что правилом не выводится: «du hältst»
    # против «du arbeitest», «du liest», и место отделяемой приставки («ich komme klar»).
    # Владелец 17.08.2026: «это лингвистическое приложение, тут не должно быть
    # придуманного». Ниже — прежний счёт от основы; он остаётся только для глаголов,
    # которых в справочнике нет, и живёт под своим именем в поле `source`.
    documented = _documented_conjugation(inf)
    if documented:
        return documented
    # Лучше НЕ показать таблицу, чем показать выдуманную. Заголовок в форме zu-инфинитива
    # спрягать нельзя — получается «ich klarzukomme», чего в языке нет. Раньше движок брал
    # любой одиночный токен за инфинитив без единой проверки.
    if looks_like_zu_infinitive(inf):
        return None
    # СПРЯГАЕМЫЙ ГЛАГОЛ ПИШЕТСЯ СО СТРОЧНОЙ — всегда, кроме начала предложения.
    # Заголовок в базе может стоять с заглавной: «Aufwachen», «Hineingehen» — это
    # субстантивированный инфинитив, и в таком виде он приходит из сохранения. Движок
    # приклеивал окончания как есть, и человек читал «ich Gehe», «ich Hineingehe»,
    # «ich Aufwache» — форм, которых в языке нет. Замер 15.08.2026 воспроизведён на
    # gehen/Gehen, arbeiten/Arbeiten, Aufwachen, Hineingehen.
    inf = inf[:1].lower() + inf[1:]
    seed = seed or {}
    # Отделяемая приставка: спрягаем ОСНОВНОЙ глагол, а приставку ставим в конец личной
    # формы. Разбор сам сообщает `is_separable`; когда он молчит, решает список
    # однозначно отделяемых приставок. Слитная форма «ich ankomme» — не немецкий язык.
    prefix, base_verb = split_separable_verb(inf)
    if seed.get("is_separable") is False:
        prefix, base_verb = "", inf
    stem, pl_end = _verb_stem(base_verb)
    reg_du, reg_er = _present_du_er(stem)

    du = str(seed.get("present_2sg") or "").strip() or reg_du
    er = str(seed.get("present_3sg") or "").strip() or reg_er
    # If the LLM gave only the 3sg of a vowel-changing strong verb, mirror its
    # stem change into the du-form when we otherwise only have the regular one.
    if seed.get("present_3sg") and not seed.get("present_2sg") and er.lower().endswith("t"):
        cand = er[:-1]
        if not cand.lower().endswith(("s", "ß", "z", "x")):
            du = cand + "st"

    ihr_present = stem + ("et" if stem.lower().endswith(("t", "d")) else "t")
    praesens = dict(zip(_PRON, [
        _detach(stem + "e", prefix), _detach(du, prefix), _detach(er, prefix),
        _detach(stem + pl_end, prefix), _detach(ihr_present, prefix),
        _detach(stem + pl_end, prefix),
    ]))

    tables: dict[str, Any] = {"praesens": praesens}

    # Präteritum: expand from the ich/er base form.
    praet_base = _strip_article(str(seed.get("praeteritum") or "").strip())
    if praet_base:
        tables["praeteritum"] = _expand_with_prefix(praet_base, prefix)

    # Konjunktiv II: the ich form already ends in -e for strong verbs (ginge),
    # weak verbs fall back to "würde + Infinitiv".
    konj2_base = _strip_article(str(seed.get("konjunktiv2") or "").strip())
    if konj2_base and konj2_base.lower().startswith("würde"):
        tables["konjunktiv2"] = dict(zip(_PRON, [
            "würde " + inf, "würdest " + inf, "würde " + inf,
            "würden " + inf, "würdet " + inf, "würden " + inf,
        ]))
    elif konj2_base:
        tables["konjunktiv2"] = _expand_with_prefix(konj2_base, prefix)

    # Perfekt: conjugate the parsed auxiliary; keep the participle fixed.
    aux, participle = _parse_perfekt(str(seed.get("perfekt") or ""), inf)
    if participle:
        forms = _AUX_FORMS.get(aux)
        if forms:
            tables["perfekt"] = dict(zip(
                _PRON, [f"{a} {participle}" for a in forms]
            ))
            tables["auxiliary"] = aux
            tables["partizip2"] = participle

    # Imperativ (du / ihr / Sie).
    imp_du = str(seed.get("imperative_sg") or "").strip() or stem
    tables["imperativ"] = {
        "du": _detach(imp_du, prefix),
        "ihr": _detach(ihr_present, prefix),
        # Вежливая форма: приставка уходит за «Sie» — «kommen Sie klar».
        "Sie": f"{stem + pl_end} Sie{(' ' + prefix) if prefix else ''}",
    }

    tables["infinitive"] = inf
    tables["is_separable"] = seed.get("is_separable")
    return tables


def _expand_with_prefix(base: str, prefix: str) -> dict[str, str]:
    """Развернуть форму ich/er на все шесть лиц, помня про отделяемую приставку.

    Разбор присылает основу уже с приставкой — иногда слитно («klarkam»), иногда
    отдельным словом («kam klar»). Приклеивать окончания к такой строке нельзя: так
    получались «kam klarst» и «kam klaren». Поэтому приставку снимаем, разворачиваем
    чистую основу и возвращаем приставку на её место — в конец."""
    text = str(base or "").strip()
    if prefix:
        low, low_prefix = text.casefold(), prefix.casefold()
        if low.endswith(" " + low_prefix):
            text = text[: -(len(prefix) + 1)].strip()
        elif low.startswith(low_prefix) and len(text) > len(prefix):
            text = text[len(prefix):].strip()
    expanded = _expand_from_base(text, ends_in_e_default=True)
    if not prefix:
        return expanded
    return {person: f"{form} {prefix}".strip() for person, form in expanded.items()}


def _expand_from_base(base: str, *, ends_in_e_default: bool) -> dict[str, str]:
    """Expand a single ich/er stem (Präteritum or Konjunktiv II) into all six
    persons. Strong bases ('ging') take -en/-st/-t; bases already ending in -e
    ('machte', 'ginge') take -n/-st/-t."""
    low = base.lower()
    ends_e = low.endswith("e")
    du = base + ("est" if low.endswith(("t", "d")) and not ends_e else "st")
    if low.endswith(("s", "ß", "z", "x")) and not ends_e:
        du = base + "t"
    pl = base + ("n" if ends_e else "en")
    ihr = base + ("t" if ends_e or not low.endswith(("t", "d")) else "et")
    return dict(zip(_PRON, [base, du, base, pl, ihr, pl]))


_AUX_FORMS = {
    "haben": ("habe", "hast", "hat", "haben", "habt", "haben"),
    "sein": ("bin", "bist", "ist", "sind", "seid", "sind"),
}


def _parse_perfekt(perfekt: str, infinitive: str) -> tuple[str, str]:
    """From an LLM 'hat gemacht' / 'ist gegangen' / 'haben gemacht' string return
    (auxiliary_infinitive, participle). Defaults to 'haben' when only a participle
    is present."""
    text = str(perfekt or "").strip()
    if not text:
        return "haben", ""
    tokens = text.split()
    aux = "haben"
    rest = tokens
    head = tokens[0].lower()
    if head in ("hat", "habe", "hast", "haben", "habt"):
        aux, rest = "haben", tokens[1:]
    elif head in ("ist", "bin", "bist", "sind", "seid"):
        aux, rest = "sein", tokens[1:]
    participle = " ".join(rest).strip()
    return aux, participle


# ── Adjective comparison ────────────────────────────────────────────────────────
# Окончания, которые прилагательное получает при склонении, и суффиксы, по которым
# видно, что перед нами именно прилагательное, а не существительное на -e.
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
    comp = str(comparative or "").strip()
    sup = str(superlative or "").strip()
    if not comp:
        comp = positive + "er"
    if not sup:
        stem = positive
        # Было `'sten' if … else 'sten'` — обе ветки одинаковые, то есть проверка стояла,
        # но ничего не делала: «alt» давало «am altsten» вместо «am ältesten», «hart» —
        # «am hartsten». После t/d/s/ß/z/x положено «-esten».
        sup = f"am {stem}{'esten' if stem.lower().endswith(('t', 'd', 's', 'ß', 'z', 'x')) else 'sten'}"
    elif not sup.lower().startswith("am "):
        sup = "am " + sup
    return {"positive": positive, "comparative": comp, "superlative": sup}


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
