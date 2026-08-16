"""
Artikel Sprint — theme noun-bank filler.

For a theme it walks EVERY subtopic, asks GPT for nouns (word + article +
meaning_ru + plural + difficulty), then a second LLM verifies each article is
correct AND unambiguous (rejects der/die-See type words), dedups, and inserts
verified rows. Mirrors our other two-model quality gates. Sync (call via thread);
each LLM call runs through asyncio.run, the proven pattern in this codebase.
"""
from __future__ import annotations

import asyncio
import logging
import os


def _run(coro):
    return asyncio.run(coro)


# ── Deterministic German gender guard ─────────────────────────────────────────
# High-confidence rules (near-zero exceptions) used to VALIDATE/CORRECT the LLM's
# article. Compound nouns take the gender of their LAST element; some suffixes are
# decisive. Only confident matches override — otherwise we trust the verifier.
_HEAD_GENDER: dict[str, str] = {
    # der
    "bruch": "der", "riss": "der", "raum": "der", "saal": "der", "schmerz": "der",
    "infarkt": "der", "erguss": "der", "pfleger": "der", "arzt": "der", "mann": "der",
    "stoff": "der", "druck": "der", "lauf": "der", "fall": "der", "gang": "der",
    "schlag": "der", "knochen": "der", "muskel": "der", "nerv": "der", "finger": "der",
    "ring": "der", "kasten": "der", "topf": "der", "tisch": "der", "schrank": "der",
    # das
    "gerät": "das", "zimmer": "das", "gefühl": "das", "mittel": "das", "fieber": "das",
    "organ": "das", "system": "das", "gewebe": "das", "herz": "das", "hirn": "das",
    "bein": "das", "blut": "das", "haus": "das", "buch": "das", "glas": "das",
    "band": "das",  # das Band (ribbon) — note: only as compound head it's ambiguous; kept out below
    # die
    "klinik": "die", "säule": "die", "arterie": "die", "vene": "die", "haut": "die",
    "zelle": "die", "drüse": "die", "niere": "die", "lunge": "die", "leber": "die",
    "spritze": "die", "tablette": "die", "salbe": "die", "wunde": "die", "narbe": "die",
    "ader": "die", "rippe": "die", "schulter": "die", "hand": "die", "nase": "die",
}
# "band" is genuinely ambiguous (der/die/das) → don't auto-decide on it.
_HEAD_GENDER.pop("band", None)
# add a few der-heads that are also -ung exceptions, so compounds resolve correctly
_HEAD_GENDER.update({"sprung": "der", "schwung": "der"})

# Wirtschaft & Geld compound heads (near-zero exceptions). These fix misses like
# "die Börsenwert" — der Wert compounds are masculine. Head-suffix match only, so a
# bare head word never triggers; the theme is full of these (Marktwert, Zinssatz…).
_HEAD_GENDER.update({
    # der
    "wert": "der", "preis": "der", "markt": "der", "kurs": "der", "zins": "der",
    "satz": "der", "betrag": "der", "gewinn": "der", "verlust": "der", "umsatz": "der",
    "kredit": "der", "haushalt": "der", "fonds": "der", "vertrag": "der",
    # das
    "geld": "das", "konto": "das", "kapital": "das", "vermögen": "das",
    "einkommen": "das", "darlehen": "das", "guthaben": "das", "wachstum": "das",
    # die
    "bank": "die", "aktie": "die", "rente": "die", "steuer": "die", "bilanz": "die",
})

# Only LOW-EXCEPTION derivational suffixes (dropped the risky -ur/-ik/-chen/-lein
# which have native root counter-examples: Flur, Kuchen, Knochen, …).
# NB: use -tion/-sion, NOT bare -ion — the latter wrongly claims die for Greek
# neuters (das Stadion, das Ganglion) and anglicisms (der Champion, der Spion).
_DIE_SUFFIXES = ("ung", "heit", "keit", "schaft", "tion", "sion", "tät", "ität", "ie", "enz", "anz")
_DER_SUFFIXES = ("ling", "ismus")
# Words that match a suffix pattern but DON'T follow the rule (root nouns).
_SUFFIX_EXCEPTIONS = {"sprung", "schwung", "dung", "schwung"}

# Two-gender / meaning-dependent nouns — ambiguous article → must NOT be in the
# bank at all (the article isn't decidable). Matched on the BARE word only.
_AMBIGUOUS_NOUNS = {
    "flur", "see", "band", "steuer", "tor", "leiter", "kiefer", "bauer", "heide",
    "mast", "otter", "golf", "erbe", "gehalt", "kunde", "hut", "bund", "verdienst",
    "schild", "moment", "teil",
    # das Pik — карточная масть, der Pik — тайная неприязнь («einen Pik auf jemanden
    # haben»). У масти на странице справочника нет блока с родом, поэтому аудит видел
    # только der и предлагал «исправить» верное das.
    "pik",
    # der Service — обслуживание, das Service — сервиз (посуда). Справочник знает оба
    # рода для первого смысла, но выбор зависит от того, о чём слово.
    "service",
}

# Nominalized adjectives / participles that denote a PERSON — their article follows
# the person's natural gender (der/die Vorsitzende, der/die Angestellte) and they
# decline like adjectives, not as plain nouns. There is no single decidable article,
# so they must NOT live in a der/die/das bank. Matched on the word's TAIL so compounds
# (Vorstandsvorsitzende, Bundestagsabgeordnete, Polizeibeamte, …) are caught too.
_PERSON_ADJ_NOUN_TAILS = (
    "vorsitzende", "angestellte", "vorgesetzte", "abgeordnete", "auszubildende",
    "studierende", "reisende", "verwandte", "bekannte", "verlobte", "jugendliche",
    "erwachsene", "angehörige", "delegierte", "gesandte", "gefangene", "verletzte",
    "obdachlose", "freiwillige", "deutsche", "beamte",
)


def is_nominalized_person_adjective(word: str) -> bool:
    """True for person-denoting nominalized adjectives/participles whose article is
    not decidable (der/die both valid by natural gender), e.g. Vorstandsvorsitzende."""
    w = str(word or "").strip().lower()
    return any(w == t or w.endswith(t) for t in _PERSON_ADJ_NOUN_TAILS)


def is_inflected_form_of_another_word(word: str) -> bool:
    """True для формы ЧУЖОГО слова: «Bänder» — множественное от «das Band», и вопрос
    «der/die/das?» на ней бессмыслен, спрашивать надо про само слово.

    Слова, которые живут ТОЛЬКО во множественном («die Eltern», «die Masern»,
    «die Kosten»), сюда не попадают: у них множественное и есть словарная форма,
    ответ «die» верен и учить его правильно."""
    try:
        from backend.german_surface import PL, german_surface
    except Exception:
        return False
    bare = str(word or "").strip()
    verdict = german_surface(bare)
    if verdict["number"] != PL or verdict["confidence"] != "high":
        return False
    lemma = str(verdict.get("lemma") or "")
    return bool(lemma) and lemma.casefold() != bare.casefold()


def is_ambiguous_noun(word: str) -> bool:
    w = str(word or "").strip().lower()
    if w in _AMBIGUOUS_NOUNS or is_nominalized_person_adjective(w):
        return True
    return is_inflected_form_of_another_word(word)


def apply_reference_plurals(rows: list[dict]) -> int:
    """Поставить строкам множественное из справочника — ДЛЯ ИХ АРТИКЛЯ. → сколько заменено.

    Форма множественного принадлежит паре «слово + артикль», а не написанию: у der Band
    это Bände, у das Band — Bänder, у die Band — Bands. Модель этого не различает: она
    не знает, какой смысл взяли, и выдаёт одну форму на написание — на двуродовых словах
    промах гарантирован. Поэтому справочник главнее, модель остаётся подстраховкой.

    Пачкой, а не по слову: иначе на раунд заливки уходит полторы сотни запросов.
    Справочник молчит (сеть, 429) — оставляем что было; это «нет ответа», а не «нет формы».
    """
    words = sorted({str(r.get("word") or "").strip() for r in rows if r.get("word")})
    if not words:
        return 0
    try:
        from backend.article_wiktionary_ref import plurals_by_article_for_titles
        by_title = plurals_by_article_for_titles(words) or {}
    except Exception:
        logging.warning("plural: справочник недоступен, оставляем формы от модели", exc_info=True)
        return 0
    fixed = 0
    for r in rows:
        word = str(r.get("word") or "").strip()
        article = str(r.get("article") or "").strip().lower()
        plural = ((by_title.get(word) or {}).get(article) or "").strip()
        if plural and plural != str(r.get("plural") or "").strip():
            r["plural"] = plural
            fixed += 1
    return fixed


def strong_gender(word: str) -> str | None:
    """Return der/die/das if a HIGH-confidence rule decides it, else None.
    Conservative: compound-head map first, then only low-exception suffixes with a
    real stem; never fires for ambiguous/exception roots."""
    w = str(word or "").strip().lower()
    if len(w) < 4 or w in _AMBIGUOUS_NOUNS or w in _SUFFIX_EXCEPTIONS:
        return None
    # 0) СПРАВОЧНИК: род самого слова из Wiktionary, иначе правило композита по всем
    # 19k родов кэша. Раньше здесь работал только словарь голов из ~60 записей ниже —
    # в нём нет ни Kurs, ни Beet, ни Strauch, поэтому в банк уехали «die Wechselkurs»,
    # «der Rosenbeet», «die Haselnussstrauch». У Wechselkurs правильный род в кэше БЫЛ.
    # Сети здесь нет: функция зовётся и на выдаче карточки (resolve_article).
    try:
        from backend.article_authority import authoritative_article
        verdict, _source = authoritative_article(word, allow_network=False)
        if verdict:
            return verdict
    except Exception:
        logging.warning("strong_gender: справочник недоступен для %s", word, exc_info=True)
    # 1) compound head (longest matching head wins) — very reliable
    best = None
    for head, g in _HEAD_GENDER.items():
        if w.endswith(head) and len(w) > len(head) + 1:
            if best is None or len(head) > best[0]:
                best = (len(head), g)
    if best:
        return best[1]
    # 2) decisive suffixes — require a real stem (>= 3 chars before the suffix)
    for suf in _DIE_SUFFIXES:
        if w.endswith(suf) and len(w) - len(suf) >= 3:
            return "die"
    for suf in _DER_SUFFIXES:
        if w.endswith(suf) and len(w) - len(suf) >= 3:
            return "der"
    return None


def resolve_article(word: str, stored: str) -> str:
    """Authoritative der/die/das for a noun, used at SERVE/GRADE time.

    A high-confidence deterministic signal (compound head like -wert/-preis, or a
    decisive suffix) OVERRIDES the stored article, so a bad bank row — the classic
    «die Börsenwert» (der Wert → der Börsenwert) — is corrected on the fly without a
    migration. `strong_gender` is conservative (never fires on ambiguous/exception
    roots), so this only flips genuine, near-certain mistakes; otherwise the stored
    article is trusted. Returns lowercased der/die/das (or the best available).

    У формы множественного числа артикль может быть только «die», поэтому род леммы
    ей не навязывается: «die Bänder» — правильно, «das Bänder» (род от «das Band») —
    нет. Ровно из-за такой подстановки в словаре и появилось «das Probleme»."""
    st = str(stored or "").strip().lower()
    try:
        from backend.german_surface import PL, german_surface
        _v = german_surface(word)
        if _v["number"] == PL and _v["confidence"] == "high":
            return "die"
    except Exception:
        logging.debug("resolve_article: опознание числа недоступно для %s", word, exc_info=True)
    sg = strong_gender(word)
    if sg and st in ("der", "die", "das") and sg != st:
        return sg
    if st in ("der", "die", "das"):
        return st
    return sg or st


def recheck_theme(theme_key: str) -> dict:
    """Apply the deterministic gender guard to already-stored rows; fix mismatches.
    Returns {"checked": n, "fixed": m, "examples": [...]}."""
    from backend.database import (
        list_article_sprint_rows, update_article_sprint_article, retire_article_sprint_noun,
    )
    rows = list_article_sprint_rows(theme_key)
    fixed = 0
    retired = 0
    examples: list[str] = []
    for r in rows:
        w = r["word"]
        if is_ambiguous_noun(w):
            retire_article_sprint_noun(r["id"])
            retired += 1
            if len(examples) < 20:
                why = "person-adj" if is_nominalized_person_adjective(w) else "ambiguous"
                examples.append(f"⊘ retired ({why}): {w}")
            continue
        hint = strong_gender(w)
        if hint and hint != str(r["article"]).lower():
            update_article_sprint_article(r["id"], hint)
            fixed += 1
            if len(examples) < 20:
                examples.append(f"{r['article']} → {hint} {w}")
    return {"checked": len(rows), "fixed": fixed, "retired": retired, "examples": examples}


_AVOID_EXISTING_CAP = 250
_AVOID_RETIRED_CAP = 150
# Докуда отказ стража считается спорным и уезжает владельцу в разбор, а не в стоп-лист.
# Порог тот же, что у дневной рассылки (RETIRE_REVIEW_MAX_RANK): иначе карантин копил бы
# слова, которые рассылка не показывает, и они лежали бы там мёртвым грузом.
QUARANTINE_MAX_RANK = max(1, int((os.getenv("RETIRE_REVIEW_MAX_RANK") or "60000").strip() or "60000"))

# Через фильтр проходит примерно каждое третье слово, поэтому просим втрое больше, чем
# осталось добрать. Меньше — отсев съест заказ, подтема не закроет нехватку, и придётся
# идти в следующую: лишний вызов, а вместе с ним ЗАНОВО весь постоянный кусок запроса.
_ASK_OVERSHOOT = 3
_ASK_MIN = 10


def _ask_count(remaining: int, ceiling: int) -> int:
    """Сколько слов просить у модели на одну подтему.

    Считаем от ВСЕЙ нехватки, а не от «нехватка, делённая на оставшиеся подтемы».
    Делить было ошибкой: заказ размазывался тонко, подтем требовалось больше, и на
    каждый лишний вызов заново уходил постоянный кусок запроса — а он тяжёлый, в нём
    список «не предлагай» на несколько сотен слов. Слов на выходе столько же, вызовов
    вдвое больше: чистый проигрыш.

    Здесь заказ НИКОГДА не превышает прежний потолок и уменьшается только тогда, когда
    полная пачка заведомо перелетит нехватку: при трёх недостающих словах платить за
    тридцать (и за их проверку) незачем. Число вызовов при этом не растёт."""
    if remaining <= 0:
        return 0
    return max(_ASK_MIN, min(int(ceiling), int(remaining) * _ASK_OVERSHOOT))


# Причины из фильтра — внутренние формулировки; наружу идёт человеческий ярлык.
# Правило простое: читатель отчёта должен понять, ЧТО не так со словом, не заглядывая в код.
_REASON_LABELS = (
    ("страж сомневается", "спорные — придут тебе на разбор"),
    ("уже есть в другой теме", "это слово уже стоит в другой теме"),
    ("уже есть в теме", "это слово в теме уже есть"),
    ("такой смысл в теме уже есть", "такой же смысл в теме уже есть"),
    ("нужно второе мнение", "редкое, не для повседневной речи"),
    ("третье производное", "третье слово от того же корня"),
    ("снято с показа", "снимали раньше — не возвращаем"),
    ("форма множественного числа", "это множественное число — нужно единственное"),
    ("двуродовое", "два рода у одного слова — артикль не определить"),
    ("артикль не подтверждён", "артикль не подтвердился при проверке"),
    ("второе мнение не получено", "модель не ответила — отложили до следующего раза"),
    ("пустое слово", "модель вернула пустое слово"),
)


def _reason_bucket(reason: str) -> str:
    low = str(reason or "").strip().lower()
    for needle, label in _REASON_LABELS:
        if needle in low:
            return label
    return "прочее"


def fill_theme(theme_key: str, *, max_to_add: int | None = None, per_subtopic: int = 30) -> dict:
    """Generate+verify+insert nouns for `theme_key`.
    max_to_add: cap how many NEW words to add this run (None → up to target).
    Returns stats dict."""
    from backend.article_sprint_themes import article_sprint_themes
    from backend.openai_manager import run_article_noun_gen, run_article_verify
    from backend.database import (
        ensure_article_sprint_schema, count_article_sprint_nouns,
        insert_article_sprint_nouns, list_article_sprint_words,
        list_article_sprint_words_all_themes,
        list_article_sprint_meanings, list_retired_article_words,
        list_article_word_blacklist, blacklist_article_words,
        list_retired_article_words_for_prompt, quarantine_article_sprint_nouns,
    )
    from backend.article_word_gate import (
        check_word, head_word, judge_everyday_words, EverydayJudgeUnavailable, word_rank,
    )

    theme = next((t for t in article_sprint_themes() if t["key"] == theme_key), None)
    if not theme:
        return {"error": "unknown_theme", "theme": theme_key}
    ensure_article_sprint_schema()

    target = int(theme["target_count"])
    have = count_article_sprint_nouns(theme_key, verified_only=True)
    cap = int(max_to_add) if max_to_add else max(0, target - have)
    if cap <= 0:
        return {"theme": theme_key, "added": 0, "rejected": 0,
                "final_verified": have, "target": target, "note": "already at target"}

    existing = {w.lower() for w in list_article_sprint_words(theme_key)}
    # Слово живёт в ОДНОЙ теме. Раньше проверка «уже есть» смотрела только свою тему,
    # и слово из «Одежды» ложилось ещё и в «Уборку»: 1 309 слов оказались размножены по
    # темам, 2 133 карточки лишние. Держим отдельно от existing: чужая тема — не повод
    # считать слово производным своего корня.
    elsewhere = list_article_sprint_words_all_themes() - existing
    # Уже выброшенное не берём заново — ни в эту тему, ни в соседнюю: снятые с показа
    # слова плюс стоп-лист прошлых отказов. Держим отдельно от existing: в счёт
    # «производных одного корня» они идти не должны, иначе выброшенный хлам закрывал бы
    # дорогу нормальному слову.
    banned = list_retired_article_words() | list_article_word_blacklist()
    to_blacklist: list[tuple[str, str, str]] = []  # (слово, причина, тема)
    to_quarantine: list[dict] = []  # спорные отказы стража — уедут владельцу в разбор
    # Отсечь мусор бесплатно — хорошо, но за его ПОРОЖДЕНИЕ мы платим всё равно.
    # Поэтому горячую часть отказов показываем модели заранее: сначала снятое в этой
    # теме, потом недавно снятое в соседних. Весь список (тысячи слов) в подсказку не лезет.
    avoid_words = list(existing)[:_AVOID_EXISTING_CAP]
    avoid_words += [w for w in list_retired_article_words_for_prompt(theme_key)
                    if w.lower() not in existing][:_AVOID_RETIRED_CAP]
    known_meanings = set(list_article_sprint_meanings(theme_key))
    family_counts: dict[str, int] = {}
    for word in existing:
        head = head_word(word, existing)
        if head:
            family_counts[head] = family_counts.get(head, 0) + 1
    added = 0
    rejected = 0
    rejected_by_reason: dict[str, int] = {}
    gen_failures: list[str] = []
    by_subtopic: dict[str, int] = {}

    def _reject(reason: str) -> None:
        nonlocal rejected
        rejected += 1
        bucket = _reason_bucket(reason)
        rejected_by_reason[bucket] = rejected_by_reason.get(bucket, 0) + 1

    for subtopic in theme["subtopics"]:
        if added >= cap:
            break
        try:
            gen = _run(run_article_noun_gen(
                theme=theme["label_de"], subtopic=subtopic,
                count=_ask_count(cap - added, per_subtopic),
                avoid=avoid_words,
            ))
        except Exception:
            gen_failures.append(subtopic)
            logging.warning("fill_theme: gen failed theme=%s subtopic=%s", theme_key, subtopic, exc_info=True)
            continue

        # ── фильтр полезности ─────────────────────────────────────────────────
        # До него банк набивался чем угодно: 48% слов не входили даже в 50 000 самых
        # частых, а 37% были производными от другого слова той же темы. Причина —
        # цель в 280 слов на тему: ходовых столько нет, и генератор добивал план.
        # Теперь слово проходит по частотности, а спорное уходит на второе мнение к
        # модели: частотный список занижает предметы («die Spülmaschine» — 33 467).
        candidates: list[dict] = []
        maybe: list[dict] = []
        for n in gen:
            w = str(n.get("word") or "").strip()
            art = str(n.get("article") or "").strip().lower()
            if not w or art not in ("der", "die", "das") or w.lower() in existing:
                continue
            if w.lower() in elsewhere:
                # В стоп-лист НЕ пишем: слово хорошее, просто его тема — другая.
                _reject("уже есть в другой теме")
                logging.info("артикли: %s мимо — уже стоит в другой теме", w)
                continue
            if w.lower() in banned:
                # Слово уже снимали. Отсекаем ДО фильтра, чтобы не платить за «второе
                # мнение» модели по тому, что и так решено.
                _reject("снято с показа")
                logging.info("артикли: %s мимо — слово уже снято с показа", w)
                continue
            ok, why = check_word(
                w, meaning_ru=str(n.get("meaning_ru") or ""),
                known_words=existing, known_meanings=known_meanings,
                family_counts=family_counts,
            )
            if ok:
                candidates.append(n)
            elif why == "нужно второе мнение":
                maybe.append(n)
            else:
                _reject(why)
                logging.info("артикли: %s мимо — %s", w, why)
        if maybe:
            try:
                verdicts_everyday = judge_everyday_words([str(n["word"]).strip() for n in maybe])
            except EverydayJudgeUnavailable:
                # Ответа не было. Сомнительное не берём, но и в стоп-лист не пишем:
                # один обрыв сети иначе похоронил бы пачку нормальных слов навсегда.
                for _ in maybe:
                    _reject("второе мнение не получено")
                logging.warning("артикли: второе мнение недоступно, пачка отложена (%s слов)", len(maybe))
                maybe = []
                verdicts_everyday = {}
            for n in maybe:
                w = str(n["word"]).strip()
                if verdicts_everyday.get(w):
                    # слово обиходное, хоть и редкое в текстах — проверяем на семью и дубль
                    ok, why = check_word(
                        w, meaning_ru=str(n.get("meaning_ru") or ""),
                        known_words=existing, known_meanings=known_meanings,
                        family_counts=family_counts, max_rank=10 ** 9,
                    )
                    if ok:
                        candidates.append(n)
                        continue
                    logging.info("артикли: %s мимо — %s", w, why)
                    _reject(why)
                elif word_rank(w) and int(word_rank(w)) <= QUARANTINE_MAX_RANK:
                    # Страж сказал «не нужно», но по частотности слово выглядит ходовым —
                    # значит он мог и ошибиться. Молчаливый стоп-лист такие ошибки хоронил
                    # навсегда: владелец о них не узнавал. Кладём слово в карантин, и оно
                    # приедет к нему в дневной разбор с кнопками «вернуть / мусор».
                    to_quarantine.append({"word": w, "article": str(n.get("article") or "").lower(),
                                          "meaning_ru": str(n.get("meaning_ru") or ""),
                                          "subtopic": subtopic})
                    _reject("страж сомневается — в карантин")
                else:
                    # Модель ответила «в быту не встречается», и по частоте слово тоже
                    # хлам. Запоминаем навсегда: второй раз платить за него не будем.
                    to_blacklist.append((w, "не нужно в быту", theme_key))
                    _reject("нужно второе мнение")
        if not candidates:
            continue

        try:
            verdicts = _run(run_article_verify(
                items=[{"word": n["word"], "article": str(n["article"]).lower()} for n in candidates]
            ))
        except Exception:
            logging.warning("fill_theme: verify failed theme=%s subtopic=%s", theme_key, subtopic, exc_info=True)
            continue

        # Ответы сопоставляем ПО СЛОВУ, а не по позиции. Порядок гарантирован только
        # фразой в промпте, а цена сдвига теперь вечная: чужой вердикт «не годится»
        # занёс бы нормальное слово в стоп-лист навсегда. Слово без ответа — это «ответа
        # не было»: пропускаем молча, следующий прогон спросит снова.
        by_word = {}
        for v in verdicts or []:
            if isinstance(v, dict) and str(v.get("word") or "").strip():
                by_word[str(v["word"]).strip().lower()] = v
        positional = None
        if not by_word and len(verdicts or []) == len(candidates):
            positional = list(verdicts)  # старый ответ без слова — но длина сошлась
        elif not by_word:
            logging.warning("fill_theme: верификатор ответил %s на %s слов — пропускаю пачку",
                            len(verdicts or []), len(candidates))

        rows: list[dict] = []
        for i, n in enumerate(candidates):
            w = str(n.get("word") or "").strip()
            v = by_word.get(w.lower()) if by_word else (positional[i] if positional else None)
            if not isinstance(v, dict):
                _reject("ответа по слову не было")
                continue
            if not v.get("ok"):
                reason = str(v.get("reason") or "").strip().lower()
                # Заголовок во множественном числе. Игра спрашивает артикль ЕДИНСТВЕННОГО
                # числа, поэтому «die Zitate» вместо «das Zitat» делает вопрос без ответа:
                # у формы множественного артикль всегда die, и знать тут нечего. Замер
                # 16.08.2026 нашёл в банке около двух десятков таких на 5103 слова —
                # немного, но каждое такое слово учит неправильному.
                # Слова, у которых единственного числа нет (die Eltern, die Kosten,
                # die Leute, die Schulden), проверяющий отклонять не должен — это
                # прописано в его инструкции отдельным исключением.
                _reject("форма множественного числа" if reason == "plural_form"
                        else "артикль не подтверждён")
                if reason in ("ambiguous", "person_adjective"):
                    # Слово не мусор: у него артикль зависит от смысла. Такие живут в
                    # игре отдельной строкой на каждый смысл и с переводом на экране —
                    # в стоп-лист им нельзя, иначе дорога туда закрыта навсегда.
                    logging.info("артикли: %s — артикль зависит от смысла (%s), стоп-лист не трогаем",
                                 w, reason)
                elif reason == "plural_form":
                    # В стоп-лист идёт САМА форма множественного числа — «Mängel».
                    # Единственное («Mangel») этим не задето и может прийти в набор
                    # завтра же: стоп-лист хранит написание, а не корень.
                    to_blacklist.append((w, "форма множественного числа", theme_key))
                else:
                    to_blacklist.append((w, "не существительное / не годится", theme_key))
                continue
            art = str(v.get("article") or n.get("article") or "").strip().lower()
            if is_ambiguous_noun(w):
                # Двуродовое: сюда оно попадать не должно, но и хоронить его нельзя —
                # место такому слову есть, через курируемые смыслы (article_two_gender).
                _reject("двуродовое")
                continue
            # СПРАВОЧНИК ГЛАВНЕЕ МОДЕЛИ. Сначала спрашиваем Wiktionary про само слово —
            # здесь МОЖНО ходить в сеть (заливка идёт фоном, а ответ оседает в кэше и
            # дальше достаётся бесплатно). Не знает — правило композита. Модель у нас
            # уже один раз ошиблась на «die Wechselkurs», её слово тут не последнее.
            source = "gpt"
            try:
                from backend.article_authority import authoritative_article
                verdict, src = authoritative_article(w, allow_network=True)
                if verdict:
                    if verdict != art:
                        logging.warning(
                            "article intake: %s — модель дала «%s», справочник «%s» (%s), берём справочник",
                            w, art, verdict, src)
                    art = verdict
                    source = src
                else:
                    # Рода не знает ни Wiktionary, ни правило композита. Спрашивать модель
                    # бессмысленно — она и ошиблась. Кладём НЕПРОВЕРЕННЫМ: в игру такие
                    # строки не попадают, но и слово не теряется, его видно на ревью.
                    logging.warning("article intake: %s — род не подтверждён (%s), в игру не пускаем", w, src)
                    rows.append({
                        "word": w, "article": art,
                        "meaning_ru": str(n.get("meaning_ru") or ""),
                        "plural": str(n.get("plural") or ""),
                        "difficulty": str(n.get("difficulty") or "B"),
                        "subtopic": subtopic, "source": "gpt-unverified", "verified": False,
                    })
                    existing.add(w.lower())
                    continue
            except Exception:
                logging.warning("article intake: справочник недоступен для %s", w, exc_info=True)
            if art not in ("der", "die", "das") or not w or w.lower() in existing:
                _reject("уже есть в теме" if w.lower() in existing else "артикль не подтверждён")
                continue
            rows.append({
                "word": w, "article": art,
                "meaning_ru": str(n.get("meaning_ru") or ""),
                "plural": str(n.get("plural") or ""),
                "difficulty": str(n.get("difficulty") or "B"),
                "subtopic": subtopic, "source": source, "verified": True,
            })
            existing.add(w.lower())
            meaning_added = str(n.get("meaning_ru") or "").strip().lower()
            if meaning_added:
                known_meanings.add(meaning_added)

        if added + len(rows) > cap:
            rows = rows[: max(0, cap - added)]
        apply_reference_plurals(rows)
        if rows:
            res = insert_article_sprint_nouns(theme_key, rows)
            added += int(res.get("inserted") or 0)
            by_subtopic[subtopic] = int(res.get("inserted") or 0)

    # Отказы запоминаем одним заходом в конце: следующий набор отсечёт их бесплатно.
    blacklisted = blacklist_article_words(to_blacklist)
    quarantined = quarantine_article_sprint_nouns(theme_key, to_quarantine)
    final = count_article_sprint_nouns(theme_key, verified_only=True)
    return {
        "theme": theme_key, "added": added, "rejected": rejected,
        "blacklisted": blacklisted, "quarantined": quarantined,
        "final_verified": final, "target": target, "by_subtopic": by_subtopic,
        # had — сколько слов лежало в теме ДО прогона. Без него «добавлено 26» повисает
        # в воздухе: непонятно, к чему эти 26, если в теме 150.
        "had": have,
        "rejected_by_reason": rejected_by_reason,
        "gen_failures": gen_failures,
    }


_ARTICLE_PREFIX = ("der ", "die ", "das ")


def _strip_article(raw: str) -> tuple[str, str]:
    """«Der Kumpel» → («Kumpel», «der»). Владелец пишет слово так, как привык.

    Без этого артикль приклеивался к слову: в разборе выходило «der Der Kumpel», а
    справочник, понятно, такого слова не знал."""
    text = str(raw or "").strip()
    low = text.lower()
    for pref in _ARTICLE_PREFIX:
        if low.startswith(pref):
            return text[len(pref):].strip(), pref.strip()
    return text, ""


def _check_manual_words(theme_key: str, entries: list[dict]):
    """Полная рецензия на каждое присланное слово, без единой записи в банк.

    По каждому слову собираем ВСЁ, а не первую причину отказа: существительное ли оно,
    какой артикль и откуда, стоит ли уже в банке и в какой теме, ходовое ли по частоте —
    и только потом рекомендацию. Владелец не должен искать слово глазами по списку из
    полутора тысяч, чтобы понять, дубль это или нет.

    → ([рецензия по каждому], тема)
    """
    from backend.article_sprint_themes import article_sprint_themes
    from backend.openai_manager import run_article_verify, run_article_translate
    from backend.article_word_gate import word_rank, judge_everyday_words, DEFAULT_MAX_RANK
    from backend.database import ensure_article_sprint_schema, where_article_words_live

    theme = next((t for t in article_sprint_themes() if t["key"] == theme_key), None)
    if not theme:
        return [], None
    ensure_article_sprint_schema()

    cleaned: list[dict] = []
    seen: set[str] = set()
    for e in entries or []:
        word, hint = _strip_article(str((e or {}).get("word") or ""))
        if not word or word.lower() in seen:
            continue
        seen.add(word.lower())
        cleaned.append({
            "word": word,
            "article": str((e or {}).get("article") or hint).strip().lower(),
            "meaning_ru": str((e or {}).get("meaning_ru") or "").strip(),
        })
    if not cleaned:
        return [], theme

    try:
        verdicts = _run(run_article_verify(
            items=[{"word": c["word"], "article": c["article"] or "der"} for c in cleaned]))
    except Exception:
        logging.warning("manual words: verify failed theme=%s", theme_key, exc_info=True)
        verdicts = []
    need_tr = [c["word"] for c in cleaned if not c["meaning_ru"]]
    try:
        trmap = _run(run_article_translate(words=need_tr)) if need_tr else {}
    except Exception:
        trmap = {}
    # Ответ ищем ПО СЛОВУ, позиция — запасной путь и только когда длина сошлась.
    verdicts_by_word = {}
    for v in verdicts or []:
        if isinstance(v, dict) and str(v.get("word") or "").strip():
            verdicts_by_word[str(v["word"]).strip().lower()] = v

    known = where_article_words_live([c["word"] for c in cleaned])

    # Второе мнение спрашиваем ОДНИМ запросом и только про то, что не прошло по частоте.
    rare = [c["word"] for c in cleaned
            if (word_rank(c["word"]) or 10 ** 9) > DEFAULT_MAX_RANK]
    everyday: dict[str, bool] = {}
    if rare:
        try:
            everyday = judge_everyday_words(rare)
        except Exception:
            logging.warning("manual words: второе мнение недоступно", exc_info=True)

    out: list[dict] = []
    for i, c in enumerate(cleaned):
        w = c["word"]
        v = verdicts_by_word.get(w.lower()) if verdicts_by_word else (
            verdicts[i] if len(verdicts or []) == len(cleaned) else None)
        row = {
            "word": w,
            "meaning_ru": c["meaning_ru"] or trmap.get(w.lower(), ""),
            "article": "", "article_source": "",
            "is_noun": True, "where": None, "rank": word_rank(w),
            "ok": False, "reason": "", "verified": True, "source": "manual",
        }

        # 1-2. Существительное ли и какой артикль. Решает СПРАВОЧНИК, а не модель:
        # у проверки ok=false означает и «не существительное», и «род зависит от смысла»,
        # и «артикль в запросе неверный». Мы отправляем ей der по умолчанию, поэтому die
        # Girlande и das Feuerwerk возвращались как ok=false и объявлялись глаголами.
        art, is_noun, verified, src_label = "", True, True, ""
        try:
            from backend.article_authority import authoritative_article
            verdict, src = authoritative_article(w, allow_network=True)
        except Exception:
            logging.warning("article manual: справочник недоступен для %s", w, exc_info=True)
            verdict, src = None, "справочник недоступен"
        if verdict:
            art, src_label, row["source"] = verdict, src, src
        elif "родовое" in str(src):
            # Слово есть, но род решает смысл. Это существительное, просто спорное.
            src_label = "два рода — зависит от смысла"
            row["reason"] = "у слова два рода — артикль зависит от смысла"
        else:
            # Справочник молчит. Вот здесь мнение модели — единственное, что у нас есть.
            if isinstance(v, dict) and v.get("ok"):
                art = str(v.get("article") or c["article"] or "").strip().lower()
                verified, src_label, row["source"] = False, "справочник молчит", "manual-unverified"
            else:
                is_noun = False
        row["is_noun"] = is_noun
        row["verified"] = verified
        row["article"] = art if (is_noun and art in ("der", "die", "das")) else ""
        row["article_source"] = src_label

        # 3. Есть ли уже в банке — и где именно.
        row["where"] = known.get(w.lower())

        # 4. Рекомендация. Порядок причин — от непреодолимой к спорной.
        if not row["is_noun"]:
            row["reason"] = "не существительное — артикля у него нет"
        elif row["where"] and not row["where"]["retired"]:
            same = row["where"]["theme_key"] == theme_key
            row["reason"] = ("уже есть в этой теме" if same
                             else f"уже стоит в теме «{row['where']['label']}»")
        elif not row["article"]:
            row["reason"] = row["reason"] or "артикль определить не удалось"
        elif not row["verified"]:
            row["reason"] = "род не подтверждён справочником — пойдёт на ревью артикля"
        elif w in rare and everyday and not everyday.get(w, False):
            row["reason"] = "проверка считает, что в быту такое слово не встречается"
        elif w in rare and not everyday:
            row["reason"] = "редкое слово, второе мнение получить не удалось"
        row["ok"] = not row["reason"]
        out.append(row)
    return out, theme


def review_manual_words(theme_key: str, entries: list[dict]) -> list[dict]:
    """Проверить присланные слова и вернуть вердикт по КАЖДОМУ, ничего не записав."""
    return _check_manual_words(theme_key, entries)[0]


def commit_manual_words(theme_key: str, rows: list[dict]) -> dict:
    """Записать в тему уже проверенные и одобренные владельцем слова."""
    from backend.article_sprint_themes import article_sprint_themes
    from backend.database import (
        count_article_sprint_nouns, insert_article_sprint_nouns, unblacklist_article_words,
    )
    theme = next((t for t in article_sprint_themes() if t["key"] == theme_key), None)
    clean = [{"word": r["word"], "article": r["article"], "meaning_ru": r.get("meaning_ru") or "",
              "plural": "", "difficulty": "B", "subtopic": "manual",
              "source": r.get("source") or "manual", "verified": bool(r.get("verified", True))}
             for r in (rows or []) if str(r.get("article") or "") in ("der", "die", "das")]
    inserted = 0
    if clean:
        inserted = int((insert_article_sprint_nouns(theme_key, clean) or {}).get("inserted") or 0)
        # Владелец добавил слово руками — его решение главнее прошлого автоотказа.
        unblacklist_article_words([r["word"] for r in clean])
    return {"theme": theme_key, "added": inserted,
            "final_verified": count_article_sprint_nouns(theme_key, verified_only=True),
            "target": int((theme or {}).get("target_count") or 0)}


def add_manual_words(theme_key: str, entries: list[dict]) -> dict:
    """Проверить и сразу записать — для команды /artikel_addwords, где подтверждения нет."""
    rows, theme = _check_manual_words(theme_key, entries)
    if theme is None:
        return {"error": "unknown_theme", "theme": theme_key}
    take = [r for r in rows if r["ok"]]
    res = commit_manual_words(theme_key, take)
    return {"theme": theme_key, "added": int(res.get("added") or 0),
            "rejected": sum(1 for r in rows if not r["ok"] and "уже" not in r["reason"]),
            "skipped_dup": sum(1 for r in rows if "уже" in r["reason"]),
            "final_verified": res.get("final_verified"),
            "target": int(theme["target_count"])}


def autofill_themes_below_target(*, per_theme_cap: int = 40, total_cap: int = 120) -> dict:
    """Nightly auto-grow: top up every theme that's below its target via fill_theme,
    bounded per theme and overall (budget guard) so it walks all themes to ~target
    over several nights. Reuses the full generate+verify pipeline."""
    from backend.article_sprint_themes import article_sprint_themes
    from backend.database import count_article_sprint_nouns

    from backend.database import list_theme_fill_report, record_theme_fill_run

    # Тему, снятую с добора, не трогаем. Раньше выбор был только по цели, поэтому
    # выдохшуюся тему ночной прогон обходил каждую ночь и каждую ночь платил за пустой
    # результат: ходовых слов там просто не осталось.
    states = {r["theme_key"]: r["state"] for r in list_theme_fill_report()}
    results: list[dict] = []
    exhausted: list[str] = []
    total_added = 0
    for t in article_sprint_themes():
        if total_added >= total_cap:
            break
        key = str(t["key"])
        if states.get(key, "auto") != "auto":
            continue
        target = int(t.get("target_count") or 0)
        have = count_article_sprint_nouns(key, verified_only=True)
        room = min(int(per_theme_cap), target - have, total_cap - total_added)
        if room <= 0:
            continue
        try:
            res = fill_theme(key, max_to_add=room)
        except Exception:
            logging.warning("autofill: fill_theme failed theme=%s", key, exc_info=True)
            continue
        added = int(res.get("added") or 0)
        total_added += added
        run = record_theme_fill_run(key, added)
        if run.get("exhausted"):
            exhausted.append(key)
        results.append({"theme": key, "added": added, "dry_streak": run.get("dry_streak"),
                        "exhausted": bool(run.get("exhausted")),
                        "final_verified": res.get("final_verified"), "target": target})
    return {"total_added": total_added, "themes": results, "exhausted": exhausted}
