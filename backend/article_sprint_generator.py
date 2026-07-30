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


def fill_theme(theme_key: str, *, max_to_add: int | None = None, per_subtopic: int = 30) -> dict:
    """Generate+verify+insert nouns for `theme_key`.
    max_to_add: cap how many NEW words to add this run (None → up to target).
    Returns stats dict."""
    from backend.article_sprint_themes import article_sprint_themes
    from backend.openai_manager import run_article_noun_gen, run_article_verify
    from backend.database import (
        ensure_article_sprint_schema, count_article_sprint_nouns,
        insert_article_sprint_nouns, list_article_sprint_words,
        list_article_sprint_meanings, list_retired_article_words,
        list_article_word_blacklist, blacklist_article_words,
    )
    from backend.article_word_gate import (
        check_word, head_word, judge_everyday_words, EverydayJudgeUnavailable,
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
    # Уже выброшенное не берём заново — ни в эту тему, ни в соседнюю: снятые с показа
    # слова плюс стоп-лист прошлых отказов. Держим отдельно от existing: в счёт
    # «производных одного корня» они идти не должны, иначе выброшенный хлам закрывал бы
    # дорогу нормальному слову.
    banned = list_retired_article_words() | list_article_word_blacklist()
    to_blacklist: list[tuple[str, str, str]] = []  # (слово, причина, тема)
    known_meanings = set(list_article_sprint_meanings(theme_key))
    family_counts: dict[str, int] = {}
    for word in existing:
        head = head_word(word, existing)
        if head:
            family_counts[head] = family_counts.get(head, 0) + 1
    added = 0
    rejected = 0
    by_subtopic: dict[str, int] = {}

    for subtopic in theme["subtopics"]:
        if added >= cap:
            break
        try:
            gen = _run(run_article_noun_gen(
                theme=theme["label_de"], subtopic=subtopic,
                count=per_subtopic, avoid=list(existing)[:200],
            ))
        except Exception:
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
            if w.lower() in banned:
                # Слово уже снимали. Отсекаем ДО фильтра, чтобы не платить за «второе
                # мнение» модели по тому, что и так решено.
                rejected += 1
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
                rejected += 1
                logging.info("артикли: %s мимо — %s", w, why)
        if maybe:
            try:
                verdicts_everyday = judge_everyday_words([str(n["word"]).strip() for n in maybe])
            except EverydayJudgeUnavailable:
                # Ответа не было. Сомнительное не берём, но и в стоп-лист не пишем:
                # один обрыв сети иначе похоронил бы пачку нормальных слов навсегда.
                rejected += len(maybe)
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
                else:
                    # Модель ответила «в быту не встречается» — это про само слово,
                    # значит запоминаем навсегда: второй раз платить за него не будем.
                    to_blacklist.append((w, "не нужно в быту", theme_key))
                rejected += 1
        if not candidates:
            continue

        try:
            verdicts = _run(run_article_verify(
                items=[{"word": n["word"], "article": str(n["article"]).lower()} for n in candidates]
            ))
        except Exception:
            logging.warning("fill_theme: verify failed theme=%s subtopic=%s", theme_key, subtopic, exc_info=True)
            continue

        rows: list[dict] = []
        for n, v in zip(candidates, verdicts):
            if not isinstance(v, dict) or not v.get("ok"):
                rejected += 1
                if isinstance(v, dict):  # ответ был, и он отрицательный — запоминаем
                    to_blacklist.append((str(n.get("word") or "").strip(),
                                         "не существительное / не годится", theme_key))
                continue
            art = str(v.get("article") or n.get("article") or "").strip().lower()
            w = str(n.get("word") or "").strip()
            # Reject two-gender / meaning-dependent nouns — their article isn't decidable.
            if is_ambiguous_noun(w):
                rejected += 1
                to_blacklist.append((w, "двуродовое — артикль не определить", theme_key))
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
                rejected += 1
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
    final = count_article_sprint_nouns(theme_key, verified_only=True)
    return {
        "theme": theme_key, "added": added, "rejected": rejected,
        "blacklisted": blacklisted,
        "final_verified": final, "target": target, "by_subtopic": by_subtopic,
    }


def add_manual_words(theme_key: str, entries: list[dict]) -> dict:
    """Insert a user-supplied list of nouns into a theme — same bank, same quality
    gate as the generator. Each entry: {word, article?(der/die/das), meaning_ru?}.

    The article is VERIFIED/CORRECTED via the same LLM + deterministic guard as
    fill_theme (so a wrong/missing article is fixed, ambiguous/non-nouns rejected);
    a missing meaning_ru is auto-translated. Rows are stored source='manual',
    verified=True → they feed every game and pick up media (audio/images/mnemonics)
    through the existing media jobs, exactly like generated words. No target cap —
    you can grow a theme beyond 300 if you want."""
    from backend.article_sprint_themes import article_sprint_themes
    from backend.openai_manager import run_article_verify, run_article_translate
    from backend.database import (
        ensure_article_sprint_schema, count_article_sprint_nouns,
        insert_article_sprint_nouns, list_article_sprint_words,
        unblacklist_article_words,
    )

    theme = next((t for t in article_sprint_themes() if t["key"] == theme_key), None)
    if not theme:
        return {"error": "unknown_theme", "theme": theme_key}
    ensure_article_sprint_schema()

    existing = {w.lower() for w in list_article_sprint_words(theme_key)}
    cleaned: list[dict] = []
    seen: set[str] = set()
    for e in entries or []:
        w = str((e or {}).get("word") or "").strip()
        if not w or w.lower() in seen:
            continue
        seen.add(w.lower())
        cleaned.append({
            "word": w,
            "article": str((e or {}).get("article") or "").strip().lower(),
            "meaning_ru": str((e or {}).get("meaning_ru") or "").strip(),
        })
    if not cleaned:
        return {"theme": theme_key, "added": 0, "rejected": 0, "skipped_dup": 0,
                "final_verified": count_article_sprint_nouns(theme_key, verified_only=True),
                "target": int(theme["target_count"])}

    try:
        verdicts = _run(run_article_verify(
            items=[{"word": c["word"], "article": c["article"] or "der"} for c in cleaned]))
    except Exception:
        logging.warning("add_manual_words: verify failed theme=%s", theme_key, exc_info=True)
        verdicts = []
    need_tr = [c["word"] for c in cleaned if not c["meaning_ru"]]
    try:
        trmap = _run(run_article_translate(words=need_tr)) if need_tr else {}
    except Exception:
        trmap = {}

    rows: list[dict] = []
    rejected = 0
    skipped_dup = 0
    for i, c in enumerate(cleaned):
        w = c["word"]
        if w.lower() in existing:
            skipped_dup += 1
            continue
        if is_ambiguous_noun(w):
            rejected += 1
            continue
        v = verdicts[i] if i < len(verdicts) else None
        art = c["article"]
        if isinstance(v, dict):
            if not v.get("ok"):
                rejected += 1
                continue
            art = str(v.get("article") or art).strip().lower()
        # СПРАВОЧНИК ГЛАВНЕЕ МОДЕЛИ — та же лестница, что и в автозаливке выше:
        # Wiktionary (кэш → живой запрос) → правило композита → «не знаем».
        # Ручной путь не привилегирован: неверный артикль отсюда учит человека
        # ровно так же, как неверный артикль из генератора.
        source = "manual"
        verified = True
        try:
            from backend.article_authority import authoritative_article
            verdict, src = authoritative_article(w, allow_network=True)
            if verdict:
                if verdict != art:
                    logging.warning(
                        "article manual: %s — заявлено «%s», справочник «%s» (%s), берём справочник",
                        w, art, verdict, src)
                art = verdict
                source = src
            else:
                # Ни Wiktionary, ни правило композита не подтвердили род. В игру не пускаем:
                # строка видна на ревью, но человеку неподтверждённый артикль не показываем.
                logging.warning("article manual: %s — род не подтверждён (%s), кладём на ревью", w, src)
                source, verified = "manual-unverified", False
        except Exception:
            logging.warning("article manual: справочник недоступен для %s", w, exc_info=True)
        if art not in ("der", "die", "das"):
            rejected += 1
            continue
        rows.append({
            "word": w, "article": art,
            "meaning_ru": c["meaning_ru"] or trmap.get(w.lower(), ""),
            "plural": "", "difficulty": "B",
            "subtopic": "manual", "source": source, "verified": verified,
        })
        existing.add(w.lower())

    inserted = 0
    if rows:
        res = insert_article_sprint_nouns(theme_key, rows)
        inserted = int(res.get("inserted") or 0)
        # Админ добавил слово руками — его решение главнее прошлого автоотказа.
        unblacklist_article_words([r["word"] for r in rows])
    final = count_article_sprint_nouns(theme_key, verified_only=True)
    return {"theme": theme_key, "added": inserted, "rejected": rejected,
            "skipped_dup": skipped_dup, "final_verified": final,
            "target": int(theme["target_count"])}


def autofill_themes_below_target(*, per_theme_cap: int = 40, total_cap: int = 120) -> dict:
    """Nightly auto-grow: top up every theme that's below its target via fill_theme,
    bounded per theme and overall (budget guard) so it walks all themes to ~target
    over several nights. Reuses the full generate+verify pipeline."""
    from backend.article_sprint_themes import article_sprint_themes
    from backend.database import count_article_sprint_nouns

    results: list[dict] = []
    total_added = 0
    for t in article_sprint_themes():
        if total_added >= total_cap:
            break
        key = str(t["key"])
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
        total_added += int(res.get("added") or 0)
        results.append({"theme": key, "added": int(res.get("added") or 0),
                        "final_verified": res.get("final_verified"), "target": target})
    return {"total_added": total_added, "themes": results}
