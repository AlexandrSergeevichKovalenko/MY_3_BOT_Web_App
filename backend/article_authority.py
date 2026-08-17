"""Арбитр артикля: единственный ответ на вопрос «какой род у этого существительного».

Зачем понадобился. Артикль в банке тренажёра проверялся словарём голов композитов,
захардкоженным на ~60 записей (article_sprint_generator._HEAD_GENDER). В нём нет ни
Kurs, ни Beet, ни Strauch — поэтому в прод уехали «die Wechselkurs», «der Rosenbeet»,
«die Haselnussstrauch». При этом рядом лежит кэш Wiktionary на 19 284 однозначных рода,
который для этого не использовался: у «Wechselkurs» правильный род ТАМ УЖЕ БЫЛ.

Три ступени, от самой надёжной к менее надёжной:

  1. САМО СЛОВО в Wiktionary. Кэш bt_3_wiktionary_genus_cache, а при отсутствии —
     живой запрос к API (он же наполняет кэш). Это наш объявленный источник истины.
  2. ПРАВИЛО КОМПОЗИТА: род = род последней части. Работает по тем же 19k родов,
     а не по списку из 60 слов.
  3. Ничего. Тогда честно возвращаем None — и вызывающий код НЕ пускает слово в игру
     молча, а помечает на ревью. Спрашивать модель бессмысленно: это она и ошиблась.

⚠️ Правило композита применяется ТОЛЬКО когда все возможные разборы слова дают один и
тот же род. «Poloshirt» разбирается и как Polo+Shirt (das), и как polo+s+hirt (der) —
разногласие, значит правило молчит. Это дороже по покрытию, зато не портит данные:
ошибка здесь учит человека неправде, а это ровно то, чего мы избегаем.
"""
from __future__ import annotations

import logging
import threading
import time

_CACHE_TTL_SEC = 900
_lock = threading.Lock()
_genus: dict[str, str] = {}
_ambiguous: set[str] = set()
# Слова, чей род известен только из НАШЕГО банка артиклей, а не из справочника
# de.wiktionary. Их ответ честно называется «банк артиклей»: он годится как «что-то
# известно», но не годится как подтверждение — банк не может подтверждать сам себя.
_bank_sourced: set[str] = set()
# Написания, которые мы САМИ признали негодной формой слова (стоп-лист, причина «форма
# множественного числа»). Такому написанию род не даёт НИ ОДНА ступень: ни банк, ни
# Wiktionary, ни правило композита. Иначе перекрытие одной ступени просто передаёт
# вопрос следующей: 17.08.2026 «Seifenblasen» перестал брать род из банка и тут же взял
# «das» у правила композита — ответ остался неверным, поменялся только источник.
_bad_forms: set[str] = set()
_loaded_at = 0.0

# Похожи на существительные, но это СУФФИКСЫ: Wirt+schaft — не композит, и род задаёт
# суффикс, а не «голова». Правило композита к ним неприменимо.
_DERIV_SUFFIXES = {
    "schaft", "heit", "keit", "ung", "nis", "tum", "sal", "ling", "chen", "lein",
    "ismus", "ion", "tion", "sion", "ität", "erei", "ur", "ade", "age", "anz", "enz",
    "ent", "ant", "ist", "eur", "ette", "esse", "logie", "ologie", "iker",
}
# Род НЕ наследуется: der Mut → die Armut/Demut/Anmut; das Wort → die Antwort.
_COMPOUND_EXCEPTIONS = ("mut", "wort")


def _two_gender() -> set[str]:
    try:
        from backend.article_two_gender import TWO_GENDER_NOUNS
        return {str(i.get("word") or "").strip().lower() for i in TWO_GENDER_NOUNS if i.get("word")}
    except Exception:
        return set()


def _load() -> tuple[dict[str, str], set[str]]:
    """{lemma: der/die/das} по однозначным родам + множество двухродовых. Кеш 15 минут."""
    global _genus, _ambiguous, _loaded_at
    now = time.time()
    with _lock:
        if _genus and (now - _loaded_at) < _CACHE_TTL_SEC:
            return _genus, _ambiguous
    genus: dict[str, str] = {}
    by_word: dict[str, set] = {}
    try:
        from backend.database import get_db_connection_context
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT title, genus FROM bt_3_wiktionary_genus_cache "
                            "WHERE COALESCE(genus,'') <> ''")
                g2a = {"m": "der", "f": "die", "n": "das"}
                for title, code in cur.fetchall() or []:
                    c = str(code or "").strip().lower()
                    if len(c) == 1 and c in g2a:      # 'mf'/'nf' = два рода → не берём
                        genus[str(title).strip().lower()] = g2a[c]
                # Написания, которые мы САМИ признали негодной формой слова, родов не
                # поставляют. Иначе снятая строка продолжает решать род во всём
                # продукте: 17.08.2026 «das Fotos» уже был снят из игры, а справочник
                # на вопрос про «Fotos» по-прежнему отвечал «das» — потому что строка
                # осталась в таблице, а выборка не смотрела на решение.
                #
                # Условие узкое НАМЕРЕННО. Замер 17.08.2026 обеих правок:
                #   «не брать все снятые строки»  → род потеряли бы 1307 слов,
                #                                   приобрело бы 1, изменилось 0;
                #   «не брать негодные написания» → род потеряли ровно 8, изменилось 0,
                #                                   и это ровно те 8 форм множественного
                #                                   числа, которые мы убрали руками.
                # Снятие само по себе НЕ означает «слово неправильное»: строку снимают
                # и за дубль, и за ротацию освоенного — род у них верный.
                cur.execute("SELECT word FROM bt_3_article_word_blacklist "
                            "WHERE reason ILIKE %s", ("%форма множественного числа%",))
                bad_forms = {str(r[0]).strip().lower() for r in cur.fetchall() or []}
                _bad_forms.clear()
                _bad_forms.update(bad_forms)
                cur.execute("SELECT word, article FROM bt_3_article_sprint_nouns "
                            "WHERE COALESCE(article,'') <> ''")
                for w, a in cur.fetchall() or []:
                    lw = str(w).strip().lower()
                    if lw in bad_forms:
                        continue
                    by_word.setdefault(lw, set()).add(str(a).strip().lower())
    except Exception:
        logging.warning("article authority: не смог загрузить роды", exc_info=True)
        return {}, set()

    ambiguous = {w for w, arts in by_word.items() if len(arts) > 1} | _two_gender()
    # ⚠ Слова, чей род взят ИЗ БАНКА, а не из справочника, помечаются отдельно и
    # отдаются под своим именем. Раньше они возвращались с источником «wiktionary», и
    # получалась кольцевая проверка: банк подтверждал сам себя.
    #
    # Это не теория. 16.08.2026: в банке жила строка «die Handschuhe» — форма
    # множественного числа. Через эту подмену german_surface объявлял «Handschuhe»
    # документированным существительным женского рода в ЕДИНСТВЕННОМ числе, правило
    # «у множественного артикль всегда die» не срабатывало, и в базу легла карточка
    # «der Handschuhe». Владелец нашёл её глазами.
    from_bank: set[str] = set()
    for w, arts in by_word.items():
        if len(arts) == 1 and w not in genus:
            genus[w] = next(iter(arts))
            from_bank.add(w)
    for w in ambiguous:
        genus.pop(w, None)
        from_bank.discard(w)
    _bank_sourced.clear()
    _bank_sourced.update(from_bank)
    with _lock:
        _genus, _ambiguous, _loaded_at = genus, ambiguous, now
    return genus, ambiguous


def _wiktionary_live(word: str) -> str | None:
    """Живой запрос к Wiktionary (он же наполняет кэш). Только однозначный род."""
    try:
        from backend.article_wiktionary_ref import genus_for_titles
        code = str((genus_for_titles([word]) or {}).get(word) or "").strip().lower()
        return {"m": "der", "f": "die", "n": "das"}.get(code) if len(code) == 1 else None
    except Exception:
        logging.warning("article authority: живой Wiktionary недоступен для %s", word, exc_info=True)
        return None


def compound_article(word: str) -> str | None:
    """Род по правилу композита — только если ВСЕ разборы слова согласны между собой."""
    genus, ambiguous = _load()
    w = str(word or "").strip().lower()
    if len(w) < 8 or w in ambiguous or w in _bad_forms or w.endswith(_COMPOUND_EXCEPTIONS):
        return None

    # Композит через дефис разбирать не нужно — он уже разобран автором написания:
    # «Stumm-Modus», «Corona-Regel», «Online-Termin». Род берёт последняя часть, и
    # догадки здесь нет ни одной, в отличие от слитного композита, который приходится
    # резать перебором. Часть после дефиса проверяется по тем же правилам: она должна
    # быть известным однозначным словом, иначе молчим.
    if "-" in w:
        head = w.rsplit("-", 1)[-1].strip()
        if len(head) >= 4 and head not in ambiguous and head in genus:
            return genus[head]
        return None

    verdicts: set[str] = set()
    for cut in range(4, len(w) - 3):
        pre_raw = w[:cut]
        pre_ok = None
        for pre in (pre_raw, pre_raw.rstrip("s"), pre_raw.rstrip("n"),
                    pre_raw[:-2] if pre_raw.endswith("es") else ""):
            if pre and len(pre) >= 4 and pre in genus:
                pre_ok = pre
                break
        if not pre_ok:
            continue
        for head in (w[cut:], w[cut + 1:] if w[cut] in "sn" else ""):
            if not head or len(head) < 4:
                continue
            if head in _DERIV_SUFFIXES or head in ambiguous or head not in genus:
                continue
            verdicts.add(genus[head])
    # Ровно один вердикт — правило сработало. Больше одного — разбор неоднозначен, молчим.
    return next(iter(verdicts)) if len(verdicts) == 1 else None


def authoritative_article(word: str, *, allow_network: bool = False) -> tuple[str | None, str]:
    """(артикль, откуда). None означает «не знаем» — слово нельзя пускать в игру молча."""
    w = str(word or "").strip()
    if not w:
        return None, "пустое слово"
    genus, ambiguous = _load()
    low = w.lower()
    if low in _bad_forms:
        # Это не «не знаем», а «знаем, что спрашивать нечего»: у формы множественного
        # числа артикль всегда die, и род леммы ей навязывать нельзя — именно так
        # рождалась карточка «der Handschuhe».
        return None, "негодная форма слова"
    if low in ambiguous:
        return None, "двухродовое (артикль зависит от значения)"
    if low in genus:
        return genus[low], ("банк артиклей" if low in _bank_sourced else "wiktionary")
    if allow_network:
        live = _wiktionary_live(w)
        if live:
            return live, "wiktionary-live"
    head = compound_article(w)
    if head:
        return head, "правило композита"
    return None, "нет данных"
