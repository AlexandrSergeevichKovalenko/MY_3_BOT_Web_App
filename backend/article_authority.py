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
# Написания, которые ДРУГАЯ строка банка держит своим полем «множественное число».
# Это защита, которой не нужна ничья дисциплина: она читает только те данные, что уже
# лежат в таблице, и работает, даже если снятие сделали в обход двери и причину забыли.
#
# Замер 17.08.2026 на живой базе: ответ не изменился НИ У ОДНОГО слова, род потеряли
# ровно 25 написаний — и у всех молчание и есть правильный ответ: формы множественного
# (die Zitate, die Mängel, die Bögen), законные pluralia tantum (die Schulden),
# субстантивированные прилагательные (die Erwachsene — у них артикль по полу человека)
# и сломанные строки вроде «der Pins». Законные слова, похожие на множественное
# (die Kohle, die Montage, der Westen), правило НЕ задевает: их род даёт Wiktionary,
# а он идёт первым.
_plural_spellings: set[str] = set()
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
        from backend.database import get_db_connection_context, list_bad_word_forms
        # Написания, которые мы САМИ признали негодной формой слова, родов не
        # поставляют. Иначе снятая строка продолжает решать род во всём продукте:
        # 17.08.2026 «das Fotos» уже был снят из игры, а справочник на вопрос про
        # «Fotos» по-прежнему отвечал «das» — строка осталась в таблице, а выборка
        # не смотрела на решение.
        #
        # Условие узкое НАМЕРЕННО. Замер 17.08.2026 обеих правок:
        #   «не брать все снятые строки»  → род потеряли бы 1307 слов, приобрело 1,
        #                                   изменилось 0;
        #   «не брать негодные написания» → род потеряли ровно 8, изменилось 0, и это
        #                                   ровно те формы множественного числа,
        #                                   которые убрали руками.
        # Снятие само по себе НЕ означает «слово неправильное»: строку снимают и за
        # дубль, и за ротацию освоенного — род у них верный.
        #
        # Список берём у двери снятия (database.list_bad_word_forms), а не собираем
        # своим запросом: там причина живёт на самой строке и записывается тем же
        # UPDATE, что снимает слово, — забыть её нельзя.
        bad_forms = list_bad_word_forms()
        _bad_forms.clear()
        _bad_forms.update(bad_forms)
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT title, genus FROM bt_3_wiktionary_genus_cache "
                            "WHERE COALESCE(genus,'') <> ''")
                g2a = {"m": "der", "f": "die", "n": "das"}
                for title, code in cur.fetchall() or []:
                    c = str(code or "").strip().lower()
                    if len(c) == 1 and c in g2a:      # 'mf'/'nf' = два рода → не берём
                        genus[str(title).strip().lower()] = g2a[c]
                cur.execute("SELECT word, article, COALESCE(plural, '') "
                            "FROM bt_3_article_sprint_nouns "
                            "WHERE COALESCE(article,'') <> ''")
                bank_rows = cur.fetchall() or []
                plural_spellings = set()
                for w, _a, pl in bank_rows:
                    p_low = str(pl or "").strip().lower()
                    if p_low and p_low != str(w).strip().lower():
                        plural_spellings.add(p_low)
                _plural_spellings.clear()
                _plural_spellings.update(plural_spellings)
                for w, a, _pl in bank_rows:
                    lw = str(w).strip().lower()
                    # Банк не поставляет род ни негодным написаниям (решение записано),
                    # ни тем, что числятся чужим множественным (видно по самим данным).
                    if lw in bad_forms or lw in plural_spellings:
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


def compound_heads(word: str) -> set[str]:
    """Все ГОЛОВЫ, какие даёт разбор составного слова. Пусто — шва нет.

    Вынесено из `compound_article` 18.08.2026, чтобы род и СКЛОНЕНИЕ пользовались одним
    и тем же швом. Второго правила разреза в проекте быть не должно: именно наивное
    отрезание хвоста когда-то дало «das Schwert → der» — «wert» похоже на «der Wert»,
    хотя никакого шва в слове нет. Здесь шов ДОКАЗЫВАЕТСЯ: обе части обязаны быть
    известными однозначными словами справочника.
    """
    genus, ambiguous = _load()
    w = str(word or "").strip().lower()
    if (len(w) < 8 or w in ambiguous or w in _bad_forms or w in _plural_spellings
            or w.endswith(_COMPOUND_EXCEPTIONS)):
        return set()
    if "-" in w:
        head = w.rsplit("-", 1)[-1].strip()
        if len(head) >= 4 and head not in ambiguous and head in genus:
            return {head}
        return set()
    heads: set[str] = set()
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
            heads.add(head)
    return heads


def compound_head(word: str) -> str | None:
    """Одна голова или None. Разборов несколько — молчим, склонять не по чему."""
    heads = compound_heads(word)
    return next(iter(heads)) if len(heads) == 1 else None


def compound_article(word: str) -> str | None:
    """Род по правилу композита — только если ВСЕ разборы слова согласны между собой.

    Шов ищет `compound_heads` — он же обслуживает склонение. Здесь остаётся только
    перевод голов в роды: разрезов у слова бывает несколько, а род у них обычно один
    и тот же. Ровно один вердикт — правило сработало. Больше одного — разбор
    неоднозначен, молчим.
    """
    genus, _ = _load()
    verdicts = {genus[h] for h in compound_heads(word) if h in genus}
    return next(iter(verdicts)) if len(verdicts) == 1 else None


def article_if_already_loaded(word: str) -> str | None:
    """Род ТОЛЬКО из уже прогретой памяти. В базу и в сеть отсюда не ходим ни при каких
    условиях — эта дверь стоит на живом пути сохранения слова.

    ЗАЧЕМ ОТДЕЛЬНАЯ ФУНКЦИЯ. `authoritative_article` при холодном кэше идёт в базу за
    справочником родов (19 тысяч строк). На пути сохранения это недопустимо дважды:
    человек ждёт, а запрос уходит из ЧУЖОЙ транзакции — второе соединение из пула, пока
    первое не отпущено, это известная ловушка проекта. Поэтому здесь: знаем — отвечаем,
    не знаем — молчим, и ночная сверка (`fix_gender_conflicts_from_authority`) доберёт.
    Дверь ловит бесплатно то, что может; остальное ловит ночь.
    """
    with _lock:
        if not _genus:
            return None
        low = str(word or "").strip().lower()
        if not low or low in _ambiguous or low in _bad_forms or low in _plural_spellings:
            return None
        return _genus.get(low)


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
    # Сюда доходим, только когда ни Wiktionary, ни банк слова не знают. Если оно при
    # этом числится чужим множественным — молчим и в сеть не идём: у формы
    # множественного числа артикль всегда die, и род леммы ей навязывать нельзя.
    if low in _plural_spellings:
        return None, "форма множественного числа другого слова"
    # ТАБЛИЦА СКЛОНЕНИЙ — ЭТО ТОЖЕ СПРАВОЧНИК, и спросить её надо ДО сети.
    #
    # 23.08.2026 в `bt_3_german_noun_declensions` загружено 89 704 таблицы вместо
    # прежних 2 909 (офлайн-выгрузка de.wiktionary). Арбитр о них не знал и ходил в
    # сеть впустую. Замер по всем 2881 нашим существительным: 249 слов таблица знает,
    # а арбитр — нет. Среди них «Kurzbefehl», «Bugfahrwerk», «Wildcard».
    #
    # РОВНО ОДИН РОД, ИНАЧЕ МОЛЧИМ. У части записей в таблице лежат ДВА рода — там,
    # где разбор страницы был неоднозначен:
    #
    #     nebel  ['f','m']   «die Nebel» / «der Nebel»
    #     mund   ['f','m']   «die Mund» — такого не существует
    #     zelt   ['m','n']   «der Zelt» — такого не существует
    #
    # Взять первый попавшийся значило бы раздать артикль по жребию: на замере это дало
    # 29 «расхождений» с арбитром, и все 29 были моей ошибкой отбора, а не данными.
    # Правило то же, что у `compound_article`: один вердикт — отвечаем, больше одного —
    # молчим.
    from_declension, _почему = _article_from_declension(w)
    if from_declension:
        return from_declension, "справочник склонений"
    if allow_network:
        live = _wiktionary_live(w)
        if live:
            return live, "wiktionary-live"
    head = compound_article(w)
    if head:
        return head, "правило композита"
    return None, "нет данных"


def _article_from_declension(word: str):
    """Артикль из напечатанной таблицы склонения. (артикль, почему) — как у соседа.

    Читатель ОДИН на всё приложение: `backend/noun_declension_reference.py`. Своего
    второго здесь быть не должно — я его написал и тут же выбросил, когда сосед сделал
    общий: два читателя одной таблицы неизбежно разъезжаются в правилах, а правил тут
    три и все неочевидные.

    Что стережёт общий читатель:
        два рода в записи   «die Nebel»/«der Nebel» — выбирать не наше дело, молчим.
                            Я на этом уже ошибся: брал первый попавшийся ключ и
                            получил 29 «расхождений» с арбитром, все 29 — моя ошибка.
        множественное       именительный единственного обязан совпасть с самим словом,
                            иначе это форма («Türen» от «die Tür»).
        нет множественного  обычно имя собственное или субстантивация («das Athen»).

    Слой БД подключается внутри: этот модуль зовут и оффлайн-скрипты, и тесты без базы.
    """
    try:
        from backend.noun_declension_reference import article_from_declension_reference
        return article_from_declension_reference(word)
    except Exception:
        logging.debug("арбитр: справочник склонений недоступен для %s", word, exc_info=True)
        return None, "справочник склонений недоступен"