"""Слой СТАТЕЙ: написание → список словарных статей, а не одна строка.

Зачем понадобился (разбор 11.08.2026). Быстрый словарь спрашивал машинных
переводчиков и получал строку без грамматики, после чего часть речи выводилась из
ОРФОГРАФИИ ответа: заглавная буква = существительное. Человек набрал «Толстый» с
большой буквы, как пишут первое слово в строке, — переводчик вернул «Der Dicke», и
карточка ушла про толстяка вместо прилагательного «dick». Замер по живой базе: за
две минуты система записала три взаимоисключающих ответа на одно слово, а верный
(«dick») приехал вообще без части речи.

Так словари не устроены. У PONS, dict.cc, DWDS единица хранения — СТАТЬЯ: лемма +
часть речи + род + значения, и она рождается целиком. Неоднозначность они не
решают за человека, а показывают списком: «толстый» → dick (прил.) · der Dicke
(сущ.). Ошибиться там нельзя, потому что выбора «один ответ» просто нет.

Правила этого модуля, из которых он весь и состоит:

  1. РЕГИСТР НЕ ЗНАЧИТ НИЧЕГО. Ключ поиска нормализован, «Толстый» и «толстый» —
     один запрос. Часть речи берётся ТОЛЬКО из справочников, никогда из написания.
  2. РОД — только из справочника родов или из нашей единицы. Молчание честнее
     догадки (тот же контракт, что у german_surface).
  3. НЕОДНОЗНАЧНОСТЬ ОТДАЁМ ЦЕЛИКОМ. Нашлось три статьи — вернём три, а выбирать
     будет человек.
  4. НИ ОДНОГО ОБРАЩЕНИЯ В СЕТЬ И НИ ОДНОГО К МОДЕЛИ. Всё лежит у нас: 26 тысяч
     статей базового словаря, 22 тысячи родов, 40 тысяч своих единиц.

Источники, в порядке убывания прав:
  • свои единицы (bt_3_lex_units) — у них есть разбор, род и часть речи;
  • базовый словарь (bt_base_dictionary, FreeDict deu-rus, CC-BY-SA) — часть речи
    и переводы;
  • справочник родов (bt_3_wiktionary_genus_cache) — только род, к уже найденному.

Порядок в списке — по частотности слова (bt_3_word_frequency, 49 739 лемм из
корпуса субтитров). Так же сортируют выдачу все ведущие словари: сверху то, что
человек встретит в жизни чаще.
"""
from __future__ import annotations

import logging
import re

from backend.lex_units import normalize_query

_SPACE_RE = re.compile(r"\s+")
_CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")
_LEADING_ARTICLE_RE = re.compile(r"^(der|die|das)\s+", re.IGNORECASE)

# Части речи, которые вообще имеет смысл показывать отдельной строкой выбора.
# Всё остальное (частицы, артикли, сокращения) в список идёт, но ниже.
_POS_ORDER = {
    "noun": 0, "verb": 1, "adjective": 2, "adverb": 3,
    "preposition": 4, "conjunction": 5, "pronoun": 6, "numeral": 7,
}

_GENUS_TO_ARTICLE = {"m": "der", "f": "die", "n": "das"}

# Дальше этого числа список перестаёт помогать и начинает пугать.
MAX_ENTRIES = 6


def _lang(value) -> str:
    return str(value or "").strip().lower()


def _is_german_query(text: str) -> bool:
    return not _CYRILLIC_RE.search(str(text or ""))


def _clean(text) -> str:
    return _SPACE_RE.sub(" ", str(text or "").strip())


def _article_for(genus: str) -> str:
    """der/die/das из кода рода. Многородовые («mn» у «der/das Band») отдаём пустым:
    напечатать один артикль из двух — соврать, а показать оба здесь нечем."""
    return _GENUS_TO_ARTICLE.get(_lang(genus), "")


def _display_of(headword: str, pos: str, gender: str) -> str:
    """Как статья выглядит в списке. Существительное — с артиклем, если род известен;
    без рода — голым словом, потому что выдуманный артикль хуже отсутствующего."""
    if pos == "noun" and gender:
        return f"{gender} {headword}"
    return headword


def _dedupe_words(values) -> list[str]:
    """Переводы без повторов, различающихся только регистром: «сосна» и «Сосна» —
    один перевод, а не два. Наши единицы приходят из разных проходов, и без этого
    в карточку уезжало «сосна, Челюсть, сосна, сосна»."""
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        word = _clean(value)
        if not word:
            continue
        key = word.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(word)
    return out


def _entry(headword, *, pos="", gender="", translations=(), source="", unit_id=0, rank=None):
    # Заголовок хранится БЕЗ артикля, всегда. Наши единицы отдают display уже с
    # артиклем («die Kiefer»), и без снятия получалось «die die Kiefer», а поиск
    # частотности искал слово «das haus» и ничего не находил.
    bare = _clean(headword)
    inline_article = ""
    match = _LEADING_ARTICLE_RE.match(bare)
    if match:
        inline_article = match.group(1).lower()
        bare = bare[match.end():].strip()
    resolved_gender = _lang(gender) or inline_article
    words = _dedupe_words(translations)
    return {
        "headword": bare,
        "pos": _lang(pos),
        "gender": resolved_gender,
        "display": _display_of(bare, _lang(pos), resolved_gender),
        "translations": words[:6],
        "translation": words[0] if words else "",
        "sources": [source] if source else [],
        "unit_id": int(unit_id or 0),
        "rank": rank,
        # Подпись «это форма» заполняется только тогда, когда статью нашли ПО ФОРМЕ
        # (см. _lemmas_of_form). Для обычного запроса поля пустые, и экран молчит.
        "form_of": "",
        "asked_form": "",
    }


def _merge(into: dict, other: dict) -> None:
    """Одна и та же статья, найденная дважды. Побеждает та сторона, у которой знание
    ЕСТЬ: пустое поле никогда не затирает заполненное — иначе базовый словарь без
    рода стирал бы род, который мы уже знаем от своей единицы."""
    if not into.get("pos") and other.get("pos"):
        into["pos"] = other["pos"]
    if not into.get("gender") and other.get("gender"):
        into["gender"] = other["gender"]
    if not into.get("unit_id") and other.get("unit_id"):
        into["unit_id"] = other["unit_id"]
    if into.get("rank") is None and other.get("rank") is not None:
        into["rank"] = other["rank"]
    if not into.get("form_of") and other.get("form_of"):
        into["form_of"] = other["form_of"]
        into["asked_form"] = other.get("asked_form") or ""
    into["translations"] = _dedupe_words(
        list(into["translations"]) + list(other.get("translations") or [])
    )[:6]
    if not into.get("translation") and into["translations"]:
        into["translation"] = into["translations"][0]
    for source in other.get("sources") or []:
        if source not in into["sources"]:
            into["sources"].append(source)
    into["display"] = _display_of(into["headword"], into["pos"], into["gender"])


def _key_of(entry: dict) -> tuple[str, str, str]:
    """Статьи различаются написанием, частью речи И родом. «essen» глагол и «Essen»
    существительное — две статьи. «der Kiefer» (челюсть) и «die Kiefer» (сосна) —
    тоже две: это разные слова, и склеивать их в одну строку означало бы повторить
    ту самую ошибку, из-за которой «die Dicke» получила примеры про «der Dicke»."""
    return (normalize_query(entry["headword"]), entry["pos"], entry["gender"])


def _fold_genderless_nouns(entries: list[dict]) -> list[dict]:
    """Существительное без рода рядом с тем же словом, у которого род известен.

    Такое приходит из базового словаря: род в нём не проставлен ни у одной статьи
    (замер 11.08.2026 — 0 из 26 809), а справочник родов о слове молчит или знает
    сразу два («der/das Band»). Показывать его отдельной безродной строкой нельзя —
    человек увидит один и тот же «Kiefer» дважды и решит, что программа сломалась.

    Если родовая статья ровно одна — вливаем переводы в неё. Если их несколько
    (настоящие омографы), безродную ОТБРАСЫВАЕМ: у наших единиц переводы разложены
    по родам правильно, а слитый ком из базового словаря только испортил бы их."""
    gendered: dict[tuple[str, str], list[dict]] = {}
    for entry in entries:
        if entry["pos"] == "noun" and entry["gender"]:
            gendered.setdefault((normalize_query(entry["headword"]), entry["pos"]), []).append(entry)
    out: list[dict] = []
    for entry in entries:
        if entry["pos"] != "noun" or entry["gender"]:
            out.append(entry)
            continue
        siblings = gendered.get((normalize_query(entry["headword"]), entry["pos"]))
        if not siblings:
            out.append(entry)
        elif len(siblings) == 1 and not siblings[0]["translations"]:
            # Родовая статья есть, но пустая — тогда переводы безродной ей нужны.
            # Если же они у неё УЖЕ есть, безродную просто отбрасываем: приписать
            # «группа» к «das Band», когда группа это «die Band», — та же ошибка,
            # из-за которой «die Dicke» получила примеры про «der Dicke».
            _merge(siblings[0], entry)
    return out


# ─────────────────────────── источники ───────────────────────────


def _from_units(cur, key: str, *, query_lang: str, other_lang: str) -> list[dict]:
    """Наши собственные единицы. Спрашиваем ПЕРВЫМИ: у них есть и часть речи, и род,
    и разбор, за который уже заплачено."""
    cur.execute(
        """
        SELECT u.id, u.display, u.pos, u.gender, u.lang
        FROM bt_3_lex_surfaces s
        JOIN bt_3_lex_units u ON u.id = s.unit_id
        WHERE s.lang = %s AND s.surface_key = %s
        LIMIT 12;
        """,
        (query_lang, key),
    )
    rows = cur.fetchall()
    if not rows:
        return []
    out: list[dict] = []
    for unit_id, display, pos, gender, lang in rows:
        cur.execute(
            """
            SELECT t.display
            FROM bt_3_lex_links l
            JOIN bt_3_lex_units t
              ON t.id = CASE WHEN l.from_unit = %s THEN l.to_unit ELSE l.from_unit END
            WHERE (l.from_unit = %s OR l.to_unit = %s) AND t.lang = %s
            ORDER BY l.rank
            LIMIT 6;
            """,
            (unit_id, unit_id, unit_id, other_lang),
        )
        translations = [r[0] for r in cur.fetchall() if r[0]]
        if lang == "de":
            out.append(_entry(display, pos=pos or "", gender=gender or "",
                              translations=translations, source="units", unit_id=unit_id))
        else:
            # Спросили по-русски: статьёй считается НЕМЕЦКАЯ сторона связи, русское
            # слово — это то, что человек и так набрал.
            cur.execute(
                """
                SELECT t.id, t.display, t.pos, t.gender
                FROM bt_3_lex_links l
                JOIN bt_3_lex_units t
                  ON t.id = CASE WHEN l.from_unit = %s THEN l.to_unit ELSE l.from_unit END
                WHERE (l.from_unit = %s OR l.to_unit = %s) AND t.lang = %s
                ORDER BY l.rank
                LIMIT 6;
                """,
                (unit_id, unit_id, unit_id, other_lang),
            )
            for de_id, de_display, de_pos, de_gender in cur.fetchall():
                out.append(_entry(de_display, pos=de_pos or "", gender=de_gender or "",
                                  translations=[display], source="units", unit_id=de_id))
    return out


def _from_base_forward(cur, key: str) -> list[dict]:
    """Базовый словарь по НЕМЕЦКОМУ написанию: «essen» → глагол «есть» И
    существительное «Essen» (еда)."""
    cur.execute(
        """
        SELECT lemma, pos, translations_ru
        FROM bt_base_dictionary
        WHERE lemma_key = %s AND source_lang = 'de'
        LIMIT 8;
        """,
        (key,),
    )
    return [
        _entry(lemma, pos=pos or "", translations=translations or [], source="freedict")
        for lemma, pos, translations in cur.fetchall()
    ]


def _from_base_reverse(cur, word_ru: str) -> list[dict]:
    """Базовый словарь по РУССКОМУ слову: «толстый» → dick, feist, blad — и все три
    честно помечены прилагательными.

    Точное совпадение по элементу массива, не «похоже»: словарь обязан отвечать за
    то, что показывает. Индекс GIN по translations_ru делает это мгновенным."""
    cur.execute(
        """
        SELECT lemma, pos, translations_ru
        FROM bt_base_dictionary
        WHERE source_lang = 'de' AND translations_ru @> ARRAY[%s]::text[]
        LIMIT 16;
        """,
        (word_ru,),
    )
    return [
        _entry(lemma, pos=pos or "", translations=[word_ru], source="freedict")
        for lemma, pos, translations in cur.fetchall()
    ]


def _attach_gender(cur, entries: list[dict]) -> None:
    """Род существительным — из справочника родов. Только тем, у кого его ещё нет.

    И только тем написаниям, о которых МОЛЧАТ наши единицы. Если единица про слово
    уже есть, род берём у неё: справочник знает одну строку на написание и на
    «Kiefer» отвечает «f», из-за чего статья базового словаря (где челюсть и сосна
    слиты в один ком) приклеивалась к «die Kiefer» и приносила ей чужую «челюсть»."""
    ours = {
        (normalize_query(e["headword"]), e["pos"])
        for e in entries if "units" in (e.get("sources") or []) and e["gender"]
    }
    wanted = [
        e["headword"] for e in entries
        if e["pos"] == "noun" and not e["gender"]
        and (normalize_query(e["headword"]), e["pos"]) not in ours
    ]
    if not wanted:
        return
    cur.execute(
        "SELECT title, genus FROM bt_3_wiktionary_genus_cache WHERE lower(title) = ANY(%s);",
        ([w.lower() for w in wanted],),
    )
    by_word = {str(t).lower(): g for t, g in cur.fetchall()}
    for entry in entries:
        if entry["pos"] != "noun" or entry["gender"]:
            continue
        article = _article_for(by_word.get(entry["headword"].lower(), ""))
        if article:
            entry["gender"] = article
            entry["display"] = _display_of(entry["headword"], entry["pos"], article)


def _attach_frequency(cur, entries: list[dict]) -> None:
    """Частотность — ключ сортировки. Нет её у слова — уводим в конец списка, но не
    выбрасываем: редкое слово всё равно может быть тем самым, что человек искал."""
    wanted = [e["headword"].lower() for e in entries if e.get("rank") is None]
    if not wanted:
        return
    cur.execute(
        "SELECT lower(lemma), MIN(rank) FROM bt_3_word_frequency "
        "WHERE lang = 'de' AND lower(lemma) = ANY(%s) GROUP BY 1;",
        (wanted,),
    )
    by_word = dict(cur.fetchall())
    for entry in entries:
        if entry.get("rank") is None:
            entry["rank"] = by_word.get(entry["headword"].lower())


def _attach_examples(cur, entries: list[dict], limit: int = 2) -> None:
    """Примеры к статье — те, что лежат на САМОМ СЛОВЕ.

    Зачем именно они, а не корпус. Корпус ищется по написанию, а написание не
    различает разные слова: «der Kiefer» (челюсть) и «die Kiefer» (сосна) пишутся
    одинаково, «Wehe» (схватка) и «wehe» (горе) — тоже. Наши примеры привязаны к
    конкретному слову, и перепутать их невозможно: у «der Kiefer» — «Der Kiefer ist
    gebrochen», у «die Kiefer» — «Im Wald wachsen viele Kiefern».

    Владелец 15.08.2026 увидел обратное: выбрал глагол «wehen» (веять), а под ним
    стояло «Die Wehen haben eingesetzt» — Схватки начались. Это существительное.
    """
    unit_ids = [int(e.get("unit_id") or 0) for e in entries if e.get("unit_id")]
    if not unit_ids:
        return
    cur.execute(
        "SELECT id, card -> 'usage_examples' FROM bt_3_lex_units "
        "WHERE id = ANY(%s) AND card IS NOT NULL;",
        (unit_ids,),
    )
    by_unit = {int(row[0]): row[1] for row in cur.fetchall()}
    for entry in entries:
        raw = by_unit.get(int(entry.get("unit_id") or 0))
        if not isinstance(raw, list):
            continue
        picked = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            source = _clean(item.get("source") or item.get("de") or "")
            target = _clean(item.get("target") or item.get("ru") or "")
            if not source:
                continue
            picked.append({"source": source, "target": target})
            if len(picked) >= limit:
                break
        if picked:
            entry["examples"] = picked


# ─────────────────────────── вход ───────────────────────────


def _lemmas_of_form(text: str) -> list[str]:
    """Словарные формы, которым принадлежит это написание. Пусто — справочник молчит.

    Источник — `bt_3_german_verb_paradigms`, таблицы со страниц Flexion de.wiktionary.
    Здесь НЕТ ни модели, ни арифметики: если справочник не знает, мы честно возвращаем
    пусто, и выше по цепочке отработает машинный перевод с честной подписью.

    Молчание справочника — не норма, а незакрытая задача: пополняет его ночной прогрев
    `warm_verb_paradigms`, который с 01.09.2026 спрашивает и про ОСНОВЫ, а не только
    про наши единицы.
    """
    try:
        from backend.german_verb_paradigms import verbs_of_form
        return verbs_of_form(text)
    except Exception:
        logging.debug("справочник форм не ответил про %r", text, exc_info=True)
        return []


def entries_for_query(query: str, *, source_lang: str, target_lang: str,
                      limit: int = MAX_ENTRIES) -> list[dict]:
    """Список словарных статей для написания. Пустой список — «нам это слово
    незнакомо», и тогда вызывающая сторона идёт в переводчик.

    ВАЖНО: пустой список не значит «ошибка». Он значит ровно то, что сказано, и
    подсовывать вместо него догадку — это ровно та болезнь, от которой модуль."""
    text = _clean(query)
    if not text or len(text) > 64:
        return []
    # АРТИКЛЬ — НЕ ФРАЗА. Приложение показывает существительные с артиклем («die Habe»),
    # человек набирает ровно то, что видит, — и до 15.08.2026 получал «этого слова нет
    # в словаре»: проверка ниже видела пробел и объявляла запрос фразой, не заглянув в
    # базу ни разу. Слово при этом лежало на месте.
    # Артикль запоминаем: он уточняет выбор, а не мешает. «der Kiefer» — челюсть,
    # «die Kiefer» — сосна, и это разные статьи.
    asked_article = ""
    article_match = _LEADING_ARTICLE_RE.match(text)
    if article_match:
        asked_article = article_match.group(1).lower()
        text = text[article_match.end():].strip()
    if not text or " " in text:
        # Фразы и предложения — работа переводчика, а не словаря статей.
        return []
    query_lang = _lang(source_lang) or ("de" if _is_german_query(text) else "ru")
    other_lang = _lang(target_lang) or ("ru" if query_lang == "de" else "de")
    if query_lang == other_lang:
        return []
    key = normalize_query(text)
    if not key:
        return []

    try:
        from backend.database import get_db_connection_context
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                raw = _from_units(cur, key, query_lang=query_lang, other_lang=other_lang)
                if query_lang == "de":
                    raw += _from_base_forward(cur, key)
                elif other_lang == "de":
                    raw += _from_base_reverse(cur, key)
                raw = [item for item in raw if item["headword"]]
                # Спросили НЕ ЗАГОЛОВОК: либо не нашли ничего, либо нашли по указателю
                # словоформы, и написание не совпало ни с одной леммой. Оба случая —
                # повод спросить справочник, чья это форма. Когда человек набрал саму
                # лемму («gehen», «arbeiten»), сюда не заходим вовсе: лишнего запроса
                # к справочнику на горячем пути быть не должно.
                asked_a_headword = any(
                    normalize_query(item["headword"]) == key for item in raw
                )
                if query_lang == "de" and not asked_a_headword:
                    # СЛОВОФОРМА. Человек нажал слово в фильме или в книге, а там оно
                    # почти всегда стоит в форме: «wühlt», «ging», «nimmt». Словарь —
                    # словарь ЛЕММ, поэтому раньше он здесь молчал, и строка «ПЕРЕВОД»
                    # уходила к машинному переводчику. Тот переводит голую форму,
                    # достраивая русскую под немецкую, и 01.09.2026 выдал владельцу
                    # «рыется» — слова, которого в русском языке нет (правильно
                    # «роется»). Верный ответ при этом лежал у нас: «wühlen — рыться».
                    #
                    # Спрашиваем СПРАВОЧНИК, а не модель: `verbs_of_form` отдаёт те
                    # глаголы, у которых написание НАПЕЧАТАНО целой ячейкой таблицы
                    # Flexion. Так же отвечают Wiktionary («ging — Konjugierte Form des
                    # Verbs gehen»), PONS, Duden и dict.cc. Ничего не достраиваем: молчит
                    # справочник — молчим и мы, и тогда работает машинный перевод,
                    # подписанный как машинный.
                    lemmas = _lemmas_of_form(text)
                    for lemma in lemmas:
                        lemma_key = normalize_query(lemma)
                        if not lemma_key or lemma_key == key:
                            continue
                        found = _from_units(cur, lemma_key, query_lang=query_lang,
                                            other_lang=other_lang)
                        found += _from_base_forward(cur, lemma_key)
                        for item in found:
                            if not item["headword"]:
                                continue
                            # Справочник сказал: это форма ГЛАГОЛА. Существительное с
                            # тем же написанием леммы такой формы не имеет — «fängt» не
                            # форма слова «das Fangen» (салочки), а «gräbt» не форма
                            # слова «der Graben» (канава). Раньше они выигрывали по
                            # частотности и отвечали вместо глагола.
                            # Часть речи неизвестна — не судим и берём: молчание нашего
                            # банка не повод выбросить единственный найденный ответ.
                            if item["pos"] and item["pos"] != "verb":
                                continue
                            # Подпись для экрана: человек нажал «wühlt», а видит
                            # «wühlen» — он обязан понимать, почему.
                            item["form_of"] = lemma
                            item["asked_form"] = text
                            raw.append(item)
                if not raw:
                    return []
                # Род ставится ДО склейки: он входит в ключ статьи, и без него
                # «der Kiefer» и «die Kiefer» слились бы в одну строку.
                _attach_gender(cur, raw)
                raw = _fold_genderless_nouns(raw)
                collected: dict[tuple[str, str, str], dict] = {}
                for item in raw:
                    entry_key = _key_of(item)
                    if entry_key in collected:
                        _merge(collected[entry_key], item)
                    else:
                        collected[entry_key] = item
                entries = list(collected.values())
                _attach_frequency(cur, entries)
                _attach_examples(cur, entries)
    except Exception as exc:
        logging.debug("слой статей промолчал для %r: %s", query, exc)
        return []

    # Статью без единого перевода показывать нечем — человек увидит слово и не
    # узнает, что оно значит.
    entries = [e for e in entries if e["translations"]]
    entries.sort(key=lambda e: (
        # Статья, подтверждённая справочником форм, идёт первой: человек нажал «fängt»,
        # и это форма ГЛАГОЛА «fangen». Существительное «das Fangen» (салочки) такой
        # формы не имеет — оно лишь совпадает написанием с леммой, и до 01.09.2026
        # выигрывало по частотности, отвечая на «fängt» словом «салочки».
        # Из списка оно НЕ пропадает: омонимы человек должен видеть рядом.
        0 if e.get("form_of") else 1,
        e["rank"] if e["rank"] is not None else 10 ** 9,
        _POS_ORDER.get(e["pos"], 9),
        e["headword"].lower(),
    ))
    # Спросили с артиклем — значит человек уже сказал, какое из слов ему нужно.
    # Ставим подходящие по роду вперёд. НЕ отбрасываем остальные: артикль мог быть
    # набран по привычке или неверно, и молча спрятать нужную статью — хуже.
    if asked_article:
        entries.sort(key=lambda e: 0 if str(e.get("gender") or "").lower() == asked_article else 1)
    return entries[:limit]


def known_in_base_dictionary(word: str, lang: str = "de") -> bool:
    """Есть ли это написание в ВЫВЕРЕННОМ внешнем словаре (FreeDict, 26 630 статей).

    Отдельно от entries_for_query, и это принципиально. entries_for_query спрашивает
    ещё и НАШИ единицы — а там живут наши же прошлые ошибки и «двери» для опечаток:
    «Bestürtz» ведёт на «bestürzt», «Neugeborenes» когда-то завелось словом, хотя
    такого слова нет. Для показа человеку это нормально, он получит верную статью.

    Но там, где решается «исправлять ли написание», своими данными пользоваться
    нельзя: слово, попавшее к нам по ошибке, само себя объявит правильным и никогда
    не починится. Поэтому вето корректору даёт только внешний словарь."""
    key = normalize_query(word)
    if not key:
        return False
    try:
        from backend.database import get_db_connection_context
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM bt_base_dictionary "
                    "WHERE source_lang = %s AND lemma_key = %s LIMIT 1;",
                    (str(lang or "de"), key),
                )
                return cur.fetchone() is not None
    except Exception:
        logging.debug("базовый словарь не ответил про %r", word, exc_info=True)
        return False


def primary_entry(entries: list[dict]) -> dict | None:
    """Статья, которую показываем крупно. Это просто первая по частотности — но
    выбор остальных при этом никуда не девается, он рядом на экране."""
    return entries[0] if entries else None
