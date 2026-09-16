# -*- coding: utf-8 -*-
"""Артикль головному существительному именной группы «сущ. + родительный».

┌─ ПОВОД, владелец 15.09.2026 ──────────────────────────────────────────────────────┐
│ В тренажёре стояла карточка «Vollstrecker einer Anordnung» — «исполнитель          │
│ распоряжения». У зависимого слова артикль есть («einer Anordnung»), у головного   │
│ нет. А соседние записи ТОЙ ЖЕ пачки, сохранённые в ту же секунду, идут с артиклем:│
│     der Vollstrecker der Strafe          (карточка 330783)                        │
│     Vollstrecker einer Anordnung         (карточка 330784)  ← жалоба               │
│     der Vollstrecker von Gerichtsurteilen (карточка 330787)                        │
│ Одна конструкция, один источник, один момент — два разных формата.                │
└───────────────────────────────────────────────────────────────────────────────────┘

ЧТО ЗДЕСЬ НЕ ЧИНИТСЯ И ПОЧЕМУ. Не «у выражений должен быть артикль» — это неверно.
Проверка 15.09.2026 по четырём независимым источникам показала обратное:
  • DWDS у статьи «Herr der Lage» в поле «Grammatik» пишет пометой дословно
    «Mehrwortausdruck, meist ohne Artikel», и заголовок статьи голый;
  • Duden не ставит артикль впереди даже у ОДИНОЧНОГО слова: статья называется
    «Vollstrecker, der» — род пометой после запятой;
  • de.wiktionary держит «Stein des Anstoßes», «Ende der Welt» леммами без артикля;
  • наш собственный FreeDict deu-rus: 32 заголовка «сущ. + генитив», с артиклем — 0.
Поэтому «привести всё к словарному виду» означало бы СНЯТЬ артикль у 88 карточек и
57 записей пула, которые владельца устраивают. Решение владельца 16.09.2026: накопленное
не переписывать, меньшинство подтянуть к большинству.

ЧТО ЧИНИТСЯ. Разнобой. Замер 15.09.2026 по живой базе, конструкция «сущ. + родительный»:
    личные карточки: 88 с артиклем / 45 без
    общий пул:       57 с артиклем / 37 без
У одиночных слов разнобоя нет — артикль стоит у 94,4% карточек. Формат многословного
заголовка сейчас решает не наше правило, а модель: ей в промпте сказано ставить артикль
(`backend/openai_manager.py:3346` — «nouns carry their article»), она слушается через раз,
а `_strip_spurious_leading_article` (`backend/backend_server.py:10627`) артикль СОХРАНЯЕТ,
если он пришёл, и НЕ добавляет, если не пришёл. Проверяющего между ними нет ни одного.

ПОЧЕМУ ЭТО НЕ ПОВТОРИТ «das Adriatisches Meer». Владелец прислал этот дефект 22.08.2026:
механическая приписка артикля к «Adriatisches Meer» даёт неверный немецкий, потому что
при определённом артикле прилагательное обязано склоняться слабо («das Adriatische Meer»).
Правило ломается ровно там, где между артиклем и существительным стоит прилагательное.
Здесь такого места нет по построению: головное существительное — ПЕРВОЕ слово заголовка,
перед ним не стоит ничего, и приписка ничего в строке не меняет. «Adriatisches Meer»
под условие не подпадает — первое слово не существительное — и не трогается.

ИСТОЧНИК АРТИКЛЯ НАЗЫВАЕТСЯ ВСЛУХ: `bt_3_german_noun_declensions`, 89 704 напечатанные
таблицы склонения, читаются через `backend/noun_declension_reference.py`. Там же лежат
три отказа, которые нам и нужны: слова нет в справочнике; у слова несколько родов
(«der/das Liter» — выбирать не наше дело); написание не совпало с именительным
единственного (так ловится форма множественного). Отказ — это «не знаем», и он считается,
а не заменяется догадкой.

ЧЕГО ЗДЕСЬ НЕТ. Ни одного правила «по хвосту слова», ни определения части речи «на глаз»,
ни выбора рода по вероятности. Родительный падеж не выводится — он ЧИТАЕТСЯ: артикль
`des/der/eines/einer` напечатан в самом заголовке вторым словом.
"""
from __future__ import annotations

import re

# Родительный артикль, напечатанный вторым словом. Мы его не выводим — читаем.
# «der» и «einer» омонимичны дательному женскому («Hilfe der Frau»), но на наш ответ это
# не влияет: головное слово в обоих прочтениях одно и то же и стоит в именительном,
# а именно его артикль мы и приписываем.
_GENITIVE_MARKERS = frozenset({"des", "der", "eines", "einer"})

# Артикли в начале строки: он уже стоит — приписывать нечего.
_LEADING_ARTICLES = frozenset({
    "der", "die", "das", "den", "dem", "des",
    "ein", "eine", "einen", "einem", "einer", "eines",
})

# ЗАКРЫТЫЙ КЛАСС СЛУЖЕБНЫХ СЛОВ — не эвристика, а перечень.
#
# Зачем он нужен. Заглавная буква НЕ доказывает существительное: в живых заголовках с
# заглавной написаны и предлоги, потому что строка начинается с начала предложения —
# «Unter der Woche», «Aus der Liste streichen», «Von einer Einnahmequelle abhängig sein».
# Хуже того, справочник склонений знает часть из них КАК СУЩЕСТВИТЕЛЬНЫЕ: «der Unter»
# (карточная фигура), «das Aus» (аут), «der Laut» (звук). Без этого перечня «Unter der
# Woche» превратилось бы в «der Unter der Woche». Замер 15.09.2026: ровно на этих строках
# разошлись два способа счёта (48 против 45), и сошлись после применения перечня.
#
# ┌─ ГЛАВНЫЙ ПОДВОХ ЭТОГО ПРАВИЛА, сухой прогон 16.09.2026 ────────────────────────────┐
# │ Систематические самозванцы здесь — ПРЕДЛОГИ, УПРАВЛЯЮЩИЕ РОДИТЕЛЬНЫМ. Они стоят    │
# │ ровно там же, где головное существительное, и ровно перед родительным падежом,     │
# │ то есть попадают под условие точь-в-точь. Первый прогон выдал «der Laut eines      │
# │ Zeitungsberichts» («согласно газетному сообщению» → «звук газетного сообщения»).   │
# │ Ещё шесть — Angesichts, Entlang, Infolge, Oberhalb, Ungeachtet, Vonseiten —        │
# │ уцелели только потому, что справочник склонений их не знает. Это везение, а не     │
# │ правило, и держаться на нём нельзя.                                                │
# │ Поэтому предлоги родительного перечислены здесь ПОЛНОСТЬЮ, а не по одному по мере  │
# │ находок. Их список в немецком закрыт.                                              │
# └────────────────────────────────────────────────────────────────────────────────────┘
#
# Список немецких предлогов, союзов и местоимений конечен и не растёт — это грамматика
# языка, а не наша подборка примеров.
_NEVER_A_HEAD_NOUN = frozenset({
    # предлоги, управляющие РОДИТЕЛЬНЫМ — главный класс самозванцев, см. врезку выше
    "angesichts", "anhand", "anlässlich", "anstatt", "anstelle", "aufgrund", "ausserhalb",
    "außerhalb", "bezüglich", "betreffs", "binnen", "dank", "diesseits", "entlang",
    "entsprechend", "gemäß", "hinsichtlich", "infolge", "innerhalb", "inmitten", "jenseits",
    "kraft", "laut", "längs", "mangels", "mithilfe", "mittels", "namens", "nördlich",
    "oberhalb", "östlich", "seitens", "seitlich", "statt", "südlich", "trotz", "unterhalb",
    "unweit", "ungeachtet", "vonseiten", "westlich", "während", "wegen", "zeit",
    "zufolge", "zugunsten", "zulasten", "zuungunsten", "zwecks",
    # остальные предлоги
    "an", "auf", "aus", "bei", "bis", "durch", "für", "gegen", "gegenüber", "hinter",
    "in", "mit", "nach", "neben", "ohne", "seit", "über", "um", "unter", "von", "vor",
    "zu", "zwischen", "nebst", "samt",
    "am", "ans", "aufs", "beim", "im", "ins", "vom", "zum", "zur", "fürs", "ums", "durchs",
    # союзы и частицы
    "aber", "als", "auch", "da", "damit", "dann", "dass", "denn", "doch", "nur", "noch",
    "oder", "ob", "obwohl", "schon", "sehr", "so", "sondern", "und", "weil", "wenn", "wie",
    "nicht", "kein", "keine", "mehr", "immer", "wieder", "erst", "etwa", "sogar",
    # местоимения и определители
    "alle", "alles", "beide", "dies", "diese", "dieser", "dieses", "diesen", "diesem",
    "du", "er", "es", "ich", "ihm", "ihn", "ihr", "ihre", "ihrem", "ihren", "ihrer",
    "jede", "jeden", "jeder", "jedes", "jemand", "man", "mein", "meine", "meinem",
    "nichts", "niemand", "sein", "seine", "seinem", "seinen", "seiner", "sich", "sie",
    "uns", "unser", "unsere", "viel", "viele", "was", "wer", "wir", "etwas",
    # вопросительные и указательные наречия
    "wo", "wohin", "woher", "warum", "wann", "wozu", "dabei", "dafür", "damit", "danach",
    "daran", "darauf", "darin", "davon", "dazu", "hier", "dort", "jetzt", "heute",
})

# Слово, написанное буквами: латиница с умлаутами, допускаются дефис и апостроф внутри.
# Нужно, чтобы не принимать за существительное числа, ссылки и обрывки разметки.
_WORD_RE = re.compile(r"^[A-Za-zÄÖÜäöüß][A-Za-zÄÖÜäöüß\-’']*$")

# Знаки конца предложения: если они есть, это не словарная именная группа.
_SENTENCE_END_RE = re.compile(r"[.!?…]")

NOT_THIS_SHAPE = "это не группа «сущ. + родительный»"


def head_noun_of_genitive_phrase(text: str | None) -> str:
    """Головное существительное группы «сущ. + родительный», иначе пустая строка.

    Условия — все читаются прямо в строке, ни одно не выводится:
      1. слов не меньше трёх, знаков конца предложения нет;
      2. первое слово не артикль (артикль уже стоит — чинить нечего);
      3. ВТОРОЕ слово — родительный артикль `des/der/eines/einer`, напечатанный буквами;
      4. первое слово написано с заглавной, буквами, и не входит в закрытый класс
         служебных слов (см. `_NEVER_A_HEAD_NOUN` — там же, почему одной заглавной мало);
      5. ПОСЛЕДНЕЕ слово написано с заглавной. Это отрезает обороты, которые кончаются
         глаголом: «Aus der Liste streichen», «Mitglied der Gewerkschaft werden»,
         «Von einer Einnahmequelle abhängig sein». Приписать им артикль значило бы
         сказать «der Mitglied der Gewerkschaft werden» — бессмыслица. Заглавная у
         ПОСЛЕДНЕГО слова не путается с началом предложения, в отличие от первого;
      6. ТРЕТЬЕ слово — не артикль. Сухой прогон 16.09.2026 выдал «der Streich eines der
         beiden Wörter»: здесь «Streich» — повелительное от streichen («вычеркни одно из
         двух слов»), а справочник знает «der Streich» (проделка). Разобрать это по слову
         нельзя, зато видно по СТРОЕНИЮ: «eines der …» — разделительный оборот, у него
         третьим словом стоит ещё один артикль, чего в обычном родительном определении
         («eines ausländischen Staates», «des Schutzes») не бывает. Строение читается в
         напечатанном тексте, а не выводится.

    Само существительное здесь НЕ подтверждается — это делает справочник склонений
    в `article_for_genitive_phrase`. Эта функция только отбирает форму строки.
    """
    compact = re.sub(r"\s+", " ", str(text or "").strip())
    if not compact or _SENTENCE_END_RE.search(compact):
        return ""
    tokens = compact.split(" ")
    if len(tokens) < 3:
        return ""
    head = tokens[0]
    if head.casefold() in _LEADING_ARTICLES:
        return ""
    if tokens[1].casefold() not in _GENITIVE_MARKERS:
        return ""
    if not _WORD_RE.match(head) or not head[:1].isupper():
        return ""
    if head.casefold() in _NEVER_A_HEAD_NOUN:
        return ""
    if tokens[2].casefold() in _LEADING_ARTICLES:
        return ""   # разделительное «eines der beiden Wörter», а не родительное определение
    last = tokens[-1]
    if not last[:1].isupper():
        return ""
    return head


def article_for_genitive_phrase(text: str | None, *, tables: dict | None = None) -> tuple:
    """(артикль, источник) либо ("", причина). Артикль — только из справочника склонений.

    `tables` позволяет отдать УЖЕ прочитанные таблицы (ночной проход читает их пачкой,
    один запрос на сотню строк вместо сотни запросов) и проверить правило тестом,
    не поднимая базу.
    """
    head = head_noun_of_genitive_phrase(text)
    if not head:
        return ("", NOT_THIS_SHAPE)
    from backend.noun_declension_reference import (
        article_from_declension_tables,
        article_from_declension_reference,
    )
    if tables is not None:
        article, source = article_from_declension_tables(head, tables)
    else:
        article, source = article_from_declension_reference(head)
    if not article:
        # Отказ справочника — это «не знаем», и он возвращается вслух, чтобы его посчитали.
        return ("", f"{head}: {source}")
    return (article, source)


def headword_with_genitive_article(text: str | None, *, tables: dict | None = None) -> tuple:
    """(готовый заголовок, источник) либо (исходный текст, причина отказа).

    Идемпотентна: у строки, которая уже начинается с артикля, форма не подходит под
    условие 2, и она возвращается как есть. Поэтому ночной проход можно гонять каждую
    ночь, а дверь — на каждом сохранении, не считая, сколько раз строка через них прошла.
    """
    compact = re.sub(r"\s+", " ", str(text or "").strip())
    article, source = article_for_genitive_phrase(compact, tables=tables)
    if not article:
        return (compact, source)
    return (f"{article} {compact}", source)


# ── НОЧНОЙ ПРОХОД ────────────────────────────────────────────────────────────────────
#
# Половин обязательно две: дверь выше не пускает новый разнобой, а этот проход разбирает
# накопленный. Одна без другой не работает — дверь не видит того, что уже лежит, а разовая
# чистка не мешает разнобою натечь заново.
#
# Проход идёт КАЖДУЮ ночь, а не один раз, по двум причинам. Прямой путь бота
# (`bot_3.py` → save_webapp_dictionary_query_returning_id_with_inserted) зовёт слой базы
# мимо `_apply_german_headword_normalization`, то есть мимо двери. И справочник склонений
# пополняется: строка, про которую сегодня «не знаем», завтра может получить ответ.
# Правило идемпотентно, поэтому лишний проход ничего не стоит и ничего не портит.

# Грубый отбор в SQL: заголовок с заглавной, вторым словом родительный артикль. Точное
# решение принимает head_noun_of_genitive_phrase — здесь только сужаем выборку, чтобы не
# тащить в память все 15 847 многословных заголовков.
_SQL_SHAPE = r"^[A-ZÄÖÜ][^[:space:]]+[[:space:]]+(des|der|eines|einer)[[:space:]]"

JOB_KEY = "genitive_phrase_article_sweep"


def _collect_candidates(cursor) -> dict:
    """{заголовок: {"units": [id…], "cards": [id…], "pool": [id…]}} — где он лежит."""
    места: dict[str, dict] = {}

    def добавить(текст, хранилище, row_id):
        ключ = re.sub(r"\s+", " ", str(текст or "").strip())
        if not ключ or not head_noun_of_genitive_phrase(ключ):
            return
        места.setdefault(ключ, {"units": [], "cards": [], "pool": []})[хранилище].append(int(row_id))

    cursor.execute(
        "SELECT id, display FROM bt_3_lex_units WHERE lang = 'de' AND display ~ %s;",
        (_SQL_SHAPE,))
    for row_id, текст in cursor.fetchall() or []:
        добавить(текст, "units", row_id)

    cursor.execute(
        "SELECT id, word_de FROM bt_3_webapp_dictionary_queries WHERE word_de ~ %s;",
        (_SQL_SHAPE,))
    for row_id, текст in cursor.fetchall() or []:
        добавить(текст, "cards", row_id)

    # Немецкая сторона пула берётся ПО СОДЕРЖИМОМУ, а не по настройке языковой пары: у
    # пары «русский → немецкий» target_lang='de', и в source_text лежит РУССКИЙ текст.
    # Ровно на этом месте 27.08.2026 сломалось соседнее правило — вердикт записан в
    # backend_server.py у _apply_german_headword_normalization.
    cursor.execute(
        "SELECT id, source_text, target_text FROM bt_3_dictionary_entries "
        "WHERE (source_lang = 'de' AND source_text ~ %s) "
        "   OR (target_lang = 'de' AND target_text ~ %s);",
        (_SQL_SHAPE, _SQL_SHAPE))
    for row_id, источник, цель in cursor.fetchall() or []:
        for текст in (источник, цель):
            добавить(текст, "pool", row_id)

    return места


def _rewrite_pool_row(cursor, row_id: int, old_text: str, new_text: str) -> int:
    """Переписать немецкую сторону записи пула. Поле выбирается по СОДЕРЖИМОМУ."""
    cursor.execute(
        "SELECT source_text, target_text, word_de FROM bt_3_dictionary_entries WHERE id = %s;",
        (row_id,))
    строка = cursor.fetchone()
    if not строка:
        return 0
    правки, значения = [], []
    for поле, текущее in zip(("source_text", "target_text", "word_de"), строка):
        if re.sub(r"\s+", " ", str(текущее or "").strip()) == old_text:
            правки.append(f"{поле} = %s")
            значения.append(new_text)
    if not правки:
        return 0
    cursor.execute(
        f"UPDATE bt_3_dictionary_entries SET {', '.join(правки)}, updated_at = NOW() "
        f"WHERE id = %s;", (*значения, row_id))
    return 1


def sweep_missing_genitive_articles(*, dry_run: bool = False) -> dict:
    """Дописать артикль всем группам «сущ. + родительный», у которых его нет.

    В отчёте ТРИ разных числа, и путать их нельзя:
      fixed   — заголовков починено;
      unknown — форма подходит, а справочник склонений ответа не дал. Это НЕ «починено» и
                НЕ «нечего чинить»: пустая ячейка — такая же незакрытая задача, как
                выдумка, просто дешевле. Уходит владельцу вслух поимённым списком;
      places  — сколько строк в трёх хранилищах затронуто (заголовок живёт не в одном).
    """
    from backend.database import get_db_connection_context, spread_correction_everywhere
    from backend.noun_declension_reference import articles_from_declension_reference

    отчёт = {"fixed": 0, "unknown": 0, "cards": 0, "pool": 0, "units": 0,
             "unknown_words": [], "examples": []}
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            места = _collect_candidates(cursor)
        if not места:
            return отчёт
        головы = {текст: head_noun_of_genitive_phrase(текст) for текст in места}
        ответы = articles_from_declension_reference(sorted(set(головы.values())))
        for текст in sorted(места):
            артикль, причина = ответы.get(
                головы[текст], (None, "справочник склонений не знает слова"))
            if not артикль:
                отчёт["unknown"] += 1
                отчёт["unknown_words"].append(f"{текст} — {причина}")
                continue
            новый = f"{артикль} {текст}"
            отчёт["fixed"] += 1
            if len(отчёт["examples"]) < 5:
                отчёт["examples"].append(f"{текст} → {новый}")
            if dry_run:
                continue
            with conn.cursor() as cursor:
                # Слово в справочнике есть: развоз чинит разбор, карточки людей и их
                # записи пула одним движением. Без него правка оседает в среднем в
                # четырёх местах из пяти (замер 16.08.2026, database.py).
                for unit_id in места[текст]["units"]:
                    spread_correction_everywhere(cursor, unit_id=unit_id,
                                                 old_text=текст, new_text=новый)
                    cursor.execute(
                        "UPDATE bt_3_lex_units SET display = %s, updated_at = NOW() "
                        "WHERE id = %s;", (новый, unit_id))
                    отчёт["units"] += 1
                # Карточки, не привязанные к слову справочника, развоз не видит — их
                # правим прямо. Условие `word_de = текст` держит правку идемпотентной.
                for card_id in места[текст]["cards"]:
                    cursor.execute(
                        "UPDATE bt_3_webapp_dictionary_queries SET word_de = %s, "
                        "updated_at = NOW() WHERE id = %s AND word_de = %s;",
                        (новый, card_id, текст))
                    отчёт["cards"] += cursor.rowcount or 0
                for pool_id in места[текст]["pool"]:
                    отчёт["pool"] += _rewrite_pool_row(cursor, pool_id, текст, новый)
            conn.commit()
    return отчёт


def count_missing_genitive_articles() -> int:
    """Сколько групп «сущ. + родительный» СЕЙЧАС стоят без артикля, хотя справочник его
    знает. Обещание держится на нуле — это же число меряет backend/fix_promises.py."""
    отчёт = sweep_missing_genitive_articles(dry_run=True)
    return int(отчёт["fixed"])

