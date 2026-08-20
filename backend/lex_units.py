# -*- coding: utf-8 -*-
"""Поиск по слою ЕДИНИЦ словаря.

Старый путь ищет СТРОКУ ТЕКСТА в общем банке: пара «что спросили → что ответили» плюс
направление. Из-за этого одно слово живёт в нескольких строках, обратное направление
(«враг» → der Feind) не находится без костыля, а разбор одного слова может прилипнуть к
заголовку другого — так карточка «der Flegel» получила формы «der Rüpel».

Здесь ищется СЛОВО: написание → указатель → единица → связи-переводы. Разбор лежит на
самой единице, поэтому приклеить к слову чужие формы физически неоткуда.

Модуль ничего не пишет: только читает слой и собирает карточку в том же виде, какой
ждёт фронт (см. _build_item). Включается рубильником DICTIONARY_UNITS_LOOKUP_ENABLED —
пока он выключен, приложение работает ровно как раньше.
"""
from __future__ import annotations

import os

import json
import logging
import re
from typing import Any

from backend.database import (
    get_db_connection_context,
    _dictionary_pool_word_fully_rich_sql,
    CARD_CONTENT_KEYS,
    card_content_score,
)
from backend.dictionary_intake import clean_text
from backend.lex_senses import split_translation

_SPACE_RE = re.compile(r"\s+")
_ARTICLE_RE = re.compile(r"^(der|die|das)\s+", re.I)
# В тексте слово стоит с ПАДЕЖНЫМ артиклем: «den Rüpeln», «des Helden», «einem Kind».
# Для поиска снимаем любой из них; для определения рода годится только именительный
# (см. article_of): «den» бывает и мужским винительным, и множественным дательным.
_ANY_ARTICLE_RE = re.compile(
    r"^(?:der|die|das|den|dem|des|ein|eine|einen|einem|einer|eines)\s+", re.I)

# Сколько переводов показывать: первый — главный, остальные как «ещё говорят».
#
# Замер 11.08.2026 (`python3 scripts/dict_defect_audit.py`, пункт 4): у 338 слов эти
# шесть мест ЗАБИТЫ пересказами одного и того же, и настоящие другие значения человек
# не видит. Поднимать лимит бессмысленно — лечится отсевом дублей, см. _build_item.
_MAX_LINKS = 6

# Ранг, в который отправлены «свалки» — старые переводы вида «1прикладывать; накладывать
# 2 надевать 3 строить». Они разрезаны на отдельные значения и в базе остались, но
# показывать их нельзя: человек должен видеть значения, а не строку из словаря.
#
# ПРОВЕРЕНО 11.08.2026 — этот приём РАБОТАЕТ, к нему возвращаться не нужно:
# 450 понижённых связей у 426 немецких слов, и ни одна не доходит до экрана (ermutigen:
# в базе лежит «1 ободрять 2 вдохновлять» с рангом 900, на карточке — ободрять ·
# поощрять · вдохновлять). Из этих 450 строк 302 полностью восстановимы из соседних
# переводов, а 148 — нет: в них единственный носитель смысла («aufschlagen → разбивать
# (яйцо)»). Поэтому «удалить всё понижённое» — нельзя, а «удалить лишние 302» — можно,
# но не нужно: они не видны и ничего не занимают.
#
# ЧЕГО ЭТОТ ПРИЁМ НЕ ЛЕЧИТ, и это отдельные открытые дефекты (пункты 1B и 4 отчёта):
#   • 380 свалок заведены в базу как РУССКИЕ ЕДИНИЦЫ со своими написаниями для поиска.
#     Понижение ранга прячет их из карточки, но из словаря не убирает;
#   • дубли через запятую («изменение» и «Перемена, изменение») понижены НЕ БЫЛИ —
#     разрезание трогало только свалки С НОМЕРАМИ. У 1147 слов из 1358 понижать нечего.
_DEMOTED_RANK = 900

# Служебные пометки, осевшие в банке под видом переводов: «приюта; хостела
# (Genitiv/Dativ)». Человеку это не перевод, а мусор — в списке значений не показываем.
# Из базы ничего не удаляем: фильтр только на выдаче.
# Заготовка упражнения «Er ___ heute früh mit dem Projekt» — не перевод, а задание
# тренажёра, осевшее в банке отдельной записью. В списке значений ему не место.
_EXERCISE_BLANK = "___"

_GRAMMAR_NOTE_RE = re.compile(
    r"\((?:[^)]*\b(?:genitiv|dativ|akkusativ|nominativ|plural|singular|мн\.?\s*ч|ед\.?\s*ч)\b[^)]*)\)",
    re.I,
)


_CYRILLIC_CAPITAL_FIRST = re.compile(r"^[А-ЯЁ]")
_CYRILLIC_ANY_RE = re.compile(r"[А-Яа-яЁё]")


def looks_like_example_not_translation(text: str) -> bool:
    """Значение из разбора — это перевод слова или пример его употребления?

    Пример распознаётся по знаку конца или по заглавной букве при трёх и более словах:
    «Ваша подписка успешно отменена», «Йо, ты можешь кататься?». Толкование, даже
    длинное, начинается со строчной и точкой не заканчивается: «ясли, учреждение по
    уходу за детьми» — это перевод, и трогать его нельзя.

    Порог в три слова стоит намеренно: «Точка зрения» — обычный перевод, просто с
    заглавной. Цена порога — пропустим короткий пример «Колокол прозвонил»; пропустить
    дешевле, чем спрятать хороший перевод.

    ПРОВЕРЕНО 11.08.2026 — ПРАВИЛО РАБОТАЕТ, переписывать его не надо.
    Разметку взяли не на глаз: в разборах перевод лежит в `value`, а пример — в
    `example_target`, это и есть правильные ответы. На 10 889 настоящих примерах и
    1 177 настоящих переводах правило ловит 10 874 примера (прошляпило 15) и задевает
    9 переводов — да и те не переводы, а толкования, положенные в поле перевода
    («Часть автомобиля, через которую выходят отработанные газы двигателя»).
    Проверенные альтернативы хуже: «только знак конца . ! ?» пропускает 36 примеров,
    «начинается с местоимения» — 7 874, комбинации выигрыша не дают.

    Отдельно замерена БОЕВАЯ цена заслона — не гипотетическая, а по единственному
    рабочему вызову (sync_unit_links_from_card ниже, там `continue`): из значений,
    реально прошедших перенос, он отбросил 8 штук на всю немецкую базу, и жаль из них
    трёх («Jammerlappen → Трус, слабый человек», «Heute → В наши дни»).

    Если захочется улучшать — улучшать надо НЕ угадайку. Пример и перевод приезжают в
    РАЗНЫХ полях, и гадать приходится лишь там, где выше по течению пример положили в
    поле перевода: таких случаев 15 на 10 889. Чинить эти 15 мест на входе — и правило
    станет не нужно вовсе.

    ⚠ Ранее в этой докстроке стояло «правило метит 717 строк, из них 684 — хорошие
    переводы, каждый прогон обогащения теряет данные». Это НЕВЕРНО и снято: то число
    считалось по другой популяции — по строкам, уже лежащим в банке и показанным на
    экране, а не по тем, что проходят через заслон. Гипотетическая цена, а не потеря.
    """
    body = str(text or "").strip()
    if not body:
        return False
    if body.endswith((".", "!", "?")):
        return True
    return bool(_CYRILLIC_CAPITAL_FIRST.match(body)) and len(body.split()) >= 3


_LATIN_ANY_RE = re.compile(r"[A-Za-zÄÖÜäöüß]")


def text_matches_language(text: str | None, lang: str | None) -> bool:
    """Написано ли слово своим алфавитом.

    Русский текст в немецкой единице — это перепутанные стороны, а не опечатка: карточка
    после такого указывает на чужое слово, разбор к ней не приезжает, поиск её не находит.
    Замер 05.08.2026: 124 немецких единицы с русским текстом, 350 русских с немецким —
    и почти все от кода, который писал в обход проверок.

    Тот же запрет стоит в самой базе (`chk_lex_units_script_matches_lang`). Здесь он
    нужен, чтобы отказать понятно и заранее, а не ловить ошибку записи."""
    body = str(text or "").strip()
    if not body:
        return False
    code = str(lang or "").strip().lower()
    if code == "de":
        return bool(_LATIN_ANY_RE.search(body))
    if code == "ru":
        return bool(_CYRILLIC_ANY_RE.search(body))
    return True


def normalize_query(text: str) -> str:
    """Ключ поиска: без лишних пробелов, без артикля, в нижнем регистре.

    Артикль снимаем именно здесь, а не в опознании единицы: «der Kiefer» и «Kiefer» —
    одно написание, а вот РАЗНЫЕ единицы за ним стоят разные, и выбирает их вызывающая
    сторона по артиклю запроса."""
    compact = _SPACE_RE.sub(" ", str(text or "").strip())
    return _ANY_ARTICLE_RE.sub("", compact).strip().casefold()


def article_of(text: str) -> str:
    m = _ARTICLE_RE.match(str(text or "").strip())
    return m.group(1).lower() if m else ""


def _fetch_units(cur, *, lang: str, surface_key: str) -> list[dict]:
    cur.execute(
        """
        SELECT u.id, u.lang, u.kind, u.lemma, u.lemma_key, u.pos, u.gender, u.display, u.card,
               s.match_kind
        FROM bt_3_lex_surfaces s
        JOIN bt_3_lex_units u ON u.id = s.unit_id
        WHERE s.lang = %s AND s.surface_key = %s;
        """,
        (lang, surface_key),
    )
    return [
        {"id": r[0], "lang": r[1], "kind": r[2], "lemma": r[3], "lemma_key": r[4],
         "pos": r[5], "gender": r[6], "display": r[7], "card": r[8] if isinstance(r[8], dict) else None,
         "match_kind": str(r[9] or "")}
        for r in cur.fetchall()
    ]


def _fetch_links(cur, unit_id: int, *, want_lang: str) -> list[dict]:
    cur.execute(
        """
        SELECT u.id, u.lang, u.kind, u.display, u.lemma, u.pos, u.gender, l.rank, u.card
        FROM bt_3_lex_links l
        JOIN bt_3_lex_units u ON u.id = l.to_unit
        -- Заготовки упражнений отсекаем В ЗАПРОСЕ, а не после: у «anlegen» их 33 штуки,
        -- и при отборе «первых шести» они съедали выдачу целиком — слово оставалось
        -- вообще без перевода. Связи с разобранным значением идут первыми.
        WHERE l.from_unit = %s AND u.lang = %s AND l.rank < %s
          AND position('___' in u.display) = 0
        ORDER BY (l.sense_id IS NULL), l.rank, u.id
        LIMIT %s;
        """,
        # Берём с запасом. Показать надо _MAX_LINKS штук, но часть из них — пересказы
        # друг друга («скобка» и «Скобка, скрепка»), и отсеиваются они уже здесь, в
        # коде. Если брать ровно шесть, дубли занимают места ДО отсева, и настоящие
        # значения человек не увидит: замер 11.08.2026 — 338 слов показывали ровно
        # шесть строк, где половина одно и то же. Отбор в _build_item.
        (unit_id, want_lang, _DEMOTED_RANK, _MAX_LINKS * 4),
    )
    return [
        {"id": r[0], "lang": r[1], "kind": r[2], "display": r[3], "lemma": r[4],
         "pos": r[5], "gender": r[6], "rank": r[7], "card": r[8] if isinstance(r[8], dict) else None}
        for r in cur.fetchall()
    ]


def _pick_unit(units: list[dict], *, requested_article: str) -> dict | None:
    """Из нескольких единиц одного написания выбираем нужную.

    Омографы («der Kiefer» челюсть / «die Kiefer» сосна) различаются только артиклем:
    если он в запросе есть — берём совпадающий, если нет — берём слово (не словоформу)
    с самым полным разбором, а при равенстве не гадаем и отдаём первое по алфавиту рода,
    чтобы ответ был устойчивым от запроса к запросу."""
    if not units:
        return None
    if len(units) == 1:
        return units[0]
    if requested_article:
        same = [u for u in units if (u.get("gender") or "") == requested_article]
        if len(same) == 1:
            return same[0]
        if same:
            units = same
    with_card = [u for u in units if u.get("card")]
    pool = with_card or units
    return sorted(pool, key=lambda u: ((u.get("gender") or "я"), u["id"]))[0]


def _collect_homographs(cur, units: list[dict], chosen: dict, *, want_lang: str) -> list[dict]:
    """Другие слова с тем же написанием: «der Kiefer» (челюсть) и «die Kiefer» (сосна).

    Запрос без артикля угадать нечем, поэтому одно слово мы показываем, а про остальные
    честно говорим «ещё есть» — иначе человек уверен, что у слова один смысл, и второй
    он никогда не увидит."""
    others = [u for u in units if u["id"] != chosen["id"]]
    out: list[dict] = []
    for unit in others:
        links = _fetch_links(cur, unit["id"], want_lang=want_lang)
        translation = ""
        for link in links:
            value = link["display"]
            if _EXERCISE_BLANK in value or _GRAMMAR_NOTE_RE.search(value):
                continue
            translation = value
            break
        out.append({
            "display": unit["display"],
            "gender": unit.get("gender") or "",
            "part_of_speech": unit.get("pos") or "",
            "translation": translation,
            "unit_id": unit["id"],
        })
    return out


_BRACKET_NOTE_RE = re.compile(r"^(.*?)\s*\([^)]*\)\s*$")

# Номер значения в начале строки: «1 колоть», «2. разгадать», «1.приём» (без пробела).
_SENSE_LEAD_RE = re.compile(r"^\s*([1-9])\s*(?:[).]\s*|\s+)(?=[А-Яа-яЁёA-Za-z])")
# Номер значения в середине: «…отбивать 2 предотвращать», «…схватить 2. поймать».
_SENSE_MID_RE = re.compile(r"(?<=[\w,;.])\s+([1-9])\s*[).]?\s+(?=[А-Яа-яЁёA-Za-z])")
# Слова, после которых цифра — ЧАСТЬ СМЫСЛА, а не номер значения. Без этого списка
# «Меню из 5 блюд» превратилось бы в «Меню из» и «блюд», а «до 16 часов» — в мусор.
_DIGIT_IS_MEANING_AFTER = {
    "из", "до", "на", "за", "в", "во", "с", "со", "к", "ко", "от", "о", "об", "по",
    "при", "для", "через", "около", "более", "менее", "свыше", "почти", "ещё", "еще",
    "уже", "лет", "года", "год", "часов", "минут", "раз",
}
# Слова ПОСЛЕ цифры, которые выдают счёт, а не номер значения: «2 недели», «5 блюд».
# Проверяются, когда номер стоит в начале строки без точки и скобки.
_COUNTED_NOUNS = {
    "лет", "год", "года", "годов", "месяц", "месяца", "месяцев", "неделя", "недели",
    "недель", "день", "дня", "дней", "час", "часа", "часов", "минута", "минуты",
    "минут", "секунда", "секунды", "секунд", "раз", "раза", "штук", "штуки", "блюд",
    "блюда", "человек", "человека", "тысяч", "тысячи", "миллиона", "миллионов",
    "процент", "процента", "процентов", "евро", "рубля", "рублей", "километр",
    "километра", "километров", "метр", "метра", "метров",
}


def split_numbered_senses(value: str) -> list[str]:
    """Свалку с номерами значений разложить на отдельные значения.

    ЗАЧЕМ. В банке осели строки вида «1.приём на работу 2. место, должность» и обрубки
    вида «1 колоть». Человеку это не перевод, а кусок словарной статьи. Замер
    11.08.2026 (scripts/dict_defect_audit.py, п.2): 15 таких строк доходят до карточек
    одиночных слов, из них 12 настоящих свалок.

    ЧЕГО ЭТО ПРАВИЛО НЕ ДЕЛАЕТ — и это главное. Цифра в переводе далеко не всегда
    номер значения: «Меню из 5 блюд», «детская группа для малышей до 3 лет», «до 16
    часов». Поэтому режем ТОЛЬКО там, где цифра ведёт себя как нумерация:
      • число от 1 до 9 (номеров значений больше девяти не бывает, а «16 часов» бывает);
      • перед ним начало строки либо конец предыдущего значения;
      • перед ним НЕ предлог и не счётное слово (список выше);
      • после него — буква.
    Если хоть одно условие не выполнено, строка остаётся как есть. Лучше показать
    строку с цифрой, чем разрезать живой перевод пополам.

    Из базы ничего не удаляется: это правило показа.
    """
    text = _SPACE_RE.sub(" ", str(value or "").strip())
    if not text:
        return []
    lead = _SENSE_LEAD_RE.match(text)
    if lead:
        rest = text[lead.end():].strip()
        first_word = re.split(r"[\s,;]+", rest)[0].strip(".,;").casefold() if rest else ""
        # «2 недели» — это счёт, а не второе значение. Точка или скобка после номера
        # («2. поймать») снимают сомнение: так пишут только нумерацию.
        numbered = bool(re.match(r"^\s*[1-9]\s*[).]", text))
        if numbered or first_word not in _COUNTED_NOUNS:
            text = rest
    parts, last = [], 0
    for match in _SENSE_MID_RE.finditer(text):
        before = text[last:match.start()].strip()
        prev_word = re.split(r"[\s,;]+", before)[-1].strip(".,;").casefold() if before else ""
        if prev_word in _DIGIT_IS_MEANING_AFTER:
            continue
        # И то же самое справа: «У нас осталось 3 штуки» — счёт, а не второе значение.
        # Слева тут стоит «осталось», предлога нет, и без этой проверки предложение
        # разрезалось бы пополам.
        after_word = re.split(r"[\s,;]+", text[match.end():].strip())[0].strip(".,;!?").casefold()
        if after_word in _COUNTED_NOUNS:
            continue
        if before:
            parts.append(before)
        last = match.end()
    tail = text[last:].strip()
    if tail:
        parts.append(tail)
    cleaned = [p.strip(" ,;.") for p in parts if p.strip(" ,;.")]
    if not cleaned:
        return [text] if text else []
    # Ничего не разрезали — отдаём строку КАК ЕСТЬ. Иначе у предложения срезается точка,
    # оно перестаёт выглядеть предложением, и правило регистра опускает ему первую букву:
    # «Прогноз оправдался.» → «прогноз оправдался». Поймано тестом 16.08.2026.
    if len(cleaned) == 1 and cleaned[0] == text.strip(" ,;."):
        return [text]
    return cleaned


def _translation_key(value: str) -> str:
    """Ключ сравнения переводов: пробелы, регистр и точка в конце значения не меняют."""
    return _SPACE_RE.sub(" ", str(value or "").strip()).casefold().rstrip(".")


def drop_nested_translations(values: list[str]) -> list[str]:
    """Убрать переводы, целиком сидящие внутри соседних.

    ЗАЧЕМ. У слова показывают и «скобка», и «Скобка, скрепка» — это не два значения, а
    одно плюс пересказ. Замер 11.08.2026 (scripts/dict_defect_audit.py, п.4): такое у
    1358 немецких слов из 4627, то есть у каждого третьего. Хуже того, у 338 слов
    пересказы занимают все шесть мест, и настоящие другие значения не влезают.

    ПРАВИЛО. Остаётся КОРОТКИЙ: он и есть перевод, длинный обычно тащит приклеенное
    пояснение («направление» против «направление, к которому движутся»). Исключение
    одно — когда длинный отличается только пометой в скобках («деньги» против «деньги
    (разг.)»): помета короткая, полезная и человеку нужна, поэтому остаётся она.

    ЧЕГО ПРАВИЛО НЕ ДЕЛАЕТ. Не режет перечисления на части. «зажим, клипса» так и
    останется одной строкой, если внутри нет отдельно лежащего перевода: резать по
    запятой вслепую опасно — «направление, к которому движутся» дало бы обрывок
    «к которому движутся», который переводом не является.

    Из базы НИЧЕГО не удаляется: это правило показа. Понижением ранга оно и не
    лечится — у 1147 слов из 1358 понижать нечего, июльское разрезание трогало
    только свалки с номерами.
    """
    keys = [_translation_key(v) for v in values]
    drop: set[int] = set()
    for i, short in enumerate(keys):
        for j, long_ in enumerate(keys):
            if i == j or i in drop or j in drop:
                continue
            if short == long_:
                # «венчик» и «венчик.» — одно значение: отличается только знак в конце
                # (12 слов в замере). Полное сравнение строк в _build_item такое не
                # ловит, оно точку считает частью перевода.
                #
                # Из двух одинаковых оставляем ту, что со СТРОЧНОЙ: её наличие рядом и
                # доказывает, что слово обычное, а не имя собственное. Само по себе
                # одиночное слово правило регистра не трогает — под ним прячутся
                # «Афины» и «Марокко».
                a, b = values[i], values[j]
                keep = i if (str(a)[:1].islower() or not str(b)[:1].islower()) else j
                drop.add(j if keep == i else i)
                continue
            inside = (
                short in [part.strip() for part in long_.split(",")]
                or long_.startswith(short + " ")
                or long_.startswith(short + ",")
            )
            if not inside:
                continue
            note = _BRACKET_NOTE_RE.match(long_)
            # «деньги» ⊂ «деньги (разг.)» — оставляем помеченный, он информативнее.
            drop.add(i if (note and _translation_key(note.group(1)) == short) else j)
    return [value for index, value in enumerate(values) if index not in drop]


# Части речи, у которых перевод точно не может быть именем собственным.
_NOT_A_NOUN_POS = {"verb", "adjective", "adverb", "adj", "adv", "preposition",
                   "conjunction", "particle", "pronoun", "numeral", "interjection"}
_RU_CAPITAL_START_RE = re.compile(r"^[А-ЯЁ][а-яё]")
_RU_CAPITAL_INSIDE_RE = re.compile(r".[А-ЯЁ]")
_SENTENCE_END_RE = re.compile(r"[.!?]$")
_ONE_TOKEN_RE = re.compile(r"^[^\s,;]+$")


def normalize_translation_case(value: str, *, german_pos: str = "") -> str:
    """Перевод в словаре пишется со строчной. Но не всякую заглавную можно опустить.

    ЗАЧЕМ. Замер 15.08.2026: на карточках одиночных слов 1418 переводов начинаются с
    заглавной — «Аккуратный, опрятный», «Тормозить, сдерживать». Это словарные статьи,
    а не предложения, и заглавная в них лишняя.

    ЧЕГО НЕ ТРОГАЕМ, и почему именно так:
      • предложение (кончается на . ! ?) — «Прогноз оправдался.» пишется с заглавной;
      • строка с заглавной ВНУТРИ («Северный Ледовитый океан») — там имя собственное;
      • ОДНО слово, если немецкое слово не названо ЯВНО глаголом, прилагательным или
        наречием. Здесь прячутся имена собственные: «Athen → Афины», «Marokko →
        Марокко», «der Anhalt → Анхальт». Часть речи у них в базе пустая, поэтому
        проверка «это существительное» их НЕ ловит — проверять надо наоборот, и
        первая версия правила из-за этого написала «афины». Разбирать такие случаи
        надо глазами, а не правилом; их 39 из 1418.

    Охват: 1365 из 1418 (96%) опускаются без единого риска, 53 остаются как есть.
    """
    text = str(value or "")
    if not _RU_CAPITAL_START_RE.match(text):
        return text
    if _SENTENCE_END_RE.search(text.strip()):
        return text
    if _RU_CAPITAL_INSIDE_RE.search(text):
        return text
    if _ONE_TOKEN_RE.match(text.strip()):
        # Одиночное слово опускаем, только если немецкое слово ЯВНО не существительное.
        # Пустая часть речи — не разрешение: у «Athen» и «Marokko» она пустая.
        if str(german_pos or "").strip().lower() not in _NOT_A_NOUN_POS:
            return text
    return text[:1].lower() + text[1:]


def _build_item(unit: dict, links: list[dict], *, source_lang: str, target_lang: str) -> dict:
    """Карточка в том виде, какой ждёт фронт.

    За основу берётся разбор, лежащий НА единице (он про неё и ни про кого больше), а
    заголовок, артикль и переводы ставятся из самой единицы и её связей — чтобы данные
    на экране всегда были про одно и то же слово.

    Спросили по-русски — разбор берём с НЕМЕЦКОЙ стороны связи. Разбор описывает
    немецкое слово: формы, род, управление, примеры. На русской единице его нет и быть
    не должно (21 534 русских единицы, разбор лежит у 158). Без этого запрос «чёткое
    направление» возвращал карточку без разбора, словарь считал слово незнакомым и шёл
    к модели — за тем, что у нас уже разобрано и оплачено."""
    de_side = unit if unit["lang"] == "de" else (links[0] if links else None)
    ru_side = links[0] if unit["lang"] == "de" else unit
    card = dict(unit.get("card") or (de_side or {}).get("card") or {})

    # Регистр заголовка: существительное с заглавной, остальное со строчной.
    # Общее правило с карточкой и таблицами — german_grammar_tables.
    from backend.german_grammar_tables import german_headword_case
    german_display = german_headword_case((de_side or {}).get("display") or "",
                                          (de_side or {}).get("pos"))
    # АРТИКЛЬ К СУЩЕСТВИТЕЛЬНОМУ. Род у слова есть, а в написании его может не быть:
    # часть единиц заведена как «das Haus», часть как «Gericht». Владелец 16.08.2026:
    # «Gericht → суд · блюдо, а где артикль?» — род у него стоял, das, просто выдача
    # справочника его не приклеивала, в отличие от карточки (compose_german_headword).
    # Существительное без артикля учить нельзя: род — половина слова.
    _gender = str((de_side or {}).get("gender") or "").strip().lower()
    if (german_display and _gender in {"der", "die", "das"}
            and (de_side or {}).get("pos") == "noun"
            and not _ANY_ARTICLE_RE.match(german_display)):
        german_display = f"{_gender} {german_display}"
    native_display = (ru_side or {}).get("display") or ""

    item: dict[str, Any] = dict(card)
    item["source_text"] = unit["display"] if unit["lang"] == source_lang else native_display
    item["target_text"] = ""
    for candidate in (links[0]["display"] if links else "", native_display, german_display):
        if candidate and candidate != item["source_text"]:
            item["target_text"] = candidate
            break
    if source_lang == "de":
        item["source_text"] = german_display or item["source_text"]
        item["target_text"] = native_display or item["target_text"]
    elif target_lang == "de":
        item["source_text"] = native_display or item["source_text"]
        item["target_text"] = german_display or item["target_text"]

    if german_display:
        item["word_de"] = german_display
        item["translation_de"] = german_display
    if native_display:
        item["word_ru"] = native_display
        item["translation_ru"] = native_display
    if (de_side or {}).get("gender"):
        item["article"] = de_side["gender"]
    if (de_side or {}).get("pos"):
        item["part_of_speech"] = de_side["pos"]
    item["entry_kind"] = unit["kind"] if unit["kind"] != "collocation" else "phrase"
    item["language_pair"] = {
        "code": f"{source_lang}-{target_lang}",
        "source_lang": source_lang,
        "target_lang": target_lang,
    }
    # Все переводы, а не только главный: «грубиян» ведёт и к der Rüpel, и к der Flegel,
    # и человек должен видеть оба, а не гадать, почему показали одно.
    shown: list[dict] = []
    seen_values: set[str] = set()
    for link in links:
        value = link["display"]
        # Заготовку упражнения не показываем НИКОГДА, даже если других переводов нет:
        # «anfangen → Er ___ heute früh mit dem Projekt» — это задание тренажёра, а не
        # перевод. Лучше карточка без перевода (её доберёт обогащение), чем с бессмыслицей.
        if _EXERCISE_BLANK in value or _GRAMMAR_NOTE_RE.search(value):
            continue
        key = _SPACE_RE.sub(" ", value.strip()).casefold()
        if key in seen_values:
            continue  # «приют» из двух разных записей банка — один перевод, не два
        # ⚠ ЗАМЕРЕНО 11.08.2026 (`scripts/dict_defect_audit.py`, п.4): эта защита ловит
        # только ПОЛНОЕ совпадение строк, поэтому «изменение» и «Перемена, изменение»
        # проходят обе — у 1358 немецких слов из 4608 (29%) в списке лежит перевод,
        # целиком сидящий внутри соседнего. Разбивка: 1152 — элемент перечисления,
        # 169 — то же плюс продолжение, 71 — то же плюс скобка, 47 — начало элемента,
        # 12 — та же строка с другим знаком в конце.
        # Понижением ранга это НЕ лечится: у 1147 из 1358 слов понижённой склейки нет
        # вовсе (июльское разрезание трогало только свалки с номерами). Лечится ниже,
        # на выдаче — drop_nested_translations.
        seen_values.add(key)
        shown.append(link)

    # Дальше работаем со СТРОКАМИ, а не со связями: одна связь может дать несколько
    # значений («1 класть, положить 2 накладывать» — это два перевода, а не один).
    german_pos = str((de_side or {}).get("pos") or "")
    values: list[str] = []
    for link in shown:
        for piece in split_numbered_senses(link["display"]):
            # Регистр правим ДО отсева повторов: иначе «Перемена, изменение» и
            # «перемена, изменение» считаются разными строками и остаются обе.
            values.append(normalize_translation_case(piece, german_pos=german_pos))
    # Свалка могла распасться на куски, уже лежащие рядом отдельными связями.
    deduped: list[str] = []
    seen_pieces: set[str] = set()
    for value in values:
        key = _translation_key(value)
        if key in seen_pieces:
            continue
        seen_pieces.add(key)
        deduped.append(value)
    # Пересказы соседних переводов убираем ЗДЕСЬ, после разрезания и до обрезки до
    # _MAX_LINKS: иначе «Скобка, скрепка» занимает место, которое должно достаться
    # настоящему другому значению. Правило и числа — в drop_nested_translations.
    values = drop_nested_translations(deduped)[:_MAX_LINKS]
    if values:
        item["translations"] = [
            {"value": value, "context": "", "is_primary": index == 0}
            for index, value in enumerate(values)
        ]
        item["dictionary_senses"] = [
            {"rank": index + 1, "label": "main" if index == 0 else "secondary",
             "value": value, "context": "", "example_source": "", "example_target": ""}
            for index, value in enumerate(values)
        ]
    item["__lex_unit_id"] = unit["id"]
    # Отдельная пометка «у единицы есть НАСТОЯЩИЙ разбор». Без неё карточка со списком
    # переводов, но без форм и примеров, считалась бы полной (так устроена общая проверка
    # полноты) и уехала бы человеку голой, минуя дообогащение.
    item["__lex_has_card"] = bool(unit.get("card") or (de_side or {}).get("card"))
    return item


def _mark_asked_form(item: dict, *, asked: str, unit: dict, query_lang: str) -> None:
    """Человек набрал ФОРМУ слова, а показываем мы словарную форму — сказать ему об этом.

    Так делают Linguee, Reverso, dict.cc: заголовок всегда словарная форма, а набранное
    стоит строкой рядом («wuchsen — форма слова wachsen»). Иначе человек набирает одно,
    видит другое и не понимает, послушала ли его программа вообще.

    Подпись ставится ТОЛЬКО по указателю вида `inflected` — это настоящая словоформа из
    парадигмы. Написания вида `exact` тоже могут отличаться от заголовка, но это не формы,
    а опечаточные входы: «Bestürtz» ведёт на «bestürzt», потому что мы починили заголовок
    и оставили старое написание дверью для того, кто наберёт его снова. Назвать опечатку
    «формой слова» в языковом приложении нельзя — это прямая неправда."""
    if not isinstance(item, dict) or unit.get("lang") != query_lang:
        return
    if str(unit.get("match_kind") or "") != "inflected":
        return
    asked_clean = _SPACE_RE.sub(" ", str(asked or "").strip())
    if not asked_clean:
        return
    asked_key = normalize_query(asked_clean)
    lemma_display = str(unit.get("display") or "").strip()
    if not asked_key or not lemma_display or asked_key == str(unit.get("lemma_key") or ""):
        return
    # Написание совпало с заголовком с точностью до регистра и артикля — не форма.
    if asked_key == normalize_query(lemma_display):
        return
    item["asked_form"] = asked_clean
    item["asked_form_of"] = lemma_display


def units_needing_card(limit: int, *, lang: str = "de", native_lang: str = "ru") -> list[dict]:
    """Слова слоя, у которых ещё нет разбора, — сначала те, что скоро спросят.

    Ночной добор обязан смотреть СЮДА, а не в старый банк: после переключения поиск
    читает единицы, и добор в банк наполнял бы то, чего никто не открывает.

    Порядок решает, что человек увидит завтра. Сначала идут слова, стоящие у кого-то
    на повторение, — по ближайшему сроку: их покажут в ближайшие дни, и без разбора
    подсказка в тренировке будет пустой. Дальше — по востребованности: сколько людей
    сохранили слово себе, а при равенстве — из скольких записей банка оно собрано.

    Без срока повторения впереди оказывались слова с общим спросом, а те 140, что люди
    учат прямо сейчас, ждали своей очереди среди 2642 (замер 01.08.2026)."""
    if limit <= 0:
        return []
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.id, u.display, u.lemma, u.gender, u.pos,
                           COALESCE(p.saved, 0) AS saved,
                           COALESCE(s.sources, 0) AS sources,
                           d.due_at,
                           (SELECT u2.display FROM bt_3_lex_links l
                              JOIN bt_3_lex_units u2 ON u2.id = l.to_unit
                             WHERE l.from_unit = u.id AND u2.lang = %s
                               AND position('___' in u2.display) = 0
                             ORDER BY l.rank, u2.id LIMIT 1) AS translation
                    FROM bt_3_lex_units u
                    LEFT JOIN (
                        SELECT lex_unit_id, COUNT(*) AS saved
                        FROM bt_3_webapp_dictionary_queries
                        WHERE lex_unit_id IS NOT NULL GROUP BY lex_unit_id
                    ) p ON p.lex_unit_id = u.id
                    LEFT JOIN (
                        SELECT unit_id, COUNT(*) AS sources
                        FROM bt_3_lex_unit_sources GROUP BY unit_id
                    ) s ON s.unit_id = u.id
                    LEFT JOIN (
                        SELECT q.lex_unit_id, MIN(st.due_at) AS due_at
                        FROM bt_3_card_srs_state st
                        JOIN bt_3_webapp_dictionary_queries q
                          ON q.id = st.card_id AND q.user_id = st.user_id
                        WHERE st.status <> 'suspended' AND q.lex_unit_id IS NOT NULL
                        GROUP BY q.lex_unit_id
                    ) d ON d.lex_unit_id = u.id
                    WHERE u.lang = %s AND u.kind = 'word' AND u.card IS NULL
                    ORDER BY (d.due_at IS NULL), d.due_at, saved DESC, sources DESC, u.id
                    LIMIT %s;
                    """,
                    (native_lang, lang, int(limit)),
                )
                rows = cur.fetchall()
    except Exception as exc:
        logging.debug("units needing card failed: %s", exc)
        return []
    # Перевод здесь не обязателен: разбор строится ПО НЕМЕЦКОМУ СЛОВУ, а перевод нужен
    # лишь для строки отчёта. Требование перевода выкидывало из очереди как раз частые
    # глаголы (anfangen, aufstehen), у которых в банке связаны только заготовки
    # упражнений, — и они бы так и остались без разбора.
    return [
        {"id": r[0], "display": r[1], "lemma": r[2], "gender": r[3], "pos": r[4],
         "saved": r[5], "sources": r[6], "due_at": r[7], "translation": r[8] or ""}
        for r in rows
    ]


def _kind_for_text(text: str) -> str:
    body = _ANY_ARTICLE_RE.sub("", str(text or "").strip()).strip()
    if not body:
        return ""
    if " " not in body:
        return "word"
    return "sentence" if len(body.split()) > 4 or body.rstrip().endswith((".", "!", "?")) else "collocation"


def door_check(text: str, lang: str) -> tuple[str, str, str] | None:
    """Дверь единицы, механическая половина. Возвращает (текст, ключ поиска, вид) или
    None, если заводить нельзя.

    Отдельной функцией она стала 20.08.2026. Причина: `sync_unit_links_from_card`
    заводила единицы ПРЯМЫМ запросом в базу, минуя `ensure_unit`, — то есть без чистки,
    без проверки языка и без запрета на свалку значений. Зовут её ночные работы и восемь
    скриптов, так что мимо двери шёл не единичный случай, а целый поток. Тест «дверь на
    каждом писателе» об этой функции не знал: она четвёртый заводчик единиц.

    Дорогая половина двери (справочник, модель, существует ли слово вообще) осталась в
    `ensure_unit` — она ходит в сеть, и внутри чужой транзакции ей не место.
    """
    text = clean_text(text)
    key = normalize_query(text)
    kind = _kind_for_text(text)
    if not key or not kind or not lang:
        return None
    if not text_matches_language(text, lang):
        # Слово чужого алфавита — это перепутанные стороны. Молча завести такую единицу
        # значит спрятать ошибку в базе: карточка будет указывать на чужое слово.
        logging.warning("единица не заведена: %r не похоже на язык %r", str(text)[:60], lang)
        return None
    # СВАЛКА НЕ СТАНОВИТСЯ СЛОВОМ СЛОВАРЯ. «1 густой, частый 2 плотный, непроницаемый» —
    # это кусок словарной статьи, а не слово. Замер 15.08.2026: 195 таких строк уже
    # заведены русскими единицами со своими написаниями для поиска — попали при
    # массовом переезде банка 27.07. Показывать их мы перестали (правило разрезания),
    # но заводить новые нельзя вовсе.
    if len(split_numbered_senses(text)) > 1:
        logging.warning("единица не заведена: %r — свалка значений, её надо разрезать",
                        str(text)[:60])
        return None
    return text, key, kind


def ensure_unit(text: str, lang: str) -> int | None:
    """Найти единицу по написанию, а если её нет — завести.

    Нужно на сохранении: слово, которое человек только что положил себе в словарь,
    обязано сразу иметь дом в слое. Иначе указатель у карточки остаётся пустым, и
    разрыв растёт с каждым новым сохранением.

    Механическая чистка здесь — последний заслон: единицы заводят и живой путь, и
    разовые скрипты, и массовые сборки. Слово с невидимым знаком внутри выглядит
    правильным, но заводит ВТОРУЮ единицу, которую поиск не найдёт никогда."""
    # Механическая половина двери — общая с прямыми писателями (см. `door_check`).
    # Тот, кто зовёт: сначала разрежь на значения (split_numbered_senses), потом заводи
    # каждое отдельно. Отказ здесь не ломает вызывающего — все проверяют результат на
    # пустоту, потому что единица и раньше могла не завестись.
    checked = door_check(text, lang)
    if not checked:
        return None
    text, key, kind = checked
    # ДВЕРЬ СЛОВА, дешёвая половина. Регистр заголовка правится ЗДЕСЬ, при заведении, а
    # не только на показе. Замер 19.08.2026: правило `german_headword_case` стояло лишь
    # в отрисовке, поэтому «Grundlegend» и «betäubung» ложились в базу как есть — экран
    # выглядел правильным, а данные оставались кривыми. Тот же урок уже разбирался
    # однажды в backend_server:10166, но до заведения единиц не дошёл.
    #
    # В сеть и к модели отсюда НЕ ходим: сохранение не должно ждать справочник и не
    # должно стоить денег. Дорогие ступени (обрезка, умлаут, устаревшее написание,
    # существует ли слово вообще) делает ночная работа, у неё для этого нет спешки.
    # Метка та же, что у парадигм глаголов: тесты и оффлайн-скрипты в боевую базу не
    # ходят. Прогон 19.08.2026 упёрся в таймаут именно на этом — дверь тянула справочник
    # родов из прода на каждом заведении единицы в тесте.
    _gate_off = (os.getenv("SKIP_STARTUP_SCHEMA_BOOTSTRAP") == "1"
                 and not os.getenv("WORD_GATE_LOOKUP"))
    if kind == "word" and lang == "de" and not _gate_off:
        try:
            from backend.german_word_gate import check_word, NOT_A_WORD
            verdict = check_word(text, allow_network=False, allow_model=False)
            if verdict.get("status") == NOT_A_WORD:
                # Дверь уже разбирала это написание и признала его не словом
                # («Abschiebu», «inkelgasse»). Новую единицу не заводим.
                #
                # Именно так мусор и получал прописку: ночное дообогащение видело текст
                # в чьей-то карточке и заводило его словом общего словаря. Замер
                # 19.08.2026: семь мусорных слов из четырнадцати завели не люди, а мы.
                # Карточка человека при этом остаётся — он её сохранил, это его право.
                logging.warning("единица не заведена: %r — дверь слова признала не словом",
                                str(text)[:60])
                return None
            fixed = str(verdict.get("text") or "").strip()
            if fixed and fixed != text:
                logging.info("дверь слова: заголовок исправлен %r → %r", text, fixed)
                text = fixed
                key = normalize_query(text)
        except Exception:
            logging.warning("дверь слова недоступна при заведении %r", str(text)[:60],
                            exc_info=True)
    body = _ANY_ARTICLE_RE.sub("", _SPACE_RE.sub(" ", str(text).strip())).strip()
    display = _SPACE_RE.sub(" ", str(text).strip()) if kind != "word" else body
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.id FROM bt_3_lex_surfaces s
                    JOIN bt_3_lex_units u ON u.id = s.unit_id
                    WHERE s.lang = %s AND s.surface_key = %s
                    ORDER BY (u.card IS NULL), u.id LIMIT 1;
                    """,
                    (lang, key),
                )
                row = cur.fetchone()
                if row:
                    return int(row[0])
                cur.execute(
                    """
                    INSERT INTO bt_3_lex_units (lang, kind, lemma, lemma_key, display, card_source)
                    VALUES (%s, %s, %s, %s, %s, 'сохранение')
                    ON CONFLICT (lang, kind, lemma_key, COALESCE(pos, ''), COALESCE(gender, ''))
                    DO UPDATE SET updated_at = NOW()
                    RETURNING id;
                    """,
                    (lang, kind, body or display, key, display),
                )
                unit_id = int(cur.fetchone()[0])
                cur.execute(
                    """
                    INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                    VALUES (%s, %s, %s, 'exact') ON CONFLICT DO NOTHING;
                    """,
                    (lang, key, unit_id),
                )
            conn.commit()
        return unit_id
    except Exception as exc:
        logging.debug("ensure unit failed for %r: %s", text, exc)
        return None


def attach_entry_to_unit(
    entry_id: int,
    *,
    word_de: str | None = None,
    word_ru: str | None = None,
    source_lang: str | None = None,
    target_lang: str | None = None,
    card: dict | None = None,
) -> int | None:
    """Проставить у только что сохранённой карточки указатель на её слово.

    Лучше делать это на сохранении, чем догонять разовыми проходами: иначе каждый
    новый день добавляет карточки без указателя, и слой отстаёт от жизни.

    Если передан разбор — он же кладётся НА ЕДИНИЦУ, и слово становится разобранным
    для всех сразу, а не только для того, кто его сохранил. Кладём лишь когда разбор
    полнее уже лежащего, и только на немецкую единицу: разбор описывает немецкое
    слово, на русской единице ему не место."""
    langs = {str(source_lang or "").lower(), str(target_lang or "").lower()}
    text, lang = "", ""
    if "de" in langs and str(word_de or "").strip():
        text, lang = str(word_de).strip(), "de"
    elif str(word_ru or "").strip():
        text = str(word_ru).strip()
        lang = next((l for l in langs if l and l != "de"), "ru")
    if not text:
        return None
    unit_id = ensure_unit(text, lang)
    if not unit_id:
        return None
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE bt_3_webapp_dictionary_queries SET lex_unit_id = %s "
                    "WHERE id = %s AND lex_unit_id IS NULL;",
                    (unit_id, int(entry_id)),
                )
            conn.commit()
    except Exception as exc:
        logging.debug("attach entry %s to unit failed: %s", entry_id, exc)
        return None
    if card and lang == "de":
        try:
            save_unit_card_if_richer(unit_id, card, source="сохранение")
        except Exception:
            logging.debug("разбор при сохранении не лёг на единицу %s", unit_id, exc_info=True)
        # Разбор лёг — значит и перевод из него становится СВЯЗЬЮ, сразу.
        #
        # Раньше связи делались только ночным разбором и разовыми прогонами, поэтому
        # каждый день появлялись «немые» слова: разбор есть, перевода нет ни одного, и
        # человек открывал карточку, не узнавая, что слово значит. Замеры: 172 таких
        # слова 15.08.2026, ещё 13 к 16.08, ещё 9 за сутки к 17.08 — то есть дыра
        # открывалась заново каждый день, а закрывалась вручную. Правило то же, что с
        # родом (см. _adopt_pos_gender_inline): если оно верно, его место на двери.
        try:
            sync_unit_links_from_card(unit_id, card, native_lang="ru")
        except Exception:
            logging.debug("перевод при сохранении не стал связью для %s", unit_id, exc_info=True)
    return unit_id


def attach_missing_entries(limit: int = 5000) -> dict:
    """Подобрать все карточки, оставшиеся без указателя на слово.

    Проставлять указатель в момент сохранения правильно, но одного этого мало: путей
    записи много (приложение, бот, шорткат, импорт, перенос по подписке), и каждый
    новый путь легко забыть — так уже случилось дважды за два дня. Поэтому кроме
    простановки на месте есть этот подбор: он ловит всё, что просочилось, независимо
    от того, каким путём карточка появилась.

    Дешёвый: обычно находит ноль строк."""
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, word_de, word_ru, source_lang, target_lang
                    FROM bt_3_webapp_dictionary_queries
                    WHERE lex_unit_id IS NULL
                    ORDER BY id DESC LIMIT %s;
                    """,
                    (int(limit),),
                )
                rows = cur.fetchall()
    except Exception as exc:
        logging.debug("attach missing entries: выборка не удалась: %s", exc)
        return {"found": 0, "attached": 0}
    attached = 0
    for entry_id, word_de, word_ru, source_lang, target_lang in rows:
        if attach_entry_to_unit(
            entry_id, word_de=word_de, word_ru=word_ru,
            source_lang=source_lang, target_lang=target_lang,
        ):
            attached += 1
    if rows:
        logging.info("привязка карточек к словам: найдено %d, привязано %d", len(rows), attached)
    return {"found": len(rows), "attached": attached}


def sync_unit_links_from_card(unit_id: int, card: dict, *, native_lang: str = "ru") -> dict:
    """Перечитать переводы слова из его РАЗБОРА и разложить по значениям.

    Разбор знает про слово больше, чем строка перевода из старого банка: у «die Scheide»
    в связях было только «влагалище», а разбор называет и «ножны» — целый смысл, которого
    человек иначе не увидит. У «betreffen» связь была «касаться, относиться» одной
    строкой, а в разборе это два значения.

    Поэтому после появления разбора переводы берём из него: главное значение первым,
    остальные следом. Старые связи НЕ удаляем — просто отодвигаем ниже: они могли
    прийти из живого сохранения человека, и терять их нельзя."""
    if not isinstance(card, dict) or not card:
        return {"senses": 0, "links": 0}
    meanings = card.get("meanings") if isinstance(card.get("meanings"), dict) else {}
    values: list[dict] = []
    primary = meanings.get("primary")
    if isinstance(primary, dict) and str(primary.get("value") or "").strip():
        values.append({"value": str(primary["value"]).strip(),
                       "note": str(primary.get("context") or "").strip()})
    for item in (meanings.get("secondary") or []):
        if isinstance(item, dict) and str(item.get("value") or "").strip():
            values.append({"value": str(item["value"]).strip(),
                           "note": str(item.get("context") or "").strip()})
    from_translations: list[dict] = []
    for item in (card.get("translations") or []):
        value = item.get("value") if isinstance(item, dict) else item
        if isinstance(value, str) and value.strip():
            from_translations.append({"value": value.strip(), "note": ""})

    # Разбор тоже бывает склеен: «ромб (геометрическая фигура); решётка (символ #)» —
    # это два значения в одной строке. Прогоняем через общий разрезатель, иначе свалка
    # вернулась бы с другой стороны. Длинные определения переводом не считаем и кладём
    # в пояснение к значению: «направление, к которому движутся» — это не перевод.
    def _pick(items: list[dict]) -> list[dict]:
        unique: list[dict] = []
        seen: set[str] = set()
        for item in items:
            for part in split_translation(item["value"]):
                value = part["value"].strip()
                if not value:
                    continue
                note = "; ".join(x for x in (part.get("label"), item.get("note")) if x)
                if len(value) > 60:
                    if unique:
                        unique[-1]["note"] = "; ".join(
                            x for x in (unique[-1].get("note"), value) if x)[:500]
                    continue
                key = normalize_query(value)
                if key and key not in seen:
                    seen.add(key)
                    unique.append({"value": value, "note": note})
        return unique

    # Список переводов смотрим не только когда значений НЕТ, но и когда после отсева от
    # них ничего не осталось. У «das Musterkind» все значения — определения на сто с
    # лишним знаков, а рядом лежат готовые «примерный ребёнок», «идеальный ребёнок»;
    # старый порядок до них не доходил, и слово оставалось вовсе без перевода — то есть
    # выпадало из выдачи, и за него платили второй раз.
    unique = _pick(values) or _pick(from_translations)
    if not unique:
        return {"senses": 0, "links": 0}

    made_links = 0
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                # Разбор описывает НАПИСАНИЕ, а не конкретное слово: у «die Kiefer»
                # (сосна) в карточке оказались оба смысла, и перенос приклеил к ней
                # «челюсть». Поэтому сверяемся со справочником разведения — он знает,
                # какому роду принадлежит значение, — и чужое не берём.
                cur.execute(
                    "SELECT lemma_key, COALESCE(gender, '') FROM bt_3_lex_units WHERE id = %s;",
                    (unit_id,),
                )
                row = cur.fetchone()
                lemma_key, own_gender = (row or ("", ""))
                cur.execute("SELECT kind FROM bt_3_lex_units WHERE id = %s;", (unit_id,))
                kind_row = cur.fetchone()
                kind_of_source = str((kind_row or ("",))[0] or "")
                rulings: dict[str, str] = {}
                if lemma_key:
                    try:
                        cur.execute(
                            "SELECT gloss_key, article FROM bt_3_lex_gloss_rulings WHERE lemma_key = %s;",
                            (lemma_key,),
                        )
                        rulings = {r[0]: r[1] for r in cur.fetchall()}
                    except Exception:
                        rulings = {}  # справочника ещё нет — работаем как раньше
                if rulings and own_gender:
                    unique = [
                        item for item in unique
                        if rulings.get(item["value"].strip().casefold(), own_gender) == own_gender
                    ]
                    if not unique:
                        return {"senses": 0, "links": 0}
                # Всё, что было раньше, отодвигаем за значения разбора, но сохраняем.
                cur.execute(
                    "UPDATE bt_3_lex_links SET rank = GREATEST(rank, 30) "
                    "WHERE from_unit = %s AND rank < 30;",
                    (unit_id,),
                )
                for sense_no, item in enumerate(unique, 1):
                    value = item["value"]
                    # Пример употребления — не перевод. Раньше он попадал в связи с
                    # рангом 10 и вставал первым: «abbestellen → Ваша подписка на
                    # рассылку успешно отменена», а настоящий перевод лежал ниже.
                    # Для слова такое пропускаем; у предложения перевод предложением —
                    # это норма, поэтому правило только для слов.
                    if kind_of_source == "word" and looks_like_example_not_translation(value):
                        continue
                    # Перевод на русский обязан содержать русские буквы. Немецкий
                    # пересказ переводом не является: «Du stinkst furchtbar.» →
                    # «Du stinkst fürchterlich» человеку ничего не объясняет.
                    if native_lang == "ru" and not _CYRILLIC_ANY_RE.search(value):
                        continue
                    cur.execute(
                        """
                        INSERT INTO bt_3_lex_senses (unit_id, sense_no, label, note, source)
                        VALUES (%s, %s, NULL, %s, 'разбор')
                        ON CONFLICT (unit_id, sense_no) DO UPDATE
                          SET note = EXCLUDED.note, source = 'разбор'
                        RETURNING id;
                        """,
                        (unit_id, sense_no, item["note"][:500] or None),
                    )
                    sense_id = cur.fetchone()[0]
                    # ДВЕРЬ. До 20.08.2026 здесь стоял прямой INSERT со своим правилом
                    # вида и с `value` как есть: ни чистки, ни проверки языка, ни
                    # запрета на свалку значений. Разбор — не человек, но и он приносит
                    # мусор: склеенные значения, невидимые знаки, русское слово там, где
                    # ждали немецкое. Заводим только то, что дверь пропустила.
                    checked = door_check(value, native_lang)
                    if not checked:
                        continue
                    clean_value, value_key, kind = checked
                    cur.execute(
                        """
                        INSERT INTO bt_3_lex_units (lang, kind, lemma, lemma_key, display, card_source)
                        VALUES (%s, %s, %s, %s, %s, 'разбор')
                        ON CONFLICT (lang, kind, lemma_key, COALESCE(pos, ''), COALESCE(gender, ''))
                        DO UPDATE SET updated_at = NOW()
                        RETURNING id;
                        """,
                        (native_lang, kind, clean_value, value_key, clean_value),
                    )
                    target_id = cur.fetchone()[0]
                    cur.execute(
                        """
                        INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                        VALUES (%s, %s, %s, 'exact') ON CONFLICT DO NOTHING;
                        """,
                        (native_lang, value_key, target_id),
                    )
                    for a, b in ((unit_id, target_id), (target_id, unit_id)):
                        cur.execute(
                            """
                            INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source, sense_id)
                            VALUES (%s, %s, %s, 'разбор', %s)
                            ON CONFLICT (from_unit, to_unit) DO UPDATE
                              SET rank = LEAST(bt_3_lex_links.rank, EXCLUDED.rank),
                                  sense_id = COALESCE(bt_3_lex_links.sense_id, EXCLUDED.sense_id),
                                  source = 'разбор';
                            """,
                            (a, b, 9 + sense_no, sense_id),
                        )
                    made_links += 1
            conn.commit()
    except Exception as exc:
        logging.debug("sync links from card failed for %s: %s", unit_id, exc)
        return {"senses": 0, "links": 0}
    return {"senses": len(unique), "links": made_links}


def count_units_needing_card(*, lang: str = "de") -> int:
    """Сколько слов ещё без разбора — ЧЕСТНОЕ число для утренней сводки.

    Считать остаток «сколько взяли минус сколько сделали» нельзя: выборка ограничена
    ночным потолком, и сводка отчиталась бы «осталось 86» при 3356 неразобранных, то
    есть «одна ночь» вместо семнадцати."""
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM bt_3_lex_units "
                    "WHERE lang = %s AND kind = 'word' AND card IS NULL;",
                    (lang,),
                )
                return int(cur.fetchone()[0])
    except Exception as exc:
        logging.debug("count units needing card failed: %s", exc)
        return 0


def unit_display(unit_id: int) -> str:
    """Написание единицы по её номеру.

    Нужно там, где на руках только номер: карточку потом собирает тот же `lookup`,
    что отдаёт разбор в приложении, — значит в личный словарь попадёт ровно то, что
    человек видел на экране, а не отдельно собранная копия."""
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(NULLIF(display, ''), lemma) FROM bt_3_lex_units WHERE id = %s;",
                    (int(unit_id),),
                )
                row = cur.fetchone()
        return str(row[0]).strip() if row and row[0] else ""
    except Exception as exc:
        logging.debug("unit display failed for %s: %s", unit_id, exc)
        return ""


def save_unit_card(unit_id: int, card: dict, *, source: str = "обогащение", cursor=None) -> bool:
    """Положить разбор НА единицу. Пишем только в слой; общий банк не трогаем.

    cursor передаёт тот, кто уже держит транзакцию (выдача слова по подписке): иначе
    из пула берётся второе соединение, пока первое не отпущено. Коммит в этом случае
    делает вызывающий — запись должна попасть в ту же транзакцию, что и карточка."""
    if not isinstance(card, dict) or not card:
        return False
    sql = ("UPDATE bt_3_lex_units SET card = %s::jsonb, card_source = %s, updated_at = NOW() "
           "WHERE id = %s;")
    params = (json.dumps(card, ensure_ascii=False), source, int(unit_id))
    try:
        if cursor is not None:
            cursor.execute(sql, params)
            _adopt_pos_gender_inline(cursor, unit_id, card)
            return True
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                _adopt_pos_gender_inline(cur, unit_id, card)
            conn.commit()
        return True
    except Exception as exc:
        logging.debug("save unit card failed for %s: %s", unit_id, exc)
        return False


# Оценка полноты разбора (CARD_CONTENT_KEYS / card_content_score) живёт в слое БД:
# ею пользуются и запись на единицу, и отдача карточки человеку, а импортировать слой БД
# отсюда обратно нельзя — вышло бы кольцо.
def save_unit_card_if_richer(unit_id: int, card: dict, *, source: str = "сохранение",
                             cursor=None) -> bool:
    """Положить разбор на единицу, но ТОЛЬКО если он полнее уже лежащего.

    Единица — общая, и её разбор виден всем, кто на слово подписан. Поэтому тонкое
    сохранение (быстрый перевод, тап в тренажёре) не имеет права затереть собранный
    ночью полный разбор: такое понижение получил бы каждый, а не только тот, кто
    сохранял. Сравниваем по числу заполненных блоков, а не по длине текста."""
    if not isinstance(card, dict) or not card:
        return False
    fresh = card_content_score(card)
    if fresh <= 0:
        return False

    def _current(cur):
        cur.execute("SELECT card FROM bt_3_lex_units WHERE id = %s;", (int(unit_id),))
        return cur.fetchone()

    try:
        if cursor is not None:
            row = _current(cursor)
        else:
            with get_db_connection_context() as conn:
                with conn.cursor() as cur:
                    row = _current(cur)
        if row is None:
            return False
        if fresh <= card_content_score(row[0] if isinstance(row[0], dict) else None):
            return False
    except Exception as exc:
        logging.debug("compare unit card failed for %s: %s", unit_id, exc)
        return False
    return save_unit_card(int(unit_id), card, source=source, cursor=cursor)


# Артикль однозначно выдаёт существительное, а вместе с ним и род. Это единственный
# признак, которому можно верить без модели: заглавная буква сама по себе НЕ признак
# («Hineingehen», «Nahtlos» — глагол и прилагательное с большой буквы), поэтому требуем
# ОБА условия сразу — артикль в разборе И заглавную первую букву.
_ARTICLE_TO_GENDER = {"der": "der", "die": "die", "das": "das"}
_CAPITALIZED_RE = re.compile(r"^[A-ZÄÖÜ]")


def _gender_from_card(card: dict | None) -> str:
    if not isinstance(card, dict):
        return ""
    return _ARTICLE_TO_GENDER.get(str(card.get("article") or "").strip().lower(), "")


# Одна и та же правка нужна и на живой двери (разбор кладут НА слово), и в ночном
# доборе. Держим её одним текстом запроса, чтобы две копии не разошлись.
#
# ⚠ kind = 'word' обязателен. Артикль в разборе фразы принадлежит существительному
# ВНУТРИ неё, а не самой фразе: у «Bei der Schlägerei wurde er übel zugerichtet» в
# разборе лежит «die», и без этой строки фраза стала бы существительным женского рода.
_ADOPT_POS_GENDER_SQL = """
    UPDATE bt_3_lex_units
       SET pos = %(pos)s,
           gender = CASE WHEN %(pos)s = 'noun'
                         THEN COALESCE(NULLIF(gender, ''), NULLIF(%(gender)s, ''))
                         ELSE gender END,
           pos_source = COALESCE(pos_source, 'card'),
           gender_source = CASE WHEN %(pos)s = 'noun' AND %(gender)s <> ''
                                THEN COALESCE(gender_source, 'card')
                                ELSE gender_source END,
           updated_at = NOW()
     WHERE id = %(id)s AND pos IS NULL AND kind = 'word'
       AND (%(pos)s <> 'noun' OR lemma ~ '^[A-ZÄÖÜ]')
       AND NOT EXISTS (
           SELECT 1 FROM bt_3_lex_units o
            WHERE o.lang = bt_3_lex_units.lang
              AND o.kind = bt_3_lex_units.kind
              AND o.lemma_key = bt_3_lex_units.lemma_key
              AND o.pos = %(pos)s
              AND COALESCE(o.gender, '') = %(gender)s
              AND o.id <> bt_3_lex_units.id
       );
"""

# Части речи, которые разбор называет ОДНОЗНАЧНО. Всё остальное — не часть речи
# («phrase», «sentence») или отказ модели («other», пусто), и брать его нельзя.
# ⚠ Составные ответы вроде «adjective|adverb» тоже не берём: два ответа — это не ответ,
# а часть речи входит в ключ опознания слова, и ошибка здесь разводит одно слово на два.
_KNOWN_POS = frozenset({
    "noun", "verb", "adjective", "adverb", "preposition", "conjunction",
    "pronoun", "numeral", "interjection", "particle", "article", "participle",
})


def _pos_from_card(card: dict | None) -> str:
    if not isinstance(card, dict):
        return ""
    said = str(card.get("part_of_speech") or "").strip().lower()
    return said if said in _KNOWN_POS else ""


def _adopt_pos_gender_inline(cur, unit_id: int, card: dict | None) -> None:
    """Разбор лёг на слово — значит слово тут же получает часть речи и род.

    Зачем на двери, а не только ночью
    ─────────────────────────────────
    Механика подбора рода существовала и раньше, но звалась ТОЛЬКО из ночного добора.
    Замер 16.08.2026: десять слов, заведённых в тот же день («Dill», «Trüffel»,
    «Chrysantheme», «Register»…), лежали с артиклем в разборе и пустым родом — то есть
    дыра открывалась заново каждый день, а чинилась раз в ночь. Человек, открывший
    слово днём, видел его без артикля.

    Опознание единицы — это лемма + часть речи + род, поэтому правка МЕНЯЕТ ключ, по
    которому слово находят, и может упереться в уникальный индекс. Запрос от этого
    защищён (NOT EXISTS), но если гонка всё же случится, ошибка не имеет права утащить
    за собой сохранение самого разбора — а внутри чужой транзакции она утащила бы всё.
    Поэтому точка отката: не вышло проставить род — разбор всё равно сохранён.

    ⚠ ПОЧЕМУ ЧАСТЬ РЕЧИ БЕРЁТСЯ ЛЮБАЯ, А НЕ ТОЛЬКО «существительное»
    ───────────────────────────────────────────────────────────────
    До 18.08.2026 здесь стояло жёсткое `pos = 'noun'`, и срабатывало оно лишь тогда,
    когда в разборе был артикль. То есть часть речи получали ТОЛЬКО существительные и
    ТОЛЬКО через артикль, а глагол, прилагательное, наречие и союз не получали её
    никогда — хотя разбор называет её прямым текстом. Единица же создаётся вообще без
    pos (INSERT выше её не заполняет), и заполнить было некому.

    Отсюда и брались слова «неизвестно что»: владелец 18.08.2026 разбирал их списком
    руками и справедливо спросил, почему это повторяется. Замер в тот день: разбор
    называет часть речи у 6 349 слов, а перенесена она была только у существительных.

    Теперь берётся то, что разбор назвал, — из закрытого списка настоящих частей речи.
    «phrase», «sentence», «other» и составные ответы вроде «adjective|adverb» не берутся:
    это не часть речи или не ответ, а pos входит в ключ опознания слова."""
    pos = _pos_from_card(card)
    gender = _gender_from_card(card)
    if not pos and gender:
        pos = "noun"                      # артикль в разборе — само по себе показание
    if not pos or not unit_id:
        return
    try:
        cur.execute("SAVEPOINT adopt_pos_gender;")
        try:
            cur.execute(_ADOPT_POS_GENDER_SQL,
                        {"pos": pos, "gender": gender, "id": int(unit_id)})
        except Exception as exc:
            cur.execute("ROLLBACK TO SAVEPOINT adopt_pos_gender;")
            logging.debug("adopt pos/gender inline for unit %s failed: %s", unit_id, exc)
        cur.execute("RELEASE SAVEPOINT adopt_pos_gender;")
    except Exception as exc:
        logging.debug("adopt pos/gender savepoint for unit %s failed: %s", unit_id, exc)


def adopt_pos_gender_from_card(unit_id: int, card: dict | None, *, lemma: str = "") -> bool:
    """Проставить слову часть речи и род, взяв их из собранного разбора. Без модели.

    Зачем: род требуется только существительным, и пока у слова не проставлена часть
    речи, оно формально «неизвестно что» — отсюда «Ausgabe» и «Käsefuß» без артикля в
    отчётах, хотя артикль лежал в разборе. Настоящая дыра — именно часть речи.

    Осторожность здесь не лишняя: опознание единицы = лемма + часть речи + род, поэтому
    правка МЕНЯЕТ ключ, по которому слово находят. Если рядом уже живёт такое же слово с
    проставленным родом, обновление упрётся в уникальный индекс — такую строку молча
    пропускаем, сливать единицы без решения владельца нельзя.

    Ничего не перезаписываем: трогаем только слова, у которых части речи нет вовсе."""
    pos = _pos_from_card(card)
    gender = _gender_from_card(card)
    if not pos and gender:
        pos = "noun"
    if not pos or not unit_id:
        return False
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(_ADOPT_POS_GENDER_SQL,
                            {"pos": pos, "gender": gender, "id": int(unit_id)})
                changed = cur.rowcount
            conn.commit()
        return bool(changed)
    except Exception as exc:
        logging.debug("adopt pos/gender for unit %s failed: %s", unit_id, exc)
        return False


def backfill_pos_gender_from_cards(*, limit: int = 500, lang: str = "de", dry_run: bool = False) -> dict:
    """Пройтись по словам, у которых часть речи не задана, а в разборе есть артикль.

    Идёт бесплатным шагом в ночной работе, поэтому новые такие слова закрываются сами:
    сегодня их 28, остальные подтянутся по мере того, как ночь соберёт им разбор."""
    report = {"candidates": 0, "updated": 0, "skipped": 0, "samples": []}
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, display, card->>'article'
                      FROM bt_3_lex_units
                     WHERE lang = %s AND kind = 'word' AND pos IS NULL
                       AND COALESCE(card->>'article', '') IN ('der', 'die', 'das')
                       AND lemma ~ '^[A-ZÄÖÜ]'
                     ORDER BY id
                     LIMIT %s;
                    """,
                    (str(lang or "de").strip().lower() or "de", int(limit)),
                )
                rows = cur.fetchall()
    except Exception as exc:
        logging.debug("backfill pos/gender selection failed: %s", exc)
        return report
    report["candidates"] = len(rows)
    for unit_id, display, article in rows:
        if len(report["samples"]) < 15:
            report["samples"].append({"word": display, "article": article})
        if dry_run:
            continue
        if adopt_pos_gender_from_card(int(unit_id), {"article": article}):
            report["updated"] += 1
        else:
            report["skipped"] += 1
    if report["updated"]:
        logging.info("часть речи и род проставлены по артиклю: %d слов", report["updated"])
    return report


def units_with_thin_card(limit: int, *, lang: str = "de", native_lang: str = "ru") -> list[dict]:
    """Слова, у которых разбор ЕСТЬ, но куцый: примеры и формы на месте, а значений,
    управления и сочетаний нет.

    Ночной добор их не берёт СОЗНАТЕЛЬНО: он смотрит только на слова вовсе без разбора,
    чтобы переключение планки «что считать полной карточкой» не запустило разом
    массовый пересбор за деньги. Поэтому такой пересбор — отдельный явный шаг с
    потолком, и вот его выборка.

    Порядок тот же, что у ночного: сначала слова, стоящие у людей на повторение по
    ближайшему сроку, потом по числу сохранивших."""
    if limit <= 0:
        return []
    rich = _dictionary_pool_word_fully_rich_sql("u.card")
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT u.id, u.display, u.lemma, u.gender, u.pos,
                           COALESCE(p.saved, 0) AS saved,
                           d.due_at,
                           (SELECT u2.display FROM bt_3_lex_links l
                              JOIN bt_3_lex_units u2 ON u2.id = l.to_unit
                             WHERE l.from_unit = u.id AND u2.lang = %s
                               AND position('___' in u2.display) = 0
                             ORDER BY l.rank, u2.id LIMIT 1) AS translation
                    FROM bt_3_lex_units u
                    LEFT JOIN (
                        SELECT lex_unit_id, COUNT(*) AS saved
                        FROM bt_3_webapp_dictionary_queries
                        WHERE lex_unit_id IS NOT NULL GROUP BY lex_unit_id
                    ) p ON p.lex_unit_id = u.id
                    LEFT JOIN (
                        SELECT q.lex_unit_id, MIN(st.due_at) AS due_at
                        FROM bt_3_card_srs_state st
                        JOIN bt_3_webapp_dictionary_queries q
                          ON q.id = st.card_id AND q.user_id = st.user_id
                        WHERE st.status <> 'suspended' AND q.lex_unit_id IS NOT NULL
                        GROUP BY q.lex_unit_id
                    ) d ON d.lex_unit_id = u.id
                    WHERE u.lang = %s AND u.kind = 'word'
                      AND u.card IS NOT NULL AND u.card <> '{{}}'::jsonb
                      AND NOT {rich}
                    ORDER BY (d.due_at IS NULL), d.due_at, saved DESC, u.id
                    LIMIT %s;
                    """,
                    (native_lang, str(lang or "de").strip().lower() or "de", int(limit)),
                )
                rows = cur.fetchall()
    except Exception as exc:
        logging.debug("units with thin card failed: %s", exc)
        return []
    return [
        {
            "id": r[0], "display": r[1], "lemma": r[2], "gender": r[3], "pos": r[4],
            "saved": r[5], "due_at": r[6], "translation": r[7],
        }
        for r in rows
    ]


def count_units_with_thin_card(*, lang: str = "de") -> int:
    """Сколько всего слов ждут пересбора — считаем отдельно от выборки, иначе отчёт
    покажет размер потолка вместо реального остатка."""
    rich = _dictionary_pool_word_fully_rich_sql("u.card")
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""SELECT COUNT(*) FROM bt_3_lex_units u
                        WHERE u.lang = %s AND u.kind = 'word'
                          AND u.card IS NOT NULL AND u.card <> '{{}}'::jsonb
                          AND NOT {rich};""",
                    (str(lang or "de").strip().lower() or "de",),
                )
                row = cur.fetchone()
        return int(row[0]) if row else 0
    except Exception as exc:
        logging.debug("count units with thin card failed: %s", exc)
        return 0


def thin_entries_with_unit_card(
    limit: int = 500,
    *,
    unit_id: int | None = None,
    entry_id: int | None = None,
    due_first: bool = True,
    lang: str = "de",
) -> list[dict]:
    """Личные карточки, которые пусты, хотя разбор их слова УЖЕ собран и оплачен.

    Ночной добор кладёт разбор на единицу — общую для всех. Тренажёр же читает личную
    карточку, и до неё разбор сам не доходит: на 01.08 таких карточек было 3648. Здесь
    мы их находим, чтобы перенести готовое даром (см. fill_thin_cards_from_units).

    Забираем ДВА случая, и второй важнее, чем кажется:
    1. в личной карточке нет ни одного примера — заготовка, человеку показывать нечего;
    2. в личной карточке примеры есть, но нет разборных блоков (значения, управление,
       сочетания), а на единице они ЕСТЬ. Такую карточку прежняя выборка не видела —
       примеры-то на месте, — и 195 человеческих карточек стояли пустыми при готовом и
       уже оплаченном разборе (замер 02.08.2026). Перенос только дополняет пустые поля,
       поэтому расширение не может ничего испортить.

    Порядок — сначала то, что человек увидит раньше всех: карточки, стоящие на
    повторение по ближайшему сроку, а уже потом всё остальное."""
    if limit <= 0:
        return []
    unit_rich = _dictionary_pool_word_fully_rich_sql("u.card")
    card_rich = _dictionary_pool_word_fully_rich_sql("q.response_json")
    # NOT (… IS TRUE), а не NOT (…): при отсутствующем ключе сравнение даёт NULL,
    # и обычное NOT выбрасывает строку из выборки вместо того, чтобы взять её.
    card_has_examples = (
        "((jsonb_typeof(q.response_json->'usage_examples') = 'array'"
        " AND jsonb_array_length(q.response_json->'usage_examples') > 0) IS TRUE)"
    )
    where = [
        # Разбор строится ПО ИЗУЧАЕМОМУ слову, поэтому и переносим только с единицы на
        # изучаемом языке. Без этого условия карточке «враг → der Feind» мог бы достаться
        # разбор русской единицы, и человек увидел бы русские формы у немецкого слова.
        "u.lang = %s",
        "u.card IS NOT NULL",
        "u.card <> '{}'::jsonb",
        "jsonb_typeof(u.card->'usage_examples') = 'array'",
        "jsonb_array_length(u.card->'usage_examples') > 0",
        f"(NOT {card_has_examples} OR ({unit_rich} AND NOT {card_rich}))",
    ]
    params: list[Any] = [str(lang or "de").strip().lower() or "de"]
    if unit_id:
        where.append("q.lex_unit_id = %s")
        params.append(int(unit_id))
    if entry_id:
        where.append("q.id = %s")
        params.append(int(entry_id))
    order = (
        "ORDER BY (s.due_at IS NULL), s.due_at, q.id DESC"
        if due_first else "ORDER BY q.id DESC"
    )
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT q.id, q.user_id, q.word_ru, q.word_de,
                           q.translation_de, q.translation_ru,
                           q.source_lang, q.target_lang,
                           q.response_json, u.card, u.id
                    FROM bt_3_webapp_dictionary_queries q
                    JOIN bt_3_lex_units u ON u.id = q.lex_unit_id
                    LEFT JOIN bt_3_card_srs_state s
                           ON s.card_id = q.id AND s.user_id = q.user_id
                          AND s.status <> 'suspended'
                    WHERE {' AND '.join(where)}
                    {order}
                    LIMIT %s;
                    """,
                    (*params, int(limit)),
                )
                rows = cur.fetchall()
    except Exception as exc:
        logging.debug("выборка тонких карточек с готовым разбором не удалась: %s", exc)
        return []
    return [
        {
            "entry_id": r[0], "user_id": r[1], "word_ru": r[2], "word_de": r[3],
            "translation_de": r[4], "translation_ru": r[5],
            "source_lang": r[6], "target_lang": r[7],
            "response_json": r[8] if isinstance(r[8], dict) else {},
            "card": r[9] if isinstance(r[9], dict) else {},
            "unit_id": r[10],
        }
        for r in rows
    ]


def lookup(word: str, *, source_lang: str, target_lang: str) -> dict | None:
    """Карточка из слоя единиц или None, если слово нам незнакомо.

    Порядок ровно тот, что задумывался: нормализуем написание → ищем указатель на языке
    запроса → берём единицу → добираем переводы по связям. Обратное направление отдельной
    ветки не требует: «враг» — такая же единица, у неё есть связь с «der Feind»."""
    query_lang = str(source_lang or "").strip().lower()
    other_lang = str(target_lang or "").strip().lower()
    # Сначала пробуем написание КАК ЕСТЬ, и только потом без артикля. Порядок важен:
    # «Das kriegen wir hin» — целая фраза, и снятие «Das» превратило бы её в обрубок,
    # тогда как «der Rüpel» и «Rüpel» обязаны вести в одно слово.
    exact_key = _SPACE_RE.sub(" ", str(word or "").strip()).casefold()
    keys = [k for k in dict.fromkeys([exact_key, normalize_query(word)]) if k]
    if not keys or not query_lang or not other_lang:
        return None
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                units: list[dict] = []
                for key in keys:
                    units = _fetch_units(cur, lang=query_lang, surface_key=key)
                    if units:
                        break
                if not units:
                    return None
                unit = _pick_unit(units, requested_article=article_of(word))
                if not unit:
                    return None
                links = _fetch_links(cur, unit["id"], want_lang=other_lang)
                if not links:
                    # Единица есть, а перевода на нужный язык нет — отдавать нечего,
                    # пусть обычный путь сходит в переводчик.
                    return None
                item = _build_item(unit, links, source_lang=query_lang, target_lang=other_lang)
                _mark_asked_form(item, asked=word, unit=unit, query_lang=query_lang)
                # Соседей ищем по написанию БЕЗ артикля, даже когда спросили с ним:
                # человек, открывший «der Kiefer», должен знать, что есть и «die Kiefer».
                siblings = units
                bare_key = normalize_query(word)
                if bare_key and bare_key != keys[0]:
                    siblings = _fetch_units(cur, lang=query_lang, surface_key=bare_key) or units
                if len(siblings) > 1:
                    item["homographs"] = _collect_homographs(
                        cur, siblings, unit, want_lang=other_lang,
                    )
                return item
    except Exception as exc:
        logging.debug("lex units lookup failed for %r: %s", word, exc)
        return None


def pos_of_surface(text: str, lang: str = "de") -> str:
    """Часть речи слова по нашему собственному банку. Без модели и без денег.

    Зачем отдельная функция. Быстрый перевод — гонка обычных переводчиков, они частей
    речи не отдают, и в карточке быстрого перевода её не было вовсе: владелец 08.08.2026
    прислал «Soweit → Насколько» без единой пометы. Между тем часть речи у нас уже лежит
    у 4 787 немецких единиц, и артикль на этот же экран подтягивается ровно так же —
    отдельным дешёвым запросом после ответа переводчика.

    Пример из того же скриншота: «Soweit» с большой буквы выглядит существительным, и
    помета «наречие» сразу показала бы, что это не оно.

    Возвращает пустую строку, если слова у нас нет или часть речи не проставлена, —
    молчание здесь честнее догадки.
    """
    surface_key = normalize_query(text)
    if not surface_key:
        return ""
    try:
        from backend.database import get_db_connection_context
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                units = _fetch_units(cur, lang=str(lang or "de"), surface_key=surface_key)
    except Exception:
        return ""
    if not units:
        return ""
    # Одно написание может стоять за несколькими единицами («der Kiefer» — челюсть и
    # сосна). Часть речи у них при этом обычно одна; когда мнения расходятся, молчим,
    # а не выбираем наугад.
    kinds = {str(u.get("pos") or "").strip() for u in units}
    kinds.discard("")
    return kinds.pop() if len(kinds) == 1 else ""


def units_needing_synonyms(limit: int, *, lang: str = "de") -> list[dict]:
    """Слова, у которых разбор УЖЕ есть, а синонимов в нём нет.

    Отдельный отбор понадобился потому, что ночной добор смотрит на слова БЕЗ разбора
    (units_needing_card) и уже обогащённые не трогает вовсе. А синонимы мы стали просить
    только 10.08.2026 — до этого их не просил ни один промпт живого пути. Значит всё
    накопленное так и осталось бы без них: замер того же дня — 9 469 слов, из которых
    9 261 лежат у людей в личных карточках.

    Порядок тот же, что у основного добора, и по той же причине: сначала то, что человек
    увидит завтра. Слова, стоящие у кого-то на повторение, идут по ближайшему сроку;
    дальше — по числу людей, сохранивших слово себе.

    Берём и перевод: он нужен запросу как опора, иначе модель ищет синонимы не тому
    значению («der Zug» — поезд или тяга).
    """
    if limit <= 0:
        return []
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.id, u.display, u.pos, u.gender,
                           COALESCE(p.saved, 0) AS saved,
                           d.due_at,
                           (SELECT u2.display FROM bt_3_lex_links l
                              JOIN bt_3_lex_units u2 ON u2.id = l.to_unit
                             WHERE l.from_unit = u.id AND u2.lang = 'ru'
                               AND position('___' in u2.display) = 0
                             ORDER BY l.rank, u2.id LIMIT 1) AS translation
                    FROM bt_3_lex_units u
                    LEFT JOIN (
                        SELECT lex_unit_id, COUNT(*) AS saved
                        FROM bt_3_webapp_dictionary_queries
                        WHERE lex_unit_id IS NOT NULL GROUP BY lex_unit_id
                    ) p ON p.lex_unit_id = u.id
                    LEFT JOIN (
                        SELECT q.lex_unit_id, MIN(st.due_at) AS due_at
                        FROM bt_3_card_srs_state st
                        JOIN bt_3_webapp_dictionary_queries q
                          ON q.id = st.card_id AND q.user_id = st.user_id
                        WHERE st.status <> 'suspended' AND q.lex_unit_id IS NOT NULL
                        GROUP BY q.lex_unit_id
                    ) d ON d.lex_unit_id = u.id
                    WHERE u.lang = %s AND u.kind = 'word'
                      AND u.card IS NOT NULL AND jsonb_typeof(u.card) = 'object'
                      AND jsonb_array_length(COALESCE(u.card->'synonyms', '[]'::jsonb)) = 0
                      -- Слово, у которого мы уже спрашивали и получили пустоту, больше
                      -- не берём: у части слов близких синонимов действительно нет, и
                      -- спрашивать о них каждую ночь значит платить за один и тот же
                      -- отказ бесконечно.
                      AND NOT (u.card ? 'synonyms_asked_at')
                    ORDER BY (d.due_at IS NULL), d.due_at, saved DESC, u.id
                    LIMIT %s;
                    """,
                    (lang, int(limit)),
                )
                rows = cur.fetchall()
    except Exception as exc:
        logging.debug("units needing synonyms failed: %s", exc)
        return []
    return [
        {"id": r[0], "display": r[1], "pos": r[2], "gender": r[3],
         "saved": r[4], "due_at": r[5], "translation": r[6] or ""}
        for r in rows
    ]


def count_units_needing_synonyms(*, lang: str = "de") -> int:
    """Сколько слов ещё ждут синонимов — для утренней строки отчёта."""
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT count(*) FROM bt_3_lex_units
                    WHERE lang = %s AND kind = 'word'
                      AND card IS NOT NULL AND jsonb_typeof(card) = 'object'
                      AND jsonb_array_length(COALESCE(card->'synonyms', '[]'::jsonb)) = 0
                      AND NOT (card ? 'synonyms_asked_at');
                    """,
                    (lang,),
                )
                return int(cur.fetchone()[0] or 0)
    except Exception:
        return 0
