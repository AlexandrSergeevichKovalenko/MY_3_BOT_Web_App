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

# Подпись связи, которую поставил рукой ВЛАДЕЛЕЦ, разбирая спорную фразу. Пишет её
# `promote_owner_translation` (backend/database.py) вместе с рангом 1. Имя вынесено в
# константу, чтобы выдача и запись не разошлись строкой: 20.08.2026 они разошлись не
# строкой, а правилом сортировки, и решение человека проигрывало машине на 56 словах.
OWNER_CHOICE_SOURCE = "вычитка"

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
        SELECT u.id, u.lang, u.kind, u.display, u.lemma, u.pos, u.gender, l.rank, u.card,
               l.source
        FROM bt_3_lex_links l
        JOIN bt_3_lex_units u ON u.id = l.to_unit
        -- Заготовки упражнений отсекаем В ЗАПРОСЕ, а не после: у «anlegen» их 33 штуки,
        -- и при отборе «первых шести» они съедали выдачу целиком — слово оставалось
        -- вообще без перевода. Связи с разобранным значением идут первыми.
        --
        -- ВЫБОР ВЛАДЕЛЬЦА ИДЁТ ПЕРЕД ЛЮБОЙ МАШИННОЙ СОРТИРОВКОЙ. Отбор «сначала со
        -- значением» появился 27.07.2026 вместе с разрезанием свалок и был верен: у
        -- разрезанного значения номер есть, у старой свалки — нет. Но связь, которую
        -- ставит рукой человек на экране спорных фраз (promote_owner_translation,
        -- backend/database.py), номера значения не получает — и проигрывала машинному
        -- переводу, хотя лежит с рангом 1. Замер 20.08.2026 по 119 решениям владельца:
        -- в базе всё записано верно, а на экране его перевод стоял вторым в 56 случаях
        -- («Es tut mir leid wegen der Verwirrung!» — выбрано «Извините из-за путаницы!»,
        -- показано «Извините за путаницу!»). Тот же экран в списке слов брал перевод по
        -- рангу и показывал ПРАВИЛЬНЫЙ — одно слово, два разных русских на двух экранах.
        WHERE l.from_unit = %s AND u.lang = %s AND l.rank < %s
          AND position('___' in u.display) = 0
        ORDER BY (l.source IS DISTINCT FROM %s OR l.rank <> 1),
                 (l.sense_id IS NULL), l.rank, u.id
        LIMIT %s;
        """,
        # Берём с запасом. Показать надо _MAX_LINKS штук, но часть из них — пересказы
        # друг друга («скобка» и «Скобка, скрепка»), и отсеиваются они уже здесь, в
        # коде. Если брать ровно шесть, дубли занимают места ДО отсева, и настоящие
        # значения человек не увидит: замер 11.08.2026 — 338 слов показывали ровно
        # шесть строк, где половина одно и то же. Отбор в _build_item.
        (unit_id, want_lang, _DEMOTED_RANK, OWNER_CHOICE_SOURCE, _MAX_LINKS * 4),
    )
    return [
        {"id": r[0], "lang": r[1], "kind": r[2], "display": r[3], "lemma": r[4],
         "pos": r[5], "gender": r[6], "rank": r[7], "card": r[8] if isinstance(r[8], dict) else None,
         "source": str(r[9] or "")}
        for r in cur.fetchall()
    ]


def is_owner_choice(link: dict) -> bool:
    """Эту связь поставил рукой ВЛАДЕЛЕЦ на экране спорных фраз, а не модель.

    Подпись ставит `promote_owner_translation` (backend/database.py): source «вычитка»
    и ранг 1. Ранг здесь обязателен — той же подписью помечены понижённые связи
    (ранг 950) у слов, которые владелец разбирал раньше и признал негодными; поднимать
    их обратно наверх нельзя."""
    return str(link.get("source") or "") == OWNER_CHOICE_SOURCE and int(link.get("rank") or 0) == 1


def _pick_unit(units: list[dict], *, requested_article: str) -> dict | None:
    """Из нескольких единиц одного написания выбираем нужную.

    Омографы («der Kiefer» челюсть / «die Kiefer» сосна) различаются только артиклем:
    если он в запросе есть — берём совпадающий, если нет — берём слово (не словоформу)
    с самым полным разбором, а при равенстве не гадаем и отдаём первое по алфавиту рода,
    чтобы ответ был устойчивым от запроса к запросу.

    ⚠ ТОЧНОЕ НАПИСАНИЕ БЬЁТ СЛОВОФОРМУ, и это стоит ПЕРЕД сравнением разборов.
    «rutsche» — это и заголовок слова «die Rutsche» (горка), и форма глагола
    «ausrutschen» (поскользнуться). У глагола разбор был, у существительного нет, и
    отбор «у кого разбор полнее» уверенно отдавал горку за глагол. Набравший «die
    Rutsche» читал «подскользнуться» — замер 21.08.2026. Полнота разбора не может
    перевешивать то, ЧЕМ написание является: у формы всегда есть своё слово, и
    показывать её вместо точного совпадения — значит отвечать не на заданный вопрос.
    Виды написаний ставит указатель `bt_3_lex_surfaces.match_kind` при заведении, мы
    ничего не выводим сами."""
    if not units:
        return None
    if len(units) == 1:
        return units[0]
    exact = [u for u in units if str(u.get("match_kind") or "") != "inflected"]
    if exact:
        units = exact
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


def drop_nested_translations(values: list[str], *, protected: set[str] | None = None) -> list[str]:
    """Убрать переводы, целиком сидящие внутри соседних.

    `protected` — ключи переводов, которые выбрасывать НЕЛЬЗЯ ни при каких условиях: это
    выбор владельца, сделанный руками на экране спорных фраз. Правило «остаётся короткий»
    его съедало целиком: «перелезать» из пула сидит внутри выбранного «Перелезать через
    что-то», и на экране оставался пул, а решение человека исчезало вовсе. Замер
    20.08.2026: так пропали 3 выбора из 108 («Über etwas steigen», «Wappnen gegen etwas»,
    карибский круиз — там от выбора остался обрубок «…такие как» вместо «…такие как эти»).
    Когда защищён длинный, выбрасывается короткий: пересказ убрать всё равно надо, просто
    не тот, который выбрал человек.

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
    safe = {index for index, key in enumerate(keys) if key in (protected or set())}
    drop: set[int] = set()

    def _drop(index: int, instead: int) -> None:
        """Выбросить `index`, а если он защищён — то `instead`. Защищены оба — не трогаем."""
        if index not in safe:
            drop.add(index)
        elif instead not in safe:
            drop.add(instead)

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
                _drop(j if keep == i else i, i if keep == i else j)
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
            if note and _translation_key(note.group(1)) == short:
                _drop(i, j)
            else:
                _drop(j, i)
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
    # Выбор владельца — то, что он выбрал руками на экране спорных фраз. Его нельзя
    # выбросить как «пересказ соседнего»: правило показа не имеет права отменять
    # решение человека. Ключи собираем ДО отсева, потому что отсев работает по ним.
    owner_keys: set[str] = set()
    for link in shown:
        for piece in split_numbered_senses(link["display"]):
            # Регистр правим ДО отсева повторов: иначе «Перемена, изменение» и
            # «перемена, изменение» считаются разными строками и остаются обе.
            value = normalize_translation_case(piece, german_pos=german_pos)
            values.append(value)
            if is_owner_choice(link):
                owner_keys.add(_translation_key(value))
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
    values = drop_nested_translations(deduped, protected=owner_keys)[:_MAX_LINKS]
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
                    -- СЛОВА И СЛОВОСОЧЕТАНИЯ. Предложения сюда НЕ входят намеренно.
                    --
                    -- Решение владельца 25.08.2026: «словосочетания греем, предложения
                    -- не греем — там есть само предложение и перевод, этого достаточно».
                    -- У словосочетания перевода мало: «Die Jagd auf» не объясняет ни
                    -- падежа, ни того, как это употребить, — поэтому его разбор нужен.
                    --
                    -- Пока условие было `kind = 'word'`, словосочетания не попадали в
                    -- очередь ВООБЩЕ и копились пустыми: замер 25.08.2026 — 1 793 штуки,
                    -- на них подписаны живые люди.
                    WHERE u.lang = %s AND u.kind IN ('word', 'collocation') AND u.card IS NULL
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


def retitle_unit(cur, unit_id: int, text: str) -> str:
    """Переименовать слово: написание, лемма, ключ поиска И ВИД ЗАПИСИ — одним местом.

    ⚠ ВИД ЗАПИСИ МЕНЯЕТСЯ ВМЕСТЕ С НАПИСАНИЕМ, и забыть его нельзя. `kind` это не
    украшение: ночной добор берёт в работу ТОЛЬКО `kind = 'word'`
    (`units_needing_card`). Слово, оставшееся «оборотом» после переименования, не
    получит разбор НИКОГДА — не завтра, а никогда, и на экране навсегда останется пусто.

    Так и вышло с «der Simulator, -en»: школьный хвост «, -n» делал из одного слова две
    лексемы, дверь честно записала «оборот», а после снятия хвоста вид никто не пересчитал.
    Замер 21.08.2026: шесть слов висели невидимками для ночной работы. Правку названия
    делают три места (решение по спорной фразе, разгон правки, скрипты уборки), поэтому
    правило живёт здесь, а не копией в каждом.

    Возвращает вид записи, который получился.
    """
    # ВХОД ЧИСТИТСЯ ЗДЕСЬ, а не у вызывающего. Раньше стояло `str(text).strip()`, а
    # чистка была обязанностью того, кто зовёт: сегодня зовущий один и он чистит, но
    # правило, которое держится на памяти вызывающего, ломается на втором вызывающем.
    #
    # Что именно проезжало без неё (замер 22.08.2026 по 41 628 словам живой базы):
    #
    #     'трениe'      латинская «e» внутри русского слова — глазом не видно,
    #     'плаксa'      латинская «a»                          а поиск не найдёт
    #     'устройcтво'  латинская «c»                          НИКОГДА: ключ поиска
    #     'Грубo …'     латинская «o»                          хранит ту же букву
    #     'Кроме того,' хвостовая запятая
    #
    # Семь заголовков, и шесть из них — дубликаты уже существующего правильного слова.
    text = clean_text(str(text or "")).strip()
    kind = _kind_for_text(text)
    key = normalize_query(text)
    cur.execute(
        "UPDATE bt_3_lex_units SET display=%s, lemma=%s, lemma_key=%s, kind=%s, "
        "updated_at=NOW() WHERE id=%s;",
        (text, text, key, kind, int(unit_id)),
    )
    # ⚠ НОВОЕ НАПИСАНИЕ ОБЯЗАНО СТАТЬ ДВЕРЬЮ ПОИСКА. Слово ищется не по заголовку, а по
    # указателю написаний: без этой строки переименованное слово перестаёт находиться ПО
    # СВОЕМУ ЖЕ ИМЕНИ. Поймано 21.08.2026 на «die Wettbewerbsregeln» — заголовок
    # почистили, а в указателе остался только старый мусорный ключ, и словарь отвечал
    # «не знаю» на собственное слово.
    #
    # Старые написания НЕ удаляются: указатель для того и заведён, чтобы человек,
    # запомнивший кривой вариант, продолжал находить слово и видел исправленный заголовок.
    cur.execute(
        """INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
           SELECT lang, %s, id, 'exact' FROM bt_3_lex_units WHERE id = %s
           ON CONFLICT DO NOTHING;""",
        (key, int(unit_id)),
    )
    return kind


def merge_unit_into(cur, form_id: int, keep_id: int) -> None:
    """Склеить лишнюю строку словаря с настоящим словом. Переносит, а не удаляет.

    ПОЧЕМУ НЕ ПРОСТО DELETE. Проверка вреда 19.08.2026: на строку словаря ссылаются
    восемь таблиц — поверхности, личные карточки людей, источники, связи, значения,
    разбор фраз. Простой DELETE либо падает на внешнем ключе, либо утаскивает за собой
    чужие данные. Поэтому сначала ПЕРЕНОС, потом снятие строки.

    ЖИВЁТ ЗДЕСЬ, А НЕ В СКРИПТЕ. До 25.08.2026 эти двадцать запросов лежали внутри
    `scripts/merge_form_units_into_lemma.py`, и второму вызывающему пришлось бы их
    скопировать. Копия такого размера расходится с оригиналом на первой же правке —
    а цена расхождения тут потеря чьей-то карточки. Скрипт теперь зовёт эту функцию.

    ЧТО КУДА ЕДЕТ
        поверхности      → на настоящее слово: поиск по старому написанию продолжает
                           находить слово;
        личные карточки  → перецепляются, человек своё слово не теряет;
        источники, связи → переносятся, при совпадении пропускаются (есть уникальность);
        значения         → НЕ переносятся: они описывают форму, а у настоящего слова
                           свои. Уходят вместе со строкой, и это осознанно.
    """
    cur.execute("""UPDATE bt_3_lex_surfaces s SET unit_id=%s
                   WHERE s.unit_id=%s AND NOT EXISTS (
                       SELECT 1 FROM bt_3_lex_surfaces t
                        WHERE t.lang=s.lang AND t.surface_key=s.surface_key
                          AND t.unit_id=%s)""", (keep_id, form_id, keep_id))
    cur.execute("DELETE FROM bt_3_lex_surfaces WHERE unit_id=%s", (form_id,))
    cur.execute("UPDATE bt_3_webapp_dictionary_queries SET lex_unit_id=%s "
                "WHERE lex_unit_id=%s", (keep_id, form_id))
    cur.execute("""UPDATE bt_3_lex_unit_sources s SET unit_id=%s
                   WHERE s.unit_id=%s AND NOT EXISTS (
                       SELECT 1 FROM bt_3_lex_unit_sources t
                        WHERE t.unit_id=%s AND t.entry_id=s.entry_id
                          AND t.side=s.side)""", (keep_id, form_id, keep_id))
    cur.execute("DELETE FROM bt_3_lex_unit_sources WHERE unit_id=%s", (form_id,))
    # Связи с обеих сторон. Уникальность (from_unit, to_unit): переносим только те,
    # которых у настоящего слова ещё нет. Прогон 19.08.2026 упал именно здесь, и
    # слияние откатилось целиком.
    cur.execute("""UPDATE bt_3_lex_links l SET from_unit=%s
                   WHERE l.from_unit=%s AND l.to_unit <> %s AND NOT EXISTS (
                       SELECT 1 FROM bt_3_lex_links t
                        WHERE t.from_unit=%s AND t.to_unit=l.to_unit)""",
                (keep_id, form_id, keep_id, keep_id))
    cur.execute("""UPDATE bt_3_lex_links l SET to_unit=%s
                   WHERE l.to_unit=%s AND l.from_unit <> %s AND NOT EXISTS (
                       SELECT 1 FROM bt_3_lex_links t
                        WHERE t.to_unit=%s AND t.from_unit=l.from_unit)""",
                (keep_id, form_id, keep_id, keep_id))
    cur.execute("DELETE FROM bt_3_lex_links WHERE from_unit=%s OR to_unit=%s",
                (form_id, form_id))
    for table in ("bt_3_phrase_check", "bt_3_phrase_review"):
        cur.execute(f"UPDATE {table} SET unit_id=%s WHERE unit_id=%s", (keep_id, form_id))
    cur.execute("DELETE FROM bt_3_lex_senses WHERE unit_id=%s", (form_id,))
    cur.execute("DELETE FROM bt_3_lex_units WHERE id=%s", (form_id,))


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


# ┌─ НАЙДЕНО 22.08.2026, ОТКРЫТО, ЖДЁТ РЕШЕНИЯ ВЛАДЕЛЬЦА ───────────────────────┐
# │                                                                             │
# │ 2460 РУССКИХ СЛОВ ЛЕЖАТ В БАЗЕ ДВАЖДЫ. Ровно по две копии, ни у одного      │
# │ три — то есть это след одного события, а не накопление.                     │
# │                                                                             │
# │     ключ поиска у копий ОДИНАКОВЫЙ, написание отличается заглавной буквой:  │
# │         «Совершенный» / «совершенный», «Щёлкать» / «щёлкать»                │
# │     первые копии заведены 27.07.2026 одним прогоном,                        │
# │     вторые накапливались по дням с 29.07 по 06.08.                          │
# │                                                                             │
# │ ЧЕЛОВЕКУ ЭТО СТОИТ ПЕРЕВОДОВ, а не только порядка: переводы разъезжаются    │
# │ по половинкам и он видит не все.                                            │
# │                                                                             │
# │     «выбрасывать»  половина 1: wegwerfen, entsorgen                         │
# │                    половина 2: wegschmeißen, wegwerfen, werfen, ausschütteln│
# │     «направление»  половина 1: die Zielrichtung, die Überweisung            │
# │                    половина 2: Entsendung                                   │
# │                                                                             │
# │ НЕМЕЦКИХ таких пар всего 5 — там же, где ключ совпал.                       │
# │                                                                             │
# │ ПОЧЕМУ НЕ ПОЧИНЕНО СРАЗУ: слияние удаляет записи, а удаление в этом проекте │
# │ показывают владельцу до, а не после. Плюс сначала надо понять, что за       │
# │ прогон 27.07 их завёл, иначе следующий такой же заведёт их снова.           │
# │                                                                             │
# │ Перемерить: пары по (lang, lemma_key) при a.id < b.id.                      │
# └─────────────────────────────────────────────────────────────────────────────┘
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
    passed = _word_gate_for_new_unit(text, key, kind, lang)
    if not passed:
        return None
    text, key = passed
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


def _word_gate_for_new_unit(text: str, key: str, kind: str, lang: str) -> tuple[str, str] | None:
    """Дверь СЛОВА перед заведением единицы. None — заводить нельзя.

    Вынесена из `ensure_unit` 28.08.2026, когда у слоя появился второй заводчик
    (`ensure_unit_for_lemma`). Копировать дверь второму нельзя ни в каком виде: этот
    проект уже проходил ровно это — `sync_unit_links_from_card` заводила единицы мимо
    двери, и мусор получал прописку в общем словаре. Одна дверь на всех заводчиков.

    Возвращает (написание, ключ поиска) — оба могут быть исправлены дверью.
    """
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
    return text, key


def ensure_unit_for_lemma(text: str, lang: str, *, pos: str) -> int | None:
    """Единица ИМЕННО ЭТОГО слова с ЭТОЙ частью речи. Заводит, если её нет.

    Чем отличается от `ensure_unit` и зачем нужна отдельно
    ─────────────────────────────────────────────────────
    `ensure_unit` ищет по УКАЗАТЕЛЮ ФОРМ: набранное написание может оказаться формой
    совсем другого слова, и тогда она вернёт чужую единицу. Для сохранения это верно —
    человек, набравший «Häuser», должен попасть в «das Haus». Но когда мы кладём разбор
    СЛОВА, чужая единица — это катастрофа.

    ┌─ ПОВОД, ЗАМЕР 27.08.2026. ──────────────────────────────────────────────────┐
    │ «entscheiden» — глагол «решать». И одновременно дательный падеж              │
    │ множественного числа существительного «der Entscheid» (решение). Указатель   │
    │ форм законно ведёт это написание на существительное (единица 26384,          │
    │ match_kind='inflected'), поэтому `ensure_unit('entscheiden')` возвращала      │
    │ «der Entscheid», и разбор ГЛАГОЛА лёг бы на СУЩЕСТВИТЕЛЬНОЕ.                 │
    └─────────────────────────────────────────────────────────────────────────────┘

    Здесь опознание идёт по ЛЕММЕ и ЧАСТИ РЕЧИ, а не по написанию. Слово-двойник
    получает СВОЮ единицу и свой ярлычок в указателе — база это разрешает и давно так
    живёт: ключ указателя уникален по тройке (язык, написание, единица), а на «auf»
    висит 35 единиц (замер 28.08.2026). Читающая сторона к этому готова: `_from_units`
    берёт до 12 единиц на написание, а какая нужна человеку — спрашивают у него
    (правило владельца 26.08.2026: не решаем за пользователя).

    Часть речи ОБЯЗАТЕЛЬНА: она входит в опознание единицы, и заводить «неизвестно что»
    нельзя — такая единица не сольётся со статьёй и породит лишний вопрос человеку.
    """
    known_pos = str(pos or "").strip().lower()
    if not known_pos:
        return None
    checked = door_check(text, lang)
    if not checked:
        return None
    text, key, kind = checked
    passed = _word_gate_for_new_unit(text, key, kind, lang)
    if not passed:
        return None
    text, key = passed
    body = _ANY_ARTICLE_RE.sub("", _SPACE_RE.sub(" ", str(text).strip())).strip()
    display = _SPACE_RE.sub(" ", str(text).strip()) if kind != "word" else body
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                # Своё слово — это совпадение ЛЕММЫ. Часть речи либо та же, либо ещё не
                # проставлена (её допишет разбор). Чужая часть речи — чужое слово.
                cur.execute(
                    """
                    SELECT id FROM bt_3_lex_units
                    WHERE lang = %s AND kind = %s AND lemma_key = %s
                      AND COALESCE(pos, '') IN ('', %s)
                    ORDER BY (COALESCE(pos, '') = %s) DESC, (card IS NULL), id
                    LIMIT 1;
                    """,
                    (lang, kind, key, known_pos, known_pos),
                )
                row = cur.fetchone()
                if row:
                    unit_id = int(row[0])
                else:
                    cur.execute(
                        """
                        INSERT INTO bt_3_lex_units
                            (lang, kind, lemma, lemma_key, display, pos, card_source)
                        VALUES (%s, %s, %s, %s, %s, %s, 'сравнение отличий')
                        ON CONFLICT (lang, kind, lemma_key, COALESCE(pos, ''), COALESCE(gender, ''))
                        DO UPDATE SET updated_at = NOW()
                        RETURNING id;
                        """,
                        (lang, kind, body or display, key, display, known_pos),
                    )
                    unit_id = int(cur.fetchone()[0])
                # Ярлычок в указателе форм. Соседние единицы того же написания остаются
                # на месте: ключ уникален по тройке (язык, написание, единица).
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
        logging.debug("ensure unit for lemma failed for %r (%s): %s", text, known_pos, exc)
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
                    # Считаем ТО ЖЕ, что и берёт очередь (units_needing_card): слова и
                    # словосочетания. Иначе отчёт «осталось N» врал бы в меньшую сторону
                    # и владелец не увидел бы 1 793 пустых словосочетания.
                    "SELECT COUNT(*) FROM bt_3_lex_units "
                    "WHERE lang = %s AND kind IN ('word', 'collocation') AND card IS NULL;",
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


# Поля разбора, у которых есть «сторона запроса» и «сторона ответа». Разворот меняет
# их местами парами — по одному правилу, без исключений.
_CARD_SIDE_PAIRS = (
    ("word_source", "word_target"),
    ("source_text", "target_text"),
    ("source_lang", "target_lang"),
)


def card_is_facing_away(card: dict | None, lang: str) -> bool:
    """Разбор собран НЕ на языке слова: спрашивали по-русски, а слово немецкое."""
    if not isinstance(card, dict) or not card:
        return False
    head = ""
    for name in ("word_source", "source_text"):
        value = card.get(name)
        if isinstance(value, str) and value.strip():
            head = value
            break
    if not head:
        return False
    other = card.get("word_target") or card.get("target_text") or ""
    # Разворачиваем, только когда ОБЕ стороны опознаны и они разные: одна сторона —
    # это не улика, у фразы обе половины могут быть на одном языке.
    return (not text_matches_language(head, lang)
            and text_matches_language(str(other or ""), lang))


def orient_card_to_unit_language(card: dict, lang: str) -> dict:
    """Развернуть разбор лицом к языку слова. Ничего не выдумывает — меняет местами.

    ЗАЧЕМ. Человек искал ПО-РУССКИ, разбор собрался «русский → немецкий» и лёг как есть
    на НЕМЕЦКОЕ слово. Выдача читает его как «немецкий → русский», и в карточке слева
    оказывается русский: у «die Tonne» пример выглядел «Эта машина может увезти десять
    тонн.» → «Dieses Fahrzeug kann zehn Tonnen transportieren». Замер 21.08.2026: 410
    немецких слов из 10 335 с разбором, у 224 по-русски был заголовок, у 378 — примеры.

    Стражи «немецкое поле = латиница» существуют с 22.07.2026, но стоят на карточках
    людей и на общем пуле; слой единиц появился позже и ими прикрыт не был.

    ЧТО ТЕРЯЕТСЯ, честно: список `translations` у развёрнутого разбора хранил НЕМЕЦКИЕ
    синонимы (это же был перевод русского запроса). Держать их под видом русских значений
    нельзя, поэтому остаётся один — та сторона, что теперь стала переводом. Значения
    слова на экран всё равно приходят из связей, а не отсюда (`_build_item`).
    """
    if not card_is_facing_away(card, lang):
        return card
    flipped = dict(card)
    for left, right in _CARD_SIDE_PAIRS:
        if left in flipped or right in flipped:
            flipped[left], flipped[right] = flipped.get(right), flipped.get(left)
    pair = flipped.get("language_pair")
    if isinstance(pair, dict):
        pair = dict(pair)
        pair["source_lang"], pair["target_lang"] = pair.get("target_lang"), pair.get("source_lang")
        pair["code"] = f"{pair.get('source_lang') or ''}-{pair.get('target_lang') or ''}"
        flipped["language_pair"] = pair
    native = str(flipped.get("word_target") or flipped.get("target_text") or "").strip()
    if native:
        flipped["translations"] = [{"value": native, "context": "", "is_primary": True}]
        flipped.pop("dictionary_senses", None)
    return orient_examples_to_unit_language(flipped, lang)


def orient_examples_to_unit_language(card: dict, lang: str) -> dict:
    """Примеры разбора: слева — язык слова, справа — перевод. По каждому примеру отдельно.

    ⚠ ЭТО ОТДЕЛЬНОЕ ПРАВИЛО, А НЕ ЧАСТЬ ПРЕДЫДУЩЕГО. Замер 21.08.2026: из 410 слов с
    перевёрнутым разбором у 224 развёрнут весь разбор, а у 186 заголовок правильный и
    зеркальны ТОЛЬКО примеры («Wir werden höhere Kosten haben…» с примером «Компания
    понесла большие затраты…» → «Das Unternehmen hat hohe Kosten…»). Разворачивать
    у них весь разбор было бы ошибкой — портится верный заголовок.

    Пример трогаем, только когда обе стороны опознаны и они разные: одна сторона —
    не улика.
    """
    if not isinstance(card, dict):
        return card
    examples = card.get("usage_examples")
    if not isinstance(examples, list) or not examples:
        return card
    fixed: list = []
    changed = False
    for item in examples:
        if not isinstance(item, dict):
            fixed.append(item)
            continue
        source, target = item.get("source"), item.get("target")
        if (text_matches_language(str(target or ""), lang)
                and str(source or "").strip()
                and not text_matches_language(str(source or ""), lang)):
            fixed.append({**item, "source": target, "target": source})
            changed = True
        else:
            fixed.append(item)
    if not changed:
        return card
    return {**card, "usage_examples": fixed}


# Источники, где текст разбора СОЧИНИЛА МОДЕЛЬ. Только их проверяет второй голос.
# Остальные («сохранение», «подъём из карточки», «переезд пула», «слияние дубликата»,
# «выдача по подписке», «сведение») переносят уже существующий текст — сочинять там
# нечего, и лишний платный запрос был бы просто тратой.
MODEL_INVENTED_SOURCES = frozenset({
    "обогащение",
    "пересбор",
    "пересборка после правки",
    "пересборка со смыслом человека",
    "добор синонимов",
    # Разбор, собранный ПО ОТКРЫТИЮ карточки (человек ждёт его на экране). Он ничем
    # не надёжнее ночного — та же модель, тот же промпт, — поэтому и проверяется тем
    # же вторым голосом. Скорость показа не повод пропускать проверку.
    "дозаполнение при открытии",
})


def _second_voice_disabled() -> bool:
    """Выключатель для тестов и локальной отладки, где сети нет.

    Это НЕ продуктовый дефолт: в проде переменная не ставится, и проверка обязательна.
    Без выключателя каждый тест, кладущий разбор, ходил бы в сеть и падал."""
    return str(os.getenv("SECOND_VOICE_CHECK_DISABLED") or "").strip() == "1"


def save_unit_card(unit_id: int, card: dict, *, source: str = "обогащение", cursor=None) -> bool:
    """Положить разбор НА единицу. Пишем только в слой; общий банк не трогаем.

    cursor передаёт тот, кто уже держит транзакцию (выдача слова по подписке): иначе
    из пула берётся второе соединение, пока первое не отпущено. Коммит в этом случае
    делает вызывающий — запись должна попасть в ту же транзакцию, что и карточка."""
    if not isinstance(card, dict) or not card:
        return False
    # РАЗБОР ЛОЖИТСЯ ЛИЦОМ К ЯЗЫКУ СЛОВА, всегда. Это единственная дверь, через которую
    # разбор попадает на единицу (save_unit_card_if_richer зовёт её же), поэтому правило
    # стоит здесь, а не копией у каждого пишущего.
    # ⚠ Язык спрашиваем ПЕРЕДАННЫМ курсором, если он есть: вызывающий уже держит
    # транзакцию, и второе соединение из пула здесь — известная ловушка проекта.
    try:
        if cursor is not None:
            cursor.execute("SELECT lang FROM bt_3_lex_units WHERE id = %s;", (int(unit_id),))
            _row = cursor.fetchone()
        else:
            with get_db_connection_context() as _conn:
                with _conn.cursor() as _cur:
                    _cur.execute("SELECT lang FROM bt_3_lex_units WHERE id = %s;", (int(unit_id),))
                    _row = _cur.fetchone()
        _lang = str((_row or ("",))[0] or "")
        card = orient_card_to_unit_language(card, _lang)
        card = orient_examples_to_unit_language(card, _lang)
    except Exception as exc:
        # Разворот не удался — кладём как есть: разбор важнее, а перевёрнутые ловит
        # ночная сверка. Молча ошибку не глотаем.
        logging.warning("разворот разбора для слова %s не удался: %s", unit_id, exc)
    # СТРАЖ ЦЕЛОСТНОСТИ РАЗБОРА. С 20.08.2026 заголовок слова защищён правилом в самой
    # базе, а разбор — примеры, сочетания, объяснения — не защищён ничем. Порча 16.08
    # дошла именно сюда: 15 слов и 143 карточки людей. Через эту функцию идут оба
    # ночных пути (обогащение и живое сохранение), поэтому страж стоит здесь.
    #
    # Отвергаем ЗАПИСЬ, а не чиним текст: размноженный сам на себя разбор — след
    # поломки у того, кто его прислал, и подчистить его тихо значило бы спрятать её.
    from backend.mangled_text import mangled_strings_inside
    порча = mangled_strings_inside(card)
    if порча:
        logging.warning("разбор слова %s не записан — текст размножен сам на себя: %s",
                        unit_id, " | ".join(x[:70] for x in порча[:3]))
        return False
    # ВТОРОЙ ГОЛОС НА ЗАПИСИ — там, где текст ПРИДУМАЛИ МЫ.
    #
    # Проход по всей базе 23.08.2026: из 5 073 фраз у 655 (13%) кривые примеры. Все они
    # когда-то легли сюда одинаково — модель сочинила, никто не проверил. Владелец в тот
    # же день: «может, и наполнять когда будем ночью, то будем переспрашивать две
    # модели?» Проверяет модель ДРУГОГО производителя: два голоса OpenAI обучены
    # одинаково и ошибаются одинаково (замерено: 15% пустых разногласий).
    #
    # ПОЧЕМУ ЗДЕСЬ, А НЕ В НОЧНОМ ЦИКЛЕ. Разбор ложится на слово семью путями: ночное
    # обогащение, пересбор, пересборка после правки, пересборка со смыслом человека,
    # добор синонимов, подъём из карточки, выдача по подписке. Врезка в один вызов
    # починила бы один случай, а через год уборка вернулась бы с другой стороны.
    #
    # ПРОВЕРЯЕМ НЕ ВСЁ. Только источники, где текст сочинён моделью. Живое сохранение
    # человеком и переносы готового текста («подъём из карточки», «переезд пула»,
    # «слияние дубликата») модель не сочиняла — проверять там нечего.
    #
    # ⚠ И НИКОГДА ВНУТРИ ЧУЖОЙ ТРАНЗАКЦИИ. Запрос к модели держится секундами, а cursor
    # означает, что вызывающий уже занял соединение пула. Единственный такой путь —
    # «выдача по подписке» — ничего не сочиняет, поэтому в список и не входит.
    if source in MODEL_INVENTED_SOURCES and not _second_voice_disabled():
        if cursor is not None:
            logging.warning(
                "разбор слова %s (%s) пришёл внутри чужой транзакции — второй голос "
                "спросить нельзя, запись отклонена", unit_id, source)
            return False
        from backend.second_voice_check import review_new_card
        cur_display = ""
        try:
            with get_db_connection_context() as _c:
                with _c.cursor() as _cu:
                    _cu.execute("SELECT display, kind FROM bt_3_lex_units WHERE id=%s;",
                                (int(unit_id),))
                    _r = _cu.fetchone() or ("", "word")
                    cur_display, _kind = str(_r[0] or ""), str(_r[1] or "word")
        except Exception as exc:
            logging.warning("второй голос: не смог прочитать слово %s: %s", unit_id, exc)
            return False
        review = review_new_card(headword=cur_display, card=card, kind=_kind)
        if not review.get("checked"):
            # «Не проверено» — это НЕ «хорошо». Слово остаётся кандидатом и вернётся
            # следующей ночью: непроверенный выдуманный текст в базу не идёт.
            logging.warning("разбор слова %s не записан — второй голос не ответил (%s)",
                            unit_id, review.get("why"))
            return False
        if not review.get("ok"):
            logging.info("разбор слова %s забракован вторым голосом (%s): %s",
                         unit_id, ", ".join(review.get("fields") or []), review.get("why"))
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


def _gender_from_card(card: dict | None, *, word: str = "") -> str:
    """Род из разбора — но НЕ вперёд справочника, если справочник уже под рукой.

    ⚠ ДВЕРЬ ДЛЯ РОДА. Разбор бывает собран про МНОЖЕСТВЕННОЕ число («die Spritpreise»),
    и его «die» уезжало на единственное — на экране выходило «die Spritpreis», «die Narr»,
    «die Elektrogerät». Замер 21.08.2026: 12 таких слов из 691.

    Ночная сверка (`fix_gender_conflicts_from_authority`) это чинит, но только к утру, а
    до утра человек читает неверный род. Поэтому здесь стоит бесплатная половина той же
    проверки: спрашиваем арбитра ТОЛЬКО из прогретой памяти (`article_if_already_loaded`)
    — ни в базу, ни в сеть, потому что это живой путь сохранения и чужая транзакция.

    Знает арбитр и не согласен — род из разбора НЕ берём: пусть слово побудет без
    артикля до ночи. Пустая ячейка — незакрытая задача, а неверный род — ложь, которую
    человек заучит. Не знает или согласен — ведём себя как раньше.
    """
    if not isinstance(card, dict):
        return ""
    gender = _ARTICLE_TO_GENDER.get(str(card.get("article") or "").strip().lower(), "")
    if not gender or not str(word or "").strip():
        return gender
    try:
        from backend.article_authority import article_if_already_loaded
        verdict = article_if_already_loaded(normalize_query(word))
    except Exception:
        logging.debug("справочник родов не отозвался — берём род из разбора", exc_info=True)
        return gender
    if verdict and verdict != gender:
        logging.info("род из разбора (%s) разошёлся со справочником (%s) у %r — не берём",
                     gender, verdict, str(word)[:60])
        return ""
    return gender


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
     WHERE id = %(id)s AND (pos IS NULL OR gender IS NULL) AND kind = 'word'
       -- Существительное обязано начинаться с заглавной. Но артикль, если он хранится
       -- ВНУТРИ заголовка («der Simulator»), эту заглавную прячет, и условие отвергало
       -- законные слова: замер 25.08.2026 — 4 слова висели с пустым родом с 23.08,
       -- механизм их видел и каждый раз отказывался брать.
       AND (%(pos)s <> 'noun'
            OR lemma ~ '^[A-ZÄÖÜ]'
            OR lemma ~* '^(der|die|das)[[:space:]]+[A-ZÄÖÜ]')
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
    # Написание слова берём у САМОГО разбора: единицу здесь ещё не читали, а лишний
    # запрос на живом пути не нужен. Разбор описывает то самое слово, на которое ложится.
    gender = _gender_from_card(card, word=_card_headword(card))
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
    gender = _gender_from_card(card, word=(lemma or _card_headword(card)))
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


# ┌─ ПРОВЕРЕНО 22.08.2026. НЕ ПОДНИМАТЬ ЭТО КАК НОВУЮ НАХОДКУ. ─────────────────┐
# │                                                                             │
# │ Запрос «у скольких слов заголовок разбора не совпадает с самим словом»      │
# │ даёт 788. ЭТО НЕ ЧИСЛО ДЕФЕКТОВ. Разбор:                                    │
# │                                                                             │
# │     655  различие ТОЛЬКО в артикле — «Überalterung» / «die Überalterung».   │
# │          Так и задумано: заголовок единицы без артикля, разбор с артиклем.  │
# │          ДЕФЕКТА НЕТ. Сравнивать эти два поля в лоб — значит каждый раз     │
# │          находить 655 «ошибок», которых не существует.                      │
# │       8  дореформенное ß/ss («der Schlußverkauf» / «der Schlussverkauf»)    │
# │       6  перенос строки или пробел перед знаком                             │
# │      13  концевой знак в разборе («…werden» / «…werden;»)                   │
# │      25  слово в разборе ОБРЕЗАНО («die Umschaltsituationen» / «…situatio») │
# │      41  одно содержит другое, форма против леммы («Felg» / «die Felge»)    │
# │      40  разный текст, смотреть глазами («verraten» / «verraen»)            │
# │                                                                             │
# │ НАСТОЯЩИХ КАНДИДАТОВ ~78 (обрезки + концевой знак + разный текст), а не     │
# │ 788. Решение по ним владелец ещё не принимал — вслепую разворачивать        │
# │ правило нельзя, часть «разного текста» верна.                               │
# │                                                                             │
# │ Перемерить: сравнивать ПОСЛЕ снятия артикля с обеих сторон.                 │
# └─────────────────────────────────────────────────────────────────────────────┘
def _card_headword(card: dict | None) -> str:
    """Немецкий заголовок САМОГО разбора: про какое написание он собран."""
    if not isinstance(card, dict):
        return ""
    for name in ("word_de", "word_source", "source_text"):
        value = card.get(name)
        if isinstance(value, str) and value.strip():
            return value
    return ""


def fix_gender_conflicts_from_authority(*, limit: int = 400, lang: str = "de",
                                        dry_run: bool = False) -> dict:
    """Род лежит колонкой и врёт на экране — найти и починить, пока ночь.

    ОТКУДА ЛОЖЬ. Артикль в написании есть не у всех слов: у части он лежит колонкой
    `gender`, и выдача приклеивает его к заголовку сама (`_build_item`). Колонку
    заполняют из разбора, а разбор нередко собран про МНОЖЕСТВЕННОЕ число: у слова
    «Spritpreis» разбор озаглавлен «die Spritpreise», и его «die» уезжает на
    единственное. У множественного артикль всегда die, родом слова он не является.
    На экране получалось «die Spritpreis», «die Narr», «die Elektrogerät».

    ЧТО ЧИНИМ САМИ, А ЧТО НЕТ. Молча правится ТОЛЬКО класс с известной причиной:
    род взят из разбора, который собран про ДРУГОЕ написание, и арбитр рода называет
    другой род. Совпало написание — значит разбор про это самое слово, и его артикль
    мы не перебиваем: там род зависит от значения («der Dicke» толстяк / «die Dicke»
    толщина), а такое решает владелец, а не правило. Такие расхождения считаются и
    уходят в ночной отчёт числом, а не чинятся догадкой.

    Кто решает род — `article_authority.authoritative_article`: Wiktionary, банк
    артиклей, правило композита по 19 тысячам родов и честное «не знаю». Молчание
    арбитра уликой не считается. Мы не выводим род сами ни в одной ветке.

    Замер 21.08.2026: 691 слово с родом-колонкой, арбитр подтвердил 534, промолчал про
    145, возразил по 12; из этих 12 фингерпринт «разбор про другое написание» стоял у 5,
    остальные разобраны руками (scripts/dict_fix_gender_column_conflicts.py).
    """
    from backend.article_authority import authoritative_article

    report = {"checked": 0, "fixed": 0, "doubts": 0, "samples": [], "doubt_samples": []}
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, display, lemma_key, gender, card
                      FROM bt_3_lex_units
                     WHERE lang = %s AND pos = 'noun'
                       AND gender IN ('der', 'die', 'das')
                       AND display !~* '^(der|die|das)\\s'
                       AND card IS NOT NULL
                     ORDER BY id
                     LIMIT %s;
                    """,
                    (str(lang or "de").strip().lower() or "de", int(limit)),
                )
                rows = cur.fetchall()

                for unit_id, display, lemma_key, gender, card in rows:
                    report["checked"] += 1
                    bare = normalize_query(str(display or ""))
                    verdict, source = authoritative_article(bare)
                    if not verdict or verdict == gender:
                        continue
                    # Разбор про ЭТО ЖЕ написание — его артикль не перебиваем.
                    card_word = normalize_query(_card_headword(card))
                    if card_word == str(lemma_key or ""):
                        report["doubts"] += 1
                        if len(report["doubt_samples"]) < 15:
                            report["doubt_samples"].append(
                                {"unit_id": unit_id, "word": display,
                                 "stored": gender, "authority": verdict, "source": source})
                        continue
                    if len(report["samples"]) < 15:
                        report["samples"].append(
                            {"unit_id": unit_id, "word": display, "was": gender,
                             "became": verdict, "source": source,
                             "card_about": _card_headword(card)})
                    if dry_run:
                        continue
                    cur.execute(
                        "UPDATE bt_3_lex_units SET gender = %s, gender_source = %s, "
                        "updated_at = NOW() WHERE id = %s;",
                        (verdict, "арбитр рода", int(unit_id)),
                    )
                    report["fixed"] += 1
            if not dry_run:
                conn.commit()
    except Exception as exc:
        logging.warning("сверка рода с арбитром не удалась: %s", exc, exc_info=True)
        return report
    if report["fixed"]:
        logging.info("род поправлен по арбитру у %d слов", report["fixed"])
    if report["doubts"]:
        logging.info("род расходится с арбитром, но разбор про то же слово: %d — владельцу",
                     report["doubts"])
    return report


def backfill_links_from_cards(*, limit: int = 300, lang: str = "de",
                              native_lang: str = "ru", dry_run: bool = False) -> dict:
    """Развесить связи «немецкое слово ↔ русское» тем словам, у кого их нет.

    ЗАЧЕМ. `sync_unit_links_from_card` вызывается ПОШТУЧНО, в момент сохранения разбора.
    Но разбор появляется и другими путями — ночным добором, переносом из пула, правкой
    заголовка, — и на этих путях связи никто не развешивает. Слово получает переводы
    внутри разбора и остаётся без связей навсегда.

    Пакетного прохода не существовало вовсе. Поэтому такие слова копились: замер
    25.08.2026 — 67 из 10 437, и число росло на глазах (утром было 41). Каждое из них
    я потом «находил» руками по два-три и нёс владельцу. Отсюда и ощущение бесконечности:
    выгребали ковшом кучу, которую никто не разгребает.

    ЧТО ЛОМАЕТСЯ БЕЗ СВЯЗИ. Перевод человек видит — он лежит внутри разбора. Но поиск по
    РУССКОЙ стороне слово не находит, и подбор пар для тренировок его не берёт.

    Своих переводов НЕ ВЫДУМЫВАЕТ: берёт то, что уже написано в разборе, тем же кодом,
    что и обычное сохранение. Слово без разбора не трогает вовсе.
    """
    report = {"candidates": 0, "linked": 0, "skipped": 0, "samples": []}
    try:
        with get_db_connection_context() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.id, u.display, u.card
                      FROM bt_3_lex_units u
                     WHERE u.lang = %s
                       AND jsonb_typeof(u.card->'translations') = 'array'
                       AND jsonb_array_length(u.card->'translations') > 0
                       AND NOT EXISTS (
                             SELECT 1 FROM bt_3_lex_links l
                               JOIN bt_3_lex_units t
                                 ON t.id = CASE WHEN l.from_unit = u.id
                                                THEN l.to_unit ELSE l.from_unit END
                              WHERE (l.from_unit = u.id OR l.to_unit = u.id)
                                AND t.lang = %s AND l.rank < 900)
                     ORDER BY u.updated_at DESC NULLS LAST
                     LIMIT %s;
                    """,
                    (lang, native_lang, int(limit)),
                )
                строки = cur.fetchall() or []
    except Exception:
        logging.warning("добор связей: не прочитал список", exc_info=True)
        return report

    report["candidates"] = len(строки)
    if dry_run:
        report["samples"] = [str(d) for _i, d, _c in строки[:10]]
        return report

    for unit_id, display, card in строки:
        if not isinstance(card, dict):
            report["skipped"] += 1
            continue
        try:
            итог = sync_unit_links_from_card(int(unit_id), card, native_lang=native_lang)
        except Exception:
            # Не глушим молча: слово остаётся без связей и попадёт в следующий проход.
            logging.warning("добор связей: %r не развесил", display, exc_info=True)
            report["skipped"] += 1
            continue
        if (итог or {}).get("links"):
            report["linked"] += 1
            if len(report["samples"]) < 10:
                report["samples"].append(str(display))
        else:
            report["skipped"] += 1
    return report


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
                     WHERE lang = %s AND kind = 'word'
                       AND (pos IS NULL OR gender IS NULL)
                       AND COALESCE(card->>'article', '') IN ('der', 'die', 'das')
                       -- ⚠ АРТИКЛЬ БЫВАЕТ ВНУТРИ ЗАГОЛОВКА, и тогда слово начинается со
                       -- СТРОЧНОЙ: «der Simulator», «die Airline», «das Cockpit». Условие
                       -- «заголовок с заглавной» их не видело, и они висели с пустым родом
                       -- с 23.08.2026 — механизм был, но проходил мимо (замер 25.08.2026:
                       -- 4 слова). Хранение артикля в заголовке — устройство нашей базы,
                       -- а не дефект: 655 записей отличаются от карточки только артиклем.
                       AND (lemma ~ '^[A-ZÄÖÜ]' OR lemma ~* '^(der|die|das)[[:space:]]+[A-ZÄÖÜ]')
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
