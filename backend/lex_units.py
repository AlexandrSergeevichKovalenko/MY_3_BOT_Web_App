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

import logging
import re
from typing import Any

from backend.database import get_db_connection_context

_SPACE_RE = re.compile(r"\s+")
_ARTICLE_RE = re.compile(r"^(der|die|das)\s+", re.I)

# Сколько переводов показывать: первый — главный, остальные как «ещё говорят».
_MAX_LINKS = 6

# Служебные пометки, осевшие в банке под видом переводов: «приюта; хостела
# (Genitiv/Dativ)». Человеку это не перевод, а мусор — в списке значений не показываем.
# Из базы ничего не удаляем: фильтр только на выдаче.
_GRAMMAR_NOTE_RE = re.compile(
    r"\((?:[^)]*\b(?:genitiv|dativ|akkusativ|nominativ|plural|singular|мн\.?\s*ч|ед\.?\s*ч)\b[^)]*)\)",
    re.I,
)


def normalize_query(text: str) -> str:
    """Ключ поиска: без лишних пробелов, без артикля, в нижнем регистре.

    Артикль снимаем именно здесь, а не в опознании единицы: «der Kiefer» и «Kiefer» —
    одно написание, а вот РАЗНЫЕ единицы за ним стоят разные, и выбирает их вызывающая
    сторона по артиклю запроса."""
    compact = _SPACE_RE.sub(" ", str(text or "").strip())
    return _ARTICLE_RE.sub("", compact).strip().casefold()


def article_of(text: str) -> str:
    m = _ARTICLE_RE.match(str(text or "").strip())
    return m.group(1).lower() if m else ""


def _fetch_units(cur, *, lang: str, surface_key: str) -> list[dict]:
    cur.execute(
        """
        SELECT u.id, u.lang, u.kind, u.lemma, u.lemma_key, u.pos, u.gender, u.display, u.card
        FROM bt_3_lex_surfaces s
        JOIN bt_3_lex_units u ON u.id = s.unit_id
        WHERE s.lang = %s AND s.surface_key = %s;
        """,
        (lang, surface_key),
    )
    return [
        {"id": r[0], "lang": r[1], "kind": r[2], "lemma": r[3], "lemma_key": r[4],
         "pos": r[5], "gender": r[6], "display": r[7], "card": r[8] if isinstance(r[8], dict) else None}
        for r in cur.fetchall()
    ]


def _fetch_links(cur, unit_id: int, *, want_lang: str) -> list[dict]:
    cur.execute(
        """
        SELECT u.id, u.lang, u.kind, u.display, u.lemma, u.pos, u.gender, l.rank, u.card
        FROM bt_3_lex_links l
        JOIN bt_3_lex_units u ON u.id = l.to_unit
        WHERE l.from_unit = %s AND u.lang = %s
        ORDER BY l.rank, u.id
        LIMIT %s;
        """,
        (unit_id, want_lang, _MAX_LINKS),
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


def _build_item(unit: dict, links: list[dict], *, source_lang: str, target_lang: str) -> dict:
    """Карточка в том виде, какой ждёт фронт.

    За основу берётся разбор, лежащий НА единице (он про неё и ни про кого больше), а
    заголовок, артикль и переводы ставятся из самой единицы и её связей — чтобы данные
    на экране всегда были про одно и то же слово."""
    card = dict(unit.get("card") or {})
    de_side = unit if unit["lang"] == "de" else (links[0] if links else None)
    ru_side = links[0] if unit["lang"] == "de" else unit

    german_display = (de_side or {}).get("display") or ""
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
    shown = [link for link in links if not _GRAMMAR_NOTE_RE.search(link["display"])] or links[:1]
    if shown:
        item["translations"] = [
            {"value": link["display"], "context": "", "is_primary": index == 0}
            for index, link in enumerate(shown)
        ]
        item["dictionary_senses"] = [
            {"rank": index + 1, "label": "main" if index == 0 else "secondary",
             "value": link["display"], "context": "", "example_source": "", "example_target": ""}
            for index, link in enumerate(shown)
        ]
    item["__lex_unit_id"] = unit["id"]
    # Отдельная пометка «у единицы есть НАСТОЯЩИЙ разбор». Без неё карточка со списком
    # переводов, но без форм и примеров, считалась бы полной (так устроена общая проверка
    # полноты) и уехала бы человеку голой, минуя дообогащение.
    item["__lex_has_card"] = bool(unit.get("card"))
    return item


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
                return _build_item(unit, links, source_lang=query_lang, target_lang=other_lang)
    except Exception as exc:
        logging.debug("lex units lookup failed for %r: %s", word, exc)
        return None
