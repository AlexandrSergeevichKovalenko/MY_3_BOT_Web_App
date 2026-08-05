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
from backend.lex_senses import split_translation

_SPACE_RE = re.compile(r"\s+")
_ARTICLE_RE = re.compile(r"^(der|die|das)\s+", re.I)
# В тексте слово стоит с ПАДЕЖНЫМ артиклем: «den Rüpeln», «des Helden», «einem Kind».
# Для поиска снимаем любой из них; для определения рода годится только именительный
# (см. article_of): «den» бывает и мужским винительным, и множественным дательным.
_ANY_ARTICLE_RE = re.compile(
    r"^(?:der|die|das|den|dem|des|ein|eine|einen|einem|einer|eines)\s+", re.I)

# Сколько переводов показывать: первый — главный, остальные как «ещё говорят».
_MAX_LINKS = 6

# Ранг, в который отправлены «свалки» — старые переводы вида «1прикладывать; накладывать
# 2 надевать 3 строить». Они разрезаны на отдельные значения и в базе остались, но
# показывать их нельзя: человек должен видеть значения, а не строку из словаря.
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
        -- Заготовки упражнений отсекаем В ЗАПРОСЕ, а не после: у «anlegen» их 33 штуки,
        -- и при отборе «первых шести» они съедали выдачу целиком — слово оставалось
        -- вообще без перевода. Связи с разобранным значением идут первыми.
        WHERE l.from_unit = %s AND u.lang = %s AND l.rank < %s
          AND position('___' in u.display) = 0
        ORDER BY (l.sense_id IS NULL), l.rank, u.id
        LIMIT %s;
        """,
        (unit_id, want_lang, _DEMOTED_RANK, _MAX_LINKS),
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
        seen_values.add(key)
        shown.append(link)
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


def ensure_unit(text: str, lang: str) -> int | None:
    """Найти единицу по написанию, а если её нет — завести.

    Нужно на сохранении: слово, которое человек только что положил себе в словарь,
    обязано сразу иметь дом в слое. Иначе указатель у карточки остаётся пустым, и
    разрыв растёт с каждым новым сохранением."""
    key = normalize_query(text)
    kind = _kind_for_text(text)
    if not key or not kind or not lang:
        return None
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
    if not values:
        for item in (card.get("translations") or []):
            value = item.get("value") if isinstance(item, dict) else item
            if isinstance(value, str) and value.strip():
                values.append({"value": value.strip(), "note": ""})
    # Разбор тоже бывает склеен: «ромб (геометрическая фигура); решётка (символ #)» —
    # это два значения в одной строке. Прогоняем через общий разрезатель, иначе свалка
    # вернулась бы с другой стороны. Длинные определения переводом не считаем и кладём
    # в пояснение к значению: «направление, к которому движутся» — это не перевод.
    unique: list[dict] = []
    seen: set[str] = set()
    for item in values:
        for part in split_translation(item["value"]):
            value = part["value"].strip()
            if not value:
                continue
            note = "; ".join(x for x in (part.get("label"), item.get("note")) if x)
            if len(value) > 60:
                if unique:
                    unique[-1]["note"] = "; ".join(x for x in (unique[-1].get("note"), value) if x)[:500]
                continue
            key = normalize_query(value)
            if key and key not in seen:
                seen.add(key)
                unique.append({"value": value, "note": note})
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
                    kind = "word" if " " not in value else (
                        "sentence" if len(value.split()) > 4 else "collocation")
                    cur.execute(
                        """
                        INSERT INTO bt_3_lex_units (lang, kind, lemma, lemma_key, display, card_source)
                        VALUES (%s, %s, %s, %s, %s, 'разбор')
                        ON CONFLICT (lang, kind, lemma_key, COALESCE(pos, ''), COALESCE(gender, ''))
                        DO UPDATE SET updated_at = NOW()
                        RETURNING id;
                        """,
                        (native_lang, kind, value, normalize_query(value), value),
                    )
                    target_id = cur.fetchone()[0]
                    cur.execute(
                        """
                        INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                        VALUES (%s, %s, %s, 'exact') ON CONFLICT DO NOTHING;
                        """,
                        (native_lang, normalize_query(value), target_id),
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


def save_unit_card(unit_id: int, card: dict, *, source: str = "обогащение") -> bool:
    """Положить разбор НА единицу. Пишем только в слой; общий банк не трогаем."""
    if not isinstance(card, dict) or not card:
        return False
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE bt_3_lex_units SET card = %s::jsonb, card_source = %s, updated_at = NOW() "
                    "WHERE id = %s;",
                    (json.dumps(card, ensure_ascii=False), source, int(unit_id)),
                )
            conn.commit()
        return True
    except Exception as exc:
        logging.debug("save unit card failed for %s: %s", unit_id, exc)
        return False


# Оценка полноты разбора (CARD_CONTENT_KEYS / card_content_score) живёт в слое БД:
# ею пользуются и запись на единицу, и отдача карточки человеку, а импортировать слой БД
# отсюда обратно нельзя — вышло бы кольцо.
def save_unit_card_if_richer(unit_id: int, card: dict, *, source: str = "сохранение") -> bool:
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
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT card FROM bt_3_lex_units WHERE id = %s;", (int(unit_id),))
                row = cur.fetchone()
        if row is None:
            return False
        if fresh <= card_content_score(row[0] if isinstance(row[0], dict) else None):
            return False
    except Exception as exc:
        logging.debug("compare unit card failed for %s: %s", unit_id, exc)
        return False
    return save_unit_card(int(unit_id), card, source=source)


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
    gender = _gender_from_card(card)
    if not gender or not unit_id:
        return False
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE bt_3_lex_units
                       SET pos = 'noun',
                           gender = COALESCE(NULLIF(gender, ''), %s),
                           pos_source = COALESCE(pos_source, 'card'),
                           gender_source = COALESCE(gender_source, 'card'),
                           updated_at = NOW()
                     WHERE id = %s AND pos IS NULL AND lemma ~ '^[A-ZÄÖÜ]'
                       AND NOT EXISTS (
                           SELECT 1 FROM bt_3_lex_units o
                            WHERE o.lang = bt_3_lex_units.lang
                              AND o.kind = bt_3_lex_units.kind
                              AND o.lemma_key = bt_3_lex_units.lemma_key
                              AND o.pos = 'noun'
                              AND COALESCE(o.gender, '') = %s
                              AND o.id <> bt_3_lex_units.id
                       );
                    """,
                    (gender, int(unit_id), gender),
                )
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
