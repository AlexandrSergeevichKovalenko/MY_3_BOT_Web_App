# -*- coding: utf-8 -*-
"""ОДНА проверка целостности словаря. Все правила разом, одно число владельцу.

ЗАЧЕМ ОНА СУЩЕСТВУЕТ. Владелец 22.08.2026:

    «Как можно постоянно что-то проверять, потом ты опять находишь какие-то 264
    карточки? Есть какой-то предел? Мы же работаем на будущее, чтобы такого не было —
    то есть мы должны строить стражей. Эта работа закончится когда-то или нет?»

Он прав, и вот в чём была ошибка. За один день я написал десяток РАЗОВЫХ скриптов: под
артикль, под род, под перевёрнутый разбор, под школьный хвост, под язык ответа. Каждый
находил свой класс, чинил его и уходил. Классы при этом находил ЧЕЛОВЕК ПО СКРИНШОТУ, а
скрипт только считал — поэтому за каждым экраном открывался следующий, и конца не было
видно по построению.

Здесь они сведены в одно. Проверка гоняется ночью и отвечает ОДНИМ ЧИСЛОМ: сколько
записей нарушают хоть одно правило. Ноль — словарь чист, и это проверяемо, а не «я
посмотрел». Не ноль — класс назвала машина, до того как владелец увидел его на экране.

ЧТО СЧИТАЕТСЯ ПРАВИЛОМ. Только то, что уже закрыто стражем в коде. Проверка сторожит
СТРАЖЕЙ: если правило перестало работать или кто-то завёл новый путь в обход, число
перестанет быть нулём. Правило без стража сюда не добавляется — иначе это снова список
работ, а не проверка.

    python3 -c "from backend.dictionary_integrity import run; print(run()['total'])"
"""
from __future__ import annotations

import logging
import re
from typing import Any, Callable

from backend.database import get_db_connection_context

_CYRILLIC = re.compile(r"[А-Яа-яЁё]")
_LATIN = re.compile(r"[A-Za-zÄÖÜäöüß]")
_LEADING_ARTICLE = re.compile(r"^(?:der|die|das)\s+", re.I)
# Школьная запись множественного числа в конце заголовка: «die Brücke, -n».
_SCHOOL_TAIL = re.compile(r",\s*[-–—]\s*(?:e|en|n|s|er|se|nen)?\s*$", re.I)
# Немецкое снаружи скобки, русское внутри: «abheben (снять трубку)».
_BRACKET = re.compile(r"\s*\([^)]*\)")


def _bare(value: str) -> str:
    return _LEADING_ARTICLE.sub("", str(value or "").strip()).strip()


def _is_multiword(value: str) -> bool:
    return len(_bare(value).split()) > 1


def _german_with_russian_hint(value) -> bool:
    text = str(value or "")
    if "(" not in text:
        return False
    outside = _BRACKET.sub(" ", text)
    inside = " ".join(re.findall(r"\(([^)]*)\)", text))
    return (bool(_LATIN.search(outside)) and not _CYRILLIC.search(outside)
            and bool(_CYRILLIC.search(inside)))


# ── ПРАВИЛА ──────────────────────────────────────────────────────────────────────
#
# У каждого: имя для человека, запрос-счётчик и КОММЕНТАРИЙ, где стоит страж. Правило
# без стража здесь не живёт.

def _rule_phrase_is_not_a_noun(cur) -> tuple[int, list]:
    """Фраза помечена существительным — отсюда артикль над многословным заголовком.
    Страж: backend_server._apply_german_headword_normalization."""
    cur.execute("""SELECT id, display FROM bt_3_lex_units
                    WHERE lang='de' AND card->>'part_of_speech' = 'noun';""")
    hits = [(uid, disp) for uid, disp in cur.fetchall() if _is_multiword(disp)]
    return len(hits), hits[:5]


def _rule_gender_on_multiword(cur) -> tuple[int, list]:
    """Род у многословного: выдача приклеит из него артикль.
    Страж: lex_units._adopt_pos_gender_inline (kind='word') и _gender_from_card."""
    cur.execute("""SELECT id, display FROM bt_3_lex_units
                    WHERE lang='de' AND gender IS NOT NULL AND pos='noun';""")
    hits = [(uid, disp) for uid, disp in cur.fetchall() if _is_multiword(disp)]
    return len(hits), hits[:5]


def _rule_school_tail(cur) -> tuple[int, list]:
    """Школьный хвост «, -n» в заголовке. Страж: dictionary_intake.clean_text."""
    cur.execute(r"""SELECT id, display FROM bt_3_lex_units
                     WHERE lang='de' AND display ~ ',\s*[-–—]\s*(e|en|n|s|er|se|nen)?\s*$';""")
    hits = cur.fetchall()
    return len(hits), hits[:5]


def _rule_russian_hint_inside_german(cur) -> tuple[int, list]:
    """Русская подсказка внутри немецкого слова. Страж: dict_strip_russian_hint_from_german
    закрыл накопленное; новое приходит только руками, поэтому сторожим числом."""
    cur.execute("SELECT id, display FROM bt_3_lex_units WHERE lang='de' AND display LIKE '%(%';")
    hits = [(uid, disp) for uid, disp in cur.fetchall() if _german_with_russian_hint(disp)]
    return len(hits), hits[:5]


def _rule_word_not_findable(cur) -> tuple[int, list]:
    """Слово не находится по своему же написанию. Страж: lex_units.retitle_unit."""
    cur.execute("""SELECT u.id, u.display FROM bt_3_lex_units u
                    WHERE u.lemma_key IS NOT NULL AND u.lemma_key <> ''
                      AND NOT EXISTS (SELECT 1 FROM bt_3_lex_surfaces s
                                       WHERE s.lang=u.lang AND s.unit_id=u.id
                                         AND s.surface_key=u.lemma_key)
                    LIMIT 200;""")
    hits = cur.fetchall()
    return len(hits), hits[:5]


def _rule_invisible_to_night(cur) -> tuple[int, list]:
    """Слово числится оборотом, а по написанию это слово, и разбора нет: ночь его не
    возьмёт НИКОГДА. Страж: lex_units.retitle_unit пересчитывает вид записи."""
    cur.execute("""SELECT id, display FROM bt_3_lex_units
                    WHERE lang='de' AND kind <> 'word' AND card IS NULL;""")
    hits = [(uid, disp) for uid, disp in cur.fetchall() if not _is_multiword(disp)]
    return len(hits), hits[:5]


def _rule_card_faces_away(cur) -> tuple[int, list]:
    """Разбор лежит лицом не в ту сторону. Страж: lex_units.save_unit_card."""
    from backend.lex_units import card_is_facing_away, orient_examples_to_unit_language
    hits: list = []
    last_id = 0
    while True:
        cur.execute("""SELECT id, lang, display, card FROM bt_3_lex_units
                        WHERE card IS NOT NULL AND id > %s ORDER BY id LIMIT 400;""",
                    (last_id,))
        batch = cur.fetchall()
        if not batch:
            break
        last_id = batch[-1][0]
        for uid, lang, display, card in batch:
            if (card_is_facing_away(card, lang)
                    or orient_examples_to_unit_language(card, lang) is not card):
                hits.append((uid, display))
    return len(hits), hits[:5]


def _rule_pool_answers_wrong_language(cur) -> tuple[int, list]:
    """Пул отвечает не на том языке или отдаёт заготовку задания.
    Стражи: _upsert_dictionary_canonical_entry_with_cursor и get_pool_dictionary_candidates."""
    from backend.dictionary_intake import answer_language_is_wrong, is_exercise_blank
    cur.execute("""SELECT id, source_text, target_text, target_lang
                     FROM bt_3_dictionary_entries;""")
    hits = [(eid, src) for eid, src, tgt, tl in cur.fetchall()
            if answer_language_is_wrong(tgt, tl) or is_exercise_blank(src) or is_exercise_blank(tgt)]
    return len(hits), hits[:5]


def _rule_dangling_card_pointer(cur) -> tuple[int, list]:
    """Карточка человека указывает на строку пула, которой больше нет."""
    cur.execute("""SELECT q.id, q.word_de FROM bt_3_webapp_dictionary_queries q
                    WHERE q.canonical_entry_id IS NOT NULL
                      AND NOT EXISTS (SELECT 1 FROM bt_3_dictionary_entries e
                                       WHERE e.id = q.canonical_entry_id)
                    LIMIT 200;""")
    hits = cur.fetchall()
    return len(hits), hits[:5]


def _rule_russian_in_german_field(cur) -> tuple[int, list]:
    """Русский текст в немецком поле карточки человека."""
    cur.execute("""SELECT id, word_de FROM bt_3_webapp_dictionary_queries
                    WHERE translation_de ~ '[А-Яа-яЁё]' AND word_de !~ '[А-Яа-яЁё]'
                    LIMIT 200;""")
    hits = cur.fetchall()
    return len(hits), hits[:5]


def _rule_owner_choice_not_first(cur) -> tuple[int, list]:
    """Перевод, выбранный владельцем руками, не стоит первым.
    Страж: lex_units._fetch_links ставит его поперёк машинной сортировки."""
    cur.execute("""SELECT o.from_unit, f.display
                     FROM bt_3_lex_links o JOIN bt_3_lex_units f ON f.id = o.from_unit
                    WHERE o.source = 'вычитка' AND o.rank = 1
                      AND EXISTS (SELECT 1 FROM bt_3_lex_links m
                                   WHERE m.from_unit = o.from_unit AND m.rank < o.rank)
                    LIMIT 200;""")
    hits = cur.fetchall()
    return len(hits), hits[:5]


def _rule_write_without_a_door(cur) -> tuple[int, list]:
    """Появился путь, который пишет немецкий текст в базу МИМО двери.

    Единственное правило здесь, которое смотрит не в данные, а В КОД, — и это ровно то,
    ради чего заведена вся проверка: она сторожит СТРАЖЕЙ. Данные сегодня чистые именно
    потому, что на каждом пути записи стоит дверь; уберут дверь или заведут новый путь
    в обход — грязь начнёт копиться заново, и увидим мы её не раньше, чем на экране у
    человека. Это правило показывает такое в то же утро.

    Список путей и признак двери — в backend/write_doors.py, там же написано, почему
    дверей два вида. Курсор не нужен: в базу правило не ходит.
    """
    from backend.write_doors import places_without_a_door
    open_places = places_without_a_door()
    sample = [(item["number"], item["human"], item["state"]) for item in open_places[:5]]
    return len(open_places), sample


RULES: tuple[tuple[str, Callable], ...] = (
    ("фраза помечена существительным", _rule_phrase_is_not_a_noun),
    ("род повешен на многословное", _rule_gender_on_multiword),
    ("школьный хвост «, -n» в заголовке", _rule_school_tail),
    ("русская подсказка внутри немецкого", _rule_russian_hint_inside_german),
    ("слово не находится по своему написанию", _rule_word_not_findable),
    ("слово невидимо для ночного добора", _rule_invisible_to_night),
    ("разбор лежит лицом не в ту сторону", _rule_card_faces_away),
    ("пул отвечает не на том языке", _rule_pool_answers_wrong_language),
    ("карточка указывает в никуда", _rule_dangling_card_pointer),
    ("русский текст в немецком поле", _rule_russian_in_german_field),
    ("выбор владельца не первый", _rule_owner_choice_not_first),
    ("немецкий текст пишется мимо двери", _rule_write_without_a_door),
)


def run() -> dict[str, Any]:
    """Прогнать все правила. Возвращает {'total': N, 'broken': {...}, 'samples': {...}}.

    Правило, которое упало, НЕ считается пройденным: оно попадает в `failed` и видно
    отдельно. Молча пропустить проверку — то же самое, что соврать «чисто»."""
    broken: dict[str, int] = {}
    samples: dict[str, list] = {}
    failed: dict[str, str] = {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for title, rule in RULES:
                try:
                    count, sample = rule(cur)
                except Exception as exc:
                    failed[title] = str(exc)[:200]
                    logging.warning("проверка целостности словаря: правило %r упало: %s",
                                    title, exc)
                    continue
                broken[title] = int(count)
                if sample:
                    samples[title] = sample
    total = sum(broken.values())
    return {"total": total, "broken": broken, "samples": samples, "failed": failed}


def report_lines() -> list[str]:
    """Человеческий отчёт: одно число сверху, разбивка ниже, и только по ненулевым."""
    result = run()
    lines = [f"Целостность словаря: {result['total']} нарушений"]
    for title, count in sorted(result["broken"].items(), key=lambda kv: -kv[1]):
        if count:
            lines.append(f"  • {title}: {count}")
    for title, why in result["failed"].items():
        lines.append(f"  ⚠️ правило не отработало — {title}: {why}")
    if result["total"] == 0 and not result["failed"]:
        lines.append("  всё чисто")
    return lines
