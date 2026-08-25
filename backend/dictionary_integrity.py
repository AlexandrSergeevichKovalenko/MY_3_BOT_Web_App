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
def _rule_verb_without_a_conjugation_source(cur) -> tuple[int, list]:
    """Глагол, у которого таблицу спряжения строить НЕ ИЗ ЧЕГО.

    Страж: german_grammar_tables.build_verb_conjugation отдаёт только напечатанное —
    справочник de.wiktionary или ответ модели, подтверждённый вторым спросом
    (german_verb_paradigms). Счёт от основы удалён 23.08.2026 (решение владельца):
    на не-глаголе он давал «ich boree», «ich aspettiamoe», «ich besagte».

    Поэтому это число — НЕ поломка, а наряд на работу: столько заголовков помечены
    глаголом, а таблицы у них нет. Обычно это значит одно из двух: глагола ещё не
    спрашивали (закроет ночь) или заголовком стоит не глагол — причастие, форма,
    чужое слово, — и чинить надо часть речи.
    """
    cur.execute("""SELECT DISTINCT lower(display) FROM bt_3_lex_units
                    WHERE lang='de' AND display ~ '^[a-zäöüßA-ZÄÖÜ]+$'
                      AND (pos='verb' OR card->>'part_of_speech'='verb');""")
    words = [r[0] for r in cur.fetchall()]
    if not words:
        return 0, []
    # Справочник читается ОДНИМ запросом: иначе на каждое слово приходится поход в базу.
    from backend.german_verb_paradigms import _MODEL_KEY_PREFIX
    import backend.german_verb_paradigms as paradigms
    cur.execute("SELECT verb, tables, documented FROM bt_3_german_verb_paradigms;")
    known = {v: (t if d else {}) for v, t, d in cur.fetchall()}
    original = paradigms.load_paradigm
    paradigms.load_paradigm = lambda verb: known.get(str(verb or "").strip().lower())
    try:
        hits = [w for w in words if not paradigms.paradigm_for_verb(w)]
    finally:
        paradigms.load_paradigm = original
    return len(hits), hits[:5]


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
    ("глагол без источника спряжения", _rule_verb_without_a_conjugation_source),
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


# ── КТО ЗАКРЫВАЕТ КАЖДУЮ СТРОКУ ──────────────────────────────────────────────
#
# Владелец 23.08.2026, дословно: «вот что это, для чего это мне, что я буду на него
# смотреть? Я должен какой-то вывод по ним сделать или что это значит? Я иду по улице с
# телефоном, приходит такое сообщение — мои действия какие?»
#
# Он прав, и это была ошибка проектирования, а не оформления. Число, на которое нельзя
# нажать, не работа, а перекладывание работы на владельца: он всё равно ничего не сделает
# с телефона со списком из тридцати трёх слов.
#
# ПРАВИЛО: у строки отчёта не бывает состояния «просто посмотри». Она либо
#   САМА     — это чинит ночь, смотреть не надо, число обязано убывать; или
#   КНОПКА   — машина исчерпала себя, нужен человек, и рядом сказано, ГДЕ кнопка.
#
# Правило без того и другого в отчёт не выносится вовсе: значит, работа не доделана,
# и доделывать её должен я, а не владелец глазами.
FIXES_ITSELF = "сама"
NEEDS_OWNER = "кнопка"

CLOSED_BY: dict[str, tuple[str, str]] = {
    "фраза помечена существительным": (FIXES_ITSELF, "снимается на выдаче"),
    "род повешен на многословное": (FIXES_ITSELF, "снимается на выдаче"),
    "школьный хвост «, -n» в заголовке": (FIXES_ITSELF, "чистится на записи"),
    "русская подсказка внутри немецкого": (FIXES_ITSELF, "чистится на записи"),
    "слово не находится по своему написанию": (FIXES_ITSELF, "дверь заводит написание"),
    "слово невидимо для ночного добора": (FIXES_ITSELF, "ночной добор"),
    "разбор лежит лицом не в ту сторону": (FIXES_ITSELF, "разворот на записи"),
    "пул отвечает не на том языке": (FIXES_ITSELF, "заслон на входе и на выдаче"),
    "карточка указывает в никуда": (FIXES_ITSELF, "ночная сверка ссылок"),
    "русский текст в немецком поле": (FIXES_ITSELF, "заслон на записи"),
    "выбор владельца не первый": (FIXES_ITSELF, "порядок при выдаче"),
    "немецкий текст пишется мимо двери": (NEEDS_OWNER, "новый путь записи — нужен я, "
                                                       "а не владелец"),
    "глагол без источника спряжения": (FIXES_ITSELF, "ночь спрашивает справочник, "
                                                     "потом модель дважды"),
}


def owner_line() -> str:
    """ОДНА строка для телефона. Отвечает на единственный вопрос: мне что-то делать?

    Владелец 25.08.2026, дословно: «ну хорошо, получу я отчёт о целостности словаря — и
    что мне это даст? Я получу его на телефон и что, смотреть на странные цифры? Я его
    просто пролистываю, потому что это человек читать не может».

    Он прав, и вина тут не в оформлении. Отчёт из тринадцати строк с числами — это
    перекладывание работы на владельца: он всё равно ничего не сделает с телефона.
    Поэтому здесь НЕ отчёт, а ВЕРДИКТ, и состояний у него ровно три:

        чисто            — делать нечего;
        ночь разбирает   — делать нечего, но число названо, чтобы было видно движение;
        нужен человек    — вот тут и только тут владельца зовут, и сказано куда идти.

    Развёрнутая разбивка осталась в report_lines() и вызывается по команде — тем, кто
    вправду полез разбираться. В ежедневное сообщение идёт эта строка.
    """
    try:
        result = run()
    except Exception as exc:
        # Молчание проверки — не «всё чисто». Об этом говорим вслух.
        logging.warning("целостность словаря: проверка не отработала: %s", exc)
        return "📖 Словарь: ⚠️ проверка не отработала — я разбираюсь."

    людям, машине = 0, 0
    for title, count in result["broken"].items():
        if not count:
            continue
        кто, _ = CLOSED_BY.get(title, (NEEDS_OWNER, ""))
        if кто == NEEDS_OWNER:
            людям += count
        else:
            машине += count

    if result["failed"]:
        # Правило упало — это НЕ «чисто». Соврать здесь дороже всего: владелец увидит
        # зелёную строку и решит, что можно выпускать.
        сколько = len(result["failed"])
        return (f"📖 Словарь: ⚠️ {сколько} проверк{'а' if сколько == 1 else 'и'} не "
                f"отработал{'а' if сколько == 1 else 'и'} — состояние НЕ известно. Я разбираюсь.")
    if людям:
        return (f"📖 Словарь: <b>{людям}</b> записей машина закрыть не может — нужен ты. "
                f"Разбор: /admin_dict_integrity")
    if машине:
        return (f"📖 Словарь в порядке. {машине} записей ночь разбирает сама — "
                f"завтра их должно стать меньше. От тебя ничего не нужно.")
    return "📖 Словарь чист. Ошибок нет."


def report_lines() -> list[str]:
    """Человеческий отчёт: одно число сверху, разбивка ниже, и только по ненулевым."""
    result = run()
    mine: list[str] = []
    yours: list[str] = []
    for title, count in sorted(result["broken"].items(), key=lambda kv: -kv[1]):
        if not count:
            continue
        who, how = CLOSED_BY.get(title, (NEEDS_OWNER, "закрывать нечем — это моя задача"))
        (yours if who == NEEDS_OWNER else mine).append(f"  • {title}: {count} — {how}")

    lines = [f"Целостность словаря: {result['total']} нарушений"]
    if mine:
        lines.append("Чинится само, смотреть не надо (число должно убывать):")
        lines.extend(mine)
    if yours:
        lines.append("Нужен человек:")
        lines.extend(yours)
    for title, why in result["failed"].items():
        lines.append(f"  ⚠️ правило не отработало — {title}: {why}")
    if result["total"] == 0 and not result["failed"]:
        lines.append("  всё чисто")
    return lines
