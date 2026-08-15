# -*- coding: utf-8 -*-
"""ОДИН отчёт о дефектах словаря: что сломано, что НЕ сломано, и почему.

Зачем этот файл существует
──────────────────────────
Одни и те же вопросы («сколько записей испорчено?», «а это вообще дефект?»)
разбирались несколько раз и каждый раз давали РАЗНЫЕ числа — потому что каждый
замер придумывал себе новое правило отбора. Здесь правила записаны один раз,
в коде, и берутся ОТТУДА ЖЕ, откуда их берёт продукт (`backend.lex_units`):
_MAX_LINKS, _DEMOTED_RANK, _EXERCISE_BLANK, _GRAMMAR_NOTE_RE,
looks_like_example_not_translation. Поэтому отчёт не может разойтись с экраном:
если продукт поменяет правило, отчёт поменяется вместе с ним.

Скрипт ТОЛЬКО ЧИТАЕТ. Он ничего не чинит и ничего не удаляет.

    python3 scripts/dict_defect_audit.py            # весь отчёт
    python3 scripts/dict_defect_audit.py --list 5   # + примеры по 5 штук на пункт

Как читать вердикты
───────────────────
    ДЕФЕКТ   — человек видит испорченное; чинить.
    ЧИСТО    — проверено, это НЕ дефект; больше не возвращаться.
    ВЛАДЕЛЬЦУ— данные собраны, решение продуктовое, за владельцем.

Замер от 11.08.2026 записан в каждом пункте: если новый прогон даёт другое число,
значит что-то изменилось — это и есть повод разбираться, а не мерить заново с нуля.

╔══════════════════════════════════════════════════════════════════════════════════╗
║  ЭТО НЕ ДЕФЕКТ. КАРТОЧКИ-УПРАЖНЕНИЯ. НЕ ПОДНИМАТЬ СНОВА.                         ║
║                                                                                  ║
║  В словаре живут 199 карточек, у которых вместо пары «слово — перевод» лежит      ║
║  учебное задание с пропуском:                                                    ║
║        source_text = «Er ___ jeden Morgen früh zur Arbeit.»                      ║
║        target_text = «abfahren»  либо русский перевод предложения                ║
║                                                                                  ║
║  Они ВЫГЛЯДЯТ сломанными почти под любое правило отбора и всплывают снова и       ║
║  снова: как «перепутанные языки» (метка языка к заданию не подходит), как         ║
║  «дубли» (у одного глагола их до девяти), как «не словарная форма», как           ║
║  «свалка». Каждый раз разбор заканчивался одинаково — это не дефект, они          ║
║  нужны продукту и работают.                                                      ║
║                                                                                  ║
║  ОПОЗНАЮТСЯ ТОЧНО, три признака совпадают до единицы (замер 14.08.2026):          ║
║        origin_process = 'sentence_gpt_seed'                                      ║
║        response_json ? 'sentence_with_gap'                                       ║
║        response_json ? 'quiz_type'                                               ║
║                                                                                  ║
║  ЛЮБОЙ новый замер по словарю обязан их исключать — функция is_training_exercise  ║
║  ниже. Владелец 14.08.2026: «ты поднимаешь это раз двадцатый, пометь так, чтобы   ║
║  больше не тратить на это время».                                                ║
║                                                                                  ║
║  ⚠ И отдельная ловушка замера: в SQL знак подчёркивания в LIKE — это подстановка  ║
║    «любой символ». Условие LIKE '%___%' совпадает СО ВСЕМ подряд (24 906 из       ║
║    24 906). Искать прочерк надо через position('___' in …) или регулярку.         ║
╚══════════════════════════════════════════════════════════════════════════════════╝
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context          # noqa: E402
from backend import lex_units as LU                             # noqa: E402

SPACE_RE = re.compile(r"\s+")
CAPITAL_RU = re.compile(r"^[А-ЯЁ]")
# Номер значения внутри перевода: «1 ободрять 2 вдохновлять». Именно ЭТИ строки резал
# scripts/dict_units_split_senses.py, поэтому правило здесь то же самое, что там.
SENSE_NUM_RE = re.compile(r"(?:^|[\s;,])(\d{1,2})\s*[).]?\s*(?=[А-Яа-яЁёA-Za-z])")
# Свалка ОТЛИЧАЕТСЯ от предложения с числом. «Отпуск продлили на 2 недели» — не свалка:
# цифра там часть смысла. Свалка — это НУМЕРАЦИЯ: номер в начале строки либо номер
# после конца предыдущего значения. Без этого различия отчёт насчитывает лишнего.
DUMP_HEAD_RE = re.compile(r"^\s*\d{1,2}\s*[).]?\s+?[А-Яа-яЁё]")
DUMP_MID_RE = re.compile(r"[а-яё,;]\s+\d{1,2}\s*[).]?\s+[А-Яа-яЁё]")


def norm(text) -> str:
    return SPACE_RE.sub(" ", str(text or "").strip()).casefold()


def enum_parts(text) -> list[str]:
    """Перечисление → отдельные элементы. «зажим, клипса» → [«зажим», «клипса»]."""
    return [p.strip() for p in re.split(r"[;,]", str(text or "")) if p.strip()]


def dump_pieces(text) -> list[str]:
    """Свалка → куски ровно так, как их резал dict_units_split_senses.py."""
    raw = SPACE_RE.sub(" ", str(text or "").strip())
    marks = [(m.start(1), m.end(0)) for m in SENSE_NUM_RE.finditer(raw)]
    chunks: list[str] = []
    if len(marks) >= 2:
        for index, (start, body) in enumerate(marks):
            end = marks[index + 1][0] if index + 1 < len(marks) else len(raw)
            chunks.append(raw[body:end])
        head = raw[:marks[0][0]].strip(" ;,")
        if head:
            chunks.insert(0, head)
    else:
        chunks = [raw]
    out: list[str] = []
    for chunk in chunks:
        out.extend(p.strip(" ;,.") for p in chunk.split(";") if p.strip(" ;,."))
    return out


TRAINING_EXERCISE_SQL = """
    (q.origin_process = 'sentence_gpt_seed'
     OR q.response_json ? 'sentence_with_gap'
     OR q.response_json ? 'quiz_type')
"""


def is_training_exercise(payload, origin_process: str | None = None) -> bool:
    """Это карточка-УПРАЖНЕНИЕ, а не пара «слово — перевод». Из замеров исключать.

    Смотри рамку в начале файла: такие карточки всплывают почти под любое правило
    отбора и каждый раз оказываются не дефектом. Три признака совпадают до единицы:
    дверь `sentence_gpt_seed`, поле `sentence_with_gap`, поле `quiz_type` — по 199 штук.
    Проверяем все три: любой из них достаточен, если какой-то путь его не проставил.
    """
    if str(origin_process or "").strip() == "sentence_gpt_seed":
        return True
    data = payload if isinstance(payload, dict) else {}
    return bool(data.get("sentence_with_gap") or data.get("quiz_type"))


def is_dump(text) -> bool:
    return bool(DUMP_HEAD_RE.match(str(text or "")) or DUMP_MID_RE.search(str(text or "")))


def say(verdict: str, title: str, number, measured: str, measured_at: str = "11.08.2026") -> None:
    mark = {"ДЕФЕКТ": "✗", "ЧИСТО": "✓", "ВЛАДЕЛЬЦУ": "?"}.get(verdict, "•")
    print("\n%s %-9s %s" % (mark, verdict, title))
    # Дата стоит рядом со СВОИМ числом: пункты добавлялись в разные дни, и общая
    # шапка «замер 11.08» подписывала бы более поздние замеры чужой датой.
    print("    сейчас: %-28s замер %s: %s" % (number, measured_at, measured))


def screen_of_words(cur) -> tuple[dict[int, list[str]], dict[int, str]]:
    """Точный слепок того, что человек видит на карточке ОДИНОЧНОГО немецкого слова.

    Повторяет _fetch_links + отбор в _build_item шаг в шаг: ранг ниже понижённого,
    без заготовок упражнений, без грамматических помет, без повторов, первые _MAX_LINKS."""
    cur.execute(
        """
        SELECT l.from_unit, f.display, u.display
        FROM bt_3_lex_links l
        JOIN bt_3_lex_units u ON u.id = l.to_unit
        JOIN bt_3_lex_units f ON f.id = l.from_unit
        WHERE f.lang = 'de' AND f.kind = 'word' AND u.lang = 'ru'
          AND l.rank < %s AND position(%s in u.display) = 0
        ORDER BY l.from_unit, (l.sense_id IS NULL), l.rank, u.id;
        """,
        (LU._DEMOTED_RANK, LU._EXERCISE_BLANK),
    )
    screen: dict[int, list[str]] = defaultdict(list)
    names: dict[int, str] = {}
    for from_unit, german, russian in cur.fetchall():
        names[from_unit] = german
        shown = screen[from_unit]
        if len(shown) >= LU._MAX_LINKS:
            continue
        if LU._EXERCISE_BLANK in russian or LU._GRAMMAR_NOTE_RE.search(russian):
            continue
        if any(norm(russian) == norm(x) for x in shown):
            continue
        shown.append(russian)
    return screen, names


# ═══ 1. Понижённые в ранг 900 ═══════════════════════════════════════════════════
def audit_demoted(cur, examples: int) -> None:
    print("\n" + "═" * 78)
    print("1. СТРОКИ, ПОНИЖЕННЫЕ В РАНГ %d" % LU._DEMOTED_RANK)
    print("═" * 78)
    print("""
Что это. Разрезая свалку «1 ободрять 2 вдохновлять» на отдельные значения, скрипт
dict_units_split_senses.py исходную строку не удалял, а ставил ей ранг 900. Выдача
берёт только ранг НИЖЕ 900 (backend/lex_units.py, _fetch_links), поэтому строка лежит
в таблице, но ни один экран её не запрашивает.

Здесь ДВЕ РАЗНЫЕ популяции, и их нельзя складывать — они лечатся по-разному.""")

    # ── A. de → ru: понижённые переводы немецких слов
    cur.execute(
        """
        SELECT l.from_unit, f.display, u.display
        FROM bt_3_lex_links l
        JOIN bt_3_lex_units u ON u.id = l.to_unit
        JOIN bt_3_lex_units f ON f.id = l.from_unit
        WHERE l.rank >= %s AND f.lang = 'de' AND u.lang = 'ru';
        """,
        (LU._DEMOTED_RANK,),
    )
    demoted = cur.fetchall()
    units = {r[0] for r in demoted}
    cur.execute(
        """
        SELECT l.from_unit, u.display FROM bt_3_lex_links l
        JOIN bt_3_lex_units u ON u.id = l.to_unit
        WHERE l.from_unit = ANY(%s) AND l.rank < %s AND u.lang = 'ru';
        """,
        (list(units), LU._DEMOTED_RANK),
    )
    alive: dict[int, set[str]] = defaultdict(set)
    for from_unit, russian in cur.fetchall():
        alive[from_unit].add(norm(russian))

    recoverable, lossy = [], []
    for from_unit, german, russian in demoted:
        have = alive.get(from_unit, set())
        missing = []
        for piece in dump_pieces(russian):
            key = norm(piece)
            if any(key == q or key in [norm(x) for x in enum_parts(q)] for q in have):
                continue
            missing.append(piece)
        (recoverable if not missing else lossy).append((german, russian, missing))

    say("ЧИСТО", "A. de→ru: понижённые склейки НЕ видны человеку — это работает",
        "%d связей у %d слов" % (len(demoted), len(units)), "450 связей / 426 слов")
    print("""    Проверка руками: ermutigen — в базе лежит «1 ободрять 2 вдохновлять» с рангом
    900, а на карточке стоят ободрять · поощрять · вдохновлять. Строка не мешает.""")

    say("ВЛАДЕЛЬЦУ", "   можно ли их УДАЛИТЬ — считано, а не выбрано",
        "лишних %d, с потерей %d" % (len(recoverable), len(lossy)), "лишних 302, с потерей 148")
    print("""    Удалять безопасно только то, что полностью восстановимо из видимых переводов
    того же слова. Таких %d — в них нет ни одного смысла, которого нет рядом.
    Но у %d строк часть смысла живёт ТОЛЬКО в них, и удаление — потеря:
        aufschlagen  «открывать; разбивать (яйцо); ударяться» → «разбивать (яйцо)» больше нигде нет
        zutraulich   «доверчивый; ручной (о животных)»        → «ручной (о животных)» больше нигде нет
    Отдельно замерено: этих связей никто себе не сохранял (saves_count=0 у всех),
    личных карточек на них не ссылается. То есть удаление ничего не сломает — но и
    ничего не даст: строки уже не видны и места не занимают. Решение продуктовое.""" % (
        len(recoverable), len(lossy)))
    for german, russian, missing in lossy[:examples]:
        print("      потеряли бы: %-18s %s  ↳ %s" % (german[:18], russian[:52], "; ".join(missing)[:40]))

    # ── B. ru → de: мусорные русские единицы
    cur.execute(
        """
        SELECT l.from_unit, f.display, u.display
        FROM bt_3_lex_links l
        JOIN bt_3_lex_units u ON u.id = l.to_unit
        JOIN bt_3_lex_units f ON f.id = l.from_unit
        WHERE l.rank >= %s AND f.lang = 'ru';
        """,
        (LU._DEMOTED_RANK,),
    )
    reverse = cur.fetchall()
    junk = {r[0] for r in reverse}
    cur.execute("SELECT count(*) FROM bt_3_lex_surfaces WHERE unit_id = ANY(%s);", (list(junk),))
    surfaces = cur.fetchone()[0]
    cur.execute(
        "SELECT count(*) FROM bt_3_lex_links WHERE to_unit = ANY(%s) AND rank < %s;",
        (list(junk), LU._DEMOTED_RANK),
    )
    still_shown = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM bt_3_webapp_dictionary_queries WHERE lex_unit_id = ANY(%s);", (list(junk),))
    personal = cur.fetchone()[0]

    say("ДЕФЕКТ", "B. ru→de: сама СВАЛКА заведена русской единицей словаря",
        "%d единиц, %d написаний" % (len(junk), surfaces), "380 единиц, 435 написаний")
    print("""    Свалка «1 густой, частый 2 плотный, непроницаемый» лежит в базе не как текст
    перевода, а как РУССКОЕ СЛОВО со своими написаниями для поиска. Понижение ранга
    прячет её из карточки немецкого слова, но саму единицу из словаря не убирает.
    Личных карточек на них: %d. Показываются как перевод немецкого слова: %d.""" % (personal, still_shown))
    cur.execute(
        """
        SELECT f.display, u.display FROM bt_3_lex_links l
        JOIN bt_3_lex_units u ON u.id = l.to_unit
        JOIN bt_3_lex_units f ON f.id = l.from_unit
        WHERE l.to_unit = ANY(%s) AND l.rank < %s LIMIT %s;
        """,
        (list(junk), LU._DEMOTED_RANK, examples),
    )
    for german, russian in cur.fetchall():
        print("      всё ещё на экране: %-16s → %s" % (german[:16], russian[:50]))


# ═══ 2. Свалки, которые ВИДНО ═══════════════════════════════════════════════════
def audit_visible_dumps(cur, screen, names, examples: int) -> None:
    print("\n" + "═" * 78)
    print("2. СВАЛКИ С НОМЕРАМИ, КОТОРЫЕ ЧЕЛОВЕК ВСЁ-ТАКИ ВИДИТ")
    print("═" * 78)
    print("""
Разрезание прошло не по всем. Считаем ТОЛЬКО карточки одиночных слов: в предложении
цифра — часть смысла («Отпуск продлили на 2 недели»), и это не дефект.""")
    hits = [(names[u], v) for u, vals in screen.items() for v in vals if is_dump(v)]
    say("ДЕФЕКТ", "номер значения внутри перевода на карточке слова",
        "%d строк" % len(hits), "15 строк")
    for german, russian in sorted(hits):
        print("      %-22s → %s" % (german[:22], russian[:80]))


# ═══ 3. Регистр ═════════════════════════════════════════════════════════════════
def audit_capitals(cur, screen, names, examples: int) -> None:
    print("\n" + "═" * 78)
    print("3. ПЕРЕВОДЫ С ЗАГЛАВНОЙ БУКВЫ")
    print("═" * 78)
    texts = {v for vals in screen.values() for v in vals}
    capital = {v for v in texts if CAPITAL_RU.match(v)}
    sentences = {v for v in capital if v.rstrip().endswith((".", "!", "?"))}
    ordinary = capital - sentences

    say("ДЕФЕКТ", "обычный перевод начинается с заглавной",
        "%d из %d" % (len(ordinary), len(texts)), "1429 из 12238")
    print("""    «Аккуратный, опрятный», «Аванс, задаток», «Боль в мышцах» — это переводы, а не
    предложения. Настоящих предложений среди заглавных всего %d (кончаются на . ! ?).""" % len(sentences))
    for value in sorted(ordinary)[:examples]:
        print("      %s" % value[:80])

    # ⚠ Здесь раньше стоял вердикт «ДЕФЕКТ: заслон приёмки выбрасывает 684 хороших
    # перевода». Он был НЕВЕРЕН и снят 11.08.2026. Считалось так: взять строки, уже
    # лежащие в банке, прогнать через заслон и объявить все срабатывания ложными — то
    # есть без разметки, на допущении «раз лежит в переводах, значит перевод».
    # С настоящей разметкой (перевод лежит в card->value, пример — в card->example_target)
    # правило ловит 10 874 примера из 10 889 и задевает 9 переводов из 1 177, а по
    # единственному боевому вызову теряет 8 значений на всю базу. Правило РАБОТАЕТ.
    # Оставляем здесь замер настоящей дыры, которая рядом и в сотни раз крупнее.
    # ⚠ И ЗДЕСЬ ЖЕ снят второй ложный вердикт того же дня: «4373 единицы с разбором
    # никогда не переносили значения в переводы, 5500 значений потеряно». Проверка
    # встречным вопросом «а не пришли ли переводы другим путём?» его убила: из 4508
    # таких единиц 4426 переводы ИМЕЮТ — просто из пула (source='пул'), а не из разбора.
    # Отсутствие переноса из разбора само по себе не потеря. Урок общий: прежде чем
    # называть число дырой, спроси, чем ещё оно может объясняться.
    cur.execute(
        """
        SELECT count(*), count(*) FILTER (WHERE u.kind = 'word')
        FROM bt_3_lex_units u
        WHERE u.lang = 'de' AND u.card IS NOT NULL AND u.card::text <> '{}'
          AND NOT EXISTS (SELECT 1 FROM bt_3_lex_links l WHERE l.from_unit = u.id);
        """
    )
    mute_total, mute_words = cur.fetchone()
    say("ДЕФЕКТ", "разбор есть, а перевода НЕТ НИ ОДНОГО — карточка немая",
        "%d единиц, из них слов %d" % (mute_total, mute_words), "82 единицы, слов 43")
    print("""    Вот это — настоящий остаток, и он маленький. У слова лежит разобранная
    карточка, но ни одной связи-перевода нет вообще: показать человеку нечего.
    Среди них видна и посторонняя болезнь — немецкие единицы с русским текстом
    («die раскопки», «der кислотность», «die растение») и грамматическая помета «vt»,
    заведённая как слово. Их стережёт chk_lex_units_script_matches_lang, но он NOT VALID,
    поэтому накопленное до него живо.""")


# ═══ 4. Дубли ═══════════════════════════════════════════════════════════════════
def audit_duplicates(cur, screen, names, examples: int) -> None:
    print("\n" + "═" * 78)
    print("4. ДУБЛИ: ОДИН ПЕРЕВОД ЦЕЛИКОМ СИДИТ ВНУТРИ ДРУГОГО")
    print("═" * 78)
    print("""
Правило: у слова показывают и «изменение», и «Перемена, изменение». Второе не новый
смысл, а первое плюс мусор. Защита от повторов в _build_item такое не ловит — она
сравнивает строки целиком.""")
    multi = {u: v for u, v in screen.items() if len(v) >= 2}
    kinds: dict[str, set[int]] = defaultdict(set)
    samples: dict[str, list] = defaultdict(list)
    for unit, values in multi.items():
        keys = [norm(v) for v in values]
        for i, long_ in enumerate(keys):
            chunks = [norm(p) for p in enum_parts(long_)]
            for j, short in enumerate(keys):
                if i == j or long_ == short:
                    continue
                # Порядок веток ЗНАЧИМ: одна пара попадает ровно в одну корзину,
                # иначе сумма по корзинам расходится с общим числом слов.
                kind = None
                if len(chunks) > 1 and short in chunks:
                    kind = "перечисление содержит другой перевод"
                elif long_.startswith(short + " ("):
                    kind = "то же слово плюс уточнение в скобках"
                elif long_.rstrip(".") == short.rstrip("."):
                    kind = "та же строка, отличается только знаком в конце"
                elif long_.startswith(short + " "):
                    kind = "то же слово плюс продолжение"
                elif len(chunks) > 1 and any(c.startswith(short + " ") for c in chunks):
                    kind = "начало элемента перечисления"
                if kind:
                    kinds[kind].add(unit)
                    if len(samples[kind]) < examples:
                        samples[kind].append((names[unit], values[j], values[i]))

    affected = set().union(*kinds.values()) if kinds else set()
    say("ДЕФЕКТ", "слов, где перевод повторён внутри другого перевода",
        "%d из %d (%.0f%%)" % (len(affected), len(multi), 100.0 * len(affected) / max(1, len(multi))),
        "1358 из 4608 (29%)")
    for kind in sorted(kinds, key=lambda k: -len(kinds[k])):
        print("      %-40s %5d слов" % (kind, len(kinds[kind])))
        for german, short, long_ in samples[kind][:2]:
            print("          %-18s %-26s ⊂ %s" % (german[:18], repr(short)[:26], repr(long_)[:44]))

    cur.execute("SELECT DISTINCT from_unit FROM bt_3_lex_links WHERE rank >= %s;", (LU._DEMOTED_RANK,))
    demoted_units = {r[0] for r in cur.fetchall()}
    with_glue = len(affected & demoted_units)
    say("ЧИСТО", "«просто перестанем показывать склейку» — к этой куче НЕ применимо",
        "склейка есть у %d, нет у %d" % (with_glue, len(affected) - with_glue),
        "есть у 211, нет у 1147")
    print("""    Понижать нечего: у %d слов из %d никакой понижённой склейки не существует.
    Разрезание в июле трогало только свалки С НОМЕРАМИ, а склейки через запятую не
    видело в принципе. Значит, лечится это не понижением в базе, а правилом на выдаче.""" % (
        len(affected) - with_glue, len(affected)))

    full = sum(1 for u in affected if len(multi[u]) >= LU._MAX_LINKS)
    say("ДЕФЕКТ", "дубли съедают лимит в %d переводов" % LU._MAX_LINKS,
        "%d слов показывают ровно %d" % (full, LU._MAX_LINKS), "338 слов")
    print("""    die Vorgehensweise: способ действия · метод · подход · процедура ·
    «Процедура, порядок действий, инструкция» · «процедура, порядок действий».
    Шесть строк, четыре из них — одно и то же. Другие значения человек не увидит.""")


# ═══ 5. Заглавные немецкие единицы ══════════════════════════════════════════════
def audit_german_capitals(cur, examples: int) -> None:
    print("\n" + "═" * 78)
    print("5. НЕМЕЦКИЕ СЛОВА, НАПИСАННЫЕ С ЗАГЛАВНОЙ БЕЗ РОДА")
    print("═" * 78)
    cur.execute(
        """
        SELECT id, display, pos FROM bt_3_lex_units
        WHERE lang = 'de' AND kind = 'word' AND gender IS NULL AND display ~ '^[A-ZÄÖÜ]'
          AND (pos IS NULL OR pos NOT ILIKE '%noun%')
        ORDER BY display;
        """
    )
    rows = cur.fetchall()
    say("ВЛАДЕЛЬЦУ", "заглавная без рода — это ЧЕТЫРЕ разные болезни, не одна",
        "%d единиц" % len(rows), "83 единицы")
    print("""    Опускать их списком нельзя: среди них есть настоящие существительные, которым
    не хватает только рода. Разложение по смыслу:
      • инфинитив, который бывает и глаголом, и существительным (Aufwachen, Prägen,
        Übergießen, Zurücklegen) → ДВЕ единицы: глагол со строчной и «das …» с родом.
        База это уже позволяет: ключ опознания — (язык, вид, ключ, часть речи, род).
      • существительное без рода (Aschenbecher, Stau, Verkehr, Blinker) → заглавную
        ОСТАВИТЬ, дотянуть род. Опустить их было бы порчей.
      • не существительное (Aber, Danke, Genau, Weil, Wenn, Heute) → строчная.
      • обрубок, не слово (Aufrechtha, Auftre, Erwe, Ic) → в словаре ему не место.""")
    for unit_id, display, pos in rows[:examples]:
        print("      %-8s %-22s %s" % (unit_id, display[:22], pos or "—"))


def audit_separable_gap_tasks(cur, examples: int) -> None:
    """Задания с пропуском на отделяемый глагол: собираются ли они обратно.

    Правило берётся ИЗ ПРОДУКТА (`backend_server.separable_gap_entry_is_sound`,
    оно же `gap_reconstructs_sentence`): подставь спрягаемую форму в первый
    пропуск и приставку во второй — обязано выйти правильное предложение.
    Если продукт поменяет правило, поменяется и это число.

    Замер 14.08.2026: 199 записей, годных 0. Все 199 старого формата — один
    пропуск и инфинитив в спрягаемую позицию («Er ___ die neuen Aufgaben
    sofort.» с ответом «annehmen»), чего в немецком не бывает. Показывались
    21 раз, живым людям — 5 показов на 2 человек, получен 1 ответ.
    """
    from backend.backend_server import separable_gap_entry_is_sound   # noqa: E402

    cur.execute("""
        SELECT id, response_json
        FROM bt_3_webapp_dictionary_queries
        WHERE response_json::text LIKE '%separable_prefix_verb_gap%'
        ORDER BY id
    """)
    rows = cur.fetchall()
    broken = [(rid, rj) for rid, rj in rows if not separable_gap_entry_is_sound(rj)]

    verdict = "ЧИСТО" if not broken else "ДЕФЕКТ"
    say(verdict, "Задания с отделяемым глаголом не собираются обратно",
        "%d из %d" % (len(broken), len(rows)), "199 из 199 → починено 0 из 199",
        measured_at="14.08.2026")
    if not broken:
        print("      Все задания собираются: форма в первый пропуск, приставка во второй.")
        return
    print("""      Человеку показывают предложение, куда правильный ответ не встаёт ни одним
      способом. У отделяемого глагола ДВА места в предложении, значит и пропуска
      должно быть два: «Er ___ die neuen Aufgaben sofort ___.» → «nimmt … an».
      Записи старого формата на экран не идут (страж в bot_3._extract_prefix_quiz_context),
      но и в норму 100 заготовок не засчитываются — добираются правильными по ночам.""")
    for rid, rj in broken[:examples]:
        payload = rj if isinstance(rj, dict) else {}
        print("      %-8s %-38s → %s" % (
            rid,
            str(payload.get("sentence_with_gap") or "")[:38],
            str(payload.get("correct_infinitive") or "")[:18],
        ))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", type=int, default=8, help="сколько примеров печатать на пункт")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            screen, names = screen_of_words(cur)
            print("\nОснова отчёта: %d немецких слов с непустой карточкой, %d строк-переводов." % (
                len(screen), sum(len(v) for v in screen.values())))
            print("Замер 11.08.2026: 5066 слов, 15827 строк.")
            audit_demoted(cur, args.list)
            audit_visible_dumps(cur, screen, names, args.list)
            audit_capitals(cur, screen, names, args.list)
            audit_duplicates(cur, screen, names, args.list)
            audit_german_capitals(cur, args.list)
            audit_separable_gap_tasks(cur, args.list)
    print("\n" + "═" * 78)
    print("Скрипт ничего не менял. Все правила отбора — в этом файле, рядом с числом.")
    print("═" * 78)


if __name__ == "__main__":
    main()
