# -*- coding: utf-8 -*-
"""Кто пишет немецкий текст в базу — и стоит ли у него дверь. ОДИН список на всё.

╔══════════════════════════════════════════════════════════════════════════════════╗
║  ЭТО НЕ СПИСОК ЗАДАЧ, А ПРИЗНАК ДЛЯ ПРОВЕРКИ.                                    ║
║                                                                                  ║
║  Владелец, 22.08.2026: «А как мне понять, кто конкретно их делает, сделали ли     ║
║  они их или нет? Пометили ли они себе их или нет?»                               ║
║                                                                                  ║
║  Список, в котором каждый помечает своё, врёт ровно настолько, насколько агент    ║
║  забыл его обновить. Поэтому здесь не пометки, а признак, по которому состояние   ║
║  ЧИТАЕТСЯ ИЗ КОДА. Отсюда его берут двое: показ владельцу                         ║
║  (`scripts/dict_write_doors_audit.py`) и ночная проверка целостности словаря      ║
║  (`backend/dictionary_integrity.py`), где число уходит в утренний отчёт.          ║
║                                                                                  ║
║  Появился новый путь записи — добавляй СЮДА. Свой список у себя в файле заводить  ║
║  нельзя: ровно так тема «размноженного текста» открывалась четыре раза подряд.    ║
╚══════════════════════════════════════════════════════════════════════════════════╝

ОТКУДА ВЗЯЛСЯ СПИСОК. Карту собрал соседний агент 21.08.2026, обойдя весь код:
четырнадцать мест кладут немецкий ТЕКСТ в три хранилища (справочник слов, карточка
человека, общий пул), а чистка входа стояла на четырёх. Владелец распорядился закрывать,
работа была поделена между агентами по номерам и закрыта за два дня.

ДВЕРИ БЫВАЮТ ДВУХ РАЗНЫХ ВИДОВ, и сводить их к «да/нет» — терять половину картины:

    чистка   прибирает вход: невидимые символы из буфера, двойные пробелы с телефона,
             два разных начертания «ä». Без неё одно и то же слово перестаёт быть
             равно самому себе: не находится поиском, задваивается, не сходится с
             общей записью.
    отказ    не пускает уже испорченный текст (размноженный хвост). Развезти такой
             текст хуже, чем не почистить: он размножится сразу по трём хранилищам.

Место может иметь одну дверь и не иметь другой — это законно, если вторая ему не нужна.
Открытым считается место, у которого нет НИ ОДНОЙ.
"""
from __future__ import annotations

import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DOORS: dict[str, tuple[str, ...]] = {
    "чистка": ("clean_text", "door_check", "_apply_german_headword_normalization",
               "retitle_unit", "spread_correction_everywhere", "ensure_unit",
               "_create_or_attach_user_dictionary_entry_with_cursor",
               "_upsert_dictionary_canonical_entry_with_cursor"),
    "отказ": ("mangled_strings_inside", "is_mangled"),
}

# Место, проверенное и признанное НЕОПАСНЫМ, помечается этой фразой в самом коде —
# рядом с объяснением, почему. Так следующий не поднимет его как новую находку.
SAFE_MARK = "НЕ ПОДНИМАТЬ КАК НАХОДКУ"

# (номер, файл, функция или None для всего файла, что это для человека)
PLACES: tuple[tuple[int, str, str | None, str], ...] = (
    (1, "backend/database.py", "edit_vocabulary_entry",
     "правка карточки человеком в мини-аппе"),
    (2, "backend/database.py", "split_vocabulary_entry_senses",
     "«разбить карточку на значения»"),
    (3, "backend/word_confirm_digest.py", "apply_decisions",
     "человек вписывает своё написание слова"),
    (4, "backend/phrase_night_check.py", "_apply_silent_fix",
     "ночная тихая правка фразы"),
    (5, "backend/database.py", "reset_dictionary_card_for_rebuild",
     "«пересобрать неверно написанное слово»"),
    (6, "backend/database.py", "update_dictionary_entry_full_columns",
     "добор перевода быстрого словаря"),
    (7, "backend/database.py", "update_webapp_dictionary_entry",
     "ночное обогащение карточек (8 вызывающих)"),
    (8, "backend/database.py", "spread_correction_everywhere",
     "развоз правки по трём хранилищам"),
    (9, "backend/lex_units.py", "retitle_unit", "переименование слова"),
    (10, "backend/lex_units.py", "save_unit_card", "сохранение разбора на слове"),
    (11, "backend/backend_server.py", "_run_synonym_backfill", "ночной добор синонимов"),
    (12, "backend/dictionary_article_backfill.py", None, "склейка артикля с заголовком"),
    # ┌─ №13 ВЫЧЕРКНУТ 29.08.2026. НЕ ВОЗВРАЩАТЬ БЕЗ СЛОВА ВЛАДЕЛЬЦА. ───────────────┐
    # │ Здесь стоял `scripts/import_lingualeo.py` — «импорт из Lingualeo». Файла в   │
    # │ коде давно нет, поэтому проверка каждый день показывала состояние `missing`  │
    # │ и требовала работы, а работы не было: строка сторожила несуществующий путь.  │
    # │ Это и есть тот самый «1 — немецкий текст пишется мимо двери», из-за которого │
    # │ владельцу приходило «нужен ты» при пустом экране без кнопок.                 │
    # │                                                                             │
    # │ Владелец 29.08.2026, дословно: «зачем тут какая-то дверь? у нас просто один  │
    # │ раз было импортировано определённое количество слов и всё, и больше мы не    │
    # │ работаем с импортом из Lingualeo».                                           │
    # │                                                                             │
    # │ Импорт был РАЗОВЫМ и завершён. Пути записи нет — сторожить нечего. Если      │
    # │ импорт когда-нибудь вернут, строка возвращается ВМЕСТЕ с дверью, а не сама   │
    # │ по себе: список сторожит живые пути, а не память о мёртвых.                  │
    # └─────────────────────────────────────────────────────────────────────────────┘
    (14, "backend/database.py", "ensure_webapp_tables",
     "миграция 2026_02_19 (пометки языка)"),
)


def read_source(path: str, ref: str | None = None) -> str:
    """Код из рабочего каталога или из указанной ветки.

    В проде ветки нет и не нужно: рабочий каталог ТАМ и есть задеплоенный код. Ветка
    нужна показу на машине разработчика, где копия отстаёт: проверка по своему дереву
    показала бы «двери нет» у мест, которые сосед уже закрыл и запушил. Так и вышло на
    первом прогоне — пять закрытых мест выглядели открытыми.
    """
    if not ref:
        try:
            with open(os.path.join(ROOT, path), encoding="utf-8") as handle:
                return handle.read()
        except OSError:
            return ""
    import subprocess
    try:
        out = subprocess.run(["git", "show", f"{ref}:{path}"], cwd=ROOT,
                             capture_output=True, text=True, timeout=60)
        return out.stdout
    except Exception:
        return ""


def body_of(path: str, name: str | None, ref: str | None = None) -> str:
    """Тело функции или весь файл, если функция не названа."""
    text = read_source(path, ref)
    if not text or not name:
        return text
    # `^\s*` здесь нельзя: `\s` захватывает перевод строки, начало тела уезжает на
    # пустую строку ПЕРЕД функцией, следующая `def` находится сразу же — и тело выходит
    # длиной в один символ. На первом прогоне это объявило открытыми все места подряд.
    match = re.search(rf"(?m)^(?:[ \t]*)(?:async )?def {re.escape(name)}\(", text)
    if not match:
        return ""
    nxt = re.search(r"(?m)^(?:async )?(?:def|class) ", text[match.end():])
    return text[match.start(): match.end() + nxt.start()] if nxt else text[match.start():]


def inspect(ref: str | None = None) -> list[dict]:
    """Состояние каждого места. Ничего не чинит и в базу не ходит — читает код.

    state: "ok" — дверь стоит | "safe" — проверено и неопасно | "open" — двери нет
           | "missing" — места нет в коде (переименовали или удалили — тоже повод)
    """
    out = []
    for number, path, name, human in PLACES:
        body = body_of(path, name, ref)
        found = [kind for kind, tokens in DOORS.items()
                 if any(token in body for token in tokens)]
        if not body:
            state = "missing"
        elif SAFE_MARK in body:
            state = "safe"
        elif found:
            state = "ok"
        else:
            state = "open"
        out.append({"number": number, "path": path, "name": name, "human": human,
                    "state": state, "doors": found})
    return out


def places_without_a_door(ref: str | None = None) -> list[dict]:
    """Только то, что требует работы: двери нет либо место пропало из кода."""
    return [item for item in inspect(ref) if item["state"] in ("open", "missing")]
