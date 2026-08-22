# -*- coding: utf-8 -*-
"""Кто ещё пишет немецкий текст в базу мимо проверок — и что из этого уже закрыто.

╔══════════════════════════════════════════════════════════════════════════════════╗
║  ЭТО НЕ СПИСОК ЗАДАЧ, А ПРОВЕРКА ПО КОДУ.                                        ║
║                                                                                  ║
║  Владелец, 22.08.2026: «А как мне понять, кто конкретно их делает, сделали ли     ║
║  они их или нет? Пометили ли они себе их или нет?»                               ║
║                                                                                  ║
║  Список, в котором каждый сам помечает своё, врёт ровно на столько, на сколько    ║
║  агент забыл его обновить. Поэтому здесь не пометки, а ЧТЕНИЕ КОДА: у каждого     ║
║  места есть признак «дверь стоит» — вызов чистки входа. Прогнал — видно, что      ║
║  правда, а не что обещано.                                                       ║
║                                                                                  ║
║      python3 scripts/dict_write_doors_audit.py                                    ║
╚══════════════════════════════════════════════════════════════════════════════════╝

ОТКУДА ВЗЯЛСЯ СПИСОК МЕСТ. Карту собрал соседний агент 21.08.2026, обойдя весь код:
четырнадцать мест кладут немецкий ТЕКСТ в три хранилища (справочник слов, карточка
человека, общий пул), а чистка входа стояла на четырёх. Владелец распорядился закрывать
и поделил работу между агентами по номерам.

ЧТО СЧИТАЕТСЯ ЗАКРЫТЫМ. В теле функции есть вызов чистки (`clean_text`, `door_check`,
`_apply_german_headword_normalization`) либо она зовёт того, у кого чистка внутри
(`retitle_unit`, `spread_correction_everywhere`, `ensure_unit`). Отдельно помечены места,
проверенные и признанные НЕОПАСНЫМИ, — у каждого в коде написано, почему.
"""
from __future__ import annotations

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Двери бывают ДВУХ РАЗНЫХ ВИДОВ, и это надо показывать, а не сводить к «да/нет»:
#   чистка   — прибирает вход (невидимые символы, двойные пробелы, начертания «ä»);
#   отказ    — не пускает уже испорченный текст (размноженный хвост).
# Место может иметь одну и не иметь другой. «Дверь стоит» = есть хотя бы одна.
DOORS = {
    "чистка": ("clean_text", "door_check", "_apply_german_headword_normalization",
               "retitle_unit", "spread_correction_everywhere", "ensure_unit",
               "_create_or_attach_user_dictionary_entry_with_cursor",
               "_upsert_dictionary_canonical_entry_with_cursor"),
    "отказ": ("mangled_strings_inside", "is_mangled"),
}

# (номер, файл, функция или None для всего файла, что это для человека, за кем закреплено)
PLACES = (
    (1, "backend/database.py", "edit_vocabulary_entry",
     "правка карточки человеком в мини-аппе", "закрыто 22.08"),
    (2, "backend/database.py", "split_vocabulary_entry_senses",
     "«разбить карточку на значения»", "закрыто 22.08"),
    (3, "backend/word_confirm_digest.py", "apply_decisions",
     "человек вписывает своё написание слова", "закрыто соседом 21.08"),
    (4, "backend/phrase_night_check.py", "_apply_silent_fix",
     "ночная тихая правка фразы", "закрыто 21.08"),
    (5, "backend/database.py", "reset_dictionary_card_for_rebuild",
     "«пересобрать неверно написанное слово»", "закрыто 22.08"),
    (6, "backend/database.py", "update_dictionary_entry_full_columns",
     "добор перевода быстрого словаря", "за соседним агентом"),
    (7, "backend/database.py", "update_webapp_dictionary_entry",
     "ночное обогащение карточек (8 вызывающих)", "за соседним агентом"),
    (8, "backend/database.py", "spread_correction_everywhere",
     "развоз правки по трём хранилищам", "закрыто 22.08"),
    (9, "backend/lex_units.py", "retitle_unit",
     "переименование слова", "за соседним агентом"),
    (10, "backend/lex_units.py", "save_unit_card",
     "сохранение разбора на слове", "закрыто соседом 21.08"),
    (11, "backend/backend_server.py", "_run_synonym_backfill",
     "ночной добор синонимов", "закрыто соседом 21.08"),
    (12, "backend/dictionary_article_backfill.py", None,
     "склейка артикля с заголовком", "за соседним агентом"),
    (13, "scripts/import_lingualeo.py", None,
     "импорт из Lingualeo", "проверено 22.08 — неопасен"),
    # Точечно на функцию, а не на весь файл: по целому database.py проверка находила
    # чужие двери из соседних функций и объявляла миграцию закрытой без оснований.
    (14, "backend/database.py", "ensure_webapp_tables",
     "миграция 2026_02_19 (пометки языка)", "проверено 22.08 — неопасна"),
)

SAFE_MARK = "НЕ ПОДНИМАТЬ КАК НАХОДКУ"


def read_source(path: str, ref: str | None) -> str:
    """Код из ПРОДА (ветка на сервере) или из рабочего каталога.

    По умолчанию смотрим то, что задеплоено. В общем каталоге одновременно работают
    несколько агентов, и у каждого копия отстаёт на сколько-то коммитов: проверка по
    своему дереву показала бы «двери нет» там, где сосед её уже поставил и запушил.
    Именно это и случилось на первом прогоне.
    """
    if not ref:
        try:
            return open(os.path.join(ROOT, path), encoding="utf-8").read()
        except OSError:
            return ""
    import subprocess
    out = subprocess.run(["git", "show", f"{ref}:{path}"], cwd=ROOT,
                         capture_output=True, text=True)
    return out.stdout


def body_of(path: str, name: str | None, ref: str | None = None) -> str:
    text = read_source(path, ref)
    if not text:
        return ""
    if not name:
        return text
    # `^\s*` тут нельзя: `\s` захватывает и перевод строки, поэтому начало тела уезжало
    # на пустую строку ПЕРЕД функцией, а следующая `def` находилась сразу же — тело
    # получалось длиной в один символ, и проверка объявляла «двери нет» у всех подряд.
    # Поймано на первом же прогоне: закрытые мною места показались открытыми.
    match = re.search(rf"(?m)^(?:[ \t]*)(?:async )?def {re.escape(name)}\(", text)
    if not match:
        return ""
    start = match.start()
    nxt = re.search(r"(?m)^(?:async )?(?:def|class) ", text[match.end():])
    return text[start: match.end() + nxt.start()] if nxt else text[start:]


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--here", action="store_true",
                        help="смотреть рабочий каталог, а не то, что в проде")
    args = parser.parse_args()
    ref = None if args.here else "bot3_webapp/refactor/interface"
    if ref:
        import subprocess
        subprocess.run(["git", "fetch", "-q", "bot3_webapp"], cwd=ROOT,
                       capture_output=True, timeout=180)
    print("\nисточник: " + ("рабочий каталог" if args.here else "код в проде"))
    closed = open_places = safe = missing = 0
    print("\nМЕСТА, КОТОРЫЕ ПИШУТ НЕМЕЦКИЙ ТЕКСТ В БАЗУ\n")
    print(f"{'№':>3}  {'состояние':<26} {'что это':<44} закреплено")
    print("─" * 118)
    for number, path, name, human, owner in PLACES:
        body = body_of(path, name, ref)
        found = [kind for kind, tokens in DOORS.items()
                 if any(token in body for token in tokens)]
        if not body:
            state, mark = "НЕ НАЙДЕНО В КОДЕ", "⁉️"
            missing += 1
        elif SAFE_MARK in body:
            state, mark = "проверено, неопасно", "🟢"
            safe += 1
        elif found:
            state, mark = "дверь: " + " + ".join(found), "✅"
            closed += 1
        else:
            state, mark = "ДВЕРИ НЕТ", "❌"
            open_places += 1
        print(f"{number:>3}  {mark} {state:<24} {human:<44} {owner}")

    print("─" * 118)
    print(f"\nдверь стоит: {closed}   проверено и неопасно: {safe}   "
          f"ДВЕРИ НЕТ: {open_places}" + (f"   не найдено: {missing}" if missing else ""))
    if open_places:
        print("\nСтроки с ❌ — настоящая работа. Всё остальное закрыто и перепроверять не нужно.")
    else:
        print("\nОткрытых мест не осталось.")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
