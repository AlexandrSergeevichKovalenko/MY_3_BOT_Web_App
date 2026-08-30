# -*- coding: utf-8 -*-
"""ЗАМЕР: как часто модель ЗАТРАНСЛИТЕРИРОВАЛА немецкое слово вместо перевода.

Повод. Владелец, 30.08.2026, на разборе слова `millionenfach` в видео:
«в переводе по-русски написано немецкое слово миллионенфах». Русская половина
примера выглядит русской (кириллица), поэтому ни один существующий страж её не
видит: они ищут ЛАТИНИЦУ («разные Auffassungen», «rast ein LKW»).

Скрипт ТОЛЬКО ЧИТАЕТ. Он ничего не чинит и ничего не удаляет.

    railway run --service Postgres python3 scripts/dict_transliteration_audit.py
    railway run --service Postgres python3 scripts/dict_transliteration_audit.py --list 20

ПОЧЕМУ ЭТО ЗАМЕР, А НЕ СТРАЖ
────────────────────────────
Наивно поймать транслитерацию нельзя, и в этом вся сложность. У заимствований
ВЕРНЫЙ перевод и есть транслитерация: «Kultur» → «культура», «Musik» →
«музыка», «Adresse» → «адрес». Страж, построенный на одном сходстве написания,
срезал бы правильные переводы — это ровно тот случай, когда верный факт даёт
неверный вывод.

Поэтому здесь ДВА шага, и они разделены:
  1) широкий невод — практическая транскрипция немецкого заголовка на кириллицу
     (немецко-русская практическая транскрипция, Гиляревский–Старостин: правила
     sch→ш, ei→ай, eu→ой, ch→х, tz→ц, v→ф, w→в, ß→с и далее по списку ниже);
  2) РАЗБОР невода на классы — потому что сырое число само по себе не находка:
       • КОГНАТ — похожее слово И ЕСТЬ перевод карточки («культура» при «Kultur»);
         это НЕ дефект, трогать нельзя;
       • ДЕФЕКТ — похожее слово переводом карточки не является, то есть немецкое
         слово просто переписали кириллицей и оставили в «переводе».

Транскрипция здесь ПРИБЛИЖЁННАЯ и намеренно широкая: её задача — не пропустить,
а не угадать. Точность даёт второй шаг.

┌─ ЗАМЕР 30.08.2026. НЕ ПОДНИМАТЬ ЭТО КАК НОВУЮ НАХОДКУ. ────────────────────────┐
│ Живой пул `bt_3_dictionary_entries`: 17 937 записей, 6 726 карточек с          │
│ примерами, 3 246 переводов примеров прошло проверку.                           │
│                                                                                │
│ Невод поймал 51 совпадение. Разложены на классы:                               │
│   45 — КОГНАТЫ: «Pedal ~ педаль», «Soldat ~ солдат», «Diagramm ~ диаграмма».   │
│         Это и есть верный перевод. Дефекта нет.                                │
│    6 — КАНДИДАТЫ, прочитаны глазами ПООДИНОЧКЕ 30.08.2026: planlos→«плана»,    │
│         Knopf→«Кнопка», Pony→«пони», Skript→«скрипт» (дважды). Все шесть —     │
│         НОРМАЛЬНЫЕ РУССКИЕ СЛОВА в нормальных русских предложениях.            │
│                                                                                │
│ ИТОГО НАСТОЯЩИХ ДЕФЕКТОВ ТРАНСЛИТЕРАЦИИ В БАЗЕ: 0.                             │
│                                                                                │
│ ВЫВОД, И ОН ГЛАВНЫЙ: стража на транслитерацию строить НЕЛЬЗЯ. На живых данных  │
│ он не поймал бы ничего и срезал бы 51 верный перевод. Чистить в базе тоже       │
│ нечего.                                                                        │
│                                                                                │
│ ОТКУДА ЖЕ «миллионенфах» НА ЭКРАНЕ ВЛАДЕЛЬЦА. Тот разбор шёл НЕ отсюда: попап  │
│ выделения в видео до 30.08.2026 звал урезанный промпт                          │
│ `dictionary_assistant_multilang_reader` живьём и не писал в пул НИЧЕГО. То     │
│ есть класс жил в удалённом пути, а не в данных словаря. Промпт удалён вместе   │
│ с этим путём.                                                                  │
│                                                                                │
│ Перемерить: railway run --service Postgres python3 scripts/dict_transliteration_audit.py --list 20
└────────────────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter

import psycopg2

CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")
CYR_TOKEN_RE = re.compile(r"[А-Яа-яЁё]{4,}")
LATIN_RE = re.compile(r"[A-Za-zÄÖÜäöüß]")

# ── Немецко-русская практическая транскрипция (Гиляревский–Старостин) ────────
# Порядок правил важен: длинные сочетания идут раньше одиночных букв.
_TRANSCRIPTION_RULES: tuple[tuple[str, str], ...] = (
    ("schsch", "шш"), ("tsch", "ч"), ("sch", "ш"),
    ("chs", "кс"), ("ch", "х"), ("ck", "к"), ("ph", "ф"), ("th", "т"),
    ("qu", "кв"), ("tz", "ц"), ("ss", "с"), ("ß", "с"),
    ("eu", "ой"), ("äu", "ой"), ("ei", "ай"), ("ai", "ай"), ("ie", "и"),
    ("ee", "е"), ("aa", "а"), ("oo", "о"), ("ah", "а"), ("eh", "е"),
    ("oh", "о"), ("uh", "у"), ("ih", "и"),
    ("ä", "э"), ("ö", "ё"), ("ü", "ю"),
    ("a", "а"), ("b", "б"), ("c", "к"), ("d", "д"), ("e", "е"), ("f", "ф"),
    ("g", "г"), ("h", "х"), ("i", "и"), ("j", "й"), ("k", "к"), ("l", "л"),
    ("m", "м"), ("n", "н"), ("o", "о"), ("p", "п"), ("r", "р"), ("s", "с"),
    ("t", "т"), ("u", "у"), ("v", "ф"), ("w", "в"), ("x", "кс"), ("y", "и"),
    ("z", "ц"),
)


def transcribe_de_to_ru(word: str) -> str:
    text = str(word or "").strip().lower()
    if not text:
        return ""
    out: list[str] = []
    i = 0
    while i < len(text):
        for src, dst in _TRANSCRIPTION_RULES:
            if text.startswith(src, i):
                out.append(dst)
                i += len(src)
                break
        else:
            i += 1
    return "".join(out)


def skeleton(word: str) -> str:
    """Огрубление кириллицы: разные написания одного звучания сходятся в одно.

    Транскрипция приближённая, поэтому сравнивать посимвольно бессмысленно:
    «миллионенфах» из базы и «миллионенфах» из правил разойдутся на одной букве.
    Схлопываем удвоения и сводим близкие гласные — невод должен быть широким."""
    text = str(word or "").lower()
    text = text.replace("ё", "е").replace("э", "е").replace("ъ", "").replace("ь", "")
    text = text.replace("й", "и").replace("ы", "и").replace("ю", "у").replace("я", "а")
    text = re.sub(r"(.)\1+", r"\1", text)
    return text


def looks_like_same_word(a: str, b: str) -> bool:
    """Достаточно ли близки два огрублённых слова, чтобы считать их одним."""
    x, y = skeleton(a), skeleton(b)
    if not x or not y or min(len(x), len(y)) < 4:
        return False
    if x == y:
        return True
    shorter, longer = (x, y) if len(x) <= len(y) else (y, x)
    if len(longer) - len(shorter) > 3:
        return False
    # Общее начало почти на всю длину короткого: «культур» ⊂ «культура».
    common = 0
    for ch_a, ch_b in zip(shorter, longer):
        if ch_a != ch_b:
            break
        common += 1
    return common >= max(4, len(shorter) - 1)


def native_half(example: dict) -> str:
    """Русская половина примера — определяем по письму, как это делает продукт."""
    if not isinstance(example, dict):
        return ""
    left = str(example.get("source") or "").strip()
    right = str(example.get("target") or "").strip()
    left_cyr = bool(CYRILLIC_RE.search(left))
    right_cyr = bool(CYRILLIC_RE.search(right))
    if left_cyr == right_cyr:
        return ""  # различить нечем — в замер не берём
    return left if left_cyr else right


def german_headword(row: dict) -> str:
    """Немецкий заголовок карточки: та сторона, что написана латиницей."""
    for value in (row.get("source_text"), row.get("target_text"),
                  row.get("word_de"), row.get("translation_de")):
        text = str(value or "").strip()
        if text and LATIN_RE.search(text) and not CYRILLIC_RE.search(text):
            # Артикль в заголовке транскрипции только мешает.
            return re.sub(r"^(der|die|das)\s+", "", text, flags=re.I).strip()
    return ""


def russian_side(row: dict) -> str:
    for value in (row.get("target_text"), row.get("source_text"),
                  row.get("translation_ru"), row.get("word_ru")):
        text = str(value or "").strip()
        if text and CYRILLIC_RE.search(text):
            return text
    return ""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", type=int, default=0, help="показать N примеров каждого класса")
    args = parser.parse_args()

    dsn = os.environ.get("DATABASE_PUBLIC_URL") or os.environ.get("DATABASE_URL")
    if not dsn:
        print("Нет DATABASE_PUBLIC_URL. Запускать так:")
        print("  railway run --service Postgres python3 scripts/dict_transliteration_audit.py")
        sys.exit(2)

    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM bt_3_dictionary_entries")
    total_rows = cur.fetchone()[0]
    cur.execute(
        """
        SELECT id, source_text, target_text, word_de, translation_de,
               word_ru, translation_ru, response_json->'usage_examples'
        FROM bt_3_dictionary_entries
        WHERE jsonb_typeof(response_json->'usage_examples') = 'array'
          AND jsonb_array_length(response_json->'usage_examples') > 0
        """
    )
    rows = cur.fetchall()

    stats = Counter()
    defects: list[tuple] = []
    cognates: list[tuple] = []

    for (row_id, source_text, target_text, word_de, translation_de,
         word_ru, translation_ru, examples) in rows:
        row = {
            "source_text": source_text, "target_text": target_text,
            "word_de": word_de, "translation_de": translation_de,
            "word_ru": word_ru, "translation_ru": translation_ru,
        }
        stats["карточек с примерами"] += 1
        head_de = german_headword(row)
        if not head_de or " " in head_de:
            stats["пропущено: заголовок не одно немецкое слово"] += 1
            continue
        transcribed = transcribe_de_to_ru(head_de)
        if len(skeleton(transcribed)) < 4:
            stats["пропущено: слишком короткое слово"] += 1
            continue
        ru_side = russian_side(row)
        ru_words = CYR_TOKEN_RE.findall(ru_side)

        for example in (examples or []):
            native = native_half(example)
            if not native:
                continue
            stats["проверено примеров"] += 1
            for token in CYR_TOKEN_RE.findall(native):
                if not looks_like_same_word(token, transcribed):
                    continue
                # Похожее слово И ЕСТЬ перевод карточки → это когнат, а не дефект.
                if any(looks_like_same_word(token, w) for w in ru_words):
                    stats["КОГНАТ (не дефект)"] += 1
                    cognates.append((row_id, head_de, token, native))
                else:
                    stats["КАНДИДАТ (читать глазами, замер 30.08: все были верны)"] += 1
                    defects.append((row_id, head_de, transcribed, token, native))
                break

    print()
    print("ЗАМЕР ТРАНСЛИТЕРАЦИИ В ПЕРЕВОДАХ ПРИМЕРОВ")
    print("=" * 64)
    print(f"всего записей в общем пуле: {total_rows}")
    for key in ("карточек с примерами", "проверено примеров",
                "пропущено: заголовок не одно немецкое слово",
                "пропущено: слишком короткое слово",
                "КОГНАТ (не дефект)", "КАНДИДАТ (читать глазами, замер 30.08: все были верны)"):
        print(f"{key:<48} {stats[key]}")

    if args.list:
        print()
        print("─ КАНДИДАТЫ: каждого прочесть глазами, это НЕ приговор ─" + "─" * 8)
        for row_id, head, transcribed, token, native in defects[: args.list]:
            print(f"#{row_id} {head} → ожидали «{transcribed}», в переводе «{token}»")
            print(f"    {native[:150]}")
        print()
        print("─ КОГНАТЫ (трогать нельзя) ─" + "─" * 36)
        for row_id, head, token, native in cognates[: args.list]:
            print(f"#{row_id} {head} ~ «{token}» — это и есть перевод")
            print(f"    {native[:150]}")


if __name__ == "__main__":
    main()
