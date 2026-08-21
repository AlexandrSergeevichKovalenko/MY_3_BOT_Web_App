# -*- coding: utf-8 -*-
"""Починить разборы, в которых текст размножен сам на себя.

ЗАЧЕМ. 16.08.2026 разовый скрипт применил одну замену шесть раз подряд к уже
заменённому тексту («sterile Gaze» → «sterile Gazennnnnn»). Заголовки слов и карточки
людей уже вычищены, а РАЗБОР — примеры, устойчивые сочетания, объяснения — нет: там
защиты не было вовсе. Замер 21.08.2026 по всем 10454 разборам живой базы нашёл один
уцелевший случай.

Дыра закрыта в коде: `lex_units.save_unit_card` и ночной добор синонимов больше не
записывают размноженный текст. Признак порчи берётся из общего модуля
`backend.mangled_text` — там он один на всё приложение, и своего второго заводить
нельзя: тема открывалась четыре раза подряд ровно потому, что каждый заход придумывал
себе новый признак. Этот скрипт делает вторую половину — чинит то, что уже лежит.

КАК ЧИНИТ. Хвост из повторённой буквы срезается до одной: «Gazennnnnn» → «Gazen»?
НЕТ. Правильное написание восстанавливается по ЗАГОЛОВКУ самого слова — он уже
вычищен и проверен, и брать основу из него честнее, чем угадывать длину хвоста.
Кусок, который заголовком не восстанавливается, не трогается и уходит в отчёт: тихо
подчистить наугад значило бы то же самое выдумывание.

ЗАПУСК:
    python3 scripts/card_repair_multiplied_text.py           # показать
    python3 scripts/card_repair_multiplied_text.py --apply   # починить
"""
from __future__ import annotations

import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# НИ ОДНОЙ СВОЕЙ РЕГУЛЯРКИ. Признак порчи и свёртка берутся из `backend.mangled_text`
# — там они одни на всё приложение. Своя копия здесь и была бы тем самым «новым
# признаком», из-за которого тема открывалась четыре раза подряд.
from backend.mangled_text import MANGLED_LETTER_INSIDE as _ХВОСТ  # noqa: E402
from backend.mangled_text import collapse_multiplied  # noqa: E402


def _починить_строку(текст: str, основы: set[str]) -> tuple[str, list[str]]:
    """(починенный текст, что не удалось починить)."""
    не_смогли: list[str] = []

    def замена(m: re.Match) -> str:
        начало, буква = m.group(1), m.group(2)
        # Заголовок слова — источник правильного написания. Ищем среди основ ту,
        # что начинается с найденного куска: «Gaze» для «Gazennnnnn».
        подходят = [o for o in основы
                    if o.lower().startswith(начало.lower()) and len(o) >= len(начало)]
        if len(подходят) == 1:
            return подходят[0]
        не_смогли.append(m.group(0))
        return m.group(0)

    # Хвост из буквы восстанавливается по свидетелю (заголовку), повтор слова и знака
    # сворачивается общей функцией: там вариант ровно один — механизм порчи доказан.
    промежуточно = _ХВОСТ.sub(замена, текст)
    return collapse_multiplied(промежуточно), не_смогли


def _пройти(value, основы, не_смогли):
    if isinstance(value, str):
        новое, плохо = _починить_строку(value, основы)
        не_смогли.extend(плохо)
        return новое
    if isinstance(value, dict):
        return {k: _пройти(v, основы, не_смогли) for k, v in value.items()}
    if isinstance(value, list):
        return [_пройти(v, основы, не_смогли) for v in value]
    return value


def main() -> int:
    apply = "--apply" in sys.argv
    from backend.mangled_text import mangled_strings_inside as найти_порчу
    from backend.database import get_db_connection_context

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, display, lemma, card FROM bt_3_lex_units "
                        "WHERE card IS NOT NULL;")
            rows = cur.fetchall() or []

    план = []
    for unit_id, display, lemma, card in rows:
        порча = найти_порчу(card)
        if not порча:
            continue
        # Основы для восстановления: слова из заголовка этой самой единицы.
        основы = set()
        for текст in (display or "", lemma or ""):
            основы |= {w for w in re.findall(r"\w+", текст, re.UNICODE) if len(w) >= 3}
        не_смогли: list[str] = []
        новый = _пройти(card, основы, не_смогли)
        план.append((int(unit_id), str(display), порча, новый, не_смогли))

    print(f"Разборов в базе: {len(rows)}")
    print(f"С размноженным текстом: {len(план)}")
    for unit_id, display, порча, _новый, не_смогли in план:
        print(f"\n  {unit_id}  {display!r}")
        for кусок in порча:
            print(f"        было:  {кусок!r}")
        if не_смогли:
            print(f"        НЕ ЧИНИТСЯ заголовком: {не_смогли}")
    починено = 0
    if apply:
        for unit_id, display, _порча, новый, не_смогли in план:
            if не_смогли:
                print(f"  {unit_id} {display!r} пропущен: заголовком не восстанавливается")
                continue
            with get_db_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE bt_3_lex_units SET card=%s::jsonb, updated_at=NOW() "
                                "WHERE id=%s;", (json.dumps(новый, ensure_ascii=False), unit_id))
                conn.commit()
            починено += 1
        print(f"\nПочинено разборов на словах: {починено}")

    # ── Вторая половина: карточки людей ──────────────────────────────────────
    # Свидетель здесь — колонка `word_de` той же строки: она уже вычищена, и порча
    # 16.08 её не касалась. Чиним внутренность карточки только до того вида, который
    # свидетель подтверждает; всё прочее оставляем и показываем.
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, word_de, response_json FROM bt_3_webapp_dictionary_queries "
                        "WHERE response_json IS NOT NULL;")
            карточки = cur.fetchall() or []
    план_карточек = []
    for row_id, word_de, payload in карточки:
        if not найти_порчу(payload):
            continue
        основы = {w for w in re.findall(r"\w+", str(word_de or ""), re.UNICODE) if len(w) >= 3}
        не_смогли: list[str] = []
        новый = _пройти(payload, основы, не_смогли)
        осталось = найти_порчу(новый)
        план_карточек.append((int(row_id), str(word_de), новый, осталось))

    print(f"\nКарточек людей с размноженным текстом: {len(план_карточек)}")
    for row_id, word_de, _новый, осталось in план_карточек:
        метка = "ПОЧИНИТСЯ" if not осталось else f"НЕ ЧИНИТСЯ: {осталось[:1]}"
        print(f"  {row_id}  {word_de!r}  {метка}")
    if apply:
        for row_id, word_de, новый, осталось in план_карточек:
            if осталось:
                continue
            with get_db_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE bt_3_webapp_dictionary_queries "
                                "SET response_json=%s::jsonb, updated_at=NOW() WHERE id=%s;",
                                (json.dumps(новый, ensure_ascii=False), row_id))
                conn.commit()
    if not apply:
        print("\nЭто показ. Чтобы починить — добавь --apply")
        return 0

    # Проверка ФАКТОМ: спрашиваем базу заново тем же стражем, а не верим намерению.
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, display, card FROM bt_3_lex_units WHERE card IS NOT NULL;")
            хвост_слов = [(i, d) for i, d, c in cur.fetchall() if найти_порчу(c)]
            cur.execute("SELECT id, word_de, response_json FROM bt_3_webapp_dictionary_queries "
                        "WHERE response_json IS NOT NULL;")
            хвост_карточек = [(i, w) for i, w, r in cur.fetchall() if найти_порчу(r)]
    print(f"\nОсталось с порчей: разборов {len(хвост_слов)}, карточек {len(хвост_карточек)}")
    for x in (хвост_слов + хвост_карточек)[:10]:
        print("   ", x)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
