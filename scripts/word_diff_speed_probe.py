# -*- coding: utf-8 -*-
"""Сколько стоит показать сравнение слов: разложено по шагам, на живых данных.

Зачем этот файл
───────────────
27.08.2026 владелец спросил: «почему уже разобранная пара отображается так долго?»
Ответ оказался не «модель медленная», а порядок действий: проверка общей полки
стояла НИЖЕ сбора статей по словам. Готовая пара доставалась из базы за 0,7 c, но
чтобы до неё дойти, человек ждал, пока обе статьи соберутся заново.

Скрипт ТОЛЬКО МЕРЯЕТ и ничего не чинит. Он отвечает на два вопроса:
  1) лежит ли эта пара в общей полке и за сколько она достаётся;
  2) во что обходится подготовка источников по каждому слову — и становится ли
     повтор дешевле (если нет, значит работа никуда не сохраняется).

Второй вопрос требует настоящих обращений к модели, поэтому прогон платный.

    python3 scripts/word_diff_speed_probe.py entscheiden beschließen
    python3 scripts/word_diff_speed_probe.py --repeats 3 gehen laufen
    python3 scripts/word_diff_speed_probe.py --shelf-only entscheiden beschließen

Замер 27.08.2026 на entscheiden · beschließen (для сверки при следующем прогоне):
    полка: НАЙДЕНО за 0,7 c
    beschließen  0,93 → 0,98 → 0,99 c   своя единица, карточка полная
    entscheiden  17,7 → 23,0 → 15,8 c   своей единицы нет, разбор не сохраняется
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.database import (                                        # noqa: E402
    WORD_DIFF_SCHEMA_VERSION,
    build_word_diff_pair_key,
    get_db_connection_context,
    get_lex_unit_card,
    get_word_diff_card,
    get_word_readings,
    get_word_usage,
)
from backend.dictionary_entries import entries_for_query               # noqa: E402
from backend.backend_server import (                                   # noqa: E402
    _word_diff_card_is_thin,
    _word_diff_legit_entries,
    _word_diff_lookup_sources,
    _word_diff_pick_entry,
)


def _shelf(words: list[str], studied: str, explain: str) -> None:
    key = build_word_diff_pair_key(words, studied, explain)
    print(f"Ключ пары: {key!r}")
    started = time.monotonic()
    card = get_word_diff_card(key, bump_open=False, any_version=False)
    took = time.monotonic() - started
    if card:
        print(f"  В полке: ЕСТЬ, версия схемы текущая ({WORD_DIFF_SCHEMA_VERSION}), "
              f"достаётся за {took:.2f} c")
        print(f"  Собрана: {card.get('created_at')}")
    else:
        stale = get_word_diff_card(key, bump_open=False, any_version=True)
        if stale:
            print(f"  В полке: есть СТАРАЯ версия — платному человеку она не отдаётся, "
                  f"пара будет разобрана заново ({took:.2f} c)")
        else:
            print(f"  В полке: НЕТ — это новая пара, её и правда надо разбирать ({took:.2f} c)")

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT split_part(pair_key, '#', 1) AS base, COUNT(*)
                FROM bt_3_word_diff_cards
                GROUP BY 1 ORDER BY 2 DESC LIMIT 5;
            """)
            rows = cur.fetchall() or []
    print(f"  Всего в полке пар (по основному ключу): {len(rows)} верхних показано")
    for base, count in rows:
        print(f"     {base}  ×{count}")


def _why_slow(word: str, studied: str, explain: str) -> None:
    """Из чего складывается подготовка ОДНОГО слова. Без обращений к модели."""
    entries = entries_for_query(word, source_lang=studied, target_lang=explain)
    usable = _word_diff_legit_entries(word, entries, studied)
    entry = _word_diff_pick_entry(word, entries, studied)
    if not entry:
        print(f"     прочтений годных: {len(usable)} — человека спросят, что он имел в виду")
        return
    unit_id = int(entry.get("unit_id") or 0)
    pos = str(entry.get("pos") or "")
    card = get_lex_unit_card(unit_id) if unit_id else None
    thin = _word_diff_card_is_thin(card)
    readings = get_word_readings(word, lang=studied, explain_lang=explain)
    usage = get_word_usage(word, lang=studied, explain_lang=explain, pos=pos)
    print(f"     наша единица: {unit_id or 'НЕТ'}   часть речи: {pos or '—'}")
    print(f"     прочтения в базе: {'есть' if readings is not None else 'НЕТ → вопрос модели'}")
    print(f"     картина употребления: {'есть' if usage else 'НЕТ → вопрос модели'}")
    if not unit_id:
        print("     ⛔ статья не из наших единиц: разбор соберётся заново и НИКУДА "
              "не сохранится — так будет при каждом обращении")
    elif thin:
        print("     ⚠️  карточка бедная: разбор соберётся сейчас и ляжет в единицу — "
              "в следующий раз будет быстро")
    else:
        print("     ✅ статья готова, модель не нужна")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("words", nargs="+", help="слова пары, как их вводит человек")
    parser.add_argument("--studied", default="de")
    parser.add_argument("--explain", default="ru")
    parser.add_argument("--repeats", type=int, default=3,
                        help="сколько раз подряд готовить источники (повтор обязан дешеветь)")
    parser.add_argument("--shelf-only", action="store_true",
                        help="только полка, без обращений к модели и без трат")
    args = parser.parse_args()

    words = [" ".join(str(w).split()) for w in args.words if str(w).strip()]
    if len(words) < 2:
        print("Нужно хотя бы два слова.")
        return 2

    print("── Общая полка разборов ─────────────────────────────────────────────")
    _shelf(words, args.studied, args.explain)

    print("\n── Из чего складывается подготовка каждого слова ─────────────────────")
    for word in words:
        print(f"  {word}:")
        _why_slow(word, args.studied, args.explain)

    if args.shelf_only:
        print("\n(--shelf-only: обращения к модели пропущены)")
        return 0

    print(f"\n── Подготовка источников, {args.repeats} прогона подряд ────────────────")
    print("   Если повтор НЕ дешевеет — работа никуда не сохраняется.")
    for word in words:
        times = []
        outcome = ""
        for _ in range(max(1, args.repeats)):
            started = time.monotonic()
            try:
                result = _word_diff_lookup_sources(word, args.studied, args.explain)
            except Exception as exc:  # noqa: BLE001
                print(f"   {word}: упало — {exc}")
                outcome = "ошибка"
                break
            times.append(time.monotonic() - started)
            if isinstance(result, dict) and result.get("needs_choice"):
                outcome = "спросят прочтение"
            elif result:
                outcome = f"статья, источник {result.get('source')!r}"
            else:
                outcome = "слово не нашли"
        if times:
            line = " → ".join(f"{t:.2f} c" for t in times)
            verdict = ""
            if len(times) > 1 and min(times[1:]) > times[0] * 0.5:
                verdict = "   ⛔ повтор НЕ дешевеет"
            print(f"   {word}: {line}   ({outcome}){verdict}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
