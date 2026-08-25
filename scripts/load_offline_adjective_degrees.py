# -*- coding: utf-8 -*-
"""Загрузить офлайн-таблицу степеней сравнения из UniMorph.

ЗАЧЕМ. Степени сравнения у нас спрашивались у de.wiktionary ПО ОДНОМУ СЛОВУ через сеть,
и в базе их было 945 против 89 704 таблиц склонения. Отказ по частоте при этом неотличим
от «слова нет» — на существительных я на этом уже дважды обжигался.

ЧТО ГРУЗИМ. UniMorph немецкий (github.com/unimorph/deu, CC BY-SA 3.0): 519 143 строки,
39 373 леммы. Нас интересуют строки `ADJ;CMPR` и `ADJ;SPRL` — 4 838 прилагательных,
у которых есть ОБЕ степени.

СВЕРЕНО ДО ЗАГРУЗКИ, и результат хуже, чем был у существительных:

    сошлось     446
    разошлось     8
    нет в UniMorph 475 (из наших 945)

И в этих восьми ПРАВЫ МЫ, а не UniMorph:

    ausgeglichen   у нас «ausgeglichener»  UniMorph «ausgeglichner»
    doof           у нас «doofer»          UniMorph «dööfer»
    düster         у нас «düsterer»        UniMorph «düstrer»
    schmal         у нас «schmaler»        UniMorph «schmäler»
    verlässlich    у нас «verlässlicher»   UniMorph «verlässlich» — вообще не степень

Наши значения — обычные формы, у UniMorph редкие и устаревшие варианты. Поэтому правило
жёсткое: СВОЁ НЕ ПЕРЕЗАПИСЫВАЕМ НИКОГДА, грузим только то, чего у нас нет. Подпись
источника `unimorph` ставится, чтобы происхождение было видно и чтобы ночной прогрев мог
позже заменить его чтением со страницы справочника.

ЗАПУСК:
    python3 scripts/load_offline_adjective_degrees.py --tsv <путь к deu>
    python3 scripts/load_offline_adjective_degrees.py --tsv <путь> --apply
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main() -> int:
    apply = "--apply" in sys.argv
    путь = sys.argv[sys.argv.index("--tsv") + 1] if "--tsv" in sys.argv else ""
    if not путь or not os.path.exists(путь):
        print("Нужен путь к файлу UniMorph: --tsv <путь>")
        return 1

    from backend.database import get_db_connection_context

    степени: dict[str, dict] = {}
    with open(путь, encoding="utf-8") as f:
        for строка in f:
            части = строка.rstrip("\n").split("\t")
            if len(части) != 3:
                continue
            лемма, форма, теги = части
            if теги == "ADJ;CMPR":
                степени.setdefault(лемма, {})["comparative"] = форма
            elif теги == "ADJ;SPRL":
                степени.setdefault(лемма, {})["superlative"] = форма

    полные = {л: v for л, v in степени.items()
              if v.get("comparative") and v.get("superlative")}
    print(f"в UniMorph прилагательных с ОБЕИМИ степенями: {len(полные)}")

    with get_db_connection_context() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT lower(adjective) FROM bt_3_german_adjective_degrees")
            уже_есть = {r[0] for r in cur.fetchall()}

    новые = {л: v for л, v in полные.items() if л.lower() not in уже_есть}
    print(f"у нас уже есть: {len(уже_есть)}")
    print(f"ДОБАВИТСЯ:      {len(новые)}")
    for л, v in list(новые.items())[:5]:
        print(f"    {л:22} {v['comparative']:22} {v['superlative']}")
    if not apply:
        print("\nЭто показ. Чтобы загрузить — добавь --apply")
        return 0

    from psycopg2.extras import execute_values

    # Порциями со своим коммитом: один общий коммит на 4 тысячи строк уже подводил —
    # соединение через прокси рвётся, и всё откатывается целиком.
    пары = [(л, json.dumps({**v, "positive": л, "gradable": True, "source": "unimorph"},
                           ensure_ascii=False))
            for л, v in новые.items()]
    записано = 0
    for начало in range(0, len(пары), 1000):
        порция = пары[начало:начало + 1000]
        try:
            with get_db_connection_context() as conn:
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        "INSERT INTO bt_3_german_adjective_degrees "
                        "(adjective, degrees, documented, checked_at) VALUES %s "
                        "ON CONFLICT (adjective) DO NOTHING;",
                        порция, template="(%s, %s::jsonb, TRUE, NOW())", page_size=500)
                conn.commit()
            записано += len(порция)
            print(f"  закреплено {записано} из {len(пары)}")
        except Exception as exc:
            print(f"  порция с {начало} не прошла: {str(exc)[:70]}")

    with get_db_connection_context() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM bt_3_german_adjective_degrees")
            print(f"\nЗагружено {записано} · степеней сравнения в базе: {cur.fetchone()[0]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
