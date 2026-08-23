# -*- coding: utf-8 -*-
"""Загрузить к себе офлайн-таблицу склонений на 100 000 существительных.

ЗАЧЕМ. Сейчас склонение спрашивается у de.wiktionary ПО ОДНОМУ СЛОВУ через сеть.
У этого пути два изъяна, и оба вылезли 23.08.2026 в один день:

    отказ по частоте неотличим от «слова нет» — прогон объявил неизвестными
    «Ratte», «Scherbe», «Rivalität», «Verbindlichkeit», которые справочник знает;

    ночная порция всего 120 слов, потому что дальше начинается отказ, — новое слово
    ждёт таблицу до нескольких ночей.

ЧТО ГРУЗИМ. Пакет `german-nouns` (CC BY-SA 4.0): 99 938 существительных, у каждого
все четыре падежа в единственном и множественном. Данные собраны из того же
de.wiktionary, что и наш справочник, — то есть это не второе мнение, а та же правда,
только скачанная целиком и разом.

СВЕРЕНО ДО ЗАГРУЗКИ, а не после. Из 2162 слов, по которым у нас уже есть таблица от
Wiktionary, скачанная сходится с нашей в 2161. Единственное расхождение — «hammer» со
строчной буквы: у нас там артефакт, а не слово.

ЧЕГО ЭТО НЕ ЗАКРЫВАЕТ. 488 наших существительных в скачанной таблице отсутствуют — это
составные слова вроде «Umschaltsituation». Их бесконечно много, и ни одна выгрузка мира
их не содержит; это отдельная задача.

ПРОИСХОЖДЕНИЕ СОХРАНЯЕТСЯ. У каждой загруженной таблицы стоит подпись
`source: german-nouns`. Уже имеющиеся записи НЕ ПЕРЕЗАПИСЫВАЮТСЯ: то, что мы прочитали
со страницы справочника сами, остаётся как есть.

ЗАПУСК:
    python3 scripts/load_offline_noun_tables.py --csv <путь>            # показать
    python3 scripts/load_offline_noun_tables.py --csv <путь> --apply    # загрузить
"""
from __future__ import annotations

import csv
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_РОД = {"m": "der", "f": "die", "n": "das"}
_АРТИКЛЬ_ЕД = {
    "m": {"nom": "der", "gen": "des", "dat": "dem", "akk": "den"},
    "f": {"nom": "die", "gen": "der", "dat": "der", "akk": "die"},
    "n": {"nom": "das", "gen": "des", "dat": "dem", "akk": "das"},
}
_АРТИКЛЬ_МН = {"nom": "die", "gen": "der", "dat": "den", "akk": "die"}
_ПОДПИСЬ = {"nom": "Nominativ", "gen": "Genitiv", "dat": "Dativ", "akk": "Akkusativ"}
_КОЛОНКА = {"nom": "nominativ", "gen": "genitiv", "dat": "dativ", "akk": "akkusativ"}


def _значение(строка: dict, падеж: str, число: str) -> str:
    """Форма из строки таблицы. Пробуем основную колонку, потом нумерованные варианты.

    Нумерация в источнике означает омографы и слова с двумя парадигмами: у «Kiefer»
    два рода, у «Band» два множественных. Берём первый вариант — он же основной.
    """
    основа = f"{_КОЛОНКА[падеж]} {число}"
    for проба in (основа, f"{основа} 1", f"{основа} 2", f"{основа}*"):
        значение = str(строка.get(проба) or "").strip()
        if значение:
            return значение
    return ""


def _в_наш_формат(строка: dict) -> dict | None:
    """Строка скачанной таблицы → наша структура {'m': {...}} с артиклями."""
    род = str(строка.get("genus") or строка.get("genus 1") or "").strip().lower()
    if род not in _РОД:
        return None
    ряды = []
    есть_мн = False
    for падеж in ("nom", "gen", "dat", "akk"):
        ед = _значение(строка, падеж, "singular")
        мн = _значение(строка, падеж, "plural")
        if not ед and not мн:
            continue
        ряд = {"case": падеж, "label": _ПОДПИСЬ[падеж]}
        if ед:
            ряд["singular"] = f"{_АРТИКЛЬ_ЕД[род][падеж]} {ед}"
        if мн:
            есть_мн = True
            ряд["plural"] = f"{_АРТИКЛЬ_МН[падеж]} {мн}"
        ряды.append(ряд)
    if not ряды:
        return None
    return {род: {"rows": ряды, "has_plural": есть_мн,
                  "has_singular": bool(_значение(строка, "nom", "singular"))},
            "source": "german-nouns"}


def main() -> int:
    apply = "--apply" in sys.argv
    путь = None
    if "--csv" in sys.argv:
        путь = sys.argv[sys.argv.index("--csv") + 1]
    if not путь or not os.path.exists(путь):
        print("Нужен путь к nouns.csv: --csv <путь>")
        return 1

    from backend.database import get_db_connection_context

    готовые = {}
    пропущено = 0
    with open(путь, encoding="utf-8") as f:
        for строка in csv.DictReader(f):
            лемма = str(строка.get("lemma") or "").strip()
            if not лемма or " " in лемма or лемма.startswith("-"):
                пропущено += 1
                continue
            таблица = _в_наш_формат(строка)
            if not таблица:
                пропущено += 1
                continue
            готовые.setdefault(лемма, таблица)

    with get_db_connection_context() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT lower(noun) FROM bt_3_german_noun_declensions;")
            уже_есть = {r[0] for r in (cur.fetchall() or [])}

    новые = {л: т for л, т in готовые.items() if л.lower() not in уже_есть}
    print(f"В скачанной таблице строк:      {len(готовые) + пропущено}")
    print(f"  пригодных (есть род и формы): {len(готовые)}")
    print(f"  пропущено (нет рода, обрывки):{пропущено}")
    print(f"Уже есть у нас:                 {len(уже_есть)}")
    print(f"ДОБАВИТСЯ НОВЫХ:                {len(новые)}")
    примеры = list(новые.items())[:3]
    for л, т in примеры:
        род = next(k for k in т if k != "source")
        print(f"    {л}: {т[род]['rows'][0].get('singular')} · мн. "
              f"{т[род]['rows'][0].get('plural') or '—'}")
    if not apply:
        print("\nЭто показ. Чтобы загрузить — добавь --apply")
        return 0

    # ПОРЦИЯМИ, И КАЖДАЯ СО СВОИМ КОММИТОМ. Первый заход коммитил один раз в конце —
    # соединение через прокси оборвалось на середине, и 87 тысяч строк откатились
    # целиком. Теперь порция закрепляется сразу: обрыв стоит одной порции, а повторный
    # запуск досыпает остальное (ON CONFLICT DO NOTHING делает прогон безопасным).
    from psycopg2.extras import execute_values

    записано = 0
    пары = [(л, json.dumps(т, ensure_ascii=False)) for л, т in новые.items()]
    РАЗМЕР = 2000
    for начало_порции in range(0, len(пары), РАЗМЕР):
        порция = пары[начало_порции:начало_порции + РАЗМЕР]
        try:
            with get_db_connection_context() as conn:
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        "INSERT INTO bt_3_german_noun_declensions "
                        "(noun, tables, documented, checked_at) VALUES %s "
                        "ON CONFLICT (noun) DO NOTHING;",
                        порция,
                        template="(%s, %s::jsonb, TRUE, NOW())",
                        page_size=500,
                    )
                conn.commit()
            записано += len(порция)
            print(f"  закреплено {записано} из {len(пары)}")
        except Exception as exc:
            # Обрыв на порции — не повод потерять уже закреплённое. Говорим и идём дальше.
            print(f"  ПОРЦИЯ НЕ ПРОШЛА (с {начало_порции}): {str(exc)[:80]}")

    # Проверка ФАКТОМ: спрашиваем базу заново.
    with get_db_connection_context() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM bt_3_german_noun_declensions;")
            всего = cur.fetchone()[0]
    print(f"\nЗагружено: {записано} · таблиц склонения в базе теперь: {всего}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
