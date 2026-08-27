# -*- coding: utf-8 -*-
"""Перепроверить отказы справочника: «страницы нет» — незакрытая задача, а не приговор.

ЗАЧЕМ. Быстрый ночной путь (`warm_from_source_bulk`) пропускает любое слово, у которого
в кэше форм УЖЕ есть строка, — неважно, стоит она ответом или отказом. Поэтому один
вопрос, заданный не по тому адресу, закрывал слово от системы навсегда.

Так и вышло 23.08.2026: прогон спросил справочник про «gehen», «vier», «wenn»,
«schwimmen» со СТРОЧНОЙ буквы. По этим адресам напечатаны глагол, числительное и союз,
таблицы существительного там нет — и в кэш легло «страницы нет». Через два дня «дверь
слова» переименовала слова в «Gehen», «Vier», но ключ кэша хранится в нижнем регистре,
и отказ остался. 27.08.2026 эти слова ушли владельцу как «форм нет нигде», хотя
справочник печатает их таблицы целиком: das Gehen / des Gehens, die Vier / der Vieren.

Правило регистра починено в `german_reference_forms._reference_title`, ночная работа
теперь возвращается к отказам сама (`recheck_negatives` внутри `warm_nightly`). Этот
скрипт — разовая уборка того, что уже лежит в базе.

ЗАПУСК (нужен доступ к живой базе):
    railway run --service Postgres python scripts/forms_recheck_negatives.py
    railway run --service Postgres python scripts/forms_recheck_negatives.py --apply
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main() -> int:
    apply = "--apply" in sys.argv
    from backend.database import get_db_connection_context
    from backend.german_reference_forms import (
        _reference_title,
        fetch_sources_bulk,
        forms_from_source,
        recheck_negatives,
    )

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT noun, 'noun' FROM bt_3_german_noun_declensions WHERE NOT documented
                UNION ALL
                SELECT adjective, 'adjective' FROM bt_3_german_adjective_degrees
                 WHERE NOT documented
                 ORDER BY 2, 1
            """)
            отказы = [(str(a), str(b)) for a, b in (cur.fetchall() or [])]

    print(f"Отказов в кэше форм: {len(отказы)}")
    if not отказы:
        return 0

    if apply:
        # Ночная перепроверка ждёт неделю; разовой уборке ждать нечего.
        import backend.german_reference_forms as R
        R._RECHECK_NEGATIVE_AFTER_DAYS = 0
        отчёт = recheck_negatives(limit=len(отказы))
        print("Перепроверка:", отчёт)
        return 0

    закроются, останутся = [], []
    for start in range(0, len(отказы), 50):
        chunk = отказы[start:start + 50]
        sources = fetch_sources_bulk([_reference_title(w, p) for w, p in chunk])
        if sources is None:
            print("  справочник молчит — повторите позже")
            return 1
        for word, pos in chunk:
            text = sources.get(_reference_title(word, pos)) or sources.get(word) or ""
            # То же самое правило, что и у записи, — не «похожее».
            (закроются if forms_from_source(pos, text) else останутся).append((word, pos))

    print(f"\nСПРАВОЧНИК ЗАКРОЕТ СЕЙЧАС: {len(закроются)}")
    for word, pos in закроются:
        print(f"   {pos:10} {word}  →  спрашивали как «{_reference_title(word, pos)}»")
    print(f"\nОСТАНУТСЯ НЕПОКРЫТЫМИ: {len(останутся)}")
    for word, pos in останутся:
        print(f"   {pos:10} {word}")
    print("\nЭто показ. Чтобы записать ответы — добавь --apply")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
