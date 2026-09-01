# -*- coding: utf-8 -*-
"""Разовая заливка уже полученных ответов DWDS в кеш `bt_3_dwds_frequency`.

Зачем: 01.09.2026 все 338 слов банка ребусов были прогнаны через DWDS вручную
(разбор — `docs/tasks/rebus_words_frequency.md`). Заливаем результат, чтобы страж
на входе (`backend/rebus_word_gate.py`) не ходил спрашивать то же самое второй раз
и не нагружал чужой бесплатный сервис.

Формат входного файла — то, что отдаёт https://www.dwds.de/api/frequency/:
    {"Eieruhr": {"hits": 6393, "total": 53303287841, "band": 1, "lemma": "Eieruhr"}}
Записи с ключом "error" НЕ заливаются: «не спросили» и «ноль вхождений» — разные
вещи, и смешивать их нельзя.

    DATABASE_URL="…" python3 scripts/dwds_frequency_seed.py путь/к/dwds_hits.json
"""
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main() -> int:
    if len(sys.argv) < 2:
        print("укажи файл с ответами DWDS", file=sys.stderr)
        return 2
    if not str(os.getenv("DATABASE_URL") or "").strip():
        print("DATABASE_URL не задан", file=sys.stderr)
        return 2

    from backend.database import upsert_dwds_frequency
    from backend.dwds_frequency import per_billion

    data = json.loads(Path(sys.argv[1]).read_text())
    written = skipped = 0
    for word, row in sorted(data.items()):
        if "hits" not in row:
            skipped += 1
            continue
        upsert_dwds_frequency({
            "word": word,
            "hits": int(row["hits"]),
            "total": int(row["total"]),
            "band": int(row.get("band") or 0),
            "lemma": str(row.get("lemma") or ""),
            "per_billion": per_billion(int(row["hits"]), int(row["total"])),
        })
        written += 1
    print(f"залито: {written}, пропущено (нет ответа): {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
