"""Насколько пересказ ролика совпадает с субтитрами дословно.

Зачем: владелец 02.09.2026 запретил отдавать людям субтитры в исходном виде — текст
обязан быть пересказом. На словах в задании модели это не удержать, поэтому в
backend/video_reader_text.py стоят два механических порога. Этот скрипт — способ их
ПЕРЕМЕРИТЬ, а не поверить на слово: и на уже собранных текстах, и на новых.

    DATABASE_URL=... python3 scripts/video_text_overlap.py              # все собранные
    DATABASE_URL=... python3 scripts/video_text_overlap.py <video_id>   # один ролик

Что показывает:
    доля  — сколько двенадцатисловных окон пересказа встречаются в субтитрах слово
            в слово. Главный признак копии: замер 02.09.2026 дал 72% у «вычищенных»
            субтитров и 0–12% у честного пересказа.
    кусок — самый длинный дословный кусок, слов подряд. 119 у копии, 7–30 у пересказа.

Строки, вылезающие за пороги, помечаются ⚠️ — это тексты, которые надо пересобрать.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.video_reader_text import (  # noqa: E402
    MAX_VERBATIM_OVERLAP,
    MAX_VERBATIM_RUN,
    longest_verbatim_run,
    transcript_to_text,
    verbatim_overlap,
)


def main() -> int:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("Нужен DATABASE_URL", file=sys.stderr)
        return 2
    only_video = (sys.argv[1].strip() if len(sys.argv) > 1 else "")

    import psycopg2

    conn = psycopg2.connect(dsn)
    with conn.cursor() as cursor:
        if only_video:
            cursor.execute(
                "SELECT video_id, mode, content_text FROM bt_3_video_reader_texts "
                "WHERE status = 'ready' AND video_id = %s;", (only_video,))
        else:
            cursor.execute(
                "SELECT video_id, mode, content_text FROM bt_3_video_reader_texts "
                "WHERE status = 'ready' ORDER BY updated_at DESC;")
        rows = cursor.fetchall()

    if not rows:
        print("Собранных пересказов нет.")
        return 0

    print(f"пороги: доля ≤ {round(MAX_VERBATIM_OVERLAP * 100)}%, кусок ≤ {MAX_VERBATIM_RUN} слов")
    print()
    print(f"{'ролик':<14} {'режим':<10} {'доля':>7} {'кусок':>7}")
    bad = 0
    for video_id, mode, content_text in rows:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT items FROM bt_3_youtube_transcripts WHERE video_id = %s;", (video_id,))
            row = cursor.fetchone()
        if not row:
            print(f"{video_id:<14} {str(mode or ''):<10}   субтитров в кеше уже нет — сверить не с чем")
            continue
        items = row[0]
        if isinstance(items, str):
            items = json.loads(items)
        source = transcript_to_text(items or [])
        share = verbatim_overlap(source, content_text or "")
        run = longest_verbatim_run(source, content_text or "")
        over = share > MAX_VERBATIM_OVERLAP or run > MAX_VERBATIM_RUN
        bad += 1 if over else 0
        mark = " ⚠️ пересобрать" if over else ""
        print(f"{video_id:<14} {str(mode or ''):<10} {share * 100:6.1f}% {run:6d}{mark}")

    print()
    print(f"всего {len(rows)}, за порогом {bad}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
