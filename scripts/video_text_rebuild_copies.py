"""Пересобрать тексты роликов, оказавшиеся копией субтитров.

Зачем: до 02.09.2026 короткие ролики шли «дословным» режимом — субтитры чистились и
отдавались людям почти как есть (замер: 72% двенадцатисловных окон совпадали слово в
слово, длиннейший дословный кусок 119 слов). Владелец это запретил: текст обязан быть
ПЕРЕСКАЗОМ с лексикой оригинала, а не копией.

Починить систему мало — надо убрать и то, что уже роздано. Скрипт находит такие тексты
двумя признаками (старое имя режима ИЛИ превышенный порог дословности), пересобирает их
новым способом и переливает в КАЖДУЮ книгу, сделанную из этого ролика у людей.

    DATABASE_URL=... OPENAI_API_KEY=... python3 scripts/video_text_rebuild_copies.py
    DATABASE_URL=... OPENAI_API_KEY=... python3 scripts/video_text_rebuild_copies.py --apply

Против прода удобнее так (ключ приходит из окружения сервиса, DSN — файлом):

    railway run python3 scripts/video_text_rebuild_copies.py --dsn-file /tmp/pg --apply

Без --apply только показывает, что будет сделано, и ничего не трогает.

Книги НЕ удаляются: у человека может стоять закладка, и молча забирать у него книгу
нельзя. Меняется только содержимое — копия заменяется пересказом.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.video_reader_text import (  # noqa: E402
    MAX_VERBATIM_OVERLAP,
    MAX_VERBATIM_RUN,
    build_reader_text,
    longest_verbatim_run,
    transcript_to_text,
    verbatim_overlap,
)

GOOD_MODES = {"close", "condensed"}


def main() -> int:
    # DSN можно дать файлом: в окружении разработчика DATABASE_URL указывает на
    # мёртвую базу, а прод-переменные приходят из railway run и перебить их нельзя.
    dsn = os.getenv("DATABASE_URL")
    if "--dsn-file" in sys.argv:
        with open(sys.argv[sys.argv.index("--dsn-file") + 1]) as handle:
            dsn = handle.read().strip()
    if not dsn:
        print("Нужен DATABASE_URL или --dsn-file <путь>", file=sys.stderr)
        return 2
    apply_changes = "--apply" in sys.argv

    import psycopg2

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT video_id, mode, content_text FROM bt_3_video_reader_texts "
            "WHERE status = 'ready' ORDER BY updated_at;")
        rows = cursor.fetchall()

    if not rows:
        print("Собранных текстов нет — чистить нечего.")
        return 0

    suspect = []
    for video_id, mode, content_text in rows:
        with conn.cursor() as cursor:
            cursor.execute("SELECT items FROM bt_3_youtube_transcripts WHERE video_id = %s;",
                           (video_id,))
            row = cursor.fetchone()
        if not row:
            print(f"{video_id}: субтитров в кеше нет — пересобрать не из чего, пропускаю")
            continue
        items = row[0]
        if isinstance(items, str):
            items = json.loads(items)
        source = transcript_to_text(items or [])
        share = verbatim_overlap(source, content_text or "")
        run = longest_verbatim_run(source, content_text or "")
        old_mode = str(mode or "") not in GOOD_MODES
        over = share > MAX_VERBATIM_OVERLAP or run > MAX_VERBATIM_RUN
        verdict = "ПЕРЕСОБРАТЬ" if (old_mode or over) else "в порядке"
        print(f"{video_id}: режим «{mode}», совпадение {share * 100:.1f}%, "
              f"длиннейший кусок {run} слов → {verdict}")
        if old_mode or over:
            suspect.append((video_id, items))

    if not suspect:
        print("\nКопий не найдено.")
        return 0
    print(f"\nК пересборке: {len(suspect)}")
    if not apply_changes:
        print("Это разбор без правки. Чтобы пересобрать: добавьте --apply")
        return 0

    for video_id, items in suspect:
        print(f"\n— {video_id}: пересобираю…")
        result = build_reader_text(items=items,
                                   on_progress=lambda d, t: print(f"    кусок {d}/{t}"))
        source = transcript_to_text(items)
        share = verbatim_overlap(source, result["text"])
        run = longest_verbatim_run(source, result["text"])
        print(f"    {result['source_chars']} → {result['result_chars']} символов, "
              f"режим «{result['mode']}», совпадение {share * 100:.1f}%, кусок {run} слов")
        if share > MAX_VERBATIM_OVERLAP or run > MAX_VERBATIM_RUN:
            # Пересобрали — и снова копия. Не записываем: заменить одну копию другой
            # значит сделать вид, что починили.
            print("    ⚠️ снова за порогом — НЕ записываю, разбираться руками")
            continue
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE bt_3_video_reader_texts SET content_text = %s, mode = %s, "
                "model = %s, source_chars = %s, result_chars = %s, chunks_total = %s, "
                "chunks_done = %s, updated_at = NOW() WHERE video_id = %s;",
                (result["text"], result["mode"], result["model"], result["source_chars"],
                 result["result_chars"], result["chunks"], result["chunks"], video_id))
            # Книги людей, сделанные из этого ролика. Не удаляем — переливаем.
            cursor.execute(
                "UPDATE bt_3_reader_library SET content_text = %s, total_chars = %s, "
                "text_hash = encode(sha256(%s::bytea), 'hex'), updated_at = NOW() "
                "WHERE source_type = 'video' AND source_url = %s RETURNING id, user_id;",
                (result["text"], result["result_chars"],
                 result["text"].encode("utf-8"), f"https://youtu.be/{video_id}"))
            touched = cursor.fetchall()
        print(f"    переписано книг у людей: {len(touched)} {touched}")

    print("\nГотово.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
