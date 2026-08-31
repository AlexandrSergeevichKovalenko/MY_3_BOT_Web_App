# -*- coding: utf-8 -*-
"""Папки с названием ролика → источники слов. Разовая уборка накопленного.

ЗАЧЕМ
──────
До 31.08.2026 ролик изображали ПАПКОЙ: сохранение из плеера заводило папку по первым
двум словам заголовка («Die großen», «2 Jahre», „Sport und», «An der»), а сервер тут же
переписывал папку на тематическую. Обе роли дрались за одно поле `folder_id`.

Замер по живой базе 31.08.2026 (аккаунт владельца):
    240 слов сохранено из плеера за всё время
     25 из них остались в папке ролика
    215 уехали в тематические папки
     43 папки всего, 18 пустых — 16 из них папки роликов

Решение владельца 31.08.2026: тема — ДОМ слова (folder_id), ролик — его СВОЙСТВО
(source_id → bt_3_dictionary_sources). Папок-роликов больше не бывает.

ЧТО ДЕЛАЕТ СКРИПТ
─────────────────
1. Находит папки, которые НЕ являются тематическими и не GENERAL, — то есть остатки
   старой схемы. Правило отбора берётся ОТТУДА ЖЕ, откуда его берёт продукт:
   database.DICTIONARY_SEMANTIC_FOLDER_META. Никаких «похоже на название ролика».
2. Для непустых заводит источник:
   • если по дате сохранения слов находится ровно один просмотр в bt_3_youtube_watch_state —
     ключом идёт настоящий id ролика, а обрезанное имя папки записывается как ВРЕМЕННОЕ
     название (title_source='legacy_folder_name'): его заменит настоящий заголовок,
     как только человек откроет ролик снова;
   • если просмотр не найден — id ролика нам взять НЕГДЕ (в origin_meta его никогда не
     писали). Тогда ключ синтетический, `legacy-folder:<id>`, а имя папки остаётся
     единственным, что мы про этот ролик знаем. Это не выдумка: имя реальное, просто
     обрезанное, и оно помечено как временное.
3. Слова из такой папки переезжают в СВОЮ ТЕМУ (по semantic_tag). Темы нет — папка
   становится пустой (NULL, «Без папки»), а не GENERAL и не «Прочее»: «Прочее» по
   решению владельца означает «модель посмотрела и не смогла отнести», а у этих слов
   темы не спрашивали вовсе. Число таких слов печатается отдельной строкой.
4. Опустевшие папки удаляются.

ЧЕГО СКРИПТ НЕ ДЕЛАЕТ
─────────────────────
• Не трогает «Прочее» (4 275 слов) — прямое указание владельца 31.08.2026.
• Не трогает тематические папки и GENERAL.
• Не удаляет ни одного слова.
• Не спрашивает модель и ничего не досочиняет.

    python3 scripts/dict_folders_to_sources.py            # только показать план
    python3 scripts/dict_folders_to_sources.py --apply    # выполнить
    python3 scripts/dict_folders_to_sources.py --apply --user 117649764
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backend.database import (                                    # noqa: E402
    DICTIONARY_SEMANTIC_FOLDER_META,
    get_db_connection_context,
)

# Папки, которые остатками старой схемы НЕ являются и трогать которые нельзя.
#
# Тематические и GENERAL — устройство системы. Остальные три имени защищены поимённо,
# потому что отличить папку, заведённую человеком РУКАМИ, от заведённой старым кодом
# плеера в данных НЕЧЕМ: обе лежат в одной таблице с одинаковыми color='#5ddcff' и
# icon='book' (проверено запросом 31.08.2026 — таких папок 38, и все с этой парой).
# Поэтому вместо догадки «похоже на название ролика» — закрытый список защищённых имён.
# Он составлен по живой базе: у всех остальных пользователей, кроме владельца, из
# нетематических папок есть только «Базовый словарь», «Слова» и четыре пустых
# названия роликов. Появится новое системное имя — дописать СЮДА.
KEEP_NAMES = set(DICTIONARY_SEMANTIC_FOLDER_META) | {
    "GENERAL",
    "Базовый словарь",   # заводится импортом стартового словаря
    "📚 Wortschatz",     # старое системное имя
    "Слова",             # папка, заведённая человеком руками
}


def collect_legacy_folders(cursor, user_id: int | None) -> list[dict]:
    where = "WHERE f.name <> ALL(%s)"
    params: list = [sorted(KEEP_NAMES)]
    if user_id is not None:
        where += " AND f.user_id = %s"
        params.append(int(user_id))
    cursor.execute(
        f"""
        SELECT f.id, f.user_id, f.name, f.created_at,
               COUNT(q.id)::BIGINT AS word_count,
               MIN(q.created_at)::date AS first_day,
               MAX(q.created_at)::date AS last_day
        FROM bt_3_dictionary_folders f
        LEFT JOIN bt_3_webapp_dictionary_queries q
               ON q.folder_id = f.id AND q.user_id = f.user_id
        {where}
        GROUP BY f.id, f.user_id, f.name, f.created_at
        ORDER BY COUNT(q.id) DESC, f.created_at;
        """,
        params,
    )
    return [
        {
            "id": int(row[0]), "user_id": int(row[1]), "name": row[2],
            "created_at": row[3], "word_count": int(row[4] or 0),
            "first_day": row[5], "last_day": row[6],
        }
        for row in cursor.fetchall() or []
    ]


def find_watched_video(cursor, user_id: int, first_day, last_day) -> str | None:
    """id ролика по дню, когда слова сохраняли. Ровно один — берём; иначе честно None.

    Двусмысленность (в тот день открывали два ролика) — это НЕ повод выбрать первый:
    угаданная привязка хуже отсутствующей, её потом никто не отличит от настоящей.
    """
    if first_day is None or last_day is None:
        return None
    cursor.execute(
        """
        SELECT DISTINCT video_id
        FROM bt_3_youtube_watch_state
        WHERE user_id = %s
          AND last_opened_at::date BETWEEN %s AND %s;
        """,
        (int(user_id), first_day, last_day),
    )
    rows = [r[0] for r in cursor.fetchall() or [] if r and r[0]]
    return str(rows[0]) if len(rows) == 1 else None


def known_video_title(cursor, video_id: str) -> str | None:
    cursor.execute(
        "SELECT video_title FROM bt_3_video_recommendations WHERE video_id = %s AND video_title IS NOT NULL LIMIT 1;",
        (video_id,),
    )
    row = cursor.fetchone()
    return str(row[0]).strip() if row and row[0] else None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="выполнить, а не только показать")
    parser.add_argument("--user", type=int, default=None, help="только один аккаунт")
    args = parser.parse_args()

    moved_to_theme = 0
    moved_to_none = 0
    linked_by_video_id = 0
    linked_by_folder_name = 0
    deleted_folders = 0
    kept_folders: list[str] = []

    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            folders = collect_legacy_folders(cursor, args.user)
            print(f"Папок старой схемы найдено: {len(folders)}"
                  f" (пустых: {sum(1 for f in folders if f['word_count'] == 0)})\n")

            for folder in folders:
                mark = "пустая" if folder["word_count"] == 0 else f"{folder['word_count']} слов"
                video_id = None
                if folder["word_count"] > 0:
                    video_id = find_watched_video(
                        cursor, folder["user_id"], folder["first_day"], folder["last_day"]
                    )
                where_from = (
                    f"ролик {video_id}" if video_id
                    else ("имя папки" if folder["word_count"] > 0 else "—")
                )
                print(f"  [{folder['id']:>5}] {folder['name']!r:<36} {mark:<10} → {where_from}")

                if not args.apply:
                    continue

                source_id = None
                if folder["word_count"] > 0:
                    if video_id:
                        title = known_video_title(cursor, video_id) or folder["name"]
                        title_source = "recommendations" if known_video_title(cursor, video_id) else "legacy_folder_name"
                        kind, key = "youtube", video_id
                        linked_by_video_id += 1
                    else:
                        title, title_source = folder["name"], "legacy_folder_name"
                        kind, key = "youtube", f"legacy-folder:{folder['id']}"
                        linked_by_folder_name += 1
                    cursor.execute(
                        """
                        INSERT INTO bt_3_dictionary_sources (user_id, kind, external_key, title, title_source)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (user_id, kind, external_key) DO UPDATE SET updated_at = NOW()
                        RETURNING id;
                        """,
                        (folder["user_id"], kind, key, title, title_source),
                    )
                    source_id = int(cursor.fetchone()[0])

                    # Слово уходит в СВОЮ тему; темы нет — остаётся без папки (не GENERAL
                    # и не «Прочее»: у этих слов тему не спрашивали, а не «не смогли»).
                    cursor.execute(
                        """
                        UPDATE bt_3_webapp_dictionary_queries AS q
                        SET source_id = COALESCE(q.source_id, %s),
                            folder_id = (
                                SELECT tf.id FROM bt_3_dictionary_folders tf
                                WHERE tf.user_id = q.user_id
                                  AND tf.name = q.semantic_tag
                                LIMIT 1
                            ),
                            updated_at = NOW()
                        WHERE q.user_id = %s AND q.folder_id = %s
                        RETURNING (q.semantic_tag IS NOT NULL AND q.semantic_tag <> '');
                        """,
                        (source_id, folder["user_id"], folder["id"]),
                    )
                    for (had_theme,) in cursor.fetchall() or []:
                        if had_theme:
                            moved_to_theme += 1
                        else:
                            moved_to_none += 1

                cursor.execute(
                    "SELECT COUNT(*) FROM bt_3_webapp_dictionary_queries WHERE folder_id = %s;",
                    (folder["id"],),
                )
                left = int(cursor.fetchone()[0])
                if left == 0:
                    cursor.execute("DELETE FROM bt_3_dictionary_folders WHERE id = %s;", (folder["id"],))
                    deleted_folders += 1
                else:
                    # Не смогли опустошить — папку НЕ удаляем и говорим об этом вслух.
                    kept_folders.append(f"{folder['name']} (осталось {left})")

    print("\n── ИТОГ ──")
    if not args.apply:
        print("  Это был только показ. Ничего не изменено. Повторить с --apply.")
        return
    print(f"  Папок удалено:                    {deleted_folders}")
    print(f"  Слов переехало в свою тему:       {moved_to_theme}")
    print(f"  Слов осталось без папки:          {moved_to_none}   ← открытая задача, не норма")
    print(f"  Источников по настоящему id:      {linked_by_video_id}")
    print(f"  Источников по имени старой папки: {linked_by_folder_name}   ← имя временное, заменится настоящим")
    if kept_folders:
        print("  НЕ удалены (в них остались слова): " + "; ".join(kept_folders))


if __name__ == "__main__":
    main()
