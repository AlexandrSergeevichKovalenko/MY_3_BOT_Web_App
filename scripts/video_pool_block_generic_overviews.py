# -*- coding: utf-8 -*-
"""Уборка пула теории: ролик-обзор перестаёт числиться теорией по 50 темам сразу.

Что это было
────────────
Пул `bt_3_video_recommendations` наполняют двое: администратор руками (/addvideo) и
ночной автопрогрев поиском по YouTube (`warm_grammar_video_pool`). Поиск почти на любой
грамматический запрос возвращает одни и те же популярные обзоры, и они оседали во всех
темах подряд.

Замер по живой базе 25.08.2026 (370 активных строк, 74 темы):

    j8CNbA765fg  «Die komplette B2-Grammatik in 25 Minuten»      — 51 тема
    Pt4nZ8iWmpk  «GAST / TELC - B1-Prüfung»                      — 49 тем
    bcj9SRTbCgY  «B1 Deutsch komplett erklärt»                   — 33 темы
    rnBzBQ3lc7s  «DOPPELKONNEKTOREN»                             — 24 темы
    2F4u8ljrw4A  «100 Passiv-Sätze für B2 Deutsch»               — 20 тем

Итого 177 размещений. Проверено поштучно: НИ ОДИН из пяти не стоял под своей собственной
темой — «Doppelkonnektoren» не было в теме про союзы, «100 Passiv-Sätze» не было в теме
про пассив. То есть выключается не «спорное», а заведомо чужое.

Правило отбора — то же, что у стража на входе
──────────────────────────────────────────────
Ролик, который числится теорией больше чем в `_GENERIC_VIDEO_TOPIC_LIMIT` (6) разных
темах, — обзор, а не тема. Граница взята по разрыву в живых данных: у тематических
роликов максимум 5 тем («ALLE Zeiten auf Deutsch» в пяти темах про времена — там он
уместен), у обзоров — 20 и больше. Правило здесь и правило в bot_3.py — одно и то же
число, поэтому уборка и приёмка не могут разъехаться.

Почему не DELETE, а выключение
──────────────────────────────
`is_blocked = TRUE` переживает автопрогрев: upsert при повторной находке ставит
`is_active = NOT is_blocked`, поэтому выключенное не воскресает. Удаление строки такой
защиты не даёт — ролик вернулся бы в тему следующей же ночью.

    python3 scripts/video_pool_block_generic_overviews.py          # только показать
    python3 scripts/video_pool_block_generic_overviews.py --apply  # выключить
"""
import argparse
import sys

sys.path.insert(0, ".")

from backend.database import get_db_connection_context  # noqa: E402

TOPIC_LIMIT = 6   # = bot_3._GENERIC_VIDEO_TOPIC_LIMIT


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="выключить найденное (без флага — только показать)")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE bt_3_video_recommendations
                ADD COLUMN IF NOT EXISTS is_blocked BOOLEAN NOT NULL DEFAULT FALSE;
            """)
            cur.execute(
                """
                SELECT video_id, max(video_title), count(DISTINCT skill_id), count(*)
                FROM bt_3_video_recommendations
                WHERE is_active = TRUE AND skill_id IS NOT NULL
                GROUP BY video_id
                HAVING count(DISTINCT skill_id) >= %s
                ORDER BY count(DISTINCT skill_id) DESC;
                """,
                (TOPIC_LIMIT,),
            )
            rows = cur.fetchall()
            if not rows:
                print("Роликов-обзоров в пуле нет — чистить нечего.")
                conn.commit()
                return

            print("Ролики, числящиеся теорией больше чем в %d темах:\n" % (TOPIC_LIMIT - 1))
            total = 0
            for vid, title, themes, placements in rows:
                total += int(placements)
                print("  %-13s %3d тем · %3d строк · %s" % (vid, themes, placements, (title or "")[:64]))
            print("\nВсего размещений к выключению: %d" % total)

            cur.execute(
                """
                SELECT count(*) FROM (
                    SELECT skill_id
                    FROM bt_3_video_recommendations
                    WHERE is_active = TRUE AND skill_id IS NOT NULL
                    GROUP BY skill_id
                    HAVING count(*) FILTER (
                        WHERE video_id NOT IN (
                            SELECT video_id FROM bt_3_video_recommendations
                            WHERE is_active = TRUE AND skill_id IS NOT NULL
                            GROUP BY video_id HAVING count(DISTINCT skill_id) >= %s
                        )
                    ) = 0
                ) t;
                """,
                (TOPIC_LIMIT,),
            )
            empty_after = int(cur.fetchone()[0])
            print("Тем, которые останутся совсем без видео: %d" % empty_after)

            if not args.apply:
                print("\nЭто показ. Чтобы выключить: --apply")
                conn.commit()
                return

            ids = [r[0] for r in rows]
            cur.execute(
                """
                UPDATE bt_3_video_recommendations
                SET is_blocked = TRUE, is_active = FALSE, updated_at = NOW()
                WHERE video_id = ANY(%s) AND is_active = TRUE;
                """,
                (ids,),
            )
            changed = cur.rowcount
        conn.commit()
    print("\n✓ Выключено размещений: %d" % changed)
    print("  Они не вернутся: upsert выставляет is_active = NOT is_blocked, а страж на")
    print("  входе (bot_3._GENERIC_VIDEO_TOPIC_LIMIT) не даёт положить обзор в новые темы.")


if __name__ == "__main__":
    main()
