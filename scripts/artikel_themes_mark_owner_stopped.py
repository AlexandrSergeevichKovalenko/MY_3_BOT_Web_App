# -*- coding: utf-8 -*-
"""Перевести уже остановленные темы наполнения из `paused` в `stopped` (11.08.2026).

Зачем. У наполнения тем было одно состояние `paused` на две разные вещи: «добор встал сам,
жду решения владельца» и «владелец решил: не добирать». Отчёт раз в три дня спрашивает
именно по `paused` с двумя пустыми прогонами — значит каждая закрытая владельцем тема
возвращалась к нему тем же вопросом снова и снова. Замер 11.08.2026: 20 таких тем, все
закрыты владельцем в один заход, и все двадцать пришли бы опять через три дня.

Код различие уже понимает (`stopped`). Этот скрипт закрывает хвост: переводит темы,
решение по которым владелец принял ДО правки, чтобы вопрос по ним больше не задавался.

Переводим ВСЕ активные темы в `paused`: на момент запуска все они уже были показаны
владельцу и им же закрыты. Тема, которая выдохнется после этого, встанет в `paused` заново
ночным прогоном, и вопрос по ней будет задан один раз — как и задумано.

Запуск:
    DATABASE_URL=... python3 scripts/artikel_themes_mark_owner_stopped.py            # отчёт
    DATABASE_URL=... python3 scripts/artikel_themes_mark_owner_stopped.py --apply
"""
from __future__ import annotations

import argparse
import os
import sys

import psycopg2


def _dsn() -> str:
    for key in ("DATABASE_PUBLIC_URL", "DATABASE_URL"):
        value = (os.getenv(key) or "").strip()
        if value:
            return value
    sys.exit("Нужен DATABASE_URL (или DATABASE_PUBLIC_URL).")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="записать, а не только показать")
    args = parser.parse_args()

    with psycopg2.connect(_dsn(), connect_timeout=20) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT theme_key, label_ru, COALESCE(dry_streak, 0) "
                "FROM bt_3_article_sprint_themes "
                "WHERE active AND COALESCE(fill_state, 'auto') = 'paused' "
                "ORDER BY label_ru;"
            )
            rows = cur.fetchall() or []
            if not rows:
                print("Тем в состоянии «paused» нет — переводить нечего.")
                return
            print(f"Тем к переводу в «stopped»: {len(rows)}")
            for key, label, dry in rows:
                asked = " (отчёт спрашивал по ней каждые 3 дня)" if int(dry) >= 2 else ""
                print(f"  • {label or key}{asked}")
            if not args.apply:
                print("\nЭто отчёт. Записать: повторить с --apply")
                return
            cur.execute(
                "UPDATE bt_3_article_sprint_themes "
                "SET fill_state = 'stopped', fill_state_at = NOW(), updated_at = NOW() "
                "WHERE active AND COALESCE(fill_state, 'auto') = 'paused';"
            )
            print(f"\nПереведено: {cur.rowcount}. Вопросов по этим темам больше не будет.")
        conn.commit()


if __name__ == "__main__":
    main()
