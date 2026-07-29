# -*- coding: utf-8 -*-
"""Форма слова — не отдельное слово: складываем её в единицу леммы.

Слой единиц опознаёт СЛОВО, а все способы его напечатать — указатели на него
(bt_3_lex_surfaces). Но пока система не умела отличать слово от формы, формы заводились
как самостоятельные единицы: «Probleme» жило рядом с «Problem» и имело собственный род.
Отсюда и «das Probleme» на экране, и двойники в словаре.

Что делает скрипт для каждой подтверждённой формы чужого слова:
  1. находит (или заводит) единицу самой леммы;
  2. переносит на неё переводы, значения и происхождение формы;
  3. вешает написание формы указателем match_kind='inflected' — теперь запрос
     «Probleme» находит СЛОВО «das Problem», а не двойника;
  4. удаляет пустую единицу-форму.

Ничего не удаляется, пока перенесено не всё: при любой неожиданности запись
пропускается и печатается в отчёте. По умолчанию — прогон вхолостую.

Запуск:  python3 scripts/lex_units_fold_forms.py [--apply] [--limit N]
"""
from __future__ import annotations

import argparse
import os
import re
import sys

import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.german_surface import PL, german_surface  # noqa: E402

ARTICLE_RE = re.compile(r"^(der|die|das)\s+", re.I)


def bare(text: str) -> str:
    return ARTICLE_RE.sub("", re.sub(r"\s+", " ", str(text or "").strip())).strip()


def find_forms(cur, limit: int) -> list[tuple]:
    """Единицы, которые на самом деле являются формой ДРУГОГО слова."""
    cur.execute("SELECT id, lemma, lemma_key, display, gender, card IS NOT NULL "
                "FROM bt_3_lex_units WHERE lang = 'de' AND kind = 'word' ORDER BY id")
    out = []
    for unit_id, lemma, lemma_key, display, gender, has_card in cur.fetchall() or []:
        surface = bare(display) or bare(lemma)
        if not surface or " " in surface or not surface[:1].isupper():
            continue
        verdict = german_surface(surface)
        if verdict["number"] != PL or verdict["confidence"] != "high":
            continue
        target = verdict["lemma"] or ""
        if not target or target.casefold() == surface.casefold():
            continue          # живёт только во множественном — это и есть слово
        out.append((unit_id, surface, target, lemma_key, gender, has_card))
        if len(out) >= limit:
            break
    return out


def lemma_unit_id(cur, lemma: str) -> int | None:
    """Единица леммы, если она уже есть в слое."""
    key = bare(lemma).casefold()
    cur.execute(
        """
        SELECT u.id FROM bt_3_lex_units u
        WHERE u.lang = 'de' AND u.kind = 'word' AND u.lemma_key = %s
        ORDER BY (u.card IS NULL), u.id LIMIT 1;
        """,
        (key,),
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def fold(conn, unit_id: int, surface: str, target_unit: int, apply: bool) -> None:
    with conn.cursor() as cur:
        # Разбор формы не выбрасываем: если у слова его ещё нет, он достаётся слову.
        # Пометка источника оставляет след, чтобы ночное обогащение переписало карточку
        # на настоящую словарную форму, а не считало её готовой.
        cur.execute("""
            UPDATE bt_3_lex_units AS target
               SET card = form.card,
                   card_source = 'перенесён с формы',
                   updated_at = NOW()
              FROM bt_3_lex_units AS form
             WHERE target.id = %s AND form.id = %s
               AND target.card IS NULL AND form.card IS NOT NULL;
        """, (target_unit, unit_id))
        # Переводы: те же пары к целевой единице, дубли не плодим.
        cur.execute("""
            INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source, saves_count)
            SELECT %s, to_unit, rank, COALESCE(source, 'форма'), saves_count
            FROM bt_3_lex_links WHERE from_unit = %s
            ON CONFLICT (from_unit, to_unit) DO NOTHING;
        """, (target_unit, unit_id))
        cur.execute("""
            INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source, saves_count)
            SELECT from_unit, %s, rank, COALESCE(source, 'форма'), saves_count
            FROM bt_3_lex_links WHERE to_unit = %s
            ON CONFLICT (from_unit, to_unit) DO NOTHING;
        """, (target_unit, unit_id))
        # Происхождение: из каких строк банка собрана единица.
        cur.execute("""
            INSERT INTO bt_3_lex_unit_sources (unit_id, entry_id, side)
            SELECT %s, entry_id, side FROM bt_3_lex_unit_sources WHERE unit_id = %s
            ON CONFLICT DO NOTHING;
        """, (target_unit, unit_id))
        # Написание формы становится указателем на слово.
        cur.execute("""
            INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
            VALUES ('de', %s, %s, 'inflected') ON CONFLICT DO NOTHING;
        """, (surface.casefold(), target_unit))
        cur.execute("DELETE FROM bt_3_lex_units WHERE id = %s", (unit_id,))
    if apply:
        conn.commit()
    else:
        conn.rollback()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()
    dsn = (os.getenv("DATABASE_URL_PGBOUNCER_RAILWAY") or os.getenv("DATABASE_URL") or "").strip()
    if not dsn:
        print("Нет DATABASE_URL", file=sys.stderr)
        return 2
    conn = psycopg2.connect(dsn, connect_timeout=20)
    print("═══ Формы в слое единиц ═══"
          f"\nрежим: {'ЗАПИСЬ' if args.apply else 'вхолостую'}")
    with conn.cursor() as cur:
        forms = find_forms(cur, args.limit)
    print(f"единиц-форм найдено: {len(forms)}")

    folded = skipped = created = 0
    shown = 0
    for unit_id, surface, target, _key, _gender, has_card in forms:
        with conn.cursor() as cur:
            target_id = lemma_unit_id(cur, target)
        if not target_id:
            # Слова ещё нет в слое — заводим дом для него, иначе форму некуда сложить.
            if args.apply:
                from backend.lex_units import ensure_unit
                target_id = ensure_unit(target, "de")
                created += 1
            else:
                created += 1
                if shown < 15:
                    print(f"  + завести слово «{target}» и сложить в него «{surface}»")
                    shown += 1
                continue
        if not target_id or target_id == unit_id:
            skipped += 1
            continue
        try:
            fold(conn, unit_id, surface, target_id, args.apply)
            folded += 1
            if shown < 15:
                print(f"  «{surface}» (id={unit_id}{', есть разбор' if has_card else ''}) "
                      f"→ единица «{target}» (id={target_id})")
                shown += 1
        except Exception as exc:
            conn.rollback()
            skipped += 1
            print(f"  ⚠ пропущено «{surface}»: {str(exc)[:120]}")
    print(f"\nсложено: {folded}, новых слов заведено: {created}, пропущено: {skipped}")
    if not args.apply:
        print("Это был прогон вхолостую. Записать: --apply")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
