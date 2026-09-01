# -*- coding: utf-8 -*-
"""Словоформы как указатели: «Rüpeln», «des Rüpels», «angefangen» ведут в своё слово.

Зачем. Человек тапает слово в книге или в субтитрах — оно почти всегда стоит в форме:
«den Rüpeln», «Häuser», «ging». Слой такую форму не знает, запрос уходит в GPT (деньги),
а в общий банк ложится НОВАЯ запись на словоформу. Так там и появились «Kindern»,
«Hunde» и 189 записей, которые Wiktionary опознаёт как «deklinierte Form». Пока формы не
раздать указателям, мусор копится дальше, и каждая форма стоит отдельного разбора.

Формы берём у СВОЕГО генератора парадигм (backend/german_grammar_tables.py): ему хватает
леммы, артикля и части речи, поэтому ни сети, ни GPT не нужно, и покрываются все слова, а
не только те, у кого уже есть разбор.

Безопасность: указатель НЕ ставится, если такое написание уже принадлежит другому слову
как его собственное («die Kiefer» — множественное число от «der Kiefer», но при этом
самостоятельное слово «сосна»). В таких случаях форму пропускаем: лучше не найти, чем
увести человека не в то слово.

Запуск:
    DATABASE_URL=... python3 scripts/dict_units_paradigm_surfaces.py --dry-run
    DATABASE_URL=... python3 scripts/dict_units_paradigm_surfaces.py --apply
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time

import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.german_grammar_tables import (  # noqa: E402
    build_grammar_tables,
    form_token_of_cell,
)

SPACE_RE = re.compile(r"\s+")
# Служебные слова, которые не являются формой САМОГО слова: артикли и вспомогательные
# глаголы. «hat angefangen» интересен нам как «angefangen».
STOP_TOKENS = {
    "der", "die", "das", "den", "dem", "des", "ein", "eine", "einen", "einem", "einer", "eines",
    "hat", "habe", "hast", "haben", "habt", "ist", "bin", "bist", "sind", "seid", "war", "waren",
    "wird", "werde", "wirst", "werden", "werdet", "sich", "zu", "nicht", "wurde", "wurden",
}
MIN_LEN = 3  # «am», «im» и прочие обрывки в указатели не пускаем


def connect(dsn: str):
    last = None
    for attempt in range(6):
        try:
            return psycopg2.connect(dsn, connect_timeout=20)
        except Exception as exc:
            last = exc
            print("  переподключение %d/6: %s" % (attempt + 1, exc))
            time.sleep(5)
    raise SystemExit("база недоступна: %s" % last)


def norm(text: str) -> str:
    return SPACE_RE.sub(" ", str(text or "").strip()).casefold()


def forms_from_tables(tables: dict) -> set[str]:
    """Все словоформы из построенной парадигмы — по ОДНОЙ ЦЕЛОЙ ЯЧЕЙКЕ за раз."""
    out: set[str] = set()
    if not isinstance(tables, dict):
        return out
    for block in tables.values():
        if not isinstance(block, dict):
            continue
        for row in (block.get("rows") or []):
            if not isinstance(row, dict):
                continue
            for key, value in row.items():
                if key in {"case", "label", "person", "tense", "degree"} or not isinstance(value, str):
                    continue
                form = form_token_of_cell(value)
                if form:
                    out.add(form)
    return out



def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if not args.dry_run and not args.apply:
        raise SystemExit("укажи --dry-run или --apply")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("нужен DATABASE_URL")

    conn = connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, display, lemma, pos, gender, card
        FROM bt_3_lex_units
        WHERE lang = 'de' AND kind = 'word' AND pos IS NOT NULL;
        """
    )
    units = cur.fetchall()
    print("немецких слов с известной частью речи: %d" % len(units))

    # Написания, которые уже принадлежат словам как СВОИ: в них форму подставлять нельзя.
    cur.execute(
        """
        SELECT s.surface_key, s.unit_id FROM bt_3_lex_surfaces s
        JOIN bt_3_lex_units u ON u.id = s.unit_id
        WHERE s.lang = 'de' AND s.match_kind IN ('exact', 'no_article');
        """
    )
    owned: dict[str, set] = {}
    for surface_key, unit_id in cur.fetchall():
        owned.setdefault(surface_key, set()).add(unit_id)

    cur.execute("SELECT surface_key, unit_id FROM bt_3_lex_surfaces WHERE lang = 'de';")
    existing = {(r[0], r[1]) for r in cur.fetchall()}

    payload: list[tuple] = []
    collisions = covered = 0
    samples: list[str] = []
    for unit_id, display, lemma, pos, gender, card in units:
        item = {
            "part_of_speech": pos,
            "word_de": display,
            "article": gender,
            "forms": (card or {}).get("forms") if isinstance(card, dict) else {},
        }
        try:
            tables = build_grammar_tables(item)
        except Exception:
            continue
        forms = forms_from_tables(tables)
        forms.discard(norm(lemma))
        if not forms:
            continue
        added_here = 0
        for form in forms:
            holders = owned.get(form)
            if holders and unit_id not in holders:
                collisions += 1  # это чужое слово, а не форма нашего
                continue
            if (form, unit_id) in existing:
                continue
            payload.append(("de", form, unit_id, "inflected"))
            added_here += 1
        if added_here:
            covered += 1
            if len(samples) < 6:
                samples.append("%-18s → %s" % (display, ", ".join(sorted(forms)[:6])))

    print("  слов, которым добавятся формы: %d" % covered)
    print("  новых указателей:              %d" % len(payload))
    print("  пропущено из-за совпадения с другим словом: %d" % collisions)
    print()
    for line in samples:
        print("   " + line)

    if not args.apply:
        conn.rollback()
        print("\n(--dry-run: в базу ничего не записано)")
        return 0

    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind) VALUES %s "
        "ON CONFLICT (lang, surface_key, unit_id) DO NOTHING",
        payload, page_size=1000,
    )
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM bt_3_lex_surfaces WHERE match_kind = 'inflected';")
    print("\nзаписано. указателей-словоформ в слое: %d" % cur.fetchone()[0])
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
