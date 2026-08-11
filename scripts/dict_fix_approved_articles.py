# -*- coding: utf-8 -*-
"""Починка десяти артиклей, утверждённых владельцем поимённо 11.08.2026.

Почему список ЗАШИТ, а не выведен правилом. Проверка пула нашла 33 записи, где
артикль расходится со справочником родов, и ни одно автоматическое правило не
отделило ошибку от нормального немецкого:

  • «die Reifen», «die Mitfahrer» — именительный множественного, там «die» верное,
    а у половины немецких существительных множественное пишется как единственное,
    и отличить их по написанию нечем;
  • «der Gehalt» (содержание) / «das Gehalt» (зарплата), «der Schild» (щит) /
    «das Schild» (вывеска), «der Kiefer» (челюсть) / «die Kiefer» (сосна),
    «der Weise» (мудрец) / «die Weise» (способ), «der/die Abgeordnete» (мужчина /
    женщина) — настоящие двуродовые, наши записи верные, а справочник знает одну
    строку на написание.

Поэтому каждый случай разобран глазами, и здесь лежит РЕЗУЛЬТАТ разбора, а не
правило. Любое новое слово в этом списке — тоже через разбор, а не через догадку.

Правка идёт по всей записи разом — заголовок, обе видимые стороны и разбор:
починить половину означает оставить карточку спорить самой с собой.

Запуск:
    DATABASE_URL=... python3 scripts/dict_fix_approved_articles.py            # отчёт
    DATABASE_URL=... python3 scripts/dict_fix_approved_articles.py --apply
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

import psycopg2

# слово → (что стоит сейчас, что должно стоять)
APPROVED: dict[str, tuple[str, str]] = {
    "Aula":           ("der", "die"),   # актовый зал, второго рода нет
    "Fachgebiet":     ("der", "das"),   # -gebiet всегда das
    "Geburtsurkunde": ("das", "die"),   # -urkunde всегда die
    "Grundmauer":     ("der", "die"),   # -mauer всегда die
    "Hoden":          ("das", "der"),   # яичко, второго рода нет
    "Schließfach":    ("der", "das"),   # -fach всегда das
    "Verzeichnis":    ("der", "das"),   # перечень
    "Versammlung":    ("der", "die"),   # рядом лежат две верные записи того же слова
    "Konsens":        ("das", "der"),   # рядом лежит верная «der Konsens»
    "Wassertropfen":  ("das", "der"),   # der Tropfen; рядом лежит верная запись
}

# Род единицы слоя, который тоже неверен и потому кормит неверным родом всех.
UNIT_FIXES: dict[str, str] = {
    "Wassertropfen": "der",
}


def _swap(value, wrong: str, right: str, noun: str):
    if not value:
        return value
    return re.sub(rf"\b{wrong}\s+{re.escape(noun)}\b", f"{right} {noun}",
                  str(value), flags=re.IGNORECASE)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("Нужен DATABASE_URL", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(dsn, connect_timeout=25)
    conn.autocommit = False
    touched_entries = 0
    touched_units = 0

    with conn.cursor() as cur:
        for noun, (wrong, right) in APPROVED.items():
            cur.execute(
                """
                SELECT id, word_de, translation_de, source_text, target_text, response_json
                FROM bt_3_dictionary_entries
                WHERE word_de ILIKE %s;
                """,
                (f"{wrong} {noun}",),
            )
            rows = cur.fetchall()
            if not rows:
                print(f"  {wrong} {noun}: записей не найдено (уже починено?)")
                continue
            for entry_id, word_de, translation_de, source_text, target_text, payload in rows:
                print(f"  {word_de}  →  {right} {noun}   (запись {entry_id})")
                if not args.apply:
                    continue
                data = dict(payload) if isinstance(payload, dict) else {}
                if data.get("article"):
                    data["article"] = right
                for field in ("word_de", "translation_de", "source_text", "target_text"):
                    if data.get(field):
                        data[field] = _swap(data[field], wrong, right, noun)
                cur.execute(
                    """
                    UPDATE bt_3_dictionary_entries
                    SET word_de = %s, translation_de = %s, source_text = %s,
                        target_text = %s, response_json = %s, updated_at = NOW()
                    WHERE id = %s;
                    """,
                    (
                        _swap(word_de, wrong, right, noun),
                        _swap(translation_de, wrong, right, noun),
                        _swap(source_text, wrong, right, noun),
                        _swap(target_text, wrong, right, noun),
                        json.dumps(data, ensure_ascii=False),
                        entry_id,
                    ),
                )
                touched_entries += 1

        print("\nЕдиницы слоя:")
        for lemma, right in UNIT_FIXES.items():
            cur.execute(
                "SELECT id, gender FROM bt_3_lex_units WHERE lemma = %s AND lang = 'de';",
                (lemma,),
            )
            for unit_id, gender in cur.fetchall():
                if (gender or "") == right:
                    print(f"  {lemma}: уже {right}")
                    continue
                print(f"  {lemma}: {gender or '—'} → {right}   (единица {unit_id})")
                if args.apply:
                    cur.execute(
                        "UPDATE bt_3_lex_units SET gender = %s, gender_source = 'разбор владельца', "
                        "updated_at = NOW() WHERE id = %s;",
                        (right, unit_id),
                    )
                    touched_units += 1

    if args.apply:
        conn.commit()
        print(f"\n✓ Исправлено записей пула: {touched_entries}, единиц слоя: {touched_units}")
    else:
        conn.rollback()
        print("\nЭто отчёт, база не изменена. Применить: --apply")
    conn.close()


if __name__ == "__main__":
    main()
