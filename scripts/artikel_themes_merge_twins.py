# -*- coding: utf-8 -*-
"""Слить темы-близнецы банка артиклей: одна область — одна тема.

ПОВОД. 19.08.2026, разбор темы «Computer & Geräte». В теме 80 живых слов, набор дня
строится на 140 — игрок увидел 77 из 80, то есть весь банк темы разом. Причина, по
которой тема не может набраться, оказалась структурной: компьютерных тем в базе ДВЕ —
«Computer & Geräte» (80 слов) и «Technik & Computer» (104), — а дедупликация не даёт
одному слову лежать в обеих. Область разрезана пополам, и каждая половина в одиночку
гонится за целью и скребёт дно. Замер: пересечение этих двух тем — РОВНО НОЛЬ слов.

Тем в базе 37, в коде — 21: недостающие 16 добавили позже разрезанием существующих.
Так появились пары «Деньги и банк» / «Экономика и деньги», «Семья и люди» / «Дети»,
«Инструменты» / «Стройка», «Магазин» / «Услуги», «Уборка» / «Дом», «Вечеринки» /
«Праздники». Каждая половина — тема, которой не хватает на полный набор.

РЕШЕНИЕ ВЛАДЕЛЬЦА 19.08.2026: пары слить. Тем станет меньше, но каждая — полноценная,
и день ведёт тема, которую нельзя пройти насквозь за две минуты.

ЧТО ДЕЛАЕТ СКРИПТ, по одной паре:
  1. переносит слова из поглощаемой темы в тему-приёмник; слово, которое там уже
     есть с тем же артиклем, НЕ переносится, а снимается как дубль (уникальный
     индекс theme+слово+артикль этого и не позволил бы);
  2. складывает подтемы обеих тем в тему-приёмник;
  3. переименовывает приёмник, если у объединённой области другое имя;
  4. переводит на приёмник расписание, батлы, наборы и стоп-лист;
  5. гасит поглощённую тему (`active = FALSE`) — НЕ удаляет: её слова и история
     остаются на месте, и решение обратимо.

Пары заданы списком ниже, а не подобраны кодом «по похожести имени»: какие темы
считать одной областью — продуктовое решение, его принимает владелец.

Запуск:
    python -m scripts.artikel_themes_merge_twins            # отчёт
    python -m scripts.artikel_themes_merge_twins --apply
"""
from __future__ import annotations

import argparse
import json
import sys

# (поглощаемая, приёмник, новое имя приёмника или "" — оставить как есть)
# Решение владельца 19.08.2026. Пары, где обе половины про одно и то же, а вместе
# они дают полноценную тему. Не сливаются: «Семья» + «Дети» (вместе 300 — слишком
# крупно), «Кухня» + «Еда» (то же), темы, которым до полного набора немного.
MERGES: list[tuple[str, str, str, str]] = [
    ("computer_geraete", "technik_computer", "Technik & Computer", "Техника и компьютеры"),
    ("haushalt_putzen",  "haus_wohnen",      "Haus & Haushalt",    "Дом и хозяйство"),
    ("bau_montage",      "werkzeug_material", "Werkzeug & Bau",    "Инструменты и стройка"),
    ("party_freizeit",   "feste_traditionen", "Feste & Feiern",    "Праздники и застолья"),
    ("einkauf_laden",    "dienstleistung",   "Einkauf & Service",  "Магазин и услуги"),
]

# Таблицы, где тема встречается ссылкой и должна поехать за словами.
_REPOINT = (
    "bt_3_article_sprint_schedule",
    "bt_3_article_sprint_battles",
    "bt_3_article_sprint_sets",
    "bt_3_article_word_blacklist",
    "bt_3_article_learn_answers",
    "bt_3_article_learn_focus",
)


def theme_counts(cur, key: str) -> tuple[int, int]:
    cur.execute(
        "SELECT COUNT(*) FILTER (WHERE retired = FALSE AND verified), COUNT(*) "
        "FROM bt_3_article_sprint_nouns WHERE theme_key = %s;", (key,))
    row = cur.fetchone()
    return int(row[0] or 0), int(row[1] or 0)


def subtopics_of(cur, key: str) -> list[str]:
    cur.execute("SELECT subtopics_json FROM bt_3_article_sprint_themes WHERE theme_key = %s;", (key,))
    row = cur.fetchone()
    if not row or not row[0]:
        return []
    value = row[0]
    if isinstance(value, str):
        value = json.loads(value)
    return [str(s).strip() for s in (value or []) if str(s).strip()]


def merge_one(cur, source: str, target: str, label_de: str, label_ru: str) -> dict:
    """Одна пара. Возвращает, что произошло — числами."""
    # Дубли: слово+артикль уже есть у приёмника. Переносить нельзя (уникальный
    # индекс), и терять нельзя — снимаем как дубль, с причиной.
    cur.execute(
        """
        UPDATE bt_3_article_sprint_nouns s
           SET retired = TRUE, retire_reason = 'дубль при слиянии тем',
               retire_reviewed = TRUE, updated_at = NOW()
         WHERE s.theme_key = %s AND s.retired = FALSE
           AND EXISTS (SELECT 1 FROM bt_3_article_sprint_nouns t
                        WHERE t.theme_key = %s AND lower(t.word) = lower(s.word)
                          AND t.article = s.article);
        """, (source, target))
    dupes = cur.rowcount or 0

    cur.execute(
        """
        UPDATE bt_3_article_sprint_nouns s
           SET theme_key = %s, updated_at = NOW()
         WHERE s.theme_key = %s
           AND NOT EXISTS (SELECT 1 FROM bt_3_article_sprint_nouns t
                            WHERE t.theme_key = %s AND lower(t.word) = lower(s.word)
                              AND t.article = s.article);
        """, (target, source, target))
    moved = cur.rowcount or 0

    merged_subtopics = subtopics_of(cur, target)
    for sub in subtopics_of(cur, source):
        if sub not in merged_subtopics:
            merged_subtopics.append(sub)

    # Состояние наполнения НЕ трогаем. Темы, помеченные `stopped`, остановил владелец
    # решением 11.08.2026 — снимать это решение слиянием, да ещё молча, нельзя.
    # Объединённой теме, возможно, стоит разрешить добор заново (подтем стало вдвое
    # больше, а «тема выдохлась» почти всегда значит «подтемы узкие») — но это его
    # решение, и он получит его вопросом, а не постфактум.
    cur.execute(
        "UPDATE bt_3_article_sprint_themes "
        "SET subtopics_json = %s::jsonb, label_de = COALESCE(NULLIF(%s, ''), label_de), "
        "    label_ru = COALESCE(NULLIF(%s, ''), label_ru), updated_at = NOW() "
        "WHERE theme_key = %s;",
        (json.dumps(merged_subtopics, ensure_ascii=False), label_de, label_ru, target))

    repointed = 0
    for table in _REPOINT:
        cur.execute(f"UPDATE {table} SET theme_key = %s WHERE theme_key = %s;", (target, source))
        repointed += cur.rowcount or 0

    cur.execute(
        "UPDATE bt_3_article_sprint_themes SET active = FALSE, updated_at = NOW() "
        "WHERE theme_key = %s;", (source,))
    return {"moved": moved, "dupes": dupes, "repointed": repointed,
            "subtopics": len(merged_subtopics)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    from backend.database import get_db_connection_context

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            print(f"{'поглощается':22s} {'приёмник':22s}  было → станет")
            plans = []
            for source, target, label_de, label_ru in MERGES:
                src_live, _ = theme_counts(cur, source)
                dst_live, _ = theme_counts(cur, target)
                plans.append((source, target, src_live, dst_live))
                print(f"{source:22s} {target:22s}  {src_live} + {dst_live} → "
                      f"{src_live + dst_live}   «{label_de}»")
            if not args.apply:
                print("\n(отчёт; чтобы применить — --apply)")
                return 0
            for source, target, label_de, label_ru in MERGES:
                stats = merge_one(cur, source, target, label_de, label_ru)
                print(f"\n{source} → {target}: перенесено {stats['moved']}, "
                      f"снято дублей {stats['dupes']}, ссылок переведено {stats['repointed']}, "
                      f"подтем у приёмника {stats['subtopics']}")
            conn.commit()
    print("\nготово")
    return 0


if __name__ == "__main__":
    sys.exit(main())
