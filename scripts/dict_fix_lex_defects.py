# -*- coding: utf-8 -*-
"""Вычитка слоя единиц: чужие смыслы у омографов и мусор в пояснениях значений.

Найдено сплошным разбором 09.08.2026 (`scripts/lex_senses_audit.py`).

ЧТО ЧИНИМ

1. Семь связей, где смысл протёк между родами или пришёл мусор из старого пула. Каждая
   названа поимённо — никаких эвристик. Связь НЕ удаляется, а понижается в ранге ниже
   порога показа (правило дома: ничего не теряем, но и не показываем). У каждой из этих
   единиц на месте остаётся верный перевод — проверено поштучно перед правкой.

2. 122 значения, где вместо пояснения смысла лежит техническое слово `full_sentence`.
   Пояснение стирается, само значение и его перевод остаются. Это важно не для показа
   (значения человеку не видны), а для моста на английский/испанский/итальянский:
   пояснение уходит модели как контекст, и `full_sentence` дал бы случайный перевод.

Запуск:
    DATABASE_URL=... python3 scripts/dict_fix_lex_defects.py --dry-run
    DATABASE_URL=... python3 scripts/dict_fix_lex_defects.py --apply
"""
from __future__ import annotations

import argparse
import os
import sys

import psycopg2
import psycopg2.extras

DEMOTED_RANK = 950  # ниже порога показа (_DEMOTED_RANK = 900 в backend/lex_units.py)

# (единица, перевод, что показано, почему неверно) — только поимённо.
WRONG_LINKS = [
    (24613, 43013, "der Kiefer → берёза",
     "der Kiefer — челюсть; сосна это die Kiefer, берёза вообще Birke"),
    (11985, 40441, "die Kiefer → «Челюсть, сосна»",
     "склейка двух смыслов, и челюсть принадлежит мужскому роду"),
    (9801, 33607, "der Gehalt → оклад",
     "зарплата это das Gehalt; der Gehalt — содержание, доля"),
    (24626, 33610, "die Kunde → заказчик",
     "заказчик это der Kunde; die Kunde — весть, известие"),
    (24616, 43017, "das Verdienst → премия",
     "das Verdienst — заслуга; премия это Prämie"),
    (28725, 28724, "die Kenntnisnahme → допозна",
     "такого слова нет, мусор из старого пула"),
    (20477, 20476, "Kenntnisnahme → познание",
     "Kenntnisnahme — ознакомление, принятие к сведению"),
]


# Чужие смыслы, засевшие в самом разборе. Пока они там, `sync_unit_links_from_card`
# будет создавать связи заново, и понижение выше окажется бессмысленным.
WRONG_MEANINGS = [
    (24613, ["берёза"], "der Kiefer — только челюсть"),
    (11985, ["челюсть"], "die Kiefer — только сосна; челюсть принадлежит мужскому роду"),
    (9801, ["оклад"], "der Gehalt — содержание, суть; зарплата это das Gehalt"),
    (24626, ["заказчик", "клиент"], "die Kunde — весть, известие; заказчик это der Kunde"),
    (24616, ["заработок", "премия"], "das Verdienst — заслуга; заработок это der Verdienst"),
]


# Примеры употребления подмешиваются в личную карточку (CARD_CONTENT_KEYS в database.py),
# поэтому пример от чужого смысла человек увидит.
WRONG_EXAMPLES = [
    (11985, "Die Kiefer bewegt sich beim Sprechen.",
     "die Kiefer — сосна; пример про челюсть принадлежит мужскому роду"),
]


def norm_value(text) -> str:
    return " ".join(str(text or "").split()).casefold()


def meaning_values(card: dict) -> list[str]:
    meanings = card.get("meanings") if isinstance(card.get("meanings"), dict) else {}
    out = []
    primary = meanings.get("primary")
    if isinstance(primary, dict) and str(primary.get("value") or "").strip():
        out.append(str(primary["value"]).strip())
    for item in (meanings.get("secondary") or []):
        if isinstance(item, dict) and str(item.get("value") or "").strip():
            out.append(str(item["value"]).strip())
    return out


def strip_meanings(card: dict, drop_values: list[str]) -> tuple[dict, list[str]]:
    """Убрать названные смыслы из разбора. Если ушёл главный — главным становится
    первый из оставшихся, порядок прочих сохраняется. Возвращает (новый разбор, что убрали)."""
    drop = {norm_value(v) for v in drop_values}
    new_card = dict(card)
    meanings = dict(card.get("meanings") or {}) if isinstance(card.get("meanings"), dict) else {}
    removed: list[str] = []

    kept: list[dict] = []
    primary = meanings.get("primary")
    if isinstance(primary, dict) and str(primary.get("value") or "").strip():
        if norm_value(primary.get("value")) in drop:
            removed.append(str(primary["value"]).strip())
        else:
            kept.append(primary)
    for item in (meanings.get("secondary") or []):
        if not isinstance(item, dict) or not str(item.get("value") or "").strip():
            continue
        if norm_value(item.get("value")) in drop:
            removed.append(str(item["value"]).strip())
        else:
            kept.append(item)
    if kept:
        meanings["primary"] = kept[0]
        meanings["secondary"] = kept[1:]
        new_card["meanings"] = meanings

    translations = card.get("translations")
    if isinstance(translations, list):
        fresh = []
        for item in translations:
            value = item.get("value") if isinstance(item, dict) else item
            if norm_value(value) in drop:
                removed.append(str(value).strip())
                continue
            fresh.append(item)
        new_card["translations"] = fresh
    return new_card, removed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    apply = bool(args.apply) and not args.dry_run

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("нет DATABASE_URL"); return 2
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    print("1. ПОНИЖЕНИЕ ЧУЖИХ СМЫСЛОВ У ОМОГРАФОВ")
    for from_unit, to_unit, label, why in WRONG_LINKS:
        cur.execute(
            """SELECT l.rank, l.source,
                      (SELECT COUNT(*) FROM bt_3_lex_links x
                       JOIN bt_3_lex_units t ON t.id = x.to_unit
                       WHERE x.from_unit = l.from_unit AND t.lang = 'ru'
                         AND x.rank < %s AND x.to_unit <> l.to_unit) AS останется
               FROM bt_3_lex_links l WHERE l.from_unit = %s AND l.to_unit = %s;""",
            (900, from_unit, to_unit),
        )
        found = cur.fetchone()
        if not found:
            print(f"   ⚠ {label}: связи уже нет, пропускаю")
            continue
        rank, source, remaining = found
        if int(remaining or 0) < 1:
            print(f"   ⚠ {label}: это ЕДИНСТВЕННЫЙ перевод единицы — не трогаю")
            continue
        print(f"   {label}\n      почему: {why}\n      ранг {rank} ({source}) → {DEMOTED_RANK}; "
              f"у единицы остаётся переводов: {remaining}")
        if apply:
            cur.execute(
                "UPDATE bt_3_lex_links SET rank = %s, source = 'вычитка' "
                "WHERE from_unit = %s AND to_unit = %s;",
                (DEMOTED_RANK, from_unit, to_unit),
            )

    print("\n3. ЧУЖОЙ СМЫСЛ В САМОМ РАЗБОРЕ (иначе понижение отменится при обогащении)")
    backup: list[dict] = []
    for unit_id, drop_values, why in WRONG_MEANINGS:
        cur.execute("SELECT display, card FROM bt_3_lex_units WHERE id = %s;", (unit_id,))
        found = cur.fetchone()
        if not found:
            print(f"   ⚠ единицы {unit_id} нет"); continue
        display, card = found
        card = card if isinstance(card, dict) else {}
        new_card, removed = strip_meanings(card, drop_values)
        if not removed:
            print(f"   {display}: нечего убирать"); continue
        kept = meaning_values(new_card)
        if not kept:
            print(f"   ⚠ {display}: после чистки не осталось ни одного смысла — не трогаю")
            continue
        print(f"   {display}\n      почему: {why}\n      убираю: {', '.join(removed)}"
              f"\n      остаётся: {', '.join(kept)}")
        if apply:
            backup.append({"unit_id": unit_id, "card": card})
            cur.execute("UPDATE bt_3_lex_units SET card = %s, updated_at = NOW() WHERE id = %s;",
                        (psycopg2.extras.Json(new_card), unit_id))

    print("\n4. ПРИМЕР ОТ ЧУЖОГО СМЫСЛА (примеры подмешиваются в личную карточку)")
    for unit_id, example_source, why in WRONG_EXAMPLES:
        cur.execute("SELECT display, card FROM bt_3_lex_units WHERE id = %s;", (unit_id,))
        found = cur.fetchone()
        if not found:
            print(f"   ⚠ единицы {unit_id} нет"); continue
        display, card = found
        card = card if isinstance(card, dict) else {}
        examples = card.get("usage_examples")
        if not isinstance(examples, list):
            print(f"   {display}: примеров нет"); continue
        kept = [x for x in examples
                if not (isinstance(x, dict) and norm_value(x.get("source")) == norm_value(example_source))]
        if len(kept) == len(examples):
            print(f"   {display}: пример уже убран"); continue
        if not kept:
            print(f"   ⚠ {display}: это единственный пример — не трогаю"); continue
        print(f"   {display}\n      почему: {why}\n      убираю: {example_source}"
              f"\n      остаётся примеров: {len(kept)}")
        if apply:
            backup.append({"unit_id": unit_id, "card": card})
            new_card = dict(card); new_card["usage_examples"] = kept
            cur.execute("UPDATE bt_3_lex_units SET card = %s, updated_at = NOW() WHERE id = %s;",
                        (psycopg2.extras.Json(new_card), unit_id))

    print("\n2. ПОЯСНЕНИЯ СО СЛОВОМ full_sentence")
    cur.execute("SELECT COUNT(*) FROM bt_3_lex_senses WHERE note = 'full_sentence';")
    total = int(cur.fetchone()[0])
    print(f"   найдено значений: {total} — пояснение стираем, значение и перевод остаются")
    if apply and total:
        cur.execute("UPDATE bt_3_lex_senses SET note = NULL WHERE note = 'full_sentence';")
        print(f"   обновлено строк: {cur.rowcount}")

    if apply:
        if backup:
            import json as _json
            path = "/private/tmp/claude-501/-Users-alexandr-Desktop-TELEGRAM-BOT-DEUTSCHESPRACHE/279651db-94f1-4953-9ff2-37f22326bd0f/scratchpad/cards_backup.json"
            with open(path, "w", encoding="utf-8") as fh:
                _json.dump(backup, fh, ensure_ascii=False, indent=1, default=str)
            print(f"\nстарые разборы: {path}")
        conn.commit()
        print("записано")
    else:
        conn.rollback()
        print("\nсухой прогон — база не менялась")
    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
