# -*- coding: utf-8 -*-
"""Разрезание «свалок» в переводах на отдельные значения.

Общий банк хранил перевод строкой, поэтому в него попадали записи вида
«1прикладывать; накладывать, приставлять 2 надевать 3 строить, закладывать» — одно
«значение», внутри которого их шесть. Человеку это показывать нельзя, тренировать по
такому — тем более: карточка с шестью смыслами ломает интервальное повторение.

Так это устроено у нормальных словарей (Wiktionary/Wikidata, PONS, Duden): у значения
свой номер, переводы висят на значении, а пометки («перен.», «разг.», «Genitiv/Dativ»)
лежат отдельными полями, а не внутри текста.

Что делает скрипт:
  • находит переводы-свалки у немецких слов;
  • режет их по номерам значений и точке с запятой;
  • вынимает пометки из скобок в отдельное поле;
  • заводит значения (bt_3_lex_senses) и вешает на них связи;
  • исходную свалку НЕ удаляет, а понижает в ранге — данные не теряем, но и не
    показываем: правило «ничего не удаляем» остаётся в силе.

Запуск:
    DATABASE_URL=... python3 scripts/dict_units_split_senses.py --dry-run
    DATABASE_URL=... python3 scripts/dict_units_split_senses.py --apply
"""
from __future__ import annotations

import argparse
import os
import re
import time

import psycopg2

SPACE_RE = re.compile(r"\s+")
# «1прикладывать … 2 надевать … 3 строить» — номер значения может стоять и без пробела.
SENSE_NUM_RE = re.compile(r"(?:^|[\s;,])(\d{1,2})\s*[).]?\s*(?=[А-Яа-яЁёA-Za-z])")
# Пометки в скобках: «(перен.)», «(разг.)», «(Genitiv/Dativ)», «(напр. письма)».
PAREN_RE = re.compile(r"\s*\(([^)]{1,60})\)\s*")
GRAMMAR_NOTE_RE = re.compile(
    r"\b(genitiv|dativ|akkusativ|nominativ|plural|singular|мн\.?\s*ч|ед\.?\s*ч)\b", re.I)
# Словарные пометки в начале значения: «vi причаливать», «vt. ставить», «2. …».
GRAMMAR_MARKER_RE = re.compile(r"^((?:v[itrp]|vimp|refl|adj|adv|разг|перен|уст)\.?)\s+", re.I)
# Ранг, в который отправляется исходная свалка: показывать её незачем, а терять нельзя.
DEMOTED_RANK = 900


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


def split_dump(text: str) -> list[dict]:
    """Свалка → список значений: [{"value": "прикладывать", "label": "перен."}, …].

    Режем по номерам значений; внутри каждого номера — по точке с запятой. Запятую НЕ
    трогаем: «строить, закладывать, сооружать» — это оттенки одного значения, и дробить
    их на отдельные карточки было бы хуже, чем оставить вместе."""
    raw = SPACE_RE.sub(" ", str(text or "").strip())
    if not raw:
        return []
    # Границы значений по номерам.
    marks = [(m.start(1), m.end(0), m.group(1)) for m in SENSE_NUM_RE.finditer(raw)]
    chunks: list[str] = []
    if len(marks) >= 2:
        for index, (start, body_start, _num) in enumerate(marks):
            end = marks[index + 1][0] if index + 1 < len(marks) else len(raw)
            chunks.append(raw[body_start:end])
        head = raw[:marks[0][0]].strip(" ;,")
        if head:
            chunks.insert(0, head)
    else:
        chunks = [raw]

    out: list[dict] = []
    for chunk in chunks:
        for piece in re.split(r"\s*;\s*", chunk):
            label_parts: list[str] = []

            def _grab(match):
                label_parts.append(match.group(1).strip())
                return " "

            value = PAREN_RE.sub(_grab, piece).strip(" ,;.—-")
            value = SPACE_RE.sub(" ", value)
            # Словарные пометки «vi», «vt», «vr», «1.» в начале — это грамматика, а не
            # перевод: «vi причаливать» человеку показывать нельзя.
            marker = GRAMMAR_MARKER_RE.match(value)
            if marker:
                label_parts.append(marker.group(1))
                value = value[marker.end():].strip(" ,;.—-")
            value = re.sub(r"\s+([,;])", r"\1", value)  # «накладывать , приставлять»
            if not value or len(value) < 2:
                continue
            # Чисто грамматическая пометка — это не перевод, а служебная запись.
            if not re.search(r"[А-Яа-яЁёA-Za-z]", value):
                continue
            label = "; ".join(p for p in label_parts if p and not GRAMMAR_NOTE_RE.search(p))
            if GRAMMAR_NOTE_RE.search(value):
                continue
            out.append({"value": value, "label": label})
    # Дубликаты внутри одной свалки схлопываем: «приют … приют» — одно значение.
    seen: set[str] = set()
    unique: list[dict] = []
    for item in out:
        key = norm(item["value"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def looks_like_dump(text: str) -> bool:
    raw = str(text or "").strip()
    if len(raw) > 45:
        return True
    if ";" in raw:
        return True
    if len(SENSE_NUM_RE.findall(raw)) >= 2:
        return True
    return bool(PAREN_RE.search(raw))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    if not args.dry_run and not args.apply:
        raise SystemExit("укажи --dry-run или --apply")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("нужен DATABASE_URL")

    conn = connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()
    if args.apply:
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(here, "backend", "lex_units_schema.sql"), encoding="utf-8") as fh:
            cur.execute(fh.read())

    if args.apply:
        # Заготовки упражнений («Er ___ heute früh mit dem Projekt») переводами не
        # являются: у «anlegen» их 33 штуки, и они забивали выдачу так, что слово
        # оставалось вообще без перевода. Понижаем их тем же способом — не удаляя.
        cur.execute(
            """
            UPDATE bt_3_lex_links l SET rank = %s, source = 'упражнение'
            FROM bt_3_lex_units u
            WHERE u.id = l.to_unit AND position('___' in u.display) > 0 AND l.rank < %s;
            """,
            (DEMOTED_RANK, DEMOTED_RANK),
        )
        print("заготовок упражнений понижено: %d" % cur.rowcount)
        conn.commit()

    cur.execute(
        """
        SELECT de.id, de.display, de.lemma_key, ru.id, ru.display, ru.lang, l.rank
        FROM bt_3_lex_links l
        JOIN bt_3_lex_units de ON de.id = l.from_unit AND de.lang = 'de' AND de.kind = 'word'
        JOIN bt_3_lex_units ru ON ru.id = l.to_unit AND ru.lang <> 'de'
        WHERE l.rank < %s
        ORDER BY de.id;
        """,
        (DEMOTED_RANK,),
    )
    rows = cur.fetchall()
    dumps = [r for r in rows if looks_like_dump(r[4])]
    if args.limit:
        dumps = dumps[: args.limit]
    print("переводов у немецких слов: %d, из них свалок: %d" % (len(rows), len(dumps)))

    made_senses = made_units = relinked = demoted = untouched = failed = 0
    shown = 0
    for de_id, de_display, _lemma_key, ru_id, ru_display, ru_lang, _rank in dumps:
        parts = split_dump(ru_display)
        if len(parts) < 2:
            untouched += 1
            continue
        if shown < 8:
            shown += 1
            print("\n  %s ← %r" % (de_display, ru_display[:70]))
            for index, part in enumerate(parts, 1):
                print("      %d. %s%s" % (index, part["value"],
                                          ("   [%s]" % part["label"]) if part["label"] else ""))
        if not args.apply:
            made_senses += len(parts)
            made_units += len(parts)
            demoted += 1
            continue

        try:
          for sense_no, part in enumerate(parts, 1):
            cur.execute(
                """
                INSERT INTO bt_3_lex_senses (unit_id, sense_no, label, source)
                VALUES (%s, %s, %s, 'разрез')
                ON CONFLICT (unit_id, sense_no) DO UPDATE SET label = EXCLUDED.label
                RETURNING id;
                """,
                (de_id, sense_no, part["label"] or None),
            )
            sense_id = cur.fetchone()[0]
            made_senses += 1
            value = part["value"]
            kind = "word" if " " not in value else (
                "sentence" if len(value.split()) > 4 else "collocation")
            cur.execute(
                """
                INSERT INTO bt_3_lex_units (lang, kind, lemma, lemma_key, display, card_source)
                VALUES (%s, %s, %s, %s, %s, 'разрез')
                ON CONFLICT (lang, kind, lemma_key, COALESCE(pos, ''), COALESCE(gender, ''))
                DO UPDATE SET updated_at = NOW()
                RETURNING id;
                """,
                (ru_lang, kind, value, norm(value), value),
            )
            part_id = cur.fetchone()[0]
            made_units += 1
            cur.execute(
                """
                INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                VALUES (%s, %s, %s, 'exact') ON CONFLICT DO NOTHING;
                """,
                (ru_lang, norm(value), part_id),
            )
            for a, b in ((de_id, part_id), (part_id, de_id)):
                cur.execute(
                    """
                    INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source, sense_id)
                    VALUES (%s, %s, %s, 'разрез', %s)
                    ON CONFLICT (from_unit, to_unit) DO UPDATE
                      SET rank = LEAST(bt_3_lex_links.rank, EXCLUDED.rank),
                          sense_id = COALESCE(bt_3_lex_links.sense_id, EXCLUDED.sense_id);
                    """,
                    (a, b, 10 + sense_no - 1, sense_id),
                )
            relinked += 1
          # Свалку не удаляем — понижаем, чтобы она не показывалась, но осталась в базе.
          cur.execute(
              "UPDATE bt_3_lex_links SET rank = %s, source = 'свалка' "
              "WHERE (from_unit, to_unit) IN ((%s,%s),(%s,%s));",
              (DEMOTED_RANK, de_id, ru_id, ru_id, de_id),
          )
          demoted += 1
          # Фиксируем каждую свалку отдельно: проход длинный, а прокси базы рвёт
          # соединение — иначе один обрыв стоил бы всей работы.
          conn.commit()
        except psycopg2.Error as exc:
          failed += 1
          print("    свалка %r не записалась (%s), переподключаюсь" % (de_display[:24], str(exc)[:60]))
          try:
              conn.close()
          except Exception:
              pass
          conn = connect(dsn)
          conn.autocommit = False
          cur = conn.cursor()

    print("\nитог: значений %d, единиц-переводов %d, связей %d, свалок понижено %d, "
          "не поддалось разрезу %d, сорвалось %d"
          % (made_senses, made_units, relinked, demoted, untouched, failed))
    if args.apply:
        conn.commit()
        print("записано (повторный запуск дочистит то, что сорвалось).")
    else:
        conn.rollback()
        print("(--dry-run: в базу ничего не записано)")
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
