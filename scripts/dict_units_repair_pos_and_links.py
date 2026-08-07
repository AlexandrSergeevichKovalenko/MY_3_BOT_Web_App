"""Две недоделки слоя, найденные аудитом 07.08.2026. Модель не спрашивается вообще.

1. ЧАСТЬ РЕЧИ. У 75 слов метка не «существительное», хотя слово пишется с заглавной:
   «Katzenjammer», «Schnürsenkel», «Krankenwagen» помечены «выражением». Человек видит
   неверный чип под заголовком, а артикль рядом со словом не печатается — для языкового
   приложения это ошибка по существу.

   Но заглавная буква сама по себе НЕ признак: «Dennoch», «Vorne», «Inzwischen» —
   наречия, «Aufzudecken», «Prägen» — глаголы, и метки у них верные; человек просто
   набрал их с заглавной. Поэтому решает не буква, а два источника:
     • разбор слова прямо говорит «noun» — этого достаточно, он куплен про это слово;
     • иначе спрашиваем справочник родов (`article_authority`, Wiktionary + список
       двуродовых). Знает род — значит существительное. Молчит — НЕ ТРОГАЕМ.
   Догадка по окончанию сюда не пускается: ею когда-то «der Vorsitzende» превратился в
   «das Vorsitzende».

2. РАЗБОР БЕЗ ПЕРЕВОДА. У 25 слов разбор куплен и лежит, а связи с русским словом нет.
   `lex_units.lookup` такое слово не отдаёт вовсе («перевода нет — отдавать нечего»), и
   за него платят ВТОРОЙ раз. Переводы лежат внутри самого разбора — раскладываем их по
   связям тем же кодом, что и ночной добор (`sync_unit_links_from_card`). Бесплатно.

Опознание единицы — это лемма + часть речи + род, поэтому правка МЕНЯЕТ ключ, по которому
слово находят. Если рядом уже живёт такое же слово с такой меткой, строку пропускаем:
сливать единицы без решения владельца нельзя.

По умолчанию НИЧЕГО НЕ ПИШЕТ.

    python scripts/dict_units_repair_pos_and_links.py           # вхолостую
    python scripts/dict_units_repair_pos_and_links.py --apply
"""

from __future__ import annotations

import argparse
import os
import re
import sys

_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, ".."))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

import lex_units  # noqa: E402
from database import get_db_connection_context  # noqa: E402

ARTICLE_RE = re.compile(r"^(der|die|das|den|dem|des)\s+", re.I)
ARTICLE_TO_GENDER = {"der": "der", "die": "die", "das": "das"}


def bare(text: str) -> str:
    return ARTICLE_RE.sub("", re.sub(r"\s+", " ", str(text or "").strip())).strip()


def collect_pos(cur) -> list[dict]:
    """Одиночные слова с заглавной буквы, помеченные НЕ существительным."""
    cur.execute(
        """SELECT id, lang, kind, lemma_key, display, pos, gender,
                  card->>'part_of_speech', card->>'article'
           FROM bt_3_lex_units
           WHERE lang = 'de' AND kind = 'word' AND pos IS NOT NULL AND pos <> 'noun'
           ORDER BY id;"""
    )
    out = []
    for uid, lang, kind, key, display, pos, gender, card_pos, card_article in cur.fetchall() or []:
        body = bare(display)
        if not body or " " in body or not body[:1].isupper():
            continue
        out.append({"id": uid, "lang": lang, "kind": kind, "lemma_key": key, "display": display,
                    "pos": pos, "gender": gender, "body": body,
                    "card_pos": str(card_pos or "").strip().lower(),
                    "card_article": str(card_article or "").strip().lower()})
    return out


def noun_gender(unit: dict) -> tuple[str | None, str]:
    """Род слова и откуда он взят, или (None, причина молчания)."""
    if unit["card_pos"] == "noun":
        return ARTICLE_TO_GENDER.get(unit["card_article"]) or unit["gender"], "разбор"
    try:
        from backend.article_authority import authoritative_article
        verdict, source = authoritative_article(unit["body"], allow_network=True)
    except Exception as exc:
        return None, "справочник недоступен (%s)" % type(exc).__name__
    if verdict in ARTICLE_TO_GENDER:
        return verdict, "справочник (%s)" % source
    return None, "справочник молчит — не существительное"


def looks_like_noun_to_model(unit: dict) -> bool:
    """Последний рубеж — и только для метки «выражение».

    «Выражение» у одиночного слова это отговорка разбора, а не разбор: «Bugfahrwerk»,
    «Kurzbefehl», «Einwegbecher» — обычные составные существительные, которых просто нет
    в Wiktionary. А вот «наречие», «сравнительная степень», «артикль» — это уже сказанное
    про слово, и переспрашивать его мы не будем.

    У модели берём ТОЛЬКО ответ «это существительное». Род у неё не берём: замер
    29.07.2026 дал 10 ошибок из 20 на числе и роде, поэтому род в этой программе говорит
    справочник и никто больше. Слово останется без артикля — но с верной частью речи,
    а артикль доберёт ночной проход, когда справочник его узнает."""
    if unit["pos"] != "phrase":
        return False
    try:
        from openai_manager import run_quick_article_facts
        facts = run_quick_article_facts(word=unit["body"].rstrip("."))
    except Exception:
        return False
    return str((facts or {}).get("article") or "").strip() in ARTICLE_TO_GENDER


def identity_taken(cur, unit: dict, *, gender: str | None) -> bool:
    cur.execute(
        """SELECT 1 FROM bt_3_lex_units
           WHERE lang = %s AND kind = %s AND lemma_key = %s AND COALESCE(pos, '') = 'noun'
             AND COALESCE(gender, '') = COALESCE(%s, '') AND id <> %s LIMIT 1;""",
        (unit["lang"], unit["kind"], unit["lemma_key"], gender, unit["id"]),
    )
    return bool(cur.fetchone())


def collect_linkless(cur) -> list[dict]:
    """Слова с разбором, но без единой связи на русский: поиск их не отдаёт."""
    cur.execute(
        """SELECT u.id, u.display, u.card
           FROM bt_3_lex_units u
           WHERE u.lang = 'de' AND u.kind = 'word' AND u.card IS NOT NULL
             AND NOT EXISTS (
                   SELECT 1 FROM bt_3_lex_links l
                     JOIN bt_3_lex_units t ON t.id = l.to_unit
                    WHERE l.from_unit = u.id AND t.lang = 'ru')
           ORDER BY u.id;"""
    )
    return [{"id": r[0], "display": r[1], "card": r[2] if isinstance(r[2], dict) else {}}
            for r in cur.fetchall() or []]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            pos_candidates = collect_pos(cur)
            linkless = collect_linkless(cur)

    print("ЧАСТЬ РЕЧИ: слов с заглавной, помеченных не существительным: %d" % len(pos_candidates))
    print("РАЗБОР БЕЗ ПЕРЕВОДА: слов, которые поиск не отдаёт: %d" % len(linkless))
    if not args.apply:
        for u in pos_candidates[:20]:
            print("   %-6s %r метка=%s разбор=%s" % (u["id"], u["display"][:30], u["pos"], u["card_pos"] or "—"))
        for u in linkless[:10]:
            print("   %-6s %r без перевода" % (u["id"], u["display"][:30]))
        print()
        print("ВХОЛОСТУЮ. Записать: --apply")
        return 0

    fixed_pos, kept_pos = [], []
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for unit in pos_candidates:
                gender, why = noun_gender(unit)
                if unit["card_pos"] != "noun" and gender is None:
                    if looks_like_noun_to_model(unit):
                        why = "метка была «выражение», модель говорит «существительное» (род не ставим)"
                    else:
                        kept_pos.append((unit, why))
                        continue
                if identity_taken(cur, unit, gender=gender):
                    kept_pos.append((unit, "такое слово с меткой «существительное» уже есть"))
                    continue
                # Точка на конце одиночного слова — мусор набора: «Schneebesen.» это то же
                # слово. У фразы точка законна, поэтому чистим только одиночные.
                clean_display = unit["display"].rstrip(".") if " " not in bare(unit["display"]) else unit["display"]
                cur.execute(
                    """UPDATE bt_3_lex_units
                       SET pos = 'noun', pos_source = 'справочник',
                           gender = COALESCE(%s, gender),
                           gender_source = CASE WHEN %s IS NULL THEN gender_source
                                                ELSE COALESCE(gender_source, 'справочник') END,
                           display = %s, lemma = %s,
                           updated_at = NOW()
                       WHERE id = %s;""",
                    (gender, gender, clean_display, bare(clean_display), unit["id"]),
                )
                conn.commit()
                fixed_pos.append((unit, gender, why))
                print("   %-6s %r: %s → существительное%s   [%s]"
                      % (unit["id"], unit["display"][:30], unit["pos"],
                         " (%s)" % gender if gender else "", why))

    filled, empty = 0, 0
    for unit in linkless:
        try:
            report = lex_units.sync_unit_links_from_card(unit["id"], unit["card"])
        except Exception as exc:
            print("   %-6s %r: связи не разложились (%s)" % (unit["id"], unit["display"][:30], type(exc).__name__))
            continue
        added = int((report or {}).get("links") or (report or {}).get("added") or 0)
        if added:
            filled += 1
            print("   %-6s %r: переводов разложено %d" % (unit["id"], unit["display"][:30], added))
        else:
            empty += 1

    # Третий шаг. Слово с разбором, у которого переводов так и не нашлось, отдать нельзя:
    # `lookup` его пропускает, и за слово платят второй раз. Значит разбор непригоден —
    # снимаем, и ночь соберёт заново (очередь как раз «слово без разбора»). А если на
    # единицу вдобавок никто не ссылается — это не слово, а мусор: «die спокойствие»,
    # «pflanze» с разбором про «растение». Такому не поможет и пересборка.
    dropped_cards, dropped_units = 0, 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for unit in collect_linkless(cur):
                cur.execute("SELECT kind FROM bt_3_lex_units WHERE id = %s;", (unit["id"],))
                row = cur.fetchone()
                if not row or str(row[0]) != "word":
                    continue   # у фразы перевод живёт в личной карточке, это не дефект
                cur.execute(
                    """SELECT
                         (SELECT count(*) FROM bt_3_webapp_dictionary_queries q WHERE q.lex_unit_id = %s)
                       + (SELECT count(*) FROM bt_3_lex_links l WHERE l.from_unit = %s OR l.to_unit = %s)
                       + (SELECT count(*) FROM bt_3_lex_unit_sources s WHERE s.unit_id = %s);""",
                    (unit["id"], unit["id"], unit["id"], unit["id"]),
                )
                held = int((cur.fetchone() or [0])[0] or 0)
                if held == 0:
                    cur.execute("DELETE FROM bt_3_lex_units WHERE id = %s;", (unit["id"],))
                    dropped_units += cur.rowcount or 0
                    print("   %-6s %r: ничем не держится — убрано" % (unit["id"], unit["display"][:34]))
                else:
                    cur.execute(
                        """UPDATE bt_3_lex_units SET card = NULL, card_source = NULL, updated_at = NOW()
                           WHERE id = %s;""",
                        (unit["id"],),
                    )
                    dropped_cards += cur.rowcount or 0
                    print("   %-6s %r: разбор непригоден — снят под ночную пересборку"
                          % (unit["id"], unit["display"][:34]))
            conn.commit()

    print()
    print("НЕПРИГОДНЫХ РАЗБОРОВ СНЯТО: %d, мусорных слов убрано: %d" % (dropped_cards, dropped_units))
    print("ЧАСТЬ РЕЧИ ИСПРАВЛЕНА: %d, оставлено как есть: %d" % (len(fixed_pos), len(kept_pos)))
    for unit, why in kept_pos:
        print("   %-6s %-32r %s" % (unit["id"], unit["display"][:30], why))
    print("ПЕРЕВОДЫ РАЗЛОЖЕНЫ: %d, в разборе переводов не нашлось: %d" % (filled, empty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
