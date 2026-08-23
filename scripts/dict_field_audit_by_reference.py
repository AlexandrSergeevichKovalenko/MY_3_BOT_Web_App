# -*- coding: utf-8 -*-
"""ПЕРВЫЙ ШАГ СПЛОШНОГО ПРОХОДА: сверить с справочником то, что он знает точно.

ПОЧЕМУ ЭТО ПЕРВЫМ. Владелец, 23.08.2026: мы полгода ловим классы ошибок, и конца этому
нет — потому что у класса конца и не бывает. У карточек конец есть: их 15 656. Значит
идём по карточкам, а не по болезням, и на каждой ставим отметку, чтобы больше к ней не
возвращаться. Начинаем с того, что стоит НОЛЬ: справочник уже лежит у нас в базе.

ЧТО ЭТОТ ПРОХОД ДЕЛАЕТ И ЧЕГО НЕ ДЕЛАЕТ. Он НЕ чинит. Он ставит по каждому полю один из
трёх вердиктов — и это главное отличие от прежней работы, где было только «нашли ошибку»:

    подтверждено      источник знает ответ, и он совпал с нашим;
    расхождение       источник знает ответ, и он ДРУГОЙ — карточка идёт на разбор;
    источник молчит   ответа нет ни в одной таблице. Это НЕ «всё хорошо»: это наряд
                      достроить источник, и он виден числом.

Расхождение НЕ применяется молча. Сегодняшняя сверка показала, почему: у «der Eimer»
наш ответ «die Eimer» верен, а справочник даёт «Eimeren» — мусорная строка. Слепая запись
испортила бы верные данные. Решает второй источник или владелец, а не этот скрипт.

ИСТОЧНИКИ, ВСЕ УЖЕ В БАЗЕ, НИ ОДНОГО ЗАПРОСА В СЕТЬ И К МОДЕЛИ:
    род               bt_3_wiktionary_genus_cache   (22 988 строк)
    множественное     bt_3_wiktionary_forms         (24 764)
    склонение         bt_3_german_noun_declensions  ← пополняет german_reference_forms
    спряжение         bt_3_german_verb_paradigms    ← пополняет german_verb_paradigms
    степени сравнения bt_3_german_adjective_degrees

⚠ СВОЕГО КЭША ФОРМ ЗДЕСЬ НЕТ И НЕ БУДЕТ. Таблицы выше наполняются чужим кодом
(`paradigm_for_verb`, `noun_declension_for`, `adjective_degrees_for`); мы их только
ЧИТАЕМ. Второй кэш тех же форм означал бы два разных ответа на один вопрос.

⚠ ЛОВУШКИ СРАВНЕНИЯ, пойманные на живых данных 23.08.2026 — не убирать:
  • род у нас хранится «der/die/das», в справочнике «m/f/n». Сравнение в лоб давало
    100% брака на 2 779 словах;
  • «-» в справочнике означает «не знаю», а не ответ. Считался расхождением — давал
    441 несуществующую ошибку;
  • у слова бывает ДВА рода («das/der Verdienst», «die/der Haft»): справочник пишет их
    вместе («mn», «fm»). Наш ответ, входящий в эту пару, — верный, а не расхождение;
  • у множественного мы храним с артиклем («die Eimer»), справочник — без.

    python3 scripts/dict_field_audit_by_reference.py            # посчитать, ничего не писать
    python3 scripts/dict_field_audit_by_reference.py --apply    # записать отметки
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")

from backend.database import get_db_connection_context      # noqa: E402

CONFIRMED, CONFLICT, SILENT = "подтверждено", "расхождение", "источник молчит"
ARTICLE_TO_GENUS = {"der": "m", "die": "f", "das": "n"}
_ARTICLE_PREFIX = re.compile(r"^(?:der|die|das)\s+", re.I)


def bare(word: str) -> str:
    """Слово без артикля: в справочнике заголовок стоит голым."""
    return _ARTICLE_PREFIX.sub("", str(word or "").strip()).strip()


def ensure_schema(cur) -> None:
    """Реестр отметок: одна строка на ПОЛЕ карточки, а не на карточку целиком.

    Владелец 23.08.2026: «проверять нужно каждое поле». Поэтому ключ — (единица, поле):
    у одной карточки род может быть подтверждён справочником, а перевод ещё не смотрел
    никто, и это два разных состояния, а не одно «карточка проверена».
    """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bt_3_field_checks (
            unit_id     BIGINT NOT NULL,
            field       TEXT   NOT NULL,
            verdict     TEXT   NOT NULL,
            source      TEXT   NOT NULL,      -- кто проверял: справочник по имени
            ours        TEXT,                 -- что лежало у нас
            reference   TEXT,                 -- что говорит источник
            checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (unit_id, field)
        );""")
    cur.execute("""CREATE INDEX IF NOT EXISTS bt_3_field_checks_verdict
                   ON bt_3_field_checks (verdict, field);""")


def stamp(cur, unit_id: int, field: str, verdict: str, source: str,
          ours=None, reference=None) -> None:
    cur.execute("""
        INSERT INTO bt_3_field_checks (unit_id, field, verdict, source, ours, reference, checked_at)
        VALUES (%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (unit_id, field) DO UPDATE
           SET verdict=EXCLUDED.verdict, source=EXCLUDED.source, ours=EXCLUDED.ours,
               reference=EXCLUDED.reference, checked_at=NOW();""",
                (unit_id, field, verdict, source,
                 None if ours is None else str(ours)[:400],
                 None if reference is None else str(reference)[:400]))


def check_gender(unit, genus: dict) -> tuple[str, str, str] | None:
    """Род существительного. Возвращает (вердикт, наше, справочник)."""
    if unit["pos"] != "noun":
        return None
    ours_article = str(unit["gender"] or "").strip().lower()
    if not ours_article:
        return (SILENT, "", "")           # у нас его нет — это тоже незакрытая клетка
    ref = genus.get(bare(unit["display"]).lower())
    if not ref or ref == "-":
        return (SILENT, ours_article, "")
    ours = ARTICLE_TO_GENUS.get(ours_article)
    if not ours:
        return (SILENT, ours_article, ref)
    # «mn» / «fm» — у слова два рода; наш ответ верен, если он один из них.
    return (CONFIRMED if ours in ref else CONFLICT, ours_article, ref)


def check_plural(unit, plurals: dict) -> tuple[str, str, str] | None:
    if unit["pos"] != "noun":
        return None
    ours = str(((unit["card"] or {}).get("forms") or {}).get("plural") or "").strip()
    ref = plurals.get(bare(unit["display"]).lower())
    if not ref:
        return (SILENT, ours, "")
    if not ours:
        return (SILENT, "", ref)
    same = bare(ours).split("(")[0].strip().lower() == ref.strip().lower()
    return (CONFIRMED if same else CONFLICT, ours, ref)


def check_verb_forms(unit, paradigms: dict) -> tuple[str, str, str] | None:
    """Спряжение: сверяем Präteritum и Perfekt с документированной таблицей."""
    if unit["pos"] != "verb":
        return None
    forms = (unit["card"] or {}).get("forms") or {}
    ours = " / ".join(str(forms.get(k) or "").strip() for k in ("praeteritum", "perfekt")).strip(" /")
    table = paradigms.get(bare(unit["display"]).lower())
    if not table:
        return (SILENT, ours, "")
    if not ours:
        return (SILENT, "", "есть в справочнике")
    # ⚠ ОТДЕЛЯЕМЫЕ ГЛАГОЛЫ. Справочник печатает Präteritum РАЗДЕЛЁННЫМ («fügte zu»),
    # а у нас в карточке слитно («zufügte»). Первая версия этой проверки сравнивала
    # слова напрямую и объявила расхождением 74 глагола, из которых почти все были
    # верны — ошибка была в сравнении, а не в данных (поймано 23.08.2026).
    #
    # Поэтому сравниваем по БУКВАМ: «zufügte» и «fügte zu» — один и тот же набор.
    # Это сопоставление текста, а не вывод формы: мы ничего не достраиваем.
    def letters(value: str) -> str:
        return "".join(sorted(ch for ch in str(value).lower() if ch.isalpha()))

    # Местоимения и вспомогательные глаголы в сравнении не участвуют: у нас в карточке
    # «er/sie/es laugte aus», в справочнике «laugte aus» — это одна и та же форма,
    # разница только в оформлении. (Первые две версии проверки спотыкались об это.)
    SERVICE = {"er", "sie", "es", "ich", "du", "wir", "ihr", "man",
               "hat", "hatte", "ist", "war", "haben", "sind", "wird", "werden"}

    cell_words: set[str] = set()
    for cell in re.findall(r'"([^"]{2,80})"', json.dumps(table, ensure_ascii=False)):
        cell_words.add(letters(cell))
        for word in cell.replace("/", " ").split():
            if word.lower() not in SERVICE:
                cell_words.add(letters(word))
    cell_words.discard("")

    def known(word: str) -> bool:
        """Слово есть в таблице — целиком или как «основа + отделяемая приставка»."""
        key = letters(word)
        if not key or key in cell_words:
            return True
        # «zufügte» = «fügte» + «zu», разнесённые в справочнике по разным клеткам.
        return any(letters(word[:i]) in cell_words and letters(word[i:]) in cell_words
                   for i in range(2, len(word) - 1))

    parts = [p.strip() for p in ours.split("/") if p.strip()]
    missing = []
    for part in parts:
        content = [w for w in part.replace("/", " ").split() if w.lower() not in SERVICE]
        if content and not all(known(w) for w in content):
            missing.append(part)
    return (CONFIRMED if not missing else CONFLICT, ours,
            "документированная таблица" if not missing else "; ".join(missing)[:200])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="записать отметки")
    parser.add_argument("--limit", type=int, default=0, help="ограничить число единиц")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            ensure_schema(cur)
            conn.commit()

            cur.execute("SELECT lower(title), genus FROM bt_3_wiktionary_genus_cache;")
            genus = dict(cur.fetchall())
            cur.execute("""SELECT lower(title), value FROM bt_3_wiktionary_forms
                           WHERE form_key = 'Nominativ Plural';""")
            plurals = dict(cur.fetchall())
            cur.execute("SELECT lower(verb), tables FROM bt_3_german_verb_paradigms WHERE documented;")
            paradigms = dict(cur.fetchall())
            print(f"справочник: родов {len(genus)}, множественных {len(plurals)}, "
                  f"спряжений {len(paradigms)}")

            sql = """SELECT id, display, pos, gender, card FROM bt_3_lex_units
                     WHERE lang='de' AND kind='word' ORDER BY id"""
            cur.execute(sql + (f" LIMIT {int(args.limit)}" if args.limit else "") + ";")
            units = [{"id": r[0], "display": r[1], "pos": r[2], "gender": r[3], "card": r[4]}
                     for r in cur.fetchall()]
            print(f"единиц-слов к проверке: {len(units)}\n")

            tally: dict[tuple[str, str], int] = {}
            conflicts: list[tuple] = []
            for unit in units:
                for field, checker, source in (
                    ("gender", check_gender, "wiktionary_genus"),
                    ("plural", check_plural, "wiktionary_forms"),
                    ("verb_forms", check_verb_forms, "german_verb_paradigms"),
                ):
                    outcome = checker(unit, {"gender": genus, "plural": plurals,
                                             "verb_forms": paradigms}[field])
                    if outcome is None:
                        continue
                    verdict, ours, ref = outcome
                    tally[(field, verdict)] = tally.get((field, verdict), 0) + 1
                    if verdict == CONFLICT:
                        conflicts.append((unit["id"], unit["display"], field, ours, ref))
                    if args.apply:
                        stamp(cur, unit["id"], field, verdict, source, ours, ref)
            if args.apply:
                conn.commit()

    for field in ("gender", "plural", "verb_forms"):
        line = "  ".join(f"{v}: {tally.get((field, v), 0)}"
                         for v in (CONFIRMED, CONFLICT, SILENT))
        print(f"{field:12} {line}")

    print(f"\nрасхождений всего: {len(conflicts)} — их разбирает второй источник или владелец,")
    print("этот скрипт НЕ переписывает данные по расхождению.")
    for row in conflicts[:15]:
        print(f"   [{row[0]}] {row[1][:28]:30} {row[2]:11} у нас «{str(row[3])[:24]}» "
              f"справочник «{str(row[4])[:24]}»")
    if not args.apply:
        print("\n(холостой прогон: ничего не записано, нужен --apply)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
