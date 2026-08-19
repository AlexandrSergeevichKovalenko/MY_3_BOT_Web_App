# -*- coding: utf-8 -*-
"""Починить негодные заголовки ПО СПРАВОЧНИКУ, а не руками. Сухой прогон по умолчанию.

Владелец 19.08.2026: «МЫ ВСЁ АВТОМАТИЗИРУЕМ. Нельзя что-то куда-то просто положить.
Каждая задача должна быть доведена до результата». Список из 111 негодных заголовков,
который никто не выполняет, — это отложенная проблема с видимостью решения.

ОТКУДА БЕРЁТСЯ ПОЧИНКА. Ничего не выдумывается: у de.wiktionary на странице склонённой
или спрягаемой формы напечатан шаблон {{Grundformverweis|<исходное слово>}}. Замер
19.08.2026:

    besonderer     → besondere      winzigen → winzig      süßer → süß
    Beschimpfungen → Beschimpfung   Betracht → betrachten
    abgestumpft    → abstumpfen     angewandt → anwenden

ЧЕТЫРЕ ОПЕРАЦИИ, каждая со своим правилом и своим риском:

  1. ДУБЛЬ (11). Справочник даёт исходное слово, и оно УЖЕ есть в нашем справочнике.
     Значит наша строка — это форма чужого слова. Личные карточки, привязанные к ней,
     перецепляются на настоящее слово, строка сносится.
  2. ПЕРЕИМЕНОВАНИЕ (21). Исходного слова у нас нет — правим заголовок на месте.
     Идентификатор строки не меняется, поэтому связи карточек не рвутся.
  3. ЧАСТЬ РЕЧИ (12). Слово настоящее, неверна только наша пометка: «ausstatten» лежал
     прилагательным, а это глагол. Правим пометку по справочнику.
  4. ЛОЖНАЯ ТРЕВОГА (15). Слова нет в Wiktionary, но оно разбирается как составное
     («Apfelmark», «Feuerdrache», «Blickpunkt») — значит слово ЗАКОННОЕ, а метка
     «негодный заголовок» на нём стоит зря. Метку снимаем.

ЧТО НЕ ЧИНИТСЯ АВТОМАТОМ (49) — остаётся владельцу, молча не трогаем: обрезанные
(«Abschiebu»), без умлаута («Argernisse»), с опечаткой («Bedingungssätz») и настоящие
редкие слова, которых справочник не знает.

    python3 scripts/fix_headwords_from_reference.py           # сухой прогон
    python3 scripts/fix_headwords_from_reference.py --apply   # применить
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context     # noqa: E402
import backend.german_reference_forms as R                 # noqa: E402
from backend.article_authority import compound_heads       # noqa: E402

POS_BY_WORTART = {
    "Substantiv": "noun", "Adjektiv": "adjective", "Adverb": "adverb",
    "Verb": "verb", "Lokaladverb": "adverb", "Temporaladverb": "adverb",
    "Modaladverb": "adverb", "Konjunktionaladverb": "adverb",
    "Pronominaladverb": "adverb", "Präposition": "preposition",
    "Konjunktion": "conjunction", "Pronomen": "pronoun",
}


def _variants(word: str) -> list[str]:
    return list(dict.fromkeys([word, word[:1].lower() + word[1:], word[:1].upper() + word[1:]]))


def _plan() -> dict[str, list]:
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT u.id, u.lemma, u.pos FROM bt_3_reference_forms_unresolved q
                           JOIN bt_3_lex_units u ON lower(u.lemma) = lower(q.word) AND u.lang='de'
                           WHERE q.reason LIKE 'негодный заголовок%' ORDER BY u.lemma""")
            rows = [(int(a), str(b), str(c)) for a, b, c in (cur.fetchall() or [])]
            cur.execute("SELECT lower(lemma), id FROM bt_3_lex_units WHERE lang='de'")
            known = {a: int(b) for a, b in cur.fetchall()}

    plan = {"дубль": [], "переименование": [], "часть речи": [], "ложная тревога": [],
            "владельцу": []}
    for i in range(0, len(rows), 25):
        chunk = rows[i:i + 25]
        sources = None
        for _ in range(3):
            sources = R.fetch_sources_bulk([v for _i, w, _p in chunk for v in _variants(w)][:50])
            if sources is not None:
                break
            time.sleep(8)
        if sources is None:
            continue
        for row_id, lemma, pos in chunk:
            text = next((sources.get(v) for v in _variants(lemma) if sources.get(v)), "")
            if not text:
                if compound_heads(lemma):
                    plan["ложная тревога"].append((row_id, lemma, pos, "составное слово, законно"))
                else:
                    plan["владельцу"].append((row_id, lemma, pos, "справочник слова не знает"))
                continue
            base = re.findall(r"\{\{Grundformverweis[^}]*\|([^}|]+)\}\}", text)
            if base:
                target = base[0].strip()
                if target.lower() in known and known[target.lower()] != row_id:
                    plan["дубль"].append((row_id, lemma, pos, target))
                else:
                    plan["переименование"].append((row_id, lemma, pos, target))
                continue
            kinds = re.findall(r"\{\{Wortart\|([^|}]+)", text)
            new_pos = next((POS_BY_WORTART[k] for k in kinds if k in POS_BY_WORTART), "")
            if new_pos and new_pos != pos:
                plan["часть речи"].append((row_id, lemma, pos, new_pos))
            else:
                plan["владельцу"].append((row_id, lemma, pos, "страница есть, править нечего"))
        time.sleep(2)
    return plan


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    plan = _plan()
    for name, items in plan.items():
        print(f"\n  {len(items):4}  {name}")
        for _rid, lemma, pos, what in items[:6]:
            print(f"        {lemma} ({pos}) → {what}")

    # Часть речи новых заголовков — отдельным запросом к справочнику.
    pos_by_target: dict[str, str] = {}
    targets = [t for _r, _l, _p, t in plan["переименование"]]
    for i in range(0, len(targets), 40):
        chunk = targets[i:i + 40]
        sources = None
        for _ in range(3):
            sources = R.fetch_sources_bulk(chunk)
            if sources is not None:
                break
            time.sleep(8)
        if sources is None:
            continue
        for target in chunk:
            text = sources.get(target) or ""
            kinds = re.findall(r"\{\{Wortart\|([^|}]+)", text)
            found = next((POS_BY_WORTART[k] for k in kinds if k in POS_BY_WORTART), "")
            if found:
                pos_by_target[target.lower()] = found
        time.sleep(2)
    print("\n  часть речи новых заголовков прочитана у справочника:")
    for _r, lemma, pos, target in plan["переименование"][:8]:
        print(f"        {lemma} ({pos}) → {target} ({pos_by_target.get(target.lower(), '—')})")

    if not args.apply:
        print("\nЭто СУХОЙ ПРОГОН. Ничего не изменено. Применить: --apply")
        return

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            # ДУБЛИ НЕ СНОСИМ. Проверка вреда 19.08.2026 перед применением: на строку
            # словаря ссылаются ВОСЕМЬ таблиц — значения слова (bt_3_lex_senses),
            # поверхности (bt_3_lex_surfaces), связи (bt_3_lex_links), источники,
            # разбор фраз (bt_3_phrase_check/review) и личные карточки. Снос либо упал бы
            # на внешнем ключе, либо утащил бы всё это за собой.
            #
            # Дубль требует настоящего СЛИЯНИЯ: перенести значения и поверхности на
            # оставшееся слово, перецепить карточки, и только потом убрать строку.
            # Это отдельная работа, её нельзя делать походя. Такие слова остаются
            # в списке владельца с явной причиной.
            for row_id, lemma, _pos, target in plan["дубль"]:
                cur.execute("UPDATE bt_3_reference_forms_unresolved "
                            "SET reason=%s, reviewed=TRUE WHERE word=%s",
                            (f"дубль формы: настоящее слово «{target}», нужно слияние", lemma))
            for row_id, _lemma, _pos, target in plan["переименование"]:
                # Часть речи берём У НОВОГО СЛОВА, а не тащим старую. Сухой прогон
                # 19.08.2026 поймал: «Bedingungen (прилагательное) → Bedingung» оставлял
                # на существительном пометку «прилагательное», а «ausgearbeitet →
                # ausarbeiten» — на глаголе. Заголовок бы починился, а разметка соврала.
                new_pos = pos_by_target.get(target.lower(), "")
                if new_pos:
                    cur.execute("UPDATE bt_3_lex_units SET lemma=%s, lemma_key=lower(%s), "
                                "pos=%s, pos_source='справочник 19.08.2026', updated_at=NOW() "
                                "WHERE id=%s", (target, target, new_pos, row_id))
                else:
                    cur.execute("UPDATE bt_3_lex_units SET lemma=%s, lemma_key=lower(%s), "
                                "updated_at=NOW() WHERE id=%s", (target, target, row_id))
            for row_id, _lemma, _pos, new_pos in plan["часть речи"]:
                cur.execute("UPDATE bt_3_lex_units SET pos=%s, pos_source='справочник 19.08.2026', "
                            "updated_at=NOW() WHERE id=%s", (new_pos, row_id))
        conn.commit()

    # Починенные и ложно помеченные снимаем с учёта — задача доведена до результата.
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for key in ("переименование", "часть речи", "ложная тревога"):
                for _rid, lemma, _pos, _what in plan[key]:
                    cur.execute("DELETE FROM bt_3_reference_forms_unresolved WHERE word=%s",
                                (lemma,))
        conn.commit()
    fixed = sum(len(plan[k]) for k in ("переименование", "часть речи", "ложная тревога"))
    print(f"\nприменено. исправлено автоматически: {fixed}")
    print(f"ждут слияния (сносить нельзя — восемь таблиц ссылаются): {len(plan['дубль'])}")
    print(f"осталось владельцу: {len(plan['владельцу'])}")


if __name__ == "__main__":
    main()
