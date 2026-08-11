# -*- coding: utf-8 -*-
"""Проверка общего пула на ПРОТИВОРЕЧИЯ. Не на «соответствие новому стандарту».

Владелец 11.08.2026 решил прогнать разовую проверку по 16 318 записям, набранным по
старой схеме. Проверка узкая намеренно: фильтр приёмки нельзя разворачивать на уже
принятое — иначе половина словаря объявляется браком за то, что её собирали, когда
правил ещё не было. Ищем только то, что противоречит САМО СЕБЕ или проверяемому
справочнику.

Что считаем противоречием:

  A. АРТИКЛЬ СПОРИТ СО СПРАВОЧНИКОМ. В заголовке «die Beistand», а справочник родов
     знает «Beistand» однозначно мужским. Тут доказано и чинится: артикль заменяется
     на справочный. Только однозначный род (m/f/n) — «der/das Band» не трогаем.

  B. ОДНО НАПИСАНИЕ, РАЗНЫЕ ОТВЕТЫ. «Толстый» → die Dicke и «толстый» → dick лежат
     двумя записями, потому что раньше регистр делал из одного слова два запроса.
     Чинить нечем без выбора значения — считаем и показываем.

  C. ЗАГОЛОВОК СПОРИТ С НАШИМ ЖЕ СЛОЕМ СТАТЕЙ. Записан «die Dicke», а слой статей
     на этот запрос знает только прилагательное «dick». Считаем и показываем.

  D. ЗАГОЛОВОК — ФОРМА, А НЕ СЛОВО. «die Probleme» как самостоятельная статья.
     Считаем и показываем; у форм есть свои чинилки.

Удалять нельзя: 14 916 записей пула привязаны к единицам слоя, а часть — к личным
карточкам людей (bt_3_flashcard_seen). Удаление осиротит чужую карточку. Поэтому
--apply чинит ТОЛЬКО пункт A, где правильный ответ доказан, а остальное уходит в
отчёт владельцу.

Запуск:
    DATABASE_URL=... python3 scripts/dict_pool_contradictions.py            # только отчёт
    DATABASE_URL=... python3 scripts/dict_pool_contradictions.py --apply    # + починка A
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

import psycopg2

_HEADWORD_RE = re.compile(r"^(der|die|das)\s+([A-ZÄÖÜ][A-Za-zÄÖÜäöüß\-]*)$", re.IGNORECASE)
_GENUS_TO_ARTICLE = {"m": "der", "f": "die", "n": "das"}


def connect(dsn: str):
    return psycopg2.connect(dsn, connect_timeout=25)


def _fetch_genus(cur) -> dict[str, str]:
    """Справочник родов, только ОДНОЗНАЧНЫЙ. «mn», «fm», «-» пропускаем: там наш
    артикль не обязан совпадать и спорить не с чем."""
    cur.execute("SELECT lower(title), genus FROM bt_3_wiktionary_genus_cache WHERE genus IN ('m','f','n');")
    return {title: _GENUS_TO_ARTICLE[genus] for title, genus in cur.fetchall()}


def _is_singular_lemma(word: str) -> bool:
    """Отсечь ФОРМЫ. «die Reifen» — именительный множественного от «der Reifen», и
    «die» там правильное; справочник же знает род ЛЕММЫ и скажет «der». Без этой
    проверки чинилка ломала бы нормальный немецкий: из первых 34 «противоречий»
    множественным числом оказалось большинство (замер 11.08.2026)."""
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from backend.german_surface import PL, german_surface
    try:
        return german_surface(word)["number"] != PL
    except Exception:
        return False


def _fetch_nominalized_adjectives(cur, words: list[str]) -> set[str]:
    """Субстантивированные прилагательные: «der Dicke» — толстяк, «die Dicke» —
    толщина, и оба существуют. Справочник родов знает одну строку на написание, и
    его «f» здесь НЕ опровергает наше «der» — просто это разные слова.

    Опознаём по данным, а не по виду: снимаем окончание -e/-er/-en/-es и смотрим,
    есть ли такое ПРИЛАГАТЕЛЬНОЕ в базовом словаре. «Dicke» → «dick» есть, значит
    трогать нельзя. «Aufschwung» → «aufschw…» нет, значит это обычное слово."""
    stems: dict[str, str] = {}
    for word in words:
        low = word.lower()
        for suffix in ("es", "en", "er", "e"):
            if low.endswith(suffix) and len(low) > len(suffix) + 2:
                stems.setdefault(low[: -len(suffix)], word)
    if not stems:
        return set()
    cur.execute(
        "SELECT lemma_key FROM bt_base_dictionary "
        "WHERE source_lang = 'de' AND pos = 'adjective' AND lemma_key = ANY(%s);",
        (list(stems.keys()),),
    )
    return {stems[row[0]] for row in cur.fetchall() if row[0] in stems}


def find_article_conflicts(cur, genus: dict[str, str]) -> list[dict]:
    cur.execute(
        """
        SELECT id, word_de, target_text, source_text, translation_de, source_lang, target_lang
        FROM bt_3_dictionary_entries
        WHERE word_de ~ '^(der|die|das|Der|Die|Das) [A-ZÄÖÜ][A-Za-zÄÖÜäöüß-]*$';
        """
    )
    out: list[dict] = []
    for row in cur.fetchall():
        entry_id, word_de, target_text, source_text, translation_de, s_lang, t_lang = row
        match = _HEADWORD_RE.match(str(word_de or "").strip())
        if not match:
            continue
        stored_article, noun = match.group(1).lower(), match.group(2)
        right = genus.get(noun.lower())
        if not right or right == stored_article:
            continue
        out.append({
            "id": entry_id, "word_de": word_de, "noun": noun,
            "stored": stored_article, "right": right,
            "target_text": target_text, "source_text": source_text,
            "translation_de": translation_de,
            "source_lang": s_lang, "target_lang": t_lang,
        })

    # Два отсева, без которых чинилка ломала бы нормальный немецкий. Первый замер
    # 11.08.2026 дал 34 «противоречия» — и почти все оказались формами
    # множественного числа («die Reifen») и субстантивированными прилагательными
    # («der Dicke»), где наш артикль верный, а справочник просто про другое слово.
    nominalized = _fetch_nominalized_adjectives(cur, [item["noun"] for item in out])
    kept, excused_plural, excused_nominal = [], [], []
    for item in out:
        if item["noun"] in nominalized:
            excused_nominal.append(item)
        elif item["stored"] == "die" or not _is_singular_lemma(item["noun"]):
            # «die» рядом с мужским/средним родом справочника — это, скорее всего,
            # ИМЕНИТЕЛЬНЫЙ МНОЖЕСТВЕННОГО, и он всегда «die». У доброй половины
            # немецких существительных множественное пишется как единственное
            # («der Reifen» → «die Reifen»), и отличить их по написанию нечем.
            # Молчим: сломать правильный немецкий дороже, чем пропустить ошибку.
            excused_plural.append(item)
        else:
            kept.append(item)
    return kept, excused_plural, excused_nominal


def find_case_duplicates(cur) -> list[tuple]:
    cur.execute(
        """
        SELECT lower(source_text), array_agg(DISTINCT source_text), array_agg(DISTINCT target_text)
        FROM bt_3_dictionary_entries
        WHERE source_text IS NOT NULL AND source_text NOT LIKE '% %'
        GROUP BY 1
        HAVING COUNT(DISTINCT source_text) > 1
           AND COUNT(DISTINCT lower(coalesce(target_text,''))) > 1
        ORDER BY 1;
        """
    )
    return cur.fetchall()


def find_pos_conflicts(cur, limit: int) -> list[dict]:
    """Записи, чей немецкий заголовок наш слой статей на этот же запрос не знает вовсе.

    Именно так выглядит разобранный случай: запрос «Толстый», в пуле «die Dicke», а
    слой статей на «толстый» знает dick / blad / feist — и «Dicke» среди них нет."""
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from backend.dictionary_entries import entries_for_query

    cur.execute(
        """
        SELECT id, source_text, word_de, source_lang, target_lang
        FROM bt_3_dictionary_entries
        WHERE source_lang = 'ru' AND target_lang = 'de'
          AND source_text NOT LIKE '%% %%' AND word_de IS NOT NULL AND word_de <> ''
        ORDER BY id DESC
        LIMIT %s;
        """,
        (limit,),
    )
    rows = cur.fetchall()
    out: list[dict] = []
    for entry_id, source_text, word_de, s_lang, t_lang in rows:
        known = entries_for_query(source_text, source_lang=s_lang, target_lang=t_lang)
        if not known:
            continue  # слой молчит — спорить не с чем
        stored = re.sub(r"^(der|die|das)\s+", "", str(word_de).strip(), flags=re.IGNORECASE).casefold()
        if any(e["headword"].casefold() == stored for e in known):
            continue
        out.append({
            "id": entry_id, "source_text": source_text, "word_de": word_de,
            "known": [e["display"] + f" ({e['pos']})" for e in known[:3]],
        })
    return out


def find_plural_headwords(cur, limit: int) -> list[dict]:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from backend.german_surface import PL, german_surface

    cur.execute(
        """
        SELECT id, word_de FROM bt_3_dictionary_entries
        WHERE word_de ~ '^(der|die|das|Der|Die|Das)? ?[A-ZÄÖÜ][A-Za-zÄÖÜäöüß-]*$'
          AND word_de <> '' ORDER BY id DESC LIMIT %s;
        """,
        (limit,),
    )
    out: list[dict] = []
    for entry_id, word_de in cur.fetchall():
        bare = re.sub(r"^(der|die|das)\s+", "", str(word_de).strip(), flags=re.IGNORECASE)
        try:
            verdict = german_surface(bare)
        except Exception:
            continue
        if verdict["number"] == PL and verdict["lemma"] and verdict["lemma"].casefold() != bare.casefold():
            out.append({"id": entry_id, "word_de": word_de, "lemma": verdict["lemma"]})
    return out


def repair_articles(cur, conflicts: list[dict]) -> int:
    """Заменить артикль на справочный ВЕЗДЕ в записи: заголовок, обе видимые стороны
    и разбор. Половинчатая починка хуже никакой — карточка снова будет спорить
    сама с собой."""
    fixed = 0
    for item in conflicts:
        wrong, right = item["stored"], item["right"]

        def swap(value):
            if not value:
                return value
            return re.sub(rf"\b{wrong}\s+{re.escape(item['noun'])}\b", f"{right} {item['noun']}",
                          str(value), flags=re.IGNORECASE)

        cur.execute("SELECT response_json FROM bt_3_dictionary_entries WHERE id = %s;", (item["id"],))
        row = cur.fetchone()
        payload = row[0] if row and isinstance(row[0], dict) else {}
        if payload.get("article"):
            payload["article"] = right
        for field in ("word_de", "translation_de", "source_text", "target_text"):
            if payload.get(field):
                payload[field] = swap(payload[field])
        cur.execute(
            """
            UPDATE bt_3_dictionary_entries
            SET word_de = %s, translation_de = %s, source_text = %s, target_text = %s,
                response_json = %s, updated_at = NOW()
            WHERE id = %s;
            """,
            (
                swap(item["word_de"]), swap(item["translation_de"]),
                swap(item["source_text"]), swap(item["target_text"]),
                json.dumps(payload, ensure_ascii=False), item["id"],
            ),
        )
        fixed += 1
    return fixed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="починить пункт A (артикль против справочника)")
    parser.add_argument("--scan-limit", type=int, default=4000, help="сколько записей смотреть в пунктах C и D")
    args = parser.parse_args()

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("Нужен DATABASE_URL", file=sys.stderr)
        sys.exit(1)

    conn = connect(dsn)
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM bt_3_dictionary_entries;")
        total = cur.fetchone()[0]
        genus = _fetch_genus(cur)

        article_conflicts, excused_plural, excused_nominal = find_article_conflicts(cur, genus)
        case_duplicates = find_case_duplicates(cur)
        pos_conflicts = find_pos_conflicts(cur, args.scan_limit)
        plural_headwords = find_plural_headwords(cur, args.scan_limit)

        print(f"\nВсего записей в общем пуле: {total}")
        print(f"Справочник родов, однозначных: {len(genus)}")

        print(f"\nA. Артикль спорит со справочником родов: {len(article_conflicts)}")
        for item in article_conflicts[:20]:
            print(f"   {item['word_de']}  →  по справочнику «{item['right']} {item['noun']}»")
        print(f"\n   Не в счёт — там наш артикль скорее всего верный:")
        print(f"     • {len(excused_plural)} записей с «die» при мужском/среднем роде — это"
              f" именительный множественного ({', '.join(i['word_de'] for i in excused_plural[:6])}…)")
        print(f"     • {len(excused_nominal)} субстантивированных прилагательных, где оба рода"
              f" настоящие ({', '.join(i['word_de'] for i in excused_nominal[:6])})")

        print(f"\nB. Одно написание, разные ответы (регистр): {len(case_duplicates)}")
        for key, forms, targets in case_duplicates[:15]:
            print(f"   {key}: {list(forms)} → {list(targets)}")

        print(f"\nC. Заголовок не совпадает ни с одной статьёй нашего словаря: {len(pos_conflicts)}"
              f"   (смотрели последние {args.scan_limit} записей ru→de из одного слова)")
        for item in pos_conflicts[:15]:
            print(f"   «{item['source_text']}» → {item['word_de']}, а словарь знает: {', '.join(item['known'])}")

        print(f"\nD. Заголовок — форма множественного, а не слово: {len(plural_headwords)}"
              f"   (смотрели последние {args.scan_limit} записей)")
        for item in plural_headwords[:15]:
            print(f"   {item['word_de']}  — форма слова «{item['lemma']}»")

        if args.apply:
            fixed = repair_articles(cur, article_conflicts)
            conn.commit()
            print(f"\n✓ Починено артиклей: {fixed}. Пункты B, C, D не трогали — там нужен выбор значения.")
        else:
            conn.rollback()
            print("\nЭто ОТЧЁТ, база не изменена.")
            print("Починка (--apply) трогает только пункт A и только записи из списка выше.")
            print("Прежде чем запускать — прочитайте список: автоматического критерия,")
            print("который отличает ошибку от «der Gehalt / das Gehalt», у нас нет.")
    conn.close()


if __name__ == "__main__":
    main()
