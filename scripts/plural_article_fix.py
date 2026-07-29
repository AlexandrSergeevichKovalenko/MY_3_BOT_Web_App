# -*- coding: utf-8 -*-
"""Уборка: артикль леммы, приклеенный к форме слова, — для ВСЕХ пользователей.

Чинит то, что нашёл scripts/plural_article_audit.py: заголовки вида «das Probleme»,
где «das» — артикль леммы «das Problem». У именительного множественного артикль может
быть только «die».

Правки делаются ТОЛЬКО по подтверждённому справочником вердикту (confidence=high).
Догадка по окончанию принимает за формы настоящие слова («der Stürmer» — якобы форма
от «Sturm»), поэтому к уборке не допускается вовсе.

Что делает:
  • общий пул и личные карточки — артикль в заголовке и в разборе → «die»;
    рядом записываются число и лемма, из которой строится склонение;
  • слой единиц — тот же артикль (род формы);
  • банк игры «Артикли» — формы выводятся из ротации: у вопроса «der/die/das?»
    на форме множественного нет верного ответа, как и у двуродовых слов.

Запуск:  python3 scripts/plural_article_fix.py            # вхолостую, ничего не пишет
         python3 scripts/plural_article_fix.py --apply    # записать
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.german_surface import PL, german_surface  # noqa: E402

ARTICLE_RE = re.compile(r"^(der|die|das)\s+", re.I)
CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")


def split_article(text: str) -> tuple[str, str]:
    compact = re.sub(r"\s+", " ", str(text or "").strip())
    match = ARTICLE_RE.match(compact)
    return (match.group(1).lower(), compact[match.end():]) if match else ("", compact)


def _looks_german_noun(value: str) -> bool:
    _article, bare = split_article(value)
    return bool(bare) and " " not in bare and bare[:1].isupper() and not CYRILLIC_RE.search(bare)


def _pick_german(texts: list, payload: dict) -> str:
    """Немецкое однословное существительное среди колонок карточки, иначе ''."""
    candidates = [str(payload.get("word_de") or "")] + [str(t or "") for t in texts]
    return next((c for c in candidates if _looks_german_noun(c)), "")


_COLUMNS: dict[str, set[str]] = {}


def _column_exists(conn, table: str, column: str) -> bool:
    if table not in _COLUMNS:
        with conn.cursor() as cur:
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                        (table,))
            _COLUMNS[table] = {str(r[0]) for r in cur.fetchall() or []}
    return column in _COLUMNS[table]


def verdict_for(bare: str, cache: dict) -> dict | None:
    """Подтверждённое «это форма множественного», иначе None."""
    if not bare or " " in bare or not bare[:1].isupper() or CYRILLIC_RE.search(bare):
        return None
    if bare not in cache:
        cache[bare] = german_surface(bare)
    v = cache[bare]
    return v if v["number"] == PL and v["confidence"] == "high" else None


def fix_cards(conn, table: str, text_columns: list[str], apply: bool, cache: dict) -> dict:
    """Общий пул и личные карточки устроены одинаково: тексты + разбор в response_json.

    У пула рядом с текстами лежат нормализованные ключи поиска. Меняем текст — обязаны
    пересчитать и их, иначе запись просто перестанет находиться: ключ будет от старого
    «das Probleme», а в тексте уже «die Probleme»."""
    from backend.database import (  # ключи считаем ровно тем же кодом, что и запись
        _normalize_dictionary_headword_key, _normalize_dictionary_text_key,
    )
    norm_columns = {
        "source_text": ("source_text_norm", "source_headword_norm"),
        "target_text": ("target_text_norm", "target_headword_norm"),
    }
    stats = {"осмотрено": 0, "артикль исправлен": 0, "число проставлено": 0}
    examples: list[str] = []
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT id, {', '.join(text_columns)}, response_json, source_lang, target_lang FROM {table} "
            f"WHERE source_lang = 'de' OR target_lang = 'de'"
        )
        rows = cur.fetchall() or []
    updates = []
    for row in rows:
        row_id, *rest = row
        texts, payload = rest[:-3], rest[-3]
        payload = payload if isinstance(payload, dict) else {}
        # Немецкую сторону ищем ПО СОДЕРЖИМОМУ, а не по направлению: раскладка колонок
        # у пула и у личных карточек разная и историческая (в личных при de→ru немецкое
        # слово лежит в word_de, а word_ru держит русский перевод). Направление здесь
        # обмануло бы — уборка прошла мимо шести карточек.
        head = _pick_german(texts, payload)
        _article, bare = split_article(head)
        verdict = verdict_for(bare, cache)
        if not verdict:
            continue
        stats["осмотрено"] += 1
        new_texts, changed = {}, False
        for name, value in zip(text_columns, texts):
            article, value_bare = split_article(value)
            if article in ("der", "das") and value_bare.casefold() == bare.casefold():
                new_texts[name] = f"die {value_bare}"
                changed = True
        new_payload = dict(payload)
        if str(new_payload.get("article") or "").lower() in ("der", "das"):
            new_payload["article"] = "die"
            changed = True
        for key in ("word_de", "translation_de", "source_text", "target_text"):
            article, value_bare = split_article(new_payload.get(key) or "")
            if article in ("der", "das") and value_bare.casefold() == bare.casefold():
                new_payload[key] = f"die {value_bare}"
                changed = True
        if new_payload.get("grammatical_number") != PL:
            new_payload["grammatical_number"] = PL
            changed = True
            stats["число проставлено"] += 1
        if verdict["lemma"] and verdict["lemma"].casefold() != bare.casefold():
            new_payload["lemma_de"] = verdict["lemma"]
        if not changed:
            continue
        if new_texts:
            stats["артикль исправлен"] += 1
            if len(examples) < 15:
                examples.append(f"  id={row_id} «{head}» → «{list(new_texts.values())[0]}» "
                                f"(форма от {verdict['lemma'] or '?'})")
        updates.append((row_id, new_texts, new_payload))

    if apply:
        collisions = 0
        for row_id, new_texts, new_payload in updates:
            sets, params = [], []
            for name, value in new_texts.items():
                sets.append(f"{name} = %s")
                params.append(value)
                for norm_col, headword_col in [norm_columns.get(name, ("", ""))]:
                    if norm_col and _column_exists(conn, table, norm_col):
                        sets.append(f"{norm_col} = %s")
                        params.append(_normalize_dictionary_text_key(value))
                    if headword_col and _column_exists(conn, table, headword_col):
                        sets.append(f"{headword_col} = %s")
                        params.append(_normalize_dictionary_headword_key(value))
            sets.append("response_json = %s")
            params.append(json.dumps(new_payload, ensure_ascii=False))
            params.append(row_id)
            try:
                with conn.cursor() as cur:
                    cur.execute(f"UPDATE {table} SET {', '.join(sets)} WHERE id = %s", params)
                conn.commit()
            except psycopg2.errors.UniqueViolation:
                # Правильная запись «die Probleme» уже есть — оставляем обе как есть,
                # склейкой дублей занимается слой единиц, а не эта уборка.
                conn.rollback()
                collisions += 1
        if collisions:
            print(f"  пропущено из-за уже существующей верной записи: {collisions}")
    print(f"\n{table}: " + ", ".join(f"{k} {v}" for k, v in stats.items()))
    for line in examples:
        print(line)
    return stats


def fix_lex_units(conn, apply: bool, cache: dict) -> int:
    fixed = 0
    with conn.cursor() as cur:
        cur.execute("SELECT id, display, gender FROM bt_3_lex_units "
                    "WHERE lang = 'de' AND kind = 'word' AND gender IN ('der', 'das')")
        rows = cur.fetchall() or []
    to_fix = []
    for unit_id, display, _gender in rows:
        _article, bare = split_article(display)
        if verdict_for(bare, cache):
            to_fix.append((unit_id, bare))
    if apply and to_fix:
        with conn.cursor() as cur:
            for unit_id, bare in to_fix:
                cur.execute("UPDATE bt_3_lex_units SET gender = 'die', display = %s, "
                            "gender_source = 'wiktionary_форма', updated_at = NOW() WHERE id = %s",
                            (f"die {bare}", unit_id))
        conn.commit()
    fixed = len(to_fix)
    print(f"\nслой единиц: род формы исправлен у {fixed}")
    for unit_id, bare in to_fix[:10]:
        print(f"  id={unit_id} → die {bare}")
    return fixed


def retire_sprint_forms(conn, apply: bool, cache: dict) -> int:
    """Из игры «der/die/das?» убираем ФОРМЫ ЧУЖОГО слова: у «Bänder» верного ответа
    нет, спрашивать надо про «das Band».

    А вот слова, которые живут только во множественном («die Eltern», «die Masern»,
    «die Kosten»), — законный учебный материал: у них множественное И ЕСТЬ словарная
    форма, ответ «die» верен. Их не трогаем, только доправляем артикль, если он не «die».
    """
    with conn.cursor() as cur:
        cur.execute("SELECT id, word, article FROM bt_3_article_sprint_nouns WHERE NOT retired")
        rows = cur.fetchall() or []
    to_retire, to_fix = [], []
    for row_id, word, article in rows:
        bare = split_article(word)[1]
        verdict = verdict_for(bare, cache)
        if not verdict:
            continue
        lemma = verdict["lemma"] or ""
        if lemma and lemma.casefold() != bare.casefold():
            to_retire.append((row_id, word, lemma))
        elif str(article or "").lower() != "die":
            to_fix.append((row_id, word, str(article or "")))
    if apply:
        with conn.cursor() as cur:
            if to_retire:
                cur.execute("UPDATE bt_3_article_sprint_nouns SET retired = TRUE WHERE id = ANY(%s)",
                            ([row_id for row_id, _, _ in to_retire],))
            if to_fix:
                cur.execute("UPDATE bt_3_article_sprint_nouns SET article = 'die' WHERE id = ANY(%s)",
                            ([row_id for row_id, _, _ in to_fix],))
        conn.commit()
    print(f"\nбанк «Артикли»: форм чужого слова выведено {len(to_retire)}, "
          f"артикль поправлен у {len(to_fix)}")
    for row_id, word, lemma in to_retire[:15]:
        print(f"  ⊘ id={row_id} {word} — форма от «{lemma}»")
    for row_id, word, article in to_fix[:15]:
        print(f"  ✎ id={row_id} {word}: {article} → die")
    return len(to_retire) + len(to_fix)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="записать изменения")
    args = parser.parse_args()
    dsn = (os.getenv("DATABASE_URL_PGBOUNCER_RAILWAY") or os.getenv("DATABASE_URL") or "").strip()
    if not dsn:
        print("Нет DATABASE_URL", file=sys.stderr)
        return 2
    print("═══ Уборка «артикль леммы на форме» ═══"
          f"\nрежим: {'ЗАПИСЬ' if args.apply else 'вхолостую (ничего не меняется)'}")
    conn = psycopg2.connect(dsn, connect_timeout=20)
    cache: dict = {}
    fix_cards(conn, "bt_3_dictionary_entries", ["source_text", "target_text", "word_de"], args.apply, cache)
    fix_cards(conn, "bt_3_webapp_dictionary_queries", ["word_ru", "translation_de", "word_de"], args.apply, cache)
    fix_lex_units(conn, args.apply, cache)
    retire_sprint_forms(conn, args.apply, cache)
    conn.close()
    if not args.apply:
        print("\nЭто был прогон вхолостую. Записать: --apply")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
