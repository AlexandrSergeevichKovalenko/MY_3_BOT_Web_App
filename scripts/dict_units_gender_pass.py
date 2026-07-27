# -*- coding: utf-8 -*-
"""Проход по родам ПЕРЕД сборкой слоя единиц.

Берёт из общего пула немецкие однословные записи, у которых род неизвестен
(нет артикля ни в тексте, ни в разборе), и спрашивает род у de.wiktionary.

Пишет ТОЛЬКО в кеш родов bt_3_wiktionary_genus_cache — таблица-справочник,
которая лишь дополняется. Ни пул, ни личные словари не трогаются.

Запуск:  DATABASE_URL=... python3 scripts/dict_units_gender_pass.py [--limit N] [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time

import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.article_wiktionary_ref import _api_fetch, genera_to_articles  # noqa: E402

ARTICLE_RE = re.compile(r"^(der|die|das)\s+", re.I)
NOT_NOUN_POS = {
    "verb", "adverb", "adjective", "preposition", "particle",
    "pronoun", "conjunction", "numeral", "interjection",
}
BATCH = 45      # предел MediaWiki — 50 заголовков за запрос, оставляем запас
PAUSE = 2.0     # пауза между пачками: на 0.1 с Wiktionary отвечает 429
RETRIES = 4     # повтор той же пачки с растущим отступом, прежде чем сдаться


def connect(dsn: str):
    last = None
    for attempt in range(6):
        try:
            return psycopg2.connect(dsn, connect_timeout=20)
        except Exception as exc:  # прокси Railway изредка отваливается
            last = exc
            print("  переподключение %d/6: %s" % (attempt + 1, exc))
            time.sleep(5)
    raise SystemExit("база недоступна: %s" % last)


def collect_candidates(cur) -> dict[str, str]:
    """{лемма в нижнем регистре: исходное написание} для слов без известного рода."""
    cur.execute(
        """
        SELECT CASE WHEN source_lang='de' THEN source_text ELSE target_text END AS de_text,
               response_json->>'article'      AS article,
               response_json->>'part_of_speech' AS pos
        FROM bt_3_dictionary_entries
        WHERE (source_lang='de' AND target_lang='ru') OR (source_lang='ru' AND target_lang='de');
        """
    )
    seen: dict[str, str] = {}
    gendered: set[str] = set()
    not_noun: set[str] = set()
    for de_text, article, pos in cur.fetchall():
        raw = (de_text or "").strip()
        body = ARTICLE_RE.sub("", raw).strip()
        if not body or " " in body:
            continue  # фразы и предложения рода не имеют
        key = body.casefold()
        has_article = bool(ARTICLE_RE.match(raw)) or (article or "").strip().lower() in {"der", "die", "das"}
        if has_article:
            gendered.add(key)
        if (pos or "").strip().lower() in NOT_NOUN_POS:
            not_noun.add(key)
        # существительное в немецком пишется с заглавной — это и есть отбор
        if body[:1].isupper():
            seen.setdefault(key, body)
    return {k: v for k, v in seen.items() if k not in gendered and k not in not_noun}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="взять не больше N слов (0 = все)")
    ap.add_argument("--dry-run", action="store_true", help="только показать, сколько и чего, без сети")
    args = ap.parse_args()

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("нужен DATABASE_URL")

    conn = connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()

    candidates = collect_candidates(cur)
    cur.execute("SELECT lower(title), genus FROM bt_3_wiktionary_genus_cache;")
    cached = dict(cur.fetchall())

    todo = {k: v for k, v in candidates.items() if k not in cached}
    print("существительных без известного рода: %d" % len(candidates))
    print("  уже есть в кеше родов:             %d" % (len(candidates) - len(todo)))
    print("  надо спросить у Wiktionary:        %d  (~%d запросов)" % (len(todo), (len(todo) - 1) // BATCH + 1))
    if args.dry_run or not todo:
        return 0

    titles = sorted(todo.values())
    if args.limit:
        titles = titles[: args.limit]

    fetched = 0
    for i in range(0, len(titles), BATCH):
        batch = titles[i:i + BATCH]
        got = {}
        for attempt in range(RETRIES):
            got = _api_fetch(batch)
            if got:
                break
            # Пустой ответ = сеть или лимит (429). Пометить эти слова «страницы нет»
            # нельзя: слово навсегда осталось бы без рода. Ждём дольше и повторяем.
            wait = PAUSE * (2 ** attempt) * 5
            print("  пачка не ответила, жду %.0f с и повторяю (%d/%d)" % (wait, attempt + 1, RETRIES))
            time.sleep(wait)
        if not got:
            print("  не отвечает после %d повторов на %d слове — остальное доспросим позже" % (RETRIES, i))
            break
        for title in batch:
            got.setdefault(title, "-")
        cur.executemany(
            """
            INSERT INTO bt_3_wiktionary_genus_cache (title, genus, checked_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (title) DO UPDATE SET genus = EXCLUDED.genus, checked_at = NOW();
            """,
            list(got.items()),
        )
        fetched += len(got)
        print("  %5d/%d …" % (min(i + BATCH, len(titles)), len(titles)), flush=True)
        time.sleep(PAUSE)  # вежливость к API

    cur.execute("SELECT lower(title), genus FROM bt_3_wiktionary_genus_cache;")
    cached = dict(cur.fetchall())
    single = two = nopage = unasked = 0
    for key in candidates:
        if key not in cached:
            unasked += 1
            continue
        arts = genera_to_articles(cached.get(key))
        if len(arts) == 1:
            single += 1
        elif len(arts) > 1:
            two += 1
        else:
            nopage += 1
    print("\nитог по %d словам без рода:" % len(candidates))
    print("  род определён однозначно:      %d" % single)
    print("  слово двуродовое:              %d" % two)
    print("  страницы существительного нет: %d  ← это не существительные, им нужна часть речи" % nopage)
    print("  ещё не спрашивали:             %d" % unasked)
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
