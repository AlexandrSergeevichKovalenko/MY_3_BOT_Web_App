# -*- coding: utf-8 -*-
"""Проход по ЧАСТЯМ РЕЧИ перед сборкой слоя единиц.

Зачем: в пуле слово сохраняется с заглавной буквы независимо от того, чем оно
является, поэтому «с заглавной = существительное» — ложный признак. Проверка на
живых данных: из 1501 «существительного без рода» настоящие существительные —
меньшинство, остальное глаголы (Hineingehen, Durchführen) и прилагательные
(Nahtlos, Beharrlich), просто записанные с большой буквы.

Часть речи лежит на той же странице Wiktionary, что и род: шаблон
{{Wortart|Substantiv|Deutsch}}. Достаём её тем же запросом — лишних обращений
к сети нет.

Пишет ТОЛЬКО в свою новую таблицу-справочник bt_3_wiktionary_pos_cache.
Пул и личные словари не трогаются.

Запуск:  DATABASE_URL=... python3 scripts/dict_units_pos_pass.py [--limit N] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.article_wiktionary_ref import (  # noqa: E402
    _API, _DE_HEADER, _GENUS, _NEXT_LANG, _OVERVIEW, _UA,
)

ARTICLE_RE = re.compile(r"^(der|die|das)\s+", re.I)
# {{Wortart|Substantiv|Deutsch}} — самый надёжный признак части речи на странице
WORTART_RE = re.compile(r"\{\{Wortart\|([^|}]+)\|Deutsch\}\}")
BATCH = 45
PAUSE = 2.0
RETRIES = 4

# Немецкие названия частей речи → наши коды. Всё незнакомое сохраняем как есть,
# чтобы потом было видно, чего мы не учли, а не молча потерять.
POS_MAP = {
    "Substantiv": "noun", "Verb": "verb", "Adjektiv": "adjective",
    "Adverb": "adverb", "Pronomen": "pronoun", "Personalpronomen": "pronoun",
    "Demonstrativpronomen": "pronoun", "Possessivpronomen": "pronoun",
    "Indefinitpronomen": "pronoun", "Interrogativpronomen": "pronoun",
    "Präposition": "preposition", "Konjunktion": "conjunction",
    "Subjunktion": "conjunction", "Partikel": "particle",
    "Antwortpartikel": "particle", "Gradpartikel": "particle",
    "Modalpartikel": "particle", "Interjektion": "interjection",
    "Numerale": "numeral", "Artikel": "article", "Abkürzung": "abbreviation",
    "Redewendung": "idiom", "Sprichwort": "idiom", "Eigenname": "proper_noun",
    "Toponym": "proper_noun", "Nachname": "proper_noun", "Vorname": "proper_noun",
    "Partizip II": "participle", "Partizip I": "participle",
}


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


def german_block(wikitext: str) -> str | None:
    m = _DE_HEADER.search(wikitext)
    if not m:
        return None
    blk = wikitext[m.end():]
    nxt = _NEXT_LANG.search(blk)
    return blk[:nxt.start()] if nxt else blk


def parse_page(wikitext: str) -> tuple[list[str], str]:
    """→ (части речи, код рода). Род читаем тем же правилом, что и раньше."""
    blk = german_block(wikitext)
    if blk is None:
        return [], "-"
    pos: list[str] = []
    for raw in WORTART_RE.findall(blk):
        code = POS_MAP.get(raw.strip(), raw.strip().lower())
        if code not in pos:
            pos.append(code)
    genera: set[str] = set()
    for tmpl in _OVERVIEW.findall(blk):
        genera.update(_GENUS.findall(tmpl))
    return pos, ("".join(sorted(genera)) or "-")


def fetch(titles: list[str]) -> dict[str, tuple[list[str], str]]:
    params = {
        "action": "query", "format": "json", "formatversion": "2",
        "prop": "revisions", "rvprop": "content", "rvslots": "main",
        "redirects": "1", "titles": "|".join(titles),
    }
    req = urllib.request.Request(_API + "?" + urllib.parse.urlencode(params), headers={"User-Agent": _UA})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except Exception as exc:
        print("    запрос не прошёл: %s" % exc)
        return {}
    out: dict[str, tuple[list[str], str]] = {}
    for p in (data.get("query") or {}).get("pages") or []:
        title = str(p.get("title") or "")
        if p.get("missing"):
            out[title] = ([], "-")
            continue
        try:
            content = p["revisions"][0]["slots"]["main"]["content"]
        except (KeyError, IndexError, TypeError):
            out[title] = ([], "-")
            continue
        out[title] = parse_page(content)
    return out


def ensure_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bt_3_wiktionary_pos_cache (
            title      TEXT PRIMARY KEY,
            pos_list   TEXT NOT NULL,      -- через запятую, в порядке появления на странице
            genus      TEXT NOT NULL,      -- тот же код, что в кеше родов: m/f/n/mn/'-'
            checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )


def collect_candidates(cur) -> dict[str, str]:
    """Однословные немецкие записи пула, у которых часть речи не проставлена."""
    cur.execute(
        """
        SELECT CASE WHEN source_lang='de' THEN source_text ELSE target_text END AS de_text,
               response_json->>'part_of_speech' AS pos
        FROM bt_3_dictionary_entries
        WHERE (source_lang='de' AND target_lang='ru') OR (source_lang='ru' AND target_lang='de');
        """
    )
    known: set[str] = set()
    seen: dict[str, str] = {}
    for de_text, pos in cur.fetchall():
        body = ARTICLE_RE.sub("", (de_text or "").strip()).strip()
        if not body or " " in body:
            continue
        key = body.casefold()
        if (pos or "").strip():
            known.add(key)
        seen.setdefault(key, body)
    return {k: v for k, v in seen.items() if k not in known}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--titles-file", default="",
                    help="спросить именно эти заголовки (по одному в строке), а не весь банк")
    args = ap.parse_args()

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("нужен DATABASE_URL")
    conn = connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()
    ensure_table(cur)

    cur.execute("SELECT title FROM bt_3_wiktionary_pos_cache;")
    have = {r[0] for r in cur.fetchall()}
    if args.titles_file:
        # Точечный доспрос: слова, у которых страница с ЗАГЛАВНОЙ говорит
        # «существительное», а русский перевод глагольный («Wählen» → «Выбрать»).
        # Спрашиваем ту же страницу со строчной — в Wiktionary это другая статья.
        with open(args.titles_file, encoding="utf-8") as fh:
            wanted = [w.strip() for w in fh if w.strip()]
        candidates = {w.casefold(): w for w in wanted}
        todo = sorted({w for w in wanted if w not in have})
    else:
        candidates = collect_candidates(cur)
        todo = sorted({v for k, v in candidates.items() if v not in have})
    print("однословных записей без части речи: %d" % len(candidates))
    print("  уже разобрано:                    %d" % (len(candidates) - len(todo)))
    print("  спросить у Wiktionary:            %d  (~%d запросов)" % (len(todo), (len(todo) - 1) // BATCH + 1 if todo else 0))
    if args.dry_run:
        return 0
    if args.limit:
        todo = todo[: args.limit]

    # Пустой первый заход не должен обрывать проход: второй заход (со строчной
    # буквы) добирает как раз то, чего первый найти не мог.
    for i in range(0, len(todo), BATCH):
        batch = todo[i:i + BATCH]
        got = {}
        for attempt in range(RETRIES):
            got = fetch(batch)
            if got:
                break
            wait = PAUSE * (2 ** attempt) * 5
            print("  пачка не ответила, жду %.0f с и повторяю (%d/%d)" % (wait, attempt + 1, RETRIES))
            time.sleep(wait)
        if not got:
            print("  не отвечает после %d повторов на %d слове — остальное доспросим позже" % (RETRIES, i))
            break
        for title in batch:
            got.setdefault(title, ([], "-"))
        # Проход по сети длинный, а прокси базы рвёт простаивающую сессию —
        # на записи переподключаемся, иначе теряем всю пачку и весь остаток прохода.
        payload = [(t, ",".join(p), g) for t, (p, g) in got.items()]
        for write_try in range(3):
            try:
                cur.executemany(
                    """
                    INSERT INTO bt_3_wiktionary_pos_cache (title, pos_list, genus, checked_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (title) DO UPDATE
                      SET pos_list = EXCLUDED.pos_list, genus = EXCLUDED.genus, checked_at = NOW();
                    """,
                    payload,
                )
                break
            except psycopg2.Error as exc:
                print("    запись не прошла (%s), переподключаюсь" % exc)
                try:
                    conn.close()
                except Exception:
                    pass
                conn = connect(dsn)
                conn.autocommit = True
                cur = conn.cursor()
        print("  %5d/%d …" % (min(i + BATCH, len(todo)), len(todo)), flush=True)
        time.sleep(PAUSE)

    # ── Второй заход: та же страница, но со строчной буквы ──────────────────────
    # В Wiktionary регистр ПЕРВОЙ буквы значим: «bewältigen» и «Bewältigen» —
    # разные страницы. У нас слово может лежать с заглавной просто потому, что
    # приехало из начала предложения, и тогда первый заход не находит ничего.
    cur.execute("SELECT title FROM bt_3_wiktionary_pos_cache WHERE pos_list = '';")
    empties = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT title FROM bt_3_wiktionary_pos_cache;")
    known_titles = {r[0] for r in cur.fetchall()}
    lower_todo = sorted({t[:1].lower() + t[1:] for t in empties
                         if t[:1].isupper() and (t[:1].lower() + t[1:]) not in known_titles})
    if lower_todo:
        print("\nвторой заход, те же слова со строчной буквы: %d (~%d запросов)"
              % (len(lower_todo), (len(lower_todo) - 1) // BATCH + 1))
        for i in range(0, len(lower_todo), BATCH):
            batch = lower_todo[i:i + BATCH]
            got = {}
            for attempt in range(RETRIES):
                got = fetch(batch)
                if got:
                    break
                time.sleep(PAUSE * (2 ** attempt) * 5)
            if not got:
                print("  не отвечает на %d слове — остальное доспросим позже" % i)
                break
            for title in batch:
                got.setdefault(title, ([], "-"))
            payload = [(t, ",".join(pp), g) for t, (pp, g) in got.items()]
            for _ in range(3):
                try:
                    cur.executemany(
                        """
                        INSERT INTO bt_3_wiktionary_pos_cache (title, pos_list, genus, checked_at)
                        VALUES (%s, %s, %s, NOW())
                        ON CONFLICT (title) DO UPDATE
                          SET pos_list = EXCLUDED.pos_list, genus = EXCLUDED.genus, checked_at = NOW();
                        """,
                        payload,
                    )
                    break
                except psycopg2.Error as exc:
                    print("    запись не прошла (%s), переподключаюсь" % exc)
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = connect(dsn)
                    conn.autocommit = True
                    cur = conn.cursor()
            print("  %5d/%d …" % (min(i + BATCH, len(lower_todo)), len(lower_todo)), flush=True)
            time.sleep(PAUSE)

    cur.execute("SELECT pos_list, COUNT(*) FROM bt_3_wiktionary_pos_cache GROUP BY 1 ORDER BY 2 DESC LIMIT 15;")
    print("\nчто получилось (часть речи → сколько слов):")
    for pos_list, n in cur.fetchall():
        print("   %-28s %d" % (pos_list or "— страницы нет —", n))
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
