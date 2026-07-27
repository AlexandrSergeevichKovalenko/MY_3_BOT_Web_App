# -*- coding: utf-8 -*-
"""Разведение одинаково пишущихся немецких слов в слое единиц.

Одно написание с разными артиклями — это ТРИ разные ситуации:

  1. Настоящий омограф: «der Kiefer» (челюсть) и «die Kiefer» (сосна) — два слова.
  2. Словоформа, осевшая в банке отдельной записью: «der Herberge» (Genitiv/Dativ от
     «die Herberge»), «die Spaten» (множественное число). Это не слова, им место в
     указателях базового слова.
  3. Просто неверный артикль в старой записи: «die Hindernis» вместо «das Hindernis».

РОД РЕШАЕТ ТОЛЬКО WIKTIONARY. Проверка на живых данных показала, почему: GPT-4.1 в
одном вызове перевернул «der Kunde» (клиент) с «die Kunde» (весть) и объявил
«Glückseligkeit» средним родом, хотя суффикс -keit всегда женский. Поэтому здесь
Wiktionary говорит, СКОЛЬКО у написания настоящих родов, а GPT получает единственную
задачу — сопоставить наше русское значение с немецким толкованием, взятым с той же
страницы. Про род модель не спрашивают вовсе.

Решение записывается в справочник bt_3_lex_form_rulings: без него следующая сборка
создала бы отброшенные единицы заново.

Трогается только новый слой. Общий банк и личные словари не изменяются.

Запуск:
    OPENAI_API_KEY=... DATABASE_URL=... python3 scripts/dict_units_disambiguate.py --dry-run
    OPENAI_API_KEY=... DATABASE_URL=... python3 scripts/dict_units_disambiguate.py --apply
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
from backend.article_wiktionary_ref import _API, _DE_HEADER, _NEXT_LANG, _UA  # noqa: E402

MODEL = "gpt-4.1"
GENUS_TO_ARTICLE = {"m": "der", "f": "die", "n": "das"}
_SECTION_RE = re.compile(r"\n===\s*[^\n]*===")
# Род в разделе стоит либо в шаблоне обзора (Genus=m), либо прямо в заголовке, причём
# слитно для нескольких родов: «=== {{Wortart|Substantiv|Deutsch}}, {{mn.}} ===» — это
# «der/das Kiefer» (челюсть) рядом с отдельным разделом «{{f}}» (сосна).
_GENUS_IN_TEXT = re.compile(r"\bGenus\s*\d*\s*=\s*([mfn])\b|\{\{([mfn]{1,3})\.?\}\}")
_BEDEUTUNGEN_RE = re.compile(r"\{\{Bedeutungen\}\}(.*?)(?=\n\{\{[A-ZÄÖÜ]|\Z)", re.DOTALL)
_WIKI_MARKUP_RE = re.compile(r"\[\[([^\]|]*\|)?|\]\]|'''|''|\{\{[^}]*\}\}")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS bt_3_lex_form_rulings (
    lemma_key     TEXT NOT NULL,
    article       TEXT NOT NULL,      -- разбираемый вариант: der | die | das
    verdict       TEXT NOT NULL,      -- 'word' = самостоятельное слово, 'form' = словоформа/ошибка
    base_article  TEXT,               -- для 'form': чья это форма
    note          TEXT,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (lemma_key, article)
);
"""

MATCH_PROMPT = """Немецкое написание «{lemma}» — омограф: несколько разных слов с разным родом.
Вот их толкования, взятые из de.wiktionary (род указан, менять его нельзя):

{senses}

Определи, к какому из этих слов относится каждое русское значение:
{glosses}

Ответь СТРОГИМ JSON без пояснений: {{"<русское значение>": "der|die|das|unknown", ...}}
Ключи — ровно те строки, что даны выше. Если значение не подходит ни к одному
толкованию или относится к другой части речи — "unknown"."""


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


def fetch_senses_by_genus(title: str) -> dict[str, list[str]]:
    """{'der': ['Knochen …', …], 'die': ['Nadelbaum …']} — толкования с их родом.

    Страница Wiktionary разбита на разделы по частям речи и роду; берём из каждого
    раздела его род и блок {{Bedeutungen}}. Так «какое значение какого рода» отвечает
    сам словарь, а не модель."""
    params = {
        "action": "query", "format": "json", "formatversion": "2",
        "prop": "revisions", "rvprop": "content", "rvslots": "main",
        "redirects": "1", "titles": title,
    }
    req = urllib.request.Request(_API + "?" + urllib.parse.urlencode(params), headers={"User-Agent": _UA})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        page = ((data.get("query") or {}).get("pages") or [{}])[0]
        wikitext = page["revisions"][0]["slots"]["main"]["content"]
    except Exception as exc:
        print("    страница %r не прочиталась: %s" % (title, exc))
        return {}
    m = _DE_HEADER.search(wikitext)
    if not m:
        return {}
    block = wikitext[m.end():]
    nxt = _NEXT_LANG.search(block)
    if nxt:
        block = block[:nxt.start()]

    out: dict[str, list[str]] = {}
    parts = _SECTION_RE.split(block)
    heads = _SECTION_RE.findall(block)
    for head, body in zip(heads, parts[1:]):
        chunk = head + body
        letters = set()
        for g1, g2 in _GENUS_IN_TEXT.findall(chunk):
            letters.update(ch for ch in (g1 or g2 or "") if ch in GENUS_TO_ARTICLE)
        if not letters:
            continue
        found = _BEDEUTUNGEN_RE.search(chunk)
        if not found:
            continue
        senses = []
        for line in found.group(1).split("\n"):
            line = _WIKI_MARKUP_RE.sub("", line).strip(" :*#")
            if len(line) > 3:
                senses.append(line)
        if not senses:
            continue
        # Раздел на два рода сразу («{{mn.}}») — его толкования относятся к обоим.
        for letter in letters:
            out.setdefault(GENUS_TO_ARTICLE[letter], []).extend(senses[:4])
    return out


def ask_match(client, lemma: str, senses: dict[str, list[str]], glosses: list[str]) -> dict:
    listed = "\n".join(
        "%s %s: %s" % (article, lemma, "; ".join(items[:3]))
        for article, items in sorted(senses.items())
    )
    prompt = MATCH_PROMPT.format(lemma=lemma, senses=listed,
                                 glosses="\n".join("- %s" % g for g in glosses))
    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=400,
        )
        data = json.loads(resp.choices[0].message.content or "{}") or {}
    except Exception as exc:
        print("    сопоставление не удалось (%s): %s" % (lemma, exc))
        return {}
    return {str(k).strip(): str(v or "").strip().lower() for k, v in data.items()}


def merge_unit(cur, *, victim: int, keeper: int) -> None:
    """Слить лишнюю единицу в настоящее слово: её написания становятся указателями
    базового (это и есть механизм словоформ — «der Herberge» ведёт в «die Herberge»),
    связи и ссылки на строки банка переезжают, сама единица исчезает."""
    cur.execute(
        """
        INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
        SELECT lang, surface_key, %s, 'inflected' FROM bt_3_lex_surfaces WHERE unit_id = %s
        ON CONFLICT (lang, surface_key, unit_id) DO NOTHING;
        """, (keeper, victim))
    cur.execute(
        """
        INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source)
        SELECT %s, to_unit, rank, source FROM bt_3_lex_links
        WHERE from_unit = %s AND to_unit <> %s
        ON CONFLICT (from_unit, to_unit) DO UPDATE SET rank = LEAST(bt_3_lex_links.rank, EXCLUDED.rank);
        """, (keeper, victim, keeper))
    cur.execute(
        """
        INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source)
        SELECT from_unit, %s, rank, source FROM bt_3_lex_links
        WHERE to_unit = %s AND from_unit <> %s
        ON CONFLICT (from_unit, to_unit) DO UPDATE SET rank = LEAST(bt_3_lex_links.rank, EXCLUDED.rank);
        """, (keeper, victim, keeper))
    cur.execute(
        """
        INSERT INTO bt_3_lex_unit_sources (unit_id, entry_id, side)
        SELECT %s, entry_id, side FROM bt_3_lex_unit_sources WHERE unit_id = %s
        ON CONFLICT DO NOTHING;
        """, (keeper, victim))
    cur.execute("DELETE FROM bt_3_lex_units WHERE id = %s;", (victim,))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if not args.dry_run and not args.apply:
        raise SystemExit("укажи --dry-run или --apply")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("нужен DATABASE_URL")
    from openai import OpenAI
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"], timeout=60)

    conn = connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute(SCHEMA_SQL)

    # Что Wiktionary знает о роде — оба наших справочника, объединённо.
    cur.execute("SELECT title, genus FROM bt_3_wiktionary_genus_cache;")
    genus_by_title = dict(cur.fetchall())
    cur.execute("SELECT title, genus FROM bt_3_wiktionary_pos_cache;")
    for title, code in cur.fetchall():
        if code and code != "-":
            genus_by_title[title] = (genus_by_title.get(title, "") or "") + code

    cur.execute(
        """
        SELECT lemma_key, array_agg(id ORDER BY id), array_agg(COALESCE(gender,'') ORDER BY id),
               array_agg(display ORDER BY id)
        FROM bt_3_lex_units WHERE lang = 'de' AND kind = 'word'
        GROUP BY lemma_key HAVING COUNT(*) > 1;
        """
    )
    groups = cur.fetchall()
    print("написаний с несколькими единицами: %d\n" % len(groups))

    merged = relinked = unresolved = homographs = 0
    for lemma_key, ids, genders, displays in groups:
        by_gender = {g: i for i, g in zip(ids, genders) if g}
        if len(by_gender) < 2:
            continue
        lemma = displays[0].split()[-1]
        title = lemma[:1].upper() + lemma[1:]
        code = genus_by_title.get(title, "")
        real = {GENUS_TO_ARTICLE[ch] for ch in code if ch in GENUS_TO_ARTICLE}
        print("  %s (%s) — Wiktionary: %s" % (lemma, " / ".join(displays), "/".join(sorted(real)) or "нет данных"))
        if not real:
            unresolved += len(by_gender)
            print("     рода нет в справочнике — оставляю как есть")
            continue

        # 1) Варианты, которых Wiktionary не знает, — не слова: словоформа или неверный
        #    артикль в старой записи. Сливаем в настоящее слово.
        bogus = [a for a in by_gender if a not in real]
        base = sorted(real & set(by_gender)) or sorted(real)
        for article in bogus:
            if not base:
                unresolved += 1
                continue
            keeper_article = base[0]
            keeper = by_gender.get(keeper_article)
            if keeper is None:
                unresolved += 1
                continue
            print("     %s %-14s → не слово, сливаю в %s %s" % (article, lemma, keeper_article, lemma))
            merged += 1
            if args.apply:
                cur.execute(
                    """
                    INSERT INTO bt_3_lex_form_rulings (lemma_key, article, verdict, base_article, note)
                    VALUES (%s, %s, 'form', %s, %s)
                    ON CONFLICT (lemma_key, article) DO UPDATE
                      SET verdict='form', base_article=EXCLUDED.base_article,
                          note=EXCLUDED.note, checked_at=NOW();
                    """,
                    (lemma_key, article, keeper_article,
                     "Wiktionary знает только: " + "/".join(sorted(real))),
                )
                merge_unit(cur, victim=by_gender[article], keeper=keeper)
            by_gender.pop(article, None)

        if len(by_gender) < 2:
            continue

        # 2) Настоящий омограф: значения разводим по толкованиям с той же страницы.
        homographs += 1
        cur.execute(
            """
            SELECT l.from_unit, l.to_unit, u2.display FROM bt_3_lex_links l
            JOIN bt_3_lex_units u2 ON u2.id = l.to_unit
            WHERE l.from_unit = ANY(%s) AND u2.lang <> 'de';
            """, (list(by_gender.values()),))
        links = cur.fetchall()
        glosses = sorted({g for _f, _t, g in links})
        if not glosses:
            continue
        senses = fetch_senses_by_genus(title)
        if len(senses) < 2:
            print("     толкований по родам не нашлось — оставляю значения как есть")
            unresolved += len(glosses)
            continue
        for article, items in sorted(senses.items()):
            print("        %s %s: %s" % (article, lemma, "; ".join(items[:2])[:110]))
        verdict = ask_match(client, lemma, senses, glosses)
        for gloss in glosses:
            want = verdict.get(gloss, "unknown")
            if want not in by_gender:
                unresolved += 1
                print("     «%s» → не определено, оставляю" % gloss)
                continue
            right = by_gender[want]
            wrong = [(f, t) for f, t, g in links if g == gloss and f != right]
            print("     «%s» → %s %s%s" % (gloss, want, lemma, "" if wrong else " (уже верно)"))
            if not wrong or not args.apply:
                relinked += 1 if wrong else 0
                continue
            relinked += 1
            target_id = [t for f, t, g in links if g == gloss][0]
            for from_unit, to_unit in wrong:
                cur.execute(
                    "DELETE FROM bt_3_lex_links WHERE (from_unit, to_unit) IN ((%s,%s),(%s,%s));",
                    (from_unit, to_unit, to_unit, from_unit))
            cur.execute(
                """
                INSERT INTO bt_3_lex_links (from_unit, to_unit, rank, source)
                VALUES (%s, %s, 10, 'разведение'), (%s, %s, 10, 'разведение')
                ON CONFLICT (from_unit, to_unit) DO UPDATE
                  SET rank = LEAST(bt_3_lex_links.rank, 10), source = 'разведение';
                """, (right, target_id, target_id, right))

    print("\nитог: лишних единиц слито %d, настоящих омографов %d, значений переставлено %d, "
          "не определено %d" % (merged, homographs, relinked, unresolved))
    if args.apply:
        conn.commit()
        print("записано.")
    else:
        conn.rollback()
        print("(--dry-run: в базу ничего не записано)")
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
