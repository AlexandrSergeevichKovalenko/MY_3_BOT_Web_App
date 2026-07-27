# -*- coding: utf-8 -*-
"""Словоформы из Wiktionary → указатели слоя.

Человек тапает слово в тексте в той форме, в какой оно там стоит: «den Rüpeln»,
«des Helden», «Häuser», «angefangen». Слой знает только начальную форму, поэтому такой
запрос уходит в GPT (деньги), а в общий банк ложится новая запись на словоформу — так
там и появились «Kindern», «Hunde» и записи, которые сам Wiktionary зовёт «deklinierte
Form».

Почему не свой генератор парадигм: он строит формы по правилам и на слабом склонении
ошибается — «der Held» даёт «Helds» вместо «des Helden». Мы уже условились, что факты о
слове берёт авторитет, а не догадка (так чинили род), поэтому формы читаем из шаблона
обзора на странице Wiktionary — там они выписаны явно.

Пишет в свою таблицу-справочник bt_3_wiktionary_forms и добавляет указатели в слой.
Указатель НЕ ставится, если такое написание уже принадлежит другому слову как его
собственное: «die Kiefer» — и множественное число «челюсти», и отдельное слово «сосна».

Запуск:
    DATABASE_URL=... python3 scripts/dict_units_wikt_forms.py --dry-run
    DATABASE_URL=... python3 scripts/dict_units_wikt_forms.py --apply [--limit N]
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
import psycopg2.extras

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.article_wiktionary_ref import _API, _DE_HEADER, _NEXT_LANG, _UA  # noqa: E402

SPACE_RE = re.compile(r"\s+")
BATCH = 45
PAUSE = 2.0
RETRIES = 4
MIN_LEN = 3

# Шаблоны обзора: в них формы выписаны параметрами («Nominativ Plural=Rüpel»).
OVERVIEW_RE = re.compile(r"\{\{Deutsch[- ](?:Substantiv|Verb|Adjektiv)[- ]Übersicht(.*?)\}\}", re.DOTALL)
# Параметры, в которых лежат ИМЕННО формы слова, а не служебные пометки.
FORM_PARAM_RE = re.compile(
    r"\|\s*((?:Nominativ|Genitiv|Dativ|Akkusativ)\s+(?:Singular|Plural)\*?\d?"
    r"|Präsens_(?:ich|du|er, sie, es)|Präteritum_ich|Partizip II|Konjunktiv II_ich"
    r"|Positiv|Komparativ|Superlativ)\s*=\s*([^|}\n]+)")
STOP_TOKENS = {
    "der", "die", "das", "den", "dem", "des", "ein", "eine", "einen", "einem", "einer", "eines",
    "hat", "habe", "ist", "sind", "war", "wird", "werden", "sich", "zu", "am", "—", "-", "?",
}

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS bt_3_wiktionary_forms (
    title      TEXT NOT NULL,
    form_key   TEXT NOT NULL,      -- «Genitiv Singular», «Partizip II», …
    value      TEXT NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (title, form_key, value)
);
CREATE INDEX IF NOT EXISTS idx_wikt_forms_title ON bt_3_wiktionary_forms (title);
"""


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


def fetch_forms(titles: list[str]) -> dict[str, list[tuple[str, str]]]:
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
    out: dict[str, list[tuple[str, str]]] = {}
    for page in (data.get("query") or {}).get("pages") or []:
        title = str(page.get("title") or "")
        if page.get("missing"):
            out[title] = []
            continue
        try:
            wikitext = page["revisions"][0]["slots"]["main"]["content"]
        except (KeyError, IndexError, TypeError):
            out[title] = []
            continue
        match = _DE_HEADER.search(wikitext)
        if not match:
            out[title] = []
            continue
        block = wikitext[match.end():]
        nxt = _NEXT_LANG.search(block)
        if nxt:
            block = block[:nxt.start()]
        found: list[tuple[str, str]] = []
        for overview in OVERVIEW_RE.findall(block):
            for key, value in FORM_PARAM_RE.findall(overview):
                value = value.strip()
                if value and value not in {"—", "-", "?"}:
                    found.append((key.strip(), value))
        out[title] = found
    return out


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
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(SCHEMA_SQL)

    cur.execute(
        """
        SELECT id, lemma, display, pos FROM bt_3_lex_units
        WHERE lang = 'de' AND kind = 'word'
          AND pos IN ('noun', 'verb', 'adjective');
        """
    )
    units = cur.fetchall()
    by_title: dict[str, list[int]] = {}
    for unit_id, lemma, _display, _pos in units:
        title = lemma[:1].upper() + lemma[1:] if _pos == "noun" else lemma[:1].lower() + lemma[1:]
        by_title.setdefault(title, []).append(unit_id)
    cur.execute("SELECT DISTINCT title FROM bt_3_wiktionary_forms;")
    have = {r[0] for r in cur.fetchall()}
    todo = sorted(t for t in by_title if t not in have)
    print("слов (сущ./глаг./прил.): %d, страниц спросить: %d (~%d запросов)"
          % (len(units), len(todo), (len(todo) - 1) // BATCH + 1 if todo else 0))
    if args.limit:
        todo = todo[: args.limit]

    if args.apply and todo:
        for index in range(0, len(todo), BATCH):
            batch = todo[index:index + BATCH]
            got = {}
            for attempt in range(RETRIES):
                got = fetch_forms(batch)
                if got:
                    break
                time.sleep(PAUSE * (2 ** attempt) * 5)
            if not got:
                print("  не отвечает на %d слове — остальное доспросим позже" % index)
                break
            rows = [(t, k, v) for t, pairs in got.items() for k, v in pairs]
            if rows:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO bt_3_wiktionary_forms (title, form_key, value) VALUES %s "
                    "ON CONFLICT DO NOTHING", rows, page_size=500)
            # Пустую страницу тоже отмечаем, иначе будем спрашивать её каждый прогон.
            empty = [(t, "—", "—") for t, pairs in got.items() if not pairs]
            if empty:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO bt_3_wiktionary_forms (title, form_key, value) VALUES %s "
                    "ON CONFLICT DO NOTHING", empty, page_size=500)
            print("  %5d/%d …" % (min(index + BATCH, len(todo)), len(todo)), flush=True)
            time.sleep(PAUSE)

    # ── формы → указатели ─────────────────────────────────────────────────────
    cur.execute("SELECT title, form_key, value FROM bt_3_wiktionary_forms WHERE form_key <> '—';")
    forms_by_title: dict[str, set] = {}
    for title, _key, value in cur.fetchall():
        for token in SPACE_RE.split(str(value).strip()):
            token = token.strip(" ,;.—-").casefold()
            if len(token) >= MIN_LEN and token not in STOP_TOKENS:
                forms_by_title.setdefault(title, set()).add(token)

    cur.execute(
        """
        SELECT s.surface_key, s.unit_id FROM bt_3_lex_surfaces s
        WHERE s.lang = 'de' AND s.match_kind IN ('exact', 'no_article');
        """
    )
    owned: dict[str, set] = {}
    for surface_key, unit_id in cur.fetchall():
        owned.setdefault(surface_key, set()).add(unit_id)
    cur.execute("SELECT surface_key, unit_id FROM bt_3_lex_surfaces WHERE lang = 'de';")
    existing = {(r[0], r[1]) for r in cur.fetchall()}

    payload: list[tuple] = []
    collisions = covered = 0
    samples: list[str] = []
    for title, unit_ids in by_title.items():
        forms = forms_by_title.get(title) or set()
        if not forms:
            continue
        for unit_id in unit_ids:
            added = 0
            for form in forms:
                holders = owned.get(form)
                if holders and unit_id not in holders:
                    collisions += 1
                    continue
                if (form, unit_id) in existing:
                    continue
                payload.append(("de", form, unit_id, "inflected"))
                added += 1
            if added:
                covered += 1
                if len(samples) < 6:
                    samples.append("%-20s → %s" % (title, ", ".join(sorted(forms)[:7])))

    print("\n  слов, у которых Wiktionary дал формы: %d" % len(forms_by_title))
    print("  новых указателей:                    %d" % len(payload))
    print("  пропущено (написание занято другим словом): %d" % collisions)
    for line in samples:
        print("   " + line)

    if not args.apply:
        print("\n(--dry-run: в базу ничего не записано)")
        return 0
    if payload:
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind) VALUES %s "
            "ON CONFLICT (lang, surface_key, unit_id) DO NOTHING",
            payload, page_size=1000)
    cur.execute("SELECT COUNT(*) FROM bt_3_lex_surfaces WHERE match_kind = 'inflected';")
    print("\nзаписано. указателей-словоформ в слое: %d" % cur.fetchone()[0])
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
