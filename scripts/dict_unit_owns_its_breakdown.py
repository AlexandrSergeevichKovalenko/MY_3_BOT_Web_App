"""То же самое, но для СЛОВА в общем словаре, а не для личной карточки.

Почему понадобилось. Я почистил личные карточки от чужого разбора и доложил «готово», а
владелец открыл ту же карточку — и увидел прежнее. Потому что карточка тянет разбор со
СЛОВА, а слово я не тронул: чужое тело осталось лежать там. Половина работы, поданная
как целая.

Механизм тот же. Разбор, собранный для одного слова, попадал на другое при сохранении
второго варианта, а оттуда «сведением» уезжал на слово в общий словарь. Замер
06.08.2026: 3 380 слов делят тело с другим словом.

И решается так же: общее тело САМО ПО СЕБЕ не беда («die Zielrichtung» и «klare
Zielrichtung» — про одно), беда, когда тело про другое выражение. Правилом эту границу
не провести, она смысловая, поэтому спрашиваем модель по одному вопросу на слово.

Ответ «нет» — снимаем разбор со слова целиком. Слово останется с переводом и связями,
а разбор соберётся заново ночным добором. Пусто честнее, чем чужое.

    python scripts/dict_unit_owns_its_breakdown.py --sample 40
    python scripts/dict_unit_owns_its_breakdown.py --all --apply
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor

_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, ".."))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

from database import get_db_connection_context  # noqa: E402

WORKERS = 6
CHUNK = 250


def body_key(card) -> str:
    if not isinstance(card, dict):
        return ""
    blob = "|".join([
        json.dumps(card.get("usage_examples") or [], ensure_ascii=False, sort_keys=True),
        json.dumps(card.get("common_collocations") or [], ensure_ascii=False, sort_keys=True),
        json.dumps(card.get("government_patterns") or [], ensure_ascii=False, sort_keys=True),
        str(card.get("memory_tip") or ""),
    ])
    if len(blob.strip("|[]{} \"")) < 20:
        return ""
    return hashlib.sha1(blob.encode("utf-8", "ignore")).hexdigest()


def collect(cur) -> list:
    cur.execute("SELECT id, display, card FROM bt_3_lex_units WHERE card IS NOT NULL ORDER BY id;")
    groups: dict = {}
    for uid, display, card in cur.fetchall():
        key = body_key(card)
        if key:
            groups.setdefault(key, []).append((uid, display, card))
    out = []
    for members in groups.values():
        if len({str(d or "").strip().casefold() for _i, d, _c in members}) > 1:
            out.extend(members)
    return out


def asks_model(row) -> tuple:
    from backend.synthetic_load import build_sync_openai_client
    uid, display, card = row
    api_key = str(os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return uid, None
    payload = {
        "headword": display,
        "examples": [str((e or {}).get("source") or "") for e in (card.get("usage_examples") or [])
                     if isinstance(e, dict)][:3],
        "collocations": [str(c) for c in (card.get("common_collocations") or [])][:4],
        "government": [str((g or {}).get("pattern") or "") for g in (card.get("government_patterns") or [])
                       if isinstance(g, dict)][:2],
    }
    system = (
        "A German dictionary entry has a HEADWORD and a body (examples, collocations, "
        "government patterns) generated for some German word. Decide whether the body "
        "describes THE HEADWORD.\n"
        "- Answer yes if the body is about the headword or its immediate word family "
        "(a noun and its verb, a phrase and its base form, singular and plural).\n"
        "- Answer no ONLY if the body is about a clearly DIFFERENT expression: a synonym "
        "built from other words, or an unrelated phrase.\n"
        "- When unsure, answer yes: removing a body that fits is worse than keeping one "
        "that is merely loose.\n"
        'Answer STRICT JSON only: {"belongs":"yes|no","why":"<short RUSSIAN>"}'
    )
    try:
        client = build_sync_openai_client(api_key=api_key, timeout=15)
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "system", "content": system},
                      {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}],
            temperature=0,
            response_format={"type": "json_object"},
        )
        data = json.loads(resp.choices[0].message.content or "{}") or {}
    except Exception:
        return uid, None
    return uid, {"belongs": str(data.get("belongs") or "yes").strip().lower(),
                 "why": str(data.get("why") or "").strip()}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--sample", type=int, default=40)
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            rows = collect(cur)
    print("СЛОВ С ОБЩИМ ТЕЛОМ: %d" % len(rows))
    if not args.all:
        import random
        random.seed(7)
        rows = random.sample(rows, min(args.sample, len(rows)))
    print("СПРАШИВАЕМ ПРО: %d" % len(rows))

    foreign, kept, failed = [], 0, 0
    for start in range(0, len(rows), CHUNK):
        chunk = rows[start:start + CHUNK]
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            answers = dict(pool.map(asks_model, chunk))
        chunk_foreign = []
        for uid, display, card in chunk:
            a = answers.get(uid)
            if a is None:
                failed += 1
                continue
            if a["belongs"] == "no":
                chunk_foreign.append((uid, display, a["why"]))
            else:
                kept += 1
        foreign.extend(chunk_foreign)
        if args.apply and chunk_foreign:
            with get_db_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE bt_3_lex_units SET card = NULL, card_source = NULL WHERE id = ANY(%s);",
                        ([int(u) for u, _d, _w in chunk_foreign],),
                    )
                conn.commit()
        print("   партия %d–%d: чужих %d, своих %d"
              % (start + 1, start + len(chunk), len(chunk_foreign), len(chunk) - len(chunk_foreign)))

    print()
    print("РАЗБОР СВОЙ:  %d" % kept)
    print("РАЗБОР ЧУЖОЙ: %d" % len(foreign))
    print("НЕ ОТВЕТИЛА:  %d" % failed)
    for uid, display, why in foreign[:20]:
        print("   unit=%s %r\n      %s" % (uid, (display or "")[:58], why[:90]))
    if not args.apply:
        print()
        print("ВХОЛОСТУЮ. Записать: --apply (и --all по всем)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
