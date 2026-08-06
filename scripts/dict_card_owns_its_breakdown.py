"""Спросить у модели: этот разбор описывает ЭТО слово — да или нет?

Что чиним. Карточка «Das viele Geld kommt nicht von ungefähr.» показывает управление,
примеры и мнемонику фразы «Kein Wunder, dass er reich ist». Причина закрыта в коде
06.08.2026 (сохранение второго варианта из разбора больше не копирует чужое тело), но
накопленное осталось: 2 438 карточек делят тело с карточкой другого заголовка.

Почему спрашиваем, а не правилом. Общее тело САМО ПО СЕБЕ не беда: «die
Verhaltensweise» и «ungewöhnliche Verhaltensweise» сохранены из одного разбора и обе
про одно слово — тело им подходит. Беда, когда заголовок с телом не связан вовсе. Я
пробовал развести это правилом трижды: по совпадению основ, по переводу, по похожести —
каждый раз в улов попадали здоровые карточки («Dewählt» при примере «Ich wähle diesen
Film», «Das Auftreten» рядом с «auftreten»). Граница здесь смысловая, и правилом её не
провести. Поэтому вопрос задаётся прямо, по одному на карточку.

Что делаем с ответом «нет»: снимаем ЧУЖОЕ ТЕЛО, оставляя заголовок, перевод и личные
поля человека. Карточка станет тонкой, а разбор приедет с её собственного слова — тем
же путём, которым его получают 12 тысяч других карточек. Ничего не переписываем и не
выдумываем: только убираем то, что принадлежит другому слову.

Партиями с записью после каждой: обрыв связи не должен сжигать оплаченное.

    python scripts/dict_card_owns_its_breakdown.py --sample 40      # проба, без записи
    python scripts/dict_card_owns_its_breakdown.py --all --apply    # всё, партиями
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

from database import get_db_connection_context, strip_card_content_for_subscription  # noqa: E402

WORKERS = 6
CHUNK = 250


def body_key(card) -> str:
    """Отпечаток ТЕЛА разбора. Заголовок и перевод не входят: их подменяют при
    сохранении варианта, а тело остаётся от искомого слова — по нему и опознаём пару."""
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
    cur.execute(
        """SELECT id, user_id, word_de, word_ru, response_json
           FROM bt_3_webapp_dictionary_queries
           WHERE response_json IS NOT NULL AND jsonb_typeof(response_json) = 'object'
             AND COALESCE(word_de, '') <> ''
           ORDER BY id;"""
    )
    groups: dict = {}
    for cid, uid, de, ru, card in cur.fetchall():
        key = body_key(card)
        if not key:
            continue
        groups.setdefault((uid, key), []).append((cid, de, ru, card))
    out = []
    for members in groups.values():
        if len(members) < 2:
            continue
        heads = {str(m[1] or "").strip().casefold() for m in members}
        if len(heads) < 2:
            continue          # одно слово сохранено дважды — тело общее законно
        out.extend(members)
    return out


def asks_model(row) -> tuple:
    """Один вопрос: описывает ли этот разбор это слово."""
    from backend.synthetic_load import build_sync_openai_client
    cid, de, ru, card = row
    api_key = str(os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return cid, None
    examples = [str((e or {}).get("source") or "") for e in (card.get("usage_examples") or [])
                if isinstance(e, dict)][:3]
    payload = {
        "headword": de,
        "translation": ru,
        "examples": examples,
        "collocations": [str(c) for c in (card.get("common_collocations") or [])][:4],
        "government": [str((g or {}).get("pattern") or "") for g in (card.get("government_patterns") or [])
                       if isinstance(g, dict)][:2],
    }
    system = (
        "A learner's dictionary card has a HEADWORD and a body (examples, collocations, "
        "government patterns) that was generated for some German word. Decide whether the "
        "body describes THE HEADWORD.\n"
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
        return cid, None
    return cid, {"belongs": str(data.get("belongs") or "yes").strip().lower(),
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
    print("КАРТОЧЕК С ОБЩИМ ТЕЛОМ: %d" % len(rows))
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
        for cid, de, ru, card in chunk:
            a = answers.get(cid)
            if a is None:
                failed += 1
                continue
            if a["belongs"] == "no":
                chunk_foreign.append((cid, de, ru, card, a["why"]))
            else:
                kept += 1
        foreign.extend(chunk_foreign)
        if args.apply and chunk_foreign:
            with get_db_connection_context() as conn:
                with conn.cursor() as cur:
                    for cid, _de, _ru, card, _why in chunk_foreign:
                        cur.execute(
                            "UPDATE bt_3_webapp_dictionary_queries SET response_json = %s::jsonb WHERE id = %s;",
                            (json.dumps(strip_card_content_for_subscription(card), ensure_ascii=False), cid),
                        )
                conn.commit()
        print("   партия %d–%d: чужих %d, своих %d"
              % (start + 1, start + len(chunk), len(chunk_foreign), len(chunk) - len(chunk_foreign)))

    print()
    print("РАЗБОР СВОЙ:   %d" % kept)
    print("РАЗБОР ЧУЖОЙ:  %d" % len(foreign))
    print("НЕ ОТВЕТИЛА:   %d" % failed)
    print()
    for cid, de, ru, _card, why in foreign[:25]:
        print("   card=%s  %r\n      %s" % (cid, (de or "")[:60], why[:90]))
    if not args.apply:
        print()
        print("ВХОЛОСТУЮ. Записать: --apply (и --all по всем)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
