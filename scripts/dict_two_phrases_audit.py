# -*- coding: utf-8 -*-
"""Разбор: где в одной карточке лежат ДВЕ фразы и что с ними делать.

Владелец 11.08.2026 прислал карточку, где на немецкой стороне две фразы подряд:
«Die häufigen Ermahnungen helfen nicht, sein Verhalten zu ändern. - Häufige
Ermahnungen haben nicht dazu beigetragen, sein Verhalten zu ändern.» Это два
варианта одного русского предложения, слитые в одну карточку.

Механически такое не разделить, и это не оговорка, а измеренный факт. Признак
«есть тире» даёт 78 записей, и среди них три РАЗНЫХ случая вперемешку:

    Hast du Zeit? – Jein.                              — диалог, делить нельзя
    Zusammenarbeit ist keine Einbahnstraße – beide…    — тире внутри предложения
    infolge der steigenden Mieten - wegen der Miet…    — ВОТ ЭТО варианты

Признак «две точки» (108 записей) почти весь ложный: «Hast du das gehört? Ich habe
nichts mitbekommen.» — осмысленная пара реплик, карточка целая.

Поэтому работа в два шага: механика только НАХОДИТ кандидатов (широко, с запасом),
а решает модель. Скрипт ничего не меняет — он считает и показывает.

ЧЕСТНО О ТОЧНОСТИ. Прогон 11.08.2026 по 185 кандидатам дал 33 «варианта», но на
самой карточке, с которой всё началось, модель ошиблась: «Die häufigen Ermahnungen
helfen nicht… - Häufige Ermahnungen haben nicht dazu beigetragen…» она назвала одним
предложением, хотя в объяснении сама написала «вторая часть переформулирует». То
есть числа отсюда — ориентир для глаз, а не приговор записи.

И главное: настоящая причина нашлась НЕ здесь, а в сырых полях. У 68 записей
апреля–июня 2026 в source_text склеены немецкая фраза и её русский перевод
(«Sie weinte vor lauter Kummer. — Она плакала…»), а в target_text лежит вторая
немецкая формулировка. Такое чинится механически и без модели — см. отчёт владельцу.

Запуск:
    DATABASE_URL=... OPENAI_API_KEY=... python3 scripts/dict_two_phrases_audit.py
    DATABASE_URL=... OPENAI_API_KEY=... python3 scripts/dict_two_phrases_audit.py --limit 30
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter

import psycopg2

# Кандидаты ищем ШИРОКО: пропустить случай дороже, чем показать лишний — лишний
# отсеет модель, а пропущенный не увидит никто.
CANDIDATE_PATTERNS = (
    re.compile(r"[\wäöüß.!?»\"']\s+[-–—]\s+[\wÄÖÜA-ZА-Я«\"']"),   # тире между кусками
    re.compile(r"[.!?]\s+\S.*[.!?]\s*$"),                          # два законченных предложения
    re.compile(r"\S\s*/\s*\S"),                                    # слэш
    re.compile(r"\S;\s+\S"),                                       # точка с запятой
)

VERDICTS = {
    "варианты":     "две записи одного смысла — можно развести на две карточки",
    "диалог":       "реплика и ответ — карточка целая",
    "одно":         "одно предложение, знак внутри — карточка целая",
    "продолжение":  "вторая фраза продолжает первую — карточка целая",
    "форма":        "варианты управления или рода в одной строке — карточка целая",
}

SYSTEM = (
    "Ты лексикограф. Тебе дают ОДНУ сторону карточки словаря (немецкий или русский "
    "текст) и её перевод. Реши, что перед тобой, и ответь СТРОГИМ JSON:\n"
    '{"verdict":"варианты|диалог|одно|продолжение|форма","parts":["…","…"],'
    '"why":"<одна короткая фраза по-русски>"}\n\n'
    "Значения:\n"
    "• варианты — ДВА самостоятельных высказывания, выражающих примерно одно и то же "
    "(перефразировки, синонимичные обороты). Каждое можно учить отдельно.\n"
    "• диалог — вопрос и ответ, реплика и отклик. Смысл рождается только вместе.\n"
    "• одно — это ОДНО предложение, а тире/двоеточие стоит внутри него как знак "
    "препинания.\n"
    "• продолжение — вторая фраза развивает первую, вместе они одна мысль или "
    "маленькая сценка.\n"
    "• форма — перечислены варианты управления, рода или формы одного и того же "
    "слова (der/die, jemanden/etwas, jdn./etw., ед./мн.).\n\n"
    "ГЛАВНАЯ ПРОВЕРКА для «варианты»: возьми КАЖДУЮ часть отдельно и спроси — "
    "переводится ли она тем же самым переводом, что дан у карточки целиком? Если да "
    "обеим — это варианты. Если part по отдельности значит НЕ то же самое, это не "
    "варианты.\n\n"
    "Примеры, которые НЕ являются вариантами:\n"
    "• «Punkt. Fertig. Aus.» — куски одного устойчивого выражения; «Punkt.» отдельно "
    "не значит «точка, готово, всё».\n"
    "• «jdn./etw. zurichten» — это управление глагола, verdict=\"форма\".\n"
    "• «Hast du Zeit? – Jein.» — вопрос и ответ, verdict=\"диалог\".\n\n"
    "Пример, который ЯВЛЯЕТСЯ вариантами:\n"
    "• «Sie weinte vor lauter Kummer. - Sie weinte allein vor Kummer.» — два целых "
    "предложения об одном и том же.\n\n"
    "parts заполняй ТОЛЬКО для verdict=\"варианты\": два текста без разделителя, "
    "каждый самостоятельный и грамматически целый. Иначе parts = [].\n"
    "Сомневаешься — выбирай НЕ «варианты»: разделить лишнее хуже, чем оставить."
)

# Сокращения немецкого управления. Слэш между ними — это грамматика глагола, а не
# две фразы, и спрашивать про такое модель незачем.
_GOVERNMENT_RE = re.compile(
    r"\b(?:jdn|jdm|jds|jemanden|jemandem|jemandes|etw|etwas|sich)\b\.?\s*/",
    re.IGNORECASE,
)


def find_candidates(cur, limit: int | None) -> list[dict]:
    cur.execute(
        """
        SELECT id, source_text, target_text, source_lang, target_lang
        FROM bt_3_dictionary_entries
        WHERE source_text LIKE '%% %%' OR target_text LIKE '%% %%'
        ORDER BY id DESC;
        """
    )
    out: list[dict] = []
    for eid, src, tgt, s_lang, t_lang in cur.fetchall():
        studied = (src if s_lang != "ru" else tgt) or ""
        native = (tgt if s_lang != "ru" else src) or ""
        if not studied or len(studied) > 400:
            continue
        if _GOVERNMENT_RE.search(studied):
            continue  # «jdn./etw. …» — управление глагола, а не две фразы
        if not any(rx.search(studied) for rx in CANDIDATE_PATTERNS):
            continue
        out.append({"id": eid, "studied": studied, "native": native,
                    "source_lang": s_lang, "target_lang": t_lang})
        if limit and len(out) >= limit:
            break
    return out


def judge(client, item: dict) -> dict:
    payload = {"text": item["studied"], "translation": item["native"]}
    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            temperature=0,
            response_format={"type": "json_object"},
        )
        data = json.loads(resp.choices[0].message.content or "{}") or {}
    except Exception as exc:
        return {"verdict": "не ответила", "parts": [], "why": str(exc)[:80]}
    verdict = str(data.get("verdict") or "").strip().lower()
    if verdict not in VERDICTS:
        verdict = "не ответила"
    parts = [str(p).strip() for p in (data.get("parts") or []) if str(p).strip()]
    why = str(data.get("why") or "")[:120]
    # Страховка: «варианты» без двух целых кусков — это не варианты, а недосказ.
    if verdict == "варианты" and len(parts) != 2:
        verdict = "не ответила"
    # Модель объяснила словом «управление», а вердикт поставила «варианты» — так она
    # спорит сама с собой. Верим объяснению: оно конкретнее ярлыка.
    if verdict == "варианты" and re.search(r"управлени|род[аы]?\b|форм[аы]", why, re.IGNORECASE):
        verdict, parts = "форма", []
    return {"verdict": verdict, "parts": parts[:2], "why": why}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="сколько кандидатов разбирать (0 — все)")
    parser.add_argument("--out", default="", help="куда сложить разбор в JSON")
    args = parser.parse_args()

    dsn = os.getenv("DATABASE_URL")
    api_key = str(os.getenv("OPENAI_API_KEY") or "").strip()
    if not dsn or not api_key:
        print("Нужны DATABASE_URL и OPENAI_API_KEY", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(dsn, connect_timeout=25)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM bt_3_dictionary_entries WHERE source_text LIKE '%% %%';")
        phrases_total = cur.fetchone()[0]
        candidates = find_candidates(cur, args.limit or None)
    conn.close()

    print(f"Фраз в пуле: {phrases_total}")
    print(f"Кандидатов «здесь может быть две фразы»: {len(candidates)}"
          f"  ({100.0 * len(candidates) / max(phrases_total, 1):.1f}%)\n")

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from backend.synthetic_load import build_sync_openai_client
    client = build_sync_openai_client(api_key=api_key, timeout=30)

    results = []
    counts = Counter()
    for index, item in enumerate(candidates, 1):
        verdict = judge(client, item)
        counts[verdict["verdict"]] += 1
        results.append({**item, **verdict})
        print(f"  {index}/{len(candidates)}", end="\r")

    print("\nЧТО ЭТО ОКАЗАЛОСЬ:")
    for name, n in counts.most_common():
        print(f"   {name:<14} {n:>4}   {VERDICTS.get(name, '')}")

    splittable = [r for r in results if r["verdict"] == "варианты"]
    print(f"\nМОЖНО РАЗВЕСТИ НА ДВЕ КАРТОЧКИ: {len(splittable)}")
    for row in splittable[:20]:
        print(f"\n   [{row['id']}] {row['studied'][:150]}")
        print(f"        RU: {row['native'][:100]}")
        print(f"        → 1) {row['parts'][0][:110]}")
        print(f"        → 2) {row['parts'][1][:110]}")
        print(f"        ({row['why']})")

    print("\nОСТАВИТЬ КАК ЕСТЬ — примеры:")
    for name in ("диалог", "одно", "продолжение", "форма"):
        sample = next((r for r in results if r["verdict"] == name), None)
        if sample:
            print(f"   {name:<12} [{sample['id']}] {sample['studied'][:100]}")

    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(results, fh, ensure_ascii=False, indent=2)
        print(f"\nПолный разбор: {args.out}")
    print("\nБаза НЕ изменена — это разбор.")


if __name__ == "__main__":
    main()
