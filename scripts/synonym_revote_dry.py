# -*- coding: utf-8 -*-
"""СУХОЙ ПРОГОН: пересудить тремя голосами то, что отсудили одним 06.09.2026.

ПОВОД. Перемер 14.09.2026 показал, что одноголосый вердикт НЕ ВОСПРОИЗВОДИТСЯ: тот же
судья тем же правилом меняет решение у 16% отклонённых синонимов и 38% антонимов.
Почти весь банк (374 кандидата) отсудили одним голосом 06.09 — за два дня до того, как
владелец ввёл три голоса. Сейчас три голоса работают на всём новом.

ЧТО ДЕЛАЕМ: те же кандидаты, тот же промпт, но ТРИ голоса и вердикт по большинству —
ровно тот механизм, что уже стоит в двери (synonym_judge.judge_by_majority).

БАЗУ НЕ ТРОГАЕТ. Печатает, что изменилось бы в обе стороны.
"""
import os, sys, collections
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
for k, v in (("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1"), ("SKIP_BILLING_LEDGER_WRITES", "1"),
             ("BILLING_OPENAI_SNAPSHOT_SYNC_ON_STARTUP", "0")):
    os.environ.setdefault(k, v)
import logging
logging.disable(logging.WARNING)
from backend.database import get_db_connection_context
from backend.synonym_judge import judge_by_majority
OUT = os.getenv("REVOTE_FILE") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "synonym_revote.json")

with get_db_connection_context() as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT relation, wort, hint_ru, de, ru, judge_verdict, judge_example_target,
                   id, status
            FROM bt_3_sprint_accepted_review
            WHERE judge_verdict IS NOT NULL AND judge_voice NOT LIKE '%:3 %'
            ORDER BY relation, wort, id;""")
        rows = cur.fetchall() or []

groups = collections.OrderedDict()
for relation, wort, hint, de, ru, old, base, rid, status in rows:
    g = groups.setdefault((relation, wort), {"meaning_ru": hint or "", "base": base or "", "c": []})
    g["c"].append({"de": de, "ru": ru or "", "old": old, "id": rid, "status": status})

print(f"### Кандидатов одним голосом: {len(rows)} у {len(groups)} слов")
print(f"### Обращений будет примерно: {len(groups) * 3}\n")

stat = collections.Counter()
saved = []
back, dropped, unstable = [], [], []
failed = 0
for (relation, wort), g in groups.items():
    res = judge_by_majority(target=wort, relation=relation, meaning_ru=g["meaning_ru"],
                            base_de=g["base"], candidates=g["c"])
    if not res.get("rows"):
        failed += len(g["c"])
        print(f"   ⚠️ {wort} ({relation}): {res.get('why')}")
        continue
    for c, r in zip(g["c"], res["rows"]):
        old, new = c["old"], r["verdict"]
        saved.append({"id": c["id"], "relation": relation, "wort": wort, "de": c["de"],
                      "ru": c["ru"], "old": old, "status": c["status"], "new": new,
                      "votes": f"{r['yes_votes']}/{r['heard']}", "reason_ru": r["reason_ru"],
                      "example_target_de": r["example_target_de"],
                      "example_candidate_de": r["example_candidate_de"],
                      "voice": res.get("voice", "")})
        stat[(relation, c["status"], old, new)] += 1
        if r["yes_votes"] not in (0, r["heard"]):
            unstable.append((wort, c["de"], f"{r['yes_votes']}/{r['heard']}"))
        if c["status"] == "removed" and new == "yes":
            back.append((relation, wort, c["de"], f"{r['yes_votes']}/{r['heard']}", r["reason_ru"][:58]))
        if c["status"] == "kept" and new == "no":
            dropped.append((relation, wort, c["de"], f"{r['yes_votes']}/{r['heard']}", r["reason_ru"][:58]))

print(f"\n### ИТОГ (не ответил судья: {failed})")
for (rel, st, old, new), n in sorted(stat.items()):
    mark = ""
    if st == "removed" and new == "yes": mark = "→ ВЕРНЁТСЯ В ИГРУ"
    if st == "kept" and new == "no": mark = "→ УЙДЁТ ИЗ ИГРЫ"
    print(f"   {rel:8s} в игре={st:8s} было «{old}» стало «{new}»: {n}  {mark}")
import json
json.dump(saved, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print(f"\n### Вердикты сохранены: {OUT} ({len(saved)} строк)")
print(f"\n### ВЕРНУЛОСЬ БЫ В ИГРУ: {len(back)}")
for rel, w, de, votes, why in back[:25]:
    print(f"   {rel[:3]} {w} → {de}   [{votes}]  {why}")
print(f"\n### УШЛО БЫ ИЗ ИГРЫ: {len(dropped)}")
for rel, w, de, votes, why in dropped[:25]:
    print(f"   {rel[:3]} {w} → {de}   [{votes}]  {why}")
print(f"\n### СПОРНЫХ (голоса разошлись, не 0/3 и не 3/3): {len(unstable)}")
