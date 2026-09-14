# -*- coding: utf-8 -*-
"""Применить решения владельца 14.09.2026 по пересуду. Вердикты берутся ИЗ ФАЙЛА,
который он видел, — ничего не пересуживается заново.

  36 вернуть (с правкой опечатки detailiert → detailliert)
   7 убрать
 331 не трогать

Без --apply идёт СУХИМ: печатает, что сделает, базу не трогает.
"""
import os, sys, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
for k, v in (("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1"), ("SKIP_BILLING_LEDGER_WRITES", "1"),
             ("BILLING_OPENAI_SNAPSHOT_SYNC_ON_STARTUP", "0")):
    os.environ.setdefault(k, v)
import logging
logging.disable(logging.WARNING)
from backend.sprint_revote import revote_row

DRY = "--apply" not in sys.argv
OUT = os.getenv("REVOTE_FILE") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "synonym_revote.json")
rows = json.load(open(OUT, encoding="utf-8"))

# Опечатки, названные владельцем поимённо. Список закрытый: своей правкой написания
# здесь не занимаемся — это работа двери приёма, а не пересуда.
FIX = {"detailiert": "detailliert"}

back = [r for r in rows if r["status"] == "removed" and r["new"] == "yes"]
drop = [r for r in rows if r["status"] == "kept" and r["new"] == "no"]
print(f"### {'СУХОЙ ПРОГОН' if DRY else 'ПРИМЕНЯЮ'}: вернуть {len(back)}, убрать {len(drop)}\n")

done = {"back": 0, "drop": 0, "skip": 0, "fixed": 0}
for kind, group, verdict in (("ВЕРНУТЬ", back, "yes"), ("УБРАТЬ", drop, "no")):
    print(f"--- {kind} ---")
    for r in group:
        final = FIX.get(r["de"].strip().lower())
        if final:
            done["fixed"] += 1
        res = revote_row(row_id=int(r["id"]), verdict=verdict, final_de=final,
                         votes=f"revote:3 · {r['votes']}", dry=DRY)
        if res is None:
            done["skip"] += 1
            print(f"   ⚠️ строка {r['id']} ({r['wort']} → {r['de']}) пропущена: нет или ещё open")
            continue
        if not res.get("changed"):
            done["skip"] += 1
            continue
        done["back" if verdict == "yes" else "drop"] += 1
        mark = f"  ✏️ опечатка → {res['final_de']}" if final else ""
        print(f"   {r['relation'][:3]} {r['wort']:20s} → {res['final_de']:22s} "
              f"[{r['votes']}] {res['from']} → {res['to']}{mark}")
print(f"\n### Итог: вернули {done['back']}, убрали {done['drop']}, "
      f"пропущено {done['skip']}, опечаток поправлено {done['fixed']}")
if DRY:
    print("### Это был сухой прогон. База не тронута.")
