# -*- coding: utf-8 -*-
"""Строгий проход по куче «неточно». Бинарный вопрос с высокой планкой. В базу не пишет."""
import json, os, sys, time, threading, collections
from concurrent.futures import ThreadPoolExecutor
from openai import OpenAI
S = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, S)
from strict68 import SYSTEM, MODEL, PRICE

BATCH, THREADS = 15, 8
OUT = os.path.join(S, "strict_all.jsonl")
client, lock = OpenAI(), threading.Lock()
usage = {"in": 0, "cached": 0, "out": 0}; done = {"n": 0}

items = {f'{x["unit_id"]}:{x.get("sense_id") or 0}': x
         for x in json.load(open(os.path.join(S, "items_all.json"), encoding="utf-8"))}
# кого проверяем: те, кого ПЕРВЫЙ голос назвал «неточно», плюс его же предложение
fixes, target = {}, []
for line in open(os.path.join(S, "judge_all_votes.jsonl"), encoding="utf-8"):
    try: r = json.loads(line)
    except Exception: continue
    if r.get("voice") == 0 and str(r.get("class")) == "imprecise":
        k = str(r.get("key"))
        if k in items and k not in fixes:
            fixes[k] = r.get("fix"); target.append(k)
print(f"на строгую проверку: {len(target)}", flush=True)

def задача(chunk):
    pl = [{"key": k, "kind": items[k]["kind"], "de": items[k]["de"], "sense": items[k].get("sense"),
           "ru": items[k].get("ru"), "предлагали_вместо": fixes.get(k)} for k in chunk]
    for п in range(4):
        try:
            r = client.chat.completions.create(model=MODEL,
                messages=[{"role": "system", "content": SYSTEM},
                          {"role": "user", "content": json.dumps(pl, ensure_ascii=False)}],
                response_format={"type": "json_object"}, temperature=0, timeout=120)
            u = r.usage; c = getattr(getattr(u, "prompt_tokens_details", None), "cached_tokens", 0) or 0
            got = json.loads(r.choices[0].message.content).get("items", [])
            with lock:
                usage["in"] += u.prompt_tokens - c; usage["cached"] += c; usage["out"] += u.completion_tokens
                with open(OUT, "a", encoding="utf-8") as fh:
                    for it in got: fh.write(json.dumps(it, ensure_ascii=False) + "\n")
                done["n"] += 1
                if done["n"] % 40 == 0:
                    ц = (usage["in"]*PRICE["input"]+usage["cached"]*PRICE["cached"]+usage["out"]*PRICE["output"])/1e6
                    print(f"  пачек {done['n']}/{ВСЕГО}  ${ц:.2f}  {round(time.time()-t0)} c", flush=True)
            return
        except Exception as e:
            if п == 3:
                with lock: print(f"  !! пачка НЕ УДАЛАСЬ: {e}", flush=True)
                return
            time.sleep(3*(п+1))

пачки = [target[i:i+BATCH] for i in range(0, len(target), BATCH)]
ВСЕГО = len(пачки); t0 = time.time(); open(OUT, "w").close()
with ThreadPoolExecutor(max_workers=THREADS) as ex: list(ex.map(задача, пачки))
ц = (usage["in"]*PRICE["input"]+usage["cached"]*PRICE["cached"]+usage["out"]*PRICE["output"])/1e6
res = collections.Counter()
for line in open(OUT, encoding="utf-8"):
    try: res[json.loads(line).get("v")] += 1
    except Exception: pass
print(json.dumps({"проверено": sum(res.values()), "из": len(target), "вердикты": dict(res),
                  "цена_usd": round(ц,3), "минут": round((time.time()-t0)/60,1)}, ensure_ascii=False, indent=1), flush=True)
