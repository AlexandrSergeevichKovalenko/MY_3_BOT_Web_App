# -*- coding: utf-8 -*-
"""Мост немецкий → английский по всей базе. НИЧЕГО НЕ ПИШЕТ В БАЗУ — только файл."""
import json, os, sys, time, threading, collections
from concurrent.futures import ThreadPoolExecutor
from openai import OpenAI
S = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, S)
from bridge_trial import SYSTEM, MODEL, PRICE      # тот же промпт, что владелец одобрил на пробе

BATCH, THREADS = 10, 8
OUT = os.path.join(S, "bridge_out.jsonl")
client, lock = OpenAI(), threading.Lock()
usage = {"in": 0, "cached": 0, "out": 0}; done = {"n": 0}

items = json.load(open(os.path.join(S, "bridge_input.json"), encoding="utf-8"))
for it in items:
    it["key"] = f'{it["unit_id"]}:{it.get("sense_id") or 0}'
уже = set()
if os.path.exists(OUT):
    for line in open(OUT, encoding="utf-8"):
        try: уже.add(str(json.loads(line).get("key")))
        except Exception: pass
работа = [x for x in items if x["key"] not in уже]
пачки = [работа[i:i+BATCH] for i in range(0, len(работа), BATCH)]
ВСЕГО = len(пачки); t0 = time.time()
print(f"на мост: {len(работа)} (уже сделано {len(уже)}), пачек {ВСЕГО}", flush=True)

def задача(chunk):
    pl = [{"key": i["key"], "kind": i["kind"], "de": i["de"],
           "sense": i.get("sense"), "ru": i.get("ru")} for i in chunk]
    for п in range(5):
        try:
            r = client.chat.completions.create(model=MODEL,
                messages=[{"role": "system", "content": SYSTEM},
                          {"role": "user", "content": json.dumps(pl, ensure_ascii=False)}],
                response_format={"type": "json_object"}, temperature=0.2, timeout=180)
            u = r.usage; c = getattr(getattr(u, "prompt_tokens_details", None), "cached_tokens", 0) or 0
            got = json.loads(r.choices[0].message.content).get("items", [])
            with lock:
                usage["in"] += u.prompt_tokens - c; usage["cached"] += c; usage["out"] += u.completion_tokens
                with open(OUT, "a", encoding="utf-8") as fh:
                    for it in got: fh.write(json.dumps(it, ensure_ascii=False) + "\n")
                    fh.flush()
                done["n"] += 1
                if done["n"] % 25 == 0:
                    ц = (usage["in"]*PRICE["input"]+usage["cached"]*PRICE["cached"]+usage["out"]*PRICE["output"])/1e6
                    print(f"  пачек {done['n']}/{ВСЕГО}  ${ц:.2f}  {round(time.time()-t0)} c", flush=True)
            return
        except Exception as e:
            if п == 4:
                with lock: print(f"  !! пачка НЕ УДАЛАСЬ: {e}", flush=True)
                return
            time.sleep(4*(п+1))

with ThreadPoolExecutor(max_workers=THREADS) as ex: list(ex.map(задача, пачки))
ц = (usage["in"]*PRICE["input"]+usage["cached"]*PRICE["cached"]+usage["out"]*PRICE["output"])/1e6
res = collections.Counter(); ключи = set()
for line in open(OUT, encoding="utf-8"):
    try:
        o = json.loads(line); ключи.add(str(o.get("key")))
        res["с переводом" if o.get("en") else "честное «не знаю»"] += 1
    except Exception: pass
print(json.dumps({"получено": len(ключи), "из": len(items), "итоги": dict(res),
                  "цена_usd": round(ц,3), "минут": round((time.time()-t0)/60,1)},
                 ensure_ascii=False, indent=2), flush=True)
