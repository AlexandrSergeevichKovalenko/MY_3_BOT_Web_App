# -*- coding: utf-8 -*-
"""Полная диагностика немецко-русской базы. ТОЛЬКО ЧТЕНИЕ, в базу не пишет ничего.
Три голоса, 8 потоков, контрольные точки — обрыв не теряет сделанное."""
import json, os, sys, time, threading, collections
from concurrent.futures import ThreadPoolExecutor
from openai import OpenAI

S = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, S)
from judge400 import SYSTEM, MODEL, PRICE           # один и тот же судья, что мерили на 400

VOICES, BATCH, THREADS = 3, 25, 8
OUT = os.path.join(S, "judge_all_votes.jsonl")

client = OpenAI()
lock = threading.Lock()
usage = {"in": 0, "cached": 0, "out": 0}
done = {"n": 0}


def задача(аргс):
    voice, start, chunk = аргс
    payload = [{"key": i["key"], "kind": i["kind"], "de": i["de"],
                "sense": i.get("sense"), "ru": i.get("ru")} for i in chunk]
    for попытка in range(4):
        try:
            r = client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "system", "content": SYSTEM},
                          {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}],
                response_format={"type": "json_object"}, temperature=0.3, timeout=120)
            u = r.usage
            c = getattr(getattr(u, "prompt_tokens_details", None), "cached_tokens", 0) or 0
            items = json.loads(r.choices[0].message.content).get("items", [])
            with lock:
                usage["in"] += u.prompt_tokens - c
                usage["cached"] += c
                usage["out"] += u.completion_tokens
                with open(OUT, "a", encoding="utf-8") as fh:
                    for it in items:
                        fh.write(json.dumps({"voice": voice, **it}, ensure_ascii=False) + "\n")
                done["n"] += 1
                if done["n"] % 25 == 0:
                    цена = (usage["in"] * PRICE["input"] + usage["cached"] * PRICE["cached"]
                            + usage["out"] * PRICE["output"]) / 1_000_000
                    print(f"  пачек {done['n']}/{ВСЕГО}  ${цена:.2f}  {round(time.time()-t0)} c",
                          flush=True)
            return
        except Exception as e:
            if попытка == 3:
                with lock:
                    print(f"  !! пачка voice={voice} start={start} НЕ УДАЛАСЬ: {e}", flush=True)
                return
            time.sleep(3 * (попытка + 1))


items = json.load(open(os.path.join(S, "items_all.json"), encoding="utf-8"))
for it in items:
    it["key"] = f'{it["unit_id"]}:{it.get("sense_id") or 0}'
задачи = [(v, st, items[st:st + BATCH])
          for v in range(VOICES) for st in range(0, len(items), BATCH)]
ВСЕГО = len(задачи)
t0 = time.time()
print(f"записей {len(items)}, пачек {ВСЕГО}, потоков {THREADS}", flush=True)
open(OUT, "w").close()
with ThreadPoolExecutor(max_workers=THREADS) as ex:
    list(ex.map(задача, задачи))
цена = (usage["in"] * PRICE["input"] + usage["cached"] * PRICE["cached"]
        + usage["out"] * PRICE["output"]) / 1_000_000
print(json.dumps({"пачек": done["n"], "из": ВСЕГО, "токены": usage,
                  "цена_usd": round(цена, 3), "минут": round((time.time() - t0) / 60, 1)},
                 ensure_ascii=False, indent=1), flush=True)
