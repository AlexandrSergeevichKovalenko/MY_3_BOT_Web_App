# -*- coding: utf-8 -*-
"""ТРЕТЬЯ проверка: правильна ли сама ЗАМЕНА. Вопрос другой, чем «плох ли оригинал».
В базу не пишет."""
import json,os,sys,time,threading,collections,random
from concurrent.futures import ThreadPoolExecutor
from openai import OpenAI
S=os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0,S)
MODEL,PRICE="gpt-4.1-mini",{"input":0.40,"output":1.60,"cached":0.10}

SYSTEM="""Ты последний контроль перед записью в живую базу учебного словаря.
Кто-то предложил ЗАМЕНИТЬ русский перевод немецкого слова. Твоя работа — не согласиться
вежливо, а НЕ ДАТЬ ИСПОРТИТЬ хорошую запись.

Тебе дают: немецкое "de", вид "kind", пояснение значения "sense", текущий перевод "было"
и предлагаемый "станет".

Ответь одним словом в поле "v":

"replace" — замена ВЕРНА и нужна: старое действительно вводит в заблуждение, новое точнее
            и соответствует ИМЕННО ТОМУ ЗНАЧЕНИЮ, которое описано в "sense".

"keep"    — НЕ менять. Ставь это во всех сомнительных случаях, в том числе:
            · старое соответствует "sense", а новое — другому значению этого слова.
              Пример: "Ziegel" с пояснением «элемент для покрытия крыш» = «черепица»,
              и заменять на «кирпич» НЕЛЬЗЯ, хотя Ziegel бывает и кирпичом;
            · старое и новое — синонимы, разница косметическая;
            · новое сужает или расширяет смысл без нужды;
            · ты не уверен. Молчание дешевле порчи.

"bad_fix" — сама ЗАМЕНА негодная: в неё попал немецкий текст, стрелки, пояснения в
            скобках вместо перевода, пустота, или она не по-русски.

ГЛАВНОЕ ПРАВИЛО: "sense" — это адрес значения. Перевод обязан отвечать ЕМУ, а не самому
частому значению слова вообще. Судья, который проглядел "sense", уже ошибся однажды.

Формат — строго JSON: {"items":[{"key":"...","v":"replace|keep|bad_fix","why":"кратко"}]}"""

items={f'{x["unit_id"]}:{x.get("sense_id") or 0}':x for x in json.load(open(os.path.join(S,"items_all.json"),encoding="utf-8"))}
fin=json.load(open(os.path.join(S,"final_verdicts.json"),encoding="utf-8")); fixes=fin["fixes"]
cls=json.load(open(os.path.join(S,"final_classes.json"),encoding="utf-8"))
цель=[k for k,c in cls.items() if c in ("ЧИНИМ: вредный перевод","ЧИНИМ: значение выдумано") and fixes.get(k)]
random.seed(3); проба=random.sample(цель,min(300,len(цель)))
print(f"к починке с предложенной заменой: {len(цель)}; проверяем пробу {len(проба)}",flush=True)
client,lock=OpenAI(),threading.Lock(); OUT=os.path.join(S,"verify_fix.jsonl"); open(OUT,"w").close()
def z(ch):
    pl=[{"key":k,"kind":items[k]["kind"],"de":items[k]["de"],"sense":items[k].get("sense"),
         "было":items[k].get("ru"),"станет":fixes.get(k)} for k in ch]
    for п in range(4):
        try:
            r=client.chat.completions.create(model=MODEL,messages=[{"role":"system","content":SYSTEM},
                {"role":"user","content":json.dumps(pl,ensure_ascii=False)}],
                response_format={"type":"json_object"},temperature=0,timeout=120)
            with lock:
                with open(OUT,"a",encoding="utf-8") as fh:
                    for it in json.loads(r.choices[0].message.content).get("items",[]):
                        fh.write(json.dumps(it,ensure_ascii=False)+"\n")
            return
        except Exception as e:
            if п==3:
                with lock: print("  !!",e,flush=True); return
            time.sleep(3*(п+1))
пачки=[проба[i:i+10] for i in range(0,len(проба),10)]
t0=time.time()
with ThreadPoolExecutor(max_workers=8) as ex: list(ex.map(z,пачки))
res=collections.Counter(); seen=set()
for line in open(OUT,encoding="utf-8"):
    try:
        o=json.loads(line); res[o.get("v")]+=1; seen.add(str(o.get("key")))
    except Exception: pass
print(json.dumps({"проверено":len(seen),"из":len(проба),"вердикты":dict(res),
                  "минут":round((time.time()-t0)/60,1)},ensure_ascii=False,indent=1),flush=True)
