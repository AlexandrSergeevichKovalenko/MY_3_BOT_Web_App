# -*- coding: utf-8 -*-
"""Второй проход по спорной куче. Вопрос БИНАРНЫЙ и с высокой планкой. В базу не пишет."""
import json, os, collections
from openai import OpenAI
S = os.path.dirname(os.path.abspath(__file__))
MODEL, PRICE = "gpt-4.1-mini", {"input": 0.40, "output": 1.60, "cached": 0.10}

SYSTEM = """Ты решаешь ОДИН вопрос про запись учебного словаря, и планка у тебя ВЫСОКАЯ.

Вопрос: ВЫУЧИТ ЛИ УЧЕНИК НЕВЕРНОЕ, если оставить русский перевод как есть?

Отвечай "harm" ТОЛЬКО если да — то есть перевод назовёт не то понятие, собьёт с толку
или заставит употребить слово неправильно. Примеры "harm":
  · "allmählich" (постепенно) переведено как "потом" — это другое понятие;
  · "aufwickeln" (наматывать) переведено как "расслаблять" — перепутано с англ. unwind;
  · "Willkommen" (приветствие) переведено как "встреча".

Отвечай "ok" во ВСЕХ остальных случаях, даже если перевод можно улучшить:
  · синоним или более широкое слово, которое всё равно ведёт к верному пониманию
    ("феucht" → "сырой" вместо "влажный", "пользоваться" вместо "применять") — это "ok";
  · неуклюжая формулировка, канцелярит, длиннота — "ok";
  · твой вариант просто красивее — "ok". Красота не повод переписывать живую базу.

Отдельный класс: "dup" — если в русском поле одно и то же значение повторено дважды
через запятую или точку с запятой («поймать; поймать», «образец, ... , образец»).
Это чинится удалением повтора, модель для этого не нужна.

Формат — строго JSON: {"items":[{"key":"...","v":"harm|ok|dup","why":"кратко"}]}"""


def main():
    d = json.load(open(os.path.join(S, "judge400_result.json"), encoding="utf-8"))
    rows = [x for x in d if x["verdict"] == "imprecise"]
    client, usage, out = OpenAI(), {"in": 0, "cached": 0, "out": 0}, []
    for start in range(0, len(rows), 25):
        chunk = rows[start:start + 25]
        payload = [{"key": i["key"], "kind": i["kind"], "de": i["de"],
                    "sense": i.get("sense"), "ru": i.get("ru"),
                    "предлагали_вместо": i.get("fix")} for i in chunk]
        r = client.chat.completions.create(
            model=MODEL, messages=[{"role": "system", "content": SYSTEM},
                                   {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}],
            response_format={"type": "json_object"}, temperature=0)
        u = r.usage
        c = getattr(getattr(u, "prompt_tokens_details", None), "cached_tokens", 0) or 0
        usage["in"] += u.prompt_tokens - c; usage["cached"] += c; usage["out"] += u.completion_tokens
        out.extend(json.loads(r.choices[0].message.content).get("items", []))
    by = {str(r["key"]): r for r in out}
    merged = [{**x, "strict": by.get(x["key"], {}).get("v", "?"),
               "strict_why": by.get(x["key"], {}).get("why")} for x in rows]
    json.dump(merged, open(os.path.join(S, "strict68_result.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    cost = (usage["in"] * PRICE["input"] + usage["cached"] * PRICE["cached"]
            + usage["out"] * PRICE["output"]) / 1_000_000
    print(json.dumps({"из imprecise": len(rows),
                      "строгий вердикт": dict(collections.Counter(m["strict"] for m in merged)),
                      "цена_usd": round(cost, 4)}, ensure_ascii=False, indent=1))
    print("\n--- ЧТО ПРИЗНАНО ВРЕДНЫМ ---")
    for m in merged:
        if m["strict"] == "harm":
            print(f"  {m['de']}  →  {m['ru']}   [вместо: {m.get('fix')}]")
    print("\n--- ПОВТОРЫ В ПЕРЕЧИСЛЕНИИ ---")
    for m in merged:
        if m["strict"] == "dup":
            print(f"  {m['de']}  →  {m['ru']}")


if __name__ == "__main__":
    main()
