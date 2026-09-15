# -*- coding: utf-8 -*-
"""Пробная пачка моста de→en. НИЧЕГО НЕ ПИШЕТ В БАЗУ. Только читает файл и зовёт модель."""
import json, os, sys, time
from openai import OpenAI

S = os.path.dirname(os.path.abspath(__file__))
MODEL = "gpt-4.1-mini"
PRICE = {"input": 0.40, "output": 1.60, "cached": 0.10}  # $/1M токенов, backend/provider_cost_truth.py:953

SYSTEM = """Ты строишь мост немецко-английского учебного словаря.

ЧТО ТЕБЕ ДАЮТ. Список записей. У каждой:
  · "de"    — немецкий текст (слово, устойчивое сочетание или целое предложение);
  · "kind"  — что это: word | collocation | sentence;
  · "sense" — пояснение КОНКРЕТНОГО значения по-русски (у слова значений бывает много);
  · "ru"    — русский перевод именно этого значения, написанный человеком.

ЗАЧЕМ ДАЮТ И НЕМЕЦКОЕ, И РУССКОЕ. Немецкое слово в отрыве многозначно: "Absatz" — это и
абзац, и каблук, и сбыт. Русская сторона и пояснение говорят, о КАКОМ ИЗ ЗНАЧЕНИЙ речь.
Источник — немецкий; русский и пояснение служат адресом значения, а не текстом перевода.
Переводить надо немецкое в этом значении, а НЕ пересказывать русское.

ЧТО ВЕРНУТЬ.
  · kind = word         → английский эквивалент этого значения, в словарной форме.
                          Обычно 1–3 слова. Без артикля "a/the", без "to" у глагола.
  · kind = collocation  → английское устойчивое соответствие. Если у англичан есть своя
                          идиома с тем же смыслом — дай её, а не дословную кальку.
  · kind = sentence     → естественный перевод ВСЕГО предложения на английский.
                          Не по словам. Сохрани регистр речи: разговорное остаётся
                          разговорным, официальное — официальным.

ЖЁСТКИЕ ЗАПРЕТЫ.
  · НЕ ВЫДУМЫВАЙ. Если для этого значения нет устойчивого английского соответствия или
    ты не уверен — верни "en": null и коротко объясни в "why". Пустая ячейка честнее
    выдуманной: по этому словарю человек будет учить язык.
  · Не давай несколько вариантов через запятую. Нужен ОДИН, самый частотный. Если второй
    вариант действительно необходим — положи его в "alt", не в "en".
  · Не добавляй пояснений, помет и скобок внутрь "en". Только сам текст.
  · Не меняй смысл ради красоты. Английское должно значить ровно то, что немецкое.

ФОРМАТ ОТВЕТА — строго JSON, без markdown, без пояснений вокруг:
{"items": [{"key": "<тот же key, что пришёл>", "en": "<текст или null>",
            "alt": "<второй вариант или null>", "why": "<почему null, иначе null>"}]}
Ответь ровно по одной записи на каждый пришедший key, в том же порядке."""


def main():
    items = json.load(open(os.path.join(S, "items100.json"), encoding="utf-8"))
    for it in items:
        it["key"] = f'{it["unit_id"]}:{it.get("sense_id") or 0}'

    client = OpenAI()
    out, usage_total = [], {"in": 0, "cached": 0, "out": 0}
    started = time.time()

    for start in range(0, len(items), 50):
        chunk = items[start:start + 50]
        payload = [{"key": i["key"], "kind": i["kind"], "de": i["de"],
                    "sense": i.get("sense"), "ru": i.get("ru")} for i in chunk]
        rsp = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "system", "content": SYSTEM},
                      {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}],
            response_format={"type": "json_object"},
            temperature=0.2,
        )
        u = rsp.usage
        cached = getattr(getattr(u, "prompt_tokens_details", None), "cached_tokens", 0) or 0
        usage_total["in"] += u.prompt_tokens - cached
        usage_total["cached"] += cached
        usage_total["out"] += u.completion_tokens
        got = json.loads(rsp.choices[0].message.content).get("items", [])
        out.extend(got)
        print(f"  пачка {start//50 + 1}: получено {len(got)}, "
              f"вход {u.prompt_tokens} (из кеша {cached}), выход {u.completion_tokens}")

    by_key = {str(r.get("key")): r for r in out}
    merged = []
    for it in items:
        r = by_key.get(it["key"], {})
        merged.append({**it, "en": r.get("en"), "alt": r.get("alt"), "why": r.get("why")})

    json.dump(merged, open(os.path.join(S, "trial100_result.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)

    cost = (usage_total["in"] * PRICE["input"] + usage_total["cached"] * PRICE["cached"]
            + usage_total["out"] * PRICE["output"]) / 1_000_000
    stats = {"модель": MODEL, "штук": len(merged),
             "ответов_с_текстом": sum(1 for m in merged if m["en"]),
             "честных_не_знаю": sum(1 for m in merged if not m["en"]),
             "токены": usage_total, "цена_за_100_usd": round(cost, 5),
             "секунд": round(time.time() - started, 1)}
    json.dump(stats, open(os.path.join(S, "trial100_stats.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("\n" + json.dumps(stats, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
