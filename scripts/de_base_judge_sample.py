# -*- coding: utf-8 -*-
"""Замер кривизны немецко-русской базы. Судья в ТРИ ГОЛОСА. В БАЗУ НЕ ПИШЕТ НИЧЕГО."""
import json, os, time, collections
from openai import OpenAI

S = os.path.dirname(os.path.abspath(__file__))
MODEL = "gpt-4.1-mini"
PRICE = {"input": 0.40, "output": 1.60, "cached": 0.10}
VOICES, BATCH = 3, 25

SYSTEM = """Ты придирчивый редактор немецко-русского учебного словаря. Тебе дают записи
из живой базы. Твоя работа — сказать, ГОДНА ли запись, и если нет — назвать класс брака
и дать ГОТОВОЕ исправление. Диагноз без исправления не принимается.

СОСТАВ ЗАПИСИ:
  · "de"    — немецкий текст (слово | устойчивое сочетание | предложение);
  · "kind"  — что это;
  · "sense" — пояснение значения по-русски (может отсутствовать);
  · "ru"    — русский перевод, лежащий в базе. Именно его ты и проверяешь.

КЛАССЫ. Выбери РОВНО ОДИН:

"clean"        — всё верно. Перевод передаёт немецкое в этом значении.
                 Неидеальная стилистика — это ВСЁ ЕЩЁ clean. Придирки к оттенкам не в счёт.

"invented"     — у немецкого слова НЕТ такого значения. Значение выдумано.
                 Пример: "ringen" (бороться) с пояснением «о сердце: западать».

"wrong_word"   — перевод относится к ДРУГОМУ слову, часто похожему.
                 Пример: "Ist angezogen" (одет) переведено как «обут».
                 Пример: "die Fähigkeit" (способность) переведено как «талант» (= Talent).

"garbage"      — в поле не перевод, а мусор: две фразы со стрелкой "->", техническое слово,
                 обрывок OCR, html, повтор самого немецкого в русском поле.
                 Это чинится разбором строки, а не переводом.

"imprecise"    — значение то же, но перевод заметно неточен или слишком широк/узок,
                 и ученик выучит неверно. НЕ используй этот класс для мелких придирок.

"unsure"       — ты не можешь решить. Пользуйся честно: лучше "unsure", чем угаданный класс.

ЖЁСТКИЕ ПРАВИЛА.
  · Источник истины — НЕМЕЦКОЕ. Русское ты проверяешь, а не берёшь за образец.
  · РЕГИСТР НЕПРИКОСНОВЕНЕН. Если немецкое грубое, вульгарное или бранное — русское
    ОБЯЗАНО быть таким же. Смягчённый перевод грубого — это "imprecise", а не "clean".
    И сам, предлагая исправление, НЕ СМЯГЧАЙ и не облагораживай.
  · Многозначность — не брак. Если у слова много значений, а здесь разобрано одно —
    это "clean", при условии что разобрано верно.
  · Синоним — не брак. «курировать» вместо «руководить» — "clean".
  · Если класс не "clean" и не "unsure" — ОБЯЗАН дать "fix": исправленный русский перевод.
    Для "garbage" в "fix" положи очищенный текст, если он восстановим, иначе null.

ФОРМАТ — строго JSON, без markdown:
{"items":[{"key":"<пришедший key>","class":"<один из шести>","fix":"<текст или null>",
           "why":"<одна короткая фраза, почему>"}]}
Ровно по одной записи на каждый key, в том же порядке."""


def ask(client, chunk):
    payload = [{"key": i["key"], "kind": i["kind"], "de": i["de"],
                "sense": i.get("sense"), "ru": i.get("ru")} for i in chunk]
    r = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "system", "content": SYSTEM},
                  {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}],
        response_format={"type": "json_object"}, temperature=0.3)
    u = r.usage
    cached = getattr(getattr(u, "prompt_tokens_details", None), "cached_tokens", 0) or 0
    return (json.loads(r.choices[0].message.content).get("items", []),
            {"in": u.prompt_tokens - cached, "cached": cached, "out": u.completion_tokens})


def main():
    items = json.load(open(os.path.join(S, "items400.json"), encoding="utf-8"))
    for it in items:
        it["key"] = f'{it["unit_id"]}:{it.get("sense_id") or 0}'
    client = OpenAI()
    votes = collections.defaultdict(list)
    fixes = collections.defaultdict(list)
    usage = {"in": 0, "cached": 0, "out": 0}
    t0 = time.time()

    for v in range(VOICES):
        for start in range(0, len(items), BATCH):
            got, u = ask(client, items[start:start + BATCH])
            for k in usage:
                usage[k] += u[k]
            for r in got:
                key = str(r.get("key"))
                votes[key].append(str(r.get("class") or "unsure"))
                if r.get("fix"):
                    fixes[key].append({"fix": r["fix"], "why": r.get("why"), "class": r.get("class")})
        print(f"  голос {v + 1} из {VOICES} пройден, {round(time.time() - t0)} c")

    out, agree = [], collections.Counter()
    for it in items:
        k = it["key"]
        c = collections.Counter(votes.get(k, []))
        top, n = (c.most_common(1)[0] if c else ("unsure", 0))
        verdict = top if n >= 2 else "unsure"          # большинство из трёх, иначе не уверены
        agree[f"{n} из {len(votes.get(k, []))}"] += 1
        out.append({**it, "verdict": verdict, "votes": votes.get(k, []),
                    "fix": (fixes[k][0]["fix"] if fixes.get(k) else None),
                    "why": (fixes[k][0]["why"] if fixes.get(k) else None)})

    json.dump(out, open(os.path.join(S, "judge400_result.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    cost = (usage["in"] * PRICE["input"] + usage["cached"] * PRICE["cached"]
            + usage["out"] * PRICE["output"]) / 1_000_000
    by_class = collections.Counter(o["verdict"] for o in out)
    by_kind = collections.defaultdict(collections.Counter)
    for o in out:
        by_kind[o["kind"]][o["verdict"]] += 1
    stats = {"модель": MODEL, "голосов": VOICES, "записей": len(out),
             "вердикты": dict(by_class),
             "по видам": {k: dict(v) for k, v in by_kind.items()},
             "согласие голосов": dict(agree),
             "токены": usage, "цена_usd": round(cost, 4), "секунд": round(time.time() - t0)}
    json.dump(stats, open(os.path.join(S, "judge400_stats.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("\n" + json.dumps(stats, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
