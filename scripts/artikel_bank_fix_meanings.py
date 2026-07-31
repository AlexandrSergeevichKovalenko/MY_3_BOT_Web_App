# -*- coding: utf-8 -*-
"""Слово хорошее — перевод кривой. Поправить перевод, тему и, если надо, артикль.

Зачем. В банке нашлись 82 карточки, где немецкое слово ходовое, а перевод в базе —
неверный или редчайшее побочное значение: «die Hebamme — шпатель», «die Flamme —
фламме (цветок)», «die Dose — коробка розеточная», «das Herz — червы (масть)».
Снимать такие слова нельзя, у них надо чинить перевод.

Три вещи, которые нельзя делать наспех:

  1. АРТИКЛЬ. У части слов род зависит от значения: die Kiefer — сосна, der Kiefer —
     челюсть; der See — озеро, die See — море. Поправить перевод и оставить старый
     артикль — значит закрепить ошибку. Поэтому род берём у арбитра
     (article_authority), а не у модели. Арбитр сказал «двуродовое» — ставим артикль
     под выбранное значение и помечаем two_gender, чтобы в игре рядом со словом был
     виден перевод и вопрос остался честным.

  2. ТЕМА. Новый смысл часто не подходит старой теме: «Flamme» с переводом «пламя»
     нечего делать в «Растениях и саде». Тему выбираем заново.

  3. КАРТИНКА И ПОДСКАЗКА. Их подбирали под неверный смысл, значит они врут. Стираем,
     чтобы их пересобрали. Озвучку стираем, только если сменился артикль: файл
     называется по артиклю и слову.

Запуск:
    python -m scripts.artikel_bank_fix_meanings --words scripts/data/artikel_meanings_2026_07_31.tsv
    python -m scripts.artikel_bank_fix_meanings --words <файл> --apply
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor

BATCH = 20
WORKERS = 6
MODEL = "gpt-4.1"

PROMPT = """Ты проверяешь словник тренажёра немецких артиклей. Ниже немецкие слова и
перевод, который стоит у нас в базе. По каждому ответь: перевод в порядке или испорчен.

"ok" — перевод верный и это обиходное значение слова. Ничего не меняем.
   Melone — «дыня», Herz — «сердце», Dose — «банка», Kaffee — «кофе», Reibe — «тёрка».
   Побочное, но честное и уместное теме значение — тоже ok.

"fix" — перевод неверен либо это такая редкость, что человек о ней не подумает.
   Hebamme — «шпатель» (акушерка), Flamme — «фламме (цветок)» (пламя),
   Wagen — «вагончик (жилой)» (машина), Tor — «глупец (устар.)» (ворота).
   Тогда дай правильный перевод.

"drop" — слово вообще не годится тренажёру: имя собственное, страна, город, марка.
   Birma, Bermuda, Pentagon.

Для "fix" верни ещё:
  "ru"    — перевод, который обычный русскоязычный человек назвал бы ПЕРВЫМ. Коротко,
            одно-два слова, без пояснений в скобках, если без них понятно.
  "art"   — артикль (der/die/das) именно для этого значения.
  "theme" — ключ темы из списка ниже, куда слово с этим значением попадёт естественно.

Если род зависит от значения (der See — озеро, die See — море; die Kiefer — сосна,
der Kiefer — челюсть), бери самое обиходное значение и артикль ИМЕННО для него.

НЕ путай похожие слова: die Winde — это лебёдка или вьюнок, а ветер — это der Wind.
Не уверен — ставь "ok": испортить верный перевод хуже, чем оставить редкий.

Темы: {themes}

Отвечай СТРОГИМ JSON:
{{"НемецкоеСлово": {{"verdict": "ok"|"fix"|"drop", "ru": "...", "art": "der|die|das", "theme": "ключ"}}}}
Ключи объекта — ровно как даны слова.

Слова (немецкое — перевод в нашей базе — тема, где лежит):
"""


def read_words(path: str) -> list[str]:
    out = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                out.append(line.split("\t")[0].strip())
    return out


def load_rows(words: list[str]) -> list[dict]:
    from backend.database import get_db_connection_context
    low = [w.lower() for w in words]
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT theme_key, label_ru FROM bt_3_article_sprint_themes WHERE active")
            labels = {k: (v or k) for k, v in cur.fetchall()}
            cur.execute(
                "SELECT id, word, article, meaning_ru, theme_key, COALESCE(two_gender, FALSE) "
                "FROM bt_3_article_sprint_nouns "
                "WHERE lower(word) = ANY(%s) AND NOT retired ORDER BY word", (low,))
            rows = [{"id": r[0], "word": r[1], "article": (r[2] or "").lower(),
                     "ru": r[3] or "", "theme": r[4], "two_gender": r[5]}
                    for r in cur.fetchall()]
    return rows, labels


def ask(chunk: list[dict], labels: dict) -> dict:
    from openai import OpenAI
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"), timeout=180)
    themes = "; ".join(f"{k}={v}" for k, v in sorted(labels.items()))
    body = "\n".join(f"- {r['word']} — «{r['ru']}» — тема {labels.get(r['theme'], r['theme'])}"
                     for r in chunk)
    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model=MODEL, temperature=0, max_tokens=2500,
                response_format={"type": "json_object"},
                messages=[{"role": "user", "content": PROMPT.format(themes=themes) + body}],
            )
            data = json.loads(resp.choices[0].message.content or "{}")
            out = {}
            for r in chunk:
                v = data.get(r["word"])
                if not isinstance(v, dict):
                    continue
                verdict = str(v.get("verdict") or "").strip().lower()
                if verdict not in ("ok", "fix", "drop"):
                    continue
                out[r["word"]] = {
                    "verdict": verdict,
                    "ru": str(v.get("ru") or "").strip(),
                    "art": str(v.get("art") or "").strip().lower(),
                    "theme": str(v.get("theme") or "").strip(),
                }
            return out
        except Exception as exc:
            if attempt == 2:
                logging.warning("пачка переводов не разобрана: %s", exc)
                return {}
    return {}


def ask_twice(rows: list[dict], labels: dict) -> tuple[dict, list[str]]:
    """Два независимых прохода в разном порядке. Расходятся — не трогаем, спрашиваем владельца.

    Один проход уже пытался «починить» верное в неверное («die Winde — вьюнок» → «ветер»).
    Цена ошибки здесь выше цены пропуска: человек учит артикль по переводу, и подменённый
    смысл научит его неправде."""
    passes = []
    for order in (rows, list(reversed(rows))):
        chunks = [order[i:i + BATCH] for i in range(0, len(order), BATCH)]
        got: dict = {}
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            for part in pool.map(lambda c: ask(c, labels), chunks):
                got.update(part)
        passes.append(got)
    agreed, split = {}, []
    for r in rows:
        w = r["word"]
        a, b = passes[0].get(w), passes[1].get(w)
        if not a or not b or a["verdict"] != b["verdict"]:
            split.append(w)
            continue
        if a["verdict"] == "fix" and a["ru"].strip().lower() != b["ru"].strip().lower():
            split.append(w)  # оба видят поломку, но чинят по-разному — решает владелец
            continue
        agreed[w] = a
    return agreed, split


def decide(rows: list[dict], picks: dict, split: list[str], labels: dict) -> list[dict]:
    """Свести предложение модели с арбитром рода. Модель решает СМЫСЛ, арбитр — РОД."""
    from backend.article_authority import authoritative_article
    plan = []
    for r in rows:
        p = picks.get(r["word"])
        if r["word"] in split:
            plan.append({**r, "skip": "проходы разошлись — решает владелец"})
            continue
        if not p:
            plan.append({**r, "skip": "модель не ответила"})
            continue
        if p["verdict"] == "ok":
            plan.append({**r, "skip": "перевод в порядке"})
            continue
        if p["verdict"] == "drop":
            plan.append({**r, "skip": "не годится тренажёру — на снятие, решает владелец"})
            continue
        if not p["ru"]:
            plan.append({**r, "skip": "поломку видит, а чем чинить — не сказал"})
            continue
        art_auth, source = authoritative_article(r["word"], allow_network=True)
        two_gender = r["two_gender"]
        if art_auth:
            # Справочник знает род однозначно — он и прав, что бы ни сказала модель.
            article, why = art_auth, source
        elif "двухродовое" in source or "двуродовое" in source:
            # Род решает смысл. Берём под выбранное значение и показываем перевод в игре.
            article, why, two_gender = (p["art"] or r["article"]), "двуродовое, артикль под смысл", True
        else:
            # Источника нет. Своего артикля не выдумываем — оставляем что стояло.
            article, why = r["article"], "нет данных, артикль оставлен прежним"
        theme = p["theme"] if p["theme"] in labels else r["theme"]
        plan.append({**r, "new_ru": p["ru"], "new_article": article, "new_theme": theme,
                     "new_two_gender": two_gender, "why": why, "skip": ""})
    return plan


def apply(plan: list[dict]) -> dict:
    from backend.database import get_db_connection_context
    stat = {"перевод": 0, "артикль": 0, "тема": 0, "двуродовых": 0, "озвучка стёрта": 0}
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for p in plan:
                if p.get("skip"):
                    continue
                art_changed = p["new_article"] != p["article"]
                cur.execute(
                    "UPDATE bt_3_article_sprint_nouns SET "
                    "  meaning_ru = %s, article = %s, theme_key = %s, two_gender = %s,"
                    # Подсказку и картинку подбирали под неверный смысл — они врут, стираем.
                    "  mnemonic_ru = '', mnemonic_method = '', mnemonic_head = '',"
                    "  image_object_key = '', image_checked = FALSE,"
                    # Файл озвучки называется по артиклю: сменился артикль — файл врёт.
                    "  audio_object_key = CASE WHEN %s THEN '' ELSE audio_object_key END,"
                    "  updated_at = NOW() "
                    "WHERE id = %s;",
                    (p["new_ru"], p["new_article"], p["new_theme"], bool(p["new_two_gender"]),
                     art_changed, p["id"]),
                )
                stat["перевод"] += 1
                stat["артикль"] += 1 if art_changed else 0
                stat["тема"] += 1 if p["new_theme"] != p["theme"] else 0
                stat["двуродовых"] += 1 if p["new_two_gender"] and not p["two_gender"] else 0
                stat["озвучка стёрта"] += 1 if art_changed else 0
        conn.commit()
    return stat


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--words", required=True, help="файл со словами (по одному в строке)")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--report", default="")
    args = ap.parse_args()

    words = read_words(args.words)
    rows, labels = load_rows(words)
    print(f"слов в списке: {len(words)}, карточек в игре: {len(rows)}")
    picks, split = ask_twice(rows, labels)
    print(f"оба прохода согласны по {len(picks)} словам, разошлись по {len(split)}")
    plan = decide(rows, picks, split, labels)

    lines = []
    for p in sorted(plan, key=lambda x: x["word"]):
        if p.get("skip"):
            lines.append(f"{p['word']}\t«{p['ru']}»\t{p['skip']}")
            continue
        art = p["article"] if p["new_article"] == p["article"] else f"{p['article']} → {p['new_article']}"
        th = labels.get(p["theme"], p["theme"])
        if p["new_theme"] != p["theme"]:
            th = f"{th} → {labels.get(p['new_theme'], p['new_theme'])}"
        lines.append(f"{p['word']}\t«{p['ru']}» → «{p['new_ru']}»\t{art}\t{th}\t{p['why']}"
                     + ("\tдвуродовое" if p["new_two_gender"] else ""))
    text = "\n".join(lines)
    if args.report:
        open(args.report, "w", encoding="utf-8").write(text + "\n")
        print("разбор выписан:", args.report)
    else:
        print(text)

    changed_art = sum(1 for p in plan if not p.get("skip") and p["new_article"] != p["article"])
    changed_th = sum(1 for p in plan if not p.get("skip") and p["new_theme"] != p["theme"])
    print(f"итог: перевод {sum(1 for p in plan if not p.get('skip'))}, "
          f"артикль меняем у {changed_art}, тему у {changed_th}")
    if not args.apply:
        print("это был разбор без изменений. Применить: --apply")
        return 0
    for k, v in apply(plan).items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
