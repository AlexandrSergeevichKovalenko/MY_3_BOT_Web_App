# -*- coding: utf-8 -*-
"""Одно слово — одна тема. Убрать копии слова из чужих тем.

Зачем. Проверка «уже есть в теме» при наполнении смотрела только СВОЮ тему, поэтому
слово, давно живущее в «Одежде», спокойно ложилось ещё и в «Уборку». На 31.07 таких
слов 1 309, лишних карточек 2 133: одно и то же спрашивают по нескольку раз и часто
не в своей теме (Sweatshirt в подтеме «стирка и бельё»).

Что делает. Для каждого слова-дубля выбирает ОДНУ тему (спрашивает модель, что уместнее),
оставляет в ней лучшую карточку, остальные снимает с показа.

Дубль считается по написанию И артиклю. Одинаково пишущиеся слова с разным родом —
не копии, а РАЗНЫЕ слова: der See (озеро) и die See (море), der Kiefer (челюсть) и
die Kiefer (сосна), der Leiter (руководитель) и die Leiter (стремянка). Ради них в игре
и сделан показ перевода: артикль там решает смысл. Первая версия скрипта ключевалась по
одному написанию и выбросила 28 таких карточек — половину каждой пары.

Снятые копии помечаются retire_reviewed = TRUE и в стоп-лист НЕ идут: слово из игры не
уходит, оно просто живёт в одной теме. Без этой пометки владельца завалило бы разбором
двух тысяч «снятых» слов, которые на самом деле никуда не делись.

Запуск:
    python -m scripts.artikel_bank_dedupe            # разбор без изменений
    python -m scripts.artikel_bank_dedupe --apply    # применить
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

BATCH = 25
WORKERS = 8
MODEL = "gpt-4.1"

PROMPT = """Ниже немецкие существительные из тренажёра артиклей. Каждое сейчас по ошибке
стоит сразу в нескольких темах — его надо оставить ровно в одной.

Для каждого слова выбери ОДНУ тему из предложенных ему вариантов: ту, где обычный человек
искал бы это слово в первую очередь. Смотри на перевод.

Отвечай СТРОГИМ JSON: {"НемецкоеСлово": "ключ_темы", ...}. Ключ темы бери ровно из списка
вариантов этого слова, не выдумывай новых. Ключи объекта — ровно как даны слова.

Слова:
"""


def _log(msg: str) -> None:
    print(msg, flush=True)


def load_duplicates():
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT theme_key, label_ru FROM bt_3_article_sprint_themes")
            labels = {k: (v or k) for k, v in cur.fetchall()}
            cur.execute(
                "SELECT id, theme_key, word, meaning_ru, article, verified, "
                "       COALESCE(audio_object_key,''), COALESCE(image_object_key,''), created_at "
                "FROM bt_3_article_sprint_nouns WHERE NOT retired ORDER BY id"
            )
            rows = cur.fetchall()
    by_word: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        by_word[(str(r[2]).strip().lower(), str(r[4] or "").strip().lower())].append({
            "id": int(r[0]), "theme": r[1], "word": str(r[2]).strip(), "ru": r[3] or "",
            "article": r[4] or "", "verified": bool(r[5]),
            "media": (1 if r[6] else 0) + (1 if r[7] else 0), "created": r[8],
        })
    dups = {w: v for w, v in by_word.items() if len(v) > 1}
    return dups, labels


def ask_themes(chunk, labels):
    """chunk: [(слово, перевод, [ключи тем])] → {слово: ключ темы}"""
    from openai import OpenAI
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"), timeout=180)
    lines = []
    for word, ru, keys in chunk:
        opts = "; ".join(f"{k} = {labels.get(k, k)}" for k in keys)
        lines.append(f"- {word} — {ru}\n  варианты: {opts}")
    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model=MODEL, temperature=0, max_tokens=2000,
                response_format={"type": "json_object"},
                messages=[{"role": "user", "content": PROMPT + "\n".join(lines)}],
            )
            data = json.loads(resp.choices[0].message.content or "{}")
            out = {}
            for word, _ru, keys in chunk:
                pick = str(data.get(word, "")).strip()
                if pick in keys:
                    out[word] = pick
            return out
        except Exception as exc:
            if attempt == 2:
                logging.warning("пачка тем не разобрана: %s", exc)
                return {}
    return {}


def best_row(rows: list[dict]) -> dict:
    """Из карточек одной темы оставляем самую «полную»: проверенную, с медиа, старшую.

    Медиа важнее возраста: у поздней копии обычно пустые картинка и озвучка, и выбрав
    её мы бы выбросили уже оплаченную работу."""
    return sorted(rows, key=lambda r: (not r["verified"], -r["media"], r["created"]))[0]


def plan(dups, labels):
    """→ (что оставляем, что снимаем, чем выбрана тема)"""
    todo = []
    # Спрашиваем про «der See», а не про «See»: у двуродового слова каждый род — своё
    # слово со своим смыслом, и тема у них может быть разная.
    for key, rows in sorted(dups.items()):
        themes = sorted({r["theme"] for r in rows})
        if len(themes) > 1:
            todo.append((f"{rows[0]['article']} {rows[0]['word']}".strip(),
                         str(rows[0]["ru"])[:60], themes))
    picks: dict[str, str] = {}
    if todo:
        chunks = [todo[i:i + BATCH] for i in range(0, len(todo), BATCH)]
        _log(f"спрашиваю тему для {len(todo)} слов ({len(chunks)} пачек)…")
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            for part in pool.map(lambda c: ask_themes(c, labels), chunks):
                picks.update(part)
        _log(f"модель ответила по {len(picks)} словам")

    keep, drop, how = {}, [], {}
    for word, rows in sorted(dups.items()):
        keys = sorted({r["theme"] for r in rows})
        chosen = picks.get(f"{rows[0]['article']} {rows[0]['word']}".strip())
        if chosen in keys:
            how[word] = "модель"
        else:
            # Ответа нет — берём тему самой старой карточки: до июльской доливки слово
            # стояло там, где его завели изначально, и это почти всегда его место.
            chosen = sorted(rows, key=lambda r: r["created"])[0]["theme"]
            how[word] = "по старшинству"
        winner = best_row([r for r in rows if r["theme"] == chosen])
        keep[word] = winner
        drop.extend([r for r in rows if r["id"] != winner["id"]])
    return keep, drop, how


def apply_drop(drop: list[dict]) -> int:
    from backend.database import get_db_connection_context
    ids = [r["id"] for r in drop]
    if not ids:
        return 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bt_3_article_sprint_nouns "
                "SET retired = TRUE, retire_reviewed = TRUE, updated_at = NOW() "
                "WHERE id = ANY(%s) AND NOT retired;",
                (ids,),
            )
            n = int(cur.rowcount or 0)
        conn.commit()
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="применить, а не показать")
    ap.add_argument("--report", default="", help="куда выписать полный разбор")
    args = ap.parse_args()

    dups, labels = load_duplicates()
    _log(f"слов, стоящих в 2+ карточках: {len(dups)}")
    keep, drop, how = plan(dups, labels)
    _log(f"оставляем по одной карточке: {len(keep)}")
    _log(f"снимаем лишних карточек: {len(drop)}")
    moved = sum(1 for w in keep if how[w] == "модель")
    _log(f"тему выбрала модель: {moved}, по старшинству: {len(keep) - moved}")

    if args.report:
        with open(args.report, "w", encoding="utf-8") as fh:
            for word in sorted(keep):
                k = keep[word]
                others = sorted({r["theme"] for r in drop
                                 if (r["word"].lower(), r["article"].lower()) == word})
                fh.write(f"{k['article']} {k['word']}\t{k['ru']}\tостаётся: {labels.get(k['theme'], k['theme'])}"
                         f"\tуходит из: {', '.join(labels.get(t, t) for t in others)}\t{how[word]}\n")
        _log(f"разбор выписан: {args.report}")

    if not args.apply:
        _log("это был разбор без изменений. Применить: --apply")
        return 0
    n = apply_drop(drop)
    _log(f"снято карточек: {n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
