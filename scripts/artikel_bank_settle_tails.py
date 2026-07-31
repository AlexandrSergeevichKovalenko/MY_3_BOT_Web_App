# -*- coding: utf-8 -*-
"""Разобрать хвосты: перекошенные темы, кривые переводы, имена собственные.

Правило, которое здесь закреплено: у автоматики нет права оставить решение «владельцу»
и на этом остановиться. Либо она чинит сама, либо кладёт карточку в КАРАНТИН, откуда
слово приходит владельцу в дневной разбор с кнопками «вернуть / мусор». Молча повиснуть
в базе сомнительная карточка не может.

Три беды, которые он разбирает:

  1. Тема по близнецу, а не по смыслу. Двуродовые пары возвращались в тему той половины,
     что осталась в игре, поэтому die Kiefer (сосна) лежит в «Теле и здоровье», а
     die Steuer (налог) — в «Транспорте».
  2. Перевод, который два прохода судьи не смогли согласовать: die Ballerina — «балетка»
     (это балерина), der Jogger — «джоггеры» (это бегун), der Strom — «поток, большая
     река» (это электричество).
  3. Имена собственные: Bermuda, Birma. Тренажёру артиклей они не нужны, но снимать их
     молча нельзя — решает владелец.

Запуск:
    python -m scripts.artikel_bank_settle_tails
    python -m scripts.artikel_bank_settle_tails --apply
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

PROMPT = """Ты приводишь в порядок карточки тренажёра немецких артиклей. Карточка — это
артикль + слово + русский перевод + тема, в которой она лежит. Ученик — обычный взрослый
русскоязычный человек, учит немецкий для жизни.

По каждой карточке поставь ровно один вердикт.

"ok"    — перевод верный и тема подходит. Ничего не меняем.
"theme" — перевод верный, а тема не та. Дай правильный ключ темы.
          die Kiefer «сосна» лежит в «Тело и здоровье» → pflanzen_garten.
          die Steuer «налог» лежит в «Транспорт» → geld_bank.
"fix"   — перевод неверный. Дай правильный, и тему под него.
          die Ballerina «балетка» → «балерина»; der Jogger «джоггеры» → «бегун».
"drop"  — карточке не место в тренажёре: имя собственное (Bermuda, Birma), устаревшее
          книжное слово (der Tor «глупец»), или перевод дублирует соседнюю карточку того
          же слова.

Артикль НЕ меняй и не комментируй — его проверяет справочник, не ты.

Отвечай СТРОГИМ JSON, ключ — «артикль слово» ровно как дано:
{{"der See": {{"verdict": "ok"}}, "die Kiefer": {{"verdict": "theme", "theme": "pflanzen_garten"}},
 "die Ballerina": {{"verdict": "fix", "ru": "балерина", "theme": "kunst_kultur"}}}}

Темы: {themes}

Карточки:
"""


def load_cards() -> tuple[list[dict], dict]:
    """Карточки, которые нужно пересмотреть: двуродовые пары + спорные переводы."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT theme_key, label_ru FROM bt_3_article_sprint_themes WHERE active")
            labels = {k: (v or k) for k, v in cur.fetchall()}
            cur.execute("""
                SELECT id, word, article, meaning_ru, theme_key
                FROM bt_3_article_sprint_nouns a
                WHERE NOT retired AND (
                    -- обе половины двуродовой пары: тему им ставили по близнецу
                    (two_gender AND EXISTS (SELECT 1 FROM bt_3_article_sprint_nouns b
                                            WHERE lower(b.word) = lower(a.word)
                                              AND NOT b.retired AND b.id <> a.id))
                    -- слова из разобранного списка, где проходы судьи разошлись
                    OR lower(word) = ANY(%s)
                )
                ORDER BY lower(word), article;
            """, (DISPUTED,))
            cards = [{"id": r[0], "word": r[1], "article": (r[2] or "").lower(),
                      "ru": r[3] or "", "theme": r[4]} for r in cur.fetchall()]
    return cards, labels


# Слова, по которым два прохода судьи переводов не сошлись 31.07, плюс имена собственные.
# Держим списком, а не «на глаз»: иначе через месяц не вспомнить, что именно разбиралось.
DISPUTED = [
    "bagger", "ballerina", "bermuda", "birma", "begehr", "diamant", "früh", "föhn",
    "jogger", "kombination", "punkt", "riemen", "rolle", "scheibe", "schirm",
    "schneeball", "senf", "silicon", "strom", "umzug",
]


def ask(chunk: list[dict], labels: dict) -> dict:
    from openai import OpenAI
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"), timeout=180)
    themes = "; ".join(f"{k}={v}" for k, v in sorted(labels.items()))
    body = "\n".join(
        f"- {c['article']} {c['word']} — «{c['ru']}» — тема {labels.get(c['theme'], c['theme'])}"
        for c in chunk)
    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model=MODEL, temperature=0, max_tokens=2500,
                response_format={"type": "json_object"},
                messages=[{"role": "user", "content": PROMPT.format(themes=themes) + body}],
            )
            data = json.loads(resp.choices[0].message.content or "{}")
            out = {}
            for c in chunk:
                v = data.get(f"{c['article']} {c['word']}")
                if isinstance(v, dict) and str(v.get("verdict") or "") in ("ok", "theme", "fix", "drop"):
                    out[c["id"]] = {"verdict": str(v["verdict"]),
                                    "ru": str(v.get("ru") or "").strip(),
                                    "theme": str(v.get("theme") or "").strip()}
            return out
        except Exception as exc:
            if attempt == 2:
                logging.warning("пачка хвостов не разобрана: %s", exc)
                return {}
    return {}


def vote(cards: list[dict], labels: dict) -> tuple[dict, list[int]]:
    """Два прохода в разном порядке. Владельцу идёт только сомнение в САМОМ СЛОВЕ.

    Тема и содержание карточки — разные вопросы, и путать их дорого. Проходы почти всегда
    спорят о теме: «сервиз» — это Еда или Кухня, «щит» — Город или История. Дёргать
    владельца из-за такого спора нельзя, иначе разбор превращается в свалку, и его
    перестанут читать. Спор о теме = оставляем тему как есть, карточка в игре.

    Владельцу карточка уходит, только если проходы не сошлись в том, ГОДИТСЯ ли она:
    один говорит «перевод верный», другой «перевод неверный» или «выбросить». Вот это
    настоящее сомнение, и решать его должен человек."""
    passes = []
    for order in (cards, list(reversed(cards))):
        chunks = [order[i:i + BATCH] for i in range(0, len(order), BATCH)]
        got: dict = {}
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            for part in pool.map(lambda c: ask(c, labels), chunks):
                got.update(part)
        passes.append(got)

    def content_ok(v) -> bool:
        return str(v["verdict"]) in ("ok", "theme")

    agreed, split = {}, []
    for c in cards:
        a, b = passes[0].get(c["id"]), passes[1].get(c["id"])
        if not a or not b or content_ok(a) != content_ok(b):
            split.append(c["id"])
            continue
        if content_ok(a):
            # Слово в порядке. Тему меняем только если оба прохода назвали ОДНУ и ту же.
            same = (a["verdict"] == "theme" and b["verdict"] == "theme"
                    and a["theme"] == b["theme"] and a["theme"] in labels)
            agreed[c["id"]] = {"verdict": "theme" if same else "ok",
                               "ru": "", "theme": a["theme"] if same else c["theme"]}
        elif a["verdict"] != b["verdict"] or (
                a["verdict"] == "fix" and a["ru"].strip().lower() != b["ru"].strip().lower()):
            split.append(c["id"])  # оба видят поломку, но чинят по-разному
        else:
            agreed[c["id"]] = a
    return agreed, split


def apply(cards, agreed, split, labels) -> dict:
    """Чиним что решено, остальное — в карантин, откуда оно придёт владельцу в разбор."""
    from backend.database import get_db_connection_context, QUARANTINE_SOURCE
    stat = {"тема поправлена": 0, "перевод поправлен": 0, "ушло в разбор": 0}
    by_id = {c["id"]: c for c in cards}
    stat["не переехало"] = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for cid, v in agreed.items():
                card = by_id[cid]
                if v["verdict"] in ("ok", "drop"):
                    continue  # drop уйдёт ниже вместе со спорными
                theme = v["theme"] if v["theme"] in labels else card["theme"]
                # Каждая карточка — своей точкой отката. Иначе одна коллизия ключа
                # (тема+слово+артикль) роняет транзакцию, и не применяется НИЧЕГО:
                # именно так первый прогон молча не сделал ни одной правки.
                cur.execute("SAVEPOINT card;")
                try:
                    if theme != card["theme"]:
                        # В целевой теме может лежать снятая копия этого же слова с тем же
                        # артиклем — её оставила дедупликация. От переезжающей карточки она
                        # не отличается ничем, а ключ занимает. Убираем.
                        cur.execute(
                            "DELETE FROM bt_3_article_sprint_nouns "
                            "WHERE theme_key = %s AND lower(word) = lower(%s) AND article = %s "
                            "  AND retired AND id <> %s;",
                            (theme, card["word"], card["article"], cid))
                    if v["verdict"] == "theme":
                        if theme == card["theme"]:
                            cur.execute("RELEASE SAVEPOINT card;")
                            continue
                        cur.execute("UPDATE bt_3_article_sprint_nouns "
                                    "SET theme_key = %s, updated_at = NOW() WHERE id = %s;",
                                    (theme, cid))
                        stat["тема поправлена"] += 1
                    else:  # fix
                        # Картинку и подсказку подбирали под неверный смысл — они врут.
                        cur.execute(
                            "UPDATE bt_3_article_sprint_nouns SET meaning_ru = %s, theme_key = %s, "
                            "  mnemonic_ru = '', mnemonic_method = '', mnemonic_head = '', "
                            "  image_object_key = '', image_checked = FALSE, updated_at = NOW() "
                            "WHERE id = %s;", (v["ru"], theme, cid))
                        stat["перевод поправлен"] += 1
                    cur.execute("RELEASE SAVEPOINT card;")
                except Exception as exc:
                    cur.execute("ROLLBACK TO SAVEPOINT card;")
                    stat["не переехало"] += 1
                    logging.warning("карточка %s %s осталась на месте: %s",
                                    card["article"], card["word"], exc)

            # Всё, по чему решения нет, и всё, что предложено выбросить, — в карантин.
            # Не в стоп-лист: там решение окончательное, а тут его ещё не приняли.
            to_owner = list(split) + [cid for cid, v in agreed.items() if v["verdict"] == "drop"]
            if to_owner:
                cur.execute(
                    "UPDATE bt_3_article_sprint_nouns "
                    "SET retired = TRUE, retire_reviewed = FALSE, source = %s, updated_at = NOW() "
                    "WHERE id = ANY(%s);", (QUARANTINE_SOURCE, to_owner))
                stat["ушло в разбор"] = int(cur.rowcount or 0)
        conn.commit()
    return stat


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    cards, labels = load_cards()
    print(f"карточек на пересмотр: {len(cards)}")
    agreed, split = vote(cards, labels)
    by_id = {c["id"]: c for c in cards}
    for cid, v in sorted(agreed.items(), key=lambda kv: by_id[kv[0]]["word"]):
        c = by_id[cid]
        if v["verdict"] == "ok":
            continue
        if v["verdict"] == "drop":
            print(f"  в разбор   {c['article']} {c['word']:12} «{c['ru'][:32]}»")
        elif v["verdict"] == "theme":
            print(f"  тема       {c['article']} {c['word']:12} «{c['ru'][:26]}» "
                  f"{labels.get(c['theme'], c['theme'])} → {labels.get(v['theme'], v['theme'])}")
        else:
            print(f"  перевод    {c['article']} {c['word']:12} «{c['ru'][:26]}» → «{v['ru']}» "
                  f"({labels.get(v['theme'], v['theme'])})")
    for cid in split:
        c = by_id[cid]
        print(f"  в разбор   {c['article']} {c['word']:12} «{c['ru'][:32]}» (проходы разошлись)")
    if not args.apply:
        print("это был разбор без изменений. Применить: --apply")
        return 0
    for k, v in apply(cards, agreed, split, labels).items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
