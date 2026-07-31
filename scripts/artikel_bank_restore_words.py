# -*- coding: utf-8 -*-
"""Вернуть слова в игру — с проверкой артикля, а не на веру.

Владелец пересмотрел решение по узким терминам: их немного, они честные, и снимать их
незачем. Но вернуть слово молча нельзя: снимали мы его как сомнительное, и если у него
кривой артикль, возврат закрепит ошибку. Поэтому род каждого слова сверяем с арбитром
(article_authority: Wiktionary, затем правило композита).

Что делаем с расхождением: артикль ставим справочный, а строку помечаем, чтобы владелец
увидел, что именно поправилось. Справочник молчит — слово возвращаем с тем артиклем,
что стоял, и выписываем в отчёт: своего рода мы не выдумываем.

Слово возвращаем ОДНОЙ карточкой — той, что была в игре до снятия (у неё самое свежее
updated_at). Остальные копии сняты дедупликацией и должны остаться снятыми.

Запуск:
    python -m scripts.artikel_bank_restore_words <файл> --group "узкий термин"
    python -m scripts.artikel_bank_restore_words <файл> --group "узкий термин" --apply
"""
from __future__ import annotations

import argparse
import sys


def read_words(path: str, group: str) -> list[str]:
    """Файл: слово<TAB>причина<TAB>перевод. group — подстрока причины (пусто = все)."""
    out = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            parts = line.split("\t")
            reason = parts[1] if len(parts) > 1 else ""
            if group and group.lower() not in reason.lower():
                continue
            if parts[0].strip():
                out.append(parts[0].strip())
    return out


def pick_rows(words: list[str]) -> list[dict]:
    """По одной карточке на слово — самая полная: проверенная, с медиа, самая старая.

    Слова, у которых карточка уже в игре, пропускаем: возвращать нечего, а второй
    карточкой мы бы завели дубль. Медиа важнее возраста — у поздней копии картинка и
    озвучка обычно пустые."""
    from backend.database import get_db_connection_context
    low = [w.lower() for w in words]
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT ON (lower(word)) id, word, article, meaning_ru, theme_key "
                "FROM bt_3_article_sprint_nouns n "
                "WHERE lower(word) = ANY(%s) AND retired "
                "  AND NOT EXISTS (SELECT 1 FROM bt_3_article_sprint_nouns a "
                "                  WHERE lower(a.word) = lower(n.word) AND NOT a.retired) "
                "ORDER BY lower(word), verified DESC, "
                "         (CASE WHEN COALESCE(audio_object_key,'') <> '' THEN 1 ELSE 0 END"
                "        + CASE WHEN COALESCE(image_object_key,'') <> '' THEN 1 ELSE 0 END) DESC,"
                "         created_at, id;", (low,))
            return [{"id": r[0], "word": r[1], "article": (r[2] or "").lower(),
                     "ru": r[3] or "", "theme": r[4]} for r in cur.fetchall()]


def check_articles(rows: list[dict]) -> list[dict]:
    from backend.article_authority import authoritative_article
    for r in rows:
        art, source = authoritative_article(r["word"], allow_network=True)
        r["two_gender"] = False
        if art and art != r["article"]:
            r["new_article"], r["note"] = art, f"артикль поправлен ({source})"
        elif art:
            r["new_article"], r["note"] = art, ""
        elif "родовое" in source:
            # Род решает смысл (der Moment — миг, das Moment — фактор). Артикль тут не
            # свойство написания, поэтому помечаем слово двуродовым: в игре рядом с ним
            # покажется перевод, и вопрос станет честным.
            r["new_article"], r["two_gender"] = r["article"], True
            r["note"] = "двуродовое — в игре покажем перевод"
        else:
            r["new_article"], r["note"] = r["article"], f"справочник молчит: {source}"
    return rows


def apply(rows: list[dict], words: list[str]) -> dict:
    from backend.database import get_db_connection_context, unblacklist_article_words
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    "UPDATE bt_3_article_sprint_nouns SET retired = FALSE, retire_reviewed = TRUE, "
                    "  article = %s, two_gender = two_gender OR %s, updated_at = NOW() "
                    "WHERE id = %s;", (r["new_article"], bool(r.get("two_gender")), r["id"]))
        conn.commit()
    # Без снятия стоп-листа ночное наполнение считало бы слово выброшенным и не дало бы
    # ему вернуться, даже когда карточка снова в игре.
    freed = unblacklist_article_words(words)
    return {"вернули карточек": len(rows), "убрали из стоп-листа": freed,
            "поправили артикль": sum(1 for r in rows if r["new_article"] != r["article"])}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("file")
    ap.add_argument("--group", default="", help="какую группу вернуть (подстрока причины)")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    words = read_words(args.file, args.group)
    print(f"слов в группе: {len(words)}")
    rows = check_articles(pick_rows(words))
    print(f"нашлось снятых карточек: {len(rows)}")
    missing = sorted(set(w.lower() for w in words) - {r["word"].lower() for r in rows})
    if missing:
        print(f"не нашлись (уже в игре?): {missing}")
    for r in sorted(rows, key=lambda x: x["word"]):
        if r["note"]:
            print(f"  {r['word']:24} {r['article']} → {r['new_article']}  {r['note']}")
    if not args.apply:
        print("это был разбор без изменений. Применить: --apply")
        return 0
    for k, v in apply(rows, words).items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
