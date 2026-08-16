# -*- coding: utf-8 -*-
"""Разобрать накопленный карантин артиклей тем же правилом, что стоит на приёмке.

Зачем. В карантине скопилось 454 слова: страж приёмки спрашивал модель ОДИН раз, и
ответ был неустойчив. Замер 16.08.2026 — один и тот же вопрос про одни и те же слова,
заданный дважды подряд (gpt-4.1, температура 0), дал разные ответы по 12% слов; из 150
отвергнутых 36% при повторе получали «да». Владелец разбирал этот шум руками по 10 слов
в день — на 533 вопроса ушло бы четыре месяца.

Что делает скрипт. Прогоняет каждое карантинное слово через то же большинство голосов,
что теперь стоит на приёмке (`judge_everyday_words`), и:
  • взяло большинство «да» → слово возвращается в игру САМО, владельца не спрашиваем;
  • иначе                  → остаётся в очереди, приедет в личку как раньше.

Возврат идёт штатной дверью `restore_retired_article_noun`: она сверяет артикль со
справочником и поднимает самую полную карточку слова. Двуродовые слова (der/die Flur)
она возвращать отказывается — там артикль решает смысл, и его ставит владелец тапом.
Такие остаются в очереди, даже если голоса за них.

Правило отбора кандидатов взято из продукта (`_retired_review_rows`), не переписано.

Запуск:
    python -m scripts.artikel_quarantine_resolve              # отчёт, база не меняется
    python -m scripts.artikel_quarantine_resolve --apply
    python -m scripts.artikel_quarantine_resolve --limit 50 --apply
"""
from __future__ import annotations

import argparse
import sys

CHUNK = 50   # столько слов в одном вопросе к модели — как на приёмке


def load_candidates(limit: int | None) -> list[dict]:
    from backend.database import _retired_review_rows
    from backend.article_retire_review import MAX_RANK
    rows = [r for r in _retired_review_rows(MAX_RANK) if r.get("quarantined")]
    return rows[:limit] if limit else rows


def vote(words: list[str]) -> dict[str, bool]:
    """Тот же судья и то же большинство, что на приёмке."""
    from backend.article_word_gate import judge_everyday_words, EverydayJudgeUnavailable
    out: dict[str, bool] = {}
    for i in range(0, len(words), CHUNK):
        chunk = words[i:i + CHUNK]
        try:
            out.update(judge_everyday_words(chunk))
        except EverydayJudgeUnavailable as exc:
            # Молчание модели — не «нет». Слово остаётся в очереди и приедет владельцу.
            print(f"  ⚠️  голосов не было для {len(chunk)} слов ({exc}) — оставляю владельцу")
            for w in chunk:
                out[w] = False
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="без него — только отчёт")
    ap.add_argument("--limit", type=int, default=0, help="взять только первые N слов")
    args = ap.parse_args()

    from backend.database import restore_retired_article_noun

    rows = load_candidates(args.limit or None)
    if not rows:
        print("Карантин пуст — разбирать нечего.")
        return 0
    print(f"карантинных слов на разбор: {len(rows)}")

    verdicts = vote([r["word"] for r in rows])
    yes = [r for r in rows if verdicts.get(r["word"])]
    no = [r for r in rows if not verdicts.get(r["word"])]
    print(f"большинство ЗА возврат: {len(yes)}   ·   остаются владельцу: {len(no)}\n")

    if not args.apply:
        print("СУХОЙ ПРОГОН — база не менялась. Вернулись бы в игру:")
        for r in yes[:40]:
            print(f"  #{r['rank']:>6}  {r['article']:>4} {r['word']:<26} — {r['meaning_ru'][:34]}")
        if len(yes) > 40:
            print(f"  … и ещё {len(yes) - 40}")
        print("\nПовтори с --apply, чтобы применить.")
        return 0

    restored = sense = failed = 0
    for r in yes:
        res = restore_retired_article_noun(int(r["id"]))
        if not res:
            failed += 1
            print(f"  ✗ {r['word']} — не удалось вернуть")
        elif res.get("needs_sense"):
            # Двуродовое: артикль решает смысл. Голоса тут ничего не решают — оставляем
            # владельцу, он поставит артикль тапом и увидит перевод.
            sense += 1
        elif res.get("blocked"):
            sense += 1
        else:
            restored += 1
            mark = "" if str(res.get("stored_article") or "").lower() == str(res["article"]).lower() \
                else f"  (артикль поправлен: было «{res.get('stored_article')}»)"
            print(f"  ✅ {res['article']} {res['word']}{mark}")

    print(f"\nвернул в игру: {restored}")
    print(f"оставил владельцу (двуродовые / без перевода): {sense}")
    print(f"оставил владельцу (голоса против): {len(no)}")
    if failed:
        print(f"сбоев: {failed}")

    from backend.database import count_retired_review_candidates
    from backend.article_retire_review import MAX_RANK
    print(f"\nв очереди разбора осталось: {count_retired_review_candidates(max_rank=MAX_RANK)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
