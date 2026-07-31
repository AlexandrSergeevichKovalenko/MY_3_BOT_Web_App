# -*- coding: utf-8 -*-
"""Снять слова с показа во ВСЕХ местах, где они успели осесть.

Снять слово в банке мало: своя копия карточки лежит ещё в очереди работы над ошибками,
в банке личных квизов и в уже собранных наборах дня. Уберёшь только банк — человек всё
равно увидит слово, просто из другого места.

Поэтому здесь один проход по всем четырём хранилищам:
  1. банк тем        — retired, разобранным (в личку на проверку не поедет);
  2. стоп-лист       — чтобы ночное авто-наполнение не завело слово заново;
  3. очередь ошибок  — карточки удаляются, если слова не осталось ни в одной теме;
  4. банк квизов и уже собранные наборы дня — вычищаются тоже.

Запуск:
    python -m scripts.artikel_bank_retire_words scripts/data/artikel_retire_2026_07_31.tsv
    python -m scripts.artikel_bank_retire_words <файл> --apply
"""
from __future__ import annotations

import argparse
import sys


def read_words(path: str) -> list[tuple[str, str]]:
    """Файл: слово<TAB>причина<TAB>перевод. Строки с # — комментарии."""
    out: list[tuple[str, str]] = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            parts = line.split("\t")
            word = parts[0].strip()
            reason = parts[1].strip() if len(parts) > 1 else ""
            if word:
                out.append((word, reason))
    return out


def count_before(low: list[str]) -> dict:
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM bt_3_article_sprint_nouns "
                        "WHERE lower(word) = ANY(%s) AND NOT retired", (low,))
            bank = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM bt_3_article_quiz_bank "
                        "WHERE lower(word) = ANY(%s) AND NOT COALESCE(retired, FALSE)", (low,))
            quiz = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM bt_3_aufgabe_mistakes "
                        "WHERE format = 'artikel' AND lower(payload->>'wort') = ANY(%s)", (low,))
            mistakes = cur.fetchone()[0]
            # Ровно та же граница, что и при правке ниже: вчерашние наборы уже сыграны,
            # это история, и показывать их в разборе значит обещать работу, которой не будет.
            cur.execute("""SELECT count(*) FROM bt_3_article_sprint_sets s
                           WHERE s.play_date >= CURRENT_DATE
                             AND EXISTS (SELECT 1 FROM jsonb_array_elements(s.words_json) w
                                         WHERE lower(w->>'w') = ANY(%s))""", (low,))
            sets = cur.fetchone()[0]
    return {"банк тем": bank, "банк квизов": quiz, "очередь ошибок": mistakes,
            "наборы дня начиная с сегодня": sets}


def apply(items: list[tuple[str, str]]) -> dict:
    from backend.database import get_db_connection_context, blacklist_article_words
    low = [w.lower() for w, _ in items]
    done: dict[str, int] = {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            # 1. Банк тем. retire_reviewed = TRUE: решение принято владельцем, второй раз
            #    спрашивать его в личке об этих же словах незачем.
            cur.execute(
                "UPDATE bt_3_article_sprint_nouns "
                "SET retired = TRUE, retire_reviewed = TRUE, updated_at = NOW() "
                "WHERE lower(word) = ANY(%s) AND NOT retired;", (low,))
            done["снято в банке тем"] = int(cur.rowcount or 0)

            # 3. Банк личных квизов — своя таблица со своей копией карточки.
            cur.execute(
                "UPDATE bt_3_article_quiz_bank SET retired = TRUE, updated_at = NOW() "
                "WHERE lower(word) = ANY(%s) AND NOT COALESCE(retired, FALSE);", (low,))
            done["снято в банке квизов"] = int(cur.rowcount or 0)

            # 4. Уже собранные наборы дня. Набор хранит СВОЙ список слов, поэтому чистка
            #    банка до него не доходит: человек доиграл бы сегодняшний набор со снятым
            #    словом. Трогаем только сегодняшние и будущие — прошлые это история.
            cur.execute(
                """UPDATE bt_3_article_sprint_sets s
                      SET words_json = COALESCE((
                            SELECT jsonb_agg(w) FROM jsonb_array_elements(s.words_json) w
                             WHERE lower(w->>'w') <> ALL(%s)
                          ), '[]'::jsonb)
                    WHERE s.play_date >= CURRENT_DATE
                      AND EXISTS (SELECT 1 FROM jsonb_array_elements(s.words_json) w
                                  WHERE lower(w->>'w') = ANY(%s));""", (low, low))
            done["почищено наборов дня"] = int(cur.rowcount or 0)
            cur.execute(
                "UPDATE bt_3_article_sprint_sets "
                "SET word_count = jsonb_array_length(words_json) "
                "WHERE play_date >= CURRENT_DATE;")
        conn.commit()

    # 2. Стоп-лист: без него ночное авто-наполнение завело бы слово заново той же ночью.
    done["занесено в стоп-лист"] = blacklist_article_words(
        [(w, f"владелец: {reason}" if reason else "владелец: не нужно", "") for w, reason in items])

    # 5. Очередь ошибок. Своя функция: она удаляет карточку только если слова не осталось
    #    НИ В ОДНОЙ активной теме — поэтому запускаем после снятия в банке.
    from backend.database import purge_retired_artikel_mistakes
    done["убрано из очередей ошибок"] = purge_retired_artikel_mistakes()
    return done


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("file", help="файл со словами (слово<TAB>причина<TAB>перевод)")
    ap.add_argument("--apply", action="store_true", help="применить, а не показать")
    args = ap.parse_args()

    items = read_words(args.file)
    print(f"слов в списке: {len(items)}")
    low = [w.lower() for w, _ in items]
    for place, n in count_before(low).items():
        print(f"  {place}: {n}")
    if not args.apply:
        print("это был разбор без изменений. Применить: --apply")
        return 0
    for what, n in apply(items).items():
        print(f"  {what}: {n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
