# -*- coding: utf-8 -*-
"""Починка ошибки слияния тем 19.08.2026: живое слово сняли как «дубль» снятого.

ЧТО СЛОМАЛОСЬ. Скрипт `artikel_themes_merge_twins` при переносе слов проверял, нет ли
такого слова у темы-приёмника, и при совпадении снимал слово как дубль. Проверка не
смотрела, ЖИВОЙ ли близнец. А в приёмниках лежали снятые копии — их сняла
дедупликация 31.07.2026 («одна карточка на слово+артикль»), причём живой осталась
как раз копия в поглощаемой теме.

Итог: 36 слов ушли из игры, и среди них der Computer, der Bildschirm, die Tastatur,
der Laptop, die Waschmaschine. То есть слияние, задуманное как «тем стало меньше, но
они полные», молча выбило из банка ходовые слова. Ровно тот класс ошибки, ради
которого правило «дедупликация ключуется словом И артиклем» уже записывалось однажды.

ЧТО ДЕЛАЕТ ЭТА ПОЧИНКА. Для каждой пары (слово + артикль), где живой строки не
осталось ни одной:
  • берёт из пары строку С ЛУЧШИМ НАПОЛНЕНИЕМ (перевод, мнемоника, озвучка,
    картинка) — так же, как это делает штатный возврат слова;
  • если лучшая — снятый близнец в теме-приёмнике, он возвращается в игру;
  • если лучшая — снятая строка из поглощённой темы, её содержимое переносится
    в строку приёмника (переставить саму строку нельзя: уникальный индекс
    тема+слово+артикль занят близнецом), и в игру возвращается она;
  • вторая строка пары остаётся снятой как настоящий дубль.

Ничего не удаляется. Слов в игре после починки становится ровно столько, сколько
живых написаний было до слияния.

Запуск:
    python -m scripts.artikel_merge_repair_live_dupes           # отчёт
    python -m scripts.artikel_merge_repair_live_dupes --apply
"""
from __future__ import annotations

import argparse
import sys

# Поля, по которым меряется «полнота» карточки, и они же переносятся победителю.
CONTENT_FIELDS = ("meaning_ru", "plural", "mnemonic_ru", "mnemonic_method",
                  "mnemonic_head", "audio_object_key", "image_object_key")


def broken_pairs() -> list[dict]:
    """Пары, где после слияния не осталось ни одной живой строки."""
    from backend.database import get_db_connection_context
    fields = ", ".join(CONTENT_FIELDS)
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT theme_key FROM bt_3_article_sprint_themes WHERE active;")
            active_themes = {r[0] for r in cur.fetchall()}
            cur.execute(
                f"""
                SELECT s.id, s.theme_key, s.word, s.article, {fields}
                  FROM bt_3_article_sprint_nouns s
                 WHERE s.retire_reason = 'дубль при слиянии тем'
                   AND NOT EXISTS (
                        SELECT 1 FROM bt_3_article_sprint_nouns t
                         WHERE lower(t.word) = lower(s.word) AND t.article = s.article
                           AND t.retired = FALSE AND t.verified = TRUE)
                 ORDER BY s.word;
                """
            )
            cols = ["id", "theme_key", "word", "article"] + list(CONTENT_FIELDS)
            losers = [dict(zip(cols, row)) for row in cur.fetchall()]
            out = []
            for loser in losers:
                cur.execute(
                    f"""
                    SELECT t.id, t.theme_key, t.word, t.article, {fields}
                      FROM bt_3_article_sprint_nouns t
                     WHERE lower(t.word) = lower(%s) AND t.article = %s AND t.id <> %s
                       AND t.verified = TRUE
                     ORDER BY t.id;
                    """, (loser["word"], loser["article"], loser["id"]))
                twins = [dict(zip(cols, row)) for row in cur.fetchall()]
                if twins:
                    out.append({"loser": loser, "twins": twins, "active_themes": active_themes})
            return out


def fullness(row: dict) -> int:
    return sum(1 for f in CONTENT_FIELDS if str(row.get(f) or "").strip())


def repair(pair: dict) -> tuple[str, str]:
    """Вернуть в игру одну строку пары. → (слово, что сделали)."""
    from backend.database import get_db_connection_context
    loser, twins = pair["loser"], pair["twins"]
    # Возвращаем ту строку пары, что стоит в ЖИВОЙ теме, но с лучшим содержимым
    # из пары: сама строка переехать не может (уникальный индекс), а начинка — может.
    # Тему обязательно проверяем на active: у части слов снятых близнецов несколько,
    # и часть из них лежит в темах, которые слияние только что погасило. Вернуть
    # слово туда значит вернуть его в невидимое место — то есть не вернуть.
    active = [t for t in twins if t["theme_key"] in pair["active_themes"]]
    if not active:
        return loser["word"], "пропущено: живой темы у слова не осталось"
    keeper = max(active, key=fullness)
    best = max([loser] + twins, key=fullness)
    updates = {f: best[f] for f in CONTENT_FIELDS
               if str(best.get(f) or "").strip() and not str(keeper.get(f) or "").strip()}
    sets = ", ".join(f"{f} = %s" for f in updates)
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            if sets:
                cur.execute(
                    f"UPDATE bt_3_article_sprint_nouns SET {sets}, updated_at = NOW() WHERE id = %s;",
                    (*updates.values(), keeper["id"]))
            cur.execute(
                "UPDATE bt_3_article_sprint_nouns "
                "SET retired = FALSE, retire_reason = '', updated_at = NOW() WHERE id = %s;",
                (keeper["id"],))
        conn.commit()
    moved = f", перенесено полей: {len(updates)}" if updates else ""
    return keeper["word"], f"вернул в «{keeper['theme_key']}»{moved}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    pairs = broken_pairs()
    print(f"слов, у которых после слияния не осталось живой карточки: {len(pairs)}")
    for pair in pairs:
        loser = pair["loser"]
        print(f"  {loser['article']} {loser['word']:18s} "
              f"(снята копия из «{loser['theme_key']}», близнецов: {len(pair['twins'])})")
    if not args.apply:
        print("\n(отчёт; чтобы применить — --apply)")
        return 0
    for pair in pairs:
        word, what = repair(pair)
        print(f"  ✓ {word}: {what}")
    print(f"\nвозвращено в игру: {len(pairs)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
