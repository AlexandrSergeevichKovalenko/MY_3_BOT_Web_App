# -*- coding: utf-8 -*-
"""Добавить слово в банк артиклей руками — с артиклем от справочника, а не на веру.

Зачем дверь. Уборка иногда убирает не только форму, но и ПОНЯТИЕ: 16.08.2026 из банка
ушло «das Seifenblasen» (форма множественного числа И неверный артикль), а единственного
«die Seifenblase» в банке не было — и мыльные пузыри исчезли из игры совсем. Вернуть
сломанную строку нельзя, это вернуло бы ошибку; нужно завести слово заново, правильно.
Скрипта для этого не было, и INSERT пришлось бы писать руками — а это ровно тот способ,
которым в банк попадают новые кривые артикли.

Что делает дверь, чего не сделает рука:
  • род берётся у справочника (Wiktionary → правило композита), а не из файла. Файл
    предлагает артикль, справочник его подтверждает или поправляет;
  • справочник молчит — слово НЕ добавляется: выдумывать род мы не будем;
  • справочник говорит «род зависит от смысла» — тоже не добавляется молча, такие слова
    заводятся через курируемые смыслы (article_two_gender), иначе вопрос нечестный;
  • слово, которое уже живёт в банке, не задваивается (правило «слово живёт в одной теме»);
  • написание вычищается из стоп-листа, иначе ночное наполнение сочтёт слово выброшенным.

Картинку и озвучку скрипт НЕ делает — их дозакажут обычные ночные работы.

Файл: тема<TAB>слово<TAB>артикль<TAB>перевод<TAB>мн.число<TAB>подтема
Строки с # — комментарии.

    python -m scripts.artikel_bank_add_words scripts/data/artikel_add_2026_08_17.tsv
    python -m scripts.artikel_bank_add_words <файл> --apply
"""
from __future__ import annotations

import argparse
import sys


def read_rows(path: str) -> list[dict]:
    out: list[dict] = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            p = (line.split("\t") + [""] * 6)[:6]
            if not p[0].strip() or not p[1].strip():
                continue
            out.append({"theme_key": p[0].strip(), "word": p[1].strip(),
                        "article": p[2].strip().lower(), "meaning_ru": p[3].strip(),
                        "plural": p[4].strip(), "subtopic": p[5].strip()})
    return out


def check(rows: list[dict]) -> list[dict]:
    """Проставить артикль от справочника и решить судьбу каждой строки."""
    from backend.article_authority import authoritative_article
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute("SELECT theme_key, article, retired FROM bt_3_article_sprint_nouns "
                            "WHERE lower(word) = lower(%s);", (r["word"],))
                found = cur.fetchall() or []
            # заведомо живые копии — повод не добавлять второй раз
                r["alive_elsewhere"] = [f for f in found if not f[2]]
                r["retired_rows"] = [f for f in found if f[2]]
                verdict, source = authoritative_article(r["word"], allow_network=True)
                r["source"] = source
                if not verdict:
                    r["ok"], r["why"] = False, f"справочник не дал род ({source}) — не выдумываем"
                elif "родовое" in str(source):
                    r["ok"], r["why"] = False, "род зависит от смысла — заводить через курируемые смыслы"
                elif r["alive_elsewhere"]:
                    where = ", ".join(t for t, _a, _r in r["alive_elsewhere"])
                    r["ok"], r["why"] = False, f"слово уже живёт в банке ({where})"
                else:
                    r["ok"] = True
                    r["why"] = ("артикль подтверждён" if verdict == r["article"]
                                else f"артикль поправлен: {r['article'] or '—'} → {verdict}")
                    r["article"] = verdict
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("file")
    ap.add_argument("--apply", action="store_true", help="без него — только отчёт")
    args = ap.parse_args()

    rows = check(read_rows(args.file))
    good = [r for r in rows if r["ok"]]
    bad = [r for r in rows if not r["ok"]]
    print(f"строк в файле: {len(rows)}   ·   добавляем: {len(good)}   ·   отклонено: {len(bad)}\n")
    for r in good:
        print(f"  ✅ {r['article']} {r['word']:<20} → {r['theme_key']}/{r['subtopic']}"
              f"   — {r['meaning_ru']}   [{r['why']}, {r['source']}]")
        if r["retired_rows"]:
            print(f"     (в банке лежат снятые копии: {len(r['retired_rows'])} — их не трогаем)")
    for r in bad:
        print(f"  ⛔ {r['word']:<20} {r['why']}")
    if not good:
        print("\nДобавлять нечего.")
        return 0
    if not args.apply:
        print("\nСУХОЙ ПРОГОН — база не менялась. Повтори с --apply.")
        return 0

    from backend.database import insert_article_sprint_nouns, unblacklist_article_words
    added = 0
    for r in good:
        res = insert_article_sprint_nouns(r["theme_key"], [{
            "word": r["word"], "article": r["article"], "meaning_ru": r["meaning_ru"],
            "plural": r["plural"], "difficulty": "B", "subtopic": r["subtopic"],
            "source": r["source"], "verified": True,
        }])
        added += int(res.get("inserted") or 0)
        print(f"  {r['article']} {r['word']}: добавлено {res.get('inserted')}, "
              f"пропущено {res.get('skipped')}")
    freed = unblacklist_article_words([r["word"] for r in good])
    print(f"\nдобавлено строк: {added}   ·   убрано из стоп-листа: {freed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
