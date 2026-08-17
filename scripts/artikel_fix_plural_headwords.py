# -*- coding: utf-8 -*-
"""Убрать из банка артиклей заголовки, которые на самом деле форма множественного числа.

Зачем. У формы множественного числа артикль ВСЕГДА die — знать там нечего, и вопрос
«der/die/das?» становится без ответа. Замер 16.08.2026: в банке живут «die Zitate» при
том, что «das Zitat» лежит рядом, «die Mängel» при живом «der Mangel», «das Fotos» (и
артикль кривой). С 16.08 такие слова не проходят приёмку, но накопленные уже внутри.

Кого НЕ трогаем. Слова, у которых единственного числа нет: die Eltern, die Kosten,
die Leute, die Schulden, die Ferien, die Geschwister. Владелец сказал прямо: «если слово
используется в основном во множественном — конечно, оно будет во множественном».

Как отбираем. Дешёвого признака нет — проверял два, и оба врут:
  • «заголовок значится чужим полем мн. числа» ловит die Kohle (это не мн. от der Kohl),
    die Montage (не от der Montag), der Westen (не от die Weste);
  • «своё поле мн. числа пустое» ловит законные die Schulden и das Streben.
Поэтому список только СУЖАЕТСЯ этим признаком, а решает та же проверяющая модель, что
стоит на приёмке (`article_verify`, причина `plural_form`) — и большинством из трёх
голосов, потому что один голос неустойчив (замер 16.08.2026: 12% разнобоя на повторе).

Снятие идёт штатной дверью artikel_bank_retire_words.apply: банк тем, банк квизов,
наборы дня начиная с сегодняшнего, очередь работы над ошибками и стоп-лист. В стоп-лист
попадает САМО написание («Mängel»), поэтому единственное («Mangel») остаётся свободным
и придёт в набор ближайшей ночью.

Запуск:
    python -m scripts.artikel_fix_plural_headwords              # отчёт
    python -m scripts.artikel_fix_plural_headwords --apply
    python -m scripts.artikel_fix_plural_headwords --all        # весь банк, а не только
                                                                # те, чьё ед. ч. рядом
"""
from __future__ import annotations

import argparse
import asyncio
import sys

VOTES = 3      # столько раз спрашиваем проверяющего; решает большинство
CHUNK = 15     # слов в одном вопросе: на 37 проверяющий возвращал пусто


def live_rows() -> list[tuple]:
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT word, article, COALESCE(meaning_ru,''), COALESCE(plural,'') "
                        "FROM bt_3_article_sprint_nouns WHERE NOT retired;")
            return cur.fetchall() or []


def plural_owners(rows: list[tuple]) -> dict[str, str]:
    """слово → чьим множественным числом оно значится. В отчёт это выходит обязательно:
    без строки «Soli ← Solo» человек не видит, на чём держится подозрение."""
    plural_of: dict[str, str] = {}
    for word, _a, _ru, plural in rows:
        key = plural.strip().lower()
        if key and key != word.strip().lower():
            plural_of.setdefault(key, word)
    return {r[0]: plural_of[r[0].strip().lower()] for r in rows
            if plural_of.get(r[0].strip().lower())
            and plural_of[r[0].strip().lower()].strip().lower() != r[0].strip().lower()}


def narrow(rows: list[tuple]) -> list[tuple]:
    """Сузить до слов, которые ДРУГАЯ живая строка держит своим множественным числом.
    Ложные срабатывания тут есть и это нормально — их отсеет проверяющий."""
    plural_of: dict[str, str] = {}
    for word, _a, _ru, plural in rows:
        key = plural.strip().lower()
        if key and key != word.strip().lower():
            plural_of.setdefault(key, word)
    out = []
    for row in rows:
        owner = plural_of.get(row[0].strip().lower())
        if owner and owner.strip().lower() != row[0].strip().lower():
            out.append(row)
    return out


def judge(rows: list[tuple]) -> tuple[dict[str, int], dict[str, int]]:
    """→ (сколько голосов назвали слово множественным, сколько голосов вообще дошло).

    Считать надо по ДОШЕДШИМ голосам. run_article_verify глотает сбой и возвращает
    пустой список — если принять пустоту за «все чисты», молчание модели превращается
    в оправдательный приговор. На первом прогоне 16.08.2026 так и вышло: два голоса из
    трёх не дошли, а скрипт отрапортовал «чистить нечего»."""
    from backend.openai_manager import run_article_verify
    tally = {r[0]: 0 for r in rows}
    heard = {r[0]: 0 for r in rows}
    for vote_no in range(1, VOTES + 1):
        got = lost = 0
        for i in range(0, len(rows), CHUNK):
            batch = rows[i:i + CHUNK]
            # Перевод обязателен: промпт article_verify прямо говорит «The test is
            # meaning, not spelling». Без него судья судит написание — так 16.08.2026
            # под приговор попал der Soli (Solidaritätszuschlag), опознанный как
            # множественное от das Solo. С переводом тот же судья отвечает ok.
            items = [{"word": r[0], "article": r[1], "ru": r[2]} for r in batch]
            # Пачка иногда возвращается пустой (llm_execute отдал не-JSON). Пустота — не
            # «все чисты», а потерянный голос, поэтому пробуем ещё раз, а не идём дальше.
            res = []
            for attempt in range(3):
                try:
                    res = asyncio.run(run_article_verify(items=items))
                except Exception as exc:
                    print(f"  ⚠️  голос {vote_no}: проверяющий упал ({exc})")
                    res = []
                if res:
                    break
                if attempt < 2:
                    print(f"  ↻ голос {vote_no}: пачка вернулась пустой, повтор {attempt + 2}/3")
            answered = {str(e.get("word") or "") for e in (res or []) if isinstance(e, dict)}
            for entry in res or []:
                word = str(entry.get("word") or "")
                if word not in tally:
                    continue
                heard[word] += 1
                if str(entry.get("reason") or "").strip().lower() == "plural_form":
                    tally[word] += 1
            got += len(answered & set(tally))
            lost += sum(1 for r in batch if r[0] not in answered)
        print(f"  голос {vote_no} из {VOTES}: ответов {got}, без ответа {lost}")
    return tally, heard


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="без него — только отчёт")
    ap.add_argument("--all", action="store_true",
                    help="проверить ВЕСЬ банк, а не только слова, чьё ед. число лежит рядом")
    ap.add_argument("--only", default="",
                    help="снять ТОЛЬКО эти слова через запятую (из числа осуждённых). "
                         "Нужно, когда часть кандидатов уносит с собой смысл, которого "
                         "в банке больше не останется, — такое решает владелец, не скрипт")
    args = ap.parse_args()

    rows = live_rows()
    print(f"живых слов в банке: {len(rows)}")
    candidates = rows if args.all else narrow(rows)
    print(f"на проверку идёт: {len(candidates)}"
          + ("" if args.all else "  (у кого единственное число лежит рядом в банке)"))
    if not candidates:
        print("Проверять нечего.")
        return 0

    tally, heard = judge(candidates)
    by_word = {r[0]: r for r in candidates}
    by_article = {r[0]: str(r[1] or "").strip().lower() for r in candidates}
    owners = plural_owners(rows)
    # Большинство — от ДОШЕДШИХ голосов, ничья = «не трогаем». Слово, по которому не
    # дошло ни одного ответа, молчанием не осуждается и не оправдывается: оно просто
    # не разобрано, и это надо назвать вслух.
    # Приговор — только при большинстве И не меньше двух дошедших голосов. Иначе
    # «2 > 1» осуждало слово по ОДНОМУ голосу при заявленных трёх.
    MIN_HEARD = 2
    guilty = sorted(w for w in tally
                    if heard[w] >= MIN_HEARD and tally[w] * 2 > heard[w]
                    and by_article.get(w) == 'die')
    split = sorted(w for w in tally
                   if heard[w] >= MIN_HEARD and 0 < tally[w] * 2 <= heard[w])
    thin = sorted(w for w in tally if 0 < heard[w] < MIN_HEARD)
    silent = sorted(w for w in tally if not heard[w])
    # Бесплатный страж: у формы множественного числа артикль ВСЕГДА die. Не die —
    # значит либо законное слово (der Soli), либо сломанная строка (das Fotos).
    # И то и другое решает человек, автоматически не снимаем.
    wrong_article = sorted(w for w in tally
                           if tally[w] and by_article.get(w) != 'die')

    print(f"\nбольшинство назвало формой множественного: {len(guilty)}")
    for w in guilty:
        row = by_word[w]
        print(f"   {row[1]:>4} {w:<24} — {row[2][:28]:<28} ← мн. от «{owners.get(w, '?')}»"
              f"   ({tally[w]}/{heard[w]} голосов)")
    if split:
        print(f"\nголоса разошлись, НЕ трогаю ({len(split)}): "
              + ", ".join(f"{w} {tally[w]}/{heard[w]}" for w in split))
    if thin:
        print(f"\n⚠️  голосов дошло меньше {MIN_HEARD}, приговор не выношу ({len(thin)}): "
              + ", ".join(f"{w} {tally[w]}/{heard[w]}" for w in thin))
    if silent:
        print(f"\n⚠️  НЕ РАЗОБРАНЫ, ответа не было вовсе ({len(silent)}): {', '.join(silent)}")
    if wrong_article:
        print(f"\n✋ артикль не die — автоматически НЕ снимаю, решает человек ({len(wrong_article)}):")
        for w in wrong_article:
            row = by_word[w]
            print(f"   {row[1]:>4} {w:<24} — {row[2][:28]:<28} ← мн. от «{owners.get(w, '?')}»"
                  f"   ({tally[w]}/{heard[w]} голосов)")

    if not guilty:
        print("\nЧистить нечего.")
        return 0
    if args.only:
        wanted = {w.strip().lower() for w in args.only.split(",") if w.strip()}
        unknown = wanted - {w.lower() for w in guilty}
        if unknown:
            print(f"\n❌ в списке --only есть слова, которых нет среди осуждённых: "
                  f"{', '.join(sorted(unknown))}")
            return 1
        guilty = [w for w in guilty if w.lower() in wanted]
        print(f"\n--only: снимаю {len(guilty)} из них — {', '.join(guilty)}")

    if not args.apply:
        print("\nСУХОЙ ПРОГОН — база не менялась. Повтори с --apply.")
        return 0

    # Список — на диск ДО применения. scripts/artikel_bank_restore_words.py читает файл,
    # а без него отменить правку можно только руками по логу консоли.
    undo = "scripts/data/artikel_plural_removed.tsv"
    with open(undo, "w", encoding="utf-8") as fh:
        fh.write("# снято как форма множественного числа, 16.08.2026\n")
        for w in guilty:
            row = by_word[w]
            fh.write(f"{w}\tформа множественного числа\t{row[2]}\n")
    print(f"\nсписок для отмены записан: {undo}")

    from scripts.artikel_bank_retire_words import apply, count_before
    low = [w.lower() for w in guilty]
    print("\nгде эти слова сейчас лежат:")
    for place, n in count_before(low).items():
        print(f"  {place}: {n}")
    print("\nснимаю:")
    for what, n in apply([(w, "форма множественного числа") for w in guilty]).items():
        print(f"  {what}: {n}")
    # Причина — на саму строку, одной дверью со стоп-листом. Без неё справочник родов
    # продолжит отдавать род у формы множественного числа: 17.08.2026 так и вышло со
    # снятыми «die Schuhe» и «das Seifenblasen».
    from backend.database import retire_bad_word_forms
    res = retire_bad_word_forms(guilty)
    print(f"  причина записана на строках: {res['retired']}, "
          f"написаний в стоп-листе: {res['blacklisted']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
