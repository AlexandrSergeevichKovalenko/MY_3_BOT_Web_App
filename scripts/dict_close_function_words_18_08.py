# -*- coding: utf-8 -*-
"""Служебные слова, междометия и мусор — по решениям владельца 18.08.2026.

Разбор («part_of_speech») называет часть речи только у знаменательных слов; у союзов,
предлогов и междометий модель отвечает «other». Дверь такой ответ не берёт и правильно
делает — «other» это не часть речи, а отказ. Но список маленький и закрытый, поэтому
проставляем руками, слово за словом, с переводом перед глазами.

Мусор сносим: суффиксы и артикли, сохранённые как слова, с переводами от совсем других
записей. У всех ноль карточек — никто их не учит. Перед удалением это проверяется ещё
раз, и слово с карточками не трогается никогда.

    python3 scripts/dict_close_function_words_18_08.py            # сухой прогон
    python3 scripts/dict_close_function_words_18_08.py --apply    # записать
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import get_db_connection_context            # noqa: E402
from backend.lex_review_decisions import (                        # noqa: E402
    ensure_lex_review_decisions_schema,
    record_decision,
)

# слово → (часть речи, перевод — по нему и решали)
POS = {
    "als": ("conjunction", "поскольку, в той мере, в какой"),
    "da": ("conjunction", "потому что, поскольку"),
    "dass": ("conjunction", "что"),
    "falls": ("conjunction", "если"),
    "obwohl": ("conjunction", "хотя"),
    "Weil": ("conjunction", "потому что, поскольку"),
    "Wenn": ("conjunction", "если"),
    "wohingegen": ("conjunction", "в то время как"),
    "von": ("preposition", "от"),
    "des": ("article", "родительный артикль"),
    "Zum": ("preposition", "к"),
    "eben": ("adverb", "ровно, как раз"),
    "Nichtsdestotrotz": ("adverb", "невзирая на это"),
    "Wo": ("adverb", "где"),
    "anderthalb": ("numeral", "полтора"),
    "vier": ("numeral", "четыре"),
    "tausende": ("numeral", "тысячи"),
    "Alles": ("pronoun", "всё"),
    "nix": ("pronoun", "ничего"),
    # Междометия и обиходные восклицания.
    "jein": ("interjection", "и да, и нет"),
    "Danke": ("interjection", "спасибо"),
    "Schade": ("interjection", "жаль"),
    "Heidenei": ("interjection", "Ух ты!"),
    "Nanu-nana!": ("interjection", "Ну надо же!"),
    "Was?": ("pronoun", "что"),
}

# Мусор: не слова. Удаляем ТОЛЬКО при нуле карточек.
DELETE = {
    "-e": "суффикс, сохранённый как слово; перевод «теракт с бомбой» — от другой записи",
    "-en": "то же, перевод «наводнение»",
    "-n": "то же, перевод «правило запрета»",
    "der": "артикль как слово; перевод «должник по алиментам» — от другой записи",
    "die": "артикль как слово; перевод «дополнительные занятия»",
    "daß...": "старое написание с многоточием, перевод «предположим»",
    "Sanchez": "имя собственное, карточек нет",
}

# Разобрано и оставлено — записываем, чтобы проверки больше не спрашивали.
KEEP = {
    "know-it-all": ("не-немецкое слово", "английское «всезнайка»; человек сохранил сам"),
    "stick-in-the-mud": ("не-немецкое слово", "английское «ретроград»; человек сохранил сам"),
    "angedeutet": ("причастие отдельным словом", "сохранено человеком, перевод «намекнуто»"),
    "gelöst": ("причастие отдельным словом", "сохранено человеком, перевод «решено»"),
    "Stimmt's": ("заголовок не словарная форма", "обиходное «Правда?», так и употребляется"),
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    if args.apply:
        ensure_lex_review_decisions_schema()

    done = skipped = removed = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            print("ЧАСТЬ РЕЧИ:")
            for word, (pos, meaning) in sorted(POS.items(), key=lambda kv: kv[0].lower()):
                cur.execute(
                    "SELECT id, pos FROM bt_3_lex_units WHERE lang='de' AND display=%s;", (word,))
                rows = cur.fetchall()
                if not rows:
                    print("   %-18s слова нет — ПРОПУСК" % word)
                    skipped += 1
                    continue
                for unit_id, current in rows:
                    if current:
                        print("   %-18s уже %s" % (word, current))
                        continue
                    print("   %-18s → %-12s (%s)" % (word, pos, meaning))
                    if args.apply:
                        cur.execute(
                            "UPDATE bt_3_lex_units SET pos=%s, "
                            "pos_source=COALESCE(pos_source,'разбор владельцем 18.08.2026'), "
                            "updated_at=NOW() WHERE id=%s;",
                            (pos, unit_id),
                        )
                    done += 1

            print()
            print("СНОСИМ (только при нуле карточек):")
            for word, why in sorted(DELETE.items()):
                cur.execute(
                    "SELECT u.id, (SELECT count(*) FROM bt_3_webapp_dictionary_queries q "
                    "WHERE q.lex_unit_id=u.id) FROM bt_3_lex_units u "
                    "WHERE u.lang='de' AND u.display=%s;",
                    (word,),
                )
                for unit_id, cards in cur.fetchall():
                    if cards:
                        print("   %-12s карточек %d — НЕ ТРОГАЮ" % (word, cards))
                        skipped += 1
                        continue
                    print("   %-12s %s" % (word, why))
                    if args.apply:
                        cur.execute("DELETE FROM bt_3_lex_links WHERE from_unit=%s OR to_unit=%s;",
                                    (unit_id, unit_id))
                        cur.execute("DELETE FROM bt_3_lex_surfaces WHERE unit_id=%s;", (unit_id,))
                        cur.execute("DELETE FROM bt_3_lex_units WHERE id=%s;", (unit_id,))
                    removed += 1
        if args.apply:
            conn.commit()

    print()
    print("ОСТАВЛЯЕМ — записываем решение:")
    for word, (defect_class, why) in sorted(KEEP.items()):
        print("   %-18s [%s] %s" % (word, defect_class, why))
        if args.apply:
            record_decision(word, defect_class, decision="оставить", reason=why)

    print()
    print("часть речи проставлена: %d, снесено: %d, пропущено: %d" % (done, removed, skipped))
    if not args.apply:
        print("СУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")


if __name__ == "__main__":
    main()
