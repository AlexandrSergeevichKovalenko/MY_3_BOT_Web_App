# -*- coding: utf-8 -*-
"""Банк Artikel: форма множественного числа не может быть словом для тренировки.

Почему это дефект
─────────────────
Игра спрашивает «der / die / das?». У формы множественного числа артикль ВСЕГДА die,
знать там нечего, и вопрос остаётся без содержания. Хуже: рядом в банке живёт то же
слово в единственном числе, и человек видит «Handschuh» и «Handschuhe» как два разных
слова.

Владелец 16.08.2026 показал это на «Handschuh»: семь строк в банке (шесть уже сняты
механизмом дедупликации, живой одна) плюс ОТДЕЛЬНАЯ живая строка «Handschuhe» — и
личная карточка «der Handschuhe» с невозможным артиклем.

Дверь уже закрыта
─────────────────
Промпты генератора и проверяющего знают правило («NEVER give a PLURAL form»), и это
закреплено тестом backend/tests/test_artikel_plural_headword.py. Здесь — только уборка
накопленного до того, как дверь закрыли.

Почему таблица явная, а не правило
──────────────────────────────────
Дешёвого признака «это множественное» НЕ существует, и это проверено дважды (см. тот же
тест). Наш указатель форм bt_3_german_form_index честно называет множественным и
«die Pocken», и «das Putzen», и «der Streifen» — а это законные словарные слова. Правило
по указателю сняло бы их из банка и научило бы неправильному.

Поэтому каждая из 19 строк разобрана отдельно, и рядом с каждой написано, почему.
Строка со СНЯТИЕМ применяется только если в банке ЖИВЁТ единственное число этого слова
или если сама строка сломана ещё и артиклем: иначе слово просто исчезло бы из тренировки.

    python3 scripts/artikel_bank_retire_plural_rows.py            # сухой прогон
    python3 scripts/artikel_bank_retire_plural_rows.py --apply    # записать
"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SKIP_BILLING_LEDGER_WRITES", "1")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import (                                    # noqa: E402
    blacklist_article_words,
    get_db_connection_context,
)

# id → (что лежит, единственное число, почему снимаем)
RETIRE = {
    10579: ("Beiträge", "Beitrag", "мн. ч.; «der Beitrag» живёт в банке"),
    6752: ("Einstellungen", "Einstellung", "мн. ч.; «die Einstellung» живёт в банке"),
    10098: ("Handschuhe", "Handschuh", "мн. ч.; «der Handschuh» живёт в банке"),
    10584: ("Hausschuhe", "Hausschuh", "мн. ч.; «der Hausschuh» живёт в банке"),
    9508: ("Kronen", "Krone", "мн. ч.; «die Krone» живёт в банке"),
    9724: ("Mandeln", "Mandel", "мн. ч.; «die Mandel» живёт в банке"),
    7413: ("Papiere", "Papier", "мн. ч.; «das Papier» живёт в банке"),
    6627: ("Pfeile", "Pfeil", "мн. ч.; «der Pfeil» живёт в банке"),
    9491: ("Pralinen", "Praline", "мн. ч.; «die Praline» живёт в банке"),
    346: ("Rippen", "Rippe", "мн. ч.; «die Rippe» живёт в банке"),
    10250: ("Schuhe", "Schuh", "мн. ч.; «der Schuh» живёт в банке"),
    10400: ("Sicherheiten", "Sicherheit", "мн. ч.; «die Sicherheit» живёт в банке"),
    # Здесь сломано ДВАЖДЫ: и множественное, и артикль. Единственное число — «die
    # Seifenblase», а в строке стоит «das». Учить по ней нечему ни в каком виде.
    7798: ("Seifenblasen", "Seifenblase", "мн. ч. И неверный артикль «das» вместо «die»"),
}

# id → почему ОСТАВЛЯЕМ, хотя указатель форм называет это множественным
KEEP = {
    9943: ("Pocken", "оспа — слово живёт только во множественном, единственного нет"),
    13: ("Windpocken", "ветряная оспа — только множественное"),
    7062: ("Putzen", "«das Putzen» — уборка, субстантивированный глагол, а не мн. ч."),
    7134: ("Streifen", "«der Streifen» — полоса, самостоятельное ед. ч.; артикль der это и подтверждает"),
    10096: ("Schrecke", "«die Schrecke» — кузнечик, ед. ч.; своё мн. ч. Schrecken стоит рядом"),
    7966: ("Sorten", "наличная валюта — в банковском смысле термин существует только во мн. ч."),
}

# Настоящие дубли (не двухродовые): одно слово двумя живыми строками.
DEDUPE = {
    991: ("Kollegin", "коллега (ж) — та же строка, что 596 в другой теме"),
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    retired = skipped = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            print("СНИМАЕМ:")
            for row_id, (word, singular, why) in sorted(RETIRE.items(), key=lambda kv: kv[1][0]):
                cur.execute(
                    "SELECT word, article, retired FROM bt_3_article_sprint_nouns WHERE id = %s;",
                    (row_id,),
                )
                row = cur.fetchone()
                if not row:
                    print("   %-7s %-16s строки нет — ПРОПУСК" % (row_id, word))
                    skipped += 1
                    continue
                if row[0] != word:
                    print("   %-7s ожидали %r, лежит %r — ПРОПУСК" % (row_id, word, row[0]))
                    skipped += 1
                    continue
                if row[2]:
                    print("   %-7s %-16s уже снята" % (row_id, word))
                    continue
                print("   %-7s %-16s %s" % (row_id, word, why))
                if args.apply:
                    cur.execute(
                        "UPDATE bt_3_article_sprint_nouns SET retired = TRUE, "
                        "retire_reviewed = TRUE, updated_at = NOW() WHERE id = %s;",
                        (row_id,),
                    )
                    # ⚠ ОДНОГО СНЯТИЯ НЕ ХВАТАЕТ. Замер 18.08.2026: шесть строк, снятых
                    # накануне, снова были живыми. Слово возвращается в банк — либо
                    # генератором, либо разбором снятого (restore_retired_article_noun
                    # прямо ставит retired = FALSE). Вечное решение у нас одно:
                    # стоп-лист с причиной. Формулировку не менять — по ней же
                    # article_authority отказывается брать род у формы мн. числа.
                    blacklist_article_words([(word, "форма множественного числа", "")])
                retired += 1

            print()
            print("СНИМАЕМ ДУБЛЬ:")
            for row_id, (word, why) in sorted(DEDUPE.items()):
                cur.execute("SELECT word, retired FROM bt_3_article_sprint_nouns WHERE id = %s;",
                            (row_id,))
                row = cur.fetchone()
                if not row or row[0] != word:
                    print("   %-7s %-16s не совпало — ПРОПУСК" % (row_id, word))
                    skipped += 1
                    continue
                print("   %-7s %-16s %s" % (row_id, word, why))
                if args.apply:
                    cur.execute(
                        "UPDATE bt_3_article_sprint_nouns SET retired = TRUE, "
                        "retire_reviewed = TRUE, updated_at = NOW() WHERE id = %s;",
                        (row_id,),
                    )
                retired += 1
            if args.apply:
                conn.commit()

    print()
    print("ОСТАВЛЯЕМ — указатель форм ошибается, это словарные слова:")
    for row_id, (word, why) in sorted(KEEP.items(), key=lambda kv: kv[1][0]):
        print("   %-7s %-16s %s" % (row_id, word, why))
    print()
    print("снято: %d, пропущено: %d" % (retired, skipped))
    if not args.apply:
        print("СУХОЙ ПРОГОН. Ничего не записано. Для записи добавь --apply.")


if __name__ == "__main__":
    main()
