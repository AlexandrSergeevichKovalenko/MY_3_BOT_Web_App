# -*- coding: utf-8 -*-
"""ПЕРЕСУД: применить новый вердикт к уже РЕШЁННОЙ строке очереди приёма синонимов.

ЗАЧЕМ ЭТОТ ФАЙЛ ПОЯВИЛСЯ (14.09.2026)
─────────────────────────────────────
Замер: одноголосый вердикт судьи НЕ ВОСПРОИЗВОДИТСЯ. Те же кандидаты, тот же промпт,
та же модель — и 16% отклонённых синонимов и 38% антонимов меняют решение на противо-
положное. Почти весь банк (374 кандидата у 58 слов) отсудили ОДНИМ голосом 06.09.2026 —
за два дня до того, как владелец ввёл три голоса. Разногласий при трёх голосах 6%.

Штатная `sprint_intake.apply_decision` сюда не годится: она работает только со строками
`status = 'open'`, а эти давно решены. Поэтому отдельная операция с отдельным именем —
пересуд, а не «ещё один приём». Пишет она РОВНО ТО ЖЕ, что и дверь:

  вернуть  — кандидат входит в `bt_3_sprint_bank.accepted`, строка очереди → 'kept';
  убрать   — кандидат уходит из `accepted`, его пример «верного выбора» вычищается из
             trainer_json (иначе тренажёр показал бы карточку слова, которого в списке
             нет), строка очереди → 'removed'.

Решение владельца 14.09.2026, поимённо по кучам: 36 вернуть, 7 убрать, 331 не трогать.
Удаление делается ТОЛЬКО по явному списку: молчание согласием не считается.
"""

from __future__ import annotations

import json
import logging

_ARTICLES = ("der", "die", "das")


def _article_of(de: str) -> str:
    first = str(de or "").strip().split(" ")[0].lower()
    return first if first in _ARTICLES else ""


def revote_row(*, row_id: int, verdict: str, final_de: str | None = None,
               votes: str = "", dry: bool = True) -> dict | None:
    """Применить пересуд к одной строке. `final_de` — написание, которое должно попасть
    в банк (нужно, когда в очереди лежит опечатка). `dry=True` — ничего не пишет.

    Возвращает, что изменилось, или None, если строки нет / она ещё 'open' (такие ведёт
    обычная дверь, а не пересуд).
    """
    from backend.database import get_db_connection_context
    v = str(verdict or "").strip().lower()
    if v not in ("yes", "no"):
        raise ValueError(f"неизвестный вердикт: {verdict!r}")
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT sprint_id, wort, de, ru, relation, status "
                "FROM bt_3_sprint_accepted_review WHERE id = %s FOR UPDATE",
                (int(row_id),),
            )
            row = cur.fetchone()
            if not row:
                return None
            sprint_id, wort, de, ru, relation, status = row
            if status == "open":
                return None                     # это работа двери, не пересуда
            want_de = str(final_de or de).strip()
            # Существительному артикль обязателен: если в очереди он был, а в правленом
            # написании потерялся — возвращаем прежний, своего не придумываем.
            if _article_of(de) and not _article_of(want_de):
                want_de = f"{_article_of(de)} {want_de}"
            new_status = "kept" if v == "yes" else "removed"
            if status == new_status and want_de == de:
                return {"changed": False, "wort": wort, "de": de, "status": status}
            if dry:
                return {"changed": True, "dry": True, "wort": wort, "relation": relation,
                        "de": de, "final_de": want_de, "from": status, "to": new_status}

            cur.execute("SELECT accepted, trainer_json FROM bt_3_sprint_bank "
                        "WHERE sprint_id = %s FOR UPDATE", (sprint_id,))
            bank = cur.fetchone()
            if not bank:
                raise RuntimeError(f"записи банка {sprint_id} больше нет")
            accepted = list(bank[0] or [])
            tj = dict(bank[1] or {})

            if v == "yes":
                # Опечатку чистим и здесь: в accepted могла лежать старая форма.
                accepted = [a for a in accepted
                            if str((a or {}).get("de") or "").strip().lower()
                            not in {de.lower(), want_de.lower()}]
                accepted.append({"de": want_de, "ru": ru})
                # Пример «верного выбора» подписан прежним написанием — переименовываем,
                # иначе тренажёр не найдёт его по новому слову.
                if want_de != de:
                    ex = list(tj.get("correct_examples") or [])
                    for e in ex:
                        if str((e or {}).get("word") or "").strip().lower() == de.lower():
                            e["word"] = want_de
                    tj["correct_examples"] = ex
            else:
                accepted = [a for a in accepted
                            if str((a or {}).get("de") or "").strip().lower() != de.lower()]
                ex = list(tj.get("correct_examples") or [])
                new_ex = [e for e in ex
                          if str((e or {}).get("word") or "").strip().lower() != de.lower()]
                if len(new_ex) != len(ex):
                    tj["correct_examples"] = new_ex

            cur.execute(
                "UPDATE bt_3_sprint_bank SET accepted = %s::jsonb, trainer_json = %s::jsonb "
                "WHERE sprint_id = %s",
                (json.dumps(accepted, ensure_ascii=False),
                 json.dumps(tj, ensure_ascii=False), sprint_id),
            )
            cur.execute(
                "UPDATE bt_3_sprint_accepted_review SET status = %s, judge_verdict = %s, "
                "judge_voice = %s, decision = %s, decided_at = NOW(), judged_at = NOW(), "
                "de = %s WHERE id = %s",
                (new_status, v, str(votes or "revote:3"),
                 f"revote_{'yes' if v == 'yes' else 'no'}", want_de, int(row_id)),
            )
        conn.commit()
    logging.info("revote row=%s %s «%s» → «%s» (%s → %s)",
                 row_id, relation, wort, want_de, status, new_status)
    return {"changed": True, "wort": wort, "relation": relation, "de": de,
            "final_de": want_de, "from": status, "to": new_status,
            "accepted_now": len(accepted)}
